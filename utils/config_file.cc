/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <unordered_map>

#include <yaml-cpp/yaml.h>

#include <boost/program_options.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/format.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <seastar/json/json_elements.hh>

#include <ranges>

#include "config_file.hh"
#include "config_file_impl.hh"

namespace bpo = boost::program_options;

template<>
std::istream& std::operator>>(std::istream& is, std::unordered_map<seastar::sstring, seastar::sstring>& map) {
   std::istreambuf_iterator<char> i(is), e;

   int level = 0;
   bool sq = false, dq = false, qq = false;
   sstring key, val;
   sstring* ps = &key;

   auto add = [&] {
      if (!key.empty()) {
         map[key] = std::move(val);
      }
      key = {};
      val = {};
      ps = &key;
   };

   while (i != e && level >= 0) {
      auto c = *i++;

      switch (c) {
      case '\\':
         qq = !qq;
         if (qq) {
            continue;
         }
         break;
      case '\'':
         if (!qq) {
            sq = !sq;
         }
         break;
      case '"':
         if (!qq) {
            dq = !dq;
         }
         break;
      case '=':
         if (level <= 1 && !sq && !dq && !qq) {
            ps = &val;
            continue;
         }
         break;
      case '{': case '[':
         if (!sq && !dq && !qq) {
            ++level;
            continue;
         }
         break;
      case ']': case '}':
         if (!sq && !dq && !qq && level > 0) {
            --level;
            continue;
         }
         break;
      case ',':
         if (level == 1 && !sq && !dq && !qq) {
            add();
            continue;
         }
         break;
      case ' ': case '\t': case '\n':
         if (!sq && !dq && !qq) {
            continue;
         }
         break;
      default:
         break;
      }

      if (level == 0) {
         ++level;
      }
      qq = false;
      ps->append(&c, 1);
   }

   add();

   return is;
}

template<>
std::istream& std::operator>>(std::istream& is, std::vector<seastar::sstring>& res) {
   std::istreambuf_iterator<char> i(is), e;

   int level = 0;
   bool sq = false, dq = false, qq = false;
   sstring val;

   auto add = [&] {
      if (!val.empty()) {
         res.emplace_back(std::exchange(val, {}));
      }
      val = {};
   };

   while (i != e && level >= 0) {
      auto c = *i++;

      switch (c) {
      case '\\':
         qq = !qq;
         if (qq) {
            continue;
         }
         break;
      case '\'':
         if (!qq) {
            sq = !sq;
         }
         break;
      case '"':
         if (!qq) {
            dq = !dq;
         }
         break;
      case '{': case '[':
         if (!sq && !dq && !qq) {
            ++level;
            continue;
         }
         break;
      case '}': case ']':
         if (!sq && !dq && !qq && level > 0) {
            --level;
            continue;
         }
         break;
      case ',':
         if (level == 1 && !sq && !dq && !qq) {
            add();
            continue;
         }
         break;
      case ' ': case '\t': case '\n':
         if (!sq && !dq && !qq) {
            continue;
         }
         break;
      default:
         break;
      }

      if (level == 0) {
         ++level;
      }
      qq = false;
      val.append(&c, 1);
   }

   add();

   return is;
}

thread_local unsigned utils::config_file::s_shard_id = 0;

json::json_return_type
utils::config_type::to_json(const void* value) const {
    return _to_json(value);
}

bool
utils::config_file::config_src::matches(std::string_view name) const {
    // The below line provides support for option names in the "long_name,short_name" format,
    // such as "workdir,W". We only want the long name ("workdir") to be used in the YAML.
    // But since at some point (due to a bug) the YAML config parser expected the silly
    // double form ("workdir,W") instead, we support both for backward compatibility.
    std::string_view long_name = _name.substr(0, _name.find_first_of(','));

    if (_name == name || long_name == name) {
        return true;
    }
    if (!_alias.empty() && _alias == name) {
        return true;
    }
    return false;
}

json::json_return_type
utils::config_file::config_src::value_as_json() const {
    return _type->to_json(current_value());
}

sstring utils::hyphenate(const std::string_view& v) {
    sstring result(v.begin(), v.end());
    std::replace(result.begin(), result.end(), '_', '-');
    return result;
}

utils::config_file::config_file(std::initializer_list<cfg_ref> cfgs)
    : _cfgs(cfgs), _initialization_completed(false)
{}

void utils::config_file::add(cfg_ref cfg, std::unique_ptr<any_value> value) {
    if (_per_shard_values.size() != 1) {
        throw std::runtime_error("Can only add config_src to config_file during initialization");
    }
    _cfgs.emplace_back(cfg);
    auto undo = defer([&] { _cfgs.pop_back(); });
    cfg.get()._per_shard_values_offset = _per_shard_values[0].size();
    _per_shard_values[0].emplace_back(std::move(value));
    undo.cancel();
}

void utils::config_file::add(std::initializer_list<cfg_ref> cfgs) {
    _cfgs.insert(_cfgs.end(), cfgs.begin(), cfgs.end());
}

void utils::config_file::add(const std::vector<cfg_ref> & cfgs) {
    _cfgs.insert(_cfgs.end(), cfgs.begin(), cfgs.end());
}

bpo::options_description utils::config_file::get_options_description() {
    bpo::options_description opts("");
    return get_options_description(opts);
}

bpo::options_description utils::config_file::get_options_description(boost::program_options::options_description opts) {
    auto init = opts.add_options();
    add_options(init);
    return opts;
}

bpo::options_description_easy_init&
utils::config_file::add_options(bpo::options_description_easy_init& init) {
    for (config_src& src : _cfgs) {
        if (src.status() == value_status::Used) {
            src.add_command_line_option(init);
        }
    }
    return init;
}


bpo::options_description_easy_init&
utils::config_file::add_deprecated_options(bpo::options_description_easy_init& init) {
    for (config_src& src : _cfgs) {
        if (src.status() == value_status::Deprecated) {
            src.add_command_line_option(init);
        }
    }
    return init;
}

void utils::config_file::read_from_yaml(const sstring& yaml, error_handler h) {
    read_from_yaml(yaml.c_str(), std::move(h));
}

void utils::config_file::read_from_yaml(const char* yaml, error_handler h) {
    std::unordered_map<sstring, cfg_ref> values;

    if (!h) {
        h = [](auto & opt, auto & msg, auto) {
            throw std::invalid_argument(msg + " : " + opt);
        };
    }
    /*
     * Note: this is not very "half-fault" tolerant. I.e. there could be
     * yaml syntax errors that origin handles and still sets the options
     * where as we don't...
     * There are no exhaustive attempts at converting, we rely on syntax of
     * file mapping to the data type...
     */
    auto doc = YAML::Load(yaml);
    for (auto node : doc) {
        auto label = node.first.as<sstring>();

        auto i = std::find_if(_cfgs.begin(), _cfgs.end(), [&label](const config_src& cfg) { return cfg.matches(label); });
        if (i == _cfgs.end()) {
            h(label, "Unknown option", std::nullopt);
            continue;
        }

        config_src& cfg = *i;

        if (cfg.source() > config_source::SettingsFile) {
            // already set
            continue;
        }
        switch (cfg.status()) {
        case value_status::Deprecated:
            h(label, "Option is deprecated", cfg.status());
            continue;
        case value_status::Invalid:
            h(label, "Option is not applicable", cfg.status());
            continue;
        case value_status::Unused:
        default:
            break;
        }
        if (node.second.IsNull()) {
            continue;
        }
        // Still, a syntax error is an error warning, not a fail
        try {
            cfg.set_value(node.second, this->_initialization_completed ? config_source::SettingsFile : config_source::None);
        } catch (std::exception& e) {
            h(label, e.what(), cfg.status());
        } catch (...) {
            h(label, "Could not convert value", cfg.status());
        }
    }
}

utils::config_file::configs utils::config_file::set_values() const {
    return _cfgs | std::views::filter([] (const config_src& cfg) {
        return cfg.status() > value_status::Used || cfg.source() > config_source::None;
    }) | std::ranges::to<configs>();
}

utils::config_file::configs utils::config_file::unset_values() const {
    configs res;
    for (config_src& cfg : _cfgs) {
        if (cfg.status() > value_status::Used) {
            continue;
        }
        if (cfg.source() > config_source::None) {
            continue;
        }
        res.emplace_back(cfg);
    }
    return res;
}

future<> utils::config_file::read_from_file(file f, error_handler h) {
    return f.size().then([this, f, h](size_t s) {
        return do_with(make_file_input_stream(f), [this, s, h](input_stream<char>& in) {
            return in.read_exactly(s).then([this, h](temporary_buffer<char> buf) {
               read_from_yaml(sstring(buf.begin(), buf.end()), h);
                if (!_initialization_completed) {
                    // Boolean value set on only one shard, but broadcast_to_all_shards().get() called later
                    // in main.cc will apply the required memory barriers anyway.
                    _initialization_completed = true;
                }
            });
        });
    });
}

future<> utils::config_file::read_from_file(const sstring& filename, error_handler h) {
    return open_file_dma(filename, open_flags::ro).then([this, h](file f) {
       return read_from_file(std::move(f), h);
    });
}

future<> utils::config_file::broadcast_to_all_shards() {
    return async([this] {
        if (_per_shard_values.size() != smp::count) {
            _per_shard_values.resize(smp::count);
            smp::invoke_on_all([this] {
                auto cpu = this_shard_id();
                if (cpu != 0) {
                    s_shard_id = cpu;
                    auto& shard_0_values = _per_shard_values[0];
                    auto nr_values = shard_0_values.size();
                    auto& this_shard_values = _per_shard_values[cpu];
                    this_shard_values.resize(nr_values);
                    for (size_t i = 0; i != nr_values; ++i) {
                        this_shard_values[i] = shard_0_values[i]->clone();
                    }
                }
            }).get();
        } else {
            smp::invoke_on_all([this] {
                if (s_shard_id != 0) {
                    auto& shard_0_values = _per_shard_values[0];
                    auto nr_values = shard_0_values.size();
                    auto& this_shard_values = _per_shard_values[s_shard_id];
                    for (size_t i = 0; i != nr_values; ++i) {
                        this_shard_values[i]->update_from(shard_0_values[i].get());
                    }
                }
            }).get();
        }

        // #4713
        // We can have values retained that are not pointing to
        // our storage (extension config). Need to broadcast
        // these configs as well.
        std::set<config_file *> files;
        for (config_src& v : _cfgs) {
            auto f = v.get_config_file();
            if (f != this) {
                files.insert(f);
            }
        }
        for (auto* f : files) {
            f->broadcast_to_all_shards().get();
        }
    });
}

sstring utils::config_file::config_src::source_name() const noexcept {
    auto src = source();

    switch (src) {
    case utils::config_file::config_source::None:
        return "default";
    case utils::config_file::config_source::SettingsFile:
        return "config";
    case utils::config_file::config_source::CommandLine:
        return "cli";
    case utils::config_file::config_source::Internal:
        return "internal";
    case utils::config_file::config_source::CQL:
        return "cql";
    case utils::config_file::config_source::API:
        return "api";
    }

    __builtin_unreachable();
}
