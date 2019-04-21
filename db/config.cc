/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <unordered_map>
#include <regex>

#include <boost/any.hpp>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

#include "config.hh"
#include "extensions.hh"
#include "log.hh"
#include "utils/config_file_impl.hh"

namespace utils {

template <typename T>
static
json::json_return_type
value_to_json(const T& value) {
    return json::json_return_type(value);
}

static
json::json_return_type
log_level_to_json(const log_level& ll) {
    return value_to_json(format("{}", ll));
}

static
json::json_return_type
log_level_map_to_json(const std::unordered_map<sstring, log_level>& llm) {
    std::unordered_map<sstring, sstring> converted;
    for (auto&& [k, v] : llm) {
        converted[k] = format("{}", v);
    }
    return value_to_json(converted);
}

static
json::json_return_type
seed_provider_to_json(const db::seed_provider_type& spt) {
    return value_to_json("seed_provider_type");
}

template <>
const config_type config_type_for<bool> = config_type("bool", value_to_json<bool>);

template <>
const config_type config_type_for<uint16_t> = config_type("integer", value_to_json<uint16_t>);

template <>
const config_type config_type_for<uint32_t> = config_type("integer", value_to_json<uint32_t>);

template <>
const config_type config_type_for<uint64_t> = config_type("integer", value_to_json<uint64_t>);

template <>
const config_type config_type_for<float> = config_type("float", value_to_json<float>);

template <>
const config_type config_type_for<double> = config_type("double", value_to_json<double>);

template <>
const config_type config_type_for<log_level> = config_type("string", log_level_to_json);

template <>
const config_type config_type_for<sstring> = config_type("string", value_to_json<sstring>);

template <>
const config_type config_type_for<std::vector<sstring>> = config_type("string list", value_to_json<std::vector<sstring>>);

template <>
const config_type config_type_for<std::unordered_map<sstring, sstring>> = config_type("string map", value_to_json<std::unordered_map<sstring, sstring>>);

template <>
const config_type config_type_for<std::unordered_map<sstring, log_level>> = config_type("string map", log_level_map_to_json);

template <>
const config_type config_type_for<int64_t> = config_type("integer", value_to_json<int64_t>);

template <>
const config_type config_type_for<int32_t> = config_type("integer", value_to_json<int32_t>);

template <>
const config_type config_type_for<db::seed_provider_type> = config_type("seed provider", seed_provider_to_json);

}

namespace YAML {

// yaml-cpp conversion would do well to have some enable_if-stuff to make it possible
// to do more broad spectrum converters.
template<>
struct convert<seastar::log_level> {
    static bool decode(const Node& node, seastar::log_level& rhs) {
        std::string tmp;
        if (!convert<std::string>::decode(node, tmp)) {
            return false;
        }
        rhs = boost::lexical_cast<seastar::log_level>(tmp);
        return true;
    }
};

template<>
struct convert<db::config::seed_provider_type> {
    static bool decode(const Node& node, db::config::seed_provider_type& rhs) {
        if (!node.IsSequence()) {
            return false;
        }
        rhs = db::config::seed_provider_type();
        for (auto& n : node) {
            if (!n.IsMap()) {
                continue;
            }
            for (auto& n2 : n) {
                if (n2.first.as<sstring>() == "class_name") {
                    rhs.class_name = n2.second.as<sstring>();
                }
                if (n2.first.as<sstring>() == "parameters") {
                    auto v = n2.second.as<std::vector<db::config::string_map>>();
                    if (!v.empty()) {
                        rhs.parameters = v.front();
                    }
                }
            }
        }
        return true;
    }
};

}

#define str(x)  #x
#define _mk_init(name, type, deflt, status, desc, ...)  , name(this, str(name), value_status::status, type(deflt), desc)

db::config::config(std::shared_ptr<db::extensions> exts)
    : utils::config_file()
    _make_config_values(_mk_init)
    , default_log_level(this, "default_log_level", value_status::Used)
    , logger_log_level(this, "logger_log_level", value_status::Used)
    , log_to_stdout(this, "log_to_stdout", value_status::Used)
    , log_to_syslog(this, "log_to_syslog", value_status::Used)
    , _extensions(std::move(exts))
{}

db::config::config()
    : config(std::make_shared<db::extensions>())
{}

db::config::~config()
{}

const sstring db::config::default_tls_priority("SECURE128:-VERS-TLS1.0");

namespace utils {

template<>
void config_file::named_value<db::config::seed_provider_type>::add_command_line_option(
                boost::program_options::options_description_easy_init& init,
                const std::string_view& name, const std::string_view& desc) {
    init((hyphenate(name) + "-class-name").data(),
                    value_ex(&_value.class_name)->notifier(
                                    [this](auto&&) {_source = config_source::CommandLine;}),
                    desc.data());
    init((hyphenate(name) + "-parameters").data(),
                    value_ex(&_value.parameters)->notifier(
                                    [this](auto&&) {_source = config_source::CommandLine;}),
                    desc.data());
}

}

boost::program_options::options_description_easy_init&
db::config::add_options(boost::program_options::options_description_easy_init& init) {
    config_file::add_options(init);

    data_file_directories.add_command_line_option(init, "datadir", "alias for 'data-file-directories'");
    rpc_port.add_command_line_option(init, "thrift-port", "alias for 'rpc-port'");
    native_transport_port.add_command_line_option(init, "cql-port", "alias for 'native-transport-port'");

    return init;
}

db::fs::path db::config::get_conf_dir() {
    using namespace db::fs;

    path confdir;
    auto* cd = std::getenv("SCYLLA_CONF");
    if (cd != nullptr) {
        confdir = path(cd);
    } else {
        auto* p = std::getenv("SCYLLA_HOME");
        if (p != nullptr) {
            confdir = path(p);
        }
        confdir /= "conf";
    }

    return confdir;
}

void db::config::check_experimental(const sstring& what) const {
    if (!experimental()) {
        throw std::runtime_error(format("{} is currently disabled. Start Scylla with --experimental=on to enable.", what));
    }
}

namespace bpo = boost::program_options;

logging::settings db::config::logging_settings(const bpo::variables_map& map) const {
    struct convert {
        std::unordered_map<sstring, seastar::log_level> operator()(const seastar::program_options::string_map& map) const {
            std::unordered_map<sstring, seastar::log_level> res;
            for (auto& p : map) {
                res.emplace(p.first, (*this)(p.second));
            };
            return res;
        }
        seastar::log_level operator()(const sstring& s) const {
            return boost::lexical_cast<seastar::log_level>(s);
        }
        bool operator()(bool b) const {
            return b;
        }
    };

    auto value = [&map](auto v, auto dummy) {
        auto name = utils::hyphenate(v.name());
        const bpo::variable_value& opt = map[name];

        if (opt.defaulted() && v.is_set()) {
            return v();
        }
        using expected = std::decay_t<decltype(dummy)>;

        return convert()(opt.as<expected>());
    };

    return logging::settings{ value(logger_log_level, seastar::program_options::string_map())
        , value(default_log_level, sstring())
        , value(log_to_stdout, bool())
        , value(log_to_syslog, bool())
    };
}

const db::extensions& db::config::extensions() const {
    return *_extensions;
}

template struct utils::config_file::named_value<seastar::log_level>;

namespace utils {

sstring
config_value_as_json(const db::seed_provider_type& v) {
    // We don't support converting this to json yet
    return "seed_provider_type";
}

sstring
config_value_as_json(const log_level& v) {
    // We don't support converting this to json yet; and because the log_level config items
    // aren't part of config_file::value(), it won't be converted to json in REST
    throw std::runtime_error("config_value_as_json(log_level) is not implemented");
}

sstring config_value_as_json(const std::unordered_map<sstring, log_level>& v) {
    // We don't support converting this to json yet; and because the log_level config items
    // aren't part of config_file::value(), it won't be listed
    throw std::runtime_error("config_value_as_json(const std::unordered_map<sstring, log_level>& v) is not implemented");
}

}
