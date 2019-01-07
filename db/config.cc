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

#define _mk_name(name, ...) name,
#define str(x)  #x
#define _mk_init(name, type, deflt, status, desc, ...)  , name(str(name), type(deflt), desc)

db::config::config(std::shared_ptr<db::extensions> exts)
    : utils::config_file({ _make_config_values(_mk_name)
        default_log_level, logger_log_level, log_to_stdout, log_to_syslog })
    _make_config_values(_mk_init)
    , default_log_level("default_log_level")
    , logger_log_level("logger_log_level")
    , log_to_stdout("log_to_stdout")
    , log_to_syslog("log_to_syslog")
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
void config_file::named_value<db::config::seed_provider_type,
                db::config::value_status::Used>::add_command_line_option(
                boost::program_options::options_description_easy_init& init,
                const stdx::string_view& name, const stdx::string_view& desc) {
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

boost::filesystem::path db::config::get_conf_dir() {
    using namespace boost::filesystem;

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
        throw std::runtime_error(sprint("%s is currently disabled. Start Scylla with --experimental=on to enable.", what));
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

template struct utils::config_file::named_value<seastar::log_level, utils::config_file::value_status::Used>;
