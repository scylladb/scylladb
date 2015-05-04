/*
 * Copyright 2015 Cloudius Systems
 *
 */

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>
#include <unordered_map>
#include "config.hh"
#include "core/file.hh"
#include "core/reactor.hh"
#include "core/shared_ptr.hh"
#include "core/fstream.hh"
#include "core/do_with.hh"
#include "log.hh"

static thread_local logging::logger logger("config");

db::config::config()
:
        // Initialize members to defaults.
#define _mk_init(name, type, deflt, status, desc, ...)      \
        name(deflt),
    _make_config_values(_mk_init)
    _dummy(0)
{}

namespace bpo = boost::program_options;

namespace YAML {
/*
 * Add converters as needed here...
 */
template<>
struct convert<sstring> {
    static Node encode(sstring rhs) {
        auto p = rhs.c_str();
        return convert<const char *>::encode(p);
    }
    static bool decode(const Node& node, sstring& rhs) {
        std::string tmp;
        if (!convert<std::string>::decode(node, tmp)) {
            return false;
        }
        rhs = tmp;
        return true;
    }
};
template<typename K, typename V>
struct convert<std::unordered_map<K, V>> {
    static Node encode(const std::unordered_map<K, V>& rhs) {
        Node node(NodeType::Map);
        for(typename std::map<K, V>::const_iterator it=rhs.begin();it!=rhs.end();++it)
            node.force_insert(it->first, it->second);
        return node;
    }
    static bool decode(const Node& node, std::unordered_map<K, V>& rhs) {
        if (!node.IsMap()) {
            return false;
        }
        rhs.clear();
        for (auto& n : node) {
            rhs[n.first.as<K>()] = n.second.as<V>();
        }
        return true;
    }
};
template<>
struct convert<db::config::seed_provider_type> {
    static Node encode(const db::config::seed_provider_type& rhs) {
        throw std::runtime_error("should not reach");
    }
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

/*
 * Helper type to do compile time exclusion of Unused/invalid options from
 * command line.
 *
 * Only opts marked "used" should get a boost::opt
 *
 */
template<typename T, db::config::value_status S>
struct do_value_opt;

template<typename T>
struct do_value_opt<T, db::config::value_status::Used> {
    template<typename Func>
    void operator()(Func&& func, const char* name, const T& dflt, db::config::value<T, db::config::value_status::Used>& dst, const char* desc) const {
        func(name, dflt, dst, desc);
    }
};

template<typename T>
struct do_value_opt<T, db::config::value_status::Unused> {
    template<typename... Args> void operator()(Args&&... args) const {}
};
template<typename T>
struct do_value_opt<T, db::config::value_status::Invalid> {
    template<typename... Args> void operator()(Args&&... args) const {}
};

bpo::options_description db::config::get_options_description() {
    bpo::options_description opts("Urchin options");
    auto init = opts.add_options();
    add_options(init);
    return std::move(opts);
}

/*
 * Our own bpo::typed_valye.
 * Only difference is that we _don't_ apply defaults (they are already applied)
 * Needed to make aliases work properly.
 */
template<class T, class charT = char>
class typed_value_ex : public bpo::typed_value<T, charT> {
public:
    typedef bpo::typed_value<T, charT> _Super;

    typed_value_ex(T* store_to)
        : _Super(store_to)
    {}
    bool apply_default(boost::any& value_store) const override {
        return false;
    }
};

template<class T>
inline typed_value_ex<T>* value_ex(T* v) {
    typed_value_ex<T>* r = new typed_value_ex<T>(v);
    return r;
}

bpo::options_description_easy_init& db::config::add_options(bpo::options_description_easy_init& init) {
    auto opt_add =
            [&init](const char* name, const auto& dflt, auto& dst, const char* desc) mutable {
                sstring tmp(name);
                std::replace(tmp.begin(), tmp.end(), '_', '-');
                init(tmp.c_str(),
                        value_ex(&dst._value)->default_value(dflt)->notifier([dst](auto) mutable {
                                    dst._source = config_source::CommandLine;
                                })
                        , desc);
            };

    // Add all used opts as command line opts
#define _add_boost_opt(name, type, deflt, status, desc, ...)      \
        do_value_opt<type, value_status::status>()(opt_add, #name, type( deflt ), name, desc);
    _make_config_values(_add_boost_opt)

    auto alias_add =
            [&init](const char* name, auto& dst, const char* desc) mutable {
                init(name,
                        value_ex(&dst._value)->notifier([dst](auto) mutable {
                                    dst._source = config_source::CommandLine;
                                })
                        , desc);
            };


    // Handle "old" syntax with "aliases"
    alias_add("datadir", data_file_directories, "alias for 'data-file-directories'");
    alias_add("thrift-port", rpc_port, "alias for 'rpc-port'");
    alias_add("cql-port", native_transport_port, "alias for 'native-transport-port'");

    return init;
}

// Virtual dispatch to convert yaml->data type.
struct handle_yaml {
    virtual ~handle_yaml() {};
    virtual void operator()(const YAML::Node&) = 0;
    virtual db::config::value_status status() const = 0;
    virtual db::config::config_source source() const = 0;
};

template<typename T, db::config::value_status S>
struct handle_yaml_impl : public handle_yaml {
    typedef db::config::value<T, S> value_type;

    handle_yaml_impl(value_type& v, db::config::config_source& src) : _dst(v), _src(src) {}
    void operator()(const YAML::Node& node) override {
        _dst(node.as<T>());
        _src = db::config::config_source::SettingsFile;
    }
    db::config::value_status status() const override {
        return _dst.status();
    }
    db::config::config_source source() const override {
        return _src;
    }
    value_type& _dst;
    db::config::config_source&
        _src;
};

void db::config::read_from_yaml(const sstring& yaml) {
    read_from_yaml(yaml.c_str());
}

void db::config::read_from_yaml(const char* yaml) {
    std::unordered_map<sstring, std::unique_ptr<handle_yaml>> values;

#define _add_yaml_opt(name, type, deflt, status, desc, ...)      \
        values.emplace(#name, std::make_unique<handle_yaml_impl<type, value_status::status>>(name, name._source));
    _make_config_values(_add_yaml_opt)

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
        auto i = values.find(label);
        if (i == values.end()) {
            logger.warn("Unknown option {} ignored.", label);
            continue;
        }
        if (i->second->source() > config_source::SettingsFile) {
            logger.debug("Option {} already set by commandline. ignored.", label);
            continue;
        }
        switch (i->second->status()) {
        case value_status::Invalid:
            logger.warn("Option {} is not applicable. Ignoring.", label);
            continue;
        case value_status::Unused:
            logger.debug("Option {} is not (yet) used.", label);
            break;
        default:
            break;
        }
        if (node.second.IsNull()) {
            logger.debug("Option {}, empty value. Skipping.", label);
            continue;
        }
        // Still, a syntax error is an error warning, not a fail
        try {
            (*i->second)(node.second);
        } catch (...) {
            logger.error("Option {}, exception while converting value.", label);
        }
    }
}

future<> db::config::read_from_file(file f) {
    auto sf = make_lw_shared<file>(std::move(f));
    return sf->size().then([this, sf](size_t s) {
        return do_with(make_file_input_stream(sf), [this, s](input_stream<char>& in) {
            return in.read_exactly(s).then([this](temporary_buffer<char> buf) {
               read_from_yaml(sstring(buf.begin(), buf.end()));
            });
        });
    });
}

future<> db::config::read_from_file(const sstring& filename) {
    return engine().open_file_dma(filename, open_flags::ro).then([this](file f) {
       return read_from_file(std::move(f));
    });
}
