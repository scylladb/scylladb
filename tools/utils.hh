/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include "seastarx.hh"

namespace db {
    class extensions;
    class config;
}

namespace tools::utils {

class basic_option {
public:
    const char* name;
    const char* description;

public:
    basic_option(const char* name, const char* description) : name(name), description(description) { }

    virtual void add_option(boost::program_options::options_description& opts) const = 0;
    virtual app_template::positional_option to_positional_option() const = 0;
};

template <typename T = std::monostate>
class typed_option : public basic_option {
    std::optional<T> _default_value;
    int _count;

    virtual void add_option(boost::program_options::options_description& opts) const override {
        if (_default_value) {
            opts.add_options()(name, boost::program_options::value<T>()->default_value(*_default_value), description);
        } else {
            opts.add_options()(name, boost::program_options::value<T>(), description);
        }
    }
    virtual app_template::positional_option to_positional_option() const override {
        if (_default_value) {
            return {name, boost::program_options::value<T>()->default_value(*_default_value), description, _count};
        } else {
            return {name, boost::program_options::value<T>(), description, _count};
        }
    }

public:
    typed_option(const char* name, const char* description) : basic_option(name, description) { }
    typed_option(const char* name, T default_value, const char* description) : basic_option(name, description), _default_value(std::move(default_value)) { }
    typed_option(const char* name, const char* description, int count) : basic_option(name, description), _count(count) { }
};

template <>
class typed_option<std::monostate> : public basic_option {
    virtual void add_option(boost::program_options::options_description& opts) const override {
        opts.add_options()(name, description);
    }
    virtual app_template::positional_option to_positional_option() const override {
        throw std::runtime_error(fmt::format("typed_option<> (option {}) cannot be used as positional option", name));
    }
public:
    typed_option(const char* name, const char* description) : basic_option(name, description) { }
};

class operation_option {
    shared_ptr<basic_option> _opt; // need copy to support convenient range declaration of std::vector<option>

public:
    template <typename T>
    operation_option(typed_option<T> opt) : _opt(make_shared<typed_option<T>>(std::move(opt))) { }

    const char* name() const { return _opt->name; }
    const char* description() const { return _opt->description; }
    void add_option(boost::program_options::options_description& opts) const { _opt->add_option(opts); }
    app_template::positional_option to_positional_option() const { return _opt->to_positional_option(); }
};

class operation {
    std::vector<std::string> _name = {};
    std::vector<std::string> _aliases;
    std::string _summary;
    std::string _description;
    std::vector<operation_option> _options;
    std::vector<operation_option> _positional_options;
    std::vector<operation> _suboperations;
public:
    operation(
            std::string name,
            std::vector<std::string> aliases,
            std::string summary,
            std::string description,
            std::vector<operation_option> options = {},
            std::vector<operation_option> positional_options = {},
            std::vector<operation> suboperations = {})
        : _name({std::move(name)})
        , _aliases(std::move(aliases))
        , _summary(std::move(summary))
        , _description(std::move(description))
        , _options(std::move(options))
        , _positional_options(std::move(positional_options))
        , _suboperations(std::move(suboperations)) {
        for (auto& op: _suboperations) {
            op.set_superoperation_name(_name[0]);
        }
    }

    operation(
            std::string name,
            std::string summary,
            std::string description,
            std::vector<operation_option> options = {},
            std::vector<operation_option> positional_options = {},
            std::vector<operation> suboperations = {})
        : operation(std::move(name), {}, std::move(summary), std::move(description), std::move(options), std::move(positional_options), std::move(suboperations))
    {}

    const std::string& name() const { return _name[0]; }
    const std::vector<std::string>& fullname() const { return _name; }
    const std::vector<std::string>& aliases() const { return _aliases; }
    const std::string& summary() const { return _summary; }
    const std::string& description() const { return _description; }
    const std::vector<operation_option>& options() const { return _options; }
    const std::vector<operation_option>& positional_options() const { return _positional_options; }
    const std::vector<operation>& suboperations() const { return _suboperations; }

    // Does the name or any of the aliases matches the provided name?
    bool matches(std::string_view name) const;
private:
    void set_superoperation_name(std::string& name) {
        _name.push_back(name);
        for (auto& op: _suboperations) {
            op.set_superoperation_name(name);
        }
    }
};

inline bool operator<(const operation& a, const operation& b) {
    return a.name() < b.name();
}

// Add these to tool config to have and init the db extensions
// needed for some operations, say, decoding all sstables
struct db_config_and_extensions {
    db_config_and_extensions();
    db_config_and_extensions(db_config_and_extensions&&);
    ~db_config_and_extensions();

    std::shared_ptr<db::extensions> extensions;
    std::unique_ptr<db::config> db_cfg;
};

class tool_app_template {
public:
    static const std::vector<std::pair<const char*, const char*>> help_arguments;

public:
    struct config {
        sstring name;
        sstring description;
        sstring logger_name;
        size_t lsa_segment_pool_backend_size_mb = 1;
        std::vector<operation> operations;
        const std::vector<operation_option>* global_options = nullptr;
        const std::vector<operation_option>* global_positional_options = nullptr;
        std::optional<db_config_and_extensions> db_cfg_ext = std::nullopt;
    };

private:
    config _cfg;
public:
    tool_app_template(config);

    const config& cfg() const {
        return _cfg;
    }

    const config& get_config() const { return _cfg; }

    int run_async(int argc, char** argv, noncopyable_function<int(const operation&, const boost::program_options::variables_map&)> main_func);
};

} // namespace tools::utils
