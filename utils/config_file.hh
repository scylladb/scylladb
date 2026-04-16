/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <fmt/format.h>

#include <unordered_map>
#include <string_view>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include "utils/updateable_value.hh"

#include "seastarx.hh"

namespace seastar { class file; }
namespace seastar::json { class json_return_type; }
namespace YAML { class Node; }

namespace boost::program_options {

class options_description;
class options_description_easy_init;

}

namespace utils {

namespace bpo = boost::program_options;

class config_type {
    std::string_view _name;
    std::function<json::json_return_type (const void*)> _to_json;
private:
    template <typename NativeType>
    std::function<json::json_return_type (const void*)> make_to_json(json::json_return_type (*func)(const NativeType&)) {
        return [func] (const void* value) {
            return func(*static_cast<const NativeType*>(value));
        };
    }
public:
    template <typename NativeType>
    config_type(std::string_view name, json::json_return_type (*to_json)(const NativeType&)) : _name(name), _to_json(make_to_json(to_json)) {}
    std::string_view name() const { return _name; }
    json::json_return_type to_json(const void* value) const;
};

template <typename T>
extern const config_type& config_type_for();

class config_file {
    static thread_local unsigned s_shard_id;
    struct any_value {
        virtual ~any_value() = default;
        virtual std::unique_ptr<any_value> clone() const = 0;
        virtual void update_from(const any_value* source) = 0;
    };
    std::vector<std::vector<std::unique_ptr<any_value>>> _per_shard_values { 1 };
public:
    typedef std::unordered_map<sstring, sstring> string_map;
    typedef std::vector<sstring> string_list;

    enum class value_status {
        Used, ///< a valid option which changes scylla's behavior. only the
              ///< "Used" options are added to the command line options,
              ///< and can be specified with command line.
        Unused, ///< an option inherited or not yet implemented.
                ///< We want to minimize their "visibility" from user. So
                ///< despite that they are still accepted in the config file,
                ///< they are not considered as valid options if specified
                ///< using the command line anymore.
                ///< initially, we had loads of these options from Cassandra.
                ///< they are also used for options supported by the older
                ///< versions of Scylla.
                ///< (In the past used to deprecate an option,
                ///< now there's a separate Deprecated enumerator for that purpose)
        Invalid, ///< an option inherited, but we don't intend to
                 ///< implement it. on the contrary, we print a warning message
                 ///< at seeing each of these options if specified when booting
                 ///< up. these options are kept around only to ease the
                 ///< migration process so user can keep using their existing
                 ///< settings.
                 ///< (In the past used to deprecate an option,
                 ///< now there's a separate Deprecated enumerator for that purpose)
        Deprecated, ///< an option that was used once, but became deprecated.
                    ///< If specified in configuration file or as a cmd line param,
                    ///< we'll parse it and print a warning saying it's deprecated.
                    ///< You may expect such option will become Unused/Invalid
                    ///< or will be removed altogether some day.
    };

    enum class liveness {
        LiveUpdate,
        MustRestart,
    };

    enum class config_source : uint8_t {
        None,
        SettingsFile,
        CommandLine,
        CQL,
        Internal,
        API,
    };

    struct config_src {
        config_file* _cf;
        std::string_view _name, _alias, _desc;
        const config_type* _type;
        size_t _per_shard_values_offset;
    protected:
        virtual const void* current_value() const = 0;
    public:
        config_src(config_file* cf, std::string_view name, const config_type* type, std::string_view desc)
            : _cf(cf)
            , _name(name)
            , _desc(desc)
            , _type(type)
        {}
        config_src(config_file* cf, std::string_view name, std::string_view alias, const config_type* type, std::string_view desc)
            : _cf(cf)
            , _name(name)
            , _alias(alias)
            , _desc(desc)
            , _type(type)
        {}
        virtual ~config_src() {}

        const std::string_view & name() const {
            return _name;
        }
        std::string_view alias() const {
            return _alias;
        }
        const std::string_view & desc() const {
            return _desc;
        }
        std::string_view type_name() const {
            return _type->name();
        }
        config_file * get_config_file() const {
            return _cf;
        }
        bool matches(std::string_view name) const;
        virtual void add_command_line_option(bpo::options_description_easy_init&) = 0;
        virtual void set_value(const YAML::Node&, config_source) = 0;
        virtual bool set_value(sstring, config_source) = 0;
        virtual future<bool> set_value_on_all_shards(sstring, config_source) = 0;
        virtual value_status status() const noexcept = 0;
        virtual config_source source() const noexcept = 0;
        sstring source_name() const noexcept;
        json::json_return_type value_as_json() const;
    };

    template<typename T>
    struct named_value : public config_src {
    private:
        friend class config;
        config_source _source = config_source::None;
        value_status _value_status;
        struct the_value_type final : any_value {
            the_value_type(T value);
            utils::updateable_value_source<T> value;
            std::unique_ptr<any_value> clone() const override;
            void update_from(const any_value* source) override;
        };
        liveness _liveness;
        std::vector<T> _allowed_values;
    protected:
        updateable_value_source<T>& the_value();
        const updateable_value_source<T>& the_value() const;
        const void* current_value() const override;
    public:
        typedef T type;
        typedef named_value<T> MyType;

        named_value(config_file* file, std::string_view name, std::string_view alias, liveness liveness_, value_status vs, const T& t = T(), std::string_view desc = {},
                std::initializer_list<T> allowed_values = {});
        named_value(config_file* file, std::string_view name, liveness liveness_, value_status vs, const T& t = T(), std::string_view desc = {},
                std::initializer_list<T> allowed_values = {});
        named_value(config_file* file, std::string_view name, std::string_view alias, value_status vs, const T& t = T(), std::string_view desc = {},
                std::initializer_list<T> allowed_values = {});
        named_value(config_file* file, std::string_view name, value_status vs, const T& t = T(), std::string_view desc = {},
                std::initializer_list<T> allowed_values = {});
        value_status status() const noexcept override;
        config_source source() const noexcept override;
        bool is_set() const;
        MyType & operator()(const T& t, config_source src = config_source::Internal);
        MyType & operator()(T&& t, config_source src = config_source::Internal);
        void set(T&& t, config_source src = config_source::None);
        const T& operator()() const;

        operator updateable_value<T>() const &;

        observer<T> observe(std::function<void (const T&)> callback) const;

        void add_command_line_option(bpo::options_description_easy_init&) override;
        void set_value(const YAML::Node&, config_source) override;
        bool set_value(sstring, config_source) override;
        // For setting a single value on all shards,
        // without having to call broadcast_to_all_shards
        // that broadcasts all values to all shards.

        future<bool> set_value_on_all_shards(sstring, config_source) override;
    };

    typedef std::reference_wrapper<config_src> cfg_ref;

    config_file(std::initializer_list<cfg_ref> = {});
    config_file(const config_file&) = delete;

    virtual ~config_file() = default;

    void add(cfg_ref, std::unique_ptr<any_value> value);
    void add(std::initializer_list<cfg_ref>);
    void add(const std::vector<cfg_ref> &);

    boost::program_options::options_description get_options_description();
    boost::program_options::options_description get_options_description(boost::program_options::options_description);

    boost::program_options::options_description_easy_init&
    add_options(boost::program_options::options_description_easy_init&);
    boost::program_options::options_description_easy_init&
    add_deprecated_options(boost::program_options::options_description_easy_init&);

    /**
     * Default behaviour for yaml parser is to throw on
     * unknown stuff, invalid opts or conversion errors.
     *
     * Error handling function allows overriding this.
     *
     * error: <option name>, <message>, <optional value_status>
     *
     * The last arg, opt value_status will tell you the type of
     * error occurred. If not set, the option found does not exist.
     * If invalid, it is invalid. Otherwise, a parse error.
     *
     */
    using error_handler = std::function<void(const sstring&, const sstring&, std::optional<value_status>)>;

    void read_from_yaml(const sstring&, error_handler = {});
    void read_from_yaml(const char *, error_handler = {});
    future<> read_from_file(const sstring&, error_handler = {});
    future<> read_from_file(file, error_handler = {});

    using configs = std::vector<cfg_ref>;

    configs set_values() const;
    configs unset_values() const;
    const configs& values() const {
        return _cfgs;
    }
    future<> broadcast_to_all_shards();
private:
    virtual bool are_live_updatable_config_params_changeable_via_cql() const {
        return false;
    }

    configs
        _cfgs;
    bool _initialization_completed;
};

template <typename T>
requires requires (const config_file::named_value<T>& nv) {
    { nv().empty() } -> std::same_as<bool>;
}
const config_file::named_value<T>& operator||(const config_file::named_value<T>& a, const config_file::named_value<T>& b) {
    return !a().empty() ? a : b;
}

extern template struct config_file::named_value<seastar::log_level>;

// Explicit instantiation declarations for the most common named_value<T>
// specializations. The definitions are in db/config.cc (which is the only
// TU that includes config_file_impl.hh and therefore has the full template
// bodies). This avoids re-compiling the heavy boost::program_options /
// boost::lexical_cast / yaml-cpp machinery in every TU that includes
// config.hh.
extern template struct config_file::named_value<bool>;
extern template struct config_file::named_value<uint16_t>;
extern template struct config_file::named_value<uint32_t>;
extern template struct config_file::named_value<uint64_t>;
extern template struct config_file::named_value<int32_t>;
extern template struct config_file::named_value<int64_t>;
extern template struct config_file::named_value<float>;
extern template struct config_file::named_value<double>;
extern template struct config_file::named_value<sstring>;
extern template struct config_file::named_value<std::string>;
extern template struct config_file::named_value<config_file::string_map>;
extern template struct config_file::named_value<config_file::string_list>;

}

