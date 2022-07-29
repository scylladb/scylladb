/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_map>
#include <utility>
#include <optional>
#include <unordered_set>

#include "gms/endpoint_state.hh"
#include "locator/token_metadata.hh"
#include <seastar/core/sstring.hh>
#include "snitch_base.hh"

namespace gms {
class gossiper;
}

namespace locator {

class bad_property_file_error : public std::exception {};

class production_snitch_base : public snitch_base {
public:
    // map of inet address to (datacenter, rack) pair
    typedef std::unordered_map<inet_address, endpoint_dc_rack> addr2dc_rack_map;

    static constexpr const char* default_dc   = "UNKNOWN_DC";
    static constexpr const char* default_rack = "UNKNOWN_RACK";
    static constexpr const char* snitch_properties_filename = "cassandra-rackdc.properties";

    // only these property values are supported
    static constexpr const char* dc_property_key           = "dc";
    static constexpr const char* rack_property_key         = "rack";
    static constexpr const char* prefer_local_property_key = "prefer_local";
    static constexpr const char* dc_suffix_property_key    = "dc_suffix";
    const std::unordered_set<sstring> allowed_property_keys;

    explicit production_snitch_base(snitch_config, gms::gossiper&);

    virtual sstring get_rack(inet_address endpoint) override;
    virtual sstring get_datacenter(inet_address endpoint) override;
    virtual void set_backreference(snitch_ptr& d) override;

private:
    std::optional<sstring> get_endpoint_info(inet_address endpoint, gms::application_state key);
    sstring get_endpoint_info(inet_address endpoint, gms::application_state key,
                              const sstring& default_val);
    virtual void set_my_dc_and_rack(const sstring& new_dc, const sstring& new_rack) override;
    virtual void set_prefer_local(bool prefer_local) override;
    void parse_property_file();

protected:
    /**
     * Loads the contents of the property file into the map
     *
     * @return ready future when the file contents has been loaded.
     */
    future<> load_property_file();

    [[noreturn]]
    void throw_double_declaration(const sstring& key) const;

    [[noreturn]]
    void throw_bad_format(const sstring& line) const;

    [[noreturn]]
    void throw_incomplete_file() const;

protected:
    gms::gossiper& _gossiper;

    std::optional<addr2dc_rack_map> _saved_endpoints;
    std::string _prop_file_contents;
    sstring _prop_file_name;
    std::unordered_map<sstring, sstring> _prop_values;

    sharded<snitch_ptr>& container() noexcept {
        assert(_backreference != nullptr);
        return _backreference->container();
    }

    snitch_ptr& local() noexcept {
        assert(_backreference != nullptr);
        return *_backreference;
    }

    const snitch_ptr& local() const noexcept {
        assert(_backreference != nullptr);
        return *_backreference;
    }

private:
    size_t _prop_file_size;
    snitch_ptr* _backreference = nullptr;
};
} // namespace locator
