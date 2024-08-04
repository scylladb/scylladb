/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/assert.hh"
#include <unordered_map>
#include <unordered_set>

#include <seastar/core/sstring.hh>
#include "snitch_base.hh"

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

    explicit production_snitch_base(snitch_config);

    virtual sstring get_rack() const override;
    virtual sstring get_datacenter() const override;
    virtual void set_backreference(snitch_ptr& d) override;

private:
    virtual void set_my_dc_and_rack(const sstring& new_dc, const sstring& new_rack) override;
    virtual void set_prefer_local(bool prefer_local) override;
    void parse_property_file(std::string contents);

    virtual bool prefer_local() const noexcept override {
        return _prefer_local;
    }

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
    sstring _prop_file_name;
    std::unordered_map<sstring, sstring> _prop_values;

    sharded<snitch_ptr>& container() noexcept {
        SCYLLA_ASSERT(_backreference != nullptr);
        return _backreference->container();
    }

    snitch_ptr& local() noexcept {
        SCYLLA_ASSERT(_backreference != nullptr);
        return *_backreference;
    }

    const snitch_ptr& local() const noexcept {
        SCYLLA_ASSERT(_backreference != nullptr);
        return *_backreference;
    }

private:
    snitch_ptr* _backreference = nullptr;
protected:
    /*
     * @note Currently in order to be backward compatible we are mimicking the C*
     *       behavior, which is a bit strange: while allowing the change of
     *       prefer_local value during the same run it won't actually trigger
     *       disconnect from all remote nodes as would be logical (in order to
     *       connect using a new configuration). On the contrary, if the new
     *       prefer_local value is TRUE, it will trigger the reconnect only when
     *       there is a corresponding gossip event (e.g. on_change()) from the
     *       corresponding node has been accepted. If the new value is FALSE
     *       then it won't trigger disconnect at all! And in any case a remote
     *       node will be reconnected using the PREFERED_IP value stored in the
     *       system_table.peer.
     */

    bool _prefer_local = false;
};
} // namespace locator
