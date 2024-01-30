/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "timeout_config.hh"
#include "db/consistency_level_type.hh"
#include <seastar/core/sstring.hh>
#include <seastar/net/socket_defs.hh>
#include "schema/schema_fwd.hh"
#include "service/client_state.hh"
#include "auth/service.hh"

namespace service {
class storage_proxy;
}

namespace redis {

class redis_options {
    sstring _ks_name;
    const db::consistency_level _read_consistency;
    const db::consistency_level _write_consistency;
    const updateable_timeout_config& _timeout_config;
    service::client_state _client_state;
    size_t _total_redis_db_count;
public:
    explicit redis_options(const db::consistency_level rcl,
        const db::consistency_level wcl,
        const updateable_timeout_config& tc,
        auth::service& auth,
        const socket_address addr,
        size_t total_redis_db_count)
        :_ks_name("REDIS_0")
        ,_read_consistency(rcl)
        ,_write_consistency(wcl)
        ,_timeout_config(tc)
        ,_client_state(service::client_state::external_tag{}, auth, nullptr, tc.current_values(), addr)
        ,_total_redis_db_count(total_redis_db_count)
    {
    }
    explicit redis_options(const sstring& ks_name,
        const db::consistency_level rcl,
        const db::consistency_level wcl,
        const updateable_timeout_config& tc,
        auth::service& auth,
        const socket_address addr,
        size_t total_redis_db_count)
        :_ks_name(ks_name)
        ,_read_consistency(rcl)
        ,_write_consistency(wcl)
        ,_timeout_config(tc)
        ,_client_state(service::client_state::external_tag{}, auth, nullptr, tc.current_values(), addr)
        ,_total_redis_db_count(total_redis_db_count)
    {
    }

    db::consistency_level get_read_consistency_level() const { return _read_consistency; }
    db::consistency_level get_write_consistency_level() const { return _write_consistency; }

    const db::timeout_clock::duration get_read_timeout() const { return std::chrono::milliseconds(_timeout_config.read_timeout_in_ms); }
    const db::timeout_clock::duration get_write_timeout() const { return std::chrono::milliseconds(_timeout_config.write_timeout_in_ms); }
    const sstring& get_keyspace_name() const { return _ks_name; }
    service::client_state& get_client_state() { return _client_state; }

    void set_keyspace_name(const sstring ks_name) { _ks_name = ks_name; }
    size_t get_total_redis_db_count() const { return _total_redis_db_count; }
};

schema_ptr get_schema(service::storage_proxy& proxy, const sstring& ks_name, const sstring& cf_name);

}
