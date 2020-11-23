/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#pragma once

#include <stdexcept>
#include "timeout_config.hh"
#include "db/consistency_level_type.hh"
#include "seastar/core/sstring.hh"
#include "seastar/net/socket_defs.hh"
#include "schema_fwd.hh"
#include "service/client_state.hh"
#include "auth/service.hh"

using namespace seastar;

namespace service {
class storage_proxy;
}

namespace redis {

class redis_options {
    sstring _ks_name;
    const db::consistency_level _read_consistency;
    const db::consistency_level _write_consistency;
    const timeout_config& _timeout_config;
    service::client_state _client_state;
    size_t _total_redis_db_count;
public:
    //redis_options(redis_options&&) = default;
    
    //explicit redis_options(const redis_options&) = default;
   
    explicit redis_options(const db::consistency_level rcl,
        const db::consistency_level wcl,
        const timeout_config& tc,
        auth::service& auth,
        const socket_address addr,
        size_t total_redis_db_count)
        :_ks_name("REDIS_0")
        ,_read_consistency(rcl)
        ,_write_consistency(wcl)
        ,_timeout_config(tc)
        ,_client_state(service::client_state::external_tag{}, auth, tc, addr)
        ,_total_redis_db_count(total_redis_db_count)
    {
    }
    explicit redis_options(const sstring& ks_name,
        const db::consistency_level rcl,
        const db::consistency_level wcl,
        const timeout_config& tc,
        auth::service& auth,
        const socket_address addr,
        size_t total_redis_db_count)
        :_ks_name(ks_name)
        ,_read_consistency(rcl)
        ,_write_consistency(wcl)
        ,_timeout_config(tc)
        ,_client_state(service::client_state::external_tag{}, auth, tc, addr)
        ,_total_redis_db_count(total_redis_db_count)
    {
    }

    const db::consistency_level get_read_consistency_level() const { return _read_consistency; }
    const db::consistency_level get_write_consistency_level() const { return _write_consistency; }

    const timeout_config& get_timeout_config() const { return _timeout_config; }
    const db::timeout_clock::duration get_read_timeout() const { return _timeout_config.read_timeout; }
    const db::timeout_clock::duration get_write_timeout() const { return _timeout_config.write_timeout; }
    const sstring& get_keyspace_name() const { return _ks_name; }
    service::client_state& get_client_state() { return _client_state; }

    void set_keyspace_name(const sstring ks_name) { _ks_name = ks_name; }
    size_t get_total_redis_db_count() const { return _total_redis_db_count; }
};

schema_ptr get_schema(service::storage_proxy& proxy, const sstring& ks_name, const sstring& cf_name);

}
