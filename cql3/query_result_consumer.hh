/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <seastar/core/sstring.hh>

#include "cql3/result_set.hh"
#include "transport/event.hh"

namespace cql3 {

class prepared_cache_key_type;

class query_result_consumer {
public:
    query_result_consumer() = default;
    query_result_consumer(const query_result_consumer&) = delete;
    virtual ~query_result_consumer() {}
    virtual void move_to_shard(unsigned shard_id) = 0;
    virtual void add_warning(const sstring& w) = 0;
    virtual void set_keyspace(const sstring& keyspace) = 0;
    virtual void set_result(cql3::result rs) = 0;
    virtual void set_schema_change(shared_ptr<cql_transport::event::schema_change>& sc) = 0;
};

}
