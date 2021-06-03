/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "db/consistency_level_type.hh"
#include "db/read_repair_decision.hh"
#include "exceptions/exceptions.hh"
#include "utils/fb_utilities.hh"
#include "gms/inet_address.hh"
#include "inet_address_vectors.hh"
#include "log.hh"
#include "database_fwd.hh"

#include <iosfwd>
#include <vector>
#include <unordered_set>

namespace db {

extern logging::logger cl_logger;

size_t quorum_for(const keyspace& ks);

size_t local_quorum_for(const keyspace& ks, const sstring& dc);

size_t block_for_local_serial(keyspace& ks);

size_t block_for_each_quorum(keyspace& ks);

size_t block_for(keyspace& ks, consistency_level cl);

bool is_datacenter_local(consistency_level l);

bool is_local(gms::inet_address endpoint);

template<typename Range>
inline size_t count_local_endpoints(const Range& live_endpoints) {
    return std::count_if(live_endpoints.begin(), live_endpoints.end(), is_local);
}

inet_address_vector_replica_set
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 inet_address_vector_replica_set live_endpoints,
                 const inet_address_vector_replica_set& preferred_endpoints,
                 read_repair_decision read_repair,
                 gms::inet_address* extra,
                 column_family* cf);

inet_address_vector_replica_set filter_for_query(consistency_level cl,
        keyspace& ks,
        inet_address_vector_replica_set& live_endpoints,
        const inet_address_vector_replica_set& preferred_endpoints,
        column_family* cf);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

bool
is_sufficient_live_nodes(consistency_level cl,
                         keyspace& ks,
                         const inet_address_vector_replica_set& live_endpoints);

template<typename Range, typename PendingRange = std::array<gms::inet_address, 0>>
void assure_sufficient_live_nodes(
        consistency_level cl,
        keyspace& ks,
        const Range& live_endpoints,
        const PendingRange& pending_endpoints = std::array<gms::inet_address, 0>());

extern template void assure_sufficient_live_nodes(consistency_level, keyspace&, const inet_address_vector_replica_set&, const std::array<gms::inet_address, 0>&);
extern template void assure_sufficient_live_nodes(db::consistency_level, keyspace&, const std::unordered_set<gms::inet_address>&, const utils::small_vector<gms::inet_address, 1ul>&);

}
