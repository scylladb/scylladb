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
 * Copyright (C) 2015 ScyllaDB
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

#include "locator/network_topology_strategy.hh"
#include "db/consistency_level_type.hh"
#include "db/read_repair_decision.hh"
#include "exceptions/exceptions.hh"
#include "utils/fb_utilities.hh"
#include "gms/inet_address.hh"
#include "database.hh"

#include <iosfwd>
#include <vector>

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
inline size_t count_local_endpoints(Range& live_endpoints) {
    return std::count_if(live_endpoints.begin(), live_endpoints.end(), is_local);
}

std::vector<gms::inet_address>
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 std::vector<gms::inet_address> live_endpoints,
                 const std::vector<gms::inet_address>& preferred_endpoints,
                 read_repair_decision read_repair,
                 gms::inet_address* extra,
                 column_family* cf);

std::vector<gms::inet_address> filter_for_query(consistency_level cl,
        keyspace& ks,
        std::vector<gms::inet_address>& live_endpoints,
        const std::vector<gms::inet_address>& preferred_endpoints,
        column_family* cf);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

template <typename Range, typename PendingRange = std::array<gms::inet_address, 0>>
inline std::unordered_map<sstring, dc_node_count> count_per_dc_endpoints(
        keyspace& ks,
        Range& live_endpoints,
        const PendingRange& pending_endpoints = std::array<gms::inet_address, 0>()) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();
    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();

    network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);

    std::unordered_map<sstring, dc_node_count> dc_endpoints;
    for (auto& dc : nrs->get_datacenters()) {
        dc_endpoints.emplace(dc, dc_node_count());
    }

    //
    // Since live_endpoints are a subset of a get_natural_endpoints() output we
    // will never get any endpoints outside the dataceters from
    // nrs->get_datacenters().
    //
    for (auto& endpoint : live_endpoints) {
        ++(dc_endpoints[snitch_ptr->get_datacenter(endpoint)].live);
    }

    for (auto& endpoint : pending_endpoints) {
        ++(dc_endpoints[snitch_ptr->get_datacenter(endpoint)].pending);
    }

    return dc_endpoints;
}

bool
is_sufficient_live_nodes(consistency_level cl,
                         keyspace& ks,
                         const std::vector<gms::inet_address>& live_endpoints);

template<typename Range, typename PendingRange>
inline bool assure_sufficient_live_nodes_each_quorum(
        consistency_level cl,
        keyspace& ks,
        Range& live_endpoints,
        const PendingRange& pending_endpoints) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        for (auto& entry : count_per_dc_endpoints(ks, live_endpoints, pending_endpoints)) {
            auto dc_block_for = local_quorum_for(ks, entry.first);
            auto dc_live = entry.second.live;
            auto dc_pending = entry.second.pending;

            if (dc_live < dc_block_for + dc_pending) {
                throw exceptions::unavailable_exception(cl, dc_block_for, dc_live);
            }
        }

        return true;
    }

    return false;
}

template<typename Range, typename PendingRange = std::array<gms::inet_address, 0>>
inline void assure_sufficient_live_nodes(
        consistency_level cl,
        keyspace& ks,
        Range& live_endpoints,
        const PendingRange& pending_endpoints = std::array<gms::inet_address, 0>()) {
    size_t need = block_for(ks, cl);

    auto adjust_live_for_error = [] (size_t live, size_t pending) {
        // DowngradingConsistencyRetryPolicy uses alive replicas count from Unavailable
        // exception to adjust CL for retry. When pending node is present CL is increased
        // by 1 internally, so reported number of live nodes has to be adjusted to take
        // this into account
        return pending <= live ? live - pending : 0;
    };

    switch (cl) {
    case consistency_level::ANY:
        // local hint is acceptable, and local node is always live
        break;
    case consistency_level::LOCAL_ONE:
        if (count_local_endpoints(live_endpoints) < count_local_endpoints(pending_endpoints) + 1) {
            throw exceptions::unavailable_exception(cl, 1, 0);
        }
        break;
    case consistency_level::LOCAL_QUORUM: {
        size_t local_live = count_local_endpoints(live_endpoints);
        size_t pending = count_local_endpoints(pending_endpoints);
        if (local_live < need + pending) {
            cl_logger.debug("Local replicas {} are insufficient to satisfy LOCAL_QUORUM requirement of needed {} and pending {}", live_endpoints, local_live, pending);
            throw exceptions::unavailable_exception(cl, need, adjust_live_for_error(local_live, pending));
        }
        break;
    }
    case consistency_level::EACH_QUORUM:
        if (assure_sufficient_live_nodes_each_quorum(cl, ks, live_endpoints, pending_endpoints)) {
            break;
        }
    // Fallthough on purpose for SimpleStrategy
    default:
        size_t live = live_endpoints.size();
        size_t pending = pending_endpoints.size();
        if (live < need + pending) {
            cl_logger.debug("Live nodes {} do not satisfy ConsistencyLevel ({} required, {} pending)", live, need, pending);
            throw exceptions::unavailable_exception(cl, need, adjust_live_for_error(live, pending));
        }
        break;
    }
}

void validate_for_read(const sstring& keyspace_name, consistency_level cl);

void validate_for_write(const sstring& keyspace_name, consistency_level cl);

bool is_serial_consistency(consistency_level cl);

void validate_counter_for_write(schema_ptr s, consistency_level cl);

}
