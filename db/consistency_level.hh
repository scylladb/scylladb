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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "locator/network_topology_strategy.hh"
#include "db/read_repair_decision.hh"
#include "exceptions/exceptions.hh"
#include "utils/fb_utilities.hh"
#include "gms/inet_address.hh"
#include "database.hh"

#include <iostream>
#include <vector>

namespace db {

enum class consistency_level {
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    SERIAL,
    LOCAL_SERIAL,
    LOCAL_ONE
};

std::ostream& operator<<(std::ostream& os, consistency_level cl);

struct unavailable_exception : exceptions::cassandra_exception {
    consistency_level consistency;
    int32_t required;
    int32_t alive;

    unavailable_exception(consistency_level cl, int32_t required, int32_t alive)
        : exceptions::cassandra_exception(exceptions::exception_code::UNAVAILABLE, sprint("Cannot achieve consistency level for cl %s. Requires %ld, alive %ld", cl, required, alive))
        , consistency(cl)
        , required(required)
        , alive(alive)
    {}
};

size_t quorum_for(keyspace& ks);

size_t local_quorum_for(keyspace& ks, const sstring& dc);

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
filter_for_query_dc_local(consistency_level cl,
                          keyspace& ks,
                          const std::vector<gms::inet_address>& live_endpoints);

std::vector<gms::inet_address>
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 std::vector<gms::inet_address> live_endpoints,
                 read_repair_decision read_repair);

std::vector<gms::inet_address> filter_for_query(consistency_level cl, keyspace& ks, std::vector<gms::inet_address>& live_endpoints);

template <typename Range>
inline std::unordered_map<sstring, size_t> count_per_dc_endpoints(
        keyspace& ks,
        Range& live_endpoints) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();
    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();

    network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);

    std::unordered_map<sstring, size_t> dc_endpoints;
    for (auto& dc : nrs->get_datacenters()) {
        dc_endpoints.emplace(dc, 0);
    }

    //
    // Since live_endpoints are a subset of a get_natural_endpoints() output we
    // will never get any endpoints outside the dataceters from
    // nrs->get_datacenters().
    //
    for (auto& endpoint : live_endpoints) {
        ++(dc_endpoints[snitch_ptr->get_datacenter(endpoint)]);
    }

    return dc_endpoints;
}

bool
is_sufficient_live_nodes(consistency_level cl,
                         keyspace& ks,
                         const std::vector<gms::inet_address>& live_endpoints);

template<typename Range>
inline bool assure_sufficient_live_nodes_each_quorum(
        consistency_level cl,
        keyspace& ks,
        Range& live_endpoints) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        for (auto& entry : count_per_dc_endpoints(ks, live_endpoints)) {
            auto dc_block_for = local_quorum_for(ks, entry.first);
            auto dc_live = entry.second;

            if (dc_live < dc_block_for) {
                throw unavailable_exception(cl, dc_block_for, dc_live);
            }
        }

        return true;
    }

    return false;
}

template<typename Range>
inline void assure_sufficient_live_nodes(
        consistency_level cl,
        keyspace& ks,
        Range& live_endpoints) {
    size_t need = block_for(ks, cl);

    switch (cl) {
    case consistency_level::ANY:
        // local hint is acceptable, and local node is always live
        break;
    case consistency_level::LOCAL_ONE:
        if (count_local_endpoints(live_endpoints) == 0) {
            throw unavailable_exception(cl, 1, 0);
        }
        break;
    case consistency_level::LOCAL_QUORUM: {
        size_t local_live = count_local_endpoints(live_endpoints);
        if (local_live < need) {
#if 0
            if (logger.isDebugEnabled())
            {
                StringBuilder builder = new StringBuilder("Local replicas [");
                for (InetAddress endpoint : liveEndpoints)
                {
                    if (isLocal(endpoint))
                        builder.append(endpoint).append(",");
                }
                builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockFor).append(" live nodes in '").append(DatabaseDescriptor.getLocalDataCenter()).append("'");
                logger.debug(builder.toString());
            }
#endif
            throw unavailable_exception(cl, need, local_live);
        }
        break;
    }
    case consistency_level::EACH_QUORUM:
        if (assure_sufficient_live_nodes_each_quorum(cl, ks, live_endpoints)) {
            break;
        }
// Fallthough on purpose for SimpleStrategy
    default:
        size_t live = live_endpoints.size();
        if (live < need) {
            //                    logger.debug("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(liveEndpoints), blockFor);
            throw unavailable_exception(cl, need, live);
        }
        break;
    }
}

void validate_for_read(const sstring& keyspace_name, consistency_level cl);

void validate_for_write(const sstring& keyspace_name, consistency_level cl);

bool is_serial_consistency(consistency_level cl);

void validate_counter_for_write(schema_ptr s, consistency_level cl);

}
