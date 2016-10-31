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

#include "db/consistency_level.hh"

#include <boost/range/algorithm/stable_partition.hpp>
#include <boost/range/algorithm/find.hpp>
#include "exceptions/exceptions.hh"
#include "core/sstring.hh"
#include "schema.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "db/read_repair_decision.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "utils/fb_utilities.hh"

namespace db {

logging::logger cl_logger("consistency");

size_t quorum_for(keyspace& ks) {
    return (ks.get_replication_strategy().get_replication_factor() / 2) + 1;
}

size_t local_quorum_for(keyspace& ks, const sstring& dc) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);

        return (nrs->get_replication_factor(dc) / 2) + 1;
    }

    return quorum_for(ks);
}

size_t block_for_local_serial(keyspace& ks) {
    using namespace locator;

    //
    // TODO: Consider caching the final result in order to avoid all these
    //       useless dereferencing. Note however that this will introduce quite
    //       a lot of complications since both snitch output for a local host
    //       and the snitch itself (and thus its output) may change dynamically.
    //
    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();
    auto local_addr = utils::fb_utilities::get_broadcast_address();

    return local_quorum_for(ks, snitch_ptr->get_datacenter(local_addr));
}

size_t block_for_each_quorum(keyspace& ks) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);
        size_t n = 0;

        for (auto& dc : nrs->get_datacenters()) {
            n += local_quorum_for(ks, dc);
        }

        return n;
    } else {
        return quorum_for(ks);
    }
}

size_t block_for(keyspace& ks, consistency_level cl) {
    switch (cl) {
    case consistency_level::ONE:
    case consistency_level::LOCAL_ONE:
        return 1;
    case consistency_level::ANY:
        return 1;
    case consistency_level::TWO:
        return 2;
    case consistency_level::THREE:
        return 3;
    case consistency_level::QUORUM:
    case consistency_level::SERIAL:
        return quorum_for(ks);
    case consistency_level::ALL:
        return ks.get_replication_strategy().get_replication_factor();
    case consistency_level::LOCAL_QUORUM:
    case consistency_level::LOCAL_SERIAL:
        return block_for_local_serial(ks);
    case consistency_level::EACH_QUORUM:
        return block_for_each_quorum(ks);
    default:
        abort();
    }
}

bool is_datacenter_local(consistency_level l) {
    return l == consistency_level::LOCAL_ONE || l == consistency_level::LOCAL_QUORUM;
}

bool is_local(gms::inet_address endpoint) {
    using namespace locator;

    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();
    auto local_addr = utils::fb_utilities::get_broadcast_address();

    return snitch_ptr->get_datacenter(local_addr) ==
           snitch_ptr->get_datacenter(endpoint);
}

std::vector<gms::inet_address>
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 std::vector<gms::inet_address> live_endpoints,
                 read_repair_decision read_repair) {
    size_t local_ep_count = live_endpoints.size();
    size_t bf;

    /*
     * Endpoints are expected to be restricted to live replicas, sorted by
     * snitch preference. For LOCAL_QUORUM, move local-DC replicas in front
     * first as we need them there whether we do read repair (since the first
     * replica gets the data read) or not (since we'll take the block_for first
     * ones).
     */
    if (is_datacenter_local(cl) || read_repair == read_repair_decision::DC_LOCAL) {
        auto it = boost::range::stable_partition(live_endpoints, is_local);
        local_ep_count = std::distance(live_endpoints.begin(), it);
    }

    switch (read_repair) {
    case read_repair_decision::NONE:
        bf = block_for(ks, cl);
        break;
    case read_repair_decision::DC_LOCAL:
        bf = std::max(block_for(ks, cl), local_ep_count);
        break;
    case read_repair_decision::GLOBAL:
        bf = live_endpoints.size();
        break;
    default:
        throw std::runtime_error("Unknown read repair type");
    }

    live_endpoints.erase(live_endpoints.begin() + std::min(live_endpoints.size(), bf), live_endpoints.end());

    return std::move(live_endpoints);
}

std::vector<gms::inet_address> filter_for_query(consistency_level cl, keyspace& ks, std::vector<gms::inet_address>& live_endpoints) {
    return filter_for_query(cl, ks, live_endpoints, read_repair_decision::NONE);
}

bool
is_sufficient_live_nodes(consistency_level cl,
                         keyspace& ks,
                         const std::vector<gms::inet_address>& live_endpoints) {
    using namespace locator;

    switch (cl) {
    case consistency_level::ANY:
        // local hint is acceptable, and local node is always live
        return true;
    case consistency_level::LOCAL_ONE:
        return count_local_endpoints(live_endpoints) >= 1;
    case consistency_level::LOCAL_QUORUM:
        return count_local_endpoints(live_endpoints) >= block_for(ks, cl);
    case consistency_level::EACH_QUORUM:
    {
        auto& rs = ks.get_replication_strategy();

        if (rs.get_type() == replication_strategy_type::network_topology) {
            for (auto& entry : count_per_dc_endpoints(ks, live_endpoints)) {
                if (entry.second.live < local_quorum_for(ks, entry.first)) {
                    return false;
                }
            }

            return true;
        }
    }
        // Fallthough on purpose for SimpleStrategy
    default:
        return live_endpoints.size() >= block_for(ks, cl);
    }
}

void validate_for_read(const sstring& keyspace_name, consistency_level cl) {
    switch (cl) {
        case consistency_level::ANY:
            throw exceptions::invalid_request_exception("ANY ConsistencyLevel is only supported for writes");
        case consistency_level::EACH_QUORUM:
            throw exceptions::invalid_request_exception("EACH_QUORUM ConsistencyLevel is only supported for writes");
        default:
            break;
    }
}

void validate_for_write(const sstring& keyspace_name, consistency_level cl) {
    switch (cl) {
        case consistency_level::SERIAL:
        case consistency_level::LOCAL_SERIAL:
            throw exceptions::invalid_request_exception("You must use conditional updates for serializable writes");
        default:
            break;
    }
}

#if 0
    // This is the same than validateForWrite really, but we include a slightly different error message for SERIAL/LOCAL_SERIAL
    public void validateForCasCommit(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case EACH_QUORUM:
                requireNetworkTopologyStrategy(keyspaceName);
                break;
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException(this + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
        }
    }

    public void validateForCas() throws InvalidRequestException
    {
        if (!isSerialConsistency())
            throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
    }
#endif

bool is_serial_consistency(consistency_level cl) {
    return cl == consistency_level::SERIAL || cl == consistency_level::LOCAL_SERIAL;
}

void validate_counter_for_write(schema_ptr s, consistency_level cl) {
    if (cl == consistency_level::ANY) {
        throw exceptions::invalid_request_exception(sprint("Consistency level ANY is not yet supported for counter table %s", s->cf_name()));
    }

    if (is_serial_consistency(cl)) {
        throw exceptions::invalid_request_exception("Counter operations are inherently non-serializable");
    }
}

#if 0
    private void requireNetworkTopologyStrategy(String keyspaceName) throws InvalidRequestException
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
        if (!(strategy instanceof NetworkTopologyStrategy))
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", this, strategy.getClass().getName()));
    }
#endif

}
