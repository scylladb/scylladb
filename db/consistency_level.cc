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

#include "db/consistency_level.hh"
#include "db/consistency_level_validations.hh"

#include <boost/range/algorithm/stable_partition.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/transform.hpp>
#include "exceptions/exceptions.hh"
#include <seastar/core/sstring.hh>
#include "schema.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "db/read_repair_decision.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "utils/fb_utilities.hh"
#include "heat_load_balance.hh"

namespace db {

logging::logger cl_logger("consistency");

size_t quorum_for(const keyspace& ks) {
    size_t replication_factor = ks.get_replication_strategy().get_replication_factor();
    return replication_factor ? (replication_factor / 2) + 1 : 0;
}

size_t local_quorum_for(const keyspace& ks, const sstring& dc) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        const network_topology_strategy* nrs =
            static_cast<const network_topology_strategy*>(&rs);
        size_t replication_factor = nrs->get_replication_factor(dc);
        return replication_factor ? (replication_factor / 2) + 1 : 0;
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


template <typename Range, typename PendingRange = std::array<gms::inet_address, 0>>
std::unordered_map<sstring, dc_node_count> count_per_dc_endpoints(
        keyspace& ks,
        const Range& live_endpoints,
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

template<typename Range, typename PendingRange>
bool assure_sufficient_live_nodes_each_quorum(
        consistency_level cl,
        keyspace& ks,
        const Range& live_endpoints,
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

template<typename Range, typename PendingRange>
void assure_sufficient_live_nodes(
        consistency_level cl,
        keyspace& ks,
        const Range& live_endpoints,
        const PendingRange& pending_endpoints) {
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

template void assure_sufficient_live_nodes(consistency_level, keyspace&, const inet_address_vector_replica_set&, const std::array<gms::inet_address, 0>&);
template void assure_sufficient_live_nodes(db::consistency_level, keyspace&, const std::unordered_set<gms::inet_address>&, const utils::small_vector<gms::inet_address, 1ul>&);

inet_address_vector_replica_set
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 inet_address_vector_replica_set live_endpoints,
                 const inet_address_vector_replica_set& preferred_endpoints,
                 read_repair_decision read_repair,
                 gms::inet_address* extra,
                 column_family* cf) {
    size_t local_count;

    if (read_repair == read_repair_decision::GLOBAL) { // take RRD.GLOBAL out of the way
        return std::move(live_endpoints);
    }

    if (read_repair == read_repair_decision::DC_LOCAL || is_datacenter_local(cl)) {
        auto it = boost::range::stable_partition(live_endpoints, is_local);
        local_count = std::distance(live_endpoints.begin(), it);
        if (is_datacenter_local(cl)) {
            live_endpoints.erase(it, live_endpoints.end());
        }
    }

    size_t bf = block_for(ks, cl);

    if (read_repair == read_repair_decision::DC_LOCAL) {
        bf = std::max(block_for(ks, cl), local_count);
    }

    if (bf >= live_endpoints.size()) { // RRD.DC_LOCAL + CL.LOCAL or CL.ALL
        return std::move(live_endpoints);
    }

    inet_address_vector_replica_set selected_endpoints;

    // Pre-select endpoints based on client preference. If the endpoints
    // selected this way aren't enough to satisfy CL requirements select the
    // remaining ones according to the load-balancing strategy as before.
    if (!preferred_endpoints.empty()) {
        const auto it = boost::stable_partition(live_endpoints, [&preferred_endpoints] (const gms::inet_address& a) {
            return std::find(preferred_endpoints.cbegin(), preferred_endpoints.cend(), a) == preferred_endpoints.end();
        });
        const size_t selected = std::distance(it, live_endpoints.end());
        if (selected >= bf) {
             if (extra) {
                 *extra = selected == bf ? live_endpoints.front() : *(it + bf);
             }
             return inet_address_vector_replica_set(it, it + bf);
        } else if (selected) {
             selected_endpoints.reserve(bf);
             std::move(it, live_endpoints.end(), std::back_inserter(selected_endpoints));
             live_endpoints.erase(it, live_endpoints.end());
        }
    }

    const auto remaining_bf = bf - selected_endpoints.size();

    if (cf) {
        auto get_hit_rate = [cf] (gms::inet_address ep) -> float {
            constexpr float max_hit_rate = 0.999;
            auto ht = cf->get_hit_rate(ep);
            if (float(ht.rate) < 0) {
                return float(ht.rate);
            } else if (lowres_clock::now() - ht.last_updated > std::chrono::milliseconds(1000)) {
                // if a cache entry is not updates for a while try to send traffic there
                // to get more up to date data, mark it updated to not send to much traffic there
                cf->set_hit_rate(ep, ht.rate);
                return max_hit_rate;
            } else {
                return std::min(float(ht.rate), max_hit_rate); // calculation below cannot work with hit rate 1
            }
        };

        float ht_max = 0;
        float ht_min = 1;
        bool old_node = false;

        auto epi = boost::copy_range<std::vector<std::pair<gms::inet_address, float>>>(live_endpoints | boost::adaptors::transformed([&] (gms::inet_address ep) {
            auto ht = get_hit_rate(ep);
            old_node = old_node || ht < 0;
            ht_max = std::max(ht_max, ht);
            ht_min = std::min(ht_min, ht);
            return std::make_pair(ep, ht);
        }));

        if (!old_node && ht_max - ht_min > 0.01) { // if there is old node or hit rates are close skip calculations
            // local node is always first if present (see storage_proxy::get_live_sorted_endpoints)
            unsigned local_idx = epi[0].first == utils::fb_utilities::get_broadcast_address() ? 0 : epi.size() + 1;
            live_endpoints = boost::copy_range<inet_address_vector_replica_set>(miss_equalizing_combination(epi, local_idx, remaining_bf, bool(extra)));
        }
    }

    if (extra) {
        *extra = live_endpoints[remaining_bf]; // extra replica for speculation
    }

    std::move(live_endpoints.begin(), live_endpoints.begin() + remaining_bf, std::back_inserter(selected_endpoints));

    return selected_endpoints;
}

inet_address_vector_replica_set filter_for_query(consistency_level cl,
        keyspace& ks,
        inet_address_vector_replica_set& live_endpoints,
        const inet_address_vector_replica_set& preferred_endpoints,
        column_family* cf) {
    return filter_for_query(cl, ks, live_endpoints, preferred_endpoints, read_repair_decision::NONE, nullptr, cf);
}

bool
is_sufficient_live_nodes(consistency_level cl,
                         keyspace& ks,
                         const inet_address_vector_replica_set& live_endpoints) {
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

void validate_for_read(consistency_level cl) {
    switch (cl) {
        case consistency_level::ANY:
            throw exceptions::invalid_request_exception("ANY ConsistencyLevel is only supported for writes");
        case consistency_level::EACH_QUORUM:
            throw exceptions::invalid_request_exception("EACH_QUORUM ConsistencyLevel is only supported for writes");
        default:
            break;
    }
}

void validate_for_write(consistency_level cl) {
    switch (cl) {
        case consistency_level::SERIAL:
        case consistency_level::LOCAL_SERIAL:
            throw exceptions::invalid_request_exception("You must use conditional updates for serializable writes");
        default:
            break;
    }
}

// This is the same than validateForWrite really, but we include a slightly different error message for SERIAL/LOCAL_SERIAL
void validate_for_cas_learn(consistency_level cl, const sstring& keyspace) {
    switch (cl) {
    case consistency_level::SERIAL:
    case consistency_level::LOCAL_SERIAL:
        throw exceptions::invalid_request_exception(format("{} is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"", cl));
    default:
        break;
    }
}

bool is_serial_consistency(consistency_level cl) {
    return cl == consistency_level::SERIAL || cl == consistency_level::LOCAL_SERIAL;
}

void validate_for_cas(consistency_level cl)
{
    if (!is_serial_consistency(cl)) {
        throw exceptions::invalid_request_exception("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
    }
}


void validate_counter_for_write(const schema& s, consistency_level cl) {
    if (cl == consistency_level::ANY) {
        throw exceptions::invalid_request_exception(format("Consistency level ANY is not yet supported for counter table {}", s.cf_name()));
    }

    if (is_serial_consistency(cl)) {
        throw exceptions::invalid_request_exception("Counter operations are inherently non-serializable");
    }
}

}
