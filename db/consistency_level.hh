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

#include <boost/range/algorithm/partition.hpp>
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

#if 0
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.transport.ProtocolException;
#endif

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
    unavailable_exception(consistency_level cl, size_t required, size_t alive) :
        exceptions::cassandra_exception(exceptions::exception_code::UNAVAILABLE, sprint("Cannot achieve consistency level for cl %s. Requires %lu, alive %lu", cl, required, alive)) {}
};

#if 0
    private static final Logger logger = LoggerFactory.getLogger(ConsistencyLevel.class);

    // Used by the binary protocol
    public final int code;
    private final boolean isDCLocal;
    private static final ConsistencyLevel[] codeIdx;
    static
    {
        int maxCode = -1;
        for (ConsistencyLevel cl : ConsistencyLevel.values())
            maxCode = Math.max(maxCode, cl.code);
        codeIdx = new ConsistencyLevel[maxCode + 1];
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            if (codeIdx[cl.code] != null)
                throw new IllegalStateException("Duplicate code");
            codeIdx[cl.code] = cl;
        }
    }

    private ConsistencyLevel(int code)
    {
        this(code, false);
    }

    private ConsistencyLevel(int code, boolean isDCLocal)
    {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    public static ConsistencyLevel fromCode(int code)
    {
        if (code < 0 || code >= codeIdx.length)
            throw new ProtocolException(String.format("Unknown code %d for a consistency level", code));
        return codeIdx[code];
    }

#endif

inline size_t quorum_for(keyspace& ks) {
    return (ks.get_replication_strategy().get_replication_factor() / 2) + 1;
}

inline size_t local_quorum_for(keyspace& ks, const sstring& dc) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);

        return (nrs->get_replication_factor(dc) / 2) + 1;
    }

    return quorum_for(ks);
}

inline size_t block_for_local_serial(keyspace& ks) {
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

inline size_t block_for_each_quorum(keyspace& ks) {
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

inline size_t block_for(keyspace& ks, consistency_level cl) {
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

inline bool is_datacenter_local(consistency_level l) {
    return l == consistency_level::LOCAL_ONE || l == consistency_level::LOCAL_QUORUM;
}

inline bool is_local(gms::inet_address endpoint) {
    using namespace locator;

    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();
    auto local_addr = utils::fb_utilities::get_broadcast_address();

    return snitch_ptr->get_datacenter(local_addr) ==
           snitch_ptr->get_datacenter(endpoint);
}

template<typename Range>
inline size_t count_local_endpoints(Range& live_endpoints) {
    return std::count_if(live_endpoints.begin(), live_endpoints.end(), is_local);
}

inline std::vector<gms::inet_address>
filter_for_query_dc_local(consistency_level cl,
                          keyspace& ks,
                          const std::vector<gms::inet_address>& live_endpoints) {
    using namespace gms;

    std::vector<inet_address> local;
    std::vector<inet_address> other;
    local.reserve(live_endpoints.size());
    other.reserve(live_endpoints.size());

    std::partition_copy(live_endpoints.begin(), live_endpoints.end(),
                        std::back_inserter(local), std::back_inserter(other),
                        is_local);

    // check if blockfor more than we have localep's
    size_t bf = block_for(ks, cl);
    if (local.size() < bf) {
        size_t other_items_count = std::min(bf - local.size(), other.size());
        local.reserve(local.size() + other_items_count);

        std::move(other.begin(), other.begin() + other_items_count,
                  std::back_inserter(local));
    }

    return local;
}

inline std::vector<gms::inet_address>
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 std::vector<gms::inet_address> live_endpoints,
                 read_repair_decision read_repair) {
    /*
     * Endpoints are expected to be restricted to live replicas, sorted by
     * snitch preference. For LOCAL_QUORUM, move local-DC replicas in front
     * first as we need them there whether we do read repair (since the first
     * replica gets the data read) or not (since we'll take the block_for first
     * ones).
     */
    if (is_datacenter_local(cl)) {
        boost::range::partition(live_endpoints, is_local);
    }

    switch (read_repair) {
    case read_repair_decision::NONE:
    {
        size_t start_pos = std::min(live_endpoints.size(), block_for(ks, cl));

        live_endpoints.erase(live_endpoints.begin() + start_pos, live_endpoints.end());
    }
        // fall through
    case read_repair_decision::GLOBAL:
        return std::move(live_endpoints);
    case read_repair_decision::DC_LOCAL:
        return filter_for_query_dc_local(cl, ks, live_endpoints);
    default:
        throw std::runtime_error("Unknown read repair type");
    }
}

inline
std::vector<gms::inet_address> filter_for_query(consistency_level cl, keyspace& ks, std::vector<gms::inet_address>& live_endpoints) {
    return filter_for_query(cl, ks, live_endpoints, read_repair_decision::NONE);
}

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

inline bool
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
                if (entry.second < local_quorum_for(ks, entry.first)) {
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

static inline
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

static inline
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

static inline
bool is_serial_consistency(consistency_level cl) {
    return cl == consistency_level::SERIAL || cl == consistency_level::LOCAL_SERIAL;
}

static inline
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
