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

#include <boost/range/algorithm/sort.hpp>
#include "exceptions/exceptions.hh"
#include "core/sstring.hh"
#include "schema.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "db/read_repair_decision.hh"

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

struct unavailable_exception : std::exception {
    consistency_level cl;
    size_t required;
    size_t alive;

    unavailable_exception(consistency_level cl_, size_t required_, size_t alive_) : cl(cl_), required(required_), alive(alive_) {}
    virtual const char* what() const noexcept {
        return "Cannot achieve consistency level";
    }
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
inline size_t quorum_for(keyspace& ks)
{
    return (ks.get_replication_strategy().get_replication_factor() / 2) + 1;
}

inline size_t local_quorum_for(keyspace& ks, const sstring& dc)
{
    fail(unimplemented::cause::CONSISTENCY);
    return 0;
#if 0
    return (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
            ? (((NetworkTopologyStrategy) keyspace.getReplicationStrategy()).getReplicationFactor(dc) / 2) + 1
                    : quorum_for(ks);
#endif
}

inline size_t block_for(keyspace& ks, consistency_level cl)
{
    switch (cl)
    {
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
        return local_quorum_for(ks, "localDC"/*DatabaseDescriptor.getLocalDataCenter()*/);
    case consistency_level::EACH_QUORUM:
        assert(false);
        return 0;
#if 0
        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
            int n = 0;
            for (String dc : strategy.getDatacenters())
                n += localQuorumFor(keyspace, dc);
            return n;
        }
        else
        {
            return quorum_for(ks);
        }
#endif
    default:
        abort();
    }
}

static inline
bool is_datacenter_local(consistency_level l)
{
    return l == consistency_level::LOCAL_ONE || l == consistency_level::LOCAL_QUORUM;
}

inline
bool is_local(gms::inet_address endpoint)
{
    return true;
 //       return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint));
}

template<typename Range>
inline
size_t count_local_endpoints(Range& live_endpoints)
{
    return std::count_if(live_endpoints.begin(), live_endpoints.end(), is_local);
}

 #if 0
    private Map<String, Integer> countPerDCEndpoints(Keyspace keyspace, Iterable<InetAddress> liveEndpoints)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        Map<String, Integer> dcEndpoints = new HashMap<String, Integer>();
        for (String dc: strategy.getDatacenters())
            dcEndpoints.put(dc, 0);

        for (InetAddress endpoint : liveEndpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            dcEndpoints.put(dc, dcEndpoints.get(dc) + 1);
        }
        return dcEndpoints;
    }
#endif

inline
std::vector<gms::inet_address> filter_for_query(consistency_level cl, keyspace& ks, std::vector<gms::inet_address> live_endpoints, read_repair_decision read_repair) {
    /*
     * Endpoints are expected to be restricted to live replicas, sorted by snitch preference.
     * For LOCAL_QUORUM, move local-DC replicas in front first as we need them there whether
     * we do read repair (since the first replica gets the data read) or not (since we'll take
     * the blockFor first ones).
     */
    if (is_datacenter_local(cl)) {
        boost::range::sort(live_endpoints, [] (gms::inet_address&, gms::inet_address&) { return 0; }/*, DatabaseDescriptor.getLocalComparator()*/);
    }

    switch (read_repair)
    {
    case read_repair_decision::NONE:
        live_endpoints.erase(live_endpoints.begin() + std::min(live_endpoints.size(), block_for(ks, cl)), live_endpoints.end());
        // fall through
    case read_repair_decision::GLOBAL:
        return std::move(live_endpoints);
    case read_repair_decision::DC_LOCAL:
        throw std::runtime_error("DC local read repair is not implemented yet");
#if 0
        List<InetAddress> local = new ArrayList<InetAddress>();
        List<InetAddress> other = new ArrayList<InetAddress>();
        for (InetAddress add : liveEndpoints)
        {
            if (isLocal(add))
                local.add(add);
            else
                other.add(add);
        }
        // check if blockfor more than we have localep's
        int blockFor = blockFor(keyspace);
        if (local.size() < blockFor)
            local.addAll(other.subList(0, Math.min(blockFor - local.size(), other.size())));
        return local;
#endif
    default:
        throw std::runtime_error("Unknown read repair type");
    }
}

inline
std::vector<gms::inet_address> filter_for_query(consistency_level cl, keyspace& ks, std::vector<gms::inet_address>& live_endpoints) {
    return filter_for_query(cl, ks, live_endpoints, read_repair_decision::NONE);
}


#if 0
    public boolean isSufficientLiveNodes(Keyspace keyspace, Iterable<InetAddress> liveEndpoints)
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_ONE:
                return countLocalEndpoints(liveEndpoints) >= 1;
            case LOCAL_QUORUM:
                return countLocalEndpoints(liveEndpoints) >= blockFor(keyspace);
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    for (Map.Entry<String, Integer> entry : countPerDCEndpoints(keyspace, liveEndpoints).entrySet())
                    {
                        if (entry.getValue() < localQuorumFor(keyspace, entry.getKey()))
                            return false;
                    }
                    return true;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                return Iterables.size(liveEndpoints) >= blockFor(keyspace);
        }
    }
#endif

template<typename Range>
static inline
void assure_sufficient_live_nodes(consistency_level cl, keyspace& ks, Range& live_endpoints)
{
    size_t need = block_for(ks, cl);

    switch (cl)
    {
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
        warn(unimplemented::cause::CONSISTENCY);
#if 0
        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
        {
            for (Map.Entry<String, Integer> entry : countPerDCEndpoints(keyspace, liveEndpoints).entrySet())
            {
                int dcBlockFor = localQuorumFor(keyspace, entry.getKey());
                int dcLive = entry.getValue();
                if (dcLive < dcBlockFor)
                    throw new UnavailableException(this, dcBlockFor, dcLive);
            }
            break;
        }
#endif
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
