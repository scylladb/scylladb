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
 *
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
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

#include "locator/token_metadata.hh"
#include "streaming/stream_plan.hh"
#include "gms/inet_address.hh"
#include "gms/i_failure_detector.hh"
#include "range.hh"
#include <seastar/core/distributed.hh>
#include <unordered_map>
#include <memory>

class database;

namespace dht {
/**
 * Assists in streaming ranges to a node.
 */
class range_streamer {
public:
    using inet_address = gms::inet_address;
    using token_metadata = locator::token_metadata;
    using stream_plan = streaming::stream_plan;
    using i_failure_detector = gms::i_failure_detector;
    static bool use_strict_consistency() {
        //FIXME: Boolean.parseBoolean(System.getProperty("cassandra.consistent.rangemovement","true"));
        return true;
    }
public:
    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    class i_source_filter {
    public:
        virtual bool should_include(inet_address endpoint) = 0;
    };

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    class failure_detector_source_filter : public i_source_filter {
    private:
        gms::i_failure_detector& _fd;
    public:
        failure_detector_source_filter(i_failure_detector& fd) : _fd(fd) { }
        virtual bool should_include(inet_address endpoint) override { return _fd.is_alive(endpoint); }
    };

#if 0
    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements ISourceFilter
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean shouldInclude(InetAddress endpoint)
        {
            return snitch.getDatacenter(endpoint).equals(sourceDc);
        }
    }
#endif

    range_streamer(distributed<database>& db, token_metadata& tm, std::unordered_set<token> tokens, inet_address address, sstring description)
        : _db(db)
        , _metadata(tm)
        , _tokens(std::move(tokens))
        , _address(address)
        , _description(std::move(description))
        , _stream_plan(_description, true) {
    }

    range_streamer(distributed<database>& db, token_metadata& tm, inet_address address, sstring description)
        : range_streamer(db, tm, std::unordered_set<token>(), address, description) {
    }

#if 0
    public void addSourceFilter(ISourceFilter filter)
    {
        sourceFilters.add(filter);
    }

    public void addRanges(String keyspaceName, Collection<Range<Token>> ranges)
    {
        Multimap<Range<Token>, InetAddress> rangesForKeyspace = useStrictSourcesForRanges(keyspaceName)
                ? getAllRangesWithStrictSourcesFor(keyspaceName, ranges) : getAllRangesWithSourcesFor(keyspaceName, ranges);

        if (logger.isDebugEnabled())
        {
            for (Map.Entry<Range<Token>, InetAddress> entry : rangesForKeyspace.entries())
                logger.debug(String.format("%s: range %s exists on %s", description, entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : getRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName).asMap().entrySet())
        {
            if (logger.isDebugEnabled())
            {
                for (Range<Token> r : entry.getValue())
                    logger.debug(String.format("%s: range %s from source %s for keyspace %s", description, r, entry.getKey(), keyspaceName));
            }
            toFetch.put(keyspaceName, entry);
        }
    }

    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return !DatabaseDescriptor.isReplacing()
                && useStrictConsistency
                && tokens != null
                && metadata.getAllEndpoints().size() != strat.getReplicationFactor();
    }
#endif
private:
    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     */
    std::unordered_multimap<range<token>, inet_address>
    get_all_ranges_with_sources_for(const sstring& keyspace_name, std::vector<range<token>> desired_ranges);
#if 0
    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     */
    private Multimap<Range<Token>, InetAddress> getAllRangesWithStrictSourcesFor(String table, Collection<Range<Token>> desiredRanges)
    {

        assert tokens != null;
        AbstractReplicationStrategy strat = Keyspace.open(table).getReplicationStrategy();

        //Active ranges
        TokenMetadata metadataClone = metadata.cloneOnlyTokenMap();
        Multimap<Range<Token>,InetAddress> addressRanges = strat.getRangeAddresses(metadataClone);

        //Pending ranges
        metadataClone.updateNormalTokens(tokens, address);
        Multimap<Range<Token>,InetAddress> pendingRangeAddresses = strat.getRangeAddresses(metadataClone);

        //Collects the source that will have its range moved to the new node
        Multimap<Range<Token>, InetAddress> rangeSources = ArrayListMultimap.create();

        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Map.Entry<Range<Token>, Collection<InetAddress>> preEntry : addressRanges.asMap().entrySet())
            {
                if (preEntry.getKey().contains(desiredRange))
                {
                    Set<InetAddress> oldEndpoints = Sets.newHashSet(preEntry.getValue());
                    Set<InetAddress> newEndpoints = Sets.newHashSet(pendingRangeAddresses.get(desiredRange));

                    //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                    //So we need to be careful to only be strict when endpoints == RF
                    if (oldEndpoints.size() == strat.getReplicationFactor())
                    {
                        oldEndpoints.removeAll(newEndpoints);
                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                    }

                    rangeSources.put(desiredRange, oldEndpoints.iterator().next());
                }
            }

            //Validate
            Collection<InetAddress> addressList = rangeSources.get(desiredRange);
            if (addressList == null || addressList.isEmpty())
                throw new IllegalStateException("No sources found for " + desiredRange);

            if (addressList.size() > 1)
                throw new IllegalStateException("Multiple endpoints found for " + desiredRange);

            InetAddress sourceIp = addressList.iterator().next();
            EndpointState sourceState = Gossiper.instance.getEndpointStateForEndpoint(sourceIp);
            if (Gossiper.instance.isEnabled() && (sourceState == null || !sourceState.isAlive()))
                throw new RuntimeException("A node required to move the data consistently is down ("+sourceIp+").  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
        }

        return rangeSources;
    }
#endif
private:
    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @return
     */
    static std::unordered_multimap<inet_address, range<token>>
    get_range_fetch_map(const std::unordered_multimap<range<token>, inet_address>& ranges_with_sources,
                        const std::unordered_set<std::unique_ptr<i_source_filter>>& source_filters,
                        const sstring& keyspace);

#if 0
    public static Multimap<InetAddress, Range<Token>> getWorkMap(Multimap<Range<Token>, InetAddress> rangesWithSourceTarget, String keyspace)
    {
        return getRangeFetchMap(rangesWithSourceTarget, Collections.<ISourceFilter>singleton(new FailureDetectorSourceFilter(FailureDetector.instance)), keyspace);
    }

    // For testing purposes
    Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        for (Map.Entry<String, Map.Entry<InetAddress, Collection<Range<Token>>>> entry : toFetch.entries())
        {
            String keyspace = entry.getKey();
            InetAddress source = entry.getValue().getKey();
            InetAddress preferred = SystemKeyspace.getPreferredIP(source);
            Collection<Range<Token>> ranges = entry.getValue().getValue();
            /* Send messages to respective folks to stream data over to me */
            if (logger.isDebugEnabled())
                logger.debug("{}ing from {} ranges {}", description, source, StringUtils.join(ranges, ", "));
            streamPlan.requestRanges(source, preferred, keyspace, ranges);
        }

        return streamPlan.execute();
    }
#endif
private:
    distributed<database>& _db;
    token_metadata& _metadata;
    std::unordered_set<token> _tokens;
    inet_address _address;
    sstring _description;
    std::unordered_multimap<sstring, std::unordered_map<inet_address, std::vector<range<token>>>> _to_fetch;
    std::unordered_set<std::unique_ptr<i_source_filter>> _source_filters;
    stream_plan _stream_plan;
};

} // dht
