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
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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
#include "locator/snitch_base.hh"
#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_reason.hh"
#include "gms/inet_address.hh"
#include "range.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
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
    using token_metadata_ptr = locator::token_metadata_ptr;
    using stream_plan = streaming::stream_plan;
    using stream_state = streaming::stream_state;
public:
    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    class i_source_filter {
    public:
        virtual bool should_include(inet_address endpoint) = 0;
        virtual ~i_source_filter() {}
    };

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    class failure_detector_source_filter : public i_source_filter {
    private:
        std::set<gms::inet_address> _down_nodes;
    public:
        failure_detector_source_filter(std::set<gms::inet_address> down_nodes) : _down_nodes(std::move(down_nodes)) { }
        virtual bool should_include(inet_address endpoint) override { return !_down_nodes.contains(endpoint); }
    };

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    class single_datacenter_filter : public i_source_filter {
    private:
        sstring _source_dc;
    public:
        single_datacenter_filter(const sstring& source_dc)
            : _source_dc(source_dc) {
        }
        virtual bool should_include(inet_address endpoint) override {
            auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
            return snitch_ptr->get_datacenter(endpoint) == _source_dc;
        }
    };

    range_streamer(distributed<database>& db, const token_metadata_ptr tmptr, abort_source& abort_source, std::unordered_set<token> tokens, inet_address address, sstring description, streaming::stream_reason reason)
        : _db(db)
        , _token_metadata_ptr(std::move(tmptr))
        , _abort_source(abort_source)
        , _tokens(std::move(tokens))
        , _address(address)
        , _description(std::move(description))
        , _reason(reason)
        , _stream_plan(_description) {
        _abort_source.check();
    }

    range_streamer(distributed<database>& db, const token_metadata_ptr tmptr, abort_source& abort_source, inet_address address, sstring description, streaming::stream_reason reason)
        : range_streamer(db, std::move(tmptr), abort_source, std::unordered_set<token>(), address, description, reason) {
    }

    void add_source_filter(std::unique_ptr<i_source_filter> filter) {
        _source_filters.emplace(std::move(filter));
    }

    future<> add_ranges(const sstring& keyspace_name, dht::token_range_vector ranges);
    void add_tx_ranges(const sstring& keyspace_name, std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint);
    void add_rx_ranges(const sstring& keyspace_name, std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint);
private:
    bool use_strict_sources_for_ranges(const sstring& keyspace_name);
    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     */
    std::unordered_map<dht::token_range, std::vector<inet_address>>
    get_all_ranges_with_sources_for(const sstring& keyspace_name, dht::token_range_vector desired_ranges);
    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     */
    std::unordered_map<dht::token_range, std::vector<inet_address>>
    get_all_ranges_with_strict_sources_for(const sstring& keyspace_name, dht::token_range_vector desired_ranges);
private:
    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @return
     */
    std::unordered_map<inet_address, dht::token_range_vector>
    get_range_fetch_map(const std::unordered_map<dht::token_range, std::vector<inet_address>>& ranges_with_sources,
                        const std::unordered_set<std::unique_ptr<i_source_filter>>& source_filters,
                        const sstring& keyspace);

#if 0

    // For testing purposes
    Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }
#endif

    const token_metadata& get_token_metadata() {
        return *_token_metadata_ptr;
    }
public:
    future<> stream_async();
    future<> do_stream_async();
    size_t nr_ranges_to_stream();
private:
    distributed<database>& _db;
    const token_metadata_ptr _token_metadata_ptr;
    abort_source& _abort_source;
    std::unordered_set<token> _tokens;
    inet_address _address;
    sstring _description;
    streaming::stream_reason _reason;
    std::unordered_multimap<sstring, std::unordered_map<inet_address, dht::token_range_vector>> _to_stream;
    std::unordered_set<std::unique_ptr<i_source_filter>> _source_filters;
    stream_plan _stream_plan;
    // Retry the stream plan _nr_max_retry times
    unsigned _nr_retried = 0;
    unsigned _nr_max_retry = 5;
    // Number of tx and rx ranges added
    unsigned _nr_tx_added = 0;
    unsigned _nr_rx_added = 0;
    // Limit the number of nodes to stream in parallel to reduce memory pressure with large cluster.
    seastar::semaphore _limiter{16};
};

} // dht
