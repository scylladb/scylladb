/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_reason.hh"
#include "service/topology_guard.hh"
#include "gms/inet_address.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include <unordered_map>
#include <memory>

namespace replica {
class database;
}

namespace gms { class gossiper; }
namespace locator { class topology; }

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
        virtual bool should_include(const locator::topology&, locator::host_id endpoint) = 0;
        virtual ~i_source_filter() {}
    };

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    class failure_detector_source_filter : public i_source_filter {
    private:
        std::set<locator::host_id> _down_nodes;
    public:
        failure_detector_source_filter(std::set<locator::host_id> down_nodes) : _down_nodes(std::move(down_nodes)) { }
        virtual bool should_include(const locator::topology&, locator::host_id endpoint) override { return !_down_nodes.contains(endpoint); }
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
        virtual bool should_include(const locator::topology& topo, locator::host_id endpoint) override {
            return topo.get_datacenter(endpoint) == _source_dc;
        }
    };

    range_streamer(distributed<replica::database>& db, sharded<streaming::stream_manager>& sm, const token_metadata_ptr tmptr, abort_source& abort_source, std::unordered_set<token> tokens,
            locator::host_id address, locator::endpoint_dc_rack dr, sstring description, streaming::stream_reason reason,
            service::frozen_topology_guard topo_guard,
            std::vector<sstring> tables = {})
        : _db(db)
        , _stream_manager(sm)
        , _token_metadata_ptr(std::move(tmptr))
        , _abort_source(abort_source)
        , _tokens(std::move(tokens))
        , _address(address)
        , _dr(std::move(dr))
        , _description(std::move(description))
        , _reason(reason)
        , _tables(std::move(tables))
        , _topo_guard(topo_guard)
    {
        _abort_source.check();
    }

    range_streamer(distributed<replica::database>& db, sharded<streaming::stream_manager>& sm, const token_metadata_ptr tmptr, abort_source& abort_source,
            locator::host_id address, locator::endpoint_dc_rack dr, sstring description, streaming::stream_reason reason, service::frozen_topology_guard topo_guard, std::vector<sstring> tables = {})
        : range_streamer(db, sm, std::move(tmptr), abort_source, std::unordered_set<token>(), address, std::move(dr), description, reason, std::move(topo_guard), std::move(tables)) {
    }

    void add_source_filter(std::unique_ptr<i_source_filter> filter) {
        _source_filters.emplace(std::move(filter));
    }

    future<> add_ranges(const sstring& keyspace_name, locator::vnode_effective_replication_map_ptr erm, dht::token_range_vector ranges, gms::gossiper& gossiper, bool is_replacing);
    void add_tx_ranges(const sstring& keyspace_name, std::unordered_map<locator::host_id, dht::token_range_vector> ranges_per_endpoint);
    void add_rx_ranges(const sstring& keyspace_name, std::unordered_map<locator::host_id, dht::token_range_vector> ranges_per_endpoint);
private:
    bool use_strict_sources_for_ranges(const sstring& keyspace_name, const locator::vnode_effective_replication_map_ptr& erm);
    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     */
    std::unordered_map<dht::token_range, std::vector<locator::host_id>>
    get_all_ranges_with_sources_for(const sstring& keyspace_name, locator::vnode_effective_replication_map_ptr erm, dht::token_range_vector desired_ranges);
    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     */
    std::unordered_map<dht::token_range, std::vector<locator::host_id>>
    get_all_ranges_with_strict_sources_for(const sstring& keyspace_name, locator::vnode_effective_replication_map_ptr erm, dht::token_range_vector desired_ranges, gms::gossiper& gossiper);
private:
    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @return
     */
    std::unordered_map<locator::host_id, dht::token_range_vector>
    get_range_fetch_map(const std::unordered_map<dht::token_range, std::vector<locator::host_id>>& ranges_with_sources,
                        const std::unordered_set<std::unique_ptr<i_source_filter>>& source_filters,
                        const sstring& keyspace);

#if 0

    // For testing purposes
    Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }
#endif

    // Can be called only before stream_async().
    const token_metadata& get_token_metadata() {
        return *_token_metadata_ptr;
    }
public:
    future<> stream_async();
    size_t nr_ranges_to_stream();
private:
    distributed<replica::database>& _db;
    sharded<streaming::stream_manager>& _stream_manager;
    token_metadata_ptr _token_metadata_ptr;
    abort_source& _abort_source;
    std::unordered_set<token> _tokens;
    locator::host_id _address;
    locator::endpoint_dc_rack _dr;
    sstring _description;
    streaming::stream_reason _reason;
    std::vector<sstring> _tables;
    service::frozen_topology_guard _topo_guard;
    std::unordered_multimap<sstring, std::unordered_map<locator::host_id, dht::token_range_vector>> _to_stream;
    std::unordered_set<std::unique_ptr<i_source_filter>> _source_filters;
    // Number of tx and rx ranges added
    unsigned _nr_tx_added = 0;
    unsigned _nr_rx_added = 0;
    // Limit the number of nodes to stream in parallel to reduce memory pressure with large cluster.
    seastar::semaphore _limiter{16};
    size_t _nr_total_ranges = 0;
    size_t _nr_ranges_remaining = 0;
};

} // dht
