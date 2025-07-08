/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "locator/util.hh"
#include "replica/database.hh"
#include "gms/gossiper.hh"
#include "utils/chunked_vector.hh"
#include <seastar/coroutine/maybe_yield.hh>

namespace locator {

static future<std::unordered_map<dht::token_range, host_id_vector_replica_set>>
construct_range_to_endpoint_map(
        locator::effective_replication_map_ptr erm,
        const dht::token_range_vector& ranges) {
    std::unordered_map<dht::token_range, host_id_vector_replica_set> res;
    res.reserve(ranges.size());
    for (auto r : ranges) {
        res[r] = erm->get_natural_replicas(r.start()->value(), true);
        co_await coroutine::maybe_yield();
    }
    co_return res;
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
static future<dht::token_range_vector>
get_all_ranges(const utils::chunked_vector<token>& sorted_tokens) {
    if (sorted_tokens.empty())
        co_return dht::token_range_vector();
    int size = sorted_tokens.size();
    dht::token_range_vector ranges;
    ranges.reserve(size);
    for (int i = 1; i < size; ++i) {
        dht::token_range r(wrapping_interval<token>::bound(sorted_tokens[i - 1], true), wrapping_interval<token>::bound(sorted_tokens[i], false));
        ranges.push_back(r);
        co_await coroutine::maybe_yield();
    }
    // Add the wrapping range
    ranges.emplace_back(wrapping_interval<token>::bound(sorted_tokens[size - 1], true), wrapping_interval<token>::bound(sorted_tokens[0], false));
    co_await coroutine::maybe_yield();

    co_return ranges;
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
future<std::unordered_map<dht::token_range, host_id_vector_replica_set>>
get_range_to_address_map(locator::effective_replication_map_ptr erm,
        const utils::chunked_vector<token>& sorted_tokens) {
    co_return co_await construct_range_to_endpoint_map(erm, co_await get_all_ranges(sorted_tokens));
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
static future<utils::chunked_vector<token>>
get_tokens_in_local_dc(const locator::token_metadata& tm) {
    utils::chunked_vector<token> filtered_tokens;
    auto local_dc_filter = tm.get_topology().get_local_dc_filter();
    for (auto token : tm.sorted_tokens()) {
        auto endpoint = tm.get_endpoint(token);
        if (local_dc_filter(*endpoint))
            filtered_tokens.push_back(token);
        co_await coroutine::maybe_yield();
    }
    co_return filtered_tokens;
}

static future<std::unordered_map<dht::token_range, host_id_vector_replica_set>>
get_range_to_address_map_in_local_dc(
        locator::effective_replication_map_ptr erm) {
    auto tmptr = erm->get_token_metadata_ptr();
    auto orig_map = co_await get_range_to_address_map(erm, co_await get_tokens_in_local_dc(*tmptr));
    std::unordered_map<dht::token_range, host_id_vector_replica_set> filtered_map;
    filtered_map.reserve(orig_map.size());
    auto local_dc_filter = tmptr->get_topology().get_local_dc_filter();
    for (auto entry : orig_map) {
        auto& addresses = filtered_map[entry.first];
        addresses.reserve(entry.second.size());
        std::copy_if(entry.second.begin(), entry.second.end(), std::back_inserter(addresses), std::cref(local_dc_filter));
        co_await coroutine::maybe_yield();
    }

    co_return filtered_map;
}

// static future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
// get_range_to_address_map(const replica::database& db, const sstring& keyspace) {
//     return get_range_to_address_map(db.find_keyspace(keyspace).get_static_effective_replication_map());
// }

future<std::unordered_map<dht::token_range, host_id_vector_replica_set>>
get_range_to_address_map(locator::effective_replication_map_ptr erm) {
    return get_range_to_address_map(erm, erm->get_token_metadata_ptr()->sorted_tokens());
}

future<utils::chunked_vector<dht::token_range_endpoints>>
describe_ring(const replica::database& db, const gms::gossiper& gossiper, const sstring& keyspace, bool include_only_local_dc) {
    utils::chunked_vector<dht::token_range_endpoints> ranges;
    std::unordered_map<host_id, describe_ring_endpoint_info> host_infos;

    auto erm = db.find_keyspace(keyspace).get_static_effective_replication_map();
    std::unordered_map<dht::token_range, host_id_vector_replica_set> range_to_address_map = co_await (
            include_only_local_dc
                    ? get_range_to_address_map_in_local_dc(erm)
                    : get_range_to_address_map(erm)
    );
    ranges.reserve(range_to_address_map.size());
    auto tmptr = erm->get_token_metadata_ptr();
    const auto& topology = tmptr->get_topology();
    for (const auto& [range, addresses] : range_to_address_map) {
        dht::token_range_endpoints tr;
        if (range.start()) {
            tr._start_token = range.start()->value().to_sstring();
        }
        if (range.end()) {
            tr._end_token = range.end()->value().to_sstring();
        }
        tr._endpoints.reserve(addresses.size());
        tr._rpc_endpoints.reserve(addresses.size());
        tr._endpoint_details.reserve(addresses.size());
        for (auto endpoint : addresses) {
            auto it = host_infos.find(endpoint);
            if (it == host_infos.end()) {
                it = host_infos.emplace(endpoint, get_describe_ring_endpoint_info(endpoint, topology, gossiper)).first;
            }
            tr._rpc_endpoints.emplace_back(it->second.rpc_addr);
            tr._endpoints.emplace_back(fmt::to_string(it->second.details._host));
            tr._endpoint_details.emplace_back(it->second.details);
        }
        ranges.push_back(std::move(tr));
        co_await coroutine::maybe_yield();
    }
    // Convert to wrapping ranges
    auto left_inf = std::ranges::find_if(ranges, [] (const dht::token_range_endpoints& tr) {
        return tr._start_token.empty();
    });
    auto right_inf = std::ranges::find_if(ranges, [] (const dht::token_range_endpoints& tr) {
        return tr._end_token.empty();
    });
    using set = std::unordered_set<sstring>;
    if (left_inf != right_inf
            && left_inf != ranges.end()
            && right_inf != ranges.end()
            && ((left_inf->_endpoints | std::ranges::to<set>()) ==
                (right_inf->_endpoints | std::ranges::to<set>()))) {
        left_inf->_start_token = std::move(right_inf->_start_token);
        ranges.erase(right_inf);
    }
    co_return ranges;
}

describe_ring_endpoint_info get_describe_ring_endpoint_info(host_id endpoint, const topology& topology, const gms::gossiper& gossiper) {
    auto& loc = topology.get_node(endpoint).dc_rack();
    return describe_ring_endpoint_info{
        .details = dht::endpoint_details{
            ._host = gossiper.get_address_map().get(endpoint),
            ._datacenter = loc.dc,
            ._rack = loc.rack,
        },
        .rpc_addr = gossiper.get_rpc_address(endpoint)
    };
}

}
