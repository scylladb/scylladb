/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "locator/util.hh"
#include "replica/database.hh"
#include "gms/gossiper.hh"

namespace locator {

static future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
construct_range_to_endpoint_map(
        locator::effective_replication_map_ptr erm,
        const dht::token_range_vector& ranges) {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> res;
    res.reserve(ranges.size());
    for (auto r : ranges) {
        res[r] = erm->get_natural_endpoints(
                r.end() ? r.end()->value() : dht::maximum_token());
        co_await coroutine::maybe_yield();
    }
    co_return res;
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
static future<dht::token_range_vector>
get_all_ranges(const std::vector<token>& sorted_tokens) {
    if (sorted_tokens.empty())
        co_return dht::token_range_vector();
    int size = sorted_tokens.size();
    dht::token_range_vector ranges;
    ranges.reserve(size);
    ranges.push_back(dht::token_range::make_ending_with(interval_bound<token>(sorted_tokens[0], true)));
    co_await coroutine::maybe_yield();
    for (int i = 1; i < size; ++i) {
        dht::token_range r(wrapping_interval<token>::bound(sorted_tokens[i - 1], false), wrapping_interval<token>::bound(sorted_tokens[i], true));
        ranges.push_back(r);
        co_await coroutine::maybe_yield();
    }
    ranges.push_back(dht::token_range::make_starting_with(interval_bound<token>(sorted_tokens[size-1], false)));

    co_return ranges;
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
static future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
get_range_to_address_map(locator::effective_replication_map_ptr erm,
        const std::vector<token>& sorted_tokens) {
    co_return co_await construct_range_to_endpoint_map(erm, co_await get_all_ranges(sorted_tokens));
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
static future<std::vector<token>>
get_tokens_in_local_dc(const locator::token_metadata& tm) {
    std::vector<token> filtered_tokens;
    auto local_dc_filter = tm.get_topology().get_local_dc_filter();
    for (auto token : tm.sorted_tokens()) {
        auto endpoint = tm.get_endpoint(token);
        if (local_dc_filter(*endpoint))
            filtered_tokens.push_back(token);
        co_await coroutine::maybe_yield();
    }
    co_return filtered_tokens;
}

static future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
get_range_to_address_map_in_local_dc(
        locator::effective_replication_map_ptr erm) {
    auto tmptr = erm->get_token_metadata_ptr();
    auto orig_map = co_await get_range_to_address_map(erm, co_await get_tokens_in_local_dc(*tmptr));
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> filtered_map;
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
//     return get_range_to_address_map(db.find_keyspace(keyspace).get_vnode_effective_replication_map());
// }

static future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
get_range_to_address_map(locator::effective_replication_map_ptr erm) {
    return get_range_to_address_map(erm, erm->get_token_metadata_ptr()->sorted_tokens());
}

future<std::vector<dht::token_range_endpoints>>
describe_ring(const replica::database& db, const gms::gossiper& gossiper, const sstring& keyspace, bool include_only_local_dc) {
    std::vector<dht::token_range_endpoints> ranges;

    auto erm = db.find_keyspace(keyspace).get_vnode_effective_replication_map();
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> range_to_address_map = co_await (
            include_only_local_dc
                    ? get_range_to_address_map_in_local_dc(erm)
                    : get_range_to_address_map(erm)
    );
    auto tmptr = erm->get_token_metadata_ptr();
    for (auto entry : range_to_address_map) {
        const auto& topology = tmptr->get_topology();
        auto range = entry.first;
        auto addresses = entry.second;
        dht::token_range_endpoints tr;
        if (range.start()) {
            tr._start_token = range.start()->value().to_sstring();
        }
        if (range.end()) {
            tr._end_token = range.end()->value().to_sstring();
        }
        for (auto endpoint : addresses) {
            dht::endpoint_details details;
            details._host = endpoint;
            details._datacenter = topology.get_datacenter(endpoint);
            details._rack = topology.get_rack(endpoint);
            tr._rpc_endpoints.push_back(gossiper.get_rpc_address(endpoint));
            tr._endpoints.push_back(fmt::to_string(details._host));
            tr._endpoint_details.push_back(details);
        }
        ranges.push_back(tr);
        co_await coroutine::maybe_yield();
    }
    // Convert to wrapping ranges
    auto left_inf = boost::find_if(ranges, [] (const dht::token_range_endpoints& tr) {
        return tr._start_token.empty();
    });
    auto right_inf = boost::find_if(ranges, [] (const dht::token_range_endpoints& tr) {
        return tr._end_token.empty();
    });
    using set = std::unordered_set<sstring>;
    if (left_inf != right_inf
            && left_inf != ranges.end()
            && right_inf != ranges.end()
            && (boost::copy_range<set>(left_inf->_endpoints)
                 == boost::copy_range<set>(right_inf->_endpoints))) {
        left_inf->_start_token = std::move(right_inf->_start_token);
        ranges.erase(right_inf);
    }
    co_return ranges;
}

}
