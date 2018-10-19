/*
 * Copyright (C) 2015 ScyllaDB
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

#include <boost/test/unit_test.hpp>
#include "utils/fb_utilities.hh"
#include "locator/network_topology_strategy.hh"
#include "tests/test-utils.hh"
#include "core/sstring.hh"
#include "log.hh"
#include <vector>
#include <string>
#include <map>
#include <iostream>
#include <sstream>
#include <boost/range/algorithm/adjacent_find.hpp>

static logging::logger nlogger("NetworkTopologyStrategyLogger");

using namespace locator;

struct ring_point {
    double point;
    inet_address host;
};

void print_natural_endpoints(double point, const std::vector<inet_address> v) {
    nlogger.debug("Natural endpoints for a token {}:", point);
    std::string str;
    std::ostringstream strm(str);

    for (auto& addr : v) {
        strm<<addr<<" ";
    }

    nlogger.debug("{}", strm.str());
}

#ifndef SEASTAR_DEBUG
static void verify_sorted(const dht::token_range_vector& trv) {
    auto not_strictly_before = [] (const dht::token_range a, const dht::token_range b) {
        return !b.start()
                || !a.end()
                || a.end()->value() > b.start()->value()
                || (a.end()->value() == b.start()->value() && a.end()->is_inclusive() && b.start()->is_inclusive());
    };
    BOOST_CHECK(boost::adjacent_find(trv, not_strictly_before) == trv.end());
}
#endif

static void check_ranges_are_sorted(abstract_replication_strategy* ars, gms::inet_address ep) {
    // Too slow in debug mode
#ifndef SEASTAR_DEBUG
    verify_sorted(ars->get_ranges(ep));
    verify_sorted(ars->get_primary_ranges(ep));
    verify_sorted(ars->get_primary_ranges_within_dc(ep));
#endif
}

void strategy_sanity_check(
    abstract_replication_strategy* ars_ptr,
    const std::map<sstring, sstring>& options) {

    network_topology_strategy* nts_ptr =
        dynamic_cast<network_topology_strategy*>(ars_ptr);

    //
    // Check that both total and per-DC RFs in options match the corresponding
    // value in the strategy object.
    //
    size_t total_rf = 0;
    for (auto& val : options) {
        size_t rf = std::stol(val.second);
        BOOST_CHECK(nts_ptr->get_replication_factor(val.first) == rf);

        total_rf += rf;
    }

    BOOST_CHECK(ars_ptr->get_replication_factor() == total_rf);
}

void endpoints_check(
    abstract_replication_strategy* ars_ptr,
    std::vector<inet_address>& endpoints) {

    // Check the total RF
    BOOST_CHECK(endpoints.size() == ars_ptr->get_replication_factor());

    // Check the uniqueness
    std::unordered_set<inet_address> ep_set(endpoints.begin(), endpoints.end());
    BOOST_CHECK(endpoints.size() == ep_set.size());

    // Check the per-DC RF
    auto& snitch = i_endpoint_snitch::get_local_snitch_ptr();
    std::unordered_map<sstring, size_t> dc_rf;
    for (auto ep : endpoints) {
        sstring dc = snitch->get_datacenter(ep);

        auto rf = dc_rf.find(dc);
        if (rf == dc_rf.end()) {
            dc_rf[dc] = 1;
        } else {
            rf->second++;
        }
    }

    network_topology_strategy* nts_ptr =
        dynamic_cast<network_topology_strategy*>(ars_ptr);
    for (auto& rf : dc_rf) {
        BOOST_CHECK(rf.second == nts_ptr->get_replication_factor(rf.first));
    }
}

auto d2t = [](double d) {
    unsigned long l = net::hton(static_cast<unsigned long>(d*(std::numeric_limits<unsigned long>::max())));
    std::array<char, 8> a;
    memcpy(a.data(), &l, 8);
    return a;
};

/**
 * Check the get_natural_endpoints() output for tokens between every two
 * adjacent ring points.
 * @param ring_points ring description
 * @param options strategy options
 * @param ars_ptr strategy object
 */
void full_ring_check(const std::vector<ring_point>& ring_points,
                     const std::map<sstring, sstring>& options,
                     abstract_replication_strategy* ars_ptr) {
    strategy_sanity_check(ars_ptr, options);

    for (auto& rp : ring_points) {
        double cur_point1 = rp.point - 0.5;
        token t1{dht::token::kind::key,
             {(int8_t*)d2t(cur_point1 / ring_points.size()).data(), 8}};
        uint64_t cache_hit_count = ars_ptr->get_cache_hits_count();
        auto endpoints1 = ars_ptr->get_natural_endpoints(t1);

        endpoints_check(ars_ptr, endpoints1);
        // validate that the result hasn't been taken from the cache
        BOOST_CHECK(cache_hit_count == ars_ptr->get_cache_hits_count());

        print_natural_endpoints(cur_point1, endpoints1);

        //
        // Check a different endpoint in the same range as t1 and validate that
        // the endpoints has been taken from the cache and that the output is
        // identical to the one not taken from the cache.
        //
        cache_hit_count = ars_ptr->get_cache_hits_count();
        double cur_point2 = rp.point - 0.2;
        token t2{dht::token::kind::key,
             {(int8_t*)d2t(cur_point2 / ring_points.size()).data(), 8}};
        auto endpoints2 = ars_ptr->get_natural_endpoints(t2);

        endpoints_check(ars_ptr, endpoints2);
        check_ranges_are_sorted(ars_ptr, rp.host);
        BOOST_CHECK(cache_hit_count + 1 == ars_ptr->get_cache_hits_count());
        BOOST_CHECK(endpoints1 == endpoints2);
    }
}

future<> simple_test() {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    // Create the RackInferringSnitch
    return i_endpoint_snitch::create_snitch("RackInferringSnitch").then(
        [] {

        lw_shared_ptr<token_metadata> tm = make_lw_shared<token_metadata>();
        std::vector<ring_point> ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 2.0,  inet_address("192.101.10.1") },
            { 3.0,  inet_address("192.102.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 5.0,  inet_address("192.101.20.1") },
            { 6.0,  inet_address("192.102.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
            { 8.0,  inet_address("192.101.30.1") },
            { 9.0,  inet_address("192.102.30.1") },
            { 10.0, inet_address("192.102.40.1") },
            { 11.0, inet_address("192.102.40.2") }
        };
        // Initialize the token_metadata
        for (unsigned i = 0; i < ring_points.size(); i++) {
            tm->update_normal_token(
                {dht::token::kind::key,
                 {(int8_t*)d2t(ring_points[i].point / ring_points.size()).data(), 8}
                },
                ring_points[i].host);
        }

        /////////////////////////////////////
        // Create the replication strategy
        std::map<sstring, sstring> options323 = {
            {"100", "3"},
            {"101", "2"},
            {"102", "3"}
        };

        auto ars_uptr = abstract_replication_strategy::create_replication_strategy(
            "test keyspace", "NetworkTopologyStrategy", *tm, options323);

        auto ars_ptr = ars_uptr.get();

        full_ring_check(ring_points, options323, ars_ptr);

        ///////////////
        // Create the replication strategy
        std::map<sstring, sstring> options320 = {
            {"100", "3"},
            {"101", "2"},
            {"102", "0"}
        };

        ars_uptr = abstract_replication_strategy::create_replication_strategy(
            "test keyspace", "NetworkTopologyStrategy", *tm, options320);

        ars_ptr = ars_uptr.get();

        full_ring_check(ring_points, options320, ars_ptr);

        //
        // Check cache invalidation: invalidate the cache and run a full ring
        // check once again. If cache is not properly invalidated one of the
        // points will be taken from the cache when it shouldn't and the
        // corresponding check will fail.
        //
        tm->invalidate_cached_rings();
        full_ring_check(ring_points, options320, ars_ptr);

        return i_endpoint_snitch::stop_snitch();
    });
}

future<> heavy_origin_test() {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    // Create the RackInferringSnitch
    return i_endpoint_snitch::create_snitch("RackInferringSnitch").then(
        [] {
        std::vector<int> dc_racks = {2, 4, 8};
        std::vector<int> dc_endpoints = {128, 256, 512};
        std::vector<int> dc_replication = {2, 6, 6};

        lw_shared_ptr<token_metadata> tm = make_lw_shared<token_metadata>();
        std::map<sstring, sstring> config_options;
        std::unordered_map<inet_address, std::unordered_set<token>> tokens;
        std::vector<ring_point> ring_points;

        size_t total_eps = 0;
        for (size_t dc = 0; dc < dc_racks.size(); ++dc) {
            for (int rack = 0; rack < dc_racks[dc]; ++rack) {
                total_eps += dc_endpoints[dc]/dc_racks[dc];
            }
        }

        int total_rf = 0;
        double token_point = 1.0;
        for (size_t dc = 0; dc < dc_racks.size(); ++dc) {
            total_rf += dc_replication[dc];
            config_options.emplace(to_sstring(dc),
                                   to_sstring(dc_replication[dc]));
            for (int rack = 0; rack < dc_racks[dc]; ++rack) {
                for (int ep = 1; ep <= dc_endpoints[dc]/dc_racks[dc]; ++ep) {

                    // 10.dc.rack.ep
                    int32_t ip = 0x0a000000 + ((int8_t)dc << 16) +
                                 ((int8_t)rack << 8) + (int8_t)ep;
                    inet_address address(ip);
                    ring_point rp = {token_point, address};

                    ring_points.emplace_back(rp);
                    tokens[address].emplace(token{dht::token::kind::key,
                            {(int8_t*)d2t(token_point / total_eps).data(), 8}});

                    nlogger.debug("adding node {} at {}", address, token_point);

                    token_point++;
                }
            }
        }

        tm->update_normal_tokens(tokens);

        auto ars_uptr = abstract_replication_strategy::create_replication_strategy(
            "test keyspace", "NetworkTopologyStrategy", *tm, config_options);

        auto ars_ptr = ars_uptr.get();

        full_ring_check(ring_points, config_options, ars_ptr);

        return i_endpoint_snitch::stop_snitch();
    });
}


SEASTAR_TEST_CASE(NetworkTopologyStrategy_simple) {
    return simple_test();
}

SEASTAR_TEST_CASE(NetworkTopologyStrategy_heavy) {
    return heavy_origin_test();
}
