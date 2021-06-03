/*
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

#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>
#include "utils/fb_utilities.hh"
#include "utils/sequenced_set.hh"
#include "locator/network_topology_strategy.hh"
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/sstring.hh>
#include "log.hh"
#include <vector>
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <sstream>
#include <boost/range/algorithm/adjacent_find.hpp>
#include <boost/algorithm/cxx11/iota.hpp>
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include <seastar/core/coroutine.hh>

using namespace locator;

struct ring_point {
    double point;
    inet_address host;
};

void print_natural_endpoints(double point, const inet_address_vector_replica_set v) {
    testlog.debug("Natural endpoints for a token {}:", point);
    std::string str;
    std::ostringstream strm(str);

    for (auto& addr : v) {
        strm<<addr<<" ";
    }

    testlog.debug("{}", strm.str());
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
    verify_sorted(ars->get_primary_ranges(ep, utils::can_yield::no));
    verify_sorted(ars->get_primary_ranges_within_dc(ep, utils::can_yield::no));
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
    inet_address_vector_replica_set& endpoints) {

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

auto d2t = [](double d) -> int64_t {
    // Double to unsigned long conversion will overflow if the
    // input is greater than numeric_limits<long>::max(), so divide by two and
    // multiply again later.
    return static_cast<unsigned long>(d*(std::numeric_limits<unsigned long>::max() >> 1)) << 1;
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
        token t1(dht::token::kind::key, d2t(cur_point1 / ring_points.size()));
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
        token t2(dht::token::kind::key, d2t(cur_point2 / ring_points.size()));
        auto endpoints2 = ars_ptr->get_natural_endpoints(t2);

        endpoints_check(ars_ptr, endpoints2);
        check_ranges_are_sorted(ars_ptr, rp.host);
        BOOST_CHECK(cache_hit_count + 1 == ars_ptr->get_cache_hits_count());
        BOOST_CHECK(endpoints1 == endpoints2);
    }
}

// Run in a seastar thread.
void simple_test() {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    // Create the RackInferringSnitch
    i_endpoint_snitch::create_snitch("RackInferringSnitch").get();
    auto stop_snitch = defer([] {
        i_endpoint_snitch::stop_snitch().get();
    });

    locator::shared_token_metadata stm;

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
  stm.mutate_token_metadata([&ring_points] (token_metadata& tm) -> future<> {
    for (unsigned i = 0; i < ring_points.size(); i++) {
        co_await tm.update_normal_token(
            {dht::token::kind::key, d2t(ring_points[i].point / ring_points.size())},
            ring_points[i].host);
    }
  }).get();

    /////////////////////////////////////
    // Create the replication strategy
    std::map<sstring, sstring> options323 = {
        {"100", "3"},
        {"101", "2"},
        {"102", "3"}
    };

    auto ars_uptr = abstract_replication_strategy::create_replication_strategy(
        "test keyspace", "NetworkTopologyStrategy", stm, options323);

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
        "test keyspace", "NetworkTopologyStrategy", stm, options320);

    ars_ptr = ars_uptr.get();

    full_ring_check(ring_points, options320, ars_ptr);

    //
    // Check cache invalidation: invalidate the cache and run a full ring
    // check once again. If cache is not properly invalidated one of the
    // points will be taken from the cache when it shouldn't and the
    // corresponding check will fail.
    //
    stm.mutate_token_metadata([] (token_metadata& tm) {
        tm.invalidate_cached_rings();
        return make_ready_future<>();
    }).get();
    full_ring_check(ring_points, options320, ars_ptr);
}

// Run in a seastar thread.
void heavy_origin_test() {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    // Create the RackInferringSnitch
    i_endpoint_snitch::create_snitch("RackInferringSnitch").get();
    auto stop_snitch = defer([] {
        i_endpoint_snitch::stop_snitch().get();
    });

    locator::shared_token_metadata stm;

    std::vector<int> dc_racks = {2, 4, 8};
    std::vector<int> dc_endpoints = {128, 256, 512};
    std::vector<int> dc_replication = {2, 6, 6};

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
    auto& random_engine = seastar::testing::local_random_engine;
    std::vector<double> token_points(total_eps, 0.0);
    boost::algorithm::iota(token_points, 1.0);
    std::shuffle(token_points.begin(), token_points.end(), random_engine);
    auto token_point_iterator = token_points.begin();
    for (size_t dc = 0; dc < dc_racks.size(); ++dc) {
        total_rf += dc_replication[dc];
        config_options.emplace(to_sstring(dc),
                                to_sstring(dc_replication[dc]));
        for (int rack = 0; rack < dc_racks[dc]; ++rack) {
            for (int ep = 1; ep <= dc_endpoints[dc]/dc_racks[dc]; ++ep) {
                double token_point = *token_point_iterator++;
                // 10.dc.rack.ep
                int32_t ip = 0x0a000000 + ((int8_t)dc << 16) +
                                ((int8_t)rack << 8) + (int8_t)ep;
                inet_address address(ip);
                ring_point rp = {token_point, address};

                ring_points.emplace_back(rp);
                tokens[address].emplace(token{dht::token::kind::key, d2t(token_point / total_eps)});

                testlog.debug("adding node {} at {}", address, token_point);

                token_point++;
            }
        }
    }

    stm.mutate_token_metadata([&tokens] (token_metadata& tm) {
        return tm.update_normal_tokens(tokens);
    }).get();

    auto ars_uptr = abstract_replication_strategy::create_replication_strategy(
        "test keyspace", "NetworkTopologyStrategy", stm, config_options);

    auto ars_ptr = ars_uptr.get();

    full_ring_check(ring_points, config_options, ars_ptr);
}


SEASTAR_THREAD_TEST_CASE(NetworkTopologyStrategy_simple) {
    return simple_test();
}

SEASTAR_THREAD_TEST_CASE(NetworkTopologyStrategy_heavy) {
    return heavy_origin_test();
}

/**
 * static impl of "old" network topology strategy endpoint calculation.
 */
static size_t get_replication_factor(const sstring& dc,
                const std::unordered_map<sstring, size_t>& datacenters) noexcept {
    auto dc_factor = datacenters.find(dc);
    return (dc_factor == datacenters.end()) ? 0 : dc_factor->second;
}

static bool has_sufficient_replicas(const sstring& dc,
                const std::unordered_map<sstring, std::unordered_set<inet_address>>& dc_replicas,
                const std::unordered_map<sstring, std::unordered_set<inet_address>>& all_endpoints,
                const std::unordered_map<sstring, size_t>& datacenters) noexcept {
    auto dc_replicas_it = dc_replicas.find(dc);
    if (dc_replicas_it == dc_replicas.end()) {
        BOOST_TEST_FAIL(format("has_sufficient_replicas: dc {} not found in dc_replicas: {}", dc, dc_replicas));
    }
    auto endpoint_it = all_endpoints.find(dc);
    if (endpoint_it == all_endpoints.end()) {
        BOOST_TEST_MESSAGE(format("has_sufficient_replicas: dc {} not found in all_endpoints: {}", dc, all_endpoints));
        return true;
    }
    return dc_replicas_it->second.size()
                    >= std::min(endpoint_it->second.size(),
                                    get_replication_factor(dc, datacenters));
}

static bool has_sufficient_replicas(
                const std::unordered_map<sstring, std::unordered_set<inet_address>>& dc_replicas,
                const std::unordered_map<sstring, std::unordered_set<inet_address>>& all_endpoints,
                const std::unordered_map<sstring, size_t>& datacenters) noexcept {

    for (auto& dc : datacenters | boost::adaptors::map_keys) {
        if (!has_sufficient_replicas(dc, dc_replicas, all_endpoints,
                        datacenters)) {
            return false;
        }
    }

    return true;
}

static std::vector<inet_address> calculate_natural_endpoints(
                const token& search_token, const token_metadata& tm,
                snitch_ptr& snitch,
                const std::unordered_map<sstring, size_t>& datacenters) {
    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    utils::sequenced_set<inet_address> replicas;

    // replicas we have found in each DC
    std::unordered_map<sstring, std::unordered_set<inet_address>> dc_replicas;
    // tracks the racks we have already placed replicas in
    std::unordered_map<sstring, std::unordered_set<sstring>> seen_racks;
    //
    // tracks the endpoints that we skipped over while looking for unique racks
    // when we relax the rack uniqueness we can append this to the current
    // result so we don't have to wind back the iterator
    //
    std::unordered_map<sstring, utils::sequenced_set<inet_address>>
        skipped_dc_endpoints;

    //
    // Populate the temporary data structures.
    //
    for (auto& dc_rep_factor_pair : datacenters) {
        auto& dc_name = dc_rep_factor_pair.first;

        dc_replicas[dc_name].reserve(dc_rep_factor_pair.second);
        seen_racks[dc_name] = {};
        skipped_dc_endpoints[dc_name] = {};
    }

    const topology& tp = tm.get_topology();

    //
    // all endpoints in each DC, so we can check when we have exhausted all
    // the members of a DC
    //
    const std::unordered_map<sstring,
                       std::unordered_set<inet_address>>&
        all_endpoints = tp.get_datacenter_endpoints();
    //
    // all racks in a DC so we can check when we have exhausted all racks in a
    // DC
    //
    const std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>&
        racks = tp.get_datacenter_racks();

    // not aware of any cluster members
    assert(!all_endpoints.empty() && !racks.empty());

    for (auto& next : tm.ring_range(search_token)) {

        if (has_sufficient_replicas(dc_replicas, all_endpoints, datacenters)) {
            break;
        }

        inet_address ep = *tm.get_endpoint(next);
        sstring dc = snitch->get_datacenter(ep);

        auto& seen_racks_dc_set = seen_racks[dc];
        auto& racks_dc_map = racks.at(dc);
        auto& skipped_dc_endpoints_set = skipped_dc_endpoints[dc];
        auto& dc_replicas_dc_set = dc_replicas[dc];

        // have we already found all replicas for this dc?
        if (!datacenters.contains(dc) ||
            has_sufficient_replicas(dc, dc_replicas, all_endpoints, datacenters)) {
            continue;
        }

        //
        // can we skip checking the rack? - namely, we've seen all racks in this
        // DC already and may add this endpoint right away.
        //
        if (seen_racks_dc_set.size() == racks_dc_map.size()) {
            dc_replicas_dc_set.insert(ep);
            replicas.push_back(ep);
        } else {
            sstring rack = snitch->get_rack(ep);
            // is this a new rack? - we prefer to replicate on different racks
            if (seen_racks_dc_set.contains(rack)) {
                skipped_dc_endpoints_set.push_back(ep);
            } else { // this IS a new rack
                dc_replicas_dc_set.insert(ep);
                replicas.push_back(ep);
                seen_racks_dc_set.insert(rack);
                //
                // if we've run out of distinct racks, add the hosts we skipped
                // past already (up to RF)
                //
                if (seen_racks_dc_set.size() == racks_dc_map.size())
                {
                    auto skipped_it = skipped_dc_endpoints_set.begin();
                    while (skipped_it != skipped_dc_endpoints_set.end() &&
                           !has_sufficient_replicas(dc, dc_replicas, all_endpoints, datacenters)) {
                        inet_address skipped = *skipped_it++;
                        dc_replicas_dc_set.insert(skipped);
                        replicas.push_back(skipped);
                    }
                }
            }
        }
    }

    return std::move(replicas.get_vector());
}

static void test_equivalence(const shared_token_metadata& stm, snitch_ptr& snitch, const std::unordered_map<sstring, size_t>& datacenters) {
    class my_network_topology_strategy : public network_topology_strategy {
    public:
        using network_topology_strategy::network_topology_strategy;
        using network_topology_strategy::calculate_natural_endpoints;
    };

    my_network_topology_strategy nts("ks", stm, snitch,
                    boost::copy_range<std::map<sstring, sstring>>(
                                    datacenters
                                                    | boost::adaptors::transformed(
                                                                    [](const std::pair<sstring, size_t>& p) {
                                                                        return std::make_pair(p.first, to_sstring(p.second));
                                                                    })));

    const token_metadata& tm = *stm.get();
    for (size_t i = 0; i < 1000; ++i) {
        auto token = dht::token::get_random_token();
        auto expected = calculate_natural_endpoints(token, tm, snitch, datacenters);
        auto actual = nts.calculate_natural_endpoints(token, tm, utils::can_yield::no);

        // Because the old algorithm does not put the nodes in the correct order in the case where more replicas
        // are required than there are racks in a dc, we accept different order as long as the primary
        // replica is the same.

        BOOST_REQUIRE_EQUAL(expected[0], actual[0]);
        BOOST_REQUIRE_EQUAL(std::set<inet_address>(expected.begin(), expected.end()),
                        std::set<inet_address>(actual.begin(), actual.end()));

    }
}


std::unique_ptr<i_endpoint_snitch> generate_snitch(const std::unordered_map<sstring, size_t> datacenters, const std::vector<inet_address>& nodes) {
    auto& e1 = seastar::testing::local_random_engine;

    using addr_to_string_type = std::unordered_map<inet_address, sstring>;

    addr_to_string_type node_to_rack, node_to_dc;
    std::unordered_map<sstring, size_t> racks_per_dc;
    std::vector<std::reference_wrapper<const sstring>> dcs;

    dcs.reserve(datacenters.size() * 4);

    using udist = std::uniform_int_distribution<size_t>;

    auto out = std::back_inserter(dcs);

    for (auto& p : datacenters) {
        auto& dc = p.first;
        auto rf = p.second;
        auto rc = udist(0, rf * 3 - 1)(e1) + 1;
        racks_per_dc.emplace(dc, rc);
        out = std::fill_n(out, rf, std::cref(dc));
    }

    for (auto& node : nodes) {
        const sstring& dc = dcs[udist(0, dcs.size() - 1)(e1)];
        auto rc = racks_per_dc.at(dc);
        auto r = udist(0, rc)(e1);
        node_to_rack.emplace(node, to_sstring(r));
        node_to_dc.emplace(node, dc);
    }

    class my_snitch : public snitch_base {
    public:
        my_snitch(addr_to_string_type node_to_rack,
                        addr_to_string_type node_to_dc)
            : _node_to_rack(std::move(node_to_rack))
            , _node_to_dc(std::move(node_to_dc))
        {}
        sstring get_rack(inet_address endpoint) override {
            return _node_to_rack.at(endpoint);
        }
        sstring get_datacenter(inet_address endpoint) override {
            return _node_to_dc.at(endpoint);
        }
        sstring get_name() const {
            return "muminpappa";
        }
    private:
        addr_to_string_type _node_to_rack, _node_to_dc;
    };

    return std::make_unique<my_snitch>(std::move(node_to_rack), std::move(node_to_dc));
}

SEASTAR_THREAD_TEST_CASE(testCalculateEndpoints) {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    i_endpoint_snitch::create_snitch("RackInferringSnitch").get();
    auto stop_snitch = defer([] {
        i_endpoint_snitch::stop_snitch().get();
    });

    constexpr size_t NODES = 100;
    constexpr size_t VNODES = 64;
    constexpr size_t RUNS = 10;

    std::unordered_map<sstring, size_t> datacenters = {
                    { "rf1", 1 },
                    { "rf3", 3 },
                    { "rf5_1", 5 },
                    { "rf5_2", 5 },
                    { "rf5_3", 5 },
    };
    std::vector<inet_address> nodes;
    nodes.reserve(NODES);
    std::generate_n(std::back_inserter(nodes), NODES, [i = 0u]() mutable {
        return inet_address((127u << 24) | ++i);
    });

    auto& snitch = i_endpoint_snitch::get_local_snitch_ptr();

    for (size_t run = 0; run < RUNS; ++run) {
        shared_token_metadata stm;
        // not doing anything sharded. We can just play fast and loose with the snitch.
        (void)snitch.stop();
        snitch = generate_snitch(datacenters, nodes);

      stm.mutate_token_metadata([&nodes] (token_metadata& tm) -> future<> {
        for (auto& node : nodes) {
            for (size_t i = 0; i < VNODES; ++i) {
                co_await tm.update_normal_token(dht::token::get_random_token(), node);
            }
        }
      }).get();
        test_equivalence(stm, snitch, datacenters);
    }
}

SEASTAR_TEST_CASE(test_invalid_dcs) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto& incorrect : std::vector<std::string>{"3\"", "", "!!!", "abcb", "!3", "-5", "0x123", "999999999999999999999999999999"}) {
            BOOST_REQUIRE_THROW(e.execute_cql("CREATE KEYSPACE abc WITH REPLICATION "
                    "= {'class': 'NetworkTopologyStrategy', 'dc1':'" + incorrect + "'}").get(),
                    exceptions::configuration_exception);
            BOOST_REQUIRE_THROW(e.execute_cql("CREATE KEYSPACE abc WITH REPLICATION "
                    "= {'class': 'SimpleStrategy', 'replication_factor':'" + incorrect + "'}").get(),
                    exceptions::configuration_exception);
        };
    });
}

