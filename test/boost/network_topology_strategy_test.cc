/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>
#include "gms/inet_address.hh"
#include "locator/types.hh"
#include "utils/UUID_gen.hh"
#include "utils/fb_utilities.hh"
#include "utils/sequenced_set.hh"
#include "locator/network_topology_strategy.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/sstring.hh>
#include "log.hh"
#include "gms/gossiper.hh"
#include <vector>
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <sstream>
#include <compare>
#include <boost/range/algorithm/adjacent_find.hpp>
#include <boost/algorithm/cxx11/iota.hpp>
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include <seastar/core/coroutine.hh>
#include "db/schema_tables.hh"
#include "message/msg_addr.hh"

using namespace locator;

struct ring_point {
    double point;
    inet_address host;
    host_id id = host_id::create_random_id();
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

static void verify_sorted(const dht::token_range_vector& trv) {
    auto not_strictly_before = [] (const dht::token_range a, const dht::token_range b) {
        return !b.start()
                || !a.end()
                || a.end()->value() > b.start()->value()
                || (a.end()->value() == b.start()->value() && a.end()->is_inclusive() && b.start()->is_inclusive());
    };
    BOOST_CHECK(boost::adjacent_find(trv, not_strictly_before) == trv.end());
}

static void check_ranges_are_sorted(effective_replication_map_ptr erm, gms::inet_address ep) {
    verify_sorted(erm->get_ranges(ep));
    verify_sorted(erm->get_primary_ranges(ep));
    verify_sorted(erm->get_primary_ranges_within_dc(ep));
}

void strategy_sanity_check(
    abstract_replication_strategy::ptr_type ars_ptr,
    const token_metadata& tm,
    const std::map<sstring, sstring>& options) {

    network_topology_strategy* nts_ptr =
        dynamic_cast<network_topology_strategy*>(ars_ptr.get());

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

    BOOST_CHECK(ars_ptr->get_replication_factor(tm) == total_rf);
}

void endpoints_check(
    abstract_replication_strategy::ptr_type ars_ptr,
    const token_metadata& tm,
    inet_address_vector_replica_set& endpoints,
    const locator::topology& topo) {

    // Check the total RF
    BOOST_CHECK(endpoints.size() == ars_ptr->get_replication_factor(tm));

    // Check the uniqueness
    std::unordered_set<inet_address> ep_set(endpoints.begin(), endpoints.end());
    BOOST_CHECK(endpoints.size() == ep_set.size());

    // Check the per-DC RF
    std::unordered_map<sstring, size_t> dc_rf;
    for (auto ep : endpoints) {
        sstring dc = topo.get_location(ep).dc;

        auto rf = dc_rf.find(dc);
        if (rf == dc_rf.end()) {
            dc_rf[dc] = 1;
        } else {
            rf->second++;
        }
    }

    network_topology_strategy* nts_ptr =
        dynamic_cast<network_topology_strategy*>(ars_ptr.get());
    for (auto& rf : dc_rf) {
        BOOST_CHECK(rf.second == nts_ptr->get_replication_factor(rf.first));
    }
}

auto d2t = [](double d) -> int64_t {
    // Double to unsigned long conversion will overflow if the
    // input is greater than numeric_limits<long>::max(), so divide by two and
    // multiply again later.
    auto scale = std::numeric_limits<unsigned long>::max();
    return static_cast<unsigned long>(d * static_cast<double>(scale >> 1)) << 1;
};

/**
 * Check the get_natural_endpoints() output for tokens between every two
 * adjacent ring points.
 * @param ring_points ring description
 * @param options strategy options
 * @param ars_ptr strategy object
 *
 * Run in a seastar thread.
 */
void full_ring_check(const std::vector<ring_point>& ring_points,
                     const std::map<sstring, sstring>& options,
                     abstract_replication_strategy::ptr_type ars_ptr,
                     locator::token_metadata_ptr tmptr) {
    auto& tm = *tmptr;
    const auto& topo = tm.get_topology();
    strategy_sanity_check(ars_ptr, tm, options);

    auto erm = calculate_effective_replication_map(ars_ptr, tmptr).get0();

    for (auto& rp : ring_points) {
        double cur_point1 = rp.point - 0.5;
        token t1(dht::token::kind::key, d2t(cur_point1 / ring_points.size()));
        auto endpoints1 = erm->get_natural_endpoints(t1);

        endpoints_check(ars_ptr, tm, endpoints1, topo);

        print_natural_endpoints(cur_point1, endpoints1);

        //
        // Check a different endpoint in the same range as t1 and validate that
        // the endpoints has been taken from the cache and that the output is
        // identical to the one not taken from the cache.
        //
        double cur_point2 = rp.point - 0.2;
        token t2(dht::token::kind::key, d2t(cur_point2 / ring_points.size()));
        auto endpoints2 = erm->get_natural_endpoints(t2);

        endpoints_check(ars_ptr, tm, endpoints2, topo);
        check_ranges_are_sorted(erm, rp.host);
        BOOST_CHECK(endpoints1 == endpoints2);
    }
}

locator::endpoint_dc_rack make_endpoint_dc_rack(gms::inet_address endpoint) {
    // This resembles rack_inferring_snitch dc/rack generation which is
    // still in use by this test via token_metadata internals
    auto dc = std::to_string(uint8_t(endpoint.bytes()[1]));
    auto rack = std::to_string(uint8_t(endpoint.bytes()[2]));
    return locator::endpoint_dc_rack{dc, rack};
}

// Run in a seastar thread.
void simple_test() {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    // Create the RackInferringSnitch
    snitch_config cfg;
    cfg.name = "RackInferringSnitch";
    sharded<snitch_ptr> snitch;
    snitch.start(cfg).get();
    auto stop_snitch = defer([&snitch] { snitch.stop().get(); });
    snitch.invoke_on_all(&snitch_ptr::start).get();

    locator::token_metadata::config tm_cfg;
    tm_cfg.topo_cfg.this_host_id = host_id::create_random_id();
    tm_cfg.topo_cfg.this_endpoint = utils::fb_utilities::get_broadcast_address();
    tm_cfg.topo_cfg.local_dc_rack = { snitch.local()->get_datacenter(), snitch.local()->get_rack() };
    locator::shared_token_metadata stm([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg);

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
    stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
        auto& topo = tm.get_topology();
        for (const auto& [ring_point, endpoint, id] : ring_points) {
            std::unordered_set<token> tokens;
            tokens.insert({dht::token::kind::key, d2t(ring_point / ring_points.size())});
            topo.add_node(id, endpoint, make_endpoint_dc_rack(endpoint), locator::node::state::normal);
            co_await tm.update_normal_tokens(std::move(tokens), endpoint);
        }
    }).get();

    /////////////////////////////////////
    // Create the replication strategy
    std::map<sstring, sstring> options323 = {
        {"100", "3"},
        {"101", "2"},
        {"102", "3"}
    };

    auto ars_ptr = abstract_replication_strategy::create_replication_strategy(
        "NetworkTopologyStrategy", options323);

    full_ring_check(ring_points, options323, ars_ptr, stm.get());

    ///////////////
    // Create the replication strategy
    std::map<sstring, sstring> options320 = {
        {"100", "3"},
        {"101", "2"},
        {"102", "0"}
    };

    ars_ptr = abstract_replication_strategy::create_replication_strategy(
        "NetworkTopologyStrategy", options320);

    full_ring_check(ring_points, options320, ars_ptr, stm.get());

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
    full_ring_check(ring_points, options320, ars_ptr, stm.get());
}

// Run in a seastar thread.
void heavy_origin_test() {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    // Create the RackInferringSnitch
    snitch_config cfg;
    cfg.name = "RackInferringSnitch";
    sharded<snitch_ptr> snitch;
    snitch.start(cfg).get();
    auto stop_snitch = defer([&snitch] { snitch.stop().get(); });
    snitch.invoke_on_all(&snitch_ptr::start).get();

    locator::shared_token_metadata stm([] () noexcept { return db::schema_tables::hold_merge_lock(); }, locator::token_metadata::config{});

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

    auto& random_engine = seastar::testing::local_random_engine;
    std::vector<double> token_points(total_eps, 0.0);
    boost::algorithm::iota(token_points, 1.0);
    std::shuffle(token_points.begin(), token_points.end(), random_engine);
    auto token_point_iterator = token_points.begin();
    for (size_t dc = 0; dc < dc_racks.size(); ++dc) {
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

    stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
        auto& topo = tm.get_topology();
        for (const auto& [ring_point, endpoint, id] : ring_points) {
            topo.add_node(id, endpoint, make_endpoint_dc_rack(endpoint), locator::node::state::normal);
            co_await tm.update_normal_tokens(std::move(tokens[endpoint]), endpoint);
        }
    }).get();

    auto ars_ptr = abstract_replication_strategy::create_replication_strategy(
        "NetworkTopologyStrategy", config_options);

    full_ring_check(ring_points, config_options, ars_ptr, stm.get());
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

static locator::endpoint_set calculate_natural_endpoints(
                const token& search_token, const token_metadata& tm,
                const locator::topology& topo,
                const std::unordered_map<sstring, size_t>& datacenters) {
    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    locator::endpoint_set replicas;

    // replicas we have found in each DC
    std::unordered_map<sstring, std::unordered_set<inet_address>> dc_replicas;
    // tracks the racks we have already placed replicas in
    std::unordered_map<sstring, std::unordered_set<sstring>> seen_racks;
    //
    // tracks the endpoints that we skipped over while looking for unique racks
    // when we relax the rack uniqueness we can append this to the current
    // result so we don't have to wind back the iterator
    //
    std::unordered_map<sstring, locator::endpoint_set>
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
                       std::unordered_set<inet_address>>
        all_endpoints = tp.get_datacenter_endpoints();
    //
    // all racks in a DC so we can check when we have exhausted all racks in a
    // DC
    //
    const std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>
        racks = tp.get_datacenter_racks();

    // not aware of any cluster members
    assert(!all_endpoints.empty() && !racks.empty());

    for (auto& next : tm.ring_range(search_token)) {

        if (has_sufficient_replicas(dc_replicas, all_endpoints, datacenters)) {
            break;
        }

        inet_address ep = *tm.get_endpoint(next);
        sstring dc = topo.get_location(ep).dc;

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
            sstring rack = topo.get_location(ep).rack;
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

    return replicas;
}

// Called in a seastar thread.
static void test_equivalence(const shared_token_metadata& stm, const locator::topology& topo, const std::unordered_map<sstring, size_t>& datacenters) {
    class my_network_topology_strategy : public network_topology_strategy {
    public:
        using network_topology_strategy::network_topology_strategy;
        using network_topology_strategy::calculate_natural_endpoints;
    };

    my_network_topology_strategy nts(
                    boost::copy_range<std::map<sstring, sstring>>(
                                    datacenters
                                                    | boost::adaptors::transformed(
                                                                    [](const std::pair<sstring, size_t>& p) {
                                                                        return std::make_pair(p.first, to_sstring(p.second));
                                                                    })));

    const token_metadata& tm = *stm.get();
    for (size_t i = 0; i < 1000; ++i) {
        auto token = dht::token::get_random_token();
        auto expected = calculate_natural_endpoints(token, tm, topo, datacenters);
        auto actual = nts.calculate_natural_endpoints(token, tm).get0();

        // Because the old algorithm does not put the nodes in the correct order in the case where more replicas
        // are required than there are racks in a dc, we accept different order as long as the primary
        // replica is the same.

        BOOST_REQUIRE_EQUAL(expected[0], actual[0]);
        BOOST_REQUIRE_EQUAL(std::set<inet_address>(expected.begin(), expected.end()),
                        std::set<inet_address>(actual.begin(), actual.end()));

    }
}


void generate_topology(topology& topo, const std::unordered_map<sstring, size_t> datacenters, const std::vector<inet_address>& nodes) {
    auto& e1 = seastar::testing::local_random_engine;

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
        topo.add_node(host_id::create_random_id(), node, {dc, to_sstring(r)}, locator::node::state::normal);
    }
}

SEASTAR_THREAD_TEST_CASE(testCalculateEndpoints) {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

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

    for (size_t run = 0; run < RUNS; ++run) {
        semaphore sem(1);
        shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{});

        std::unordered_set<dht::token> random_tokens;
        while (random_tokens.size() < nodes.size() * VNODES) {
            random_tokens.insert(dht::token::get_random_token());
        }
        std::unordered_map<inet_address, std::unordered_set<token>> endpoint_tokens;
        auto next_token_it = random_tokens.begin();
        for (auto& node : nodes) {
            for (size_t i = 0; i < VNODES; ++i) {
                endpoint_tokens[node].insert(*next_token_it);
                next_token_it++;
            }
        }
        
        stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
            generate_topology(tm.get_topology(), datacenters, nodes);
            for (auto&& i : endpoint_tokens) {
                co_await tm.update_normal_tokens(std::move(i.second), i.first);
            }
        }).get();
        test_equivalence(stm, stm.get()->get_topology(), datacenters);
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

namespace locator {

void topology::test_compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const {
    std::optional<std::partial_ordering> expected;
    const auto& loc = get_location(address);
    const auto& loc1 = get_location(a1);
    const auto& loc2 = get_location(a2);
    if (a1 == a2) {
        expected = std::partial_ordering::equivalent;
    } else {
        if (a1 == address) {
            expected = std::partial_ordering::less;
        } else if (a2 == address) {
            expected = std::partial_ordering::greater;
        } else {
            if (loc1.dc == loc.dc) {
                if (loc2.dc != loc.dc) {
                    expected = std::partial_ordering::less;
                } else {
                    if (loc1.rack == loc.rack) {
                        if (loc2.rack != loc.rack) {
                            expected = std::partial_ordering::less;
                        }
                    } else if (loc2.rack == loc.rack) {
                        expected = std::partial_ordering::greater;
                    }
                }
            } else if (loc2.dc == loc.dc) {
                expected = std::partial_ordering::greater;
            }
        }
    }
    auto res = compare_endpoints(address, a1, a2);
    testlog.debug("compare_endpoint: address={} [{}/{}] a1={} [{}/{}] a2={} [{}/{}]: res={} expected={} expected_value={}",
            address, loc.dc, loc.rack,
            a1, loc1.dc, loc1.rack,
            a2, loc2.dc, loc2.rack,
            res, bool(expected), expected.value_or(std::partial_ordering::unordered));
    if (expected) {
        BOOST_REQUIRE_EQUAL(res, *expected);
    }
}

} // namespace locator

SEASTAR_THREAD_TEST_CASE(test_topology_compare_endpoints) {
    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    constexpr size_t NODES = 10;

    std::unordered_map<sstring, size_t> datacenters = {
                    { "rf1", 1 },
                    { "rf2", 2 },
                    { "rf3", 3 },
    };
    std::vector<inet_address> nodes;
    nodes.reserve(NODES);

    auto make_address = [] (unsigned i) {
        return inet_address((127u << 24) | i);
    };

    std::generate_n(std::back_inserter(nodes), NODES, [&, i = 0u]() mutable {
        return make_address(++i);
    });
    auto bogus_address = make_address(NODES + 1);

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{});
    stm.mutate_token_metadata([&] (token_metadata& tm) {
        auto& topo = tm.get_topology();
        generate_topology(topo, datacenters, nodes);

        const auto& address = nodes[tests::random::get_int<size_t>(0, NODES-1)];
        const auto& a1 = nodes[tests::random::get_int<size_t>(0, NODES-1)];
        const auto& a2 = nodes[tests::random::get_int<size_t>(0, NODES-1)];

        topo.test_compare_endpoints(address, address, address);
        topo.test_compare_endpoints(address, address, a1);
        topo.test_compare_endpoints(address, a1, address);
        topo.test_compare_endpoints(address, a1, a1);
        topo.test_compare_endpoints(address, a1, a2);
        topo.test_compare_endpoints(address, a2, a1);

        topo.test_compare_endpoints(bogus_address, bogus_address, bogus_address);
        topo.test_compare_endpoints(address, bogus_address, bogus_address);
        topo.test_compare_endpoints(address, a1, bogus_address);
        topo.test_compare_endpoints(address, bogus_address, a2);
        return make_ready_future<>();
    }).get();
}

// although not strictly related to network_topology_strategy
// the use of msg_addr is related to topology changes
SEASTAR_THREAD_TEST_CASE(test_msg_addr) {
    auto ip1 = gms::inet_address("127.0.0.1");
    auto host_id1 = locator::host_id::create_random_id();
    auto ip2 = gms::inet_address("127.0.0.2");
    auto host_id2 = locator::host_id::create_random_id();

    auto a1 = netw::msg_addr(ip1);
    auto a1_with_id1 = netw::msg_addr(ip1, host_id1);
    BOOST_REQUIRE_LT(a1, a1_with_id1);
    BOOST_REQUIRE_LE(a1, a1_with_id1);
    BOOST_REQUIRE_NE(a1, a1_with_id1);
    BOOST_REQUIRE_GE(a1_with_id1, a1);
    BOOST_REQUIRE_GT(a1_with_id1, a1);
    BOOST_REQUIRE((a1 <=> a1_with_id1) < 0);
    BOOST_REQUIRE((a1_with_id1 <=> a1) > 0);

    auto a1_with_id2 = netw::msg_addr(ip1, host_id2);
    BOOST_REQUIRE_NE(a1_with_id1, a1_with_id2);
    BOOST_REQUIRE((a1_with_id1 <=> a1_with_id2) != 0);
    BOOST_REQUIRE((a1_with_id1 <=> a1_with_id2) == (host_id1 <=> host_id2));

    auto a2 = netw::msg_addr(ip2);
    BOOST_REQUIRE_NE(a1, a2);
    BOOST_REQUIRE((a1 <=> a2) != 0);
    auto a2_with_id2 = netw::msg_addr(ip2, host_id2);
    BOOST_REQUIRE_NE(a2, a2_with_id2);
    BOOST_REQUIRE((a2 <=> a2_with_id2) != 0);
    BOOST_REQUIRE_NE(a1, a2_with_id2);
    BOOST_REQUIRE_NE(a1_with_id1, a2_with_id2);
    BOOST_REQUIRE((a1_with_id1 <=> a2_with_id2) != 0);
    BOOST_REQUIRE((a1 <=> a2) == (a1_with_id1 <=> a2_with_id2));

    std::vector<netw::msg_addr> addrs_vector = {
        a1, a1_with_id1, a1_with_id2,
        a2, a2_with_id2,
    };
    std::unordered_set<netw::msg_addr> addrs_set(addrs_vector.cbegin(), addrs_vector.cend());

    BOOST_REQUIRE_EQUAL(addrs_vector.size(), addrs_set.size());
    for (const auto& x : addrs_vector) {
        BOOST_REQUIRE(addrs_set.contains(x));
        BOOST_REQUIRE(addrs_set.erase(x));
    }
    BOOST_REQUIRE(addrs_set.empty());
}
