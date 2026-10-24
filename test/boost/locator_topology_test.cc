/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include <boost/test/unit_test.hpp>

#include <fmt/ranges.h>

#include <functional>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/closeable.hh>

#include "locator/types.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"

#include "locator/host_id.hh"
#include "locator/topology.hh"
#include "locator/load_sketch.hh"
#include "utils/log.hh"

extern logging::logger testlog;

using namespace locator;

SEASTAR_THREAD_TEST_CASE(test_add_node) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");
    auto id2 = host_id::create_random_id();
    auto id3 = host_id::create_random_id();

    topology::config cfg = {
        .this_endpoint = ep1,
        .this_host_id = id1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] noexcept {
        set_abort_on_internal_error(true);
    });

    std::unordered_set<std::reference_wrapper<const locator::node>> nodes;

    nodes.insert(std::cref(topo.add_node(id2, endpoint_dc_rack::default_location, node::state::normal)));
    nodes.insert(std::cref(topo.add_or_update_endpoint(id1, endpoint_dc_rack::default_location, node::state::normal)));

    BOOST_REQUIRE_THROW(topo.add_node(id2, endpoint_dc_rack::default_location, node::state::normal), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.add_node(id3, endpoint_dc_rack{}, node::state::normal), std::runtime_error);

    nodes.insert(std::cref(topo.add_node(id3, endpoint_dc_rack::default_location, node::state::normal)));

    topo.for_each_node([&] (const locator::node& node) {
        BOOST_REQUIRE(nodes.erase(std::cref(node)));
    });
    BOOST_REQUIRE(nodes.empty());

    topo.clear_gently().get();
}

SEASTAR_THREAD_TEST_CASE(test_moving) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");

    topology::config cfg = {
        .this_endpoint = ep1,
        .this_host_id = id1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    topo.add_or_update_endpoint(id1, endpoint_dc_rack::default_location, node::state::normal);

    BOOST_REQUIRE(topo.this_node()->topology() == &topo);

    topology topo2(std::move(topo));
    BOOST_REQUIRE(topo2.this_node()->topology() == &topo2);
    BOOST_REQUIRE(!topo.this_node());
    BOOST_REQUIRE(topo2.get_config() == cfg);

    topo = std::move(topo2);
    BOOST_REQUIRE(topo.this_node()->topology() == &topo);
    BOOST_REQUIRE(!topo2.this_node());
    BOOST_REQUIRE(topo.get_config() == cfg);
}

SEASTAR_THREAD_TEST_CASE(test_update_node) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");
    auto id2 = host_id::create_random_id();

    topology::config cfg = {
        .this_endpoint = ep1,
        .this_host_id = id1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] noexcept {
        set_abort_on_internal_error(true);
    });

    topo.add_or_update_endpoint(id1, endpoint_dc_rack::default_location, node::state::normal);

    auto node = const_cast<class node*>(topo.this_node());

    topo.update_node(*node, std::nullopt, std::nullopt, std::nullopt);

    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);

    BOOST_REQUIRE_THROW(topo.update_node(*node, host_id::create_null_id(),  std::nullopt, std::nullopt), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.update_node(*node, id2, std::nullopt, std::nullopt), std::runtime_error);
    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    BOOST_REQUIRE_EQUAL(topo.find_node(id2), nullptr);

    auto dc_rack1 = endpoint_dc_rack{"DC1", "RACK1"};
    topo.update_node(*node, std::nullopt, dc_rack1, std::nullopt);

    BOOST_REQUIRE(topo.get_location(id1) == dc_rack1);

    auto dc_rack2 = endpoint_dc_rack{"DC2", "RACK2"};
    topo.update_node(*node, std::nullopt, dc_rack2, std::nullopt);

    BOOST_REQUIRE(topo.get_location(id1) == dc_rack2);

    BOOST_REQUIRE_NE(node->get_state(), locator::node::state::being_decommissioned);
    topo.update_node(*node, std::nullopt, std::nullopt, locator::node::state::being_decommissioned);

    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::being_decommissioned);

    auto dc_rack3 = endpoint_dc_rack{"DC3", "RACK3"};
    // Note: engage state option, but keep node::state value the same
    // to reproduce #13502
    topo.update_node(*node, std::nullopt, dc_rack3, locator::node::state::being_decommissioned);

    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    BOOST_REQUIRE(topo.get_location(id1) == dc_rack3);
    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::being_decommissioned);
}

SEASTAR_THREAD_TEST_CASE(test_remove_endpoint) {
    using dc_endpoints_t = std::unordered_map<sstring, std::unordered_set<locator::host_id>>;
    using dc_racks_t = std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<locator::host_id>>>;
    using dcs_t = std::unordered_set<sstring>;

    const auto id1 = host_id::create_random_id();
    const auto ep1 = gms::inet_address("127.0.0.1");
    const auto id2 = host_id::create_random_id();
    const auto dc_rack1 = endpoint_dc_rack {
        .dc = "dc1",
        .rack = "rack1"
    };
    const auto dc_rack2 = endpoint_dc_rack {
        .dc = "dc1",
        .rack = "rack2"
    };

    topology::config cfg = {
        .this_endpoint = ep1,
        .this_host_id = id1,
        .local_dc_rack = dc_rack1
    };

    auto topo = topology(cfg);

    topo.add_or_update_endpoint(id1, dc_rack1, node::state::normal);
    topo.add_node(id2, dc_rack2, node::state::normal);

    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {id1, id2}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {id1}}, {"rack2", {id2}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));

    topo.remove_endpoint(id2);
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {id1}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {id1}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));

    // Local endpoint cannot be removed
    topo.remove_endpoint(id1);
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {id1}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {id1}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));
}

SEASTAR_THREAD_TEST_CASE(test_load_sketch) {
    inet_address ip1("192.168.0.1");
    inet_address ip2("192.168.0.2");
    inet_address ip3("192.168.0.3");

    auto host1 = host_id(utils::make_random_uuid());
    auto host2 = host_id(utils::make_random_uuid());
    auto host3 = host_id(utils::make_random_uuid());

    unsigned node1_shard_count = 7;
    unsigned node2_shard_count = 1;
    unsigned node3_shard_count = 3;

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
        topology::config{
            .this_endpoint = ip1,
            .this_host_id = host1,
            .local_dc_rack = locator::endpoint_dc_rack::default_location
        }
    });
    auto stop_stm = deferred_stop(stm);

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_topology(host1, locator::endpoint_dc_rack::default_location, node::state::normal, node1_shard_count);
        tm.update_topology(host2, locator::endpoint_dc_rack::default_location, node::state::normal, node2_shard_count);
        tm.update_topology(host3, locator::endpoint_dc_rack::default_location, node::state::normal, node3_shard_count);
        return make_ready_future<>();
    }).get();

    // Check that allocation is even when starting from empty state
    {
        auto tm = stm.get();
        load_sketch load(tm);
        load.populate().get();

        std::vector<unsigned> node1_shards(node1_shard_count, 0);
        std::vector<unsigned> node2_shards(node2_shard_count, 0);
        std::vector<unsigned> node3_shards(node3_shard_count, 0);

        for (unsigned i = 0; i < node1_shard_count * 3; ++i) {
            node1_shards[load.next_shard(host1, 1, service::default_target_tablet_size)] += 1;
        }
        for (unsigned i = 0; i < node2_shard_count * 3; ++i) {
            node2_shards[load.next_shard(host2, 1, service::default_target_tablet_size)] += 1;
        }
        for (unsigned i = 0; i < node3_shard_count * 3; ++i) {
            node3_shards[load.next_shard(host3, 1, service::default_target_tablet_size)] += 1;
        }

        for (unsigned i = 1; i < node1_shard_count; ++i) {
            BOOST_REQUIRE_EQUAL(node1_shards[i], node1_shards[0]);
        }
        for (unsigned i = 1; i < node2_shard_count; ++i) {
            BOOST_REQUIRE_EQUAL(node2_shards[i], node2_shards[0]);
        }
        for (unsigned i = 1; i < node3_shard_count; ++i) {
            BOOST_REQUIRE_EQUAL(node3_shards[i], node3_shards[0]);
        }
    }

    // Check that imbalance is reduced when starting from unbalanced prior state

    std::vector<unsigned> node3_shards(node3_shard_count, 0);

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tablet_metadata tab_meta;
        tablet_map tmap(4);

        auto tid = tmap.first_tablet();
        tmap.set_tablet(tid, tablet_info{{
                tablet_replica{host3, 2}
        }});
        node3_shards[2]++;

        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info{{
                tablet_replica{host3, 2}
        }});
        node3_shards[2]++;

        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info{{
                tablet_replica{host3, 2}
        }});
        node3_shards[2]++;

        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info{{
                tablet_replica{host3, 1}
        }});
        node3_shards[1]++;

        auto table = table_id(utils::make_random_uuid());
        tab_meta.set_tablet_map(table, std::move(tmap));
        tm.set_tablets(std::move(tab_meta));
        return make_ready_future<>();
    }).get();

    {
        auto tm = stm.get();
        load_sketch load(tm);
        load.populate().get();

        // host3 has max shard load of 3 and 3 shards, and 4 tablets allocated.
        // So to achieve even load we need to allocate 3 * 3 - 4 = 5 more tablets.
        for (int i = 0; i < 5; ++i) {
            auto s = load.next_shard(host3, 1, service::default_target_tablet_size);
            node3_shards[s] += 1;
        }

        for (unsigned i = 1; i < node3_shard_count; ++i) {
            BOOST_REQUIRE_EQUAL(node3_shards[i], node3_shards[0]);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_apply_group0_leader_shard_ratio) {
    // ratio=100 (default) and single-shard nodes are no-ops.
    for (unsigned shard = 0; shard < 4; ++shard) {
        BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, shard, 4, 100, 0), 100u);
    }
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 0, 1, 0, 0), 100u);

    // Unknown node capacity (0) is left alone rather than divided.
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(0, 0, 4, 0, 0), 0u);

    // ratio=50, floor below the resulting share: shard 0 gets half the uniform share;
    // the other 3 shards evenly absorb the rest. Total capacity is preserved.
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 0, 4, 50, 0), 50u);
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 1, 4, 50, 0), 116u);
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 2, 4, 50, 0), 116u);
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 3, 4, 50, 0), 116u);

    // ratio=0, floor (one tablet) below the uniform share: shard 0 is floored rather than
    // going to 0, and the rest absorb the difference. Total capacity is still preserved.
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 0, 4, 0, 10), 10u);
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 1, 4, 0, 10), 130u);
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 2, 4, 0, 10), 130u);
    BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, 3, 4, 0, 10), 130u);
    BOOST_REQUIRE_EQUAL(10u + 130u * 3, 100u * 4u);

    // Floor exceeds the uniform share: skew would be a no-op, so capacity stays uniform
    // for every shard (this is the one-tablet-size floor kicking in at low ratios).
    for (unsigned shard = 0; shard < 4; ++shard) {
        BOOST_REQUIRE_EQUAL(apply_group0_leader_shard_ratio(100, shard, 4, 0, 150), 100u);
    }
}

SEASTAR_THREAD_TEST_CASE(test_load_sketch_group0_leader_shard_ratio) {
    inet_address ip1("192.168.0.1");
    inet_address ip2("192.168.0.2");

    // host1 is the local node (this_host_id), i.e. the simulated group0 leader.
    auto host1 = host_id(utils::make_random_uuid());
    auto host2 = host_id(utils::make_random_uuid());

    unsigned shard_count = 4;

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
        topology::config{
            .this_endpoint = ip1,
            .this_host_id = host1,
            .local_dc_rack = locator::endpoint_dc_rack::default_location
        }
    });
    auto stop_stm = deferred_stop(stm);

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_topology(host1, locator::endpoint_dc_rack::default_location, node::state::normal, shard_count);
        tm.update_topology(host2, locator::endpoint_dc_rack::default_location, node::state::normal, shard_count);
        return make_ready_future<>();
    }).get();

    // Node capacity must be much larger than the target tablet size, or the
    // one-tablet-size floor (Bug C fix) makes the ratio skew a no-op.
    auto load_stats = make_lw_shared<locator::load_stats>();
    uint64_t node_capacity = service::default_target_tablet_size * 100;
    load_stats->capacity[host1] = node_capacity;
    load_stats->capacity[host2] = node_capacity;

    auto tm = stm.get();
    load_sketch load(tm, load_stats);
    load.set_group0_leader_shard_ratio(0);
    load.populate().get();

    std::vector<unsigned> host1_shards(shard_count, 0);
    std::vector<unsigned> host2_shards(shard_count, 0);

    for (unsigned i = 0; i < shard_count * 3; ++i) {
        host1_shards[load.next_shard(host1, 1, service::default_target_tablet_size)] += 1;
    }
    for (unsigned i = 0; i < shard_count * 3; ++i) {
        host2_shards[load.next_shard(host2, 1, service::default_target_tablet_size)] += 1;
    }

    // host1 is the local (group0-leader) node: its shard 0 is deweighted to a ratio of 0,
    // so it gets floored at roughly one tablet's worth of capacity and picks up far fewer
    // tablets than the other shards (but is not literally excluded).
    BOOST_REQUIRE_LT(host1_shards[0], host1_shards[1]);
    for (unsigned i = 2; i < shard_count; ++i) {
        BOOST_REQUIRE_LE(std::abs(int(host1_shards[i]) - int(host1_shards[1])), 1);
    }

    // host2 is not the local node, so the ratio has no effect on it: allocation stays uniform.
    for (unsigned i = 1; i < shard_count; ++i) {
        BOOST_REQUIRE_EQUAL(host2_shards[i], host2_shards[0]);
    }
}

SEASTAR_THREAD_TEST_CASE(test_left_node_is_kept_outside_dc) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");
    auto id2 = host_id::create_random_id();
    auto id3 = host_id::create_random_id();

    const auto dc_rack1 = endpoint_dc_rack {
        .dc = "dc1",
        .rack = "rack1"
    };

    topology::config cfg = {
        .this_endpoint = ep1,
        .local_dc_rack = dc_rack1
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] noexcept {
        set_abort_on_internal_error(true);
    });

    std::unordered_set<std::reference_wrapper<const locator::node>> nodes;

    nodes.insert(std::cref(topo.add_node(id2, dc_rack1, node::state::normal)));
    nodes.insert(std::cref(topo.add_node(id3, dc_rack1, node::state::left)));

    topo.for_each_node([&] (const locator::node& node) {
        BOOST_REQUIRE(node.host_id() != id3);
    });

    {
        auto *n = topo.find_node(id3);
        BOOST_REQUIRE(n);
        BOOST_REQUIRE(n->get_state() == locator::node::state::left);
    }

    // left nodes are not members.
    BOOST_REQUIRE(!topo.get_datacenter_endpoints().at(dc_rack1.dc).contains(id3));

    BOOST_REQUIRE(topo.get_datacenter(id3) == dc_rack1.dc);
    BOOST_REQUIRE(topo.get_rack(id3) == dc_rack1.rack);

    auto topo2 = topo.clone_gently().get();
    {
        auto *n = topo2.find_node(id3);
        BOOST_REQUIRE(n);
        BOOST_REQUIRE(n->get_state() == locator::node::state::left);
    }

    // Make the DC empty of nodes
    topo.remove_node(id1);
    topo.remove_node(id2);
    // Left node location is still known
    BOOST_REQUIRE(topo.get_datacenter(id3) == dc_rack1.dc);
    BOOST_REQUIRE(topo.get_rack(id3) == dc_rack1.rack);

    topo.clear_gently().get();
}
