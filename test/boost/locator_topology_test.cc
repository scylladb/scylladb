/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <boost/test/unit_test.hpp>

#include <fmt/ranges.h>

#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>

#include "locator/types.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"

#include "locator/host_id.hh"
#include "locator/topology.hh"
#include "locator/load_sketch.hh"
#include "log.hh"

extern logging::logger testlog;

using namespace locator;

SEASTAR_THREAD_TEST_CASE(test_add_node) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");
    auto id2 = host_id::create_random_id();
    auto ep2 = gms::inet_address("127.0.0.2");
    auto id3 = host_id::create_random_id();
    auto ep3 = gms::inet_address("127.0.0.3");

    topology::config cfg = {
        .this_endpoint = ep1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] {
        set_abort_on_internal_error(true);
    });

    std::unordered_set<const locator::node*> nodes;

    nodes.insert(topo.add_node(id2, ep2, endpoint_dc_rack::default_location, node::state::normal));
    nodes.insert(topo.add_node(id1, ep1, endpoint_dc_rack::default_location, node::state::normal));

    BOOST_REQUIRE_THROW(topo.add_node(id1, ep2, endpoint_dc_rack::default_location, node::state::normal), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.add_node(id2, ep1, endpoint_dc_rack::default_location, node::state::normal), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.add_node(id2, ep2, endpoint_dc_rack::default_location, node::state::normal), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.add_node(id2, ep3, endpoint_dc_rack::default_location, node::state::normal), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.add_node(id3, ep3, endpoint_dc_rack{}, node::state::normal), std::runtime_error);

    nodes.insert(topo.add_node(id3, ep3, endpoint_dc_rack::default_location, node::state::normal));

    topo.for_each_node([&] (const locator::node* node) {
        BOOST_REQUIRE(nodes.erase(node));
    });
    BOOST_REQUIRE(nodes.empty());

    topo.clear_gently().get();
}

SEASTAR_THREAD_TEST_CASE(test_moving) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");

    topology::config cfg = {
        .this_endpoint = ep1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    topo.add_node(id1, ep1, endpoint_dc_rack::default_location, node::state::normal);

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
    auto ep2 = gms::inet_address("127.0.0.2");
    auto ep3 = gms::inet_address("127.0.0.3");

    topology::config cfg = {
        .this_endpoint = ep1,
        .this_host_id = id1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] {
        set_abort_on_internal_error(true);
    });

    topo.add_or_update_endpoint(id1, std::nullopt, endpoint_dc_rack::default_location, node::state::normal);

    auto node = topo.this_node();
    auto mutable_node = const_cast<locator::node*>(node);

    node = topo.update_node(mutable_node, std::nullopt, ep1, std::nullopt, std::nullopt);
    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    mutable_node = const_cast<locator::node*>(node);

    BOOST_REQUIRE_THROW(topo.update_node(mutable_node, host_id::create_null_id(), std::nullopt, std::nullopt, std::nullopt), std::runtime_error);
    BOOST_REQUIRE_THROW(topo.update_node(mutable_node, id2, std::nullopt, std::nullopt, std::nullopt), std::runtime_error);
    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    BOOST_REQUIRE_EQUAL(topo.find_node(id2), nullptr);

    node = topo.update_node(mutable_node, std::nullopt, ep2, std::nullopt, std::nullopt);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep1), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep2), node);

    auto dc_rack1 = endpoint_dc_rack{"DC1", "RACK1"};
    node = topo.update_node(mutable_node, std::nullopt, std::nullopt, dc_rack1, std::nullopt);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE(topo.get_location(id1) == dc_rack1);
    BOOST_REQUIRE(topo.get_location(ep1) == dc_rack1);

    auto dc_rack2 = endpoint_dc_rack{"DC2", "RACK2"};
    node = topo.update_node(mutable_node, std::nullopt, std::nullopt, dc_rack2, std::nullopt);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE(topo.get_location(id1) == dc_rack2);
    BOOST_REQUIRE(topo.get_location(ep1) == dc_rack2);

    BOOST_REQUIRE_NE(node->get_state(), locator::node::state::being_decommissioned);
    node = topo.update_node(mutable_node, std::nullopt, std::nullopt, std::nullopt, locator::node::state::being_decommissioned);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::being_decommissioned);

    auto dc_rack3 = endpoint_dc_rack{"DC3", "RACK3"};
    // Note: engage state option, but keep node::state value the same
    // to reproduce #13502
    node = topo.update_node(mutable_node, std::nullopt, ep3, dc_rack3, locator::node::state::being_decommissioned);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep1), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep2), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep3), node);
    BOOST_REQUIRE(topo.get_location(id1) == dc_rack3);
    BOOST_REQUIRE(topo.get_location(ep2) == endpoint_dc_rack::default_location);
    BOOST_REQUIRE(topo.get_location(ep3) == dc_rack3);
    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::being_decommissioned);

    // In state::left the node will remain indexed only by its host_id
    node = topo.update_node(mutable_node, std::nullopt, std::nullopt, std::nullopt, locator::node::state::left);
    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep1), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep2), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep3), nullptr);
    BOOST_REQUIRE(topo.get_location(id1) == dc_rack3);
    BOOST_REQUIRE(topo.get_location(ep2) == endpoint_dc_rack::default_location);
    BOOST_REQUIRE(topo.get_location(ep3) == endpoint_dc_rack::default_location);
    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::left);
}

SEASTAR_THREAD_TEST_CASE(test_add_or_update_by_host_id) {
    auto id1 = host_id::create_random_id();
    auto id2 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");

    // In this test we check that add_or_update_endpoint searches by host_id first.
    // We create two nodes, one matches by id, another - by ip,
    // and SCYLLA_ASSERT that add_or_update_endpoint updates the first.
    // We need to make the second node 'being_decommissioned', so that
    // it gets removed from ip index and we don't get the non-unique IP error.

    auto topo = topology({});
    //auto topo = topology({});
    topo.add_node(id1, gms::inet_address{}, endpoint_dc_rack::default_location, node::state::normal);
    topo.add_node(id2, ep1, endpoint_dc_rack::default_location, node::state::being_decommissioned);

    topo.add_or_update_endpoint(id1, ep1, std::nullopt, node::state::bootstrapping);

    auto* n = topo.find_node(id1);
    BOOST_REQUIRE_EQUAL(n->get_state(), node::state::bootstrapping);
    BOOST_REQUIRE_EQUAL(n->host_id(), id1);
    BOOST_REQUIRE_EQUAL(n->endpoint(), ep1);

    auto* n2 = topo.find_node(ep1);
    BOOST_REQUIRE_EQUAL(n, n2);

    auto* n3 = topo.find_node(id2);
    BOOST_REQUIRE_EQUAL(n3->get_state(), node::state::being_decommissioned);
    BOOST_REQUIRE_EQUAL(n3->host_id(), id2);
    BOOST_REQUIRE_EQUAL(n3->endpoint(), ep1);
}

SEASTAR_THREAD_TEST_CASE(test_remove_endpoint) {
    using dc_endpoints_t = std::unordered_map<sstring, std::unordered_set<inet_address>>;
    using dc_racks_t = std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>>;
    using dcs_t = std::unordered_set<sstring>;

    const auto id1 = host_id::create_random_id();
    const auto ep1 = gms::inet_address("127.0.0.1");
    const auto id2 = host_id::create_random_id();
    const auto ep2 = gms::inet_address("127.0.0.2");
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
        .local_dc_rack = dc_rack1
    };

    auto topo = topology(cfg);

    topo.add_node(id1, ep1, dc_rack1, node::state::normal);
    topo.add_node(id2, ep2, dc_rack2, node::state::normal);

    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {ep1, ep2}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {ep1}}, {"rack2", {ep2}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));

    topo.remove_endpoint(id2);
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {ep1}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {ep1}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));

    topo.remove_endpoint(id1);
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{}));
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
            .this_host_id = host1
        }
    });

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_host_id(host1, ip1);
        tm.update_host_id(host2, ip2);
        tm.update_host_id(host3, ip3);
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
            node1_shards[load.next_shard(host1)] += 1;
        }
        for (unsigned i = 0; i < node2_shard_count * 3; ++i) {
            node2_shards[load.next_shard(host2)] += 1;
        }
        for (unsigned i = 0; i < node3_shard_count * 3; ++i) {
            node3_shards[load.next_shard(host3)] += 1;
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
        tab_meta.set_tablet_map(table, tmap);
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
            auto s = load.next_shard(host3);
            node3_shards[s] += 1;
        }

        for (unsigned i = 1; i < node3_shard_count; ++i) {
            BOOST_REQUIRE_EQUAL(node3_shards[i], node3_shards[0]);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_left_node_is_kept_outside_dc) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");
    auto id2 = host_id::create_random_id();
    auto ep2 = gms::inet_address("127.0.0.2");
    auto id3 = host_id::create_random_id();
    auto ep3 = gms::inet_address("127.0.0.3");

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
    auto reset_on_internal_abort = seastar::defer([] {
        set_abort_on_internal_error(true);
    });

    std::unordered_set<const locator::node*> nodes;

    nodes.insert(topo.add_node(id2, ep2, dc_rack1, node::state::normal));
    nodes.insert(topo.add_node(id3, ep3, dc_rack1, node::state::left));

    topo.for_each_node([&] (const locator::node* node) {
        BOOST_REQUIRE(node->host_id() != id3);
    });

    {
        auto *n = topo.find_node(id3);
        BOOST_REQUIRE(n);
        BOOST_REQUIRE(n->get_state() == locator::node::state::left);
    }

    // left nodes are not members.
    BOOST_REQUIRE(!topo.get_datacenter_endpoints().at(dc_rack1.dc).contains(ep3));

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
