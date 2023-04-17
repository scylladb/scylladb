/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>

#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>

#include "locator/types.hh"
#include "test/lib/scylla_test_case.hh"

#include "utils/fb_utilities.hh"
#include "locator/host_id.hh"
#include "locator/topology.hh"
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

    utils::fb_utilities::set_broadcast_address(ep1);
    topology::config cfg = {
        .this_host_id = id1,
        .this_endpoint = ep1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] {
        set_abort_on_internal_error(true);
    });

    std::unordered_set<const locator::node*> nodes;
    nodes.insert(topo.this_node());

    nodes.insert(topo.add_node(id2, ep2, endpoint_dc_rack::default_location, node::state::normal));

    BOOST_REQUIRE_THROW(topo.add_node(id1, ep1, endpoint_dc_rack::default_location, node::state::normal), std::runtime_error);
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


SEASTAR_THREAD_TEST_CASE(test_update_node) {
    auto id1 = host_id::create_random_id();
    auto ep1 = gms::inet_address("127.0.0.1");
    auto id2 = host_id::create_random_id();
    auto ep2 = gms::inet_address("127.0.0.2");
    auto ep3 = gms::inet_address("127.0.0.3");

    utils::fb_utilities::set_broadcast_address(ep1);
    topology::config cfg = {
        .this_host_id = host_id::create_null_id(),
        .this_endpoint = ep1,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };

    auto topo = topology(cfg);

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = seastar::defer([] {
        set_abort_on_internal_error(true);
    });

    auto node = topo.this_node();
    auto mutable_node = const_cast<locator::node*>(node);

    node = topo.update_node(mutable_node, id1, std::nullopt, std::nullopt, std::nullopt);
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

    BOOST_REQUIRE_NE(node->get_state(), locator::node::state::leaving);
    node = topo.update_node(mutable_node, std::nullopt, std::nullopt, std::nullopt, locator::node::state::leaving);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::leaving);

    auto dc_rack3 = endpoint_dc_rack{"DC3", "RACK3"};
    // Note: engage state option, but keep node::state value the same
    // to reproduce #13502
    node = topo.update_node(mutable_node, std::nullopt, ep3, dc_rack3, locator::node::state::leaving);
    mutable_node = const_cast<locator::node*>(node);
    BOOST_REQUIRE_EQUAL(topo.find_node(id1), node);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep1), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep2), nullptr);
    BOOST_REQUIRE_EQUAL(topo.find_node(ep3), node);
    BOOST_REQUIRE(topo.get_location(id1) == dc_rack3);
    BOOST_REQUIRE(topo.get_location(ep2) == endpoint_dc_rack::default_location);
    BOOST_REQUIRE(topo.get_location(ep3) == dc_rack3);
    BOOST_REQUIRE_EQUAL(node->get_state(), locator::node::state::leaving);

    // In state::left the ndoe will remain indexed only by its host_id
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

