/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>

#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>

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

