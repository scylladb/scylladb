/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include <seastar/testing/thread_test_case.hh>
#include "utils/fb_utilities.hh"
#include "locator/token_metadata.hh"
#include "locator/simple_strategy.hh"

using namespace locator;

namespace {
    const auto ks_name = sstring("test-ks");

    endpoint_dc_rack get_dc_rack(inet_address) {
        return {
            .dc = "unk-dc",
            .rack = "unk-rack"
        };
    }

    token_metadata create_token_metadata(inet_address this_endpoint) {
        return token_metadata({
            topology::config {
                .this_host_id = host_id::create_random_id(),
                .this_endpoint = this_endpoint,
                .local_dc_rack = get_dc_rack(this_endpoint)
            }
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_first_node) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto e1 = inet_address("192.168.0.1");
    const auto t1 = dht::token::from_int64(1);
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "1"}
    });
    token_metadata.add_bootstrap_token(t1, e1);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(0), ks_name),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1), ks_name),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(2), ks_name),
        inet_address_vector_topology_change{e1});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_second_node) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto e1 = inet_address("192.168.0.1");
    const auto t1 = dht::token::from_int64(1);
    const auto e2 = inet_address("192.168.0.2");
    const auto t2 = dht::token::from_int64(100);
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    token_metadata.update_topology(e2, get_dc_rack(e2));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "1"}
    });
    token_metadata.update_normal_tokens({t1}, e1).get();
    token_metadata.add_bootstrap_token(t2, e2);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(0), ks_name),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1), ks_name),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(2), ks_name),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(100), ks_name),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(101), ks_name),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_with_replicas) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    token_metadata.update_topology(e2, get_dc_rack(e2));
    token_metadata.update_topology(e3, get_dc_rack(e3));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "2"}
    });
    token_metadata.update_normal_tokens({t1, t1000}, e2).get();
    token_metadata.update_normal_tokens({t10}, e3).get();
    token_metadata.add_bootstrap_token(t100, e1);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1), ks_name),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(2), ks_name),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(11), ks_name),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(100), ks_name),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(101), ks_name),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_leave_with_replicas) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    token_metadata.update_topology(e2, get_dc_rack(e2));
    token_metadata.update_topology(e3, get_dc_rack(e3));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "2"}
    });
    token_metadata.update_normal_tokens({t1, t1000}, e2).get();
    token_metadata.update_normal_tokens({t10}, e3).get();
    token_metadata.update_normal_tokens({t100}, e1).get();
    token_metadata.add_leaving_endpoint(e1);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1), ks_name),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(2), ks_name),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(11), ks_name),
        inet_address_vector_topology_change{e3});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(100), ks_name),
        inet_address_vector_topology_change{e3});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(101), ks_name),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_replace_with_replicas) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");
    const auto e4 = inet_address("192.168.0.4");
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    token_metadata.update_topology(e2, get_dc_rack(e2));
    token_metadata.update_topology(e3, get_dc_rack(e3));
    token_metadata.update_topology(e4, get_dc_rack(e4));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "2"}
    });
    token_metadata.update_normal_tokens({t1000}, e1).get();
    token_metadata.update_normal_tokens({t1, t100}, e2).get();
    token_metadata.update_normal_tokens({t10}, e3).get();
    token_metadata.add_replacing_endpoint(e3, e4);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(100), ks_name),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1000), ks_name),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1001), ks_name),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1), ks_name),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(2), ks_name),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(10), ks_name),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(11), ks_name),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_endpoints_for_reading_when_bootstrap_with_replicas) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    token_metadata.update_topology(e2, get_dc_rack(e2));
    token_metadata.update_topology(e3, get_dc_rack(e3));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "2"}
    });
    token_metadata.update_normal_tokens({t1, t1000}, e2).get();
    token_metadata.update_normal_tokens({t10}, e3).get();
    token_metadata.add_bootstrap_token(t100, e1);

    auto check_endpoints = [&](int64_t t, inet_address_vector_replica_set expected_replicas,
        seastar::compat::source_location sl = seastar::compat::source_location::current())
    {
        BOOST_TEST_INFO("line: " << sl.line());
        const auto expected_set = std::unordered_set<inet_address>(expected_replicas.begin(),
            expected_replicas.end());
        const auto actual_replicas = token_metadata.endpoints_for_reading(dht::token::from_int64(t), ks_name);
        BOOST_REQUIRE(actual_replicas.has_value());
        const auto actual_set = std::unordered_set<inet_address>(actual_replicas->begin(),
            actual_replicas->end());
        BOOST_REQUIRE_EQUAL(expected_set, actual_set);
    };

    auto check_no_endpoints = [&](int64_t t,
        seastar::compat::source_location sl = seastar::compat::source_location::current())
    {
        BOOST_TEST_INFO("line: " << sl.line());
        BOOST_REQUIRE(!token_metadata.endpoints_for_reading(dht::token::from_int64(t), ks_name).has_value());
    };

    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    check_no_endpoints(2);

    token_metadata.set_read_new(locator::token_metadata::read_new_t::yes);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();

    check_endpoints(2, {e3, e1});
    check_endpoints(10, {e3, e1});
    check_endpoints(11, {e1, e2});
    check_endpoints(100, {e1, e2});
    check_no_endpoints(101);
    check_no_endpoints(1001);
    check_no_endpoints(1);
}

SEASTAR_THREAD_TEST_CASE(test_replace_node_with_same_endpoint) {
    dc_rack_fn get_dc_rack_fn = get_dc_rack;

    const auto t1 = dht::token::from_int64(1);
    const auto e1 = inet_address("192.168.0.1");
    auto token_metadata = create_token_metadata(e1);
    token_metadata.update_topology(e1, get_dc_rack(e1));
    const auto replication_strategy = simple_strategy(replication_strategy_config_options {
        {"replication_factor", "2"}
    });
    token_metadata.update_normal_tokens({t1}, e1).get();
    token_metadata.add_replacing_endpoint(e1, e1);
    token_metadata.update_pending_ranges(replication_strategy, ks_name, get_dc_rack_fn).get();
    BOOST_REQUIRE_EQUAL(token_metadata.pending_endpoints_for(dht::token::from_int64(1), ks_name),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata.get_endpoint(t1), e1);
}
