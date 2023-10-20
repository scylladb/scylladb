/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include "locator/token_metadata.hh"
#include "locator/simple_strategy.hh"
#include "locator/everywhere_replication_strategy.hh"

using namespace locator;

namespace {
    const auto ks_name = sstring("test-ks");

    endpoint_dc_rack get_dc_rack(gms::inet_address) {
        return {
            .dc = "unk-dc",
            .rack = "unk-rack"
        };
    }

    mutable_token_metadata_ptr create_token_metadata(inet_address this_endpoint) {
        return make_lw_shared<token_metadata>(token_metadata::config {
            topology::config {
                .this_endpoint = this_endpoint,
                .this_cql_address = this_endpoint,
                .local_dc_rack = get_dc_rack(this_endpoint)
            }
        });
    }

    template <typename Strategy>
    mutable_vnode_erm_ptr create_erm(mutable_token_metadata_ptr tmptr, replication_strategy_config_options opts = {}) {
        dc_rack_fn<gms::inet_address> get_dc_rack_fn = get_dc_rack;
        tmptr->update_topology_change_info(get_dc_rack_fn).get();
        auto strategy = seastar::make_shared<Strategy>(std::move(opts));
        return calculate_effective_replication_map(std::move(strategy), std::move(tmptr)).get0();
    }
}

SEASTAR_THREAD_TEST_CASE(test_pending_and_read_endpoints_for_everywhere_strategy) {
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto t1 = dht::token::from_int64(10);
    const auto t2 = dht::token::from_int64(20);

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_topology(e2, get_dc_rack(e2));
    token_metadata->update_normal_tokens({t1}, e1).get();
    token_metadata->add_bootstrap_token(t2, e2);
    token_metadata->set_read_new(token_metadata::read_new_t::yes);

    auto erm = create_erm<everywhere_replication_strategy>(token_metadata);
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(t2),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(erm->get_endpoints_for_reading(t2),
        (inet_address_vector_replica_set{e2, e1}));
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_second_node) {
    const auto e1 = inet_address("192.168.0.1");
    const auto t1 = dht::token::from_int64(1);
    const auto e2 = inet_address("192.168.0.2");
    const auto t2 = dht::token::from_int64(100);

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_topology(e2, get_dc_rack(e2));
    token_metadata->update_normal_tokens({t1}, e1).get();
    token_metadata->add_bootstrap_token(t2, e2);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "1"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(0)),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1)),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(2)),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(100)),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(101)),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_topology(e2, get_dc_rack(e2));
    token_metadata->update_topology(e3, get_dc_rack(e3));
    token_metadata->update_normal_tokens({t1, t1000}, e2).get();
    token_metadata->update_normal_tokens({t10}, e3).get();
    token_metadata->add_bootstrap_token(t100, e1);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1)),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(2)),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(11)),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(100)),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(101)),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_leave_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_topology(e2, get_dc_rack(e2));
    token_metadata->update_topology(e3, get_dc_rack(e3));
    token_metadata->update_normal_tokens({t1, t1000}, e2).get();
    token_metadata->update_normal_tokens({t10}, e3).get();
    token_metadata->update_normal_tokens({t100}, e1).get();
    token_metadata->add_leaving_endpoint(e1);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1)),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(2)),
        inet_address_vector_topology_change{e2});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(11)),
        inet_address_vector_topology_change{e3});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(100)),
        inet_address_vector_topology_change{e3});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(101)),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_replace_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");
    const auto e4 = inet_address("192.168.0.4");

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_topology(e2, get_dc_rack(e2));
    token_metadata->update_topology(e3, get_dc_rack(e3));
    token_metadata->update_topology(e4, get_dc_rack(e4));
    token_metadata->update_normal_tokens({t1000}, e1).get();
    token_metadata->update_normal_tokens({t1, t100}, e2).get();
    token_metadata->update_normal_tokens({t10}, e3).get();
    token_metadata->add_replacing_endpoint(e3, e4);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(100)),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1000)),
        inet_address_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1001)),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1)),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(2)),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(10)),
        inet_address_vector_topology_change{e4});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(11)),
        inet_address_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_endpoints_for_reading_when_bootstrap_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1 = inet_address("192.168.0.1");
    const auto e2 = inet_address("192.168.0.2");
    const auto e3 = inet_address("192.168.0.3");

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_topology(e2, get_dc_rack(e2));
    token_metadata->update_topology(e3, get_dc_rack(e3));
    token_metadata->update_normal_tokens({t1, t1000}, e2).get();
    token_metadata->update_normal_tokens({t10}, e3).get();
    token_metadata->add_bootstrap_token(t100, e1);

    auto check_endpoints = [](mutable_vnode_erm_ptr erm, int64_t t,
        inet_address_vector_replica_set expected_replicas,
        seastar::compat::source_location sl = seastar::compat::source_location::current())
    {
        BOOST_TEST_INFO("line: " << sl.line());
        const auto expected_set = std::unordered_set<inet_address>(expected_replicas.begin(),
            expected_replicas.end());
        const auto actual_replicas = erm->get_endpoints_for_reading(dht::token::from_int64(t));
        const auto actual_set = std::unordered_set<inet_address>(actual_replicas.begin(),
            actual_replicas.end());
        BOOST_REQUIRE_EQUAL(expected_set, actual_set);
    };

    auto check_no_endpoints = [](mutable_vnode_erm_ptr erm, int64_t t,
        seastar::compat::source_location sl = seastar::compat::source_location::current())
    {
        BOOST_TEST_INFO("line: " << sl.line());
        BOOST_REQUIRE_EQUAL(erm->get_endpoints_for_reading(dht::token::from_int64(t)),
                            erm->get_natural_endpoints(dht::token::from_int64(t)));
    };

    {
        auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
        check_no_endpoints(erm, 2);
    }

    {
        token_metadata->set_read_new(locator::token_metadata::read_new_t::yes);
        auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});

        check_endpoints(erm, 2, {e3, e1});
        check_endpoints(erm, 10, {e3, e1});
        check_endpoints(erm, 11, {e1, e2});
        check_endpoints(erm, 100, {e1, e2});
        check_no_endpoints(erm, 101);
        check_no_endpoints(erm, 1001);
        check_no_endpoints(erm, 1);
    }
}

SEASTAR_THREAD_TEST_CASE(test_replace_node_with_same_endpoint) {
    const auto t1 = dht::token::from_int64(1);
    const auto e1 = inet_address("192.168.0.1");

    auto token_metadata = create_token_metadata(e1);
    token_metadata->update_topology(e1, get_dc_rack(e1));
    token_metadata->update_normal_tokens({t1}, e1).get();
    token_metadata->add_replacing_endpoint(e1, e1);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_endpoints(dht::token::from_int64(1)),
        inet_address_vector_topology_change{e1});
    BOOST_REQUIRE_EQUAL(token_metadata->get_endpoint(t1), e1);
}
