/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <boost/test/tools/output_test_stream.hpp>
#include <chrono>
#include <fmt/ranges.h>
#include <seastar/util/closeable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include "locator/token_metadata.hh"
#include "locator/simple_strategy.hh"
#include "locator/everywhere_replication_strategy.hh"
#include <fmt/std.h>
#include "service/topology_state_machine.hh"

using namespace locator;

namespace {
    const auto ks_name = sstring("test-ks");

    host_id gen_id(int id) {
        return host_id{utils::UUID(0, id)};
    }

    endpoint_dc_rack get_dc_rack(host_id) {
        return {
            .dc = "unk-dc",
            .rack = "unk-rack"
        };
    }

    token_metadata::config create_token_metadata_config(host_id this_host_id) {
        return token_metadata::config{topology::config{
            .this_host_id = this_host_id,
            .local_dc_rack = get_dc_rack(this_host_id)
        }};
    }

    template <typename Strategy>
    mutable_static_erm_ptr create_erm(mutable_token_metadata_ptr tmptr, replication_strategy_config_options opts = {}) {
        dc_rack_fn get_dc_rack_fn = get_dc_rack;
        tmptr->update_topology_change_info(get_dc_rack_fn).get();
        auto& topo = tmptr->get_topology();
        auto strategy = seastar::make_shared<Strategy>(replication_strategy_params(opts, std::nullopt, std::nullopt), &topo);
        return calculate_vnode_effective_replication_map(std::move(strategy), tmptr).get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_pending_and_read_endpoints_for_everywhere_strategy) {
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);
    const auto t1 = dht::token::from_int64(10);
    const auto t2 = dht::token::from_int64(20);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id, get_dc_rack(e1_id), node::state::normal);
    token_metadata->update_topology(e2_id, get_dc_rack(e2_id), node::state::normal);
    token_metadata->update_normal_tokens({t1}, e1_id).get();
    token_metadata->add_bootstrap_token(t2, e2_id);
    token_metadata->set_read_new(token_metadata::read_new_t::yes);

    auto erm = create_erm<everywhere_replication_strategy>(token_metadata);
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(t2),
        host_id_vector_topology_change{e2_id});
    BOOST_REQUIRE_EQUAL(erm->get_replicas_for_reading(t2),
        (host_id_vector_replica_set{e2_id, e1_id}));
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_second_node) {
    const auto t1 = dht::token::from_int64(1);
    const auto t2 = dht::token::from_int64(100);
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id, get_dc_rack(e1_id), node::state::normal);
    token_metadata->update_topology(e2_id, get_dc_rack(e2_id), node::state::normal);
    token_metadata->update_normal_tokens({t1}, e1_id).get();
    token_metadata->add_bootstrap_token(t2, e2_id);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "1"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(0)),
        host_id_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1)),
        host_id_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(2)),
        host_id_vector_topology_change{e2_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(100)),
        host_id_vector_topology_change{e2_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(101)),
        host_id_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_bootstrap_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);
    const auto e3_id = gen_id(3);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id, get_dc_rack(e1_id), node::state::normal);
    token_metadata->update_topology(e2_id, get_dc_rack(e2_id), node::state::normal);
    token_metadata->update_topology(e3_id, get_dc_rack(e3_id), node::state::normal);
    token_metadata->update_normal_tokens({t1, t1000}, e2_id).get();
    token_metadata->update_normal_tokens({t10}, e3_id).get();
    token_metadata->add_bootstrap_token(t100, e1_id);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1)),
        host_id_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(2)),
        host_id_vector_topology_change{e1_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(11)),
        host_id_vector_topology_change{e1_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(100)),
        host_id_vector_topology_change{e1_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(101)),
        host_id_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_leave_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);
    const auto e3_id = gen_id(3);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id, get_dc_rack(e1_id), node::state::normal);
    token_metadata->update_topology(e2_id, get_dc_rack(e2_id), node::state::normal);
    token_metadata->update_topology(e3_id, get_dc_rack(e3_id), node::state::normal);
    token_metadata->update_normal_tokens({t1, t1000}, e2_id).get();
    token_metadata->update_normal_tokens({t10}, e3_id).get();
    token_metadata->update_normal_tokens({t100}, e1_id).get();
    token_metadata->add_leaving_endpoint(e1_id);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1)),
        host_id_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(2)),
        host_id_vector_topology_change{e2_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(11)),
        host_id_vector_topology_change{e3_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(100)),
        host_id_vector_topology_change{e3_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(101)),
        host_id_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_pending_endpoints_for_replace_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);
    const auto e3_id = gen_id(3);
    const auto e4_id = gen_id(4);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id, get_dc_rack(e1_id), node::state::normal);
    token_metadata->update_topology(e2_id, get_dc_rack(e2_id), node::state::normal);
    token_metadata->update_topology(e3_id, get_dc_rack(e3_id), node::state::normal);
    token_metadata->update_topology(e4_id, get_dc_rack(e4_id), node::state::normal);
    token_metadata->update_normal_tokens({t1000}, e1_id).get();
    token_metadata->update_normal_tokens({t1, t100}, e2_id).get();
    token_metadata->update_normal_tokens({t10}, e3_id).get();
    token_metadata->add_replacing_endpoint(e3_id, e4_id);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(100)),
        host_id_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1000)),
        host_id_vector_topology_change{});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1001)),
        host_id_vector_topology_change{e4_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1)),
        host_id_vector_topology_change{e4_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(2)),
        host_id_vector_topology_change{e4_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(10)),
        host_id_vector_topology_change{e4_id});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(11)),
        host_id_vector_topology_change{});
}

SEASTAR_THREAD_TEST_CASE(test_endpoints_for_reading_when_bootstrap_with_replicas) {
    const auto t1 = dht::token::from_int64(1);
    const auto t10 = dht::token::from_int64(10);
    const auto t100 = dht::token::from_int64(100);
    const auto t1000 = dht::token::from_int64(1000);
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);
    const auto e3_id = gen_id(3);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id, get_dc_rack(e1_id), node::state::normal);
    token_metadata->update_topology(e2_id, get_dc_rack(e2_id), node::state::normal);
    token_metadata->update_topology(e3_id, get_dc_rack(e3_id), node::state::normal);
    token_metadata->update_normal_tokens({t1, t1000}, e2_id).get();
    token_metadata->update_normal_tokens({t10}, e3_id).get();
    token_metadata->add_bootstrap_token(t100, e1_id);

    auto check_endpoints = [](mutable_static_erm_ptr erm, int64_t t,
        host_id_vector_replica_set expected_replicas,
        std::source_location sl = std::source_location::current())
    {
        BOOST_TEST_INFO("line: " << sl.line());
        const auto expected_set = std::unordered_set<locator::host_id>(expected_replicas.begin(),
            expected_replicas.end());
        const auto actual_replicas = erm->get_replicas_for_reading(dht::token::from_int64(t));
        const auto actual_set = std::unordered_set<locator::host_id>(actual_replicas.begin(),
            actual_replicas.end());
        BOOST_REQUIRE_EQUAL(expected_set, actual_set);
    };

    auto check_no_endpoints = [](mutable_static_erm_ptr erm, int64_t t,
        std::source_location sl = std::source_location::current())
    {
        BOOST_TEST_INFO("line: " << sl.line());
        BOOST_REQUIRE_EQUAL(erm->get_replicas_for_reading(dht::token::from_int64(t)),
                            erm->get_natural_replicas(dht::token::from_int64(t)));
    };

    {
        auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
        check_no_endpoints(erm, 2);
    }

    {
        token_metadata->set_read_new(locator::token_metadata::read_new_t::yes);
        auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});

        check_endpoints(erm, 2, {e3_id, e1_id});
        check_endpoints(erm, 10, {e3_id, e1_id});
        check_endpoints(erm, 11, {e1_id, e2_id});
        check_endpoints(erm, 100, {e1_id, e2_id});
        check_no_endpoints(erm, 101);
        check_no_endpoints(erm, 1001);
        check_no_endpoints(erm, 1);
    }
}

SEASTAR_THREAD_TEST_CASE(test_replace_node_with_same_endpoint) {
    const auto t1 = dht::token::from_int64(1);
    const auto e1_id1 = gen_id(1);
    const auto e1_id2 = gen_id(2);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id2);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto token_metadata = stm.make_token_metadata_ptr();
    token_metadata->update_topology(e1_id1, get_dc_rack(e1_id1), node::state::being_replaced);
    token_metadata->update_normal_tokens({t1}, e1_id1).get();

    token_metadata->update_topology(e1_id2, get_dc_rack(e1_id2), node::state::replacing);

    token_metadata->add_replacing_endpoint(e1_id1, e1_id2);

    auto erm = create_erm<simple_strategy>(token_metadata, {{"replication_factor", "2"}});
    BOOST_REQUIRE_EQUAL(erm->get_pending_replicas(dht::token::from_int64(1)),
        host_id_vector_topology_change{e1_id2});
    BOOST_REQUIRE_EQUAL(erm->get_natural_replicas(dht::token::from_int64(1)),
        host_id_vector_replica_set{e1_id1});
    BOOST_REQUIRE_EQUAL(token_metadata->get_endpoint(t1), e1_id1);
}

SEASTAR_THREAD_TEST_CASE(test_stale_version_notification) {
    const auto e1_id = gen_id(1);

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    stm.set_stall_detector_threshold(std::chrono::steady_clock::duration(std::chrono::milliseconds(10)));

    // hold the first version
    auto tm = stm.get();
    auto token_metadata = stm.make_token_metadata_ptr();
    stm.mutate_token_metadata([] (auto& md) {
        md.set_version(service::topology::version_t{42});
        return make_ready_future();
    }).get();

    seastar::sleep(std::chrono::milliseconds(20)).get(); // wait for the notification task to run

    boost::test_tools::output_test_stream my_stream;
    std::streambuf* oldCerr = std::cerr.rdbuf();
    std::cerr.rdbuf(my_stream.rdbuf());

    // free expired version
    tm = stm.get();

    std::cerr.rdbuf(oldCerr);

    BOOST_TEST(my_stream.str().find("topology version 0 held for") != std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(test_raw_token) {
    const auto t1 = dht::token::from_int64(1);
    const auto t2 = dht::token::from_int64(2);

    dht::raw_token_opt rt_opt;
    BOOST_REQUIRE(!rt_opt);
    rt_opt = dht::raw_token(t1);
    BOOST_REQUIRE(*rt_opt == t1);

    BOOST_REQUIRE(dht::raw_token() == dht::minimum_token());
    BOOST_REQUIRE(dht::raw_token() < dht::raw_token(dht::first_token()));
    BOOST_REQUIRE(dht::raw_token() < dht::first_token());
    BOOST_REQUIRE(dht::raw_token() < dht::maximum_token());

    auto rt1 = dht::raw_token(t1);
    BOOST_REQUIRE(bool(rt1));
    BOOST_REQUIRE(rt1 > dht::raw_token());
    BOOST_REQUIRE(rt1 > dht::minimum_token());
    BOOST_REQUIRE_EQUAL(rt1, t1);
    BOOST_REQUIRE(rt1 == t1);
    BOOST_REQUIRE(rt1 < t2);
    BOOST_REQUIRE(rt1 < dht::maximum_token());
}

// get_datacenter_token_owners() and get_datacenter_racks_token_owners()
// are cached by token_metadata. Verify that the caches are invalidated
// by the token_metadata mutators that can change them, and are not
// carried over stale by cloning.
SEASTAR_THREAD_TEST_CASE(test_datacenter_token_owners_track_mutations) {
    const auto e1_id = gen_id(1);
    const auto e2_id = gen_id(2);
    const auto e3_id = gen_id(3);
    const auto t1 = dht::token::from_int64(10);
    const auto t2 = dht::token::from_int64(20);
    const auto t3 = dht::token::from_int64(30);
    const endpoint_dc_rack dc1_rack1{"dc1", "rack1"};
    const endpoint_dc_rack dc1_rack2{"dc1", "rack2"};
    const endpoint_dc_rack dc2_rack1{"dc2", "rack1"};
    const endpoint_dc_rack dc2_rack2{"dc2", "rack2"};

    using dc_owners_map = std::unordered_map<sstring, std::unordered_set<host_id>>;
    using dc_racks_owners_map = std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<host_id>>>;

    semaphore sem(1);
    auto tm_cfg = create_token_metadata_config(e1_id);
    shared_token_metadata stm([&] () noexcept { return get_units(sem, 1); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);
    auto tmptr = stm.make_token_metadata_ptr();
    auto& tm = *tmptr;

    tm.update_topology(e1_id, dc1_rack1, node::state::normal);
    tm.update_topology(e2_id, dc1_rack2, node::state::normal);
    tm.update_normal_tokens({t1}, e1_id).get();
    tm.update_normal_tokens({t2}, e2_id).get();

    BOOST_REQUIRE(tm.get_datacenter_token_owners() == (dc_owners_map{{"dc1", {e1_id, e2_id}}}));
    BOOST_REQUIRE(tm.get_datacenter_racks_token_owners() == (dc_racks_owners_map{{"dc1", {{"rack1", {e1_id}}, {"rack2", {e2_id}}}}}));

    // A node that owns no tokens is not a token owner.
    tm.update_topology(e3_id, dc2_rack1, node::state::normal);
    BOOST_REQUIRE(tm.get_datacenter_token_owners() == (dc_owners_map{{"dc1", {e1_id, e2_id}}}));

    // Until it gets tokens.
    tm.update_normal_tokens({t3}, e3_id).get();
    BOOST_REQUIRE(tm.get_datacenter_token_owners() == (dc_owners_map{{"dc1", {e1_id, e2_id}}, {"dc2", {e3_id}}}));
    BOOST_REQUIRE(tm.get_datacenter_racks_token_owners() == (dc_racks_owners_map{
            {"dc1", {{"rack1", {e1_id}}, {"rack2", {e2_id}}}},
            {"dc2", {{"rack1", {e3_id}}}}}));

    // Moving a token owner to another DC/rack via update_topology().
    tm.update_topology(e2_id, dc2_rack2, std::nullopt);
    BOOST_REQUIRE(tm.get_datacenter_token_owners() == (dc_owners_map{{"dc1", {e1_id}}, {"dc2", {e2_id, e3_id}}}));
    BOOST_REQUIRE(tm.get_datacenter_racks_token_owners() == (dc_racks_owners_map{
            {"dc1", {{"rack1", {e1_id}}}},
            {"dc2", {{"rack1", {e3_id}}, {"rack2", {e2_id}}}}}));

    // Moving another token owner.
    tm.update_topology(e1_id, dc2_rack1);
    BOOST_REQUIRE(tm.get_datacenter_token_owners() == (dc_owners_map{{"dc2", {e1_id, e2_id, e3_id}}}));
    BOOST_REQUIRE(tm.get_datacenter_racks_token_owners() == (dc_racks_owners_map{
            {"dc2", {{"rack1", {e1_id, e3_id}}, {"rack2", {e2_id}}}}}));

    // A clone sees the same owners.
    auto clone = tm.clone_async().get();
    BOOST_REQUIRE(clone.get_datacenter_token_owners() == tm.get_datacenter_token_owners());
    BOOST_REQUIRE(clone.get_datacenter_racks_token_owners() == tm.get_datacenter_racks_token_owners());

    // Removing a token owner.
    tm.remove_endpoint(e3_id);
    BOOST_REQUIRE(tm.get_datacenter_token_owners() == (dc_owners_map{{"dc2", {e1_id, e2_id}}}));
    BOOST_REQUIRE(tm.get_datacenter_racks_token_owners() == (dc_racks_owners_map{
            {"dc2", {{"rack1", {e1_id}}, {"rack2", {e2_id}}}}}));

    // Mutating the original does not affect the clone.
    BOOST_REQUIRE(clone.get_datacenter_token_owners() == (dc_owners_map{{"dc2", {e1_id, e2_id, e3_id}}}));
    clone.clear_gently().get();
}
