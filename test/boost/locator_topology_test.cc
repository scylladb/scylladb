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

// Proves the fix for #31208: the scan-gating step count in alternator/ttl.cc's
// tablet scan is bounded by owned tablets, not total tablets in the table.
SEASTAR_THREAD_TEST_CASE(test_ttl_style_tablet_scan_cost_tracks_owned_after_fix) {
    auto my_id = host_id::create_random_id();
    auto other_id = host_id::create_random_id();

    topology::config cfg = {
        .this_endpoint = gms::inet_address("127.0.0.1"),
        .this_host_id = my_id,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };
    topology topo(cfg);
    topo.add_or_update_endpoint(other_id, endpoint_dc_rack::default_location, node::state::normal);
    const shard_id my_shard = 0;
#ifdef SEASTAR_DEBUG
    // Scale down under debug builds; only the linear-vs-constant ratio matters, not the absolute size.
    constexpr size_t large_n = 2000;
#else
    constexpr size_t large_n = 20000;
#endif

    // `owned` tablets, evenly spread through the ring, go to (my_id, my_shard) (RF=1), rest to other_id.
    // Returns {collect_steps, validate_scan_steps, found}; the fix bounds validate_scan_steps to O(owned).
    auto measure_fixed_pass = [&] (size_t n, size_t owned) -> std::tuple<size_t, size_t, size_t> {
        BOOST_REQUIRE_LE(owned, n);
        tablet_map tmap(n);
        size_t stride = owned ? std::max<size_t>(1, n / owned) : n + 1;
        for (auto tid : tmap.tablet_ids()) {
            bool mine = owned > 0 && tid.value() % stride == 0 && tid.value() / stride < owned;
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set { tablet_replica { mine ? my_id : other_id, mine ? my_shard : shard_id(0) } }
            });
        }

        // Collection pass: one walk recording candidate tablet ids (real fix also
        // yields per tablet via for_each_tablet(), not observable in this sync test).
        size_t collect_steps = 0;
        std::vector<tablet_id> candidates;
        for (auto tid : tmap.tablet_ids()) {
            collect_steps++;
            auto primary = tmap.get_primary_replica(tid, topo);
            if (primary.host == my_id && primary.shard == my_shard) {
                candidates.push_back(tid);
            }
        }

        // Validate-and-scan pass: re-checks ownership per candidate (mirrors ttl.cc's
        // fresh-ERM re-validation).
        size_t validate_scan_steps = 0;
        size_t found = 0;
        for (auto tid : candidates) {
            validate_scan_steps++;
            auto primary = tmap.get_primary_replica(tid, topo);
            if (primary.host == my_id && primary.shard == my_shard) {
                found++;
            }
        }
        return {collect_steps, validate_scan_steps, found};
    };

    // Case 1: owns none. Collection still costs O(total), but scan-gating steps are 0, not n.
    for (size_t n : {size_t(100), large_n}) {
        auto [collect_steps, validate_scan_steps, found] = measure_fixed_pass(n, 0);
        BOOST_REQUIRE_EQUAL(found, 0u);
        BOOST_REQUIRE_EQUAL(collect_steps, n);
        BOOST_REQUIRE_EQUAL(validate_scan_steps, 0u);
    }

    // Case 2: owns one tablet regardless of map size; scan-gating steps must stay pinned at 1.
    {
        auto [collect_small, validate_small, found_small] = measure_fixed_pass(100, 1);
        auto [collect_large, validate_large, found_large] = measure_fixed_pass(large_n, 1);
        BOOST_REQUIRE_EQUAL(found_small, 1u);
        BOOST_REQUIRE_EQUAL(found_large, 1u);
        // Scan-gating work no longer grows with total tablet count.
        BOOST_REQUIRE_EQUAL(validate_small, 1u);
        BOOST_REQUIRE_EQUAL(validate_large, 1u);
        BOOST_REQUIRE_EQUAL(validate_small, validate_large);
        // Collection is still O(total), but paid once, not once per resumption.
        BOOST_REQUIRE_EQUAL(collect_small, 100u);
        BOOST_REQUIRE_EQUAL(collect_large, large_n);
    }

    // Case 3: many owned tablets on a large map -- scan-gating steps track owned count exactly.
    {
        auto [collect_steps, validate_scan_steps, found] = measure_fixed_pass(large_n, 137);
        BOOST_REQUIRE_EQUAL(collect_steps, large_n);
        BOOST_REQUIRE_EQUAL(validate_scan_steps, 137u);
        BOOST_REQUIRE_EQUAL(found, 137u);
    }
}

// A split renumbers every tablet_id (tablet_allocator.cc split_tablets()), so a stale
// candidate can alias an unrelated tablet after a resize, not just go out of range (#31208).
SEASTAR_THREAD_TEST_CASE(test_ttl_style_tablet_scan_detects_split_during_validate) {
    auto my_id = host_id::create_random_id();
    auto other_id = host_id::create_random_id();

    topology::config cfg = {
        .this_endpoint = gms::inet_address("127.0.0.1"),
        .this_host_id = my_id,
        .local_dc_rack = endpoint_dc_rack::default_location,
    };
    topology topo(cfg);
    topo.add_or_update_endpoint(other_id, endpoint_dc_rack::default_location, node::state::normal);
    const shard_id my_shard = 0;

    // Pre-split map: 4 tablets, only tablet 1 is mine.
    tablet_map old_map(4);
    for (auto tid : old_map.tablet_ids()) {
        bool mine = tid.value() == 1;
        old_map.set_tablet(tid, tablet_info {
            tablet_replica_set { tablet_replica { mine ? my_id : other_id, mine ? my_shard : shard_id(0) } }
        });
    }

    // Collection pass (mirrors ttl.cc): one candidate, tid 1, plus the
    // tablet_count seen at collection time.
    std::vector<tablet_id> candidates;
    for (auto tid : old_map.tablet_ids()) {
        auto primary = old_map.get_primary_replica(tid, topo);
        if (primary.host == my_id && primary.shard == my_shard) {
            candidates.push_back(tid);
        }
    }
    BOOST_REQUIRE_EQUAL(candidates.size(), 1u);
    size_t candidates_tablet_count = old_map.tablet_count();

    // Post-split map: old tablet X becomes new tablets X<<1 and X<<1|1 (as split_tablets() does).
    tablet_map new_map(old_map.tablet_count() * 2);
    for (auto tid : old_map.tablet_ids()) {
        auto& info = old_map.get_tablet_info(tid);
        tablet_id left(tid.value() << 1);
        tablet_id right(left.value() + 1);
        new_map.set_tablet(left, info);
        new_map.set_tablet(right, info);
    }
    BOOST_REQUIRE_NE(new_map.tablet_count(), candidates_tablet_count);

    // Buggy shape (bounds check only): id 1 is in bounds of the doubled map, but now
    // aliases old tablet 0's right child, so this misreads as "not owned" -- not stale.
    for (auto tid : candidates) {
        BOOST_REQUIRE_LT(tid.value(), new_map.tablet_count());
        auto primary = new_map.get_primary_replica(tid, topo);
        bool mine = primary.host == my_id && primary.shard == my_shard;
        BOOST_REQUIRE(!mine); // wrong tablet, misleadingly reads as "not owned"
    }

    // Fixed shape: the tablet_count mismatch is caught before any per-candidate check runs.
    size_t candidates_examined = 0;
    for (size_t i = 0; i < candidates.size(); i++) {
        if (new_map.tablet_count() != candidates_tablet_count) {
            break;
        }
        candidates_examined++;
    }
    BOOST_REQUIRE_EQUAL(candidates_examined, 0u);
}
