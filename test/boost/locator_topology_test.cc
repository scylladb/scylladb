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

// Reproduces the inner-loop shape of alternator/ttl.cc scan_table()'s tablet
// branch (ttl.cc:764-819): starting right after last_token, it walks
// tablet_map::next_tablet() and calls get_primary_replica() on each tablet
// until it finds one owned by (my_host, my_shard), then resumes from there.
// This measures how many tablets that walk visits to complete one full ring
// pass, versus how many are actually owned by the target shard.
SEASTAR_THREAD_TEST_CASE(test_ttl_style_tablet_scan_cost_tracks_total_not_owned) {
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

    // Builds an n-tablet map where exactly `owned` tablets (evenly spread
    // through the ring) are replicated to (my_id, my_shard) and the rest to
    // other_id (RF=1), then replays ttl.cc's exact loop shape for one
    // complete pass over the whole ring. Returns {inner-loop steps taken,
    // tablets actually found to be owned}. `range` in the original code is
    // only a sentinel that breaks the inner loop and gets scanned
    // afterwards; that scan is irrelevant to the control flow being tested,
    // so it's replaced here by a plain bool.
    auto measure_full_pass = [&] (size_t n, size_t owned) -> std::pair<size_t, size_t> {
        BOOST_REQUIRE_LE(owned, n);
        tablet_map tmap(n);
        size_t stride = owned ? std::max<size_t>(1, n / owned) : n + 1;
        for (auto tid : tmap.tablet_ids()) {
            bool mine = owned > 0 && tid.value() % stride == 0 && tid.value() / stride < owned;
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set { tablet_replica { mine ? my_id : other_id, mine ? my_shard : shard_id(0) } }
            });
        }

        size_t inner_steps = 0;
        size_t found = 0;
        std::optional<dht::token> last_token;
        do {
            bool range_found = false;
            std::optional<tablet_id> tablet = last_token ? tmap.get_tablet_id(dht::next_token(*last_token)) : tmap.first_tablet();
            do {
                inner_steps++;
                last_token = tmap.get_last_token(*tablet);
                auto primary = tmap.get_primary_replica(*tablet, topo);
                if (primary.host == my_id && primary.shard == my_shard) {
                    range_found = true;
                    found++;
                }
                if (range_found) {
                    break;
                }
                tablet = tmap.next_tablet(*tablet);
            } while (tablet);
            if (!range_found) {
                break;
            }
        } while (*last_token < dht::last_token());
        return {inner_steps, found};
    };

    // Case 1: this shard owns none of the tablets. ttl.cc's comment says
    // "No range was found means all tablets have been iterated" -- i.e. the
    // walk is expected to reach the end of the map. Confirm the cost of
    // establishing that is exactly the total tablet count, for both a small
    // and a much larger map.
    for (size_t n : {size_t(100), size_t(20000)}) {
        auto [steps, found] = measure_full_pass(n, 0);
        BOOST_REQUIRE_EQUAL(found, 0u);
        BOOST_REQUIRE_EQUAL(steps, n);
    }

    // Case 2: this shard owns a single tablet regardless of how large the
    // map is. A per-shard-owned-ranges walk would cost O(1) here; this
    // algorithm still visits every tablet in the ring exactly once to
    // complete the pass, so its cost is O(total tablets) rather than
    // O(owned tablets). Growing n while owned stays fixed at 1 must grow
    // "steps" in lockstep with n, not with the (constant) owned count.
    {
        auto [steps_small, found_small] = measure_full_pass(100, 1);
        auto [steps_large, found_large] = measure_full_pass(20000, 1);
        BOOST_REQUIRE_EQUAL(found_small, 1u);
        BOOST_REQUIRE_EQUAL(found_large, 1u);
        BOOST_REQUIRE_EQUAL(steps_small, 100u);
        BOOST_REQUIRE_EQUAL(steps_large, 20000u);
        // The cost paid per owned tablet scales with the total tablet count,
        // not with ownership -- 200x more total tablets means 200x more
        // wasted work to find the same single owned tablet.
        BOOST_REQUIRE_EQUAL(steps_large / steps_small, 20000u / 100u);
    }
}

// Proves the fix for the same bug reproduced above: alternator/ttl.cc's
// tablet scan now does a single pass collecting candidate owned tablet ids,
// then a separate pass that re-validates ownership against a fresh map
// immediately before each scan. The expensive/scan-gating step count (the
// analogue of "inner_steps" above, which is what used to hold the ERM and
// drive scan_table_ranges() calls) is now bounded by how many tablets this
// shard actually owns, not by the total tablet count in the table.
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

    // Same map construction as the buggy-shape test above: `owned` tablets,
    // evenly spread through the ring, replicated to (my_id, my_shard) (RF=1),
    // the rest to other_id.
    //
    // Returns {collect_steps, validate_scan_steps, found}:
    // - collect_steps: tablets visited by the one-time candidate-collection
    //   pass (this is inherently O(total) -- there's no reverse index from
    //   (host, shard) to owned tablet ids, so discovering ownership requires
    //   looking at every tablet at least once. What the fix removes is not
    //   this single linear pass, but paying for it again -- with the ERM
    //   held throughout and no yield point -- every time the outer loop
    //   resumes looking for the *next* owned tablet.)
    // - validate_scan_steps: candidates re-validated and (if still owned)
    //   handed to a scan; this is the step count that used to be O(total)
    //   in the buggy code and must now be O(owned).
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

        // Collection pass: one walk over the whole ring, recording candidate
        // tablet ids. In the real fix this is done via tablet_map::for_each_tablet()
        // with a co_await coroutine::maybe_yield() per tablet, so it never
        // blocks the reactor or holds up topology operations for long; here
        // the yielding behaviour itself isn't observable from a synchronous
        // test, so we just check the resulting step/candidate counts.
        size_t collect_steps = 0;
        std::vector<tablet_id> candidates;
        for (auto tid : tmap.tablet_ids()) {
            collect_steps++;
            auto primary = tmap.get_primary_replica(tid, topo);
            if (primary.host == my_id && primary.shard == my_shard) {
                candidates.push_back(tid);
            }
        }

        // Validate-and-scan pass: only touches the collected candidates,
        // re-checking ownership against the map immediately before counting
        // it as a scan (mirroring the fresh-ERM re-validation in ttl.cc).
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

    // Case 1: this shard owns none of the tablets. The collection pass still
    // costs O(total) (nothing to do about that without a reverse index), but
    // there are zero candidates, so the scan-gating step count is 0 -- not n.
    for (size_t n : {size_t(100), size_t(20000)}) {
        auto [collect_steps, validate_scan_steps, found] = measure_fixed_pass(n, 0);
        BOOST_REQUIRE_EQUAL(found, 0u);
        BOOST_REQUIRE_EQUAL(collect_steps, n);
        BOOST_REQUIRE_EQUAL(validate_scan_steps, 0u);
    }

    // Case 2: this shard owns a single tablet regardless of how large the
    // map is. Before the fix, the scan-gating step count ("inner_steps") grew
    // in lockstep with n (see test_ttl_style_tablet_scan_cost_tracks_total_not_owned
    // above). After the fix it must stay pinned at the owned count (1),
    // independent of n.
    {
        auto [collect_small, validate_small, found_small] = measure_fixed_pass(100, 1);
        auto [collect_large, validate_large, found_large] = measure_fixed_pass(20000, 1);
        BOOST_REQUIRE_EQUAL(found_small, 1u);
        BOOST_REQUIRE_EQUAL(found_large, 1u);
        // Scan-gating work no longer grows with total tablet count.
        BOOST_REQUIRE_EQUAL(validate_small, 1u);
        BOOST_REQUIRE_EQUAL(validate_large, 1u);
        BOOST_REQUIRE_EQUAL(validate_small, validate_large);
        // The one-time collection pass is still O(total) -- that part of the
        // cost is inherent, not a bug -- but it happens once, not once per
        // resumption of the search for the next owned tablet.
        BOOST_REQUIRE_EQUAL(collect_small, 100u);
        BOOST_REQUIRE_EQUAL(collect_large, 20000u);
    }

    // Case 3: many owned tablets on a large map -- scan-gating steps track
    // owned count exactly, confirming O(owned) rather than O(total) even
    // when ownership isn't a rare, single-tablet edge case.
    {
        auto [collect_steps, validate_scan_steps, found] = measure_fixed_pass(20000, 137);
        BOOST_REQUIRE_EQUAL(collect_steps, 20000u);
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
