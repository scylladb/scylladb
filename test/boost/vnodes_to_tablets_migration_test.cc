/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "db/config.hh"
#include "test/lib/cql_test_env.hh"
#include "locator/tablets.hh"
#include "service/storage_service.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/token_metadata.hh"
#include "db/schema_tables.hh"
#include "dht/token.hh"
#include "utils/UUID_gen.hh"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

BOOST_AUTO_TEST_SUITE(vnodes_to_tablets_migration_test)

// Verify that the initial tablet map produced by prepare_for_tablets_migration()
// contains all vnode token boundaries, the MAX_TOKEN, and the pow2 boundaries
// needed for convergence to a uniform power-of-two layout.
//
// MAX_TOKEN is verified explicitly as a tablet map invariant: since tablets
// do not wrap around the token ring, the last tablet must always end at
// MAX_TOKEN. It does not matter where MAX_TOKEN came from. Depending on the
// vnode layout, MAX_TOKEN may already be present as a vnode boundary; with pow2
// pre-splitting, it is also the terminal pow2 boundary.
static future<> test_tablet_map_creation(std::vector<int64_t> tokens) {
    cql_test_config cfg;
    auto initial_token = fmt::format("{}", fmt::join(tokens, ", "));
    cfg.db_config->initial_token.set(std::move(initial_token));

    return do_with_cql_env_thread([tokens] (cql_test_env& e) {
        auto ks_name = sstring("test_migration_ks");
        e.execute_cql(format("CREATE KEYSPACE {} "
                "WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                "AND tablets = {{'enabled': false}}", ks_name)).get();

        e.execute_cql(format("CREATE TABLE {}.t (pk int PRIMARY KEY)", ks_name)).get();
        auto tid = e.local_db().find_schema(ks_name, "t")->id();

        e.get_storage_service().local().prepare_for_tablets_migration(ks_name).get();

        auto& stm = e.local_db().get_shared_token_metadata();
        auto& tmap = stm.get()->tablets().get_tablet_map(tid);

        // Collect actual tablet boundaries.
        std::set<dht::token> tablet_boundaries;
        for (size_t i = 0; i < tmap.tablet_count(); ++i) {
            tablet_boundaries.insert(tmap.get_last_token(locator::tablet_id(i)));
        }

        // Build expected vnode boundaries.
        std::set<dht::token> vnode_boundaries;
        for (const auto& t : tokens) {
            vnode_boundaries.insert(dht::token(t));
        }

        // Verify all vnode boundaries are present.
        std::vector<dht::token> missing_vnode;
        std::set_difference(vnode_boundaries.begin(), vnode_boundaries.end(),
                            tablet_boundaries.begin(), tablet_boundaries.end(),
                            std::back_inserter(missing_vnode));
        BOOST_REQUIRE_MESSAGE(missing_vnode.empty(),
            fmt::format("Vnode boundaries missing from tablet map: {}", fmt::join(missing_vnode, ", ")));

        // Verify MAX_TOKEN independently from the pow2-boundary check: ending
        // the tablet map at MAX_TOKEN is a tablet-map invariant, not just an
        // incidental pow2 boundary.
        BOOST_REQUIRE_EQUAL(tmap.get_last_token(tmap.last_tablet()), dht::last_token());

        // The tablet map must have a pow2 target set.
        BOOST_REQUIRE(tmap.is_converging_to_pow2());
        size_t P = tmap.target_pow2_tablet_count();
        BOOST_REQUIRE_MESSAGE(std::has_single_bit(P),
                fmt::format("target_pow2_tablet_count {} is not a power of two", P));
        BOOST_REQUIRE_MESSAGE(P < tmap.tablet_count(),
                fmt::format("target_pow2_tablet_count {} must be less than the current tablet count {}", P, tmap.tablet_count()));

        // Verify all pow2 boundaries are present.
        std::set<dht::token> pow2_boundaries;
        for (const auto& t : dht::get_uniform_tokens(P)) {
            pow2_boundaries.insert(dht::token(t));
        }

        std::vector<dht::token> missing_pow2;
        std::set_difference(pow2_boundaries.begin(), pow2_boundaries.end(),
                            tablet_boundaries.begin(), tablet_boundaries.end(),
                            std::back_inserter(missing_pow2));
        BOOST_REQUIRE_MESSAGE(missing_pow2.empty(),
            fmt::format("Pow2 boundaries missing from tablet map: {}", fmt::join(missing_pow2, ", ")));

        // Verify no spurious boundaries.
        std::set<dht::token> expected_boundaries;
        std::set_union(vnode_boundaries.begin(), vnode_boundaries.end(),
                       pow2_boundaries.begin(), pow2_boundaries.end(),
                       std::inserter(expected_boundaries, expected_boundaries.end()));
        expected_boundaries.insert(dht::last_token());

        std::vector<dht::token> extra;
        std::set_difference(tablet_boundaries.begin(), tablet_boundaries.end(),
                            expected_boundaries.begin(), expected_boundaries.end(),
                            std::back_inserter(extra));
        BOOST_REQUIRE_MESSAGE(extra.empty(),
            fmt::format("Unexpected boundaries in tablet map: {}", fmt::join(extra, ", ")));
    }, cfg);
}

// Verify tablet map creation with unaligned last token.
SEASTAR_TEST_CASE(test_tablet_map_creation_unaligned_last_token) {
    return test_tablet_map_creation({-7686143364045646507, 0, 7158264828641642373}); // some random tokens
}

// Verify tablet map creation with aligned last token.
SEASTAR_TEST_CASE(test_tablet_map_creation_aligned_last_token) {
    return test_tablet_map_creation({-7686143364045646507, 0, dht::last_token().raw()});
}

// Verify that pow2 convergence is skipped if the input vnode tokens already
// form a power-of-two tablet layout and the vnode count is smaller than the target pow2.
// This can happen only when vnode tokens are explicitly provided via --initial-token.
SEASTAR_TEST_CASE(test_tablet_map_creation_already_pow2_layout) {
    std::vector<int64_t> tokens;
    int vnode_count = 4;
    for (const auto& t : dht::get_uniform_tokens(vnode_count)) {
        tokens.push_back(dht::token(t).raw());
    }

    cql_test_config cfg;
    auto initial_token = fmt::format("{}", fmt::join(tokens, ", "));
    cfg.db_config->initial_token.set(std::move(initial_token));
    // Set scale factor to a high-enough value to ensure that the target pow2
    // is not smaller than the vnode count. This produces an initial tablet map
    // with no convergence needed.
    cfg.db_config->tablets_initial_scale_factor.set(vnode_count * 2);

    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto ks_name = sstring("test_migration_ks");
        e.execute_cql(format("CREATE KEYSPACE {} "
                "WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                "AND tablets = {{'enabled': false}}", ks_name)).get();

        e.execute_cql(format("CREATE TABLE {}.t (pk int PRIMARY KEY)", ks_name)).get();
        auto tid = e.local_db().find_schema(ks_name, "t")->id();

        e.get_storage_service().local().prepare_for_tablets_migration(ks_name).get();

        auto& stm = e.local_db().get_shared_token_metadata();
        auto& tmap = stm.get()->tablets().get_tablet_map(tid);

        BOOST_REQUIRE(tmap.get_layout() == locator::tablet_layout::pow_of_2);
        BOOST_REQUIRE(!tmap.is_converging_to_pow2());
        BOOST_REQUIRE_EQUAL(tmap.target_pow2_tablet_count(), 0);
    }, cfg);
}

namespace {

// Builds a single-node topology with the given shard count, places vnode tokens at
// the given positions in unbiased token space, and runs the migration tablet map
// builder over it.
//
// target_pow2 is 0, so the only boundary added on top of the vnode tokens is the
// maximum token. The tablets are therefore exactly the ranges the caller laid out,
// which is what makes the resulting assignment predictable by hand.
static locator::tablet_map build_map(const std::vector<locator::host_id>& hosts, unsigned shard_count,
                                     const std::vector<std::vector<uint64_t>>& vnode_positions_of,
                                     size_t rf) {
    locator::token_metadata::config tm_cfg;
    tm_cfg.topo_cfg.this_host_id = hosts[0];
    tm_cfg.topo_cfg.local_dc_rack = locator::endpoint_dc_rack::default_location;
    locator::shared_token_metadata stm([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);

    stm.mutate_token_metadata([&] (locator::token_metadata& tm) -> future<> {
        for (size_t n = 0; n < hosts.size(); ++n) {
            std::unordered_set<dht::token> tokens;
            for (auto pos : vnode_positions_of[n]) {
                tokens.insert(dht::bias(pos));
            }
            tm.get_topology().add_or_update_endpoint(hosts[n], locator::endpoint_dc_rack::default_location,
                                                     locator::node::state::normal, shard_count);
            co_await tm.update_normal_tokens(tokens, hosts[n]);
        }
    }).get();

    std::map<sstring, locator::replication_strategy_config_option> options = {
        { locator::endpoint_dc_rack::default_location.dc, fmt::to_string(rf) },
    };
    locator::replication_strategy_params params(options, std::nullopt, std::nullopt);
    auto rs = locator::abstract_replication_strategy::create_replication_strategy(
            "NetworkTopologyStrategy", params, stm.get()->get_topology());
    auto erm = locator::calculate_vnode_effective_replication_map(rs, stm.get()).get();

    return service::storage_service::build_tablet_map_for_migration(stm.get(), erm, 0).get();
}

static locator::tablet_map build_single_node_map(locator::host_id host, unsigned shard_count,
                                                 const std::vector<uint64_t>& vnode_positions) {
    return build_map({host}, shard_count, {vnode_positions}, 1);
}

// With RF=1 on a single node every tablet has exactly one replica, so the mapping
// from tablet to shard is unambiguous.
static void check_shard_mapping(const locator::tablet_map& tmap, locator::host_id host,
                                const std::vector<shard_id>& expected) {
    BOOST_REQUIRE_EQUAL(tmap.tablet_count(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        const auto& replicas = tmap.get_tablet_info(locator::tablet_id(i)).replicas;
        BOOST_REQUIRE_EQUAL(replicas.size(), 1);
        BOOST_REQUIRE_EQUAL(replicas[0].host, host);
        BOOST_CHECK_EQUAL(replicas[0].shard, expected[i]);
    }
}

// One hundredth of the ring, the unit the cases below express tablet widths in.
constexpr uint64_t ring_percent = std::numeric_limits<uint64_t>::max() / 100;

} // anonymous namespace

// Six tablets of uneven width over four shards.
//
// The widths are deliberately not in descending token order, so the assignment has
// to reorder them rather than just walking the map.
SEASTAR_THREAD_TEST_CASE(test_shard_assignment_maps_six_tablets_onto_four_shards) {
    auto host = locator::host_id(utils::UUID_gen::get_time_UUID());

    // Tablet widths in token order, in hundredths of the ring:
    //   t0=15  t1=30  t2=4  t3=20  t4=25  t5=the remainder, just over 6
    auto tmap = build_single_node_map(host, 4, {
        15 * ring_percent, 45 * ring_percent, 49 * ring_percent,
        69 * ring_percent, 94 * ring_percent,
    });

    // Largest tablet first onto the shard owning the least so far, ties going to
    // the lowest shard id. Shard loads after each placement:
    //   t1(30) -> shard 0   [30,  0,  0,  0]
    //   t4(25) -> shard 1   [30, 25,  0,  0]
    //   t3(20) -> shard 2   [30, 25, 20,  0]
    //   t0(15) -> shard 3   [30, 25, 20, 15]
    //   t5( 6) -> shard 3   [30, 25, 20, 21]
    //   t2( 4) -> shard 2   [30, 25, 24, 21]
    check_shard_mapping(tmap, host, { 3, 0, 2, 2, 1, 3 });
}

// Eight tablets over four shards, chosen so that the assignment can reach a
// perfectly even split: every shard ends up owning a quarter of the ring.
SEASTAR_THREAD_TEST_CASE(test_shard_assignment_maps_eight_tablets_evenly) {
    auto host = locator::host_id(utils::UUID_gen::get_time_UUID());

    // Tablet widths in token order, in hundredths of the ring:
    //   t0=5  t1=22  t2=3  t3=18  t4=12  t5=25  t6=7  t7=the remainder, just over 8
    auto tmap = build_single_node_map(host, 4, {
        5 * ring_percent, 27 * ring_percent, 30 * ring_percent, 48 * ring_percent,
        60 * ring_percent, 85 * ring_percent, 92 * ring_percent,
    });

    //   t5(25) -> shard 0   [25,  0,  0,  0]
    //   t1(22) -> shard 1   [25, 22,  0,  0]
    //   t3(18) -> shard 2   [25, 22, 18,  0]
    //   t4(12) -> shard 3   [25, 22, 18, 12]
    //   t7( 8) -> shard 3   [25, 22, 18, 20]
    //   t6( 7) -> shard 2   [25, 22, 25, 20]
    //   t0( 5) -> shard 3   [25, 22, 25, 25]
    //   t2( 3) -> shard 1   [25, 25, 25, 25]
    check_shard_mapping(tmap, host, { 3, 1, 1, 2, 3, 0, 2, 3 });
}

// Three nodes at RF=2, so every tablet lands on two of them and each node runs its
// own assignment over the subset it replicates.
//
// Tokens are laid out so the nodes own interleaved parts of the ring:
//   node A holds 10 and 70, node B holds 25, node C holds 45
// which gives five tablets of width 10, 15, 20, 25 and 30 hundredths, and replica
// sets that differ from tablet to tablet.
SEASTAR_THREAD_TEST_CASE(test_shard_assignment_across_nodes_with_replicas) {
    std::vector<locator::host_id> hosts;
    for (int i = 0; i < 3; ++i) {
        hosts.push_back(locator::host_id(utils::UUID_gen::get_time_UUID()));
    }

    auto tmap = build_map(hosts, 2, {
        { 10 * ring_percent, 70 * ring_percent },   // A
        { 25 * ring_percent },                      // B
        { 45 * ring_percent },                      // C
    }, 2);

    BOOST_REQUIRE_EQUAL(tmap.tablet_count(), 5);

    // Tablets are offered largest first, and each host picks independently from its
    // own shards. Walking the global order, with the per-host shard loads alongside:
    //
    //   t4(30) on A,B   A[30, 0]  B[30, 0]
    //   t3(25) on A,B   A[30,25]  B[30,25]
    //   t2(20) on C,A   C[20, 0]  A[30,45]
    //   t1(15) on B,C   B[30,40]  C[20,15]
    //   t0(10) on A,B   A[40,45]  B[40,40]
    //
    // A ends up at [40,45], B at [40,40], C at [20,15]: every host balanced within
    // itself, which is the point -- the assignment is per host, not global.
    const std::vector<std::vector<std::pair<size_t, shard_id>>> expected = {
        { {0, 0}, {1, 0} },   // t0 -> A:0  B:0
        { {1, 1}, {2, 1} },   // t1 -> B:1  C:1
        { {2, 0}, {0, 1} },   // t2 -> C:0  A:1
        { {0, 1}, {1, 1} },   // t3 -> A:1  B:1
        { {0, 0}, {1, 0} },   // t4 -> A:0  B:0
    };

    for (size_t i = 0; i < expected.size(); ++i) {
        std::vector<std::pair<size_t, shard_id>> got;
        for (const auto& replica : tmap.get_tablet_info(locator::tablet_id(i)).replicas) {
            auto it = std::ranges::find(hosts, replica.host);
            BOOST_REQUIRE(it != hosts.end());
            got.emplace_back(std::distance(hosts.begin(), it), replica.shard);
        }
        auto want = expected[i];
        std::ranges::sort(got);
        std::ranges::sort(want);
        BOOST_CHECK_MESSAGE(got == want,
                fmt::format("tablet {}: expected {}, got {}", i, want, got));
    }
}

BOOST_AUTO_TEST_SUITE_END()
