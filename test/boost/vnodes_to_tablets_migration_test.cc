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

// One node in a synthetic topology: where it sits, and which vnode tokens it owns.
//
// Positions are in unbiased token space, so `ring_percent` below expresses them as
// hundredths of the ring.
struct node_spec {
    locator::host_id host;
    locator::endpoint_dc_rack location = locator::endpoint_dc_rack::default_location;
    std::vector<uint64_t> vnode_positions;
};

// Builds a topology from `nodes`, with `rf_per_dc` giving each DC's replication
// factor, and invokes `func` with the resulting effective replication map and the
// tablet-aware view of the replication strategy.
//
// The ERM points into the token metadata built here, so it is passed to `func`
// rather than returned: everything stays on this frame and is torn down in order
// once `func` is done.
//
// `local_host_idx` selects which node the callee should see itself as. Both the host
// id and the location come from that one node, so the "local" node cannot drift from
// the DC and rack the caller intended it to be in.
//
// Must run in a seastar thread.
static void with_topology(
        const std::vector<node_spec>& nodes,
        const std::map<sstring, size_t>& rf_per_dc,
        unsigned shard_count,
        size_t local_host_idx,
        std::function<void(const locator::static_effective_replication_map_ptr&,
                           const locator::tablet_aware_replication_strategy*)> func) {
    BOOST_REQUIRE(local_host_idx < nodes.size());

    locator::token_metadata::config tm_cfg;
    tm_cfg.topo_cfg.this_host_id = nodes[local_host_idx].host;
    tm_cfg.topo_cfg.local_dc_rack = nodes[local_host_idx].location;
    locator::shared_token_metadata stm([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg);
    auto stop_stm = deferred_stop(stm);

    stm.mutate_token_metadata([&] (locator::token_metadata& tm) -> future<> {
        for (const auto& node : nodes) {
            std::unordered_set<dht::token> tokens;
            for (auto pos : node.vnode_positions) {
                tokens.insert(dht::bias(pos));
            }
            tm.get_topology().add_or_update_endpoint(node.host, node.location,
                                                     locator::node::state::normal, shard_count);
            co_await tm.update_normal_tokens(tokens, node.host);
        }
    }).get();

    std::map<sstring, locator::replication_strategy_config_option> options;
    for (const auto& [dc, rf] : rf_per_dc) {
        options.emplace(dc, fmt::to_string(rf));
    }
    locator::replication_strategy_params params(options, std::nullopt, std::nullopt);
    auto rs = locator::abstract_replication_strategy::create_replication_strategy(
            "NetworkTopologyStrategy", params, stm.get()->get_topology());
    auto erm = locator::calculate_vnode_effective_replication_map(rs, stm.get()).get();

    // Same cast as in prepare_for_tablets_migration().
    const auto* trs = dynamic_cast<const locator::tablet_aware_replication_strategy*>(&*rs);
    BOOST_REQUIRE(trs);

    func(erm, trs);
}

// Builds a single-DC topology with the given shard count, places vnode tokens at
// the given positions in unbiased token space, and runs the migration tablet map
// builder over it.
//
// target_pow2 is 0, so the only boundary added on top of the vnode tokens is the
// maximum token. The tablets are therefore exactly the ranges the caller laid out,
// which is what makes the resulting assignment predictable by hand.
static locator::tablet_map build_map(const std::vector<locator::host_id>& hosts, unsigned shard_count,
                                     const std::vector<std::vector<uint64_t>>& vnode_positions_of,
                                     size_t rf) {
    std::vector<node_spec> nodes;
    for (size_t n = 0; n < hosts.size(); ++n) {
        nodes.push_back({.host = hosts[n], .vnode_positions = vnode_positions_of[n]});
    }

    // The tablet map is a value that does not point back at the token metadata, so
    // unlike the ERM it can safely outlive with_topology().
    std::optional<locator::tablet_map> tmap;
    with_topology(nodes, {{locator::endpoint_dc_rack::default_location.dc, rf}}, shard_count, 0,
            [&] (const auto& erm, const auto*) {
        tmap = service::storage_service::build_tablet_map_for_migration(erm, 0).get();
    });
    return std::move(*tmap);
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

// Tests for storage_service::collect_table_sizes_for_migration():

namespace {

constexpr const char* estimate_ks = "test_migration_ks";
constexpr const char* estimate_cf = "t";

table_id create_vnode_table(cql_test_env& e) {
    e.execute_cql(format("CREATE KEYSPACE {} "
            "WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
            "AND tablets = {{'enabled': false}}", estimate_ks)).get();
    e.execute_cql(format("CREATE TABLE {}.{} (pk int PRIMARY KEY)", estimate_ks, estimate_cf)).get();
    return e.local_db().find_schema(estimate_ks, estimate_cf)->id();
}

// Fakes the table's per-shard on-disk size and returns the total across all shards.
//
// Note: This must not be followed by a flush or compaction: table::rebuild_statistics()
//       recomputes live_disk_space_used from the real compaction groups, which
//       would discard these values.
uint64_t set_local_table_size(cql_test_env& e, uint64_t size_per_shard) {
    e.db().invoke_on_all([size_per_shard] (replica::database& db) {
        auto& cf = db.find_column_family(estimate_ks, estimate_cf);
        cf.get_stats().live_disk_space_used.on_disk = int64_t(size_per_shard);
    }).get();

    return size_per_shard * this_smp_shard_count();
}

// Runs the estimator over the given topology and returns the estimated size.
uint64_t estimate_table_size(cql_test_env& e, table_id tid,
                              const locator::static_effective_replication_map_ptr& erm,
                              const locator::tablet_aware_replication_strategy* trs) {
    auto sizes = e.get_storage_service().local().collect_table_sizes_for_migration(
            estimate_ks, erm, trs, {{tid, estimate_cf}}).get();
    auto it = sizes.find(tid);
    BOOST_REQUIRE(it != sizes.end());
    return it->second;
}

// Asserts the estimate matches `expected`, within a small relative tolerance.
void check_estimate(uint64_t estimate, uint64_t expected, const sstring& context = {}) {
    BOOST_CHECK_MESSAGE(std::abs(double(estimate) - double(expected)) <= double(expected) * 0.00001,
            fmt::format("expected {}, got {}{}", expected, estimate,
                        context.empty() ? std::string() : fmt::format(" ({})", context)));
}

node_spec make_node(const sstring& dc, const sstring& rack, std::vector<uint64_t> vnode_positions) {
    return node_spec{
        .host = locator::host_id(utils::UUID_gen::get_time_UUID()),
        .location = locator::endpoint_dc_rack{dc, rack},
        .vnode_positions = std::move(vnode_positions),
    };
}

} // anonymous namespace

// Reproducer for https://scylladb.atlassian.net/browse/SCYLLADB-3736
// Table statistics are per-shard, but `collect_table_sizes_for_migration()`
// was reading the size of the local dataset from shard 0 alone, so it came out
// roughly a factor of smp too small. It should be summing over all shards.
//
// Test this on a single-node cluster with multiple shards and a dataset that
// is evenly distributed across shards.
SEASTAR_TEST_CASE(test_table_size_estimate_singlenode_many_shards) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        BOOST_REQUIRE_MESSAGE(this_smp_shard_count() > 1,
                "needs more than one shard to make sure all shards are summed");

        auto tid = create_vnode_table(e);
        auto local_size = set_local_table_size(e, 1 * 1024 * 1024);

        auto node = make_node("dc1", "rack1", {100 * ring_percent});
        with_topology({node}, {{"dc1", 1}}, this_smp_shard_count(), 0, [&] (const auto& erm, const auto* trs) {
            auto expected_global_size = local_size;
            auto estimated_global_size = estimate_table_size(e, tid, erm, trs);
            check_estimate(estimated_global_size, expected_global_size);
        });
    });
}

// Reproducer for https://scylladb.atlassian.net/browse/SCYLLADB-3760
// `collect_table_sizes_for_migration()` provided bad estimates in case of
// racks with different number of nodes. The formula was:
//
//   local_size / (local_fraction * local_rf)
//
// where
//   local_size: the size of the local dataset,
//   local_fraction: this node's primary token ownership fraction,
//   local_rf: the replication factor of the local datacenter.
//
// Consider a cluster with 2 racks, with 1 and 2 nodes respectively, and RF=2.
// The node in the small rack holds 100% of the data, but above formula would
// give us: local_fraction * local_rf = 1/3 * 2 = 2/3 = 66%, an underestimate.
//
// Test the above cluster setup with a uniformly distributed dataset and token
// ring. Expect the single-node rack to store 100% of the dataset.
SEASTAR_TEST_CASE(test_table_size_estimate_singledc_uneven_racks) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto tid = create_vnode_table(e);
        auto local_size = set_local_table_size(e, 1 * 1024 * 1024);

        // rack1 holds the local node, rack2 holds two, so primary ownership is a third each.
        std::vector<node_spec> nodes{
            make_node("dc1", "rack1", {33 * ring_percent}),
            make_node("dc1", "rack2", {66 * ring_percent}),
            make_node("dc1", "rack2", {100 * ring_percent}),
        };
        with_topology(nodes, {{"dc1", 2}}, this_smp_shard_count(), 0, [&] (const auto& erm, const auto* trs) {
            auto expected_global_size = local_size;
            auto estimated_global_size = estimate_table_size(e, tid, erm, trs);
            check_estimate(estimated_global_size, expected_global_size);
        });
    });
}

// Reproducer for https://scylladb.atlassian.net/browse/SCYLLADB-3760
// Yet another case of bad estimation by `collect_table_sizes_for_migration()`:
//
// In multi-DC clusters, the old formula (see comment in previous test case)
// would underestimate the `local_fraction` because it is computed with respect
// to the whole ring, rather than just the local datacenter's portion of the
// ring.
//
// Consider a cluster with 2 DCs, each with one node and RF=1.
// Each node holds 100% of the data, but the old formula would give us:
// local_fraction * local_rf = 1/2 * 1 = 50%, an underestimate.
//
// Test this case with a uniformly distributed dataset and token ring.
// Expect each node to store 100% of the dataset.
SEASTAR_TEST_CASE(test_table_size_estimate_multidc) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto tid = create_vnode_table(e);
        auto local_size = set_local_table_size(e, 1 * 1024 * 1024);

        std::vector<node_spec> nodes{
            make_node("dc1", "rack1", {50 * ring_percent}),
            make_node("dc2", "rack1", {100 * ring_percent}),
        };
        with_topology(nodes, {{"dc1", 1}, {"dc2", 1}}, this_smp_shard_count(), 0, [&] (const auto& erm, const auto* trs) {
            auto expected_global_size = local_size;
            auto estimated_global_size = estimate_table_size(e, tid, erm, trs);
            check_estimate(estimated_global_size, expected_global_size);
        });
    });
}

// A node whose DC does not replicate the keyspace cannot estimate its size.
// Make sure the estimation function throws.
SEASTAR_TEST_CASE(test_table_size_estimate_fails_on_rf_0_dc) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto tid = create_vnode_table(e);
        set_local_table_size(e, 1 * 1024 * 1024);

        std::vector<node_spec> nodes{
            make_node("dc1", "rack1", {50 * ring_percent}),
            make_node("dc2", "rack1", {100 * ring_percent}),
        };
        // Set `local_host_idx` to DC2 node, which has RF=0.
        with_topology(nodes, {{"dc1", 1}, {"dc2", 0}}, this_smp_shard_count(), 1, [&] (const auto& erm, const auto* trs) {
            BOOST_REQUIRE_EXCEPTION(
                    estimate_table_size(e, tid, erm, trs), std::runtime_error,
                    [] (const std::runtime_error& ex) {
                        return sstring(ex.what()).find("replication factor for local DC") != sstring::npos;
                    });
        });
    });
}

BOOST_AUTO_TEST_SUITE_END()
