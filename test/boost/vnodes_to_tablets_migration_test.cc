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

BOOST_AUTO_TEST_SUITE_END()
