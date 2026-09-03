/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/smp.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "service/client_state.hh"
#include "test/lib/cql_test_env.hh"

BOOST_AUTO_TEST_SUITE(client_state_test)

// Reproducer for SCYLLADB-3951: a client_state passed to another shard via
// move_to_other_shard() used to hold a reference to the original object, so
// the other shard read client_state::_keyspace while the owning shard could
// concurrently modify it with a USE statement. The snapshot must be taken
// eagerly on the owning shard, so later modifications of the original are
// not observed (and not raced with) by the receiving shard.
SEASTAR_TEST_CASE(test_client_state_for_another_shard_is_a_snapshot) {
    co_await do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto& cs = env.local_client_state();
        cs.set_raw_keyspace("ks_before");

        auto snapshot = cs.move_to_other_shard();

        // Simulates a concurrent USE statement on the owning shard.
        cs.set_raw_keyspace("ks_after");

        // A copy of the snapshot (e.g. when a lambda capturing it is copied
        // per target shard) must also carry the original value.
        auto snapshot_copy = snapshot;

        co_await smp::invoke_on_all([&snapshot, &snapshot_copy] {
            auto local_cs = snapshot.get();
            BOOST_REQUIRE_EQUAL(local_cs.get_raw_keyspace(), "ks_before");
            auto local_cs2 = snapshot_copy.get();
            BOOST_REQUIRE_EQUAL(local_cs2.get_raw_keyspace(), "ks_before");
        });

        BOOST_REQUIRE_EQUAL(cs.get_raw_keyspace(), "ks_after");
    });
}

BOOST_AUTO_TEST_SUITE_END()
