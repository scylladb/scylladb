/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/format.h>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"

using namespace std::chrono_literals;

// Test that two lock_tables_metadata calls don't deadlock
SEASTAR_TEST_CASE(test_lock_tables_metadata_deadlock) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        try {
            // Repeat the test scenario to increase the chance of hitting the deadlock.
            // If no deadlock occurs, each repetition should complete within a fraction of a second,
            // so even with 100 repetitions, the total test time should be reasonable.
            for (int i = 0; i < 100; ++i) {
                with_timeout(lowres_clock::now() + 30s,
                    when_all_succeed(
                        e.local_db().lock_tables_metadata(e.db()).discard_result(),
                        e.local_db().lock_tables_metadata(e.db()).discard_result()
                    )).get();
            }
        } catch (seastar::timed_out_error&) {
            fmt::print(stderr, "FAIL: lock_tables_metadata deadlocked (timed out after 30s)\n");
            _exit(1);
        }
    });
}

