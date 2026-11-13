/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/format.h>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "replica/database.hh"
#include "transport/messages/result_message.hh"

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


// Test that a schema change takes the tables_metadata lock only when it adds
// or drops a table: an ALTER TABLE completes while a preemptive iteration over
// the tables holds the read lock, and a CREATE TABLE waits for the iteration.
SEASTAR_TEST_CASE(test_lock_tables_metadata_only_on_table_add_or_drop) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();

        promise<> parked;
        promise<> release;

        // Holds the read lock on shard 0, which lock_tables_metadata() locks first.
        auto reader = e.local_db().get_tables_metadata().for_each_table_gently(
                [&, first = true] (table_id, lw_shared_ptr<replica::table>) mutable -> future<> {
            if (std::exchange(first, false)) {
                parked.set_value();
                return release.get_future();
            }
            return make_ready_future();
        });
        parked.get_future().get();

        // The statements and the iteration are awaited on every path, so the environment
        // is not torn down while they still run.
        using result = shared_future<shared_ptr<cql_transport::messages::result_message>>;
        std::optional<result> alter;
        std::optional<result> create;
        std::exception_ptr ex;
        try {
            alter.emplace(e.execute_cql("ALTER TABLE ks.t ADD w int"));
            with_timeout(lowres_clock::now() + 30s, alter->get_future()).get();

            create.emplace(e.execute_cql("CREATE TABLE ks.t2 (pk int PRIMARY KEY)"));
            sleep(100ms).get();
            BOOST_REQUIRE(!create->available());
        } catch (...) {
            ex = std::current_exception();
        }

        release.set_value();
        reader.get();
        for (auto* statement : {&alter, &create}) {
            if (*statement) {
                try {
                    (*statement)->get_future().get();
                } catch (...) {
                    if (!ex) {
                        ex = std::current_exception();
                    }
                }
            }
        }
        if (ex) {
            std::rethrow_exception(ex);
        }
    });
}
