/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "table_helper.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/cql_config.hh"
#include "service/client_state.hh"
#include "service/migration_manager.hh"
#include "service/query_state.hh"
#include "types/types.hh"
#include "utils/error_injection.hh"

// Regression test for use-after-free in table_helper::insert() when the
// prepared_statements_cache entry is invalidated (e.g. DROP TABLE) while a
// concurrent insert() is suspended in execute(). The injection point inside
// insert() is used to park fiber A deterministically, then fiber B drops the
// last strong ref; without the fix, resuming A crashes.

BOOST_AUTO_TEST_SUITE(table_helper_test)

#ifdef SCYLLA_ENABLE_ERROR_INJECTION

SEASTAR_TEST_CASE(test_concurrent_invalidation) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto& qp = env.local_qp();
        auto& mm = env.migration_manager().local();

        env.execute_cql("CREATE KEYSPACE th_ks WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        env.execute_cql("CREATE TABLE th_ks.t (id int PRIMARY KEY, v int)").get();

        const sstring create_cql = "CREATE TABLE IF NOT EXISTS th_ks.t (id int PRIMARY KEY, v int)";
        const sstring insert_cql = "INSERT INTO th_ks.t (id, v) VALUES (?, ?)";

        table_helper helper("th_ks", "t", create_cql, insert_cql);

        service::query_state qs(service::client_state::for_internal_calls(), empty_service_permit());

        auto make_opts = [] {
            std::vector<cql3::raw_value> vals {
                cql3::raw_value::make_value(int32_type->decompose(0)),
                cql3::raw_value::make_value(int32_type->decompose(0)),
            };
            return cql3::query_options(cql3::default_cql_config, db::consistency_level::ONE,
                std::nullopt, std::move(vals), false,
                cql3::query_options::specific_options::DEFAULT);
        };

        // Prime the prepared cache.
        helper.insert(qp, mm, qs, make_opts).get();

        utils::get_local_injector().enable("table_helper_insert_before_execute", true /*one_shot*/);

        // Fiber A: suspends at the injection, between cache_table_info() and execute().
        auto fiber_a = helper.insert(qp, mm, qs, make_opts);

        // Wait until fiber A is actually parked in wait_for_message.
        while (utils::get_local_injector().waiters("table_helper_insert_before_execute") == 0) {
            seastar::yield().get();
        }

        // Evict the prepared cache entry - drops its strong ref to the
        // modification_statement. helper._insert_stmt is the only ref left.
        env.execute_cql("DROP TABLE th_ks.t").discard_result().get();

        // Fiber B: cache_table_info() sees the weak ref invalidated and sets
        // _insert_stmt = nullptr; the re-prepare then throws (table is gone).
        helper.insert(qp, mm, qs, make_opts)
            .handle_exception([] (std::exception_ptr) {}).get();

        // Release fiber A. Unfixed: re-reads null _insert_stmt and crashes.
        utils::get_local_injector().receive_message("table_helper_insert_before_execute");

        try {
            fiber_a.get();
        } catch (...) {
            // execute() may fail (table is gone); only the crash matters.
        }
    });
}

#endif // SCYLLA_ENABLE_ERROR_INJECTION

#ifndef SCYLLA_ENABLE_ERROR_INJECTION
// The only test in this suite requires error injection support. Without this
// dummy case the suite would be empty, which causes boost to report
// "test tree is empty" and pytest to exit with code 5 ("no tests collected"),
// failing CI in modes (e.g. release) where error injection is disabled.
BOOST_AUTO_TEST_CASE(test_skipped_no_error_injection) {
    BOOST_TEST_MESSAGE("table_helper_test requires SCYLLA_ENABLE_ERROR_INJECTION; skipping");
}
#endif

BOOST_AUTO_TEST_SUITE_END()
