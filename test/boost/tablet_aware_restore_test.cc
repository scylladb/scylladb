/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/cql_test_env.hh"
#include "utils/assert.hh"
#include <seastar/core/sstring.hh>

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/system_distributed_keyspace.hh"

#include "test/lib/test_utils.hh"

SEASTAR_TEST_CASE(test_snapshot_cql_tables_api_works, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    auto db_cfg_ptr = make_shared<db::config>();

    return do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto snapshot_name = "snapshot";
        auto ks = "ks";
        auto table = "cf";
        auto is_view = false;
        auto schema = "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int)";

        // insert some test data into snapshot_cql_tables table
        co_await env.get_system_distributed_keyspace().local().insert_snapshot_cql_table(
                snapshot_name, ks, table, is_view, schema, db::consistency_level::ONE);

        // read it back and check if it is correct
        auto result_schema = co_await env.get_system_distributed_keyspace().local().get_snapshot_cql_table_schema(
                snapshot_name, ks, table, db::consistency_level::ONE);

        BOOST_CHECK_EQUAL(result_schema, schema);
    }, db_cfg_ptr);
}
