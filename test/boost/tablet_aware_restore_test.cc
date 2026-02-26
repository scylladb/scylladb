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
#include "replica/schema_describe_helper.hh"

#include "test/lib/test_utils.hh"


future<> create_dataset(sstring cf, cql_test_env& env) {
    co_await env.create_table([cf] (std::string_view ks_name) {
        return *schema_builder(ks_name, cf)
            .with_column("a", utf8_type, column_kind::partition_key)
            .with_column("b", int32_type, column_kind::clustering_key)
            .build();
    });
    auto stmt = co_await env.prepare(fmt::format("insert into {} (a, b) values (?, ?)", cf));
    auto make_key = [] (int64_t k) {
        std::string s = fmt::format("key{}", k);
        return cql3::raw_value::make_value(utf8_type->decompose(s));
    };
    auto make_val = [] (int64_t x) {
        return cql3::raw_value::make_value(int32_type->decompose(int32_t{x}));
    };

    co_await env.execute_prepared(stmt, {make_key(1), make_val(2)});
    co_await env.execute_prepared(stmt, {make_key(2), make_val(3)});
    co_await env.execute_prepared(stmt, {make_key(3), make_val(4)});
}

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

sstring verify_tablet_options(cql_test_env& env, table_id tid, size_t desired_count, bool expected) {
    auto schema = env.local_db().find_schema(tid);
    auto schema_desc = schema->describe(replica::make_schema_describe_helper(schema, env.local_db().as_data_dictionary()), cql3::describe_option::STMTS);
    auto create_stmt = schema_desc.create_statement.value().linearize();

    auto min_tablet_count = fmt::format("'min_tablet_count': '{}'", desired_count);
    // auto max_tablet_count = fmt::format("'max_tablet_count': '{}'", desired_count);

    BOOST_CHECK_EQUAL(create_stmt.find(min_tablet_count) != sstring::npos, expected);
    // BOOST_CHECK_EQUAL(create_stmt.find(max_tablet_count) != sstring::npos, expected); TODO: enable after https://github.com/scylladb/scylladb/pull/28450

    return create_stmt;
}

SEASTAR_TEST_CASE(test_restore_recreate_table_with_fixed_tablet_count, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);

    return do_with_cql_env([] (cql_test_env& env) -> future<> {
        co_await create_dataset("cf", env);
        table_id tid = env.local_db().find_uuid("ks", "cf");

        auto token_metadata = env.local_db().get_token_metadata_ptr();
        auto& tmap = token_metadata->tablets().get_tablet_map(tid);
        auto desired_count = tmap.tablet_count();

        auto initial_create_stmt = verify_tablet_options(env, tid, desired_count, false);

        co_await replica::recreate_table_with_fixed_tablet_count("snapshot", "ks", tid, env.local_db(),
                                                                 env.get_system_distributed_keyspace().local(),
                                                                 env.get_storage_proxy().local(), env.migration_manager().local(),
                                                                 db::consistency_level::ONE);

        tid = env.local_db().find_uuid("ks", "cf");
        auto create_stmt = verify_tablet_options(env, tid, desired_count, true);

        // throws if the table is not found
        auto stored_schema = co_await env.get_system_distributed_keyspace().local().get_snapshot_cql_table_schema("snapshot", "ks", "cf", db::consistency_level::ONE);
        BOOST_CHECK_EQUAL(stored_schema, initial_create_stmt);
        BOOST_CHECK_NE(stored_schema, create_stmt);

    }, db_cfg_ptr);
}
