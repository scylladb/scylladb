/*
 * Copyright (C) 2021-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "db/virtual_table.hh"
#include "db/system_keyspace.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "test/lib/cql_assertions.hh"

namespace db {

class test_table : public virtual_table {
public:
    test_table() : virtual_table(build_schema()) {}

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "test");
        return schema_builder(system_keyspace::NAME, "test", std::make_optional(id))
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    mutation_source as_mutation_source() override {
        throw std::runtime_error("Not implemented");
    }

    void test_set_cell() {
        mutation m(_s, partition_key::from_single_value(*_s, data_value(666).serialize_nonnull()));
        row& cr = m.partition().clustered_row(*_s, clustering_key::from_single_value(*_s, data_value(10).serialize_nonnull())).cells();

        set_cell(cr, "v", 8);

        auto result_cell = cr.cell_at(0).as_atomic_cell(column_definition("v", int32_type, column_kind::regular_column));
        auto result = result_cell.serialize();

        BOOST_REQUIRE(result[result.size() - 1] == 8);

        BOOST_CHECK_THROW(set_cell(cr, "nonexistent_column", 20), std::runtime_error);
    }
};

}

SEASTAR_TEST_CASE(test_set_cell) {
    auto table = db::test_table();
    table.test_set_cell();

    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(test_system_config_table_read) {
    do_with_cql_env_thread([] (cql_test_env& env) {
        auto res = env.execute_cql("SELECT * FROM system.config WHERE name = 'partitioner';").get();
        assert_that(res).is_rows().with_size(1).with_row({
            { utf8_type->decompose(sstring("partitioner")) },
            { utf8_type->decompose(sstring("default")) },
            { utf8_type->decompose(sstring("string")) },
            { utf8_type->decompose(format("\"{}\"", env.local_db().get_config().partitioner())) }
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_system_config_table_update) {
    if (smp::count < 2) {
        fmt::print("This test should be run with at least 2 CPUs\n");
        return;
    }

    do_with_cql_env_thread([] (cql_test_env& env) {
        auto value = env.local_db().get_config().failure_detector_timeout_in_ms();
        smp::invoke_on_others([&env, value] {
            BOOST_REQUIRE_EQUAL(env.local_db().get_config().failure_detector_timeout_in_ms(), value);
        }).get();

        env.execute_cql(format("UPDATE system.config SET value = '{}' WHERE name = 'failure_detector_timeout_in_ms';", value + 10)).get();

        smp::invoke_on_others([&env, value] {
            BOOST_REQUIRE_EQUAL(env.local_db().get_config().failure_detector_timeout_in_ms(), value + 10);
        }).get();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_system_config_table_no_live_update) {
    do_with_cql_env_thread([] (cql_test_env& env) {
        BOOST_REQUIRE_THROW(
            env.execute_cql("UPDATE system.config SET value = 'foo' WHERE name = 'cluster_name';").get(),
            exceptions::mutation_write_failure_exception
        );
    }).get();
}
