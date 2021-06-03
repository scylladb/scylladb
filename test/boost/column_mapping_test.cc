/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "db/schema_tables.hh"
#include "transport/messages/result_message.hh"

SEASTAR_TEST_CASE(test_column_mapping_persistence) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Check that column mapping history is empty initially
        const auto empty_history_res = cquery_nofail(e, "select * from system.scylla_table_schema_history");
        assert_that(empty_history_res).is_rows().is_empty();

        // Create a test table -- this should trigger insertion of a corresponding
        // column mapping into the history table
        cquery_nofail(e, "create table test (pk int PRIMARY KEY, v int)");
        auto schema = e.local_db().find_schema("ks", "test");
        const utils::UUID table_id = schema->id();
        const table_schema_version v1 = schema->version();
        const column_mapping orig_cm = schema->get_column_mapping();

        // Check that stored column mapping is correctly serialized and deserialized
        column_mapping cm;
        BOOST_REQUIRE_NO_THROW(cm = db::schema_tables::get_column_mapping(table_id, v1).get0());
        BOOST_REQUIRE_EQUAL(orig_cm, cm);

        // Alter the test table and check that new column mapping is also inserted for the new schema version
        cquery_nofail(e, "alter table test ADD dummy int");
        auto altered_schema = e.local_db().find_schema("ks", "test");
        column_mapping altered_cm;
        BOOST_REQUIRE_NO_THROW(altered_cm = db::schema_tables::get_column_mapping(table_id, altered_schema->version()).get0());
        BOOST_REQUIRE_EQUAL(altered_schema->get_column_mapping(), altered_cm);
    });
}

SEASTAR_TEST_CASE(test_column_mapping_ttl_check) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Check that column mapping history is empty initially
        const auto empty_history_res = cquery_nofail(e, "select * from system.scylla_table_schema_history");
        assert_that(empty_history_res).is_rows().is_empty();

        // Create a test table -- this should trigger insertion of a corresponding
        // column mapping into the history table
        cquery_nofail(e, "create table test (pk int PRIMARY KEY, v int)");
        auto schema = e.local_db().find_schema("ks", "test");
        const utils::UUID table_id = schema->id();
        const table_schema_version v1 = schema->version();

        const sstring select_ttl_query = format(
            "select ttl(type) from system.scylla_table_schema_history where cf_id={} and schema_version={}",
            table_id, v1);

        // The column mapping entries for the most recent table schema version
        // should not have TTL set
        auto v1_res = cquery_nofail(e, select_ttl_query);
        assert_that(v1_res)
            .is_rows()
            .with_size(1)
            .with_row({ {} });

        // Alter the test table -- this should both insert the new column mapping
        // for the new schema version and also set TTL on now obsolete column mapping entries
        cquery_nofail(e, "alter table test ADD dummy int");

        v1_res = cquery_nofail(e, select_ttl_query);
        assert_that(v1_res)
            .is_rows()
            .with_size(1);
        const auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(v1_res);
        const auto& row = rows->rs().result_set().rows().front();
        BOOST_REQUIRE(row[0]);
        // Check that TTL is set
        int32_t ttl_val = value_cast<int32_t>(int32_type->deserialize(*row[0]));
        BOOST_REQUIRE(ttl_val > 0);
    });
}