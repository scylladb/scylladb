/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <fmt/ranges.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/test_utils.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "db/config.hh"
#include "compaction/compaction_manager.hh"
#include "schema/schema_builder.hh"

BOOST_AUTO_TEST_SUITE(cql_query_large_test)

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_large_partitions) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_large_partition_warning_threshold_mb(0);
    return do_with_cql_env([](cql_test_env& e) { return make_ready_future<>(); }, cfg);
}

SEASTAR_TEST_CASE(test_large_row_count) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_rows_count_warning_threshold(0);
    return do_with_cql_env([](cql_test_env& e) { return make_ready_future<>(); }, cfg);
}

static void flush(cql_test_env& e) {
    e.db().invoke_on_all([](replica::database& dbi) {
        return dbi.flush_all_memtables();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_large_collection) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_large_cell_warning_threshold_mb(1);
    do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table tbl (a int, b list<text>, primary key (a))").get();
        e.execute_cql("insert into tbl (a, b) values (42, []);").get();
        sstring blob(1024, 'x');
        for (unsigned i = 0; i < 1024; ++i) {
            e.execute_cql("update tbl set b = ['" + blob + "'] + b where a = 42;").get();
        }

        flush(e);
        assert_that(e.execute_cql("select partition_key, column_name from system.large_cells where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .with_size(1)
            .with_row({"42", "b", "tbl"});
    }, cfg).get();
}

SEASTAR_THREAD_TEST_CASE(test_large_data) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_large_row_warning_threshold_mb(1);
    cfg->compaction_large_cell_warning_threshold_mb(1);
    cfg->compaction_large_partition_warning_threshold_mb(1);
    do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table tbl (a int, b text, primary key (a))").get();
        sstring blob(1024*1024, 'x');
        e.execute_cql("insert into tbl (a, b) values (42, 'foo');").get();
        e.execute_cql("insert into tbl (a, b) values (44, '" + blob + "');").get();
        flush(e);

        shared_ptr<cql_transport::messages::result_message> msg = e.execute_cql("select partition_key, row_size from system.large_rows where table_name = 'tbl' allow filtering;").get();
        auto res = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        auto rows = res->rs().result_set().rows();

        // Check the only the large row is added to system.large_rows.
        BOOST_REQUIRE_EQUAL(rows.size(), 1);
        auto row0 = rows[0];
        // The result has 3 columns: partition_key, row_size, and table_name
        // (CQL adds the filtered partition key column to the result set when
        // using ALLOW FILTERING with a partial partition key restriction).
        BOOST_REQUIRE_EQUAL(row0.size(), 3);
        BOOST_REQUIRE_EQUAL(to_bytes(*row0[0]), "44");
        BOOST_REQUIRE_EQUAL(to_bytes(*row0[2]), "tbl");

        // Unfortunately we cannot check the exact size, since it includes a timestamp written as a vint of the delta
        // since start of the write. This means that the size of the row depends on the time it took to write the
        // previous rows.
        auto row_size_bytes = *row0[1];
        BOOST_REQUIRE_EQUAL(row_size_bytes.size(), 8);
        long row_size = read_be<long>(reinterpret_cast<const char*>(&row_size_bytes[0]));
        BOOST_REQUIRE(row_size > 1024*1024 && row_size < 1025*1024);

        // Check that it was added to system.large_cells too
        assert_that(e.execute_cql("select partition_key, column_name from system.large_cells where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .with_size(1)
            .with_row({"44", "b", "tbl"});

        // Check that it was added to system.large_partitions too
        assert_that(e.execute_cql("select partition_key, rows from system.large_partitions where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .with_size(1)
            .with_row({ { utf8_type->decompose("44") },
                        { long_type->decompose(1L) },
                        { utf8_type->decompose("tbl") } });

        e.execute_cql("delete from tbl where a = 44;").get();

        // In order to guarantee that system.large_rows, system.large_cells and
        // system.large_partitions are empty, we need to:
        // * flush, so that a tombstone for the above delete is created.
        // * do a major compaction, so that the tombstone is combined with the old entry,
        //   and the old sstable (which holds the large data records in its metadata) is deleted.
        flush(e);
        e.db().invoke_on_all([] (replica::database& dbi) {
            return dbi.get_tables_metadata().parallel_for_each_table([&dbi] (table_id, lw_shared_ptr<replica::table> t) {
                return dbi.get_compaction_manager().perform_major_compaction(t->try_get_compaction_group_view_with_static_sharding(), tasks::task_info{});
            });
        }).get();

        assert_that(e.execute_cql("select partition_key from system.large_rows where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .is_empty();
        assert_that(e.execute_cql("select partition_key from system.large_cells where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .is_empty();
        assert_that(e.execute_cql("select partition_key from system.large_partitions where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .is_empty();
    }, cfg).get();
}

SEASTAR_THREAD_TEST_CASE(test_large_row_count_warning) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_rows_count_warning_threshold(10);
    do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table tbl (a int, b text, primary key (a, b))").get();
        for (int i = 0; i < 11; ++i) {
            e.execute_cql(format("insert into tbl (a, b) values (42, 'foo{}');", i)).get();
        }
        flush(e);

        // Check that the warning was added to system.large_partitions
        assert_that(e.execute_cql("select partition_key, rows from system.large_partitions where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .with_size(1)
            .with_row({ { utf8_type->decompose("42") },
                        { long_type->decompose(11L) },
                        { utf8_type->decompose("tbl") } });
    }, cfg).get();
}

// Test that when a partition exceeds both the size threshold and the row
// count threshold, system.large_partitions still returns a single row
// (not two rows with the same clustering key).
SEASTAR_THREAD_TEST_CASE(test_large_partitions_dual_threshold) {
    auto cfg = make_shared<db::config>();
    // Set very low thresholds so that a single partition with a handful
    // of rows containing modest data exceeds both size and row-count
    // thresholds simultaneously.
    cfg->compaction_large_partition_warning_threshold_mb(1);
    cfg->compaction_rows_count_warning_threshold(10);
    do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table tbl (a int, b int, c text, primary key (a, b))").get();
        // Insert enough rows with enough data to exceed 1 MB partition size
        // AND more than 10 rows.
        sstring blob(128 * 1024, 'x'); // 128 KB per row
        for (int i = 0; i < 11; ++i) {
            e.execute_cql(format("insert into tbl (a, b, c) values (42, {}, '{}');", i, blob)).get();
        }
        flush(e);

        // There must be exactly one row in system.large_partitions for
        // this table.  Before the fix, two rows with the same clustering
        // key would have been emitted.
        assert_that(e.execute_cql(
                "select partition_key, rows from system.large_partitions "
                "where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .with_size(1);
    }, cfg).get();
}

// Test that when a collection cell exceeds both the cell size threshold
// and the collection element count threshold, system.large_cells still
// returns a single row (not two rows with the same clustering key).
SEASTAR_THREAD_TEST_CASE(test_large_cells_dual_threshold) {
    auto cfg = make_shared<db::config>();
    // Set very low thresholds so that a collection with modest elements
    // exceeds both size and element-count thresholds simultaneously.
    cfg->compaction_large_cell_warning_threshold_mb(1);
    cfg->compaction_collection_elements_count_warning_threshold(10);
    do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table tbl (a int, b list<text>, primary key (a))").get();
        // Insert 128 KB blobs, 11 times -> ~1.4 MB total, exceeding 1 MB
        // threshold.  Also exceeds 10 element threshold.
        sstring blob(128 * 1024, 'x');
        for (int i = 0; i < 11; ++i) {
            e.execute_cql("update tbl set b = ['" + blob + "'] + b where a = 42;").get();
        }
        flush(e);

        // There must be exactly one row in system.large_cells for this
        // table.  Before the fix, two rows with the same clustering key
        // would have been emitted.
        assert_that(e.execute_cql(
                "select partition_key, column_name from system.large_cells "
                "where table_name = 'tbl' allow filtering;").get())
            .is_rows()
            .with_size(1);
    }, cfg).get();
}

SEASTAR_TEST_CASE(test_insert_large_collection_values) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            auto map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
            auto set_type = set_type_impl::get_instance(utf8_type, true);
            auto list_type = list_type_impl::get_instance(utf8_type, true);
            e.create_table([map_type, set_type, list_type] (std::string_view ks_name) {
                // CQL: CREATE TABLE tbl (pk text PRIMARY KEY, m map<text, text>, s set<text>, l list<text>);
                return *schema_builder(this_smp_shard_count(), ks_name, "tbl")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("m", map_type)
                        .with_column("s", set_type)
                        .with_column("l", list_type)
                        .build();
            }).get();
            sstring long_value(std::numeric_limits<uint16_t>::max() + 10, 'x');
            e.execute_cql(format("INSERT INTO tbl (pk, l) VALUES ('Zamyatin', ['{}']);", long_value)).get();
            assert_that(e.execute_cql("SELECT l FROM tbl WHERE pk ='Zamyatin';").get())
                    .is_rows().with_rows({
                            { make_list_value(list_type, list_type_impl::native_type({{long_value}})).serialize() }
                    });
            BOOST_REQUIRE_THROW(e.execute_cql(format("INSERT INTO tbl (pk, s) VALUES ('Orwell', {{'{}'}});", long_value)).get(), std::exception);
            e.execute_cql(format("INSERT INTO tbl (pk, m) VALUES ('Haksli', {{'key': '{}'}});", long_value)).get();
            assert_that(e.execute_cql("SELECT m FROM tbl WHERE pk ='Haksli';").get())
                    .is_rows().with_rows({
                            { make_map_value(map_type, map_type_impl::native_type({{sstring("key"), long_value}})).serialize() }
                    });
            BOOST_REQUIRE_THROW(e.execute_cql(format("INSERT INTO tbl (pk, m) VALUES ('Golding', {{'{}': 'value'}});", long_value)).get(), std::exception);
        });
    });
}

BOOST_AUTO_TEST_SUITE_END()
