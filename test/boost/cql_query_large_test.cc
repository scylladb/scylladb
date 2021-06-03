/*
 * Copyright (C) 2015-present ScyllaDB
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


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <experimental/source_location>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"
#include "types/user.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "db/config.hh"
#include "sstables/compaction_manager.hh"
#include "test/lib/exception_utils.hh"
#include "schema_builder.hh"

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_large_partitions) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_large_partition_warning_threshold_mb(0);
    return do_with_cql_env([](cql_test_env& e) { return make_ready_future<>(); }, cfg);
}

static void flush(cql_test_env& e) {
    e.db().invoke_on_all([](database& dbi) {
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
        assert_that(e.execute_cql("select partition_key, column_name from system.large_cells where table_name = 'tbl' allow filtering;").get0())
            .is_rows()
            .with_size(1)
            .with_row({"42", "b", "tbl"});

        return make_ready_future<>();
    }, cfg).get();
}

SEASTAR_THREAD_TEST_CASE(test_large_data) {
    auto cfg = make_shared<db::config>();
    cfg->compaction_large_row_warning_threshold_mb(1);
    cfg->compaction_large_cell_warning_threshold_mb(1);
    do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table tbl (a int, b text, primary key (a))").get();
        sstring blob(1024*1024, 'x');
        e.execute_cql("insert into tbl (a, b) values (42, 'foo');").get();
        e.execute_cql("insert into tbl (a, b) values (44, '" + blob + "');").get();
        flush(e);

        shared_ptr<cql_transport::messages::result_message> msg = e.execute_cql("select partition_key, row_size from system.large_rows where table_name = 'tbl' allow filtering;").get0();
        auto res = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        auto rows = res->rs().result_set().rows();

        // Check the only the large row is added to system.large_rows.
        BOOST_REQUIRE_EQUAL(rows.size(), 1);
        auto row0 = rows[0];
        BOOST_REQUIRE_EQUAL(row0.size(), 3);
        BOOST_REQUIRE_EQUAL(*row0[0], "44");
        BOOST_REQUIRE_EQUAL(*row0[2], "tbl");

        // Unfortunately we cannot check the exact size, since it includes a timestemp written as a vint of the delta
        // since start of the write. This means that the size of the row depends on the time it took to write the
        // previous rows.
        auto row_size_bytes = *row0[1];
        BOOST_REQUIRE_EQUAL(row_size_bytes.size(), 8);
        long row_size = read_be<long>(reinterpret_cast<const char*>(&row_size_bytes[0]));
        BOOST_REQUIRE(row_size > 1024*1024 && row_size < 1025*1024);

        // Check that it was added to system.large_cells too
        assert_that(e.execute_cql("select partition_key, column_name from system.large_cells where table_name = 'tbl' allow filtering;").get0())
            .is_rows()
            .with_size(1)
            .with_row({"44", "b", "tbl"});

        e.execute_cql("delete from tbl where a = 44;").get();

        // In order to guarantee that system.large_rows has been updated, we have to
        // * flush, so that a tombstone for the above delete is created.
        // * do a major compaction, so that the tombstone is combined with the old entry,
        //   and the old sstable is deleted.
        flush(e);
        e.db().invoke_on_all([] (database& dbi) {
            return parallel_for_each(dbi.get_column_families(), [&dbi] (auto& table) {
                return dbi.get_compaction_manager().submit_major_compaction(&*table.second);
            });
        }).get();

        assert_that(e.execute_cql("select partition_key from system.large_rows where table_name = 'tbl' allow filtering;").get0())
            .is_rows()
            .is_empty();
        assert_that(e.execute_cql("select partition_key from system.large_cells where table_name = 'tbl' allow filtering;").get0())
            .is_rows()
            .is_empty();

        return make_ready_future<>();
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
                return *schema_builder(ks_name, "tbl")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("m", map_type)
                        .with_column("s", set_type)
                        .with_column("l", list_type)
                        .build();
            }).get();
            sstring long_value(std::numeric_limits<uint16_t>::max() + 10, 'x');
            e.execute_cql(format("INSERT INTO tbl (pk, l) VALUES ('Zamyatin', ['{}']);", long_value)).get();
            assert_that(e.execute_cql("SELECT l FROM tbl WHERE pk ='Zamyatin';").get0())
                    .is_rows().with_rows({
                            { make_list_value(list_type, list_type_impl::native_type({{long_value}})).serialize() }
                    });
            BOOST_REQUIRE_THROW(e.execute_cql(format("INSERT INTO tbl (pk, s) VALUES ('Orwell', {{'{}'}});", long_value)).get(), std::exception);
            e.execute_cql(format("INSERT INTO tbl (pk, m) VALUES ('Haksli', {{'key': '{}'}});", long_value)).get();
            assert_that(e.execute_cql("SELECT m FROM tbl WHERE pk ='Haksli';").get0())
                    .is_rows().with_rows({
                            { make_map_value(map_type, map_type_impl::native_type({{sstring("key"), long_value}})).serialize() }
                    });
            BOOST_REQUIRE_THROW(e.execute_cql(format("INSERT INTO tbl (pk, m) VALUES ('Golding', {{'{}': 'value'}});", long_value)).get(), std::exception);

            auto make_query_options = [] (cql_protocol_version_type version) {
                    return std::make_unique<cql3::query_options>(cql3::default_cql_config, db::consistency_level::ONE, std::nullopt,
                            std::vector<cql3::raw_value_view>(), false,
                            cql3::query_options::specific_options::DEFAULT, cql_serialization_format{version});
            };

            BOOST_REQUIRE_THROW(e.execute_cql("SELECT l FROM tbl WHERE pk = 'Zamyatin';", make_query_options(2)).get(), std::exception);
            BOOST_REQUIRE_THROW(e.execute_cql("SELECT m FROM tbl WHERE pk = 'Haksli';", make_query_options(2)).get(), std::exception);
        });
    });
}
