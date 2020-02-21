/*
 * Copyright (C) 2015 ScyllaDB
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

#include <seastar/net/inet_address.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"
#include "types/user.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "db/config.hh"
#include "cql3/cql_config.hh"
#include "sstables/compaction_manager.hh"
#include "test/lib/exception_utils.hh"
#include "json.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"
#include <regex>

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
    do_with_cql_env([](cql_test_env& e) {
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
    do_with_cql_env([](cql_test_env& e) {
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

SEASTAR_TEST_CASE(test_create_keyspace_statement) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create keyspace ks2 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").discard_result().then([&e] {
            return e.require_keyspace_exists("ks2");
        });
    });
}

SEASTAR_TEST_CASE(test_create_table_statement) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table users (user_name varchar PRIMARY KEY, birth_year bigint);").discard_result().then([&e] {
            return e.require_table_exists("ks", "users");
        }).then([&e] {
            return e.execute_cql("create table cf (id int primary key, m map<int, int>, s set<text>, l list<uuid>);").discard_result();
        }).then([&e] {
            return e.require_table_exists("ks", "cf");
        });
    });
}

SEASTAR_TEST_CASE(test_create_table_with_id_statement) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("CREATE TABLE tbl (a int, b int, PRIMARY KEY (a))").get();
            auto id = e.local_db().find_schema("ks", "tbl")->id();
            e.execute_cql("DROP TABLE tbl").get();
            BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM tbl").get(), std::exception);
            e.execute_cql(
                format("CREATE TABLE tbl (a int, b int, PRIMARY KEY (a)) WITH id='{}'", id)).get();
            assert_that(e.execute_cql("SELECT * FROM tbl").get0())
                .is_rows().with_size(0);
            BOOST_REQUIRE_THROW(
                e.execute_cql(format("CREATE TABLE tbl2 (a int, b int, PRIMARY KEY (a)) WITH id='{}'", id)).get(),
                exceptions::invalid_request_exception);
            BOOST_REQUIRE_THROW(
                e.execute_cql("CREATE TABLE tbl2 (a int, b int, PRIMARY KEY (a)) WITH id='55'").get(),
                exceptions::configuration_exception);
            BOOST_REQUIRE_THROW(
                e.execute_cql("ALTER TABLE tbl WITH id='f2a8c099-e723-48cb-8cd9-53e647a011a3'").get(),
                exceptions::configuration_exception);
        });
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_si_and_mv) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("CREATE TABLE tbl (a int, b int, c float, PRIMARY KEY (a))").get();
            e.execute_cql("CREATE INDEX idx1 ON tbl (b)").get();
            e.execute_cql("CREATE INDEX idx2 ON tbl (c)").get();
            e.execute_cql("CREATE MATERIALIZED VIEW tbl_view AS SELECT c FROM tbl WHERE c IS NOT NULL PRIMARY KEY (c, a)").get();
            // dropping a table with materialized views is prohibited
            assert_that_failed(e.execute_cql("DROP TABLE tbl"));
            e.execute_cql("DROP MATERIALIZED VIEW tbl_view").get();
            // dropping a table with secondary indexes is fine
            e.execute_cql("DROP TABLE tbl").get();

            e.execute_cql("CREATE TABLE tbl (a int, b int, c float, PRIMARY KEY (a))").get();
            e.execute_cql("CREATE INDEX idx1 ON tbl (b)").get();
            e.execute_cql("CREATE INDEX idx2 ON tbl (c)").get();
            e.execute_cql("CREATE MATERIALIZED VIEW tbl_view AS SELECT c FROM tbl WHERE c IS NOT NULL PRIMARY KEY (c, a)").get();
            // dropping whole keyspace with MV and SI is fine too
            e.execute_cql("DROP KEYSPACE ks").get();
        });
    });
}

SEASTAR_TEST_CASE(test_list_elements_validation) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto test_inline = [&] (sstring value, bool should_throw) {
            auto cql = sprint("INSERT INTO tbl (a, b) VALUES(1, ['%s'])", value);
            if (should_throw) {
                BOOST_REQUIRE_THROW(e.execute_cql(cql).get(), exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_cql(cql).get());
            }
        };
        e.execute_cql("CREATE TABLE tbl (a int, b list<date>, PRIMARY KEY (a))").get();
        test_inline("definietly not a date value", true);
        test_inline("2015-05-03", false);
        e.execute_cql("CREATE TABLE tbl2 (a int, b list<text>, PRIMARY KEY (a))").get();
        auto id = e.prepare("INSERT INTO tbl2 (a, b) VALUES(?, ?)").get0();
        auto test_bind = [&] (sstring value, bool should_throw) {
            auto my_list_type = list_type_impl::get_instance(utf8_type, true);
            std::vector<cql3::raw_value> raw_values;
            raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{1})));
            auto values = my_list_type->decompose(make_list_value(my_list_type, {value}));
            raw_values.emplace_back(cql3::raw_value::make_value(values));
            if (should_throw) {
                BOOST_REQUIRE_THROW(
                    e.execute_prepared(id, raw_values).get(),
                    exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, raw_values).get());
            }
        };
        test_bind(sstring(1, '\255'), true);
        test_bind("proper utf8 string", false);
    });
}

SEASTAR_TEST_CASE(test_set_elements_validation) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto test_inline = [&] (sstring value, bool should_throw) {
            auto cql = sprint("INSERT INTO tbl (a, b) VALUES(1, {'%s'})", value);
            if (should_throw) {
                BOOST_REQUIRE_THROW(e.execute_cql(cql).get(), exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_cql(cql).get());
            }
        };
        e.execute_cql("CREATE TABLE tbl (a int, b set<date>, PRIMARY KEY (a))").get();
        test_inline("definietly not a date value", true);
        test_inline("2015-05-03", false);
        e.execute_cql("CREATE TABLE tbl2 (a int, b set<text>, PRIMARY KEY (a))").get();
        auto id = e.prepare("INSERT INTO tbl2 (a, b) VALUES(?, ?)").get0();
        auto test_bind = [&] (sstring value, bool should_throw) {
            auto my_set_type = set_type_impl::get_instance(utf8_type, true);
            std::vector<cql3::raw_value> raw_values;
            raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{1})));
            auto values = my_set_type->decompose(make_set_value(my_set_type, {value}));
            raw_values.emplace_back(cql3::raw_value::make_value(values));
            if (should_throw) {
                BOOST_REQUIRE_THROW(
                    e.execute_prepared(id, raw_values).get(),
                    exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, raw_values).get());
            }
        };
        test_bind(sstring(1, '\255'), true);
        test_bind("proper utf8 string", false);
    });
}

SEASTAR_TEST_CASE(test_map_elements_validation) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto test_inline = [&] (sstring value, bool should_throw) {
            auto cql1 = sprint("INSERT INTO tbl (a, b) VALUES(1, {'10-10-2010' : '%s'})", value);
            auto cql2 = sprint("INSERT INTO tbl (a, b) VALUES(1, {'%s' : '10-10-2010'})", value);
            if (should_throw) {
                BOOST_REQUIRE_THROW(e.execute_cql(cql1).get(), exceptions::invalid_request_exception);
                BOOST_REQUIRE_THROW(e.execute_cql(cql2).get(), exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_cql(cql1).get());
                BOOST_REQUIRE_NO_THROW(e.execute_cql(cql2).get());
            }
        };
        e.execute_cql("CREATE TABLE tbl (a int, b map<date, date>, PRIMARY KEY (a))").get();
        test_inline("definietly not a date value", true);
        test_inline("2015-05-03", false);
        e.execute_cql("CREATE TABLE tbl2 (a int, b map<text, text>, PRIMARY KEY (a))").get();
        auto id = e.prepare("INSERT INTO tbl2 (a, b) VALUES(?, ?)").get0();
        auto test_bind = [&] (sstring value, bool should_throw) {
            auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
            std::vector<cql3::raw_value> raw_values;
            raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{1})));
            auto values =
                my_map_type->decompose(make_map_value(my_map_type,
                                                      {std::make_pair(value, sstring("foo"))}));
            raw_values.emplace_back(cql3::raw_value::make_value(values));
            if (should_throw) {
                BOOST_REQUIRE_THROW(
                    e.execute_prepared(id, raw_values).get(),
                    exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, raw_values).get());
            }
            raw_values.pop_back();
            values =
                my_map_type->decompose(make_map_value(my_map_type,
                                                      {std::make_pair(sstring("foo"), value)}));
            raw_values.emplace_back(cql3::raw_value::make_value(values));
            if (should_throw) {
                BOOST_REQUIRE_THROW(
                    e.execute_prepared(id, raw_values).get(),
                    exceptions::invalid_request_exception);
            } else {
                e.execute_prepared(id, raw_values).get();
                BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, raw_values).get());
            }
        };
        test_bind(sstring(1, '\255'), true);
        test_bind("proper utf8 string", false);
    });
}

SEASTAR_TEST_CASE(test_in_clause_validation) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto test_inline = [&] (sstring value, bool should_throw) {
            auto cql = sprint("SELECT r1 FROM tbl WHERE (c1,r1) IN ((1, '%s')) ALLOW FILTERING", value);
            if (should_throw) {
                BOOST_REQUIRE_THROW(e.execute_cql(cql).get(), exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_cql(cql).get());
            }
        };
        e.execute_cql("CREATE TABLE tbl (p1 int, c1 int, r1 date, PRIMARY KEY (p1, c1,r1))").get();
        test_inline("definietly not a date value", true);
        test_inline("2015-05-03", false);
        e.execute_cql("CREATE TABLE tbl2 (p1 int, c1 int, r1 text, PRIMARY KEY (p1, c1,r1))").get();
        auto id = e.prepare("SELECT r1 FROM tbl2 WHERE (c1,r1) IN ? ALLOW FILTERING").get0();
        auto test_bind = [&] (sstring value, bool should_throw) {
            auto my_tuple_type = tuple_type_impl::get_instance({int32_type, utf8_type});
            auto my_list_type = list_type_impl::get_instance(my_tuple_type, true);
            std::vector<cql3::raw_value> raw_values;
            auto values = make_tuple_value(my_tuple_type, tuple_type_impl::native_type({int32_t{2}, value}));
            auto in_values_list = my_list_type->decompose(make_list_value(my_list_type, {values}));
            raw_values.emplace_back(cql3::raw_value::make_value(in_values_list));
            if (should_throw) {
                BOOST_REQUIRE_THROW(
                    e.execute_prepared(id, raw_values).get(),
                    exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, raw_values).get());
            }
        };
        test_bind(sstring(1, '\255'), true);
        test_bind("proper utf8 string", false);
    });
}

SEASTAR_THREAD_TEST_CASE(test_in_clause_cartesian_product_limits) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE tab1 (pk1 int, pk2 int, PRIMARY KEY ((pk1, pk2)))").get();

        // 100 partitions, should pass
        e.execute_cql("SELECT * FROM tab1 WHERE pk1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"
                "                           AND pk2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)").get();
        // 110 partitions, should fail
        BOOST_REQUIRE_THROW(
                e.execute_cql("SELECT * FROM tab1 WHERE pk1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"
                        "                          AND pk2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)").get(),
                        std::runtime_error);

        e.execute_cql("CREATE TABLE tab2 (pk1 int, ck1 int, ck2 int, PRIMARY KEY (pk1, ck1, ck2))").get();

        // 100 clustering rows, should pass
        e.execute_cql("SELECT * FROM tab2 WHERE pk1 = 1"
                "                          AND ck1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"
                "                          AND ck2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)").get();
        // 110 clustering rows, should fail
        BOOST_REQUIRE_THROW(
                e.execute_cql("SELECT * FROM tab2 WHERE pk1 = 1"
                        "                           AND ck1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"
                        "                           AND ck2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)").get(),
                        std::runtime_error);
        auto make_tuple = [] (unsigned count) -> sstring {
            std::ostringstream os;
            os << "(0";
            for (unsigned i = 1; i < count; ++i) {
                os << "," << i;
            }
            os << ")";
            return os.str();
        };
        e.execute_cql("CREATE TABLE tab3 (pk1 int, ck1 int, PRIMARY KEY (pk1, ck1))").get();
        // tuple with 100 keys, should pass
        e.execute_cql(fmt::format("SELECT * FROM tab3 WHERE pk1 IN {}", make_tuple(100))).get();
        e.execute_cql(fmt::format("SELECT * FROM tab3 WHERE pk1 = 1 AND ck1 IN {}", make_tuple(100))).get();
        // tuple with 101 keys, should fail
        BOOST_REQUIRE_THROW(
                e.execute_cql(fmt::format("SELECT * FROM tab3 WHERE pk1 IN {}", make_tuple(101))).get(),
                std::runtime_error
                );
        BOOST_REQUIRE_THROW(
                e.execute_cql(fmt::format("SELECT * FROM tab3 WHERE pk1 = 3 AND ck1 IN {}", make_tuple(101))).get(),
                std::runtime_error
                );
    }).get();
}

SEASTAR_TEST_CASE(test_tuple_elements_validation) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto test_inline = [&] (sstring value, bool should_throw) {
            auto cql = sprint("INSERT INTO tbl (a, b) VALUES(1, (1, '%s'))", value);
            if (should_throw) {
                BOOST_REQUIRE_THROW(e.execute_cql(cql).get(), exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_cql(cql).get());
            }
        };
        e.execute_cql("CREATE TABLE tbl (a int, b tuple<int, date>, PRIMARY KEY (a))").get();
        test_inline("definietly not a date value", true);
        test_inline("2015-05-03", false);
        e.execute_cql("CREATE TABLE tbl2 (a int, b tuple<int, text>, PRIMARY KEY (a))").get();
        auto id = e.prepare("INSERT INTO tbl2 (a, b) VALUES(?, ?)").get0();
        auto test_bind = [&] (sstring value, bool should_throw) {
            auto my_tuple_type = tuple_type_impl::get_instance({int32_type, utf8_type});
            std::vector<cql3::raw_value> raw_values;
            raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{1})));
            auto values = my_tuple_type->decompose(make_tuple_value(my_tuple_type, tuple_type_impl::native_type({int32_t{2}, value})));
            raw_values.emplace_back(cql3::raw_value::make_value(values));
            if (should_throw) {
                BOOST_REQUIRE_THROW(
                    e.execute_prepared(id, raw_values).get(),
                    exceptions::invalid_request_exception);
            } else {
                BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, raw_values).get());
            }
        };
        test_bind(sstring(1, '\255'), true);
        test_bind("proper utf8 string", false);
    });
}

SEASTAR_TEST_CASE(test_insert_statement) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&e] {
            return e.execute_cql("insert into cf (p1, c1, r1) values ('key1', 1, 100);").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {1}, "r1", 100);
        }).then([&e] {
            return e.execute_cql("update cf set r1 = 66 where p1 = 'key1' and c1 = 1;").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {1}, "r1", 66);
        });
    });
}

SEASTAR_TEST_CASE(test_select_statement) {
   return do_with_cql_env([] (cql_test_env& e) {
        return e.create_table([](sstring ks_name) {
            // CQL: create table cf (p1 varchar, c1 int, c2 int, r1 int, PRIMARY KEY (p1, c1, c2));
            return schema({}, ks_name, "cf",
                {{"p1", utf8_type}},
                {{"c1", int32_type}, {"c2", int32_type}},
                {{"r1", int32_type}},
                {},
                utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, c2, r1) values ('key1', 1, 2, 3);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, c2, r1) values ('key2', 1, 2, 13);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, c2, r1) values ('key3', 1, 2, 23);").discard_result();
        }).then([&e] {
            // Test wildcard
            return e.execute_cql("select * from cf where p1 = 'key1' and c2 = 2 and c1 = 1;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {utf8_type->decompose(sstring("key1"))},
                         {int32_type->decompose(1)},
                         {int32_type->decompose(2)},
                         {int32_type->decompose(3)}
                     });
            });
        }).then([&e] {
            // Test with only regular column
            return e.execute_cql("select r1 from cf where p1 = 'key1' and c2 = 2 and c1 = 1;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {int32_type->decompose(3)}
                     });
            });
        }).then([&e] {
            // Test full partition range, singular clustering range
            return e.execute_cql("select * from cf where c1 = 1 and c2 = 2 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(3)
                    .with_row({
                         {utf8_type->decompose(sstring("key1"))},
                         {int32_type->decompose(1)},
                         {int32_type->decompose(2)},
                         {int32_type->decompose(3)}})
                    .with_row({
                         {utf8_type->decompose(sstring("key2"))},
                         {int32_type->decompose(1)},
                         {int32_type->decompose(2)},
                         {int32_type->decompose(13)}})
                    .with_row({
                         {utf8_type->decompose(sstring("key3"))},
                         {int32_type->decompose(1)},
                         {int32_type->decompose(2)},
                         {int32_type->decompose(23)}
                     });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_cassandra_stress_like_write_and_read) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto execute_update_for_key = [&e](sstring key) {
            return e.execute_cql(sprint("UPDATE cf SET "
                                            "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
                                            "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
                                            "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
                                            "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
                                            "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
                                            "WHERE \"KEY\"=%s;", key)).discard_result();
        };

        auto verify_row_for_key = [&e](sstring key) {
            return e.execute_cql(
                format("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = {}", key)).then(
                [](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows()
                        .with_size(1)
                        .with_row({
                                      {from_hex(
                                          "8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a")},
                                      {from_hex(
                                          "a8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51")},
                                      {from_hex(
                                          "583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64")},
                                      {from_hex(
                                          "62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7")},
                                      {from_hex("222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27")}
                                  });
                });
        };

        return e.create_table([](sstring ks_name) {
            return schema({}, ks_name, "cf",
                          {{"KEY", bytes_type}},
                          {},
                          {{"C0", bytes_type},
                           {"C1", bytes_type},
                           {"C2", bytes_type},
                           {"C3", bytes_type},
                           {"C4", bytes_type}},
                          {},
                          utf8_type);
        }).then([execute_update_for_key, verify_row_for_key] {
            static auto make_key = [](int suffix) { return format("0xdeadbeefcafebabe{:02d}", suffix); };
            auto suffixes = boost::irange(0, 10);
            return parallel_for_each(suffixes.begin(), suffixes.end(), [execute_update_for_key](int suffix) {
                return execute_update_for_key(make_key(suffix));
            }).then([suffixes, verify_row_for_key] {
                return parallel_for_each(suffixes.begin(), suffixes.end(), [verify_row_for_key](int suffix) {
                    return verify_row_for_key(make_key(suffix));
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_range_queries) {
   return do_with_cql_env([] (cql_test_env& e) {
        return e.create_table([](sstring ks_name) {
            return schema({}, ks_name, "cf",
                {{"k", bytes_type}},
                {{"c0", bytes_type}, {"c1", bytes_type}},
                {{"v", bytes_type}},
                {},
                utf8_type);
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x01 where k = 0x00 and c0 = 0x01 and c1 = 0x01;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x02 where k = 0x00 and c0 = 0x01 and c1 = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x03 where k = 0x00 and c0 = 0x01 and c1 = 0x03;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x04 where k = 0x00 and c0 = 0x02 and c1 = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x05 where k = 0x00 and c0 = 0x02 and c1 = 0x03;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x06 where k = 0x00 and c0 = 0x02 and c1 = 0x04;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x07 where k = 0x00 and c0 = 0x03 and c1 = 0x04;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x08 where k = 0x00 and c0 = 0x03 and c1 = 0x05;").discard_result();
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows()
                   .with_rows({
                       {from_hex("01")},
                       {from_hex("02")},
                       {from_hex("03")},
                       {from_hex("04")},
                       {from_hex("05")},
                       {from_hex("06")},
                       {from_hex("07")},
                       {from_hex("08")}
                   });
           });
        }).then([&e] {
            return e.execute_cql("select v from cf where k = 0x00 and c0 = 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
                });
            });
        }).then([&e] {
            return e.execute_cql("select v from cf where k = 0x00 and c0 > 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("07")}, {from_hex("08")}
                });
            });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}, {from_hex("05")}, {from_hex("06")}, {from_hex("07")}, {from_hex("08")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 and c0 < 0x03 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 > 0x02 and c0 <= 0x03 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("07")}, {from_hex("08")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 and c0 <= 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 < 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("01")}, {from_hex("02")}, {from_hex("03")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 = 0x02 and c1 > 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("05")}, {from_hex("06")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 = 0x02 and c1 >= 0x02 and c1 <= 0x02 allow filtering;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}
               });
           });
        });
    });
}

SEASTAR_TEST_CASE(test_ordering_of_composites_with_variable_length_components) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.create_table([](sstring ks) {
            return schema({}, ks, "cf",
                {{"k", bytes_type}},
                // We need more than one clustering column so that the single-element tuple format optimisation doesn't kick in
                {{"c0", bytes_type}, {"c1", bytes_type}},
                {{"v", bytes_type}},
                {},
                utf8_type);
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x01 where k = 0x00 and c0 = 0x0001 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x02 where k = 0x00 and c0 = 0x03 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x03 where k = 0x00 and c0 = 0x035555 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x04 where k = 0x00 and c0 = 0x05 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf where k = 0x00 allow filtering;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")}, {from_hex("02")}, {from_hex("03")}, {from_hex("04")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_query_with_static_columns) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (k blob, c blob, v blob, s1 blob static, s2 blob static, primary key (k, c));").discard_result().then([&e] {
            return e.execute_cql("update cf set s1 = 0x01 where k = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x02 where k = 0x00 and c = 0x01;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x03 where k = 0x00 and c = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1, v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                    {from_hex("01"), from_hex("03")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1 from cf limit 1;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1, v from cf limit 1;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                });
            });
        }).then([&e] {
            return e.execute_cql("update cf set v = null where k = 0x00 and c = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf (k, c) values (0x00, 0x02);").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            // Try 'in' restriction out
            return e.execute_cql("select s1, v from cf where k = 0x00 and c in (0x01, 0x02);").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                    {from_hex("01"), {}}
                });
            });
        }).then([&e] {
            // Verify that limit is respected for multiple clustering ranges and that static columns
            // are populated when limit kicks in.
            return e.execute_cql("select s1, v from cf where k = 0x00 and c in (0x01, 0x02) limit 1;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_insert_without_clustering_key) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (k blob, v blob, primary key (k));").discard_result().then([&e] {
            return e.execute_cql("insert into cf (k) values (0x01);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), {}}
                });
            });
        }).then([&e] {
            return e.execute_cql("select k from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_limit_is_respected_across_partitions) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (k blob, c blob, v blob, s1 blob static, primary key (k, c));").discard_result().then([&e] {
            return e.execute_cql("update cf set s1 = 0x01 where k = 0x01;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set s1 = 0x02 where k = 0x02;").discard_result();
        }).then([&e] {
            // Determine partition order
            return e.execute_cql("select k from cf;");
        }).then([&e](shared_ptr<cql_transport::messages::result_message> msg) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            std::vector<bytes> keys;
            const auto& rs = rows->rs().result_set();
            for (auto&& row : rs.rows()) {
                BOOST_REQUIRE(row[0]);
                keys.push_back(*row[0]);
            }
            BOOST_REQUIRE(keys.size() == 2);
            bytes k1 = keys[0];
            bytes k2 = keys[1];

            return now().then([k1, k2, &e] {
                return e.execute_cql("select s1 from cf limit 1;").then([k1, k2](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {k1},
                    });
                });
            }).then([&e, k1, k2] {
                return e.execute_cql("select s1 from cf limit 2;").then([k1, k2](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {k1}, {k2}
                    });
                });
            }).then([&e, k1, k2] {
                return e.execute_cql(format("update cf set s1 = null where k = 0x{};", to_hex(k1))).discard_result();
            }).then([&e, k1, k2] {
                return e.execute_cql("select s1 from cf limit 1;").then([k1, k2](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {k2}
                    });
                });
            }).then([&e, k1, k2] {
                return e.execute_cql(format("update cf set s1 = null where k = 0x{};", to_hex(k2))).discard_result();
            }).then([&e, k1, k2] {
                return e.execute_cql("select s1 from cf limit 1;").then([k1, k2](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().is_empty();
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_partitions_have_consistent_ordering_in_range_query) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (k blob, v int, primary key (k));").discard_result().then([&e] {
            return e.execute_cql(
                "begin unlogged batch \n"
                    "  insert into cf (k, v) values (0x01, 0); \n"
                    "  insert into cf (k, v) values (0x02, 0); \n"
                    "  insert into cf (k, v) values (0x03, 0); \n"
                    "  insert into cf (k, v) values (0x04, 0); \n"
                    "  insert into cf (k, v) values (0x05, 0); \n"
                    "  insert into cf (k, v) values (0x06, 0); \n"
                    "apply batch;"
            ).discard_result();
        }).then([&e] {
            // Determine partition order
            return e.execute_cql("select k from cf;");
        }).then([&e](shared_ptr<cql_transport::messages::result_message> msg) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            std::vector<bytes> keys;
            const auto& rs = rows->rs().result_set();
            for (auto&& row : rs.rows()) {
                BOOST_REQUIRE(row[0]);
                keys.push_back(*row[0]);
            }
            BOOST_REQUIRE(keys.size() == 6);

            return now().then([keys, &e] {
                return e.execute_cql("select k from cf limit 1;").then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 2;").then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 3;").then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 4;").then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]},
                        {keys[3]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 5;").then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 6;").then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]},
                        {keys[3]},
                        {keys[4]},
                        {keys[5]}
                    });
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_partition_range_queries_with_bounds) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (k blob, v int, primary key (k));").discard_result().then([&e] {
            return e.execute_cql(
                "begin unlogged batch \n"
                    "  insert into cf (k, v) values (0x01, 0); \n"
                    "  insert into cf (k, v) values (0x02, 0); \n"
                    "  insert into cf (k, v) values (0x03, 0); \n"
                    "  insert into cf (k, v) values (0x04, 0); \n"
                    "  insert into cf (k, v) values (0x05, 0); \n"
                    "apply batch;"
            ).discard_result();
        }).then([&e] {
            // Determine partition order
            return e.execute_cql("select k, token(k) from cf;");
        }).then([&e](shared_ptr<cql_transport::messages::result_message> msg) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            std::vector<bytes> keys;
            std::vector<int64_t> tokens;
            const auto& rs = rows->rs().result_set();
            for (auto&& row : rs.rows()) {
                BOOST_REQUIRE(row[0]);
                BOOST_REQUIRE(row[1]);
                keys.push_back(*row[0]);
                tokens.push_back(value_cast<int64_t>(long_type->deserialize(*row[1])));
            }
            BOOST_REQUIRE(keys.size() == 5);

            return now().then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) > {:d};", tokens[1])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) >= {:d};", tokens[1])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[1]},
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) > {:d} and token(k) < {:d};",
                        tokens[1], tokens[4])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[2]},
                        {keys[3]},
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) < {:d};", tokens[3])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) = {:d};", tokens[3])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[3]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) < {:d} and token(k) > {:d};", tokens[3], tokens[3])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().is_empty();
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(format("select k from cf where token(k) >= {:d} and token(k) <= {:d};", tokens[4], tokens[2])).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().is_empty();
                });
            }).then([keys, tokens, &e] {
                auto min_token = std::numeric_limits<int64_t>::min();
                return e.execute_cql(format("select k from cf where token(k) > {:d} and token (k) < {:d};", min_token, min_token)).then([keys](shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_deletion_scenarios) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.create_table([](sstring ks) {
            // CQL: create table cf (k bytes, c bytes, v bytes, primary key (k, c));
            return schema({}, ks, "cf",
                {{"k", bytes_type}}, {{"c", bytes_type}}, {{"v", bytes_type}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (k, c, v) values (0x00, 0x05, 0x01) using timestamp 1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("update cf using timestamp 2 set v = null where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {{}},
                });
            });
        }).then([&e] {
            // same tampstamp, dead cell wins
            return e.execute_cql("update cf using timestamp 2 set v = 0x02 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {{}},
                });
            });
        }).then([&e] {
            return e.execute_cql("update cf using timestamp 3 set v = 0x02 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("02")},
                });
            });
        }).then([&e] {
            // same timestamp, greater value wins
            return e.execute_cql("update cf using timestamp 3 set v = 0x03 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("03")},
                });
            });
        }).then([&e] {
            // same tampstamp, delete whole row, delete should win
            return e.execute_cql("delete from cf using timestamp 3 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().is_empty();
            });
        }).then([&e] {
            // same timestamp, update should be shadowed by range tombstone
            return e.execute_cql("update cf using timestamp 3 set v = 0x04 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().is_empty();
            });
        }).then([&e] {
            return e.execute_cql("update cf using timestamp 4 set v = 0x04 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("04")},
                });
            });
        }).then([&e] {
            // deleting an orphan cell (row is considered as deleted) yields no row
            return e.execute_cql("update cf using timestamp 5 set v = null where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().is_empty();
            });
        });
    });
}

SEASTAR_TEST_CASE(test_range_deletion_scenarios) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v text, primary key (p, c));").get();
        for (auto i = 0; i < 10; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (1, {:d}, 'abc');", i)).get();
        }

        e.execute_cql("delete from cf where p = 1 and c <= 3").get();
        e.execute_cql("delete from cf where p = 1 and c >= 8").get();

        e.execute_cql("delete from cf where p = 1 and c >= 0 and c <= 5").get();
        auto msg = e.execute_cql("select * from cf").get0();
        assert_that(msg).is_rows().with_size(2);
        e.execute_cql("delete from cf where p = 1 and c > 3 and c < 10").get();
        msg = e.execute_cql("select * from cf").get0();
        assert_that(msg).is_rows().with_size(0);

        e.execute_cql("insert into cf (p, c, v) values (1, 1, '1');").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 3, '3');").get();
        e.execute_cql("delete from cf where p = 1 and c >= 2 and c <= 3").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 2, '2');").get();
        msg = e.execute_cql("select * from cf").get0();
        assert_that(msg).is_rows().with_size(2);
        e.execute_cql("delete from cf where p = 1 and c >= 2 and c <= 3").get();
        msg = e.execute_cql("select * from cf").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {utf8_type->decompose("1")} }});
    });
}

SEASTAR_TEST_CASE(test_range_deletion_scenarios_with_compact_storage) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v text, primary key (p, c)) with compact storage;").get();
        for (auto i = 0; i < 10; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (1, {:d}, 'abc');", i)).get();
        }

        try {
            e.execute_cql("delete from cf where p = 1 and c <= 3").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
        try {
            e.execute_cql("delete from cf where p = 1 and c >= 0").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
        try {
            e.execute_cql("delete from cf where p = 1 and c > 0 and c <= 3").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
        try {
            e.execute_cql("delete from cf where p = 1 and c >= 0 and c < 3").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
        try {
            e.execute_cql("delete from cf where p = 1 and c > 0 and c < 3").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
        try {
            e.execute_cql("delete from cf where p = 1 and c >= 0 and c <= 3").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
    });
}

SEASTAR_TEST_CASE(test_map_insert_update) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto make_my_map_type = [] { return map_type_impl::get_instance(int32_type, int32_type, true); };
        auto my_map_type = make_my_map_type();
        return e.create_table([make_my_map_type] (sstring ks_name) {
            // CQL: create table cf (p1 varchar primary key, map1 map<int, int>);
            return schema({}, ks_name, "cf",
                          {{"p1", utf8_type}}, {}, {{"map1", make_my_map_type()}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, map1) values ('key1', { 1001: 2001 });").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type, map_type_impl::native_type({{1001, 2001}})));
        }).then([&e] {
            return e.execute_cql("update cf set map1[1002] = 2002 where p1 = 'key1';").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type,
                                                       map_type_impl::native_type({{1001, 2001},
                                                                                  {1002, 2002}})));
        }).then([&e] {
            // overwrite an element
            return e.execute_cql("update cf set map1[1001] = 3001 where p1 = 'key1';").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type,
                                                       map_type_impl::native_type({{1001, 3001},
                                                                                  {1002, 2002}})));
        }).then([&e] {
            // overwrite whole map
            return e.execute_cql("update cf set map1 = {1003: 4003} where p1 = 'key1';").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type,
                                                          map_type_impl::native_type({{1003, 4003}})));
        }).then([&e] {
            // overwrite whole map, but bad syntax
            return e.execute_cql("update cf set map1 = {1003, 4003} where p1 = 'key1';");
        }).then_wrapped([&e](future<shared_ptr<cql_transport::messages::result_message>> f) {
            BOOST_REQUIRE(f.failed());
            f.ignore_ready_future();
            // overwrite whole map
            return e.execute_cql(
                "update cf set map1 = {1001: 5001, 1002: 5002, 1003: 5003} where p1 = 'key1';").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type,
                                                       map_type_impl::native_type({{1001, 5001},
                                                                                  {1002, 5002},
                                                                                  {1003, 5003}})));
        }).then([&e] {
            // discard some keys
            return e.execute_cql("update cf set map1 = map1 - {1001, 1003, 1005} where p1 = 'key1';").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type,
                                                          map_type_impl::native_type({{{1002, 5002}}})));
        }).then([&e, my_map_type] {
            return e.execute_cql("select * from cf where p1 = 'key1';").then([my_map_type](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                                  {utf8_type->decompose(sstring("key1"))},
                                  {my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type{{{1002, 5002}}}))},
                              });
            });
        }).then([&e] {
            // overwrite an element
            return e.execute_cql("update cf set map1[1009] = 5009 where p1 = 'key1';").discard_result();
        }).then([&e] {
            // delete a key
            return e.execute_cql("delete map1[1002] from cf where p1 = 'key1';").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", make_map_value(my_map_type,
                                                      map_type_impl::native_type({{{1009, 5009}}})));
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, map1) values ('key1', null);").discard_result();
        }).then([&e, my_map_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "map1", make_map_value(my_map_type, map_type_impl::native_type({})));
        });
    });
}

SEASTAR_TEST_CASE(test_set_insert_update) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto make_my_set_type = [] { return set_type_impl::get_instance(int32_type, true); };
        auto my_set_type = make_my_set_type();
        return e.create_table([make_my_set_type](sstring ks_name) {
            // CQL: create table cf (p1 varchar primary key, set1 set<int>);
            return schema({}, ks_name, "cf",
                          {{"p1", utf8_type}}, {}, {{"set1", make_my_set_type()}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, set1) values ('key1', { 1001 });").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", make_set_value(my_set_type, set_type_impl::native_type({{1001}})));
        }).then([&e] {
            return e.execute_cql("update cf set set1 = set1 + { 1002 } where p1 = 'key1';").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", make_set_value(my_set_type, set_type_impl::native_type({1001, 1002})));
        }).then([&e] {
            // overwrite an element
            return e.execute_cql("update cf set set1 = set1 + { 1001 } where p1 = 'key1';").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", make_set_value(my_set_type, set_type_impl::native_type({1001, 1002})));
        }).then([&e] {
            // overwrite entire set
            return e.execute_cql("update cf set set1 = { 1007, 1019 } where p1 = 'key1';").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", make_set_value(my_set_type, set_type_impl::native_type({1007, 1019})));
        }).then([&e] {
            // discard keys
            return e.execute_cql("update cf set set1 = set1 - { 1007, 1008 } where p1 = 'key1';").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", make_set_value(my_set_type, set_type_impl::native_type({{1019}})));
        }).then([&e, my_set_type] {
            return e.execute_cql("select * from cf where p1 = 'key1';").then([my_set_type](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                                  {utf8_type->decompose(sstring("key1"))},
                                  {my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type{{1019}}))},
                              });
            });
        }).then([&e] {
            return e.execute_cql("update cf set set1 = set1 + { 1009 } where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("delete set1[1019] from cf where p1 = 'key1';").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", make_set_value(my_set_type, set_type_impl::native_type({{1009}})));
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, set1) values ('key1', null);").discard_result();
        }).then([&e, my_set_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "set1", make_set_value(my_set_type, set_type_impl::native_type({})));
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, set1) values ('key1', {});").discard_result();
        }).then([&e, my_set_type] {
            // Empty non-frozen set is indistinguishable from NULL
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "set1", make_set_value(my_set_type, set_type_impl::native_type({})));
        });
    });
}

SEASTAR_TEST_CASE(test_list_insert_update) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto my_list_type = list_type_impl::get_instance(int32_type, true);
        return e.execute_cql("create table cf (p1 varchar primary key, list1 list<int>);").discard_result().then([&e] {
            return e.execute_cql("insert into cf (p1, list1) values ('key1', [ 1001 ]);").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({{1001}})));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = [ 1002, 1003 ] where p1 = 'key1';").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({1002, 1003})));
        }).then([&e] {
            return e.execute_cql("update cf set list1[1] = 2003 where p1 = 'key1';").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({1002, 2003})));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = list1 - [1002, 2004] where p1 = 'key1';").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({{2003}})));
        }).then([&e, my_list_type] {
            return e.execute_cql("select * from cf where p1 = 'key1';").then([my_list_type] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {utf8_type->decompose(sstring("key1"))},
                         {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type({{2003}})))},
                     });
            });
        }).then([&e] {
            return e.execute_cql("update cf set list1 = [2008, 2009, 2010] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("delete list1[1] from cf where p1 = 'key1';").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({2008, 2010})));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = list1 + [2012, 2019] where p1 = 'key1';").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({2008, 2010, 2012, 2019})));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = [2001, 2002] + list1 where p1 = 'key1';").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({2001, 2002, 2008, 2010, 2012, 2019})));
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, list1) values ('key1', null);").discard_result();
        }).then([&e, my_list_type] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({})));
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, list1) values ('key1', []);").discard_result();
        }).then([&e, my_list_type] {
            // Empty non-frozen list is indistinguishable from NULL
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", make_list_value(my_list_type, list_type_impl::native_type({})));
        });
    });
}

SEASTAR_TEST_CASE(test_functions) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.create_table([](sstring ks_name) {
            // CQL: create table cf (p1 varchar primary key, u uuid, tu timeuuid);
            return schema({}, ks_name, "cf",
                {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"tu", timeuuid_type}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, tu) values ('key1', 1, now());").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, tu) values ('key1', 2, now());").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, tu) values ('key1', 3, now());").discard_result();
        }).then([&e] {
            return e.execute_cql("select tu from cf where p1 in ('key1');");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            using namespace cql_transport::messages;
            struct validator : result_message::visitor {
                std::vector<bytes_opt> res;
                virtual void visit(const result_message::void_message&) override { throw "bad"; }
                virtual void visit(const result_message::set_keyspace&) override { throw "bad"; }
                virtual void visit(const result_message::prepared::cql&) override { throw "bad"; }
                virtual void visit(const result_message::prepared::thrift&) override { throw "bad"; }
                virtual void visit(const result_message::schema_change&) override { throw "bad"; }
                virtual void visit(const result_message::rows& rows) override {
                    const auto& rs = rows.rs().result_set();
                    BOOST_REQUIRE_EQUAL(rs.rows().size(), 3);
                    for (auto&& rw : rs.rows()) {
                        BOOST_REQUIRE_EQUAL(rw.size(), 1);
                        res.push_back(rw[0]);
                    }
                }
                virtual void visit(const result_message::bounce_to_shard& rows) override { throw "bad"; }
            };
            validator v;
            msg->accept(v);
            // No boost::adaptors::sorted
            boost::sort(v.res);
            BOOST_REQUIRE_EQUAL(boost::distance(v.res | boost::adaptors::uniqued), 3);
        }).then([&] {
            return e.execute_cql("select sum(c1), count(c1) from cf where p1 = 'key1';");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {int32_type->decompose(6)},
                     {long_type->decompose(3L)},
                 });
        }).then([&] {
            return e.execute_cql("select count(*) from cf where p1 = 'key1';");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {long_type->decompose(3L)},
                 });
        }).then([&] {
            return e.execute_cql("select count(1) from cf where p1 = 'key1';");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {long_type->decompose(3L)},
                 });
        }).then([&e] {
            // Inane, casting to own type, but couldn't find more interesting example
            return e.execute_cql("insert into cf (p1, c1) values ((text)'key2', 7);").discard_result();
        }).then([&e] {
            return e.execute_cql("select c1 from cf where p1 = 'key2';");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {int32_type->decompose(7)},
                 });
        });
    });
}

struct aggregate_function_test {
    private:
    cql_test_env& _e;
    shared_ptr<const abstract_type> _column_type;
    std::vector<data_value> _sorted_values;

    sstring table_name() {
        sstring tbl_name = "cf_" + _column_type->cql3_type_name();
        // Substitute troublesome characters from `cql3_type_name()':
        std::for_each(tbl_name.begin(), tbl_name.end(), [] (char& c) {
            if (c == '<' || c == '>' || c == ',' || c == ' ') { c = '_'; }
        });
        return tbl_name;
    }
    void call_function_and_expect(const char* fname, data_type type, data_value expected) {
        auto msg = _e.execute_cql(format("select {}(value) from {}", fname, table_name())).get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({type})
            .with_row({
                expected.serialize()
            });
    }
public:
    template<typename... T>
    explicit aggregate_function_test(cql_test_env& e, shared_ptr<const abstract_type> column_type, T... sorted_values)
        : _e(e), _column_type(column_type), _sorted_values{data_value(sorted_values)...}
    {
        const auto cf_name = table_name();
        _e.create_table([column_type, cf_name] (sstring ks_name) {
            return schema({}, ks_name, cf_name,
                {{"pk", utf8_type}}, {{"ck", int32_type}}, {{"value", column_type}}, {}, utf8_type);
        }).get();

        auto prepared = _e.prepare(format("insert into {} (pk, ck, value) values ('key1', ?, ?)", cf_name)).get0();
        for (int i = 0; i < (int)_sorted_values.size(); i++) {
            const auto& value = _sorted_values[i];
            BOOST_ASSERT(&*value.type() == &*_column_type);
            std::vector<cql3::raw_value> raw_values {
                cql3::raw_value::make_value(int32_type->decompose(int32_t(i))),
                cql3::raw_value::make_value(value.serialize())
            };
            _e.execute_prepared(prepared, std::move(raw_values)).get();
        }
    }
    aggregate_function_test& test_min() {
        call_function_and_expect("min", _column_type, _sorted_values.front());
        return *this;
    }
    aggregate_function_test& test_max() {
        call_function_and_expect("max", _column_type, _sorted_values.back());
        return *this;
    }
    aggregate_function_test& test_count() {
        call_function_and_expect("count", long_type, int64_t(_sorted_values.size()));
        return *this;
    }
    aggregate_function_test& test_sum(data_value expected_result) {
        call_function_and_expect("sum", _column_type, expected_result);
        return *this;
    }
    aggregate_function_test& test_avg(data_value expected_result) {
        call_function_and_expect("avg", _column_type, expected_result);
        return *this;
    }
    aggregate_function_test& test_min_max_count() {
        test_min();
        test_max();
        test_count();
        return *this;
    }
};

SEASTAR_TEST_CASE(test_aggregate_functions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Numeric types
        aggregate_function_test(e, byte_type, int8_t(1), int8_t(2), int8_t(3))
            .test_min_max_count()
            .test_sum(int8_t(6))
            .test_avg(int8_t(2));
        aggregate_function_test(e, short_type, int16_t(1), int16_t(2), int16_t(3))
            .test_min_max_count()
            .test_sum(int16_t(6))
            .test_avg(int16_t(2));
        aggregate_function_test(e, int32_type, int32_t(1), int32_t(2), int32_t(3))
            .test_min_max_count()
            .test_sum(int32_t(6))
            .test_avg(int32_t(2));
        aggregate_function_test(e, long_type, int64_t(1), int64_t(2), int64_t(3))
            .test_min_max_count()
            .test_sum(int64_t(6))
            .test_avg(int64_t(2));

        aggregate_function_test(e, varint_type,
            boost::multiprecision::cpp_int(1),
            boost::multiprecision::cpp_int(2),
            boost::multiprecision::cpp_int(3)
        ).test_min_max_count()
            .test_sum(boost::multiprecision::cpp_int(6))
            .test_avg(boost::multiprecision::cpp_int(2));

        aggregate_function_test(e, decimal_type,
            big_decimal("1.0"),
            big_decimal("2.0"),
            big_decimal("3.0")
        ).test_min_max_count()
            .test_sum(big_decimal("6.0"))
            .test_avg(big_decimal("2.0"));

        aggregate_function_test(e, float_type, 1.0f, 2.0f, 3.0f)
            .test_min_max_count()
            .test_sum(6.0f)
            .test_avg(2.0f);
        aggregate_function_test(e, double_type, 1.0, 2.0, 3.0)
            .test_min_max_count()
            .test_sum(6.0)
            .test_avg(2.0);

        // Ordered types
        aggregate_function_test(e, utf8_type, sstring("abcd"), sstring("efgh"), sstring("ijkl"))
            .test_min_max_count();
        aggregate_function_test(e, bytes_type, bytes("abcd"), bytes("efgh"), bytes("ijkl"))
            .test_min_max_count();
        aggregate_function_test(e, ascii_type,
            ascii_native_type{"abcd"},
            ascii_native_type{"efgh"},
            ascii_native_type{"ijkl"}
        ).test_min_max_count();

        aggregate_function_test(e, simple_date_type,
            simple_date_native_type{1},
            simple_date_native_type{2},
            simple_date_native_type{3}
        ).test_min_max_count();

        const db_clock::time_point now = db_clock::now();
        aggregate_function_test(e, timestamp_type,
            now,
            now + std::chrono::seconds(1),
            now + std::chrono::seconds(2)
        ).test_min_max_count();

        aggregate_function_test(e, timeuuid_type,
            timeuuid_native_type{utils::UUID("00000000-0000-1000-0000-000000000000")},
            timeuuid_native_type{utils::UUID("00000000-0000-1000-0000-000000000001")},
            timeuuid_native_type{utils::UUID("00000000-0000-1000-0000-000000000002")}
        ).test_min_max_count();

        aggregate_function_test(e, time_type,
            time_native_type{std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch() - std::chrono::seconds(1)).count()},
            time_native_type{std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch()).count()},
            time_native_type{std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch() + std::chrono::seconds(1)).count()}
        ).test_min_max_count();

        aggregate_function_test(e, uuid_type,
            utils::UUID("00000000-0000-1000-0000-000000000000"),
            utils::UUID("00000000-0000-1000-0000-000000000001"),
            utils::UUID("00000000-0000-1000-0000-000000000002")
        ).test_min_max_count();

        aggregate_function_test(e, boolean_type, false, true).test_min_max_count();

        aggregate_function_test(e, inet_addr_type,
            net::inet_address("0.0.0.0"),
            net::inet_address("::"),
            net::inet_address("::1"),
            net::inet_address("0.0.0.1"),
            net::inet_address("1::1"),
            net::inet_address("1.0.0.1")
        ).test_min_max_count();

        auto list_type_int = list_type_impl::get_instance(int32_type, false);
        aggregate_function_test(e, list_type_int,
            make_list_value(list_type_int, {1, 2, 3}),
            make_list_value(list_type_int, {1, 2, 4}),
            make_list_value(list_type_int, {2, 2, 3})
        ).test_min_max_count();

        auto set_type_int = set_type_impl::get_instance(int32_type, false);
        aggregate_function_test(e, set_type_int,
            make_set_value(set_type_int, {1, 2, 3}),
            make_set_value(set_type_int, {1, 2, 4}),
            make_set_value(set_type_int, {2, 3, 4})
        ).test_min_max_count();

        auto tuple_type_int_text = tuple_type_impl::get_instance({int32_type, utf8_type});
        aggregate_function_test(e, tuple_type_int_text,
            make_tuple_value(tuple_type_int_text, {1, "aaa"}),
            make_tuple_value(tuple_type_int_text, {1, "bbb"}),
            make_tuple_value(tuple_type_int_text, {2, "aaa"})
        ).test_min_max_count();

        auto map_type_int_text = map_type_impl::get_instance(int32_type, utf8_type, false);
        aggregate_function_test(e, map_type_int_text,
            make_map_value(map_type_int_text, {std::make_pair(data_value(1), data_value("asdf"))}),
            make_map_value(map_type_int_text, {std::make_pair(data_value(2), data_value("asdf"))}),
            make_map_value(map_type_int_text, {std::make_pair(data_value(2), data_value("bsdf"))})
        ).test_min_max_count();
    });
}

static const api::timestamp_type the_timestamp = 123456789;
SEASTAR_TEST_CASE(test_writetime_and_ttl) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (p1 varchar primary key, i int, fc frozen<set<int>>, c set<int>);").discard_result().then([&e] {
            auto q = format("insert into cf (p1, i) values ('key1', 1) using timestamp {:d};", the_timestamp);
            return e.execute_cql(q).discard_result();
        }).then([&e] {
            return e.execute_cql("select writetime(i) from cf where p1 in ('key1');");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({{
                     {long_type->decompose(int64_t(the_timestamp))},
                 }});
        }).then([&e] {
            return async([&e] {
                auto ts1 = the_timestamp + 1;
                e.execute_cql(format("UPDATE cf USING TIMESTAMP {:d} SET fc = {{1}}, c = {{2}} WHERE p1 = 'key1'", ts1)).get();
                auto msg1 = e.execute_cql("SELECT writetime(fc) FROM cf").get0();
                assert_that(msg1).is_rows()
                    .with_rows({{
                         {long_type->decompose(int64_t(ts1))},
                     }});
                auto msg2f = futurize_apply([&] { return e.execute_cql("SELECT writetime(c) FROM cf"); });
                msg2f.wait();
                assert_that_failed(msg2f);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_time_overflow_with_default_ttl) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto verify = [&e] (int value, bool bypass_cache) -> future<> {
            auto sq = format("select i from cf where p1 = 'key1' {};", bypass_cache ? "bypass cache" : "");
            return e.execute_cql(sq).then([value] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {int32_type->decompose(value)},
                     });
            });
        };

        auto cr = format("create table cf (p1 varchar primary key, i int) with default_time_to_live = {:d};", max_ttl.count());
        e.execute_cql(cr).get();
        auto q = format("insert into cf (p1, i) values ('key1', 1);");
        e.execute_cql(q).get();
        e.require_column_has_value("cf", {sstring("key1")}, {}, "i", 1).get();
        verify(1, false).get();
        verify(1, true).get();
        e.execute_cql("update cf set i = 2 where p1 = 'key1';").get();
        verify(2, true).get();
        verify(2, false).get();
    });
}

SEASTAR_TEST_CASE(test_time_overflow_using_ttl) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto verify = [&e] (int value, bool bypass_cache) -> future<> {
            auto sq = format("select i from cf where p1 = 'key1' {};", bypass_cache ? "bypass cache" : "");
            return e.execute_cql(sq).then([value] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {int32_type->decompose(value)},
                     });
            });
        };

        auto cr = "create table cf (p1 varchar primary key, i int);";
        e.execute_cql(cr).get();
        auto q = format("insert into cf (p1, i) values ('key1', 1) using ttl {:d};", max_ttl.count());
        e.execute_cql(q).get();
        e.require_column_has_value("cf", {sstring("key1")}, {}, "i", 1).get();
        verify(1, true).get();
        verify(1, false).get();
        q = format("insert into cf (p1, i) values ('key2', 0);");
        e.execute_cql(q).get();
        q = format("update cf using ttl {:d} set i = 2 where p1 = 'key2';", max_ttl.count());
        e.execute_cql(q).get();
        e.require_column_has_value("cf", {sstring("key2")}, {}, "i", 2).get();
        verify(1, false).get();
        verify(1, true).get();
    });
}

SEASTAR_TEST_CASE(test_batch) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&e] {
            return e.execute_cql(
                    "begin unlogged batch \n"
                    "  insert into cf (p1, c1, r1) values ('key1', 1, 100); \n"
                    "  insert into cf (p1, c1, r1) values ('key1', 2, 200); \n"
                    "apply batch;"
                    ).discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {1}, "r1", 100);
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {2}, "r1", 200);
        });
    });
}

SEASTAR_TEST_CASE(test_tuples) {
    auto make_tt = [] { return tuple_type_impl::get_instance({int32_type, long_type, utf8_type}); };
    auto tt = make_tt();
    return do_with_cql_env([tt, make_tt] (cql_test_env& e) {
        return e.create_table([make_tt] (sstring ks_name) {
            // this runs on all cores, so create a local tt for each core:
            auto tt = make_tt();
            // CQL: "create table cf (id int primary key, t tuple<int, bigint, text>);
            return schema({}, ks_name, "cf",
                {{"id", int32_type}}, {}, {{"t", tt}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (1, (1001, 2001, 'abc1'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t from cf where id = 1;");
        }).then([&e, tt] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({{
                     {tt->decompose(make_tuple_value(tt, tuple_type_impl::native_type({int32_t(1001), int64_t(2001), sstring("abc1")})))},
                }});
            return e.execute_cql("create table cf2 (p1 int PRIMARY KEY, r1 tuple<int, bigint, text>)").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values (1, (1, 2, 'abc'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from cf2 where p1 = 1;");
        }).then([&e, tt] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(int32_t(1)), tt->decompose(make_tuple_value(tt, tuple_type_impl::native_type({int32_t(1), int64_t(2), sstring("abc")}))) }
            });
        });
    });
}

namespace {

using std::experimental::source_location;

auto validate_request_failure(
        cql_test_env& env,
        const sstring& request,
        const sstring& expected_message,
        const source_location& loc = source_location::current()) {
    return futurize_apply([&] { return env.execute_cql(request); })
        .then_wrapped([expected_message, loc] (future<shared_ptr<cql_transport::messages::result_message>> f) {
                          BOOST_REQUIRE_EXCEPTION(f.get(),
                                                  exceptions::invalid_request_exception,
                                                  exception_predicate::message_equals(expected_message, loc));
                      });
};

} // anonymous namespace

//
// Since durations don't have a well-defined ordering on their semantic value, a number of restrictions exist on their
// use.
//
SEASTAR_TEST_CASE(test_duration_restrictions) {
    return do_with_cql_env([&] (cql_test_env& env) {
        return make_ready_future<>().then([&] {
            // Disallow "direct" use of durations in ordered collection types to avoid user confusion when their
            // ordering doesn't match expectations.
            return make_ready_future<>().then([&] {
                return validate_request_failure(
                        env,
                        "create type my_type (a set<duration>);",
                        "Durations are not allowed inside sets: set<duration>");
            }).then([&] {
                return validate_request_failure(
                        env,
                        "create type my_type (a map<duration, int>);",
                        "Durations are not allowed as map keys: map<duration, int>");
            });
        }).then([&] {
            // Disallow any type referring to a duration from being used in a primary key of a table or a materialized
            // view.
            return make_ready_future<>().then([&] {
                return validate_request_failure(
                        env,
                        "create table my_table (direct_key duration PRIMARY KEY);",
                        "duration type is not supported for PRIMARY KEY part direct_key");
            }).then([&] {
                return validate_request_failure(
                        env,
                        "create table my_table (collection_key frozen<list<duration>> PRIMARY KEY);",
                        "duration type is not supported for PRIMARY KEY part collection_key");
            }).then([&] {
                return env.execute_cql("create type my_type0 (span duration);").discard_result().then([&] {
                    return validate_request_failure(
                            env,
                            "create table my_table (udt_key frozen<my_type0> PRIMARY KEY);",
                            "duration type is not supported for PRIMARY KEY part udt_key");
                });
            }).then([&] {
                return validate_request_failure(
                        env,
                        "create table my_table (tuple_key tuple<int, duration, int> PRIMARY KEY);",
                        "duration type is not supported for PRIMARY KEY part tuple_key");
            }).then([&] {
                return validate_request_failure(
                        env,
                        "create table my_table (a int, b duration, PRIMARY KEY ((a), b)) WITH CLUSTERING ORDER BY (b DESC);",
                        "duration type is not supported for PRIMARY KEY part b");
            }).then([&] {
                return env.execute_cql("create table my_table0 (key int PRIMARY KEY, name text, span duration);")
                        .discard_result().then([&] {
                            return validate_request_failure(
                                    env,
                                    "create materialized view my_mv as"
                                    " select * from my_table0 "
                                    " primary key (key, span);",
                                    "Cannot use Duration column 'span' in PRIMARY KEY of materialized view");
                        });
            });
        }).then([&] {
            // Disallow creating secondary indexes on durations.
            return validate_request_failure(
                    env,
                    "create index my_index on my_table0 (span);",
                    "Secondary indexes are not supported on duration columns");
        }).then([&] {
            // Disallow slice-based restrictions and conditions on durations.
            //
            // Note that multi-column restrictions are only supported on clustering columns (which cannot be `duration`)
            // and that multi-column conditions are not supported in the grammar.
            return make_ready_future<>().then([&] {
                return validate_request_failure(
                        env,
                        "select * from my_table0 where key = 0 and span < 3d;",
                        "Slice restrictions are not supported on duration columns");
            }).then([&] {
                return validate_request_failure(
                        env,
                        "update my_table0 set name = 'joe' where key = 0 if span >= 5m",
                        "Slice conditions are not supported on durations");
            });
        });
    });
}

SEASTAR_TEST_CASE(test_select_multiple_ranges) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (p1 varchar, r1 int, PRIMARY KEY (p1));").discard_result().then([&e] {
            return e.execute_cql(
                    "begin unlogged batch \n"
                    "  insert into cf (p1, r1) values ('key1', 100); \n"
                    "  insert into cf (p1, r1) values ('key2', 200); \n"
                    "apply batch;"
            ).discard_result();
        }).then([&e] {
            return e.execute_cql("select r1 from cf where p1 in ('key1', 'key2');");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_size(2).with_row({
                {int32_type->decompose(100)}
            }).with_row({
                {int32_type->decompose(200)}
            });

        });
    });
}

SEASTAR_TEST_CASE(test_validate_keyspace) {
    return do_with_cql_env([] (cql_test_env& e) {
        return make_ready_future<>().then([&e] {
            return e.execute_cql("create keyspace kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkssssssssssssssssssssssssssssssssssssssssssssss with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace ks3-1 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace ks3 with replication = { 'replication_factor' : 1 };");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace ks3 with rreplication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace SyStEm with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
        });
    });
}

SEASTAR_TEST_CASE(test_validate_table) {
    return do_with_cql_env([] (cql_test_env& e) {
        return make_ready_future<>().then([&e] {
            return e.execute_cql("create table ttttttttttttttttttttttttttttttttttttttttbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (foo text PRIMARY KEY, bar text);");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, foo text);");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb-1 (foo text PRIMARY KEY, bar text);");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text, bar text);");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text PRIMARY KEY);");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text) with commment = 'aaaa';");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text) with min_index_interval = -1;");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text) with min_index_interval = 1024 and max_index_interval = 128;");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
        });
    });
}

SEASTAR_TEST_CASE(test_table_compression) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table tb1 (foo text PRIMARY KEY, bar text) with compression = { };").get();
        e.require_table_exists("ks", "tb1").get();
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb1")->get_compressor_params().get_compressor() == nullptr);

        e.execute_cql("create table tb5 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : '' };").get();
        e.require_table_exists("ks", "tb5").get();
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb5")->get_compressor_params().get_compressor() == nullptr);

        BOOST_REQUIRE_THROW(e.execute_cql(
                "create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LossyCompressor' };"),
                std::exception);
        BOOST_REQUIRE_THROW(e.execute_cql(
                "create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : -1 };"),
                std::exception);
        BOOST_REQUIRE_THROW(e.execute_cql(
                "create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 3 };"),
                std::exception);

        e.execute_cql("create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 2 };").get();
        e.require_table_exists("ks", "tb2").get();
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb2")->get_compressor_params().get_compressor() == compressor::lz4);
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb2")->get_compressor_params().chunk_length() == 2 * 1024);

        e.execute_cql("create table tb3 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'DeflateCompressor' };").get();
        e.require_table_exists("ks", "tb3").get();
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb3")->get_compressor_params().get_compressor() == compressor::deflate);

        e.execute_cql("create table tb4 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.DeflateCompressor' };").get();
        e.require_table_exists("ks", "tb4").get();
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb4")->get_compressor_params().get_compressor() == compressor::deflate);

        e.execute_cql("create table tb6 (foo text PRIMARY KEY, bar text);").get();
        e.require_table_exists("ks", "tb6").get();
        BOOST_REQUIRE(e.local_db().find_schema("ks", "tb6")->get_compressor_params().get_compressor() == compressor::lz4);
    });
}

SEASTAR_TEST_CASE(test_ttl) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto make_my_list_type = [] { return list_type_impl::get_instance(utf8_type, true); };
        auto my_list_type = make_my_list_type();
        return e.create_table([make_my_list_type] (sstring ks_name) {
            return schema({}, ks_name, "cf",
                {{"p1", utf8_type}}, {}, {{"r1", utf8_type}, {"r2", utf8_type}, {"r3", make_my_list_type()}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql(
                "update cf using ttl 100000 set r1 = 'value1_1', r3 = ['a', 'b', 'c'] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql(
                "update cf using ttl 100 set r1 = 'value1_3', r3 = ['a', 'b', 'c'] where p1 = 'key3';").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf using ttl 100 set r3[1] = 'b' where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf using ttl 100 set r1 = 'value1_2' where p1 = 'key2';").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, r2) values ('key2', 'value2_2');").discard_result();
        }).then([&e, my_list_type] {
            return e.execute_cql("select r1 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(3)
                    .with_row({utf8_type->decompose(sstring("value1_1"))})
                    .with_row({utf8_type->decompose(sstring("value1_2"))})
                    .with_row({utf8_type->decompose(sstring("value1_3"))});
            });
        }).then([&e, my_list_type] {
            return e.execute_cql("select r3 from cf where p1 = 'key1';").then([my_list_type] (shared_ptr<cql_transport::messages::result_message> msg) {
                auto my_list_type = list_type_impl::get_instance(utf8_type, true);
                assert_that(msg).is_rows().with_rows({
                    {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{sstring("a"), sstring("b"), sstring("c")}}))}
                });
            });
        }).then([&e] {
            forward_jump_clocks(200s);
            return e.execute_cql("select r1, r2 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({{}, utf8_type->decompose(sstring("value2_2"))})
                    .with_row({utf8_type->decompose(sstring("value1_1")), {}});
            });
        }).then([&e] {
            return e.execute_cql("select r2 from cf;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({ utf8_type->decompose(sstring("value2_2")) })
                    .with_row({ {} });
            });
        }).then([&e] {
            return e.execute_cql("select r1 from cf;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({ {} })
                    .with_row({ utf8_type->decompose(sstring("value1_1")) });
            });
        }).then([&e, my_list_type] {
            return e.execute_cql("select r3 from cf where p1 = 'key1';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                auto my_list_type = list_type_impl::get_instance(utf8_type, true);
                assert_that(msg).is_rows().with_rows({
                    {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{sstring("a"), sstring("c")}}))}
                });
            });
        }).then([&e] {
            return e.execute_cql("create table cf2 (p1 text PRIMARY KEY, r1 text, r2 text);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 500;").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), utf8_type->decompose(sstring("bar"))}
                });
            });
        }).then([&e] {
            forward_jump_clocks(600s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("select count(*) from cf2;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {long_type->decompose(int64_t(0))}
                });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 500;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf2 set r1 = null where p1 = 'foo';").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), { }}
                });
            });
        }).then([&e] {
            forward_jump_clocks(600s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 500;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r2) values ('foo', null);").discard_result();
        }).then([&e] {
            forward_jump_clocks(600s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), { }}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_types) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
            "CREATE TABLE all_types ("
                "    a ascii PRIMARY KEY,"
                "    b bigint,"
                "    c blob,"
                "    d boolean,"
                "    e double,"
                "    f float,"
                "    g inet,"
                "    h int,"
                "    i text,"
                "    j timestamp,"
                "    k timeuuid,"
                "    l uuid,"
                "    m varchar,"
                "    n varint,"
                "    o decimal,"
                "    p tinyint,"
                "    q smallint,"
                "    r date,"
                "    s time,"
                "    u duration,"
                ");").get();
        e.require_table_exists("ks", "all_types").get();

        e.execute_cql(
            "INSERT INTO all_types (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, u) VALUES ("
                "    'ascii',"
                "    123456789,"
                "    0xdeadbeef,"
                "    true,"
                "    3.14,"
                "    3.14,"
                "    '127.0.0.1',"
                "    3,"
                "    'zażółć gęślą jaźń',"
                "    '2001-10-18 14:15:55.134+0000',"
                "    d2177dd0-eaa2-11de-a572-001b779c76e3,"
                "    d2177dd0-eaa2-11de-a572-001b779c76e3,"
                "    'varchar',"
                "    123,"
                "    1.23,"
                "    3,"
                "    3,"
                "    '1970-01-02',"
                "    '00:00:00.000000001',"
                "    1y2mo3w4d5h6m7s8ms9us10ns"
                ");").get();

        {
            auto msg = e.execute_cql("SELECT * FROM all_types WHERE a = 'ascii'").get0();
            struct tm t = { 0 };
            t.tm_year = 2001 - 1900;
            t.tm_mon = 10 - 1;
            t.tm_mday = 18;
            t.tm_hour = 14;
            t.tm_min = 15;
            t.tm_sec = 55;
            auto tp = db_clock::from_time_t(timegm(&t)) + std::chrono::milliseconds(134);
            assert_that(msg).is_rows().with_rows({
                {
                    ascii_type->decompose(sstring("ascii")), long_type->decompose(123456789l),
                    from_hex("deadbeef"), boolean_type->decompose(true),
                    double_type->decompose(3.14), float_type->decompose(3.14f),
                    inet_addr_type->decompose(net::inet_address("127.0.0.1")),
                    int32_type->decompose(3), utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                    timestamp_type->decompose(tp),
                    timeuuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    uuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    utf8_type->decompose(sstring("varchar")), varint_type->decompose(boost::multiprecision::cpp_int(123)),
                    decimal_type->decompose(big_decimal { 2, boost::multiprecision::cpp_int(123) }),
                    byte_type->decompose(int8_t(3)),
                    short_type->decompose(int16_t(3)),
                    serialized(simple_date_native_type{0x80000001}),
                    time_type->decompose(int64_t(0x0000000000000001)),
                    duration_type->decompose(cql_duration("1y2mo3w4d5h6m7s8ms9us10ns"))
                }
            });
        }

        e.execute_cql(
            "INSERT INTO all_types (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, u) VALUES ("
                "    blobAsAscii(asciiAsBlob('ascii2')),"
                "    blobAsBigint(bigintAsBlob(123456789)),"
                "    bigintAsBlob(12),"
                "    blobAsBoolean(booleanAsBlob(true)),"
                "    blobAsDouble(doubleAsBlob(3.14)),"
                "    blobAsFloat(floatAsBlob(3.14)),"
                "    blobAsInet(inetAsBlob('127.0.0.1')),"
                "    blobAsInt(intAsBlob(3)),"
                "    blobAsText(textAsBlob('zażółć gęślą jaźń')),"
                "    blobAsTimestamp(timestampAsBlob('2001-10-18 14:15:55.134+0000')),"
                "    blobAsTimeuuid(timeuuidAsBlob(d2177dd0-eaa2-11de-a572-001b779c76e3)),"
                "    blobAsUuid(uuidAsBlob(d2177dd0-eaa2-11de-a572-001b779c76e3)),"
                "    blobAsVarchar(varcharAsBlob('varchar')), blobAsVarint(varintAsBlob(123)),"
                "    blobAsDecimal(decimalAsBlob(1.23)),"
                "    blobAsTinyint(tinyintAsBlob(3)),"
                "    blobAsSmallint(smallintAsBlob(3)),"
                "    blobAsDate(dateAsBlob('1970-01-02')),"
                "    blobAsTime(timeAsBlob('00:00:00.000000001')),"
                "    blobAsDuration(durationAsBlob(10y9mo8w7d6h5m4s3ms2us1ns))"
                ");").get();

        {
            auto msg = e.execute_cql("SELECT * FROM all_types WHERE a = 'ascii2'").get0();
            struct tm t = {0};
            t.tm_year = 2001 - 1900;
            t.tm_mon = 10 - 1;
            t.tm_mday = 18;
            t.tm_hour = 14;
            t.tm_min = 15;
            t.tm_sec = 55;
            auto tp = db_clock::from_time_t(timegm(&t)) + std::chrono::milliseconds(134);
            assert_that(msg).is_rows().with_rows({
                {
                    ascii_type->decompose(sstring("ascii2")), long_type->decompose(123456789l),
                    from_hex("000000000000000c"), boolean_type->decompose(true),
                    double_type->decompose(3.14), float_type->decompose(3.14f),
                    inet_addr_type->decompose(net::inet_address("127.0.0.1")),
                    int32_type->decompose(3), utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                    timestamp_type->decompose(tp),
                    timeuuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    uuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    utf8_type->decompose(sstring("varchar")), varint_type->decompose(boost::multiprecision::cpp_int(123)),
                    decimal_type->decompose(big_decimal { 2, boost::multiprecision::cpp_int(123) }),
                    byte_type->decompose(int8_t(3)),
                    short_type->decompose(int16_t(3)),
                    serialized(simple_date_native_type{0x80000001}),
                    time_type->decompose(int64_t(0x0000000000000001)),
                    duration_type->decompose(cql_duration("10y9mo8w7d6h5m4s3ms2us1ns"))
                }
            });
        }
    });
}

SEASTAR_TEST_CASE(test_order_by) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table torder (p1 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY(p1, c1, c2));").discard_result().get();
        e.require_table_exists("ks", "torder").get();

        e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 1, 2, 3);").get();
        e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 2, 1, 0);").get();

        {
            auto msg = e.execute_cql("select  c1, c2, r1 from torder where p1 = 0 order by c1 asc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
        }
        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 desc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
            });
        }

        e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 1, 1, 4);").get();
        e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 2, 2, 5);").get();
        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 desc, c2 desc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
            });
        }

        e.execute_cql("insert into torder (p1, c1, c2, r1) values (1, 1, 0, 6);").get();
        e.execute_cql("insert into torder (p1, c1, c2, r1) values (1, 2, 3, 7);").get();

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 desc, c2 desc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(7)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
                {int32_type->decompose(1), int32_type->decompose(0), int32_type->decompose(6)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 asc, c2 asc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(0), int32_type->decompose(6)},
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(7)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) and c1 < 2 order by c1 desc, c2 desc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) and c1 >= 2 order by c1 asc, c2 asc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 desc, c2 desc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(7)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 asc, c2 asc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(0), int32_type->decompose(6)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 > 1 order by c1 desc, c2 desc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 desc, c2 desc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 desc, c2 desc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 desc, c2 desc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });

        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 > 1 order by c1 asc, c2 asc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 asc, c2 asc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 asc, c2 asc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
        }

        {
            auto msg = e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 asc, c2 asc limit 1;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
            });
        }
    });
}

SEASTAR_TEST_CASE(test_order_by_validate) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table torderv (p1 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY(p1, c1, c2));").discard_result().then([&e] {
            return e.execute_cql("select c2, r1 from torderv where p1 = 0 order by c desc;");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("select c2, r1 from torderv where p1 = 0 order by c2 desc;");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("select c2, r1 from torderv where p1 = 0 order by c1 desc, c2 asc;");
        }).then_wrapped([&e] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
            return e.execute_cql("select c2, r1 from torderv order by c1 asc;");
        }).then_wrapped([] (future<shared_ptr<cql_transport::messages::result_message>> f) {
            assert_that_failed(f);
        });
    });
}

SEASTAR_TEST_CASE(test_multi_column_restrictions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table tmcr (p1 int, c1 int, c2 int, c3 int, r1 int, PRIMARY KEY (p1, c1, c2, c3));").get();
        e.require_table_exists("ks", "tmcr").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 0, 0, 0);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 0, 1, 1);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 1, 0, 2);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 1, 1, 3);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 0, 0, 4);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 0, 1, 5);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 1, 0, 6);").get();
        e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 1, 1, 7);").get();
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) = (0, 1, 1);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2) = (0, 1);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) in ((0, 1, 0), (1, 0, 1), (0, 1, 0));").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(5)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2) in ((0, 1), (1, 0), (0, 1));").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
                {int32_type->decompose(4)},
                {int32_type->decompose(5)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) >= (1, 0, 1);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(5)},
                {int32_type->decompose(6)},
                {int32_type->decompose(7)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) >= (0, 1, 1) and (c1, c2, c3) < (1, 1, 0);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
                {int32_type->decompose(4)},
                {int32_type->decompose(5)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2) >= (0, 1) and (c1, c2, c3) < (1, 0, 1);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
                {int32_type->decompose(4)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) > (0, 1, 0) and (c1, c2) <= (0, 1);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
            });
        }
    });
}

SEASTAR_TEST_CASE(test_select_distinct) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table tsd (p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1));").get();
        e.require_table_exists("ks", "tsd").get();
        e.execute_cql("insert into tsd (p1, c1, r1) values (0, 0, 0);").get();
        e.execute_cql("insert into tsd (p1, c1, r1) values (1, 1, 1);").get();
        e.execute_cql("insert into tsd (p1, c1, r1) values (1, 1, 2);").get();
        e.execute_cql("insert into tsd (p1, c1, r1) values (2, 2, 2);").get();
        e.execute_cql("insert into tsd (p1, c1, r1) values (2, 3, 3);").get();
        {
            auto msg = e.execute_cql("select distinct p1 from tsd;").get0();
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0)})
                .with_row({int32_type->decompose(1)})
                .with_row({int32_type->decompose(2)});
        }
        {
            auto msg = e.execute_cql("select distinct p1 from tsd limit 3;").get0();
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0)})
                .with_row({int32_type->decompose(1)})
                .with_row({int32_type->decompose(2)});
        }

        e.execute_cql("create table tsd2 (p1 int, p2 int, c1 int, r1 int, PRIMARY KEY ((p1, p2), c1));").get();
        e.require_table_exists("ks", "tsd2").get();
        e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (0, 0, 0, 0);").get();
        e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (0, 0, 1, 1);").get();
        e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (1, 1, 0, 0);").get();
        e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (1, 1, 1, 1);").get();
        e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (2, 2, 0, 0);").get();
        e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (2, 2, 1, 1);").get();
        {
            auto msg = e.execute_cql("select distinct p1, p2 from tsd2;").get0();
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0), int32_type->decompose(0)})
                .with_row({int32_type->decompose(1), int32_type->decompose(1)})
                .with_row({int32_type->decompose(2), int32_type->decompose(2)});
        }
        {
            auto msg = e.execute_cql("select distinct p1, p2 from tsd2 limit 3;").get0();
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0), int32_type->decompose(0)})
                .with_row({int32_type->decompose(1), int32_type->decompose(1)})
                .with_row({int32_type->decompose(2), int32_type->decompose(2)});
        }

        e.execute_cql("create table tsd3 (p1 int, r1 int, PRIMARY KEY (p1));").get();
        e.execute_cql("insert into tsd3 (p1, r1) values (0, 0);").get();
        e.execute_cql("insert into tsd3 (p1, r1) values (1, 1);").get();
        e.execute_cql("insert into tsd3 (p1, r1) values (1, 2);").get();
        e.execute_cql("insert into tsd3 (p1, r1) values (2, 2);").get();
        {
            auto msg = e.execute_cql("select distinct p1 from tsd3;").get0();
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0)})
                .with_row({int32_type->decompose(1)})
                .with_row({int32_type->decompose(2)});
        }

        e.execute_cql("create table tsd4 (p1 int, c1 int, s1 int static, r1 int, PRIMARY KEY (p1, c1));").get();
        e.execute_cql("insert into tsd4 (p1, c1, s1, r1) values (0, 0, 0, 0);").get();
        e.execute_cql("insert into tsd4 (p1, c1, r1) values (0, 1, 1);").get();
        e.execute_cql("insert into tsd4 (p1, s1) values (2, 1);").get();
        e.execute_cql("insert into tsd4 (p1, s1) values (3, 2);").get();
        {
            auto msg = e.execute_cql("select distinct p1, s1 from tsd4;").get0();
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0), int32_type->decompose(0)})
                .with_row({int32_type->decompose(2), int32_type->decompose(1)})
                .with_row({int32_type->decompose(3), int32_type->decompose(2)});
        }
    });
}

SEASTAR_TEST_CASE(test_select_distinct_with_where_clause) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("CREATE TABLE cf (k int, a int, b int, PRIMARY KEY (k, a))").get();
            for (int i = 0; i < 10; i++) {
                e.execute_cql(format("INSERT INTO cf (k, a, b) VALUES ({:d}, {:d}, {:d})", i, i, i)).get();
                e.execute_cql(format("INSERT INTO cf (k, a, b) VALUES ({:d}, {:d}, {:d})", i, i * 10, i * 10)).get();
            }
            BOOST_REQUIRE_THROW(e.execute_cql("SELECT DISTINCT k FROM cf WHERE a >= 80 ALLOW FILTERING").get(), std::exception);
            BOOST_REQUIRE_THROW(e.execute_cql("SELECT DISTINCT k FROM cf WHERE k IN (1, 2, 3) AND a = 10").get(), std::exception);
            BOOST_REQUIRE_THROW(e.execute_cql("SELECT DISTINCT k FROM cf WHERE b = 5").get(), std::exception);

            assert_that(e.execute_cql("SELECT DISTINCT k FROM cf WHERE k = 1").get0())
                .is_rows().with_size(1)
                .with_row({int32_type->decompose(1)});

            assert_that(e.execute_cql("SELECT DISTINCT k FROM cf WHERE k IN (5, 6, 7)").get0())
               .is_rows().with_size(3)
               .with_row({int32_type->decompose(5)})
               .with_row({int32_type->decompose(6)})
               .with_row({int32_type->decompose(7)});

            // static columns
            e.execute_cql("CREATE TABLE cf2 (k int, a int, s int static, b int, PRIMARY KEY (k, a))").get();
            for (int i = 0; i < 10; i++) {
                e.execute_cql(format("INSERT INTO cf2 (k, a, b, s) VALUES ({:d}, {:d}, {:d}, {:d})", i, i, i, i)).get();
                e.execute_cql(format("INSERT INTO cf2 (k, a, b, s) VALUES ({:d}, {:d}, {:d}, {:d})", i, i * 10, i * 10, i * 10)).get();
            }
            assert_that(e.execute_cql("SELECT DISTINCT s FROM cf2 WHERE k = 5").get0())
                .is_rows().with_size(1)
                .with_row({int32_type->decompose(50)});

            assert_that(e.execute_cql("SELECT DISTINCT s FROM cf2 WHERE k IN (5, 6, 7)").get0())
               .is_rows().with_size(3)
               .with_row({int32_type->decompose(50)})
               .with_row({int32_type->decompose(60)})
               .with_row({int32_type->decompose(70)});
        });
    });
}

SEASTAR_TEST_CASE(test_batch_insert_statement) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&e] {
            return e.execute_cql(R"(BEGIN BATCH
insert into cf (p1, c1, r1) values ('key1', 1, 100);
insert into cf (p1, c1, r1) values ('key2', 2, 200);
APPLY BATCH;)"
            ).discard_result();
        }).then([&e] {
            return e.execute_cql(R"(BEGIN BATCH
update cf set r1 = 66 where p1 = 'key1' and c1 = 1;
update cf set r1 = 33 where p1 = 'key2' and c1 = 2;
APPLY BATCH;)"
            ).discard_result();

        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {1}, "r1", 66);
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key2")}, {2}, "r1", 33);
        });
    });
}

SEASTAR_TEST_CASE(test_in_restriction) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table tir (p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1));").get();
        e.require_table_exists("ks", "tir").get();
        e.execute_cql("insert into tir (p1, c1, r1) values (0, 0, 0);").get();
        e.execute_cql("insert into tir (p1, c1, r1) values (1, 0, 1);").get();
        e.execute_cql("insert into tir (p1, c1, r1) values (1, 1, 2);").get();
        e.execute_cql("insert into tir (p1, c1, r1) values (1, 2, 3);").get();
        e.execute_cql("insert into tir (p1, c1, r1) values (2, 3, 4);").get();
        {
            auto msg = e.execute_cql("select * from tir where p1 in ();").get0();
            assert_that(msg).is_rows().with_size(0);
        }
        {
            auto msg = e.execute_cql("select r1 from tir where p1 in (2, 0, 2, 1);").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                {int32_type->decompose(4)},
                {int32_type->decompose(0)},
                {int32_type->decompose(1)},
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tir where p1 = 1 and c1 in ();").get0();
            assert_that(msg).is_rows().with_size(0);
        }
        {
            auto msg = e.execute_cql("select r1 from tir where p1 = 1 and c1 in (2, 0, 2, 1);").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1)},
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
        }
        {
            auto msg = e.execute_cql("select r1 from tir where p1 = 1 and c1 in (2, 0, 2, 1) order by c1 desc;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
                {int32_type->decompose(2)},
                {int32_type->decompose(1)},
            });
        }
        {
            auto prepared_id = e.prepare("select r1 from tir where p1 in ?;").get0();
            auto my_list_type = list_type_impl::get_instance(int32_type, true);
            std::vector<cql3::raw_value> raw_values;
            auto in_values_list = my_list_type->decompose(make_list_value(my_list_type,
                    list_type_impl::native_type{{int(2), int(0), int(2), int(1)}}));
            raw_values.emplace_back(cql3::raw_value::make_value(in_values_list));
            auto msg = e.execute_prepared(prepared_id,raw_values).get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                {int32_type->decompose(4)},
                {int32_type->decompose(0)},
                {int32_type->decompose(1)},
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
        }

        e.execute_cql("create table tir2 (p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1,r1));").get();
        e.require_table_exists("ks", "tir2").get();
        e.execute_cql("insert into tir2 (p1, c1, r1) values (0, 0, 0);").get();
        e.execute_cql("insert into tir2 (p1, c1, r1) values (1, 0, 1);").get();
        e.execute_cql("insert into tir2 (p1, c1, r1) values (1, 1, 2);").get();
        e.execute_cql("insert into tir2 (p1, c1, r1) values (1, 2, 3);").get();
        e.execute_cql("insert into tir2 (p1, c1, r1) values (2, 3, 4);").get();
        {
            auto msg = e.execute_cql("select r1 from tir2 where (c1,r1) in ((0, 1),(1,2),(0,1),(1,2),(3,3)) ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(1)},
            });
        }
        {
            auto prepared_id = e.prepare("select r1 from tir2 where (c1,r1) in ? ALLOW FILTERING;").get0();
            auto my_tuple_type = tuple_type_impl::get_instance({int32_type,int32_type});
            auto my_list_type = list_type_impl::get_instance(my_tuple_type, true);
            std::vector<tuple_type_impl::native_type> native_tuples = {
                    {int(0), int(1)},
                    {int(1), int(2)},
                    {int(0), int(1)},
                    {int(1), int(2)},
                    {int(3), int(3)},
            };
            std::vector<data_value> tuples;
            for (auto&& native_tuple : native_tuples ) {
                    tuples.emplace_back(make_tuple_value(my_tuple_type,native_tuple));
            }

            std::vector<cql3::raw_value> raw_values;
            auto in_values_list = my_list_type->decompose(make_list_value(my_list_type,tuples));
            raw_values.emplace_back(cql3::raw_value::make_value(in_values_list));
            auto msg = e.execute_prepared(prepared_id,raw_values).get0();
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(1)},
            });
        }
    });
}

SEASTAR_TEST_CASE(test_compact_storage) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table tcs (p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1)) with compact storage;").discard_result().then([&e] {
            return e.require_table_exists("ks", "tcs");
        }).then([&e] {
            return e.execute_cql("insert into tcs (p1, c1, r1) values (1, 2, 3);").discard_result();
        }).then([&e] {
            return e.require_column_has_value("tcs", {1}, {2}, "r1", 3);
        }).then([&e] {
            return e.execute_cql("update tcs set r1 = 4 where p1 = 1 and c1 = 2;").discard_result();
        }).then([&e] {
            return e.require_column_has_value("tcs", {1}, {2}, "r1", 4);
        }).then([&e] {
            return e.execute_cql("select * from tcs where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(4) },
            });
            return e.execute_cql("create table tcs2 (p1 int, c1 int, PRIMARY KEY (p1, c1)) with compact storage;").discard_result();
        }).then([&e] {
            return e.require_table_exists("ks", "tcs2");
        }).then([&e] {
            return e.execute_cql("insert into tcs2 (p1, c1) values (1, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs2 where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2) },
            });
            return e.execute_cql("create table tcs3 (p1 int, c1 int, c2 int, r1 int, PRIMARY KEY (p1, c1, c2)) with compact storage;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs3 (p1, c1, c2, r1) values (1, 2, 3, 4);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs3 (p1, c1, r1) values (1, 2, 5);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs3 (p1, c1, r1) values (1, 3, 6);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs3 (p1, c1, c2, r1) values (1, 3, 5, 7);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs3 (p1, c1, c2, r1) values (1, 3, blobasint(0x), 8);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs3 where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), {}, int32_type->decompose(5) },
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4) },
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
                { int32_type->decompose(1), int32_type->decompose(3), bytes(), int32_type->decompose(8) },
                { int32_type->decompose(1), int32_type->decompose(3), int32_type->decompose(5), int32_type->decompose(7) },
            });
            return e.execute_cql("delete from tcs3 where p1 = 1 and c1 = 2;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs3 where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
                { int32_type->decompose(1), int32_type->decompose(3), bytes(), int32_type->decompose(8) },
                { int32_type->decompose(1), int32_type->decompose(3), int32_type->decompose(5), int32_type->decompose(7) },
            });
            return e.execute_cql("delete from tcs3 where p1 = 1 and c1 = 3 and c2 = 5;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs3 where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
                { int32_type->decompose(1), int32_type->decompose(3), bytes(), int32_type->decompose(8) },
            });
            return e.execute_cql("delete from tcs3 where p1 = 1 and c1 = 3 and c2 = blobasint(0x);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs3 where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
            });
            return e.execute_cql("create table tcs4 (p1 int PRIMARY KEY, c1 int, c2 int) with compact storage;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs4 (p1) values (1);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs4;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({ });
        });
    });
}

SEASTAR_TEST_CASE(test_collections_of_collections) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto set_of_ints = set_type_impl::get_instance(int32_type, true);
        auto set_of_sets = set_type_impl::get_instance(set_of_ints, true);
        auto map_of_sets = map_type_impl::get_instance(set_of_ints, int32_type, true);
        return e.execute_cql("create table cf_sos (p1 int PRIMARY KEY, v set<frozen<set<int>>>);").discard_result().then([&e] {
            return e.execute_cql("insert into cf_sos (p1, v) values (1, {{1, 2}, {3, 4}, {5, 6}});").discard_result();
        }).then([&e, set_of_sets, set_of_ints] {
            return e.require_column_has_value("cf_sos", {1}, {},
                "v", make_set_value(set_of_sets, set_type_impl::native_type({
                    make_set_value(set_of_ints, set_type_impl::native_type({1, 2})),
                    make_set_value(set_of_ints, set_type_impl::native_type({3, 4})),
                    make_set_value(set_of_ints, set_type_impl::native_type({5, 6})),
                })));
        }).then([&e, set_of_sets, set_of_ints] {
            return e.execute_cql("delete v[{3, 4}] from cf_sos where p1 = 1;").discard_result();
        }).then([&e, set_of_sets, set_of_ints] {
            return e.require_column_has_value("cf_sos", {1}, {},
                "v", make_set_value(set_of_sets, set_type_impl::native_type({
                    make_set_value(set_of_ints, set_type_impl::native_type({1, 2})),
                    make_set_value(set_of_ints, set_type_impl::native_type({5, 6})),
                })));
        }).then([&e, set_of_sets, set_of_ints] {
            return e.execute_cql("update cf_sos set v = v - {{1, 2}, {5}} where p1 = 1;").discard_result();
        }).then([&e, set_of_sets, set_of_ints] {
            return e.require_column_has_value("cf_sos", {1}, {},
                "v", make_set_value(set_of_sets, set_type_impl::native_type({
                    make_set_value(set_of_ints, set_type_impl::native_type({5, 6})),
                })));
        }).then([&e] {
            return e.execute_cql("create table cf_mos (p1 int PRIMARY KEY, v map<frozen<set<int>>, int>);").discard_result();
        }).then([&e, map_of_sets, set_of_ints] {
            return e.execute_cql("insert into cf_mos (p1, v) values (1, {{1, 2}: 7, {3, 4}: 8, {5, 6}: 9});").discard_result();
        }).then([&e, map_of_sets, set_of_ints] {
            return e.require_column_has_value("cf_mos", {1}, {},
                "v", make_map_value(map_of_sets, map_type_impl::native_type({
                    { make_set_value(set_of_ints, set_type_impl::native_type({1, 2})), 7 },
                    { make_set_value(set_of_ints, set_type_impl::native_type({3, 4})), 8 },
                    { make_set_value(set_of_ints, set_type_impl::native_type({5, 6})), 9 },
                })));
        }).then([&e, map_of_sets, set_of_ints] {
            return e.execute_cql("delete v[{3, 4}] from cf_mos where p1 = 1;").discard_result();
        }).then([&e, map_of_sets, set_of_ints] {
            return e.require_column_has_value("cf_mos", {1}, {},
                "v", make_map_value(map_of_sets, map_type_impl::native_type({
                    { make_set_value(set_of_ints, set_type_impl::native_type({1, 2})), 7 },
                    { make_set_value(set_of_ints, set_type_impl::native_type({5, 6})), 9 },
                })));
        }).then([&e, map_of_sets, set_of_ints] {
            return e.execute_cql("update cf_mos set v = v - {{1, 2}, {5}} where p1 = 1;").discard_result();
        }).then([&e, map_of_sets, set_of_ints] {
            return e.require_column_has_value("cf_mos", {1}, {},
                "v", make_map_value(map_of_sets, map_type_impl::native_type({
                    { make_set_value(set_of_ints, set_type_impl::native_type({5, 6})), 9 },
                })));
        });
    });
}


SEASTAR_TEST_CASE(test_result_order) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table tro (p1 int, c1 text, r1 int, PRIMARY KEY (p1, c1)) with compact storage;").discard_result().then([&e] {
            return e.execute_cql("insert into tro (p1, c1, r1) values (1, 'z', 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tro (p1, c1, r1) values (1, 'bbbb', 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tro (p1, c1, r1) values (1, 'a', 3);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tro (p1, c1, r1) values (1, 'aaa', 4);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tro (p1, c1, r1) values (1, 'bb', 5);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tro (p1, c1, r1) values (1, 'cccc', 6);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tro where p1 = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), utf8_type->decompose(sstring("a")), int32_type->decompose(3) },
                { int32_type->decompose(1), utf8_type->decompose(sstring("aaa")), int32_type->decompose(4) },
                { int32_type->decompose(1), utf8_type->decompose(sstring("bb")), int32_type->decompose(5) },
                { int32_type->decompose(1), utf8_type->decompose(sstring("bbbb")), int32_type->decompose(2) },
                { int32_type->decompose(1), utf8_type->decompose(sstring("cccc")), int32_type->decompose(6) },
                { int32_type->decompose(1), utf8_type->decompose(sstring("z")), int32_type->decompose(1) },
            });
        });
    });
}

SEASTAR_TEST_CASE(test_frozen_collections) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto set_of_ints = set_type_impl::get_instance(int32_type, false);
        auto list_of_ints = list_type_impl::get_instance(int32_type, false);
        auto frozen_map_of_set_and_list = map_type_impl::get_instance(set_of_ints, list_of_ints, false);
        return e.execute_cql("CREATE TABLE tfc (a int, b int, c frozen<map<set<int>, list<int>>> static, d int, PRIMARY KEY (a, b));").discard_result().then([&e] {
            return e.execute_cql("INSERT INTO tfc (a, b, c, d) VALUES (0, 0, {}, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("SELECT * FROM tfc;");
        }).then([&e, frozen_map_of_set_and_list] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(0),
                  int32_type->decompose(0),
                  frozen_map_of_set_and_list->decompose(make_map_value(
                          frozen_map_of_set_and_list, map_type_impl::native_type({}))),
                  int32_type->decompose(0) },
            });
        });
    });
}


SEASTAR_TEST_CASE(test_alter_table) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table tat (pk1 int, c1 int, ck2 int, r1 int, r2 int, PRIMARY KEY (pk1, c1, ck2));").discard_result().then([&e] {
            return e.execute_cql("insert into tat (pk1, c1, ck2, r1, r2) values (1, 2, 3, 4, 5);").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table tat with comment = 'This is a comment.';").discard_result();
        }).then([&e] {
            BOOST_REQUIRE_EQUAL(e.local_db().find_schema("ks", "tat")->comment(), sstring("This is a comment."));
            return e.execute_cql("alter table tat alter r2 type blob;").discard_result();
        }).then([&e] {
            return e.execute_cql("select pk1, c1, ck2, r1, r2 from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(5) },
            });
        }).then([&e] {
            return e.execute_cql("insert into tat (pk1, c1, ck2, r2) values (1, 2, 3, 0x1234567812345678);").discard_result();
        }).then([&e] {
            return e.execute_cql("select pk1, c1, ck2, r1, r2 from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), from_hex("1234567812345678") },
            });
        }).then([&e] {
            return e.execute_cql("alter table tat rename pk1 to p1 and ck2 to c2;").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, c1, c2, r1, r2 from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat add r1_2 int;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tat (p1, c1, c2, r1_2) values (1, 2, 3, 6);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(6), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat drop r1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(6), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat add r1 int;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), {}, int32_type->decompose(6), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat drop r2;").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table tat add r2 int;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), {}, int32_type->decompose(6), {} },
            });
        });
    });
}

SEASTAR_TEST_CASE(test_map_query) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("CREATE TABLE xx (k int PRIMARY KEY, m map<text, int>);").get();
            e.execute_cql("insert into xx (k, m) values (0, {'v2': 1});").get();
            auto m_type = map_type_impl::get_instance(utf8_type, int32_type, true);
            assert_that(e.execute_cql("select m from xx where k = 0;").get0())
                    .is_rows().with_rows({
                        { make_map_value(m_type, map_type_impl::native_type({{sstring("v2"), 1}})).serialize() }
                    });
            e.execute_cql("delete m['v2'] from xx where k = 0;").get();
            assert_that(e.execute_cql("select m from xx where k = 0;").get0())
                    .is_rows().with_rows({{{}}});
        });
    });
}

SEASTAR_TEST_CASE(test_drop_table) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("create table tmp (pk int, v int, PRIMARY KEY (pk));").get();
            e.execute_cql("drop columnfamily tmp;").get();
            e.execute_cql("create table tmp (pk int, v int, PRIMARY KEY (pk));").get();
            e.execute_cql("drop columnfamily tmp;").get();
        });
    });
}

SEASTAR_TEST_CASE(test_reversed_slice_with_empty_range_before_all_rows) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("CREATE TABLE test (a int, b int, c int, s1 int static, s2 int static, PRIMARY KEY (a, b));").get();

            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 0, 0, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 1, 1, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 2, 2, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 3, 3, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 4, 4, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 5, 5, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 6, 6, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 7, 7, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 8, 8, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 9, 9, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 10, 10, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 11, 11, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 12, 12, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 13, 13, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 14, 14, 17, 42);").get();
            e.execute_cql("INSERT INTO test (a, b, c, s1, s2) VALUES (99, 15, 15, 17, 42);").get();

            assert_that(e.execute_cql("select * from test WHERE a = 99 and b < 0 ORDER BY b DESC limit 2;").get0())
                .is_rows().is_empty();

            assert_that(e.execute_cql("select * from test WHERE a = 99 order by b desc;").get0())
                .is_rows().with_size(16);

            assert_that(e.execute_cql("select * from test;").get0())
                .is_rows().with_size(16);
        });
    });
}

SEASTAR_TEST_CASE(test_query_with_range_tombstones) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            e.execute_cql("CREATE TABLE test (pk int, ck int, v int, PRIMARY KEY (pk, ck));").get();

            e.execute_cql("INSERT INTO test (pk, ck, v) VALUES (0, 0, 0);").get();
            e.execute_cql("INSERT INTO test (pk, ck, v) VALUES (0, 2, 2);").get();
            e.execute_cql("INSERT INTO test (pk, ck, v) VALUES (0, 4, 4);").get();
            e.execute_cql("INSERT INTO test (pk, ck, v) VALUES (0, 5, 5);").get();
            e.execute_cql("INSERT INTO test (pk, ck, v) VALUES (0, 6, 6);").get();

            e.execute_cql("DELETE FROM test WHERE pk = 0 AND ck >= 1 AND ck <= 3;").get();
            e.execute_cql("DELETE FROM test WHERE pk = 0 AND ck > 4 AND ck <= 8;").get();
            e.execute_cql("DELETE FROM test WHERE pk = 0 AND ck > 0 AND ck <= 1;").get();

            assert_that(e.execute_cql("SELECT v FROM test WHERE pk = 0 ORDER BY ck DESC;").get0())
                .is_rows()
                .with_rows({
                    { int32_type->decompose(4) },
                    { int32_type->decompose(0) },
                });

            assert_that(e.execute_cql("SELECT v FROM test WHERE pk = 0;").get0())
                .is_rows()
                .with_rows({
                   { int32_type->decompose(0) },
                   { int32_type->decompose(4) },
                });
        });
    });
}

SEASTAR_TEST_CASE(test_alter_table_validation) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table tatv (p1 int, c1 int, c2 int, r1 int, r2 set<int>, PRIMARY KEY (p1, c1, c2));").discard_result().then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv drop r2;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r2 list<int>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r2 set<text>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r2 set<int>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv rename r2 to r3;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv alter r1 type bigint;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv alter r2 type map<int, int>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r3 map<int, int>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r4 set<text>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv drop r3;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv drop r4;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r3 map<int, text>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r4 set<int>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r3 map<int, blob>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r4 set<blob>;").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert(!f.failed());
        });
    });
}

SEASTAR_TEST_CASE(test_pg_style_string_literal) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table test (p1 text, PRIMARY KEY (p1));").discard_result().then([&e] {
            return e.execute_cql("insert into test (p1) values ($$Apostrophe's$ $ not$ $ '' escaped$$);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into test (p1) values ($$$''valid$_$key$$);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into test (p1) values ('$normal$valid$$$$key$');").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into test (p1) values ($ $invalid$$);").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("insert into test (p1) values ($$invalid$ $);").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("insert into test (p1) values ($ $invalid$$$);").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("insert into test (p1) values ($$ \n\n$invalid);").discard_result();
        }).then_wrapped([&e] (future<> f) {
            assert_that_failed(f);
            return e.execute_cql("select * from test;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { utf8_type->decompose(sstring("Apostrophe's$ $ not$ $ '' escaped")) },
                { utf8_type->decompose(sstring("$''valid$_$key")) },
                { utf8_type->decompose(sstring("$normal$valid$$$$key$")) },
            });
        });
    });
}

SEASTAR_TEST_CASE(test_insert_large_collection_values) {
    return do_with_cql_env([] (cql_test_env& e) {
        return seastar::async([&e] {
            auto map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
            auto set_type = set_type_impl::get_instance(utf8_type, true);
            auto list_type = list_type_impl::get_instance(utf8_type, true);
            e.create_table([map_type, set_type, list_type] (sstring ks_name) {
                // CQL: CREATE TABLE tbl (pk text PRIMARY KEY, m map<text, text>, s set<text>, l list<text>);
                return schema({}, ks_name, "tbl",
                    {{"pk", utf8_type}},
                    {},
                    {
                        {"m", map_type},
                        {"s", set_type},
                        {"l", list_type}
                    },
                    {},
                    utf8_type);
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
                    return std::make_unique<cql3::query_options>(cql3::default_cql_config, db::consistency_level::ONE, infinite_timeout_config, std::nullopt,
                            std::vector<cql3::raw_value_view>(), false,
                            cql3::query_options::specific_options::DEFAULT, cql_serialization_format{version});
            };

            BOOST_REQUIRE_THROW(e.execute_cql("SELECT l FROM tbl WHERE pk = 'Zamyatin';", make_query_options(2)).get(), std::exception);
            BOOST_REQUIRE_THROW(e.execute_cql("SELECT m FROM tbl WHERE pk = 'Haksli';", make_query_options(2)).get(), std::exception);
        });
    });
}

SEASTAR_TEST_CASE(test_long_text_value) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto prepared = e.execute_cql("CREATE TABLE t (id int PRIMARY KEY, v text, v2 varchar)").get();
        e.require_table_exists("ks", "t").get();
        sstring big_one(17324, 'x');
        sstring bigger_one(29123, 'y');
        e.execute_cql(format("INSERT INTO t (id, v, v2) values (1, '{}', '{}')", big_one, big_one)).get();
        e.execute_cql(format("INSERT INTO t (id, v, v2) values (2, '{}', '{}')", bigger_one, bigger_one)).get();
        auto msg = e.execute_cql("select v, v2 from t where id = 1").get0();
        assert_that(msg).is_rows().with_rows({{utf8_type->decompose(big_one), utf8_type->decompose(big_one)}});
        msg = e.execute_cql("select v, v2 from t where id = 2").get0();
        assert_that(msg).is_rows().with_rows({{utf8_type->decompose(bigger_one), utf8_type->decompose(bigger_one)}});
    });
}

SEASTAR_TEST_CASE(test_time_conversions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto prepared = e.execute_cql(
            "CREATE TABLE time_data (id timeuuid PRIMARY KEY, d date, ts timestamp);").get();
        e.require_table_exists("ks", "time_data").get();

        e.execute_cql("INSERT INTO time_data (id, d, ts) VALUES (f4e30f80-6958-11e8-96d6-000000000000, '2017-06-11', '2018-06-05 00:00:00+0000');").get();

        struct tm t = { 0 };
        t.tm_year = 2018 - 1900;
        t.tm_mon = 6 - 1;
        t.tm_mday = 6;
        t.tm_hour = 7;
        t.tm_min = 12;
        t.tm_sec = 22;
        auto tp1 = db_clock::from_time_t(timegm(&t)) + std::chrono::milliseconds(136);
        t.tm_year = 2017 - 1900;
        t.tm_mday = 11;
        t.tm_hour = 0;
        t.tm_min = 0;
        t.tm_sec = 0;
        auto tp2 = db_clock::from_time_t(timegm(&t));

        auto msg = e.execute_cql("select todate(id), todate(ts), totimestamp(id), totimestamp(d), tounixtimestamp(id),"
                                 "tounixtimestamp(ts), tounixtimestamp(d), tounixtimestamp(totimestamp(todate(totimestamp(todate(id))))) from time_data;").get0();
        assert_that(msg).is_rows().with_rows({{
            serialized(simple_date_native_type{0x80004518}),
            serialized(simple_date_native_type{0x80004517}),
            timestamp_type->decompose(tp1),
            timestamp_type->decompose(tp2),
            long_type->decompose(int64_t(1528269142136)),
            long_type->decompose(int64_t(1528156800000)),
            long_type->decompose(int64_t(1497139200000)),
            long_type->decompose(int64_t(1528243200000))
        }});

    });
}

// Corner-case test that checks for the paging code's preparedness for an empty
// range list.
SEASTAR_TEST_CASE(test_empty_partition_range_scan) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create keyspace empty_partition_range_scan with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};").get();
        e.execute_cql("create table empty_partition_range_scan.tb (a int, b int, c int, val int, PRIMARY KEY ((a,b),c) );").get();


        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, infinite_timeout_config, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
        auto res = e.execute_cql("select * from empty_partition_range_scan.tb where token (a,b) > 1 and token(a,b) <= 1;", std::move(qo)).get0();
        assert_that(res).is_rows().is_empty();
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_contains) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (p frozen<map<text, text>>, c1 frozen<list<int>>, c2 frozen<set<int>>, v map<text, text>, PRIMARY KEY(p, c1, c2));").get();
        e.require_table_exists("ks", "t").get();

        e.execute_cql("INSERT INTO t (p, c1, c2, v) VALUES ({'a':'a'}, [1,2,3], {1, 5}, {'x':'xyz', 'y1':'abc'});").get();
        e.execute_cql("INSERT INTO t (p, c1, c2, v) VALUES ({'b':'b'}, [2,3,4], {3, 4}, {'d':'def', 'y1':'abc'});").get();
        e.execute_cql("INSERT INTO t (p, c1, c2, v) VALUES ({'c':'c'}, [3,4,5], {1, 2, 3}, {});").get();

        auto my_list_type = list_type_impl::get_instance(int32_type, false);
        auto my_set_type = set_type_impl::get_instance(int32_type, false);
        auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, false);
        auto my_nonfrozen_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);

        auto msg = e.execute_cql("SELECT p FROM t WHERE p CONTAINS KEY 'a' ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{sstring("a"), sstring("a")}})))}
        });

        msg = e.execute_cql("SELECT c1 FROM t WHERE c1 CONTAINS 3 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type({{1, 2, 3}})))},
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type({{2, 3, 4}})))},
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type({{3, 4, 5}})))}
        });

        msg = e.execute_cql("SELECT c2 FROM t WHERE c2 CONTAINS 1 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type({{1, 5}})))},
            {my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type({{1, 2, 3}})))}
        });

        msg = e.execute_cql("SELECT v FROM t WHERE v CONTAINS KEY 'y1' ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_nonfrozen_map_type->decompose(make_map_value(my_nonfrozen_map_type, map_type_impl::native_type({{sstring("x"), sstring("xyz")}, {sstring("y1"), sstring("abc")}})))},
            {my_nonfrozen_map_type->decompose(make_map_value(my_nonfrozen_map_type, map_type_impl::native_type({{sstring("d"), sstring("def")}, {sstring("y1"), sstring("abc")}})))}
        });

        msg = e.execute_cql("SELECT c2, v FROM t WHERE v CONTAINS KEY 'y1' AND c2 CONTAINS 5 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {
                my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type({{1, 5}}))),
                my_nonfrozen_map_type->decompose(make_map_value(my_nonfrozen_map_type, map_type_impl::native_type({{sstring("x"), sstring("xyz")}, {sstring("y1"), sstring("abc")}})))
            }
        });
    });
}

SEASTAR_TEST_CASE(test_in_restriction_on_not_last_partition_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int,b int,c int,d int,PRIMARY KEY ((a, b), c));").get();
        e.require_table_exists("ks", "t").get();

        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (1,1,1,100); ").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (1,1,2,200); ").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (1,1,3,300); ").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (1,2,1,300); ").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (1,3,1,1300);").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (1,3,2,1400);").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (2,3,2,1400);").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (2,1,2,1400);").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (2,1,3,1300);").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (2,2,3,1300);").get();
        e.execute_cql("INSERT INTO t (a,b,c,d) VALUES (3,1,3,1300);").get();

        {
            auto msg = e.execute_cql("SELECT * FROM t WHERE a IN (1,2) AND b IN (2,3) AND c>=2 AND c<=3;").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
               {
                   int32_type->decompose(1),
                   int32_type->decompose(3),
                   int32_type->decompose(2),
                   int32_type->decompose(1400),
               },
               {
                   int32_type->decompose(2),
                   int32_type->decompose(2),
                   int32_type->decompose(3),
                   int32_type->decompose(1300),
               },
               {
                   int32_type->decompose(2),
                   int32_type->decompose(3),
                   int32_type->decompose(2),
                   int32_type->decompose(1400),
               }
            });
        }
        {
           auto msg = e.execute_cql("SELECT * FROM t WHERE a IN (1,3) AND b=1 AND c>=2 AND c<=3;").get0();
           assert_that(msg).is_rows().with_rows_ignore_order({
              {
                  int32_type->decompose(1),
                  int32_type->decompose(1),
                  int32_type->decompose(2),
                  int32_type->decompose(200),
              },
              {
                  int32_type->decompose(1),
                  int32_type->decompose(1),
                  int32_type->decompose(3),
                  int32_type->decompose(300),
              },
              {
                  int32_type->decompose(3),
                  int32_type->decompose(1),
                  int32_type->decompose(3),
                  int32_type->decompose(1300),
              }
           });
       }
    });
}

SEASTAR_TEST_CASE(test_static_multi_cell_static_lists_with_ckey) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (p int, c int, slist list<int> static, v int, PRIMARY KEY (p, c));").get();
        e.execute_cql("INSERT INTO t (p, c, slist, v) VALUES (1, 1, [1], 1); ").get();

        {
            e.execute_cql("UPDATE t SET slist[0] = 3, v = 3 WHERE p = 1 AND c = 1;").get();
            auto msg = e.execute_cql("SELECT slist, v FROM t WHERE p = 1 AND c = 1;").get0();
            auto slist_type = list_type_impl::get_instance(int32_type, true);
            assert_that(msg).is_rows().with_row({
                { slist_type->decompose(make_list_value(slist_type, list_type_impl::native_type({{3}}))) },
                { int32_type->decompose(3) }
            });
        }
        {
            e.execute_cql("UPDATE t SET slist = [4], v = 4 WHERE p = 1 AND c = 1;").get();
            auto msg = e.execute_cql("SELECT slist, v FROM t WHERE p = 1 AND c = 1;").get0();
            auto slist_type = list_type_impl::get_instance(int32_type, true);
            assert_that(msg).is_rows().with_row({
                { slist_type->decompose(make_list_value(slist_type, list_type_impl::native_type({{4}}))) },
                { int32_type->decompose(4) }
            });
        }
        {
            e.execute_cql("UPDATE t SET slist = [3] + slist , v = 5 WHERE p = 1 AND c = 1;").get();
            auto msg = e.execute_cql("SELECT slist, v FROM t WHERE p = 1 AND c = 1;").get0();
            auto slist_type = list_type_impl::get_instance(int32_type, true);
            assert_that(msg).is_rows().with_row({
                { slist_type->decompose(make_list_value(slist_type, list_type_impl::native_type({3, 4}))) },
                { int32_type->decompose(5) }
            });
        }
        {
            e.execute_cql("UPDATE t SET slist = slist + [5] , v = 6 WHERE p = 1 AND c = 1;").get();
            auto msg = e.execute_cql("SELECT slist, v FROM t WHERE p = 1 AND c = 1;").get0();
            auto slist_type = list_type_impl::get_instance(int32_type, true);
            assert_that(msg).is_rows().with_row({
                { slist_type->decompose(make_list_value(slist_type, list_type_impl::native_type({3, 4, 5}))) },
                { int32_type->decompose(6) }
            });
        }
        {
            e.execute_cql("DELETE slist[2] from t WHERE p = 1;").get();
            auto msg = e.execute_cql("SELECT slist, v FROM t WHERE p = 1 AND c = 1;").get0();
            auto slist_type = list_type_impl::get_instance(int32_type, true);
            assert_that(msg).is_rows().with_row({
                { slist_type->decompose(make_list_value(slist_type, list_type_impl::native_type({3, 4}))) },
                { int32_type->decompose(6) }
            });
        }
        {
            e.execute_cql("UPDATE t SET slist = slist - [4] , v = 7 WHERE p = 1 AND c = 1;").get();
            auto msg = e.execute_cql("SELECT slist, v FROM t WHERE p = 1 AND c = 1;").get0();
            auto slist_type = list_type_impl::get_instance(int32_type, true);
            assert_that(msg).is_rows().with_row({
                { slist_type->decompose(make_list_value(slist_type, list_type_impl::native_type({3}))) },
                { int32_type->decompose(7) }
            });
        }
    });
}


/**
 * A class to represent a single multy-column slice expression.
 * The purpose of this class is to provide facilities for testing
 * the multicolumn slice expression. The class uses an abstraction
 * of integer tuples with predefined max values as a counting system
 * with Digits num of digits and Base-1 as the maximum value.
 *
 * The table this class is designed to test is a table with exactly
 * Digits clustering columns of type int and at least one none clustering
 * of type int.
 * The none clustering column should containing the conversion of the
 * clustering key into an int according to base conversion rules where the
 * last clustering key is the smallest digit.
 * This conversion produces a  mapping from int to the tuple space spanned
 * by the clustering keys of the table (including prefix tuples).
 *
 * The test case represent a specific slice expression and utility functions to
 * run and validate it.
 * For a usage example see: test_select_with_mixed_order_table
 */
template <int Base,int Digits>
class slice_testcase
{
private:
    std::vector<int> _gt_range;
    bool _gt_inclusive;
    std::vector<int> _lt_range;
    bool _lt_inclusive;
public:
    enum class column_ordering_for_results {
        ASC,
        DESC,
    };
    using ordering_spec = column_ordering_for_results[Digits];
    /**
     *  The mapping of tuples is to integers between 0 and this value.
     */
    static const int total_num_of_values = std::pow(Base,Digits);

    /**
     * Consructor for the testcase
     * @param gt_range - the tuple for the greater than part of the expression
     *        as a vector of integers.An empty vector indicates no grater than
     *        part.
     * @param gt_inclusive - is the greater than inclusive (>=).
     * @param lt_range - the tuple for the less than part of the expression
     *        as a vector of integers.An empty vector indicates no less than
     *        part.
     * @param lt_inclusive - is the less than inclusive (<=)
     */
    slice_testcase(std::vector<int> gt_range, bool gt_inclusive, std::vector<int> lt_range, bool lt_inclusive) {
        _gt_range = gt_range;
        _lt_range = lt_range;
        _gt_inclusive = gt_inclusive;
        _lt_inclusive = lt_inclusive;
    }

    /**
     * Builds a vector of the expected result assuming a select with
     * this slice expression. The select is assumed to be only on the
     * none clustering column.
     * @return
     */
    auto genrate_results_for_validation(ordering_spec& orderings) {
        std::vector<std::vector<bytes_opt>> vals;
        for (auto val : generate_results(orderings)) {
            vals.emplace_back(std::vector<bytes_opt>{ int32_type->decompose(val) });
        }
        return vals;
    }

    /**
     * Generates the actual slice expression that can be embedded
     * into an SQL select query.
     * @param column_names - the mames of the table clustering columns
     * @return the SQL expression as a text.
     */
    auto generate_cql_slice_expresion(std::vector<std::string> column_names) {
        std::string expression;
        if (!_gt_range.empty()) {
            expression += "(";
            expression += column_names[0];
            for(std::size_t i=1; i < _gt_range.size(); i++) {
                expression += ", "+column_names[i];
            }
            expression += ") ";
            expression += _gt_inclusive ? ">= " : "> ";
            expression += "(";
            expression += std::to_string(_gt_range[0]);
            for(std::size_t i=1; i<_gt_range.size(); i++) {
                expression += ", ";
                expression += std::to_string(_gt_range[i]);
            }
            expression += ")";
        }
        if (!_gt_range.empty() && !_lt_range.empty()) {
            expression += " AND ";
        }
        if (!_lt_range.empty()) {
            expression += "(";
            expression += column_names[0];
            for(std::size_t i=1; i < _lt_range.size(); i++) {
                expression += ", ";
                expression += column_names[i];
            }
            expression += ") ";
            expression += _lt_inclusive ? "<= " : "< ";

            expression += "(";
            expression += std::to_string(_lt_range[0]);
            for(std::size_t i=1; i < _lt_range.size(); i++) {
                expression += ", ";
                expression += std::to_string(_lt_range[i]);
            }
            expression += ")";
        }
        return expression;
    }

    /**
     * Maps a tuple of integers to integer
     * @param tuple - the tuple to convert as a vector of integers.
     * @return the integer this tuple is mapped to.
     */
    static int tuple_to_bound_val(std::vector<int> tuple) {
        int ret = 0;
        int factor = std::pow(Base, Digits-1);
        for (int val : tuple) {
            ret += val * factor;
            factor /= Base;
        }
        return ret;
    }

    /**
     * Maps back from integer space to tuple space.
     * There can be more than one tuple maped to the same int.
     * There will never be more than one tuple of a certain size
     * that is maped to the same int.
     * For example: (1) and (1,0) will be maped to the same integer,
     * but no other tuple of size 1 or 2 will be maped to this int.
     * @param val - the value to map
     * @param num_componnents - the size of the produced tuple.
     * @return the tuple of the requested size.
     */
    static auto bound_val_to_tuple(int val, std::size_t num_componnents = Digits) {
        std::vector<int> tuple;
        int factor = std::pow(Base, Digits-1);
        while (tuple.size() < num_componnents) {
            tuple.emplace_back(val/factor);
            val %= factor;
            factor /= Base;
        }
        return tuple;
    }
private:
    /**
     * A helper function to generate the expected results of a select
     * statement with this slice. The select statement is assumed to
     * select only the none clustering column.
     * @return a vector of integers representing the expected results.
     */
    std::vector<int> generate_results(ordering_spec& orderings) {
        std::vector<int> vals;
        int start_val = 0;
        int end_val =  total_num_of_values -1;
        if (!_gt_range.empty()) {
            start_val = tuple_to_bound_val(_gt_range);
            if (!_gt_inclusive) {
                start_val += std::pow(Base,Digits - _gt_range.size());
            }
        }
        if (!_lt_range.empty()) {
            end_val = tuple_to_bound_val(_lt_range);
            if (!_lt_inclusive) {
                end_val--;
            } else {
                end_val += std::pow(Base, Digits - _lt_range.size()) - 1;
            }
        }
        for (int i=start_val; i<=end_val; i++) {
            vals.emplace_back(i);
        }
        auto comparator_lt = [&orderings] (int lhs, int rhs){
            auto lhs_t = bound_val_to_tuple(lhs);
            auto rhs_t = bound_val_to_tuple(rhs);
            for (int i=0; i < Digits; i++) {
                if (lhs_t[i] == rhs_t[i]) {
                    continue ;
                }
                bool lt = (lhs_t[i] < rhs_t[i]);
                // the reason this is correct is because at that point we know that:
                // lhs_t[i] is not less and not equal to  rhs_t[i])
                bool gt = !lt;
                bool reverse = orderings[i] == column_ordering_for_results::DESC;
                return (lt && !reverse) || (gt && reverse);
            }
            return false;
        };
        std::sort(vals.begin(), vals.end(), comparator_lt);
        return vals;
    }
};


SEASTAR_TEST_CASE(test_select_with_mixed_order_table) {
    using slice_test_type = slice_testcase<5,4>;
    return do_with_cql_env_thread([] (cql_test_env& e) {
        std::string select_query_template = "SELECT f FROM foo WHERE a=0 AND %s;";
        std::vector<std::string> column_names = { "b", "c", "d", "e" };
        e.execute_cql("CREATE TABLE foo (a int, b int, c int,d int,e int,f int, PRIMARY KEY (a, b, c, d, e)) WITH CLUSTERING ORDER BY (b DESC, c ASC, d DESC,e ASC);").get();
        e.require_table_exists("ks", "foo").get();

        // We convert the range 0-> max mapped integers to the mapped tuple,
        // this will create a table satisfying the slice_testcase assumption.
        for(int i=0; i < slice_test_type::total_num_of_values; i++) {
            auto tuple = slice_test_type::bound_val_to_tuple(i);
            e.execute_cql(format("INSERT INTO foo (a, b, c, d, e, f) VALUES (0, {}, {}, {}, {}, {});",
                    tuple[0],tuple[1],tuple[2],tuple[3],i)).get();
        }

        // a vector to hold all test cases.
        std::vector<slice_test_type> test_cases;

        //generates all inclusiveness permutations for the specified bounds
        auto generate_with_inclusiveness_permutations = [&test_cases] (std::vector<int> gt_range,std::vector<int> lt_range) {
            if(gt_range.empty() || lt_range.empty()) {
                test_cases.emplace_back(slice_test_type{gt_range, false, lt_range,false});
                test_cases.emplace_back(slice_test_type{gt_range, true, lt_range,true});
            } else {
                for(int i=0; i<=3; i++) {
                    test_cases.emplace_back(slice_test_type{gt_range, bool(i&1), lt_range, bool(i&2)});
                }
            }
        };

        slice_test_type::ordering_spec ordering = {
            slice_test_type::column_ordering_for_results::DESC,
            slice_test_type::column_ordering_for_results::ASC,
            slice_test_type::column_ordering_for_results::DESC,
            slice_test_type::column_ordering_for_results::ASC,
        };
        // no overlap in componnents equal num of componnents - (b,c,d,e) >/>= (0,1,2,3) and (b,c,d,e) </<= (1,2,3,4)
        generate_with_inclusiveness_permutations({0,1,2,3},{1,2,3,4});
        // overlap in  componnents equal num of componnents - (b,c,d,e) >/>= (0,1,2,3) and (b,c,d,e) </<= (0,2,2,2)
        generate_with_inclusiveness_permutations({0,1,2,3},{0,2,2,2});
        // overlap in  componnents equal num of componnents - (b,c,d,e) >/>= (0,1,2,3) and (b,c,d,e) </<= (0,1,2,2)
        generate_with_inclusiveness_permutations({0,1,2,3},{0,1,2,2});
        // no overlap less compnnents in </<= expression - (b,c,d,e) >/>= (0,1,2,3) and (b,c) </<= (1,2)
        generate_with_inclusiveness_permutations({0,1,2,3},{1,2});
        // overlap in compnnents for less componnents in </<= expression - (b,c,d,e) >/>= (0,1,2,3) and (b,c) </<= (0,2)
        generate_with_inclusiveness_permutations({0,1,2,3},{0,2});
        // lt side is a prefix of gt side </<= expression - (b,c,d,e) >/>= (0,1,2,3) and (b,c) </<= (0,1)
        generate_with_inclusiveness_permutations({0,1,2,3},{0,1});
        // gt side is a prefix of lt side </<= expression - (b,c,d,e) >/>= (0,1) and (b,c) </<= (0,1,2,3)
        generate_with_inclusiveness_permutations({0,1},{0,1,2,3});
        // no overlap less compnnents in >/>= expression - (b,c) >/>= (0,1) and (b,c,d,e) </<= (1,2,3,4)
        generate_with_inclusiveness_permutations({0,1},{1,2,3,4});
        // overlap in compnnents for less componnents in >/>= expression - (b,c) >/>= (0,1) and (b,c,d,e) </<= (0,2,3,4)
        generate_with_inclusiveness_permutations({0,1},{0,2,3,4});
        // one sided >/>= 1 expression - (b) >/>= (1)
        generate_with_inclusiveness_permutations({1},{});
        // one sided >/>= partial expression - (b,c) >/>= (0,1)
        generate_with_inclusiveness_permutations({0,1},{});
        // one sided >/>= full expression - (b,c,d,e) >/>= (0,1,2,3)
        generate_with_inclusiveness_permutations({0,1,2,3},{});
        // one sided </<= 1 expression - - (b) </<= (3)
        generate_with_inclusiveness_permutations({},{3});
        // one sided </<= partial expression - (b,c) </<= (3,4)
        generate_with_inclusiveness_permutations({},{3,4});
        // one sided </<= full expression - (b,c,d,e) </<= (2,3,4,4)
        generate_with_inclusiveness_permutations({},{2,3,4,4});
        // equality and empty - (b,c,d,e) >/>= (0,1,2,3) and (b,c,d,e) </<= (0,1,2,3)
        generate_with_inclusiveness_permutations({0,1,2,3},{0,1,2,3});

        for (auto&& test_case  : test_cases) {
            auto msg = e.execute_cql(sprint(select_query_template,test_case.generate_cql_slice_expresion(column_names))).get0();
            assert_that(msg).is_rows().with_rows(test_case.genrate_results_for_validation(ordering));
        }
    });
}

uint64_t
run_and_examine_cache_stat_change(cql_test_env& e, uint64_t cache_tracker::stats::*metric, std::function<void (cql_test_env& e)> func) {
    auto read_stat = [&] {
        auto local_read_metric = [metric] (database& db) { return db.row_cache_tracker().get_stats().*metric; };
        return e.db().map_reduce0(local_read_metric, uint64_t(0), std::plus<uint64_t>()).get0();
    };
    auto before = read_stat();
    func(e);
    auto after = read_stat();
    return after - before;
}

SEASTAR_TEST_CASE(test_cache_bypass) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (k int PRIMARY KEY)").get();
        auto with_cache = run_and_examine_cache_stat_change(e, &cache_tracker::stats::reads, [] (cql_test_env& e) {
            e.execute_cql("SELECT * FROM t").get();
        });
        BOOST_REQUIRE(with_cache >= smp::count);  // scan may make multiple passes per shard
        auto without_cache = run_and_examine_cache_stat_change(e, &cache_tracker::stats::reads, [] (cql_test_env& e) {
            e.execute_cql("SELECT * FROM t BYPASS CACHE").get();
        });
        BOOST_REQUIRE_EQUAL(without_cache, 0);
    });
}

SEASTAR_TEST_CASE(test_describe_varchar) {
   // Test that, like cassandra, a varchar column is represented as a text column.
   return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table tbl (id int PRIMARY KEY, t text, v varchar);").get();
        auto ks = utf8_type->decompose("ks");
        auto tbl = utf8_type->decompose("tbl");
        auto id = utf8_type->decompose("id");
        auto t = utf8_type->decompose("t");
        auto v = utf8_type->decompose("v");
        auto none = utf8_type->decompose("NONE");
        auto partition_key = utf8_type->decompose("partition_key");
        auto regular = utf8_type->decompose("regular");
        auto pos_0 = int32_type->decompose(0);
        auto pos_m1 = int32_type->decompose(-1);
        auto int_t = utf8_type->decompose("int");
        auto text_t = utf8_type->decompose("text");
        assert_that(e.execute_cql("select * from system_schema.columns where keyspace_name = 'ks';").get0())
            .is_rows()
            .with_rows({
                    {ks, tbl, id, none, id, partition_key, pos_0, int_t},
                    {ks, tbl, t, none, t, regular, pos_m1, text_t},
                    {ks, tbl, v, none, v, regular, pos_m1, text_t}
                });
   });
}

SEASTAR_TEST_CASE(test_group_by_syntax) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e,
            "create table t1 (p1 int, p2 int, c1 int, c2 int, c3 int, npk int, primary key((p1, p2), c1, c2, c3))");
        cquery_nofail(e, "create table t2 (p1 int, p2 int, p3 int, npk int, primary key((p1, p2, p3)))");

        // Must parse correctly:
        cquery_nofail(e, "select count(c1) from t1 group by p1, p2");
        cquery_nofail(e, "select count(c1) from t1 group by p1, p2, c1");
        cquery_nofail(e, "select sum(c2) from t1 group by p1, p2");
        cquery_nofail(e, "select avg(npk) from t1 group by \"p1\", \"p2\"");
        cquery_nofail(e, "select sum(p2) from t1 group by p1, p2, c1, c2, c3");
        cquery_nofail(e, "select count(npk) from t1 where p1=1 and p2=1 group by c1, c2 order by c1 allow filtering");
        cquery_nofail(e, "select c2 from t1 where p2=2 group by p1, c1 allow filtering");
        cquery_nofail(e, "select npk from t1 where p2=2 group by p1, p2, c1 allow filtering");
        cquery_nofail(e, "select p1 from t2 group by p1, p2, p3");
        cquery_nofail(e, "select * from t2 where p1=1 group by p1, p2, p3 allow filtering");
        cquery_nofail(e, "select * from t2 where p1=1 group by p2, p3 allow filtering");
        cquery_nofail(e, "select * from t2 where p1=1 and p2=2 and p3=3 group by p1, p2, p3 allow filtering");
        cquery_nofail(e, "select * from t2 where p1=1 and p2=2 and p3=3 group by p3 allow filtering");
        cquery_nofail(e, "select * from t1 where p1>0 and p2=0 group by p1, c1 allow filtering");

        using ire = exceptions::invalid_request_exception;
        const auto unknown = exception_predicate::message_contains("unknown column");
        const auto non_primary = exception_predicate::message_contains("non-primary-key");
        const auto order = exception_predicate::message_contains("order");
        const auto partition = exception_predicate::message_contains("partition key");

        // Flag invalid columns in GROUP BY:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by xyz").get(), ire, unknown);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1, xyz").get(), ire, unknown);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by npk").get(), ire, non_primary);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1, npk").get(), ire, non_primary);
        // Even when GROUP BY lists all primary-key columns:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1, p2, c1, c2, c3, foo").get(), ire, unknown);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 group by p1, p2, c1, c2, c3, npk").get(), ire, non_primary);
        // Even when entire primary key is equality-restricted.
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 and p2=2 and p3=3 group by npk allow filtering").get(),
                ire, non_primary);
        // Flag invalid column order:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p2, p1").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where p1=1 group by p2, c2 allow filtering").get(), ire, order);
        // Even with equality restrictions:
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 group by p3 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 group by p2, p1 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p2=2 group by p1, p3, p2 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 and p2=2 and p3=3 group by p2, p1 allow filtering").get(),
                ire, order);
        // And with non-equality restrictions:
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where p1 > 0 group by p2 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where (c1,c2) > (0,0) group by p1, p2, c3 allow filtering").get(),
                ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where p1>0 and p2=0 group by c1 allow filtering").get(), ire, order);
        // Even when GROUP BY lists all primary-key columns:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1, p2, p2, p3").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1, p2, p3, p1").get(), ire, order);
        // GROUP BY must list the entire partition key:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1, p2").get(), ire, partition);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1").get(), ire, partition);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1").get(), ire, partition);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_syntax_no_value_columns) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p1 int, p2 int, p3 int, primary key((p1, p2, p3)))");
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t group by p1, p2, p3, p1").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("order"));
    });
}

namespace {

/// Asserts that cquery_nofail(e, qstr) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  const char* qstr,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const source_location& loc = source_location::current()) {
    try {
        assert_that(cquery_nofail(e, qstr, nullptr, loc)).is_rows().with_rows_ignore_order(expected);
    }
    catch (const std::exception& e) {
        BOOST_FAIL(format("query '{}' failed: {}\n{}:{}: originally from here",
                          qstr, e.what(), loc.file_name(), loc.line()));
    }
}

/// Asserts that e.execute_prepared(id, values) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  cql3::prepared_cache_key_type id,
                  const std::vector<cql3::raw_value>& values,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const source_location& loc = source_location::current()) {
    try {
        assert_that(e.execute_prepared(id, values).get0()).is_rows().with_rows_ignore_order(expected);
    } catch (const std::exception& e) {
        BOOST_FAIL(format("execute_prepared failed: {}\n{}:{}: originally from here",
                          e.what(), loc.file_name(), loc.line()));
    }
}

auto I(int32_t x) { return int32_type->decompose(x); }

auto L(int64_t x) { return long_type->decompose(x); }

auto T(const char* t) { return utf8_type->decompose(t); }

} // anonymous namespace

SEASTAR_TEST_CASE(test_group_by_aggregate_single_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, n int)");
        cquery_nofail(e, "insert into t (p, n) values (1, 10)");
        // Rows contain GROUP BY column values (later filtered in cql_server::connection).
        require_rows(e, "select sum(n) from t group by p", {{I(10), I(1)}});
        require_rows(e, "select avg(n) from t group by p", {{I(10), I(1)}});
        require_rows(e, "select count(n) from t group by p", {{L(1), I(1)}});
        cquery_nofail(e, "insert into t (p, n) values (2, 20)");
        require_rows(e, "select sum(n) from t group by p", {{I(10), I(1)}, {I(20), I(2)}});
        require_rows(e, "select avg(n) from t group by p", {{I(20), I(2)}, {I(10), I(1)}});
        require_rows(e, "select count(n) from t group by p", {{L(1), I(2)}, {L(1), I(1)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_aggregate_partition_only) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p1 int, p2 int, p3 int, v int, primary key((p1, p2, p3)))");
        cquery_nofail(e, "insert into t (p1, p2, p3, v) values (1, 1, 1, 100)");
        // Rows contain GROUP BY column values (later filtered in cql_server::connection).
        require_rows(e, "select sum(v) from t group by p1, p2, p3", {{I(100), I(1), I(1), I(1)}});
        cquery_nofail(e, "insert into t (p1, p2, p3, v) values (1, 2, 1, 100)");
        require_rows(e, "select sum(v) from t group by p1, p2, p3",
                     {{I(100), I(1), I(1), I(1)}, {I(100), I(1), I(2), I(1)}});
        require_rows(e, "select sum(v) from t where p2=2 group by p1, p3 allow filtering",
                     {{I(100), I(2), I(1), I(1)}});
        cquery_nofail(e, "insert into t (p1, p2, p3, v) values (1, 2, 2, 100)");
        require_rows(e, "select sum(v) from t group by p1, p2, p3",
                     {{I(100), I(1), I(1), I(1)}, {I(100), I(1), I(2), I(1)}, {I(100), I(1), I(2), I(2)}});
        cquery_nofail(e, "delete from t where p1=1 and p2=1 and p3=1");
        require_rows(e, "select sum(v) from t group by p1, p2, p3",
                     {{I(100), I(1), I(2), I(1)}, {I(100), I(1), I(2), I(2)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_aggregate_clustering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p1 int, c1 int, c2 int, v int, primary key((p1), c1, c2))");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (1, 1, 1, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (1, 1, 2, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (1, 1, 3, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (2, 1, 1, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (2, 2, 2, 100)");
        cquery_nofail(e, "delete from t where p1=1 and c1=1 and c2 =3");
        require_rows(e, "select sum(v) from t group by p1", {{I(200), I(1)}, {I(200), I(2)}});
        require_rows(e, "select sum(v) from t group by p1, c1",
                     {{I(200), I(1), I(1)}, {I(100), I(2), I(1)}, {I(100), I(2), I(2)}});
        require_rows(e, "select sum(v) from t where p1=1 and c1=1 group by c2 allow filtering",
                     {{I(100), I(1)}, {I(100), I(2)}});
        require_rows(e, "select sum(v) from t group by p1, c1, c2",
                     {{I(100), I(1), I(1), I(1)}, {I(100), I(1), I(1), I(2)},
                      {I(100), I(2), I(1), I(1)}, {I(100), I(2), I(2), I(2)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_text_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p text, c text, v int, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123', '1', 100)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123', '2', 200)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123', '3', 300)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123abc', '1', 150)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123abc', '2', 250)");
        cquery_nofail(e, "insert into t (p, c, v) values ('ab', 'cd', 310)");
        cquery_nofail(e, "insert into t (p, c, v) values ('abc', 'd', 420)");
        require_rows(e, "select sum(v) from t group by p",
                     {{I(600), T("123456789012345678901234567890123")},
                      {I(400), T("123456789012345678901234567890123abc")},
                      {I(310), T("ab")},
                      {I(420), T("abc")}});
        require_rows(e, "select sum(v) from t where p in ('ab','abc') group by p, c allow filtering",
                     {{I(310), T("ab"), T("cd")}, {I(420), T("abc"), T("d")}});
        require_rows(e, "select sum(v) from t where p='123456789012345678901234567890123' group by c",
                     {{I(100), T("1")}, {I(200), T("2")}, {I(300), T("3")}});
        cquery_nofail(e, "create table t2 (p text, c1 text, c2 text, v int, primary key(p, c1, c2))");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', '', '', 10)");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', '', 'b', 20)");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', 'a', '', 30)");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', 'a', 'b', 40)");
        require_rows(e, "select avg(v) from t2 group by p", {{I(25), T(" ")}});
        require_rows(e, "select avg(v) from t2 group by p, c1", {{I(15), T(" "), T("")}, {I(35), T(" "), T("a")}});
        require_rows(e, "select sum(v) from t2 where c1='' group by p, c2 allow filtering",
                     {{I(10), T(""), T(" "), T("")}, {I(20), T(""), T(" "), T("b")}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_non_aggregate) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, n int, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, n) values (1, 1, 11)");
        require_rows(e, "select n from t group by p", {{I(11), I(1)}});
        cquery_nofail(e, "insert into t (p, c, n) values (2, 1, 21)");
        require_rows(e, "select n from t group by p", {{I(11), I(1)}, {I(21), I(2)}});
        cquery_nofail(e, "delete from t where p=1");
        require_rows(e, "select n from t group by p", {{I(21), I(2)}});
        cquery_nofail(e, "insert into t (p, c, n) values (1, 1, 11)");
        cquery_nofail(e, "insert into t (p, c, n) values (1, 2, 12)");
        cquery_nofail(e, "insert into t (p, c, n) values (1, 3, 13)");
        require_rows(e, "select n from t group by p", {{I(11), I(1)}, {I(21), I(2)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_null_clustering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, sv int static, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, sv) values (1, 100)"); // c will be NULL.
        require_rows(e, "select sv from t where p=1 group by c", {{I(100), std::nullopt}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_aggregate_and_simple_selection_together) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, v int, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, v) values (1, 1, 11)");
        cquery_nofail(e, "insert into t (p, c, v) values (1, 2, 12)");
        cquery_nofail(e, "insert into t (p, c, v) values (1, 3, 13)");
        cquery_nofail(e, "insert into t (p, c, v) values (2, 2, 22)");
        require_rows(e, "select c, avg(c) from t", {{I(1), I(2)}});
        require_rows(e, "select p, sum(v) from t", {{I(1), I(58)}});
        require_rows(e, "select p, count(c) from t group by p", {{I(1), L(3)}, {I(2), L(1)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_like_operator) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, s text)");
        require_rows(e, "select s from t where s like 'abc' allow filtering", {});
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like 'abc' allow filtering", {{T("abc")}});
        require_rows(e, "select s from t where s like 'ab_' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t (p, s) values (2, 'abb')");
        require_rows(e, "select s from t where s like 'ab_' allow filtering", {{T("abc")}, {T("abb")}});
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
        require_rows(e, "select s from t where s like 'aaa' allow filtering", {});
    });
}

SEASTAR_TEST_CASE(test_like_operator_on_partition_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Fully constrained:
        cquery_nofail(e, "create table t (s text primary key)");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        require_rows(e, "select s from t where s like 'a__' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t (s) values ('acc')");
        require_rows(e, "select s from t where s like 'a__' allow filtering", {{T("abc")}, {T("acc")}});

        // Partially constrained:
        cquery_nofail(e, "create table t2 (s1 text, s2 text, primary key((s1, s2)))");
        cquery_nofail(e, "insert into t2 (s1, s2) values ('abc', 'abc')");
        require_rows(e, "select s2 from t2 where s2 like 'a%' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t2 (s1, s2) values ('aba', 'aba')");
        require_rows(e, "select s2 from t2 where s2 like 'a%' allow filtering", {{T("abc")}, {T("aba")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_on_clustering_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, s text, primary key(p, s))");
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t (p, s) values (2, 'acc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}, {T("acc")}});
        cquery_nofail(e, "insert into t (p, s) values (2, 'acd')");
        require_rows(e, "select s from t where p = 2 and s like '%c' allow filtering", {{T("acc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_conjunction) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s1 text primary key, s2 text)");
        cquery_nofail(e, "insert into t (s1, s2) values ('abc', 'ABC')");
        cquery_nofail(e, "insert into t (s1, s2) values ('a', 'A')");
        require_rows(e, "select * from t where s1 like 'a%' and s2 like '__C' allow filtering",
                     {{T("abc"), T("ABC")}});
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t where s1 like 'a%' and s1 = 'abc' allow filtering").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("more than one relation"));
    });
}

SEASTAR_TEST_CASE(test_like_operator_static_column) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c text, s text static, primary key(p, c))");
        require_rows(e, "select s from t where s like '%c' allow filtering", {});
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
        require_rows(e, "select * from t where c like '%' allow filtering", {});
    });
}

SEASTAR_TEST_CASE(test_like_operator_bind_marker) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s text primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        auto stmt = e.prepare("select s from t where s like ? allow filtering").get0();
        require_rows(e, stmt, {cql3::raw_value::make_value(T("_b_"))}, {{T("abc")}});
        require_rows(e, stmt, {cql3::raw_value::make_value(T("%g"))}, {});
        require_rows(e, stmt, {cql3::raw_value::make_value(T("%c"))}, {{T("abc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_blank_pattern) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, s text)");
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like '' allow filtering", {});
        cquery_nofail(e, "insert into t (p, s) values (2, '')");
        require_rows(e, "select s from t where s like '' allow filtering", {{T("")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_ascii) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s ascii primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_varchar) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s varchar primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
    });
}

namespace {

/// Asserts that a column of type \p type cannot be LHS of the LIKE operator.
auto assert_like_doesnt_accept(const char* type) {
    return do_with_cql_env_thread([type] (cql_test_env& e) {
        cquery_nofail(e, format("create table t (k {}, p int primary key)", type).c_str());
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t where k like 123 allow filtering").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("only on string types"));
    });
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_like_operator_fails_on_bigint)    { return assert_like_doesnt_accept("bigint");    }
SEASTAR_TEST_CASE(test_like_operator_fails_on_blob)      { return assert_like_doesnt_accept("blob");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_boolean)   { return assert_like_doesnt_accept("boolean");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_counter)   { return assert_like_doesnt_accept("counter");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_decimal)   { return assert_like_doesnt_accept("decimal");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_double)    { return assert_like_doesnt_accept("double");    }
SEASTAR_TEST_CASE(test_like_operator_fails_on_duration)  { return assert_like_doesnt_accept("duration");  }
SEASTAR_TEST_CASE(test_like_operator_fails_on_float)     { return assert_like_doesnt_accept("float");     }
SEASTAR_TEST_CASE(test_like_operator_fails_on_inet)      { return assert_like_doesnt_accept("inet");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_int)       { return assert_like_doesnt_accept("int");       }
SEASTAR_TEST_CASE(test_like_operator_fails_on_smallint)  { return assert_like_doesnt_accept("smallint");  }
SEASTAR_TEST_CASE(test_like_operator_fails_on_timestamp) { return assert_like_doesnt_accept("timestamp"); }
SEASTAR_TEST_CASE(test_like_operator_fails_on_tinyint)   { return assert_like_doesnt_accept("tinyint");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_uuid)      { return assert_like_doesnt_accept("uuid");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_varint)    { return assert_like_doesnt_accept("varint");    }
SEASTAR_TEST_CASE(test_like_operator_fails_on_timeuuid)  { return assert_like_doesnt_accept("timeuuid");  }
SEASTAR_TEST_CASE(test_like_operator_fails_on_date)      { return assert_like_doesnt_accept("date");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_time)      { return assert_like_doesnt_accept("time");      }

SEASTAR_TEST_CASE(test_like_operator_on_token) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s text primary key)");
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t where token(s) like 'abc' allow filtering").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("token function"));
    });
}

SEASTAR_TEST_CASE(test_alter_type_on_compact_storage_with_no_regular_columns_does_not_crash) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TYPE my_udf (first text);");
        cquery_nofail(e, "create table z (pk int, ck frozen<my_udf>, primary key(pk, ck)) with compact storage;");
        cquery_nofail(e, "alter type my_udf add test_int int;");
    });
}

SEASTAR_TEST_CASE(test_rf_expand) {
    constexpr static auto simple = "org.apache.cassandra.locator.SimpleStrategy";
    constexpr static auto network_topology = "org.apache.cassandra.locator.NetworkTopologyStrategy";

    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto get_replication = [&] (const sstring& ks) {
            auto msg = e.execute_cql(
                format("SELECT JSON replication FROM system_schema.keyspaces WHERE keyspace_name = '{}'", ks)).get0();
            auto res = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            auto rows = res->rs().result_set().rows();
            BOOST_REQUIRE_EQUAL(rows.size(), 1);
            auto row0 = rows[0];
            BOOST_REQUIRE_EQUAL(row0.size(), 1);

            return json::to_json_value(sstring(to_sstring_view(*row0[0])))["replication"];
        };

        auto assert_replication_contains = [&] (const sstring& ks, const std::map<sstring, sstring>& kvs) {
            auto repl = get_replication(ks);
            for (const auto& [k, v] : kvs) {
                BOOST_REQUIRE_EQUAL(v, repl[k].asString());
            }
        };

        auto assert_replication_not_contains = [&] (const sstring& ks, const std::vector<sstring>& keys) {
            auto repl = get_replication(ks);
            return std::none_of(keys.begin(), keys.end(), [&] (const sstring& k) {
                return repl.isMember(std::string(k.begin(), k.end()));
            });
        };

        e.execute_cql("CREATE KEYSPACE rf_expand_1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}").get();
        assert_replication_contains("rf_expand_1", {
            {"class", network_topology},
            {"datacenter1", "3"}
        });

        // The auto-expansion should not change existing replication factors.
        e.execute_cql("ALTER KEYSPACE rf_expand_1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}").get();
        assert_replication_contains("rf_expand_1", {
            {"class", network_topology},
            {"datacenter1", "3"}
        });

        e.execute_cql("CREATE KEYSPACE rf_expand_2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").get();
        assert_replication_contains("rf_expand_2", {
            {"class", simple},
            {"replication_factor", "3"}
        });

        // Should auto-expand when switching from SimpleStrategy to NetworkTopologyStrategy without additional options.
        e.execute_cql("ALTER KEYSPACE rf_expand_2 WITH replication = {'class': 'NetworkTopologyStrategy'}").get();
        assert_replication_contains("rf_expand_2", {
            {"class", network_topology},
            {"datacenter1", "3"}
        });

        e.execute_cql("CREATE KEYSPACE rf_expand_3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").get();
        assert_replication_contains("rf_expand_3", {
            {"class", simple},
            {"replication_factor", "3"}
        });

        // Should not auto-expand when switching to NetworkTopologyStrategy with additional options.
        e.execute_cql("ALTER KEYSPACE rf_expand_3 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter2': 2}").get();
        assert_replication_not_contains("rf_expand_3", {"datacenter1"});

        // Respect factors specified manually.
        e.execute_cql("CREATE KEYSPACE rf_expand_4 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3, 'datacenter1': 2}").get();
        assert_replication_contains("rf_expand_4", {
            {"class", network_topology},
            {"datacenter1", "2"}
        });
    });
}

SEASTAR_TEST_CASE(test_int_sum_overflow) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, ck text, val int, primary key(pk, ck));");
        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p1', 'c1', 2147483647);");
        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p1', 'c2', 1);");
        auto sum_query = "select sum(val) from cf;";
        BOOST_REQUIRE_THROW(e.execute_cql(sum_query).get(), exceptions::overflow_error_exception);

        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p2', 'c1', -1);");
        auto result = e.execute_cql(sum_query).get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({int32_type->decompose(int32_t(2147483647))});

        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p3', 'c1', 2147483647);");
        BOOST_REQUIRE_THROW(e.execute_cql(sum_query).get(), exceptions::overflow_error_exception);

        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p3', 'c2', -2147483648);");
        result = e.execute_cql(sum_query).get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({int32_type->decompose(int32_t(2147483646))});
    });
}

SEASTAR_TEST_CASE(test_bigint_sum_overflow) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, ck text, val bigint, primary key(pk, ck));");
        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p1', 'c1', 9223372036854775807);");
        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p1', 'c2', 1);");
        auto sum_query = "select sum(val) from cf;";
        BOOST_REQUIRE_THROW(e.execute_cql(sum_query).get(), exceptions::overflow_error_exception);

        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p2', 'c1', -1);");
        auto result = e.execute_cql(sum_query).get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(9223372036854775807))});

        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p3', 'c1', 9223372036854775807);");
        BOOST_REQUIRE_THROW(e.execute_cql(sum_query).get(), exceptions::overflow_error_exception);

        cquery_nofail(e, "insert into cf (pk, ck, val) values ('p3', 'c2', -9223372036854775808);");
        result = e.execute_cql(sum_query).get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(9223372036854775806))});
    });
}

SEASTAR_TEST_CASE(test_bigint_sum) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, val bigint, primary key(pk));");
        cquery_nofail(e, "insert into cf (pk, val) values ('x', 2147483647);");
        cquery_nofail(e, "insert into cf (pk, val) values ('y', 2147483647);");
        auto sum_query = "select sum(val) from cf;";
        assert_that(e.execute_cql(sum_query).get0())
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(4294967294))});

        cquery_nofail(e, "insert into cf (pk, val) values ('z', -4294967295);");
        assert_that(e.execute_cql(sum_query).get0())
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(-1))});
    });
}

SEASTAR_TEST_CASE(test_int_sum_with_cast) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, val int, primary key(pk));");
        cquery_nofail(e, "insert into cf (pk, val) values ('a', 2147483647);");
        cquery_nofail(e, "insert into cf (pk, val) values ('b', 2147483647);");
        auto sum_as_bigint_query = "select sum(cast(val as bigint)) from cf;";
        assert_that(e.execute_cql(sum_as_bigint_query).get0())
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(4294967294))});

        cquery_nofail(e, "insert into cf (pk, val) values ('a', -2147483648);");
        cquery_nofail(e, "insert into cf (pk, val) values ('b', -2147483647);");
        assert_that(e.execute_cql(sum_as_bigint_query).get0())
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(-4294967295))});
    });
}

SEASTAR_TEST_CASE(test_float_sum_overflow) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, val float, primary key(pk));");
        BOOST_TEST_MESSAGE("make sure we can sum close to the max value");
        cquery_nofail(e, "insert into cf (pk, val) values ('a', 3.4028234e+38);");
        auto result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(3.4028234e+38f)});
        BOOST_TEST_MESSAGE("cause overflow");
        cquery_nofail(e, "insert into cf (pk, val) values ('b', 1e+38);");
        result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(std::numeric_limits<float>::infinity())});
        BOOST_TEST_MESSAGE("test maximum negative value");
        cquery_nofail(e, "insert into cf (pk, val) values ('a', -3.4028234e+38);");
        result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(-2.4028234e+38f)});
        BOOST_TEST_MESSAGE("cause negative overflow");
        cquery_nofail(e, "insert into cf (pk, val) values ('c', -2e+38);");
        result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(-std::numeric_limits<float>::infinity())});
    });
}

SEASTAR_TEST_CASE(test_double_sum_overflow) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, val double, primary key(pk));");
        BOOST_TEST_MESSAGE("make sure we can sum close to the max value");
        cquery_nofail(e, "insert into cf (pk, val) values ('a', 1.79769313486231570814527423732e+308);");
        auto result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(1.79769313486231570814527423732E308)});
        BOOST_TEST_MESSAGE("cause overflow");
        cquery_nofail(e, "insert into cf (pk, val) values ('b', 0.5e+308);");
        result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(std::numeric_limits<double>::infinity())});
        BOOST_TEST_MESSAGE("test maximum negative value");
        cquery_nofail(e, "insert into cf (pk, val) values ('a', -1.79769313486231570814527423732e+308);");
        result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(-1.29769313486231570814527423732e+308)});
        BOOST_TEST_MESSAGE("cause negative overflow");
        cquery_nofail(e, "insert into cf (pk, val) values ('c', -1e+308);");
        result = e.execute_cql("select sum(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({serialized(-std::numeric_limits<double>::infinity())});
    });
}

SEASTAR_TEST_CASE(test_int_avg) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, val int, primary key(pk));");
        cquery_nofail(e, "insert into cf (pk, val) values ('a', 2147483647);");
        cquery_nofail(e, "insert into cf (pk, val) values ('b', 2147483647);");
        auto result = e.execute_cql("select avg(val) from cf;").get0();
        assert_that(result)
            .is_rows()
            .with_size(1)
            .with_row({int32_type->decompose(int32_t(2147483647))});
    });
}

SEASTAR_TEST_CASE(test_bigint_avg) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table cf (pk text, val bigint, primary key(pk));");
        cquery_nofail(e, "insert into cf (pk, val) values ('x', 9223372036854775807);");
        cquery_nofail(e, "insert into cf (pk, val) values ('y', 9223372036854775807);");
        assert_that(e.execute_cql("select avg(val) from cf;").get0())
            .is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(9223372036854775807))});
    });
}

SEASTAR_TEST_CASE(test_view_with_two_regular_base_columns_in_key) {
    return do_with_cql_env_thread([] (auto& e) {
        cquery_nofail(e, "CREATE TABLE t (p int, c int, v1 int, v2 int, primary key(p,c))");
        auto schema = e.local_db().find_schema("ks", "t");

        // Create a CQL-illegal view with two regular base columns in the view key
        schema_builder view_builder("ks", "tv");
        view_builder.with_column(to_bytes("v1"), int32_type, column_kind::partition_key)
                .with_column(to_bytes("v2"), int32_type, column_kind::clustering_key)
                .with_column(to_bytes("p"), int32_type, column_kind::clustering_key)
                .with_column(to_bytes("c"), int32_type, column_kind::clustering_key)
                .with_view_info(*schema, false, "v1 IS NOT NULL AND v2 IS NOT NULL AND p IS NOT NULL AND c IS NOT NULL");

        schema_ptr view_schema = view_builder.build();
        service::get_local_migration_manager().announce_new_view(view_ptr(view_schema)).get();

        // Verify that deleting and restoring columns behaves as expected - i.e. the row is deleted and regenerated
        cquery_nofail(e, "INSERT INTO t (p, c, v1, v2) VALUES (1, 2, 3, 4)");
        auto msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        cquery_nofail(e, "UPDATE t SET v2 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);

        cquery_nofail(e, "UPDATE t SET v2 = 7 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(3), int32_type->decompose(7), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        cquery_nofail(e, "UPDATE t SET v1 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);


        cquery_nofail(e, "UPDATE t SET v1 = 9 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(9), int32_type->decompose(7), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        cquery_nofail(e, "UPDATE t SET v1 = NULL, v2 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);

        cquery_nofail(e, "UPDATE t SET v1 = 11, v2 = 13 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(11), int32_type->decompose(13), int32_type->decompose(1), int32_type->decompose(2)}},
        });
    });
}

/*!
 * \brief this helper function changes all white space into a single space for
 * a more robust testing.
 */
std::string normalize_white_space(const std::string& str) {
  return std::regex_replace(std::regex_replace(" " + str + " ", std::regex("\\s+"), " "), std::regex(", "), ",");
}

SEASTAR_TEST_CASE(test_describe_simple_schema) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        std::unordered_map<std::string, std::string> cql_create_tables {
            {"cf", "CREATE TABLE ks.cf (\n"
                "    pk blob,\n"
                "    \"COL2\" blob,\n"
                "    col1 blob,\n"
                "    PRIMARY KEY (pk)\n"
                ") WITH bloom_filter_fp_chance = 0.01\n"
                "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
                "    AND comment = ''\n"
                "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
                "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                "    AND crc_check_chance = 1\n"
                "    AND dclocal_read_repair_chance = 0.1\n"
                "    AND default_time_to_live = 0\n"
                "    AND gc_grace_seconds = 864000\n"
                "    AND max_index_interval = 2048\n"
                "    AND memtable_flush_period_in_ms = 0\n"
                "    AND min_index_interval = 128\n"
                "    AND read_repair_chance = 0\n"
                "    AND speculative_retry = '99.0PERCENTILE';\n"},
            {"cf1", "CREATE TABLE ks.cf1 (\n"
                "    pk blob,\n"
                "    ck blob,\n"
                "    col1 blob,\n"
                "    col2 blob,\n"
                "    PRIMARY KEY (pk, ck)\n"
                ") WITH CLUSTERING ORDER BY (ck ASC)\n"
                "    AND bloom_filter_fp_chance = 0.01\n"
                "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
                "    AND comment = ''\n"
                "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
                "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                "    AND crc_check_chance = 1\n"
                "    AND dclocal_read_repair_chance = 0.1\n"
                "    AND default_time_to_live = 0\n"
                "    AND gc_grace_seconds = 864000\n"
                "    AND max_index_interval = 2048\n"
                "    AND memtable_flush_period_in_ms = 0\n"
                "    AND min_index_interval = 128\n"
                "    AND read_repair_chance = 0\n"
                "    AND speculative_retry = '99.0PERCENTILE';\n"
            },
            {"CF2", "CREATE TABLE ks.\"CF2\" (\n"
                "    pk blob,\n"
                "    \"CK\" blob,\n"
                "    col1 blob,\n"
                "    col2 blob,\n"
                "    PRIMARY KEY (pk, \"CK\")\n"
                ") WITH CLUSTERING ORDER BY (\"CK\" DESC)\n"
                "    AND bloom_filter_fp_chance = 0.02\n"
                "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
                "    AND comment = ''\n"
                "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
                "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                "    AND crc_check_chance = 1\n"
                "    AND dclocal_read_repair_chance = 0.2\n"
                "    AND default_time_to_live = 0\n"
                "    AND gc_grace_seconds = 954000\n"
                "    AND max_index_interval = 3048\n"
                "    AND memtable_flush_period_in_ms = 10\n"
                "    AND min_index_interval = 128\n"
                "    AND read_repair_chance = 0\n"
                "    AND speculative_retry = '99.0PERCENTILE';\n"
            },
            {"Cf3", "CREATE TABLE ks.\"Cf3\" (\n"
                "    pk blob,\n"
                "    pk2 blob,\n"
                "    \"CK\" blob,\n"
                "    col1 blob,\n"
                "    col2 blob,\n"
                "    PRIMARY KEY ((pk, pk2), \"CK\")\n"
                ") WITH CLUSTERING ORDER BY (\"CK\" DESC)\n"
                "    AND bloom_filter_fp_chance = 0.02\n"
                "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
                "    AND comment = ''\n"
                "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
                "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                "    AND crc_check_chance = 1\n"
                "    AND dclocal_read_repair_chance = 0.2\n"
                "    AND default_time_to_live = 0\n"
                "    AND gc_grace_seconds = 954000\n"
                "    AND max_index_interval = 3048\n"
                "    AND memtable_flush_period_in_ms = 10\n"
                "    AND min_index_interval = 128\n"
                "    AND read_repair_chance = 0\n"
                "    AND speculative_retry = '99.0PERCENTILE';\n"
            },
            {"cf4", "CREATE TABLE ks.cf4 (\n"
                "    pk blob,\n"
                "    col1 blob,\n"
                "    col2 blob,\n"
                "    pn phone,\n"
                "    PRIMARY KEY (pk)\n"
                ") WITH "
                "     bloom_filter_fp_chance = 0.02\n"
                "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
                "    AND comment = ''\n"
                "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
                "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                "    AND crc_check_chance = 1\n"
                "    AND dclocal_read_repair_chance = 0.2\n"
                "    AND default_time_to_live = 0\n"
                "    AND gc_grace_seconds = 954000\n"
                "    AND max_index_interval = 3048\n"
                "    AND memtable_flush_period_in_ms = 10\n"
                "    AND min_index_interval = 128\n"
                "    AND read_repair_chance = 0\n"
                "    AND speculative_retry = '99.0PERCENTILE';\n"
            }

        };
        e.execute_cql("CREATE TYPE phone ("
            "country_code int,"
            "number text)"
        ).get();
        for (auto &&ct : cql_create_tables) {
            e.execute_cql(ct.second).get();
            auto schema = e.local_db().find_schema("ks", ct.first);
            std::ostringstream ss;

            schema->describe(ss);
            BOOST_CHECK_EQUAL(normalize_white_space(ss.str()), normalize_white_space(ct.second));
        }
    });
}

SEASTAR_TEST_CASE(test_describe_view_schema) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        std::string base_table = "CREATE TABLE \"KS\".\"cF\" (\n"
                "    pk blob,\n"
                "    pk1 blob,\n"
                "    ck blob,\n"
                "    ck1 blob,\n"
                "    \"COL1\" blob,\n"
                "    col2 blob,\n"
                "    PRIMARY KEY ((pk, pk1), ck, ck1)\n"
                ") WITH CLUSTERING ORDER BY (ck ASC, ck1 ASC)\n"
                "    AND bloom_filter_fp_chance = 0.01\n"
                "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
                "    AND comment = ''\n"
                "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
                "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                "    AND crc_check_chance = 1\n"
                "    AND dclocal_read_repair_chance = 0.1\n"
                "    AND default_time_to_live = 0\n"
                "    AND gc_grace_seconds = 864000\n"
                "    AND max_index_interval = 2048\n"
                "    AND memtable_flush_period_in_ms = 0\n"
                "    AND min_index_interval = 128\n"
                "    AND read_repair_chance = 0\n"
                "    AND speculative_retry = '99.0PERCENTILE';\n";

        std::unordered_map<std::string, std::string> cql_create_tables {
          {"cf_view", "CREATE MATERIALIZED VIEW \"KS\".cf_view AS\n"
              "    SELECT \"COL1\", pk, pk1, ck1, ck\n"
              "    FROM \"KS\".\"cF\"\n"
              "    WHERE pk IS NOT null AND \"COL1\" IS NOT null AND pk1 IS NOT null AND ck1 IS NOT null AND ck IS NOT null\n"
              "    PRIMARY KEY (\"COL1\", pk, pk1, ck1, ck)\n"
              "    WITH CLUSTERING ORDER BY (pk ASC, pk1 ASC, ck1 ASC, ck ASC)\n"
              "    AND bloom_filter_fp_chance = 0.01\n"
              "    AND caching = {'keys': 'ALL','rows_per_partition': 'ALL'}\n"
              "    AND comment = ''\n"
              "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
              "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
              "    AND crc_check_chance = 1\n"
              "    AND dclocal_read_repair_chance = 0.1\n"
              "    AND default_time_to_live = 0\n"
              "    AND gc_grace_seconds = 864000\n"
              "    AND max_index_interval = 2048\n"
              "    AND memtable_flush_period_in_ms = 0\n"
              "    AND min_index_interval = 128\n"
              "    AND read_repair_chance = 0\n"
              "    AND speculative_retry = '99.0PERCENTILE';\n"},
          {"cf_index_index", "CREATE INDEX cf_index ON \"KS\".\"cF\"(col2);"},
          {"cf_index1_index", "CREATE INDEX cf_index1 ON \"KS\".\"cF\"(pk);"},
          {"cf_index2_index", "CREATE INDEX cf_index2 ON \"KS\".\"cF\"(pk1);"},
          {"cf_index3_index", "CREATE INDEX cf_index3 ON \"KS\".\"cF\"(ck1);"},
          {"cf_index4_index", "CREATE INDEX cf_index4 ON \"KS\".\"cF\"((pk, pk1),col2);"}
        };

        e.execute_cql("CREATE KEYSPACE \"KS\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").get();
        e.execute_cql(base_table).get();

        for (auto &&ct : cql_create_tables) {
            e.execute_cql(ct.second).get();
            auto schema = e.local_db().find_schema("KS", ct.first);
            std::ostringstream ss;

            schema->describe(ss);
            BOOST_CHECK_EQUAL(normalize_white_space(ss.str()), normalize_white_space(ct.second));

            auto base_schema = e.local_db().find_schema("KS", "cF");
            std::ostringstream base_ss;
            base_schema->describe(base_ss);
            BOOST_CHECK_EQUAL(normalize_white_space(base_ss.str()), normalize_white_space(base_table));
        }
    });
}

shared_ptr<cql_transport::messages::result_message> cql_func_require_nofail(
        cql_test_env& env,
        const seastar::sstring& fct,
        const seastar::sstring& inp,
        std::unique_ptr<cql3::query_options>&& qo = nullptr,
        const std::experimental::source_location& loc = std::experimental::source_location::current()) {
    auto res = shared_ptr<cql_transport::messages::result_message>(nullptr);
    auto query = format("SELECT {}({}) FROM t;", fct, inp);
    try {
        if (qo) {
            res = env.execute_cql(query, std::move(qo)).get0();
        } else {
            res = env.execute_cql(query).get0();
        }
        BOOST_TEST_MESSAGE(format("Query '{}' succeeded as expected", query));
    } catch (...) {
        BOOST_ERROR(format("query '{}' failed unexpectedly with error: {}\n{}:{}: originally from here",
                query, std::current_exception(),
                loc.file_name(), loc.line()));
    }
    return res;
}

// FIXME: should be in cql_assertions, but we don't want to call boost from cql_assertions.hh
template <typename Exception>
void cql_func_require_throw(
        cql_test_env& env,
        const seastar::sstring& fct,
        const seastar::sstring& inp,
        std::unique_ptr<cql3::query_options>&& qo = nullptr,
        const std::experimental::source_location& loc = std::experimental::source_location::current()) {
    auto query = format("SELECT {}({}) FROM t;", fct, inp);
    try {
        if (qo) {
            env.execute_cql(query, std::move(qo)).get();
        } else {
            env.execute_cql(query).get();
        }
        BOOST_ERROR(format("query '{}' succeeded unexpectedly\n{}:{}: originally from here", query,
                loc.file_name(), loc.line()));
    } catch (Exception& e) {
        BOOST_TEST_MESSAGE(format("Query '{}' failed as expected with error: {}", query, e));
    } catch (...) {
        BOOST_ERROR(format("query '{}' failed with unexpected error: {}\n{}:{}: originally from here",
                query, std::current_exception(),
                loc.file_name(), loc.line()));
    }
}

static void create_time_uuid_fcts_schema(cql_test_env& e) {
    cquery_nofail(e, "CREATE TABLE t (id int primary key, t timestamp, l bigint, f float, u timeuuid, d date)");
    cquery_nofail(e, "INSERT INTO t (id, t, l, f, u, d) VALUES "
            "(1, 1579072460606, 1579072460606000, 1579072460606, a66525e0-3766-11ea-8080-808080808080, '2020-01-13')");
    cquery_nofail(e, "SELECT * FROM t;");
}

SEASTAR_TEST_CASE(test_basic_time_uuid_fcts) {
    return do_with_cql_env_thread([] (auto& e) {
        create_time_uuid_fcts_schema(e);

        cql_func_require_nofail(e, "currenttime", "");
        cql_func_require_nofail(e, "currentdate", "");
        cql_func_require_nofail(e, "now", "");
        cql_func_require_nofail(e, "currenttimeuuid", "");
        cql_func_require_nofail(e, "currenttimestamp", "");
    });
}

SEASTAR_TEST_CASE(test_time_uuid_fcts_input_validation) {
    return do_with_cql_env_thread([] (auto& e) {
        create_time_uuid_fcts_schema(e);

        // test timestamp arg
        auto require_timestamp = [&e] (const sstring& fct) {
            cql_func_require_nofail(e, fct, "t");
            cql_func_require_throw<exceptions::server_exception>(e, fct, "l");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "f");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "u");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "d");

            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttime()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currentdate()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "now()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttimeuuid()");
            cql_func_require_nofail(e, fct, "currenttimestamp()");
        };

        require_timestamp("mintimeuuid");
        require_timestamp("maxtimeuuid");

        // test timeuuid arg
        auto require_timeuuid = [&e] (const sstring& fct) {
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "t");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "l");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "f");
            cql_func_require_nofail(e, fct, "u");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "d");

            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttime()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currentdate()");
            cql_func_require_nofail(e, fct, "now()");
            cql_func_require_nofail(e, fct, "currenttimeuuid()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttimestamp()");
        };

        require_timeuuid("dateof");
        require_timeuuid("unixtimestampof");

        // test timeuuid or date arg
        auto require_timeuuid_or_date = [&e] (const sstring& fct) {
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "t");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "l");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "f");
            cql_func_require_nofail(e, fct, "u");
            cql_func_require_nofail(e, fct, "d");

            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttime()");
            cql_func_require_nofail(e, fct, "currentdate()");
            cql_func_require_nofail(e, fct, "now()");
            cql_func_require_nofail(e, fct, "currenttimeuuid()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttimestamp()");
        };

        require_timeuuid_or_date("totimestamp");

        // test timestamp or timeuuid arg
        auto require_timestamp_or_timeuuid = [&e] (const sstring& fct) {
            cql_func_require_nofail(e, fct, "t");
            cql_func_require_throw<std::exception>(e, fct, "l");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "f");
            cql_func_require_nofail(e, fct, "u");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "d");

            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttime()");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currentdate()");
            cql_func_require_nofail(e, fct, "now()");
            cql_func_require_nofail(e, fct, "currenttimeuuid()");
            cql_func_require_nofail(e, fct, "currenttimestamp()");
        };

        require_timestamp_or_timeuuid("todate");

        // test timestamp, timeuuid, or date arg
        auto require_timestamp_timeuuid_or_date = [&e] (const sstring& fct) {
            cql_func_require_nofail(e, fct, "t");
            cql_func_require_throw<exceptions::server_exception>(e, fct, "l");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "f");
            cql_func_require_nofail(e, fct, "u");
            cql_func_require_nofail(e, fct, "d");

            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "currenttime()");
            cql_func_require_nofail(e, fct, "currentdate()");
            cql_func_require_nofail(e, fct, "now()");
            cql_func_require_nofail(e, fct, "currenttimeuuid()");
            cql_func_require_nofail(e, fct, "currenttimestamp()");
        };

        require_timestamp_timeuuid_or_date("tounixtimestamp");
    });
}

SEASTAR_TEST_CASE(test_time_uuid_fcts_result) {
    return do_with_cql_env_thread([] (auto& e) {
        create_time_uuid_fcts_schema(e);

        // test timestamp arg
        auto require_timestamp = [&e] (const sstring& fct) {
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "mintimeuuid(t)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "maxtimeuuid(t)");
            cql_func_require_nofail(e, fct, "dateof(u)");
            cql_func_require_nofail(e, fct, "unixtimestampof(u)");
            cql_func_require_nofail(e, fct, "totimestamp(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "todate(u)");
            cql_func_require_nofail(e, fct, "tounixtimestamp(u)");
        };

        require_timestamp("mintimeuuid");
        require_timestamp("maxtimeuuid");

        // test timeuuid arg
        auto require_timeuuid = [&e] (const sstring& fct) {
            cql_func_require_nofail(e, fct, "mintimeuuid(t)");
            cql_func_require_nofail(e, fct, "maxtimeuuid(t)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "dateof(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "unixtimestampof(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "totimestamp(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "todate(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "tounixtimestamp(u)");
        };

        require_timeuuid("dateof");
        require_timeuuid("unixtimestampof");

        // test timeuuid or date arg
        auto require_timeuuid_or_date = [&e] (const sstring& fct) {
            cql_func_require_nofail(e, fct, "mintimeuuid(t)");
            cql_func_require_nofail(e, fct, "maxtimeuuid(t)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "dateof(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "unixtimestampof(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "totimestamp(u)");
            cql_func_require_nofail(e, fct, "todate(u)");
            cql_func_require_throw<exceptions::invalid_request_exception>(e, fct, "tounixtimestamp(u)");
        };

        require_timeuuid_or_date("totimestamp");

        // test timestamp or timeuuid arg
        auto require_timestamp_or_timeuuid = [&e] (const sstring& fct) {
        };

        require_timestamp_or_timeuuid("todate");

        // test timestamp, timeuuid, or date arg
        auto require_timestamp_timeuuid_or_date = [&e] (const sstring& fct) {
            cql_func_require_nofail(e, fct, "mintimeuuid(t)");
            cql_func_require_nofail(e, fct, "maxtimeuuid(t)");
            cql_func_require_nofail(e, fct, "dateof(u)");
            cql_func_require_nofail(e, fct, "unixtimestampof(u)");
            cql_func_require_nofail(e, fct, "totimestamp(u)");
            cql_func_require_nofail(e, fct, "todate(u)");
            cql_func_require_nofail(e, fct, "tounixtimestamp(u)");
        };

        require_timestamp_timeuuid_or_date("tounixtimestamp");
    });
}

// Test that tombstones with future timestamps work correctly
// when a write with lower timestamp arrives - in such case,
// if the base row is covered by such a tombstone, a view update
// needs to take it into account. Refs #5793
SEASTAR_TEST_CASE(test_views_with_future_tombstones) {
    return do_with_cql_env_thread([] (auto& e) {
        cquery_nofail(e, "CREATE TABLE t (a int, b int, c int, d int, e int, PRIMARY KEY (a,b,c));");
        cquery_nofail(e, "CREATE MATERIALIZED VIEW tv AS SELECT * FROM t"
                " WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (b,a,c);");

        // Partition tombstone
        cquery_nofail(e, "delete from t using timestamp 10 where a=1;");
        auto msg = cquery_nofail(e, "select * from t;");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "insert into t (a,b,c,d,e) values (1,2,3,4,5) using timestamp 8;");
        msg = cquery_nofail(e, "select * from t;");
        assert_that(msg).is_rows().with_size(0);
        msg = cquery_nofail(e, "select * from tv;");
        assert_that(msg).is_rows().with_size(0);

        // Range tombstone
        cquery_nofail(e, "delete from t using timestamp 16 where a=2 and b > 1 and b < 4;");
        msg = cquery_nofail(e, "select * from t;");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "insert into t (a,b,c,d,e) values (2,3,4,5,6) using timestamp 12;");
        msg = cquery_nofail(e, "select * from t;");
        assert_that(msg).is_rows().with_size(0);
        msg = cquery_nofail(e, "select * from tv;");
        assert_that(msg).is_rows().with_size(0);

        // Row tombstone
        cquery_nofail(e, "delete from t using timestamp 24 where a=3 and b=4 and c=5;");
        msg = cquery_nofail(e, "select * from t;");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "insert into t (a,b,c,d,e) values (3,4,5,6,7) using timestamp 18;");
        msg = cquery_nofail(e, "select * from t;");
        assert_that(msg).is_rows().with_size(0);
        msg = cquery_nofail(e, "select * from tv;");
        assert_that(msg).is_rows().with_size(0);
    });
}

SEASTAR_TEST_CASE(test_null_value_tuple_floating_types_and_uuids) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_for_single_type = [&e] (const shared_ptr<const abstract_type>& type, auto update_value) {
            cquery_nofail(e, format("CREATE TABLE IF NOT EXISTS t (k int PRIMARY KEY, test {})", type->cql3_type_name()));
            cquery_nofail(e, "INSERT INTO t (k, test) VALUES (0, null)");

            auto stmt = e.prepare(format("UPDATE t SET test={} WHERE k=0 IF test IN ?", update_value)).get0();
            auto list_type = list_type_impl::get_instance(type, true);
            // decomposed (null) value
            auto arg_value = list_type->decompose(
                make_list_value(list_type, {data_value::make_null(type)}));

            require_rows(e, stmt,
                {cql3::raw_value::make_value(std::move(arg_value))},
                {{boolean_type->decompose(true), std::nullopt}});

            cquery_nofail(e, "DROP TABLE t");
        };

        test_for_single_type(double_type, 1.0);
        test_for_single_type(float_type, 1.0f);
        test_for_single_type(uuid_type, utils::make_random_uuid());
        test_for_single_type(timeuuid_type, utils::UUID("00000000-0000-1000-0000-000000000000"));
    });
}

static std::unique_ptr<cql3::query_options> q_serial_opts() {
    const auto& so = cql3::query_options::specific_options::DEFAULT;
    auto qo = std::make_unique<cql3::query_options>(
            db::consistency_level::ONE,
            infinite_timeout_config,
            std::vector<cql3::raw_value>{},
            // Ensure (optional) serial consistency is always specified.
            cql3::query_options::specific_options{
                so.page_size,
                so.state,
                db::consistency_level::SERIAL,
                so.timestamp,
            }
    );
    return qo;
}

static auto exec_cql(::shared_ptr<cql_transport::messages::result_message>& msg, cql_test_env& e,
        cql3::prepared_cache_key_type& id, std::vector<cql3::raw_value> raw_values) {

    return seastar::async([&] () mutable -> std::optional<unsigned> {
        auto qo = q_serial_opts();
        msg = e.execute_prepared(id, raw_values).get0();
        if (msg->move_to_shard()) {
            return *msg->move_to_shard();
        }
        return std::nullopt;
    });
};

static void check_sharded(cql_test_env& e, const sstring& query,
        const std::vector<std::pair<std::vector<bytes>, std::vector<std::vector<bytes_opt>>>>& tests) {

    ::shared_ptr<cql_transport::messages::result_message> msg;
    auto id = e.prepare(query).get0();

    for (auto &test : tests) {
        auto& params = test.first;
        std::vector<cql3::raw_value> raw_values;
        for (auto& p : params) {
            raw_values.emplace_back(cql3::raw_value::make_value(p));
        }
        auto execute = [&] () mutable { return exec_cql(msg, e, id, raw_values); };
        std::optional<unsigned> shard;
        BOOST_REQUIRE_NO_THROW(shard = execute().get0());
        if (shard) {
            BOOST_REQUIRE_NO_THROW(smp::submit_to(*shard, std::move(execute)).get());
        }
        assert_that(msg).is_rows().with_rows_ignore_order(test.second);;
    }
}

SEASTAR_TEST_CASE(test_like_parameter_marker) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE t (pk int PRIMARY KEY, col text)").get();
        cquery_nofail(e, "INSERT INTO  t (pk, col) VALUES (1, 'aaa')").get();
        cquery_nofail(e, "INSERT INTO  t (pk, col) VALUES (2, 'bbb')").get();
        cquery_nofail(e, "INSERT INTO  t (pk, col) VALUES (3, 'ccc')").get();

        const sstring query("UPDATE t SET col = ? WHERE pk = ? IF col LIKE ?");
        check_sharded(e, query,
                { // params                          expected (applied, row)
                    {{T("err"), I(9), T("e%")}, {{boolean_type->decompose(false), {}}}},
                    {{T("chg"), I(1), T("a%")}, {{boolean_type->decompose(true),  "aaa"}}},
                    {{T("chg"), I(2), T("b%")}, {{boolean_type->decompose(true),  "bbb"}}},
                    {{T("err"), I(1), T("a%")}, {{boolean_type->decompose(false), "chg"}}},
                });
    });
}
