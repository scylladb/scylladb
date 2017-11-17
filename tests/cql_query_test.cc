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

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "core/future-util.hh"
#include "core/sleep.hh"
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_create_keyspace_statement) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create keyspace ks2 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").discard_result().then([&e] {
            return e.require_keyspace_exists("ks2");
        });
    });
}

SEASTAR_TEST_CASE(test_create_table_statement) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table users (user_name varchar PRIMARY KEY, birth_year bigint);").discard_result().then([&e] {
            return e.require_table_exists("ks", "users");
        }).then([&e] {
            return e.execute_cql("create table cf (id int primary key, m map<int, int>, s set<text>, l list<uuid>);").discard_result();
        }).then([&e] {
            return e.require_table_exists("ks", "cf");
        });
    });
}

SEASTAR_TEST_CASE(test_insert_statement) {
    return do_with_cql_env([] (auto& e) {
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
   return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks_name) {
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
            return e.execute_cql("select * from cf where p1 = 'key1' and c2 = 2 and c1 = 1;").then([] (auto msg) {
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
            return e.execute_cql("select r1 from cf where p1 = 'key1' and c2 = 2 and c1 = 1;").then([] (auto msg) {
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {int32_type->decompose(3)}
                     });
            });
        }).then([&e] {
            // Test full partition range, singular clustering range
            return e.execute_cql("select * from cf where c1 = 1 and c2 = 2 allow filtering;").then([] (auto msg) {
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
    return do_with_cql_env([] (auto& e) {
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
                sprint("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = %s", key)).then(
                [](auto msg) {
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

        return e.create_table([](auto ks_name) {
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
            static auto make_key = [](int suffix) { return sprint("0xdeadbeefcafebabe%02d", suffix); };
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
   return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks_name) {
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
           return e.execute_cql("select v from cf where k = 0x00").then([] (auto msg) {
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
            return e.execute_cql("select v from cf where k = 0x00 and c0 = 0x02 allow filtering;").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
                });
            });
        }).then([&e] {
            return e.execute_cql("select v from cf where k = 0x00 and c0 > 0x02 allow filtering;").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("07")}, {from_hex("08")}
                });
            });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}, {from_hex("05")}, {from_hex("06")}, {from_hex("07")}, {from_hex("08")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 and c0 < 0x03 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 > 0x02 and c0 <= 0x03 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("07")}, {from_hex("08")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 and c0 <= 0x02 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 < 0x02 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("01")}, {from_hex("02")}, {from_hex("03")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 = 0x02 and c1 > 0x02 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("05")}, {from_hex("06")}
               });
           });
        }).then([&e] {
           return e.execute_cql("select v from cf where k = 0x00 and c0 = 0x02 and c1 >= 0x02 and c1 <= 0x02 allow filtering;").then([] (auto msg) {
               assert_that(msg).is_rows().with_rows({
                   {from_hex("04")}
               });
           });
        });
    });
}

SEASTAR_TEST_CASE(test_ordering_of_composites_with_variable_length_components) {
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks) {
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
            return e.execute_cql("select v from cf where k = 0x00 allow filtering;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")}, {from_hex("02")}, {from_hex("03")}, {from_hex("04")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_query_with_static_columns) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (k blob, c blob, v blob, s1 blob static, s2 blob static, primary key (k, c));").discard_result().then([&e] {
            return e.execute_cql("update cf set s1 = 0x01 where k = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x02 where k = 0x00 and c = 0x01;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x03 where k = 0x00 and c = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1, v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                    {from_hex("01"), from_hex("03")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1 from cf limit 1;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1, v from cf limit 1;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                });
            });
        }).then([&e] {
            return e.execute_cql("update cf set v = null where k = 0x00 and c = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf (k, c) values (0x00, 0x02);").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            // Try 'in' restriction out
            return e.execute_cql("select s1, v from cf where k = 0x00 and c in (0x01, 0x02);").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                    {from_hex("01"), {}}
                });
            });
        }).then([&e] {
            // Verify that limit is respected for multiple clustering ranges and that static columns
            // are populated when limit kicks in.
            return e.execute_cql("select s1, v from cf where k = 0x00 and c in (0x01, 0x02) limit 1;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_insert_without_clustering_key) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (k blob, v blob, primary key (k));").discard_result().then([&e] {
            return e.execute_cql("insert into cf (k) values (0x01);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), {}}
                });
            });
        }).then([&e] {
            return e.execute_cql("select k from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_limit_is_respected_across_partitions) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (k blob, c blob, v blob, s1 blob static, primary key (k, c));").discard_result().then([&e] {
            return e.execute_cql("update cf set s1 = 0x01 where k = 0x01;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set s1 = 0x02 where k = 0x02;").discard_result();
        }).then([&e] {
            // Determine partition order
            return e.execute_cql("select k from cf;");
        }).then([&e](auto msg) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            std::vector<bytes> keys;
            for (auto&& row : rows->rs().rows()) {
                BOOST_REQUIRE(row[0]);
                keys.push_back(*row[0]);
            }
            BOOST_REQUIRE(keys.size() == 2);
            bytes k1 = keys[0];
            bytes k2 = keys[1];

            return now().then([k1, k2, &e] {
                return e.execute_cql("select s1 from cf limit 1;").then([k1, k2](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {k1},
                    });
                });
            }).then([&e, k1, k2] {
                return e.execute_cql("select s1 from cf limit 2;").then([k1, k2](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {k1}, {k2}
                    });
                });
            }).then([&e, k1, k2] {
                return e.execute_cql(sprint("update cf set s1 = null where k = 0x%s;", to_hex(k1))).discard_result();
            }).then([&e, k1, k2] {
                return e.execute_cql("select s1 from cf limit 1;").then([k1, k2](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {k2}
                    });
                });
            }).then([&e, k1, k2] {
                return e.execute_cql(sprint("update cf set s1 = null where k = 0x%s;", to_hex(k2))).discard_result();
            }).then([&e, k1, k2] {
                return e.execute_cql("select s1 from cf limit 1;").then([k1, k2](auto msg) {
                    assert_that(msg).is_rows().is_empty();
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_partitions_have_consistent_ordering_in_range_query) {
    return do_with_cql_env([] (auto& e) {
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
        }).then([&e](auto msg) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            std::vector<bytes> keys;
            for (auto&& row : rows->rs().rows()) {
                BOOST_REQUIRE(row[0]);
                keys.push_back(*row[0]);
            }
            BOOST_REQUIRE(keys.size() == 6);

            return now().then([keys, &e] {
                return e.execute_cql("select k from cf limit 1;").then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 2;").then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 3;").then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 4;").then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]},
                        {keys[3]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 5;").then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            }).then([keys, &e] {
                return e.execute_cql("select k from cf limit 6;").then([keys](auto msg) {
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
    return do_with_cql_env([] (auto& e) {
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
        }).then([&e](auto msg) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            std::vector<bytes> keys;
            std::vector<int64_t> tokens;
            for (auto&& row : rows->rs().rows()) {
                BOOST_REQUIRE(row[0]);
                BOOST_REQUIRE(row[1]);
                keys.push_back(*row[0]);
                tokens.push_back(value_cast<int64_t>(long_type->deserialize(*row[1])));
            }
            BOOST_REQUIRE(keys.size() == 5);

            return now().then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) > %d;", tokens[1])).then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) >= %ld;", tokens[1])).then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[1]},
                        {keys[2]},
                        {keys[3]},
                        {keys[4]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) > %ld and token(k) < %ld;",
                        tokens[1], tokens[4])).then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[2]},
                        {keys[3]},
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) < %ld;", tokens[3])).then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[0]},
                        {keys[1]},
                        {keys[2]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) = %ld;", tokens[3])).then([keys](auto msg) {
                    assert_that(msg).is_rows().with_rows({
                        {keys[3]}
                    });
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) < %ld and token(k) > %ld;", tokens[3], tokens[3])).then([keys](auto msg) {
                    assert_that(msg).is_rows().is_empty();
                });
            }).then([keys, tokens, &e] {
                return e.execute_cql(sprint("select k from cf where token(k) >= %ld and token(k) <= %ld;", tokens[4], tokens[2])).then([keys](auto msg) {
                    assert_that(msg).is_rows().is_empty();
                });
            }).then([keys, tokens, &e] {
                auto min_token = std::numeric_limits<int64_t>::min();
                return e.execute_cql(sprint("select k from cf where token(k) > %ld and token (k) < %ld;", min_token, min_token)).then([keys](auto msg) {
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
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks) {
            // CQL: create table cf (k bytes, c bytes, v bytes, primary key (k, c));
            return schema({}, ks, "cf",
                {{"k", bytes_type}}, {{"c", bytes_type}}, {{"v", bytes_type}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (k, c, v) values (0x00, 0x05, 0x01) using timestamp 1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("update cf using timestamp 2 set v = null where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {{}},
                });
            });
        }).then([&e] {
            // same tampstamp, dead cell wins
            return e.execute_cql("update cf using timestamp 2 set v = 0x02 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {{}},
                });
            });
        }).then([&e] {
            return e.execute_cql("update cf using timestamp 3 set v = 0x02 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("02")},
                });
            });
        }).then([&e] {
            // same timestamp, greater value wins
            return e.execute_cql("update cf using timestamp 3 set v = 0x03 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("03")},
                });
            });
        }).then([&e] {
            // same tampstamp, delete whole row, delete should win
            return e.execute_cql("delete from cf using timestamp 3 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().is_empty();
            });
        }).then([&e] {
            // same timestamp, update should be shadowed by range tombstone
            return e.execute_cql("update cf using timestamp 3 set v = 0x04 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().is_empty();
            });
        }).then([&e] {
            return e.execute_cql("update cf using timestamp 4 set v = 0x04 where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("04")},
                });
            });
        }).then([&e] {
            // deleting an orphan cell (row is considered as deleted) yields no row
            return e.execute_cql("update cf using timestamp 5 set v = null where k = 0x00 and c = 0x05;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().is_empty();
            });
        });
    });
}

SEASTAR_TEST_CASE(test_range_deletion_scenarios) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v text, primary key (p, c));").get();
        for (auto i = 0; i < 10; ++i) {
            e.execute_cql(sprint("insert into cf (p, c, v) values (1, %d, 'abc');", i)).get();
        }

        try {
            e.execute_cql("delete from cf where p = 1 and c <= 3").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }
        try {
            e.execute_cql("delete from cf where p = 1 and c >= 0").get();
            BOOST_FAIL("should've thrown");
        } catch (...) { }

        e.execute_cql("delete from cf where p = 1 and c >= 0 and c <= 3").get();
        auto msg = e.execute_cql("select * from cf").get0();
        assert_that(msg).is_rows().with_size(6);
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

SEASTAR_TEST_CASE(test_map_insert_update) {
    return do_with_cql_env([] (auto& e) {
        auto make_my_map_type = [] { return map_type_impl::get_instance(int32_type, int32_type, true); };
        auto my_map_type = make_my_map_type();
        return e.create_table([make_my_map_type] (auto ks_name) {
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
        }).then_wrapped([](auto f) {
            BOOST_REQUIRE(f.failed());
            std::move(f).discard_result();
        }).then([&e] {
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
            return e.execute_cql("select * from cf where p1 = 'key1';").then([my_map_type](auto msg) {
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
    return do_with_cql_env([] (auto&e) {
        auto make_my_set_type = [] { return set_type_impl::get_instance(int32_type, true); };
        auto my_set_type = make_my_set_type();
        return e.create_table([make_my_set_type](auto ks_name) {
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
            return e.execute_cql("select * from cf where p1 = 'key1';").then([my_set_type](auto msg) {
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
        });
    });
}

SEASTAR_TEST_CASE(test_list_insert_update) {
    return do_with_cql_env([] (auto& e) {
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
            return e.execute_cql("select * from cf where p1 = 'key1';").then([my_list_type] (auto msg) {
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
        });
    });
}

SEASTAR_TEST_CASE(test_functions) {
    return do_with_cql_env([] (auto&& e) {
        return e.create_table([](auto ks_name) {
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
                    BOOST_REQUIRE_EQUAL(rows.rs().rows().size(), 3);
                    for (auto&& rw : rows.rs().rows()) {
                        BOOST_REQUIRE_EQUAL(rw.size(), 1);
                        res.push_back(rw[0]);
                    }
                }
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

static const api::timestamp_type the_timestamp = 123456789;
SEASTAR_TEST_CASE(test_writetime_and_ttl) {
    return do_with_cql_env([] (auto&& e) {
        return e.execute_cql("create table cf (p1 varchar primary key, i int);").discard_result().then([&e] {
            auto q = sprint("insert into cf (p1, i) values ('key1', 1) using timestamp %d;", the_timestamp);
            return e.execute_cql(q).discard_result();
        }).then([&e] {
            return e.execute_cql("select writetime(i) from cf where p1 in ('key1');");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({{
                     {long_type->decompose(int64_t(the_timestamp))},
                 }});
        });
    });
}

SEASTAR_TEST_CASE(test_batch) {
    return do_with_cql_env([] (auto&& e) {
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
    return do_with_cql_env([tt, make_tt] (auto& e) {
        return e.create_table([make_tt] (auto ks_name) {
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
        }).then([&e, tt] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(int32_t(1)), tt->decompose(make_tuple_value(tt, tuple_type_impl::native_type({int32_t(1), int64_t(2), sstring("abc")}))) }
            });
        });
    });
}

SEASTAR_TEST_CASE(test_user_type) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create type ut1 (my_int int, my_bigint bigint, my_text text);").discard_result().then([&e] {
            return e.execute_cql("create table cf (id int primary key, t frozen <ut1>);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (1, (1001, 2001, 'abc1'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1001)), long_type->decompose(int64_t(2001)), utf8_type->decompose(sstring("abc1"))},
                });
        }).then([&e] {
            return e.execute_cql("update cf set t = { my_int: 1002, my_bigint: 2002, my_text: 'abc2' } where id = 1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1002)), long_type->decompose(int64_t(2002)), utf8_type->decompose(sstring("abc2"))},
                });
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (2, (frozen<ut1>)(2001, 3001, 'abc4'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t from cf where id = 2;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            auto ut = user_type_impl::get_instance("ks", to_bytes("ut1"),
                        {to_bytes("my_int"), to_bytes("my_bigint"), to_bytes("my_text")},
                        {int32_type, long_type, utf8_type});
            auto ut_val = make_user_value(ut,
                          user_type_impl::native_type({int32_t(2001),
                                                       int64_t(3001),
                                                       sstring("abc4")}));
            assert_that(msg).is_rows()
                .with_rows({
                     {ut->decompose(ut_val)},
                });
        });
    });
}

//
// Since durations don't have a well-defined ordering on their semantic value, a number of restrictions exist on their
// use.
//
SEASTAR_TEST_CASE(test_duration_restrictions) {
    auto validate_request_failure = [] (cql_test_env& env, const sstring& request, const sstring& expected_message) {
        return futurize_apply([&] { return env.execute_cql(request); }).then_wrapped([expected_message] (auto f) {
            BOOST_REQUIRE_EXCEPTION(f.get(),
                exceptions::invalid_request_exception,
                [&expected_message](auto &&ire) {
                    BOOST_REQUIRE_EQUAL(expected_message, ire.what());
                    return true;
                });
        });
    };

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
    return do_with_cql_env([] (auto&& e) {
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
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace ks3-1 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace ks3 with replication = { 'replication_factor' : 1 };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace ks3 with rreplication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create keyspace SyStEm with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
        });
    });
}

SEASTAR_TEST_CASE(test_validate_table) {
    return do_with_cql_env([] (cql_test_env& e) {
        return make_ready_future<>().then([&e] {
            return e.execute_cql("create table ttttttttttttttttttttttttttttttttttttttttbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (foo text PRIMARY KEY, bar text);");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, foo text);");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb-1 (foo text PRIMARY KEY, bar text);");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text, bar text);");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text PRIMARY KEY);");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text) with commment = 'aaaa';");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text) with min_index_interval = -1;");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb (foo text PRIMARY KEY, bar text) with min_index_interval = 1024 and max_index_interval = 128;");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
        });
    });
}

SEASTAR_TEST_CASE(test_table_compression) {
    return do_with_cql_env([] (cql_test_env& e) {
        return make_ready_future<>().then([&e] {
            return e.execute_cql("create table tb1 (foo text PRIMARY KEY, bar text) with compression = { };");
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            e.require_table_exists("ks", "tb1");
            BOOST_REQUIRE(e.local_db().find_schema("ks", "tb1")->get_compressor_params().get_compressor() == compressor::none);
            return e.execute_cql("create table tb5 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : '' };");
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            e.require_table_exists("ks", "tb5");
            BOOST_REQUIRE(e.local_db().find_schema("ks", "tb5")->get_compressor_params().get_compressor() == compressor::none);
            return e.execute_cql("create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LossyCompressor' };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : -1 };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 3 };");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("create table tb2 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 2 };");
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            e.require_table_exists("ks", "tb2");
            BOOST_REQUIRE(e.local_db().find_schema("ks", "tb2")->get_compressor_params().get_compressor() == compressor::lz4);
            BOOST_REQUIRE(e.local_db().find_schema("ks", "tb2")->get_compressor_params().chunk_length() == 2 * 1024);
            return e.execute_cql("create table tb3 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'DeflateCompressor' };");
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            e.require_table_exists("ks", "tb3");
            BOOST_REQUIRE(e.local_db().find_schema("ks", "tb3")->get_compressor_params().get_compressor() == compressor::deflate);
            return e.execute_cql("create table tb4 (foo text PRIMARY KEY, bar text) with compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.DeflateCompressor' };");
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            e.require_table_exists("ks", "tb4");
            BOOST_REQUIRE(e.local_db().find_schema("ks", "tb4")->get_compressor_params().get_compressor() == compressor::deflate);
        });
    });
}

SEASTAR_TEST_CASE(test_ttl) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto make_my_list_type = [] { return list_type_impl::get_instance(utf8_type, true); };
        auto my_list_type = make_my_list_type();
        return e.create_table([make_my_list_type] (auto ks_name) {
            return schema({}, ks_name, "cf",
                {{"p1", utf8_type}}, {}, {{"r1", utf8_type}, {"r2", utf8_type}, {"r3", make_my_list_type()}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql(
                "update cf using ttl 1000 set r1 = 'value1_1', r3 = ['a', 'b', 'c'] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql(
                "update cf using ttl 1 set r1 = 'value1_3', r3 = ['a', 'b', 'c'] where p1 = 'key3';").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf using ttl 1 set r3[1] = 'b' where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf using ttl 1 set r1 = 'value1_2' where p1 = 'key2';").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, r2) values ('key2', 'value2_2');").discard_result();
        }).then([&e, my_list_type] {
            return e.execute_cql("select r1 from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_size(3)
                    .with_row({utf8_type->decompose(sstring("value1_1"))})
                    .with_row({utf8_type->decompose(sstring("value1_2"))})
                    .with_row({utf8_type->decompose(sstring("value1_3"))});
            });
        }).then([&e, my_list_type] {
            return e.execute_cql("select r3 from cf where p1 = 'key1';").then([my_list_type] (auto msg) {
                auto my_list_type = list_type_impl::get_instance(utf8_type, true);
                assert_that(msg).is_rows().with_rows({
                    {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{sstring("a"), sstring("b"), sstring("c")}}))}
                });
            });
        }).then([&e] {
            forward_jump_clocks(2s);
            return e.execute_cql("select r1, r2 from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({{}, utf8_type->decompose(sstring("value2_2"))})
                    .with_row({utf8_type->decompose(sstring("value1_1")), {}});
            });
        }).then([&e] {
            return e.execute_cql("select r2 from cf;").then([] (auto msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({ utf8_type->decompose(sstring("value2_2")) })
                    .with_row({ {} });
            });
        }).then([&e] {
            return e.execute_cql("select r1 from cf;").then([] (auto msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({ {} })
                    .with_row({ utf8_type->decompose(sstring("value1_1")) });
            });
        }).then([&e, my_list_type] {
            return e.execute_cql("select r3 from cf where p1 = 'key1';").then([] (auto msg) {
                auto my_list_type = list_type_impl::get_instance(utf8_type, true);
                assert_that(msg).is_rows().with_rows({
                    {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{sstring("a"), sstring("c")}}))}
                });
            });
        }).then([&e] {
            return e.execute_cql("create table cf2 (p1 text PRIMARY KEY, r1 text, r2 text);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 5;").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), utf8_type->decompose(sstring("bar"))}
                });
            });
        }).then([&e] {
            forward_jump_clocks(6s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2;").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("select count(*) from cf2;").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {long_type->decompose(int64_t(0))}
                });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 5;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf2 set r1 = null where p1 = 'foo';").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), { }}
                });
            });
        }).then([&e] {
            forward_jump_clocks(6s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 5;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r2) values ('foo', null);").discard_result();
        }).then([&e] {
            forward_jump_clocks(6s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), { }}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_types) {
    return do_with_cql_env([] (cql_test_env& e) {
        return make_ready_future<>().then([&e] {
            return e.execute_cql(
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
                    ");").discard_result();
        }).then([&e] {
            e.require_table_exists("ks", "all_types");
            return e.execute_cql(
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
                    ");").discard_result();
        }).then([&e] {
            return e.execute_cql("SELECT * FROM all_types WHERE a = 'ascii'");
        }).then([&e] (auto msg) {
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
                    inet_addr_type->decompose(net::ipv4_address("127.0.0.1")),
                    int32_type->decompose(3), utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                    timestamp_type->decompose(tp),
                    timeuuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    uuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    utf8_type->decompose(sstring("varchar")), varint_type->decompose(boost::multiprecision::cpp_int(123)),
                    decimal_type->decompose(big_decimal { 2, boost::multiprecision::cpp_int(123) }),
                    byte_type->decompose(int8_t(3)),
                    short_type->decompose(int16_t(3)),
                    simple_date_type->decompose(int32_t(0x80000001)),
                    time_type->decompose(int64_t(0x0000000000000001)),
                    duration_type->decompose(cql_duration("1y2mo3w4d5h6m7s8ms9us10ns"))
                }
            });
            return e.execute_cql(
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
                    ");").discard_result();
        }).then([&e] {
             return e.execute_cql("SELECT * FROM all_types WHERE a = 'ascii2'");
        }).then([&e] (auto msg) {
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
                    inet_addr_type->decompose(net::ipv4_address("127.0.0.1")),
                    int32_type->decompose(3), utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                    timestamp_type->decompose(tp),
                    timeuuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    uuid_type->decompose(utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"))),
                    utf8_type->decompose(sstring("varchar")), varint_type->decompose(boost::multiprecision::cpp_int(123)),
                    decimal_type->decompose(big_decimal { 2, boost::multiprecision::cpp_int(123) }),
                    byte_type->decompose(int8_t(3)),
                    short_type->decompose(int16_t(3)),
                    simple_date_type->decompose(int32_t(0x80000001)),
                    time_type->decompose(int64_t(0x0000000000000001)),
                    duration_type->decompose(cql_duration("10y9mo8w7d6h5m4s3ms2us1ns"))
                }
            });
        });
    });
}

SEASTAR_TEST_CASE(test_order_by) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table torder (p1 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY(p1, c1, c2));").discard_result().then([&e] {
            e.require_table_exists("ks", "torder");
            return e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 1, 2, 3);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 2, 1, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("select  c1, c2, r1 from torder where p1 = 0 order by c1 asc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 desc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
            });
            return e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 1, 1, 4);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into torder (p1, c1, c2, r1) values (0, 2, 2, 5);").discard_result();
        }).then([&e] {
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 desc, c2 desc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
            });
            return e.execute_cql("insert into torder (p1, c1, c2, r1) values (1, 1, 0, 6);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into torder (p1, c1, c2, r1) values (1, 2, 3, 7);").discard_result();
        }).then([&e] {
            return e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 desc, c2 desc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(7)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
                {int32_type->decompose(1), int32_type->decompose(0), int32_type->decompose(6)},
            });
        }).then([&e] {
            return e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 asc, c2 asc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(0), int32_type->decompose(6)},
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(7)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) and c1 < 2 order by c1 desc, c2 desc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) and c1 >= 2 order by c1 asc, c2 asc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 desc, c2 desc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(7)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 in (0, 1) order by c1 asc, c2 asc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(0), int32_type->decompose(6)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 > 1 order by c1 desc, c2 desc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 desc, c2 desc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 desc, c2 desc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 desc, c2 desc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 > 1 order by c1 asc, c2 asc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 asc, c2 asc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
                {int32_type->decompose(2), int32_type->decompose(2), int32_type->decompose(5)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 and c1 >= 2 order by c1 asc, c2 asc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2), int32_type->decompose(1), int32_type->decompose(0)},
            });
            return e.execute_cql("select c1, c2, r1 from torder where p1 = 0 order by c1 asc, c2 asc limit 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(4)},
            });
        });
    });
}

SEASTAR_TEST_CASE(test_order_by_validate) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table torderv (p1 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY(p1, c1, c2));").discard_result().then([&e] {
            return e.execute_cql("select c2, r1 from torderv where p1 = 0 order by c desc;");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("select c2, r1 from torderv where p1 = 0 order by c2 desc;");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("select c2, r1 from torderv where p1 = 0 order by c1 desc, c2 asc;");
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("select c2, r1 from torderv order by c1 asc;");
        }).then_wrapped([] (auto f) {
            assert_that_failed(f);
        });
    });
}

SEASTAR_TEST_CASE(test_multi_column_restrictions) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table tmcr (p1 int, c1 int, c2 int, c3 int, r1 int, PRIMARY KEY (p1, c1, c2, c3));").discard_result().then([&e] {
            e.require_table_exists("ks", "tmcr");
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 0, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 0, 1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 1, 0, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 0, 1, 1, 3);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 0, 0, 4);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 0, 1, 5);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 1, 0, 6);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tmcr (p1, c1, c2, c3, r1) values (0, 1, 1, 1, 7);").discard_result();
        }).then([&e] {
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) = (0, 1, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2) = (0, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) in ((0, 1, 0), (1, 0, 1), (0, 1, 0));");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(5)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2) in ((0, 1), (1, 0), (0, 1));");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
                {int32_type->decompose(4)},
                {int32_type->decompose(5)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) >= (1, 0, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(5)},
                {int32_type->decompose(6)},
                {int32_type->decompose(7)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) >= (0, 1, 1) and (c1, c2, c3) < (1, 1, 0);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
                {int32_type->decompose(4)},
                {int32_type->decompose(5)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2) >= (0, 1) and (c1, c2, c3) < (1, 0, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
                {int32_type->decompose(4)},
            });
            return e.execute_cql("select r1 from tmcr where p1 = 0 and (c1, c2, c3) > (0, 1, 0) and (c1, c2) <= (0, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
            });
        });
    });
}

SEASTAR_TEST_CASE(test_select_distinct) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table tsd (p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&e] {
            e.require_table_exists("ks", "tsd");
            return e.execute_cql("insert into tsd (p1, c1, r1) values (0, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd (p1, c1, r1) values (1, 1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd (p1, c1, r1) values (1, 1, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd (p1, c1, r1) values (2, 2, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd (p1, c1, r1) values (2, 3, 3);").discard_result();
        }).then([&e] {
            return e.execute_cql("select distinct p1 from tsd;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0)})
                .with_row({int32_type->decompose(1)})
                .with_row({int32_type->decompose(2)});
            return e.execute_cql("select distinct p1 from tsd limit 3;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0)})
                .with_row({int32_type->decompose(1)})
                .with_row({int32_type->decompose(2)});
            return e.execute_cql("create table tsd2 (p1 int, p2 int, c1 int, r1 int, PRIMARY KEY ((p1, p2), c1));").discard_result();
        }).then([&e] {
            e.require_table_exists("ks", "tsd2");
            return e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (0, 0, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (0, 0, 1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (1, 1, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (1, 1, 1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (2, 2, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd2 (p1, p2, c1, r1) values (2, 2, 1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("select distinct p1, p2 from tsd2;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0), int32_type->decompose(0)})
                .with_row({int32_type->decompose(1), int32_type->decompose(1)})
                .with_row({int32_type->decompose(2), int32_type->decompose(2)});
            return e.execute_cql("select distinct p1, p2 from tsd2 limit 3;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0), int32_type->decompose(0)})
                .with_row({int32_type->decompose(1), int32_type->decompose(1)})
                .with_row({int32_type->decompose(2), int32_type->decompose(2)});
            return e.execute_cql("create table tsd3 (p1 int, r1 int, PRIMARY KEY (p1));").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd3 (p1, r1) values (0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd3 (p1, r1) values (1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd3 (p1, r1) values (1, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd3 (p1, r1) values (2, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("select distinct p1 from tsd3;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0)})
                .with_row({int32_type->decompose(1)})
                .with_row({int32_type->decompose(2)});
            return e.execute_cql("create table tsd4 (p1 int, c1 int, s1 int static, r1 int, PRIMARY KEY (p1, c1));").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd4 (p1, c1, s1, r1) values (0, 0, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd4 (p1, c1, r1) values (0, 1, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd4 (p1, s1) values (2, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tsd4 (p1, s1) values (3, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("select distinct p1, s1 from tsd4;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(3)
                .with_row({int32_type->decompose(0), int32_type->decompose(0)})
                .with_row({int32_type->decompose(2), int32_type->decompose(1)})
                .with_row({int32_type->decompose(3), int32_type->decompose(2)});
        });
    });
}

SEASTAR_TEST_CASE(test_batch_insert_statement) {
    return do_with_cql_env([] (auto& e) {
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
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table tir (p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&e] {
            e.require_table_exists("ks", "tir");
            return e.execute_cql("insert into tir (p1, c1, r1) values (0, 0, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tir (p1, c1, r1) values (1, 0, 1);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tir (p1, c1, r1) values (1, 1, 2);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tir (p1, c1, r1) values (1, 2, 3);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tir (p1, c1, r1) values (2, 3, 4);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tir where p1 in ();");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(0);
            return e.execute_cql("select r1 from tir where p1 in (2, 0, 2, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(4)},
                {int32_type->decompose(0)},
                {int32_type->decompose(4)},
                {int32_type->decompose(1)},
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
            return e.execute_cql("select r1 from tir where p1 = 1 and c1 in ();");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_size(0);
            return e.execute_cql("select r1 from tir where p1 = 1 and c1 in (2, 0, 2, 1);");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(1)},
                {int32_type->decompose(2)},
                {int32_type->decompose(3)},
            });
            return e.execute_cql("select r1 from tir where p1 = 1 and c1 in (2, 0, 2, 1) order by c1 desc;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(3)},
                {int32_type->decompose(2)},
                {int32_type->decompose(1)},
            });
        });
    });
}

SEASTAR_TEST_CASE(test_compact_storage) {
    return do_with_cql_env([] (auto& e) {
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
        }).then([&e] (auto msg) {
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
        }).then([&e] (auto msg) {
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
        }).then([&e] (auto msg) {
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
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
                { int32_type->decompose(1), int32_type->decompose(3), bytes(), int32_type->decompose(8) },
                { int32_type->decompose(1), int32_type->decompose(3), int32_type->decompose(5), int32_type->decompose(7) },
            });
            return e.execute_cql("delete from tcs3 where p1 = 1 and c1 = 3 and c2 = 5;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs3 where p1 = 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
                { int32_type->decompose(1), int32_type->decompose(3), bytes(), int32_type->decompose(8) },
            });
            return e.execute_cql("delete from tcs3 where p1 = 1 and c1 = 3 and c2 = blobasint(0x);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs3 where p1 = 1;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(3), {}, int32_type->decompose(6) },
            });
            return e.execute_cql("create table tcs4 (p1 int PRIMARY KEY, c1 int, c2 int) with compact storage;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tcs4 (p1) values (1);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tcs4;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({ });
        });
    });
}

SEASTAR_TEST_CASE(test_collections_of_collections) {
    return do_with_cql_env([] (auto& e) {
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
    return do_with_cql_env([] (auto& e) {
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
        }).then([&e] (auto msg) {
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
    return do_with_cql_env([] (auto& e) {
        auto set_of_ints = set_type_impl::get_instance(int32_type, false);
        auto list_of_ints = list_type_impl::get_instance(int32_type, false);
        auto frozen_map_of_set_and_list = map_type_impl::get_instance(set_of_ints, list_of_ints, false);
        return e.execute_cql("CREATE TABLE tfc (a int, b int, c frozen<map<set<int>, list<int>>> static, d int, PRIMARY KEY (a, b));").discard_result().then([&e] {
            return e.execute_cql("INSERT INTO tfc (a, b, c, d) VALUES (0, 0, {}, 0);").discard_result();
        }).then([&e] {
            return e.execute_cql("SELECT * FROM tfc;");
        }).then([&e, frozen_map_of_set_and_list] (auto msg) {
            map_type_impl::mutation_view empty_mv{};
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(0), int32_type->decompose(0), frozen_map_of_set_and_list->to_value(empty_mv, cql_serialization_format::internal()), int32_type->decompose(0) },
            });
        });
    });
}


SEASTAR_TEST_CASE(test_alter_table) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table tat (pk1 int, c1 int, ck2 int, r1 int, r2 int, PRIMARY KEY (pk1, c1, ck2));").discard_result().then([&e] {
            return e.execute_cql("insert into tat (pk1, c1, ck2, r1, r2) values (1, 2, 3, 4, 5);").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table tat with comment = 'This is a comment.';").discard_result();
        }).then([&e] {
            BOOST_REQUIRE_EQUAL(e.local_db().find_schema("ks", "tat")->comment(), sstring("This is a comment."));
            return e.execute_cql("alter table tat alter r2 type blob;").discard_result();
        }).then([&e] {
            return e.execute_cql("select pk1, c1, ck2, r1, r2 from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(5) },
            });
        }).then([&e] {
            return e.execute_cql("insert into tat (pk1, c1, ck2, r2) values (1, 2, 3, 0x1234567812345678);").discard_result();
        }).then([&e] {
            return e.execute_cql("select pk1, c1, ck2, r1, r2 from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), from_hex("1234567812345678") },
            });
        }).then([&e] {
            return e.execute_cql("alter table tat rename pk1 to p1 and ck2 to c2;").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, c1, c2, r1, r2 from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat add r1_2 int;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into tat (p1, c1, c2, r1_2) values (1, 2, 3, 6);").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(6), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat drop r1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), int32_type->decompose(6), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat add r1 int;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), {}, int32_type->decompose(6), from_hex("1234567812345678") },
            });
            return e.execute_cql("alter table tat drop r2;").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table tat add r2 int;").discard_result();
        }).then([&e] {
            return e.execute_cql("select * from tat;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(2), int32_type->decompose(3), {}, int32_type->decompose(6), {} },
            });
        });
    });
}

SEASTAR_TEST_CASE(test_map_query) {
    return do_with_cql_env([] (auto& e) {
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
    return do_with_cql_env([] (auto& e) {
        return seastar::async([&e] {
            e.execute_cql("create table tmp (pk int, v int, PRIMARY KEY (pk));").get();
            e.execute_cql("drop columnfamily tmp;").get();
            e.execute_cql("create table tmp (pk int, v int, PRIMARY KEY (pk));").get();
            e.execute_cql("drop columnfamily tmp;").get();
        });
    });
}

SEASTAR_TEST_CASE(test_reversed_slice_with_empty_range_before_all_rows) {
    return do_with_cql_env([] (auto& e) {
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
    return do_with_cql_env([] (auto& e) {
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
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table tatv (p1 int, c1 int, c2 int, r1 int, r2 set<int>, PRIMARY KEY (p1, c1, c2));").discard_result().then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv drop r2;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r2 list<int>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r2 set<text>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r2 set<int>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv rename r2 to r3;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv alter r1 type bigint;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv alter r2 type map<int, int>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r3 map<int, int>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r4 set<text>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv drop r3;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv drop r4;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r3 map<int, text>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r4 set<int>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("alter table tatv add r3 map<int, blob>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
            return e.execute_cql("alter table tatv add r4 set<blob>;").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert(!f.failed());
        });
    });
}

SEASTAR_TEST_CASE(test_pg_style_string_literal) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table test (p1 text, PRIMARY KEY (p1));").discard_result().then([&e] {
            return e.execute_cql("insert into test (p1) values ($$Apostrophe's$ $ not$ $ '' escaped$$);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into test (p1) values ($$$''valid$_$key$$);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into test (p1) values ('$normal$valid$$$$key$');").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into test (p1) values ($ $invalid$$);").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("insert into test (p1) values ($$invalid$ $);").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("insert into test (p1) values ($ $invalid$$$);").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("insert into test (p1) values ($$ \n\n$invalid);").discard_result();
        }).then_wrapped([&e] (auto f) {
            assert_that_failed(f);
            return e.execute_cql("select * from test;");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { utf8_type->decompose(sstring("Apostrophe's$ $ not$ $ '' escaped")) },
                { utf8_type->decompose(sstring("$''valid$_$key")) },
                { utf8_type->decompose(sstring("$normal$valid$$$$key$")) },
            });
        });
    });
}

SEASTAR_TEST_CASE(test_secondary_index_query) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("CREATE TABLE users (userid int, name text, email text, country text, PRIMARY KEY (userid));").discard_result().then([&e] {
            return e.execute_cql("CREATE INDEX ON users (email);").discard_result();
        }).then([&e] {
            return e.execute_cql("CREATE INDEX ON users (country);").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (0, 'Bondie Easseby', 'beassebyv@house.gov', 'France');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (1, 'Demetri Curror', 'dcurrorw@techcrunch.com', 'France');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (2, 'Langston Paulisch', 'lpaulischm@reverbnation.com', 'United States');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (3, 'Channa Devote', 'cdevote14@marriott.com', 'Denmark');").discard_result();
        }).then([&e] {
            return e.execute_cql("SELECT email FROM users WHERE country = 'France';");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { utf8_type->decompose(sstring("beassebyv@house.gov")) },
                { utf8_type->decompose(sstring("dcurrorw@techcrunch.com")) },
            });
        });
    });
}
