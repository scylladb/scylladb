/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "tests/urchin/cql_test_env.hh"
#include "tests/urchin/cql_assertions.hh"

#include "core/future-util.hh"
#include "transport/messages/result_message.hh"

SEASTAR_TEST_CASE(test_create_keyspace_statement) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create keyspace ks with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").discard_result();
    });
}

SEASTAR_TEST_CASE(test_create_table_statement) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table users (user_name varchar PRIMARY KEY, birth_year bigint);").discard_result().then([&e] {
            return e.execute_cql("create table cf (id int primary key, m map<int, int>, s set<text>, l list<uuid>);").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_insert_statement) {
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks_name) {
            // CQL: create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));
            return schema({}, ks_name, "cf",
                          {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);
        }).then([&e] {
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
        return e.create_table([](auto ks) {
            // CQL: create table cf (k bytes, c bytes, v bytes, s1 bytes static, primary key (k, c));
            return schema({}, ks, "cf",
                {{"k", bytes_type}},
                {{"c", bytes_type}},
                {{"v", bytes_type}},
                {{"s1", bytes_type}, {"s2", bytes_type}},
                utf8_type);
        }).then([&e] {
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

SEASTAR_TEST_CASE(test_map_insert_update) {
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks_name) {
            // CQL: create table cf (p1 varchar primary key, map1 map<int, int>);
            auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
            return schema({}, ks_name, "cf",
                          {{"p1", utf8_type}}, {}, {{"map1", my_map_type}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, map1) values ('key1', { 1001: 2001 });").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{1001, 2001}}));
        }).then([&e] {
            return e.execute_cql("update cf set map1[1002] = 2002 where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{1001, 2001},
                                                                                  {1002, 2002}}));
        }).then([&e] {
            // overwrite an element
            return e.execute_cql("update cf set map1[1001] = 3001 where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{1001, 3001},
                                                                                  {1002, 2002}}));
        }).then([&e] {
            // overwrite whole map
            return e.execute_cql("update cf set map1 = {1003: 4003} where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{1003, 4003}}));
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
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{1001, 5001},
                                                                                  {1002, 5002},
                                                                                  {1003, 5003}}));
        }).then([&e] {
            // discard some keys
            return e.execute_cql("update cf set map1 = map1 - {1001, 1003, 1005} where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{{1002, 5002}}}));
        }).then([&e] {
            return e.execute_cql("select * from cf where p1 = 'key1';").then([](auto msg) {
                auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                                  {utf8_type->decompose(sstring("key1"))},
                                  {my_map_type->decompose(map_type_impl::native_type{{{1002, 5002}}})},
                              });
            });
        }).then([&e] {
            // overwrite an element
            return e.execute_cql("update cf set map1[1009] = 5009 where p1 = 'key1';").discard_result();
        }).then([&e] {
            // delete a key
            return e.execute_cql("delete map1[1002] from cf where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                              "map1", map_type_impl::native_type({{{1009, 5009}}}));
        });
    });
}

SEASTAR_TEST_CASE(test_set_insert_update) {
    return do_with_cql_env([] (auto&e) {
        return e.create_table([](auto ks_name) {
            // CQL: create table cf (p1 varchar primary key, set1 set<int>);
            auto my_set_type = set_type_impl::get_instance(int32_type, true);
            return schema({}, ks_name, "cf",
                          {{"p1", utf8_type}}, {}, {{"set1", my_set_type}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, set1) values ('key1', { 1001 });").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", set_type_impl::native_type({1001}));
        }).then([&e] {
            return e.execute_cql("update cf set set1 = set1 + { 1002 } where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", set_type_impl::native_type({1001, 1002}));
        }).then([&e] {
            // overwrite an element
            return e.execute_cql("update cf set set1 = set1 + { 1001 } where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", set_type_impl::native_type({1001, 1002}));
        }).then([&e] {
            // overwrite entire set
            return e.execute_cql("update cf set set1 = { 1007, 1019 } where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", set_type_impl::native_type({1007, 1019}));
        }).then([&e] {
            // discard keys
            return e.execute_cql("update cf set set1 = set1 - { 1007, 1008 } where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", set_type_impl::native_type({1019}));
        }).then([&e] {
            return e.execute_cql("select * from cf where p1 = 'key1';").then([](auto msg) {
                auto my_set_type = set_type_impl::get_instance(int32_type, true);
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                                  {utf8_type->decompose(sstring("key1"))},
                                  {my_set_type->decompose(set_type_impl::native_type{{1019}})},
                              });
            });
        }).then([&e] {
            return e.execute_cql("update cf set set1 = set1 + { 1009 } where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("delete set1[1019] from cf where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                                            "set1", set_type_impl::native_type({1009}));
        });
    });
}

SEASTAR_TEST_CASE(test_list_insert_update) {
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks_name) {
            // CQL: create table cf (p1 varchar primary key, list1 list<int>);
            auto my_list_type = list_type_impl::get_instance(int32_type, true);
            return schema({}, ks_name, "cf",
                {{"p1", utf8_type}}, {}, {{"list1", my_list_type}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, list1) values ('key1', [ 1001 ]);").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({boost::any(1001)}));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = [ 1002, 1003 ] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({boost::any(1002), boost::any(1003)}));
        }).then([&e] {
            return e.execute_cql("update cf set list1[1] = 2003 where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({boost::any(1002), boost::any(2003)}));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = list1 - [1002, 2004] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({2003}));
        }).then([&e] {
            return e.execute_cql("select * from cf where p1 = 'key1';").then([] (auto msg) {
                auto my_list_type = list_type_impl::get_instance(int32_type, true);
                assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                         {utf8_type->decompose(sstring("key1"))},
                         {my_list_type->decompose(list_type_impl::native_type{{2003}})},
                     });
            });
        }).then([&e] {
            return e.execute_cql("update cf set list1 = [2008, 2009, 2010] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("delete list1[1] from cf where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({2008, 2010}));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = list1 + [2012, 2019] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({2008, 2010, 2012, 2019}));
        }).then([&e] {
            return e.execute_cql("update cf set list1 = [2001, 2002] + list1 where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.require_column_has_value("cf", {sstring("key1")}, {},
                    "list1", list_type_impl::native_type({2001, 2002, 2008, 2010, 2012, 2019}));
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
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
            using namespace transport::messages;
            struct validator : result_message::visitor {
                std::vector<bytes_opt> res;
                virtual void visit(const result_message::void_message&) override { throw "bad"; }
                virtual void visit(const result_message::set_keyspace&) override { throw "bad"; }
                virtual void visit(const result_message::prepared&) override { throw "bad"; }
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
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {int32_type->decompose(6)},
                     {long_type->decompose(3L)},
                 });
        }).then([&] {
            return e.execute_cql("select count(*) from cf where p1 = 'key1';");
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {long_type->decompose(3L)},
                 });
        }).then([&] {
            return e.execute_cql("select count(1) from cf where p1 = 'key1';");
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
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
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
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
        return e.create_table([](auto ks_name) {
            // CQL: create table cf (p1 varchar primary key, i int);
            return schema({}, ks_name, "cf",
                {{"p1", utf8_type}}, {}, {{"i", int32_type}}, {}, utf8_type);
        }).then([&e] {
            auto q = sprint("insert into cf (p1, i) values ('key1', 1) using timestamp %d;", the_timestamp);
            return e.execute_cql(q).discard_result();
        }).then([&e] {
            return e.execute_cql("select writetime(i) from cf where p1 in ('key1');");
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({{
                     {long_type->decompose(int64_t(the_timestamp))},
                 }});
        });
    });
}

SEASTAR_TEST_CASE(test_batch) {
    return do_with_cql_env([] (auto&& e) {
        return e.create_table([](auto ks_name) {
            // CQL: create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));
            return schema({}, ks_name, "cf",
                          {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);
        }).then([&e] {
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
        }).then([&e, tt] (shared_ptr<transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({{
                     {tt->decompose(tuple_type_impl::native_type({int32_t(1001), int64_t(2001), sstring("abc1")}))},
                }});
        });
    });
}

SEASTAR_TEST_CASE(test_user_type) {
    auto make_user_type = [] {
        return user_type_impl::get_instance("ks", to_bytes("ut1"),
                {to_bytes("my_int"), to_bytes("my_bigint"), to_bytes("my_text")},
                {int32_type, long_type, utf8_type});
    };
    return do_with_cql_env([make_user_type] (cql_test_env& e) {
        // We don't have "CREATE TYPE" yet, so we must insert the type manually
        e.local_db().find_or_create_keyspace("ks")._user_types.add_type(make_user_type());
        return e.create_table([make_user_type] (auto ks_name) {
            // CQL: "create table cf (id int primary key, t ut1)";
            return schema({}, ks_name, "cf",
                {{"id", int32_type}}, {}, {{"t", make_user_type()}}, {}, utf8_type);
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (1, (1001, 2001, 'abc1'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1001)), long_type->decompose(int64_t(2001)), utf8_type->decompose(sstring("abc1"))},
                });
        }).then([&e] {
            return e.execute_cql("update cf set t = { my_int: 1002, my_bigint: 2002, my_text: 'abc2' } where id = 1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1002)), long_type->decompose(int64_t(2002)), utf8_type->decompose(sstring("abc2"))},
                });
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (2, (frozen<ut1>)(2001, 3001, 'abc4'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t from cf where id = 2;");
        }).then([&e, make_user_type] (shared_ptr<transport::messages::result_message> msg) {
            auto ut = make_user_type();
            auto ut_val = user_type_impl::native_type({boost::any(int32_t(2001)),
                                                       boost::any(int64_t(3001)),
                                                       boost::any(sstring("abc4"))});
            assert_that(msg).is_rows()
                .with_rows({
                     {ut->decompose(ut_val)},
                });
        });
    });
}
