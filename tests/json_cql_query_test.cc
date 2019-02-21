/*
 * Copyright (C) 2018 ScyllaDB
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

#include <seastar/net/inet_address.hh>

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "core/future-util.hh"
#include "core/sleep.hh"
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_select_json_types) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
            "CREATE TABLE all_types ("
                "    a ascii PRIMARY KEY,"
                "    b bigint,"
                "    c blob,"
                "    d boolean,"
                "    e double,"
                "    f float,"
                "    \"G\" inet," // note - case-sensitive column names
                "    \"H\" int,"
                "    \"I\" text,"
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
                "    w int,"
                ");").get();

        e.require_table_exists("ks", "all_types").get();
        e.execute_cql(
            "INSERT INTO all_types (a, b, c, d, e, f, \"G\", \"H\", \"I\", j, k, l, m, n, o, p, q, r, s, u) VALUES ("
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

        auto msg = e.execute_cql("SELECT JSON a, b, c, d, e, f, \"G\", \"H\", \"I\", j, k, l, m, n, o, p, q, r, s, u, w, unixtimestampof(k) FROM all_types WHERE a = 'ascii'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                utf8_type->decompose(
                    "{\"a\": \"ascii\", "
                    "\"b\": 123456789, "
                    "\"c\": \"0xdeadbeef\", "
                    "\"d\": true, "
                    "\"e\": 3.14, "
                    "\"f\": 3.14, "
                    "\"\\\"G\\\"\": \"127.0.0.1\", " // note the double quoting on case-sensitive column names
                    "\"\\\"H\\\"\": 3, "
                    "\"\\\"I\\\"\": \"zażółć gęślą jaźń\", "
                    "\"j\": \"2001-10-18T14:15:55.134000\", "
                    "\"k\": \"d2177dd0-eaa2-11de-a572-001b779c76e3\", "
                    "\"l\": \"d2177dd0-eaa2-11de-a572-001b779c76e3\", "
                    "\"m\": \"varchar\", "
                    "\"n\": 123, "
                    "\"o\": 1.23, "
                    "\"p\": 3, "
                    "\"q\": 3, "
                    "\"r\": \"1970-01-02\", "
                    "\"s\": 00:00:00.000000001, "
                    "\"u\": \"1y2mo25d5h6m7s8ms9us10ns\", "
                    "\"w\": null, "
                    "\"unixtimestampof(k)\": 1261009589805}"
                )
            }
         });

        msg = e.execute_cql("SELECT toJson(a), toJson(b), toJson(c), toJson(d), toJson(e), toJson(f),"
                "toJson(\"G\"), toJson(\"H\"), toJson(\"I\"), toJson(j), toJson(k), toJson(l), toJson(m), toJson(n),"
                "toJson(o), toJson(p), toJson(q), toJson(r), toJson(s), toJson(u), toJson(w),"
                "toJson(unixtimestampof(k)), toJson(toJson(toJson(p))) FROM all_types WHERE a = 'ascii'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                utf8_type->decompose("\"ascii\""),
                utf8_type->decompose("123456789"),
                utf8_type->decompose("\"0xdeadbeef\""),
                utf8_type->decompose("true"),
                utf8_type->decompose("3.14"),
                utf8_type->decompose("3.14"),
                utf8_type->decompose("\"127.0.0.1\""),
                utf8_type->decompose("3"),
                utf8_type->decompose("\"zażółć gęślą jaźń\""),
                utf8_type->decompose("\"2001-10-18T14:15:55.134000\""),
                utf8_type->decompose("\"d2177dd0-eaa2-11de-a572-001b779c76e3\""),
                utf8_type->decompose("\"d2177dd0-eaa2-11de-a572-001b779c76e3\""),
                utf8_type->decompose("\"varchar\""),
                utf8_type->decompose("123"),
                utf8_type->decompose("1.23"),
                utf8_type->decompose("3"),
                utf8_type->decompose("3"),
                utf8_type->decompose("\"1970-01-02\""),
                utf8_type->decompose("00:00:00.000000001"),
                utf8_type->decompose("\"1y2mo25d5h6m7s8ms9us10ns\""),
                utf8_type->decompose("null"),
                utf8_type->decompose("1261009589805"),
                utf8_type->decompose("\"\\\"3\\\"\"")
            }
        });
    });
}

SEASTAR_TEST_CASE(test_select_json_collections) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
            "CREATE TABLE collections ("
                "    a text PRIMARY KEY,"
                "    b map<int, text>,"
                "    c set<float>,"
                "    d list<frozen<list<tinyint>>>"
                ");").get();

        e.require_table_exists("ks", "collections").get();

        e.execute_cql(
            "INSERT INTO collections (a, b, c, d) VALUES ("
                "    'key',"
                "    { 1 : 'abc', 3 : 'de', 2 : '!' },"
                "    { 0.0, 4.5, 2.25, 1.125, NAN, INFINITY },"
                "    [[ 3 , 1, 4, 1, 5, 9 ], [ ], [ 1, 1, 2 ]]"
                ");").get();

        auto msg = e.execute_cql("SELECT JSON * FROM collections WHERE a = 'key'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                utf8_type->decompose(
                    "{\"a\": \"key\", "
                    "\"b\": {\"1\": \"abc\", \"2\": \"!\", \"3\": \"de\"}, "
                    "\"c\": [0, 1.125, 2.25, 4.5, null, null], " // note - one null is for NAN, one for INFINITY
                    "\"d\": [[3, 1, 4, 1, 5, 9], [], [1, 1, 2]]}"
                )
            }
         });

        msg = e.execute_cql("SELECT toJson(a), toJson(b), toJson(c), toJson(d) FROM collections WHERE a = 'key'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                utf8_type->decompose("\"key\""),
                utf8_type->decompose("{\"1\": \"abc\", \"2\": \"!\", \"3\": \"de\"}"),
                utf8_type->decompose("[0, 1.125, 2.25, 4.5, null, null]"),
                utf8_type->decompose("[[3, 1, 4, 1, 5, 9], [], [1, 1, 2]]"),
            }
         });

        try {
            e.execute_cql("SELECT toJson(a, b) FROM collections WHERE a = 'key'").get();
            BOOST_FAIL("should've thrown");
        } catch (...) {}
        try {
            e.execute_cql("SELECT toJson() FROM collections WHERE a = 'key'").get();
            BOOST_FAIL("should've thrown");
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_insert_json_types) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
            "CREATE TABLE all_types ("
                "    a ascii PRIMARY KEY,"
                "    b bigint,"
                "    c blob,"
                "    d boolean,"
                "    e double,"
                "    f float,"
                "    \"G\" inet,"
                "    \"H\" int,"
                "    \"I\" text,"
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
            "INSERT INTO all_types JSON '"
                "{\"a\": \"ascii\", "
                "\"b\": 123456789, "
                "\"c\": \"0xdeadbeef\", "
                "\"d\": true, "
                "\"e\": 3.14, "
                "\"f\": \"3.14\", "
                "\"\\\"G\\\"\": \"127.0.0.1\", "
                "\"\\\"H\\\"\": 3, "
                "\"\\\"I\\\"\": \"zażółć gęślą jaźń\", "
                "\"j\": \"2001-10-18T14:15:55.134+0000\", "
                "\"k\": \"d2177dd0-eaa2-11de-a572-001b779c76e3\", "
                "\"l\": \"d2177dd0-eaa2-11de-a572-001b779c76e3\", "
                "\"m\": \"varchar\", "
                "\"n\": 123, "
                "\"o\": 1.23, "
                "\"p\": \"3\", "
                "\"q\": 3, "
                "\"r\": \"1970-01-02\", "
                "\"s\": \"00:00:00.000000001\", "
                "\"u\": \"1y2mo25d5h6m7s8ms9us10ns\"}"
                "'").get();

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
                ascii_type->decompose(sstring("ascii")),
                inet_addr_type->decompose(net::inet_address("127.0.0.1")), // note - case-sensitive columns go right after the key
                int32_type->decompose(3), utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                long_type->decompose(123456789l),
                from_hex("deadbeef"), boolean_type->decompose(true),
                double_type->decompose(3.14), float_type->decompose(3.14f),
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

        e.execute_cql("UPDATE all_types SET b = fromJson('42') WHERE a = fromJson('\"ascii\"');").get();
        e.execute_cql("UPDATE all_types SET \"I\" = fromJson('\"zażółć gęślą jaźń\"') WHERE a = fromJson('\"ascii\"');").get();
        e.execute_cql("UPDATE all_types SET n = fromJson('\"2147483648\"') WHERE a = fromJson('\"ascii\"');").get();
        e.execute_cql("UPDATE all_types SET o = fromJson('\"3.45\"') WHERE a = fromJson('\"ascii\"');").get();

        msg = e.execute_cql("SELECT a, b, \"I\", n, o FROM all_types WHERE a = 'ascii'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                ascii_type->decompose(sstring("ascii")),
                long_type->decompose(42l),
                utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                varint_type->decompose(boost::multiprecision::cpp_int(2147483648)),
                decimal_type->decompose(big_decimal { 2, boost::multiprecision::cpp_int(345) }),
            }
        });

        e.execute_cql("CREATE TABLE multi_column_pk_table (p1 int, p2 int, p3 int, c1 int, c2 int, v int, PRIMARY KEY((p1, p2, p3), c1, c2));").get();
        e.require_table_exists("ks", "multi_column_pk_table").get();

        e.execute_cql("INSERT INTO multi_column_pk_table JSON '"
                "{\"p1\": 1, "
                "\"p2\": 2, "
                "\"p3\": 3, "
                "\"c1\": 4, "
                "\"c2\": 5, "
                "\"v\": 6 "
                "}'").get();

        msg = e.execute_cql("SELECT * FROM multi_column_pk_table").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(3),
                int32_type->decompose(4),
                int32_type->decompose(5),
                int32_type->decompose(6)
            }
        });
    });
}

SEASTAR_TEST_CASE(test_insert_json_collections) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
            "CREATE TABLE collections ("
                "    a text PRIMARY KEY,"
                "    b map<int, text>,"
                "    c set<float>,"
                "    d list<frozen<list<tinyint>>>"
                ");").get();

        e.require_table_exists("ks", "collections").get();

        e.execute_cql(
            "INSERT INTO collections JSON '"
                "{\"a\": \"key\", "
                "\"b\": {\"1\": \"abc\", \"2\": \"!\", \"3\": \"de\"}, "
                "\"c\": [0, 1.125, 2.25, 4.5], "
                "\"d\": [[3, 1, 4, 1, 5, 9], [], [1, 1, 2]]}"
                "'").get();

        auto msg = e.execute_cql("SELECT JSON * FROM collections WHERE a = 'key'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                utf8_type->decompose(
                    "{\"a\": \"key\", "
                    "\"b\": {\"1\": \"abc\", \"2\": \"!\", \"3\": \"de\"}, "
                    "\"c\": [0, 1.125, 2.25, 4.5], "
                    "\"d\": [[3, 1, 4, 1, 5, 9], [], [1, 1, 2]]}"
                )
            }
         });

        e.execute_cql("INSERT INTO collections JSON '{\"a\": \"key2\"}'").get();
        msg = e.execute_cql("SELECT JSON * FROM collections WHERE a = 'key2'").get0();
        assert_that(msg).is_rows().with_rows({
            {
                utf8_type->decompose(
                    "{\"a\": \"key2\", "
                    "\"b\": null, "
                    "\"c\": null, "
                    "\"d\": null}"
                )
            }
         });
    });
}

SEASTAR_TEST_CASE(test_prepared_json) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto prepared = e.execute_cql(
            "CREATE TABLE json_data ("
                "    a text PRIMARY KEY,"
                "    b map<int, text>,"
                "    c decimal,"
                "    d list<float>"
                ");").get();

        e.require_table_exists("ks", "json_data").get();

        cql3::prepared_cache_key_type prepared_id = e.prepare(
            "begin batch \n"
                "  insert into json_data json :named_bound0; \n"
                "  insert into json_data json ?; \n"
                "  insert into json_data json :named_bound1; \n"
                "  insert into json_data json ?; \n"
                "apply batch;").get0();

        std::vector<cql3::raw_value> raw_values;
        raw_values.emplace_back(cql3::raw_value::make_value(utf8_type->decompose(
            "{\"a\": \"a1\", \"b\": {\"3\": \"three\", \"6\": \"six\", \"0\": \"zero\"}, \"c\": 1.23, \"d\": [1.25, 3.75, 2.5]}")));
        raw_values.emplace_back(cql3::raw_value::make_value(utf8_type->decompose(
            "{\"a\": \"a2\", \"b\": {\"6\": \"six\", \"0\": \"zero\"}, \"c\": 1.23, \"d\": [3.75, 2.5]}")));
        raw_values.emplace_back(cql3::raw_value::make_value(utf8_type->decompose(
            "{\"a\": \"a3\", \"b\": {\"3\": \"three\", \"0\": \"zero\"}, \"c\": 1.23, \"d\": [1.25, 2.5]}")));
        raw_values.emplace_back(cql3::raw_value::make_value(utf8_type->decompose(
            "{\"a\": \"a4\", \"b\": {\"1\": \"one\"}, \"c\": 1.23, \"d\": [1]}")));

        e.execute_prepared(prepared_id, raw_values).get();

        auto msg = e.execute_cql("select json * from json_data where a='a1'").get0();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a1\", \"b\": {\"0\": \"zero\", \"3\": \"three\", \"6\": \"six\"}, \"c\": 1.23, \"d\": [1.25, 3.75, 2.5]}")
        }});
        msg = e.execute_cql("select json * from json_data where a='a2'").get0();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a2\", \"b\": {\"0\": \"zero\", \"6\": \"six\"}, \"c\": 1.23, \"d\": [3.75, 2.5]}")
        }});
        msg = e.execute_cql("select json * from json_data where a='a3'").get0();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a3\", \"b\": {\"0\": \"zero\", \"3\": \"three\"}, \"c\": 1.23, \"d\": [1.25, 2.5]}")
        }});
        msg = e.execute_cql("select json * from json_data where a='a4'").get0();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a4\", \"b\": {\"1\": \"one\"}, \"c\": 1.23, \"d\": [1]}")
        }});

    });
}

SEASTAR_TEST_CASE(test_json_default_unset) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto prepared = e.execute_cql(
            "CREATE TABLE json_data ("
                "    a text,"
                "    b int,"
                "    c int,"
                "    d int,"
                "PRIMARY KEY (a, b));").get();

        e.require_table_exists("ks", "json_data").get();

        e.execute_cql("INSERT INTO json_data JSON '{\"a\": \"abc\", \"b\": 2, \"c\": 1}'").get();

        auto msg = e.execute_cql("select * from json_data where a='abc'").get0();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("abc"))},
            {int32_type->decompose(2)},
            {int32_type->decompose(1)},
            {}
        }});

        e.execute_cql("INSERT INTO json_data JSON '{\"a\": \"abc\", \"b\": 2, \"d\": 5}' DEFAULT UNSET").get();

        msg = e.execute_cql("select * from json_data where a='abc'").get0();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("abc"))},
            {int32_type->decompose(2)},
            {int32_type->decompose(1)},
            {int32_type->decompose(5)}
        }});

        e.execute_cql("INSERT INTO json_data JSON '{\"a\": \"abc\", \"b\": 2, \"d\": 4}' DEFAULT NULL").get();

        msg = e.execute_cql("select * from json_data where a='abc'").get0();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("abc"))},
            {int32_type->decompose(2)},
            {},
            {int32_type->decompose(4)}
        }});
    });
}

SEASTAR_TEST_CASE(test_json_insert_null) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
                "CREATE TABLE mytable ("
                "    myid text PRIMARY KEY,"
                "    mytext text,"
                "    mytext1 text,"
                "    mytext2 text);"
        ).get();

       e.execute_cql("INSERT INTO mytable JSON '{\"myid\" : \"id2\", \"mytext\" : \"text234\", \"mytext1\" : \"text235\", \"mytext2\" : null}';").get();

        auto msg = e.execute_cql("SELECT * FROM mytable;").get0();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("id2"))},
            {utf8_type->decompose(sstring("text234"))},
            {utf8_type->decompose(sstring("text235"))},
            {}
        }});
    });
}
