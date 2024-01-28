/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <seastar/net/inet_address.hh>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"
#include "types/tuple.hh"
#include "types/user.hh"
#include "types/list.hh"
#include "utils/rjson.hh"

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

        BOOST_REQUIRE(e.local_db().has_schema("ks", "all_types"));
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

        auto msg = e.execute_cql("SELECT JSON a, b, c, d, e, f, \"G\", \"H\", \"I\", j, k, l, m, n, o, p, q, r, s, u, w, unixtimestampof(k) FROM all_types WHERE a = 'ascii'").get();
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
                    "\"j\": \"2001-10-18 14:15:55.134Z\", "
                    "\"k\": \"d2177dd0-eaa2-11de-a572-001b779c76e3\", "
                    "\"l\": \"d2177dd0-eaa2-11de-a572-001b779c76e3\", "
                    "\"m\": \"varchar\", "
                    "\"n\": 123, "
                    "\"o\": 1.23, "
                    "\"p\": 3, "
                    "\"q\": 3, "
                    "\"r\": \"1970-01-02\", "
                    "\"s\": \"00:00:00.000000001\", "
                    "\"u\": \"1y2mo25d5h6m7s8ms9us10ns\", "
                    "\"w\": null, "
                    "\"system.unixtimestampof(k)\": 1261009589805}"
                )
            }
         });

        msg = e.execute_cql("SELECT toJson(a), toJson(b), toJson(c), toJson(d), toJson(e), toJson(f),"
                "toJson(\"G\"), toJson(\"H\"), toJson(\"I\"), toJson(j), toJson(k), toJson(l), toJson(m), toJson(n),"
                "toJson(o), toJson(p), toJson(q), toJson(r), toJson(s), toJson(u), toJson(w),"
                "toJson(unixtimestampof(k)), toJson(toJson(toJson(p))) FROM all_types WHERE a = 'ascii'").get();
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
                utf8_type->decompose("\"2001-10-18 14:15:55.134Z\""),
                utf8_type->decompose("\"d2177dd0-eaa2-11de-a572-001b779c76e3\""),
                utf8_type->decompose("\"d2177dd0-eaa2-11de-a572-001b779c76e3\""),
                utf8_type->decompose("\"varchar\""),
                utf8_type->decompose("123"),
                utf8_type->decompose("1.23"),
                utf8_type->decompose("3"),
                utf8_type->decompose("3"),
                utf8_type->decompose("\"1970-01-02\""),
                utf8_type->decompose("\"00:00:00.000000001\""),
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

        BOOST_REQUIRE(e.local_db().has_schema("ks", "collections"));

        e.execute_cql(
            "INSERT INTO collections (a, b, c, d) VALUES ("
                "    'key',"
                "    { 1 : 'abc', 3 : 'de', 2 : '!' },"
                "    { 0.0, 4.5, 2.25, 1.125, NAN, INFINITY },"
                "    [[ 3 , 1, 4, 1, 5, 9 ], [ ], [ 1, 1, 2 ]]"
                ");").get();

        auto msg = e.execute_cql("SELECT JSON * FROM collections WHERE a = 'key'").get();
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

        msg = e.execute_cql("SELECT toJson(a), toJson(b), toJson(c), toJson(d) FROM collections WHERE a = 'key'").get();
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

        BOOST_REQUIRE(e.local_db().has_schema("ks", "all_types"));
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

        auto msg = e.execute_cql("SELECT * FROM all_types WHERE a = 'ascii'").get();
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
                utf8_type->decompose(sstring("varchar")), varint_type->decompose(utils::multiprecision_int(123)),
                decimal_type->decompose(big_decimal { 2, utils::multiprecision_int(123) }),
                byte_type->decompose(int8_t(3)),
                short_type->decompose(int16_t(3)),
                serialized(simple_date_native_type{0x80000001}),
                time_type->decompose(int64_t(0x0000000000000001)),
                duration_type->decompose(cql_duration("1y2mo3w4d5h6m7s8ms9us10ns"))
            }
        });

        e.execute_cql("UPDATE all_types SET b = fromJson('42') WHERE a = fromJson('\"ascii\"');").get();
        e.execute_cql("UPDATE all_types SET \"I\" = fromJson('\"zażółć gęślą jaźń\"') WHERE a = fromJson('\"ascii\"');").get();
        e.execute_cql("UPDATE all_types SET n = fromJson('\"2147483648\"') WHERE a = fromJson('\"ascii\"');").get();
        e.execute_cql("UPDATE all_types SET o = fromJson('\"3.45\"') WHERE a = fromJson('\"ascii\"');").get();

        msg = e.execute_cql("SELECT a, b, \"I\", n, o FROM all_types WHERE a = 'ascii'").get();
        assert_that(msg).is_rows().with_rows({
            {
                ascii_type->decompose(sstring("ascii")),
                long_type->decompose(42l),
                utf8_type->decompose(sstring("zażółć gęślą jaźń")),
                varint_type->decompose(utils::multiprecision_int(2147483648)),
                decimal_type->decompose(big_decimal { 2, utils::multiprecision_int(345) }),
            }
        });

        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "c": "6"
            }'
        )").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "c": "0392fa"
            }'
        )").get(), marshal_exception);

        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "\"G\"": 87654321
            }'
        )").get(), marshal_exception);

        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "k": 123456
            }'
        )").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "k": "3157"
            }'
        )").get(), marshal_exception);

        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "r": 12.34
            }'
        )").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO all_types JSON '{
                "a": "abc", "s": 1234
            }'
        )").get(), marshal_exception);

        e.execute_cql("CREATE TABLE multi_column_pk_table (p1 int, p2 int, p3 int, c1 int, c2 int, v int, PRIMARY KEY((p1, p2, p3), c1, c2));").get();
        BOOST_REQUIRE(e.local_db().has_schema("ks", "multi_column_pk_table"));

        e.execute_cql("INSERT INTO multi_column_pk_table JSON '"
                "{\"p1\": 1, "
                "\"p2\": 2, "
                "\"p3\": 3, "
                "\"c1\": 4, "
                "\"c2\": 5, "
                "\"v\": 6 "
                "}'").get();

        msg = e.execute_cql("SELECT * FROM multi_column_pk_table").get();
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

        BOOST_REQUIRE(e.local_db().has_schema("ks", "collections"));

        e.execute_cql(
            "INSERT INTO collections JSON '"
                "{\"a\": \"key\", "
                "\"b\": {\"1\": \"abc\", \"2\": \"!\", \"3\": \"de\"}, "
                "\"c\": [0, 1.125, 2.25, 4.5], "
                "\"d\": [[3, 1, 4, 1, 5, 9], [], [1, 1, 2]]}"
                "'").get();

        auto msg = e.execute_cql("SELECT JSON * FROM collections WHERE a = 'key'").get();
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
        msg = e.execute_cql("SELECT JSON * FROM collections WHERE a = 'key2'").get();
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

SEASTAR_TEST_CASE(test_insert_json_null_frozen_collections) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create type ut (a int, b int)").get();

        e.execute_cql(
            "CREATE TABLE collections ("
                "    k int primary key,"
                "    l frozen<list<int>>,"
                "    m frozen<map<int, int>>,"
                "    s frozen<set<int>>,"
                "    u frozen<ut>"
                ");").get();

        BOOST_REQUIRE(e.local_db().has_schema("ks", "collections"));

        e.execute_cql("INSERT INTO collections JSON '{\"k\": 0}'").get();
        e.execute_cql("INSERT INTO collections JSON '{\"k\": 1, \"m\": null, \"s\": null, \"l\": null, \"u\": null}'").get();

        assert_that(e.execute_cql("SELECT JSON * FROM collections WHERE k = 0").get()).is_rows().with_rows({
            {
                utf8_type->decompose(
                    "{\"k\": 0, "
                    "\"l\": null, "
                    "\"m\": null, "
                    "\"s\": null, "
                    "\"u\": null}"
                )
            }
         });

        assert_that(e.execute_cql("SELECT JSON * FROM collections WHERE k = 1").get()).is_rows().with_rows({
            {
                utf8_type->decompose(
                    "{\"k\": 1, "
                    "\"l\": null, "
                    "\"m\": null, "
                    "\"s\": null, "
                    "\"u\": null}"
                )
            }
         });
    });
}

SEASTAR_TEST_CASE(test_insert_json_integer_in_scientific_notation) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Verify that our JSON parsing supports
        // inserting numbers like 1.23e+7 to an integer
        // column (int, bigint, etc.). Numbers that contain
        // a fractional part (12.34) are disallowed. Note
        // that this behavior differs from Cassandra, which
        // disallows all those types (1.23e+7, 12.34).

        cquery_nofail(e,          
            "CREATE TABLE scientific_notation ("
                "    pk int primary key,"
                "    v bigint,"
                ");");

        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 1, "v": 150.0
            }'
        )");
        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 2, "v": 234
            }'
        )");
        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 3, "v": 1E+6
            }'
        )");

        // JSON standard specifies that numbers
        // in range [-2^53+1, 2^53-1] are interoperable
        // meaning implementations will agree 
        // exactly on their numeric values. This range
        // corresponds to a fact that double floating-point
        // type has a 53-bit significand and converting
        // from an integer in that range to double is non-lossy.
        //
        // This checks that precision is not lost 
        // for the largest possible value in that range
        // (2^53-1).
        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 4, "v": 9.007199254740991E+15
            }'
        )"); 

        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 5, "v": 0.0e3
            }'
        )");
        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 6, "v": -0.0e1
            }'
        )");
        cquery_nofail(e, R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 7, "v": -1.234E+3
            }'
        )");

        require_rows(e, "SELECT pk, v FROM scientific_notation", {
            {int32_type->decompose(1), long_type->decompose(int64_t(150))},
            {int32_type->decompose(2), long_type->decompose(int64_t(234))},
            {int32_type->decompose(3), long_type->decompose(int64_t(1000000))},
            {int32_type->decompose(4), long_type->decompose(int64_t(9007199254740991))},
            {int32_type->decompose(5), long_type->decompose(int64_t(0))},
            {int32_type->decompose(6), long_type->decompose(int64_t(0))},
            {int32_type->decompose(7), long_type->decompose(int64_t(-1234))},
        });

        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 8, "v": 12.34
            }'
        )").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 9, "v": 1e-1
            }'
        )").get(), marshal_exception);

        // JSON specification disallows Inf, -Inf, NaN:
        // "Numeric values that cannot be represented in the 
        // grammar below (such as Infinity and NaN) are not permitted."
        //
        // RapidJSON has a parsing flag: kParseNanAndInfFlag
        // which allows it. Verify it's not used:
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 10, "v": +Inf
            }'
        )").get(), rjson::error);
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 11, "v": -inf
            }'
        )").get(), rjson::error);
        BOOST_REQUIRE_THROW(e.execute_cql(R"(
            INSERT INTO scientific_notation JSON '{
                "pk": 12, "v": NaN
            }'
        )").get(), rjson::error);
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

        BOOST_REQUIRE(e.local_db().has_schema("ks", "json_data"));

        cql3::prepared_cache_key_type prepared_id = e.prepare(
            "begin batch \n"
                "  insert into json_data json :named_bound0; \n"
                "  insert into json_data json ?; \n"
                "  insert into json_data json :named_bound1; \n"
                "  insert into json_data json ?; \n"
                "apply batch;").get();

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

        auto msg = e.execute_cql("select json * from json_data where a='a1'").get();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a1\", \"b\": {\"0\": \"zero\", \"3\": \"three\", \"6\": \"six\"}, \"c\": 1.23, \"d\": [1.25, 3.75, 2.5]}")
        }});
        msg = e.execute_cql("select json * from json_data where a='a2'").get();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a2\", \"b\": {\"0\": \"zero\", \"6\": \"six\"}, \"c\": 1.23, \"d\": [3.75, 2.5]}")
        }});
        msg = e.execute_cql("select json * from json_data where a='a3'").get();
        assert_that(msg).is_rows().with_rows({{
                utf8_type->decompose(
                    "{\"a\": \"a3\", \"b\": {\"0\": \"zero\", \"3\": \"three\"}, \"c\": 1.23, \"d\": [1.25, 2.5]}")
        }});
        msg = e.execute_cql("select json * from json_data where a='a4'").get();
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

        BOOST_REQUIRE(e.local_db().has_schema("ks", "json_data"));

        e.execute_cql("INSERT INTO json_data JSON '{\"a\": \"abc\", \"b\": 2, \"c\": 1}'").get();

        auto msg = e.execute_cql("select * from json_data where a='abc'").get();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("abc"))},
            {int32_type->decompose(2)},
            {int32_type->decompose(1)},
            {}
        }});

        e.execute_cql("INSERT INTO json_data JSON '{\"a\": \"abc\", \"b\": 2, \"d\": 5}' DEFAULT UNSET").get();

        msg = e.execute_cql("select * from json_data where a='abc'").get();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("abc"))},
            {int32_type->decompose(2)},
            {int32_type->decompose(1)},
            {int32_type->decompose(5)}
        }});

        e.execute_cql("INSERT INTO json_data JSON '{\"a\": \"abc\", \"b\": 2, \"d\": 4}' DEFAULT NULL").get();

        msg = e.execute_cql("select * from json_data where a='abc'").get();
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
                "    myid text,"
                "    mytext text,"
                "    mytext1 text,"
                "    mytext2 text, PRIMARY KEY (myid, mytext));"
        ).get();

       e.execute_cql("INSERT INTO mytable JSON '{\"myid\" : \"id2\", \"mytext\" : \"text234\", \"mytext1\" : \"text235\", \"mytext2\" : null}';").get();

        auto msg = e.execute_cql("SELECT * FROM mytable;").get();
        assert_that(msg).is_rows().with_rows({{
            {utf8_type->decompose(sstring("id2"))},
            {utf8_type->decompose(sstring("text234"))},
            {utf8_type->decompose(sstring("text235"))},
            {}
        }});

        // Primary keys must be present
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO mytable JSON '{}';").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO mytable JSON '{\"mytext\" : \"text234\", \"mytext1\" : \"text235\", \"mytext2\" : null}';").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO mytable JSON '{\"myid\" : \"id2\", \"mytext1\" : \"text235\", \"mytext2\" : null}';").get(), exceptions::invalid_request_exception);
    });
}

SEASTAR_TEST_CASE(test_json_tuple) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t(id int PRIMARY KEY, v tuple<int, text, float>);").get();

        e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": [5, \"test123\", 2.5]}';").get();

        auto tt = tuple_type_impl::get_instance({int32_type, utf8_type, float_type});

        auto msg = e.execute_cql("SELECT * FROM t;").get();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(7)},
            {tt->decompose(make_tuple_value(tt, tuple_type_impl::native_type({int32_t(5), sstring("test123"), float(2.5)})))},
        }});

        e.execute_cql("INSERT INTO t (id, v) VALUES (3, (5, 'test543', 4.5));").get();

        msg = e.execute_cql("SELECT JSON * FROM t WHERE id = 3;").get();
        assert_that(msg).is_rows().with_rows({{
            utf8_type->decompose(sstring("{\"id\": 3, \"v\": [5, \"test543\", 4.5]}"))
        }});

        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": [5, \"test123\", 2.5, 3]}';").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": [\"test123\", 5, 2.5]}';").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": {\"1\": 5}}';").get(), marshal_exception);
    });
}

static future<> test_json_udt(bool frozen) {
    return do_with_cql_env_thread([frozen] (cql_test_env& e) {
        e.execute_cql("CREATE TYPE utype (first int, second text, third float);").get();
        e.execute_cql(format("CREATE TABLE t(id int PRIMARY KEY, v {});", frozen ? "frozen<utype>" : "utype")).get();

        e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": {\"first\": 5, \"third\": 2.5, \"second\": \"test123\"}}';").get();

        auto ut = user_type_impl::get_instance("ks", "utype", std::vector{bytes("first"), bytes("second"), bytes("third")}, {int32_type, utf8_type, float_type}, !frozen);

        auto msg = e.execute_cql("SELECT * FROM t;").get();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(7)},
            {ut->decompose(make_user_value(ut, user_type_impl::native_type({int32_t(5), sstring("test123"), float(2.5)})))},
        }});

        e.execute_cql("INSERT INTO t (id, v) VALUES (3, (5, 'test543', 4.5));").get();

        msg = e.execute_cql("SELECT JSON * FROM t WHERE id = 3;").get();
        assert_that(msg).is_rows().with_rows({{
            utf8_type->decompose(sstring("{\"id\": 3, \"v\": {\"first\": 5, \"second\": \"test543\", \"third\": 4.5}}"))
        }});

        e.execute_cql("INSERT INTO t (id, v) VALUES (3, {\"first\": 3, \"third\": 4.5});").get();

        msg = e.execute_cql("SELECT JSON * FROM t WHERE id = 3;").get();
        assert_that(msg).is_rows().with_rows({{
            utf8_type->decompose(sstring("{\"id\": 3, \"v\": {\"first\": 3, \"second\": null, \"third\": 4.5}}"))
        }});

        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": [5, \"test123\", 2.5, 3, 3]}';").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": {\"wrong\": 5, \"third\": 2.5, \"second\": \"test123\"}}';").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t JSON '{\"id\" : 7, \"v\": {\"first\": \"hi\", \"third\": 2.5, \"second\": \"test123\"}}';").get(), marshal_exception);

        e.execute_cql("CREATE TYPE utype2 (\"WeirdNameThatNeedsEscaping\\n,)\" int);").get();
        e.execute_cql(format("CREATE TABLE t2(id int PRIMARY KEY, v {});", frozen ? "frozen<utype2>" : "utype2")).get();

        e.execute_cql("INSERT INTO t2 (id, v) VALUES (1, (7));").get();
        msg = e.execute_cql("SELECT JSON * FROM t2 WHERE id = 1;").get();
        assert_that(msg).is_rows().with_rows({{
            utf8_type->decompose(sstring("{\"id\": 1, \"v\": {\"WeirdNameThatNeedsEscaping\\\\n,)\": 7}}"))
        }});
    });
}

SEASTAR_TEST_CASE(test_json_udt_frozen) {
    return test_json_udt(true);
}

SEASTAR_TEST_CASE(test_json_udt_nonfrozen) {
    return test_json_udt(false);
}

// Checks #4348
SEASTAR_TEST_CASE(test_unpack_decimal){
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create type ds (d1 decimal, d2 varint, d3 int)").get();
        e.execute_cql("create table t (id int PRIMARY KEY, ds list<frozen<ds>>);").get();
        e.execute_cql("update t set ds = fromJson('[{\"d1\":1,\"d2\":2,\"d3\":3}]') where id = 1").get();

        auto ut = user_type_impl::get_instance("ks", to_bytes("ds"),
                {to_bytes("d1"), to_bytes("d2"), to_bytes("d3")},
                {decimal_type, varint_type, int32_type}, false);
        auto ut_val = make_user_value(ut,
                user_type_impl::native_type({big_decimal{0, utils::multiprecision_int(1)},
                utils::multiprecision_int(2),
                3}));

        auto lt = list_type_impl::get_instance(ut, true);
        auto lt_val = lt->decompose(make_list_value(lt, list_type_impl::native_type{{ut_val}}));

        auto msg = e.execute_cql("select * from t where id = 1;").get();
        assert_that(msg).is_rows().with_rows({{int32_type->decompose(1), lt_val}});
    });
}
