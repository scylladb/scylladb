/*
 * Copyright (C) 2019-present ScyllaDB
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
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "types/list.hh"
#include "transport/messages/result_message.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "types/user.hh"
#include "utils/big_decimal.hh"
#include "db/config.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/exception_utils.hh"

using ire = exceptions::invalid_request_exception;
using exception_predicate::message_equals;
using exception_predicate::message_contains;

static shared_ptr<cql_transport::event::schema_change> get_schema_change(
        shared_ptr<cql_transport::messages::result_message> msg) {
    auto schema_change_msg = dynamic_pointer_cast<cql_transport::messages::result_message::schema_change>(msg);
    return schema_change_msg->get_change();
}

SEASTAR_TEST_CASE(test_user_function_disabled) {
    static const char* cql_using_udf =
            "CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';";
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto fut = e.execute_cql(cql_using_udf);
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_contains("User defined functions are disabled"));
    }).then([] {
        auto db_cfg_ptr = make_shared<db::config>();
        auto& db_cfg = *db_cfg_ptr;
        db_cfg.enable_user_defined_functions({true}, db::config::config_source::CommandLine);
        return do_with_cql_env_thread([] (cql_test_env& e) {}, db_cfg_ptr);
    }).then_wrapped([](future<> fut) {
        BOOST_REQUIRE_EXCEPTION(fut.get(), std::runtime_error, message_contains("You must use both enable_user_defined_functions and experimental_features:udf to enable user-defined functions"));
    });
}

template<typename Func>
static future<> with_udf_enabled(Func&& func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.enable_user_defined_functions({true}, db::config::config_source::CommandLine);
    // Raise timeout to survive debug mode and contention, but keep in
    // mind that some tests expect timeout.
    db_cfg.user_defined_function_time_limit_ms(1000);
    db_cfg.experimental_features({db::experimental_features_t::UDF}, db::config::config_source::CommandLine);
    return do_with_cql_env_thread(std::forward<Func>(func), db_cfg_ptr);
}

SEASTAR_TEST_CASE(test_user_function_out_of_memory) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'a = \"foo\" while true do a = a .. a end';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get0(), ire, message_equals("lua execution failed: not enough memory"));
    });
}

SEASTAR_TEST_CASE(test_user_function_use_null) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func1(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val + 1';").get();
        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func1(val) FROM my_table;").get0(), ire, message_equals("lua execution failed: ?:-1: attempt to perform arithmetic on a nil value"));
        auto res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{std::nullopt}});
        res = e.execute_cql("SELECT val FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{std::nullopt}});
    });
}

SEASTAR_TEST_CASE(test_user_function_wrong_return_type) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 1.2';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get0(), ire, message_equals("value is not an integer"));
    });
}

SEASTAR_TEST_CASE(test_user_function_too_many_return_values) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 1,2';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get0(), ire, message_equals("2 values returned, expected 1"));
    });
}

SEASTAR_TEST_CASE(test_user_function_reversed_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text, val int, PRIMARY KEY ((key), val)) WITH CLUSTERING ORDER BY (val DESC);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 1);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(2)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_boolean_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val boolean);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', true);").get();
        e.execute_cql("CREATE FUNCTION my_func(val boolean) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val and 1 or 0';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(1)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_time_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val time);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', '01:23:45.6789');").get();
        e.execute_cql("CREATE FUNCTION my_func(val time) CALLED ON NULL INPUT RETURNS bigint LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int64_t(((60 + 23)*60 + 45)*10000 + 6789)*100000)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_timestamp_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val timestamp);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', '2011-03-02 04:05+0000');").get();
        e.execute_cql("CREATE FUNCTION my_func(val timestamp) CALLED ON NULL INPUT RETURNS bigint LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int64_t(1299038700000))}});
    });
}

SEASTAR_TEST_CASE(test_user_function_date_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val date);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', '2019-08-26');").get();
        e.execute_cql("CREATE FUNCTION my_func(val date) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val - 2^31';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(18134)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_counter_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val counter);").get();
        e.execute_cql("UPDATE my_table SET val = val + 1 WHERE key = 'foo';").get();
        e.execute_cql("CREATE FUNCTION my_func(val counter) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val * 2';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(2)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_duration_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val duration);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 1mo2d3ns);").get();
        e.execute_cql("CREATE FUNCTION my_func(val duration) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 100 * val.months + 10 * val.days + val.nanoseconds';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(123)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_inet_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val inet);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', '127.0.0.1');").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('bar', '::1');").get();
        e.execute_cql("CREATE FUNCTION my_func(val inet) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({{serialized("127.0.0.1")}, {serialized("::1")}});
    });
}

SEASTAR_TEST_CASE(test_user_function_uuid_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val uuid);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 5375ddb6-d5a5-4cce-9aa1-b10c3fea36a3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val uuid) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized("5375ddb6-d5a5-4cce-9aa1-b10c3fea36a3")}});
    });
}

SEASTAR_TEST_CASE(test_user_function_utf8_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val text);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 'bár');").get();
        e.execute_cql("CREATE FUNCTION my_func(val text) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val:byte(2)';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(0xc3)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_blob_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val blob);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 0x123456);").get();
        e.execute_cql("CREATE FUNCTION my_func(val blob) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val:byte(2)';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(0x34)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_tuple_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val tuple<int, bigint, int>);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', (1, 2, 3));").get();
        e.execute_cql("CREATE FUNCTION my_func(val tuple<int, bigint, int>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE Lua AS 'return val[1] + val[2] + val[3]';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int64_t(6))}});
    });
}

SEASTAR_TEST_CASE(test_user_function_udt_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TYPE my_type (my_int int);").get();
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val frozen<my_type>);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', {my_int : 42});").get();
        e.execute_cql("CREATE FUNCTION my_func(val my_type) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val.my_int';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(42)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_set_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val set<int>);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', {1, 2, 3});").get();
        e.execute_cql("CREATE FUNCTION my_func(val set<int>) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'local ret = 0; for k in pairs(val) do ret = ret + k; end return ret';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(6)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_map_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val map<int, int>);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', {1 : 2, 3 : 4, 5: 6});").get();
        e.execute_cql("CREATE FUNCTION sum_keys(val map<int, int>) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'local ret = 0; for k, v in pairs(val) do ret = ret + k; end return ret';").get();
        e.execute_cql("CREATE FUNCTION sum_values(val map<int, int>) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'local ret = 0; for k, v in pairs(val) do ret = ret + v; end return ret';").get();
        auto res = e.execute_cql("SELECT sum_keys(val), sum_values(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(9), serialized(12)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_decimal_argument) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val decimal);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val decimal) CALLED ON NULL INPUT RETURNS bigint LANGUAGE Lua AS 'return 42';").get();
        e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
    });
}

SEASTAR_TEST_CASE(test_user_function_decimal_add) {
    return with_udf_enabled([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val1 decimal);").get();
        e.execute_cql("INSERT INTO my_table (key, val1) VALUES ('foo', 1.5);").get();

        e.execute_cql("CREATE FUNCTION my_func(a decimal) CALLED ON NULL INPUT RETURNS decimal LANGUAGE Lua AS 'return a + 1';").get();
        auto res = e.execute_cql("SELECT my_func(val1) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(big_decimal(1, 25))}
        });

        e.execute_cql("CREATE FUNCTION my_func2(a decimal) CALLED ON NULL INPUT RETURNS double LANGUAGE Lua AS 'return 42.2 + a';").get();
        res = e.execute_cql("SELECT my_func2(val1) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(43.7)}
        });
    });
}

SEASTAR_TEST_CASE(test_user_function_decimal_sub) {
    return with_udf_enabled([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val1 decimal, val2 decimal);").get();
        e.execute_cql("INSERT INTO my_table (key, val1, val2) VALUES ('foo', 4, 1);").get();

        e.execute_cql("CREATE FUNCTION my_func(a decimal, b decimal) CALLED ON NULL INPUT RETURNS decimal LANGUAGE Lua AS 'return a - b';").get();
        auto res = e.execute_cql("SELECT my_func(val1, val2) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(big_decimal(0, 3))}
        });
    });
}

SEASTAR_TEST_CASE(test_user_function_decimal_return) {
    return with_udf_enabled([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val1 varint, val2 decimal);").get();
        e.execute_cql("INSERT INTO my_table (key, val1, val2) VALUES ('foo', 42, 42.2);").get();

        e.execute_cql("CREATE FUNCTION my_func1(a varint) CALLED ON NULL INPUT RETURNS decimal LANGUAGE Lua AS 'return a';").get();
        auto res = e.execute_cql("SELECT my_func1(val1) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(big_decimal(0, 42))}
        });

        e.execute_cql("CREATE FUNCTION my_func2(a decimal) CALLED ON NULL INPUT RETURNS decimal LANGUAGE Lua AS 'return a';").get();
        res = e.execute_cql("SELECT my_func2(val2) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(big_decimal(1, 422))}
        });

        e.execute_cql("CREATE FUNCTION my_func3(a varint) CALLED ON NULL INPUT RETURNS decimal LANGUAGE Lua AS 'return 4.2';").get();
        auto fut = e.execute_cql("SELECT my_func3(val1) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not a decimal"));

        e.execute_cql("CREATE FUNCTION my_func4(a varint) CALLED ON NULL INPUT RETURNS decimal LANGUAGE Lua AS 'return \"18446744073709551616.1\"';").get();
        res = e.execute_cql("SELECT my_func4(val1) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(big_decimal(1, boost::multiprecision::cpp_int("184467440737095516161")))}
        });
    });
}

SEASTAR_TEST_CASE(test_user_function_varint_return) {
    return with_udf_enabled([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 42);").get();
        e.execute_cql("CREATE FUNCTION my_func(a int) CALLED ON NULL INPUT RETURNS varint LANGUAGE Lua AS 'return a';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(utils::multiprecision_int(42))}
        });
    });
}

SEASTAR_TEST_CASE(test_user_function_double_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val varint);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val varint) CALLED ON NULL INPUT RETURNS double LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(double(3))}});

        e.execute_cql("CREATE FUNCTION my_func2(val varint) CALLED ON NULL INPUT RETURNS double LANGUAGE Lua AS 'return 1/0';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(std::numeric_limits<double>::infinity())}});

        e.execute_cql("CREATE FUNCTION my_func3(val varint) CALLED ON NULL INPUT RETURNS double LANGUAGE Lua AS 'return -1/0';").get();
        res = e.execute_cql("SELECT my_func3(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(-std::numeric_limits<double>::infinity())}});

        e.execute_cql("CREATE FUNCTION my_func4(val varint) CALLED ON NULL INPUT RETURNS double LANGUAGE Lua AS 'return 0/0';").get();
        res = e.execute_cql("SELECT my_func4(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(std::nan(""))}});

        e.execute_cql("CREATE TABLE my_table2 (key text PRIMARY KEY, val decimal);").get();
        e.execute_cql("INSERT INTO my_table2 (key, val) VALUES ('foo', 5.1);").get();
        e.execute_cql("CREATE FUNCTION my_func5(val decimal) CALLED ON NULL INPUT RETURNS double LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func5(val) FROM my_table2;").get0();
        assert_that(res).is_rows().with_rows({{serialized(double(5.1))}});
    });
}

SEASTAR_TEST_CASE(test_user_sum_of_udf) {
    return with_udf_enabled([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (val int PRIMARY KEY);").get();
        e.execute_cql("INSERT INTO my_table (val) VALUES (1);").get();
        e.execute_cql("INSERT INTO my_table (val) VALUES (2);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT sum(my_func(val)) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(3))}});
    });
}

SEASTAR_TEST_CASE(test_user_function_tinyint_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val1 int, val2 int, val3 int, val4 varint);").get();
        e.execute_cql("INSERT INTO my_table (key, val1, val2, val3, val4) VALUES ('foo', 3, -1, 128, 9223372036854775808);").get();
        e.execute_cql("CREATE FUNCTION my_func(val varint) CALLED ON NULL INPUT RETURNS tinyint LANGUAGE Lua AS 'return val';").get();

        auto res = e.execute_cql("SELECT my_func(val1) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int8_t(3))}});

        res = e.execute_cql("SELECT my_func(val2) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int8_t(-1))}});

        res = e.execute_cql("SELECT my_func(val3) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int8_t(-128))}});

        res = e.execute_cql("SELECT my_func(val4) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int8_t(0))}});
    });
}

SEASTAR_TEST_CASE(test_user_function_int_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(6))}});

        e.execute_cql("CREATE OR REPLACE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(3))}});

        e.execute_cql("CREATE TABLE my_table2 (key text PRIMARY KEY, val tinyint);").get();
        e.execute_cql("INSERT INTO my_table2 (key, val) VALUES ('foo', 4);").get();
        e.execute_cql("CREATE FUNCTION my_func2(val tinyint) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table2;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(4))}});

        e.execute_cql("CREATE TABLE my_table3 (key text PRIMARY KEY, val double);").get();
        e.execute_cql("INSERT INTO my_table3 (key, val) VALUES ('foo', 4);").get();
        e.execute_cql("CREATE FUNCTION my_func3(val double) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func3(val) FROM my_table3;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(4))}});

        e.execute_cql("INSERT INTO my_table3 (key, val) VALUES ('foo', 4.2);").get();
        auto fut = e.execute_cql("SELECT my_func3(val) FROM my_table3;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not an integer"));

        e.execute_cql("CREATE TABLE my_table4 (key text PRIMARY KEY, val varint);").get();
        e.execute_cql("INSERT INTO my_table4 (key, val) VALUES ('foo', 4);").get();
        e.execute_cql("INSERT INTO my_table4 (key, val) VALUES ('bar', 2147483648);").get();
        e.execute_cql("CREATE FUNCTION my_func4(val varint) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func4(val) FROM my_table4;").get0();
        assert_that(res).is_rows().with_rows_ignore_order({
            {serialized(int32_t(4))},
            {serialized(int32_t(-2147483648))}
        });

        e.execute_cql("CREATE FUNCTION my_func5(val double) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return \"foo\"';").get();
        fut = e.execute_cql("SELECT my_func5(val) FROM my_table3;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not a number"));

        e.execute_cql("CREATE FUNCTION my_func6(val double) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return \"123\"';").get();
        res = e.execute_cql("SELECT my_func6(val) FROM my_table3;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(123))}});

        e.execute_cql("CREATE FUNCTION my_func7(val double) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return \"0x123p+1\"';").get();
        res = e.execute_cql("SELECT my_func7(val) FROM my_table3;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int32_t(0x246))}});

        e.execute_cql("CREATE FUNCTION my_func8(val double) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return false';").get();
        fut = e.execute_cql("SELECT my_func8(val) FROM my_table3;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("unexpected value"));

        e.execute_cql("CREATE FUNCTION my_func9(val double) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return \"\"';").get();
        fut = e.execute_cql("SELECT my_func9(val) FROM my_table3;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not a number"));
    });
}

SEASTAR_TEST_CASE(test_user_function_date_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(simple_date_native_type{uint32_t(3)})}});

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return \"2019-10-01\"';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(simple_date_native_type{uint32_t(2147501818)})}});

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return 4294967296';").get();
        auto fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("date value must fit in 32 bits"));

        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2019, month = 10, day = 1}';").get();
        res = e.execute_cql("SELECT my_func4(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(simple_date_native_type{uint32_t(2147501818)})}});

        e.execute_cql("CREATE FUNCTION my_func5(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2147483648, month = 10, day = 1}';").get();
        fut = e.execute_cql("SELECT my_func5(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("year is too large: '2147483648'"));

        e.execute_cql("CREATE FUNCTION my_func6(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2019, month = 256, day = 1}';").get();
        fut = e.execute_cql("SELECT my_func6(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("month is too large: '256'"));

        e.execute_cql("CREATE FUNCTION my_func7(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2019, month = 10, day = 256}';").get();
        fut = e.execute_cql("SELECT my_func7(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("day is too large: '256'"));

        e.execute_cql("CREATE FUNCTION my_func8(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2019, month = 10, day = 1, abc = 42}';").get();
        fut = e.execute_cql("SELECT my_func8(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("invalid date table field: 'abc'"));

        e.execute_cql("CREATE FUNCTION my_func9(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2019, month = 10}';").get();
        fut = e.execute_cql("SELECT my_func9(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("date table must have year, month and day"));

        e.execute_cql("CREATE FUNCTION my_func10(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2147483647, month = 10, day = 1}';").get();
        fut = e.execute_cql("SELECT my_func10(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("date value must fit in 32 bits"));

        e.execute_cql("CREATE FUNCTION my_func11(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return 42.2';").get();
        fut = e.execute_cql("SELECT my_func11(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("date must be a string, integer or date table"));

        e.execute_cql("CREATE FUNCTION my_func12(val int) CALLED ON NULL INPUT RETURNS date LANGUAGE Lua AS 'return {year = 2019, month = 10, day = 1, hour = 4}';").get();
        fut = e.execute_cql("SELECT my_func12(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("date type has no hour, minute or second"));
  });
}

SEASTAR_TEST_CASE(test_user_function_inet_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS inet LANGUAGE Lua AS 'return \"1.2.3.4\"';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(seastar::net::inet_address("1.2.3.4"))}});

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS inet LANGUAGE Lua AS 'return \"abc\"';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), marshal_exception, message_equals("marshaling error: Failed to parse inet_addr from 'abc'"));

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS inet LANGUAGE Lua AS 'return \"\"';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), marshal_exception, message_equals("marshaling error: Failed to parse inet_addr from ''"));
    });
}

SEASTAR_TEST_CASE(test_user_function_boolean_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS boolean LANGUAGE Lua AS 'return val > 4';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(false)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_ascii_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS ascii LANGUAGE Lua AS 'return \"foo\"';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(ascii_native_type{"foo"})}});

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS ascii LANGUAGE Lua AS 'return \"foó\"';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not valid ascii"));
    });
}

SEASTAR_TEST_CASE(test_user_function_utf8_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val varint);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val varint) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return \"foó\"';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized("foó")}});

        e.execute_cql("CREATE FUNCTION my_func2(val varint) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized("3")}});

        e.execute_cql("CREATE FUNCTION my_func3(val varint) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return \"\\xFF\"';").get();
        auto fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not valid utf8, invalid character at byte offset 0"));

        e.execute_cql("CREATE TABLE my_table2 (key text PRIMARY KEY, val decimal);").get();
        e.execute_cql("INSERT INTO my_table2 (key, val) VALUES ('foo', 4.2);").get();
        e.execute_cql("CREATE FUNCTION my_func4(val decimal) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return val';").get();
        res = e.execute_cql("SELECT my_func4(val) FROM my_table2;").get0();
        assert_that(res).is_rows().with_rows({{serialized("4.2")}});
    });
}

SEASTAR_TEST_CASE(test_user_function_blob_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS blob LANGUAGE Lua AS 'return \"foó\"';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(bytes("foó"))}});
    });
}

SEASTAR_TEST_CASE(test_user_function_counter_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS counter LANGUAGE Lua AS 'return 42';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int64_t(42))}});
    });
}

SEASTAR_TEST_CASE(test_user_function_time_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val varint);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 9223372036854775807);").get();
        e.execute_cql("CREATE FUNCTION my_func(val varint) CALLED ON NULL INPUT RETURNS time LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(time_native_type{int64_t(9223372036854775807)})}});

        e.execute_cql("CREATE FUNCTION my_func2(val varint) CALLED ON NULL INPUT RETURNS time LANGUAGE Lua AS 'return \"08:12:54.123\"';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(time_native_type{int64_t(29574123000000)})}});

        e.execute_cql("CREATE FUNCTION my_func3(val varint) CALLED ON NULL INPUT RETURNS time LANGUAGE Lua AS 'return \"abc\"';").get();
        auto fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), marshal_exception, message_equals("marshaling error: Timestamp format must be hh:mm:ss[.fffffffff]"));

        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 9223372036854775808);").get();
        fut = e.execute_cql("SELECT my_func(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("time value must fit in signed 64 bits"));

        e.execute_cql("CREATE FUNCTION my_func4(val varint) CALLED ON NULL INPUT RETURNS time LANGUAGE Lua AS 'return 42.2';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("time must be a string or an integer"));
    });
}

SEASTAR_TEST_CASE(test_user_function_timestamp_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val varint);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 9223372036854775807);").get();
        e.execute_cql("CREATE FUNCTION my_func(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({
            {serialized(db_clock::time_point(db_clock::duration(int64_t(9223372036854775807))))}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return \"2011-02-03 04:05:06+0000\"';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(db_clock::time_point(db_clock::duration(int64_t(0x12de9b1e550))))}});

        e.execute_cql("CREATE FUNCTION my_func5(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return {year = 2011, month = 2, day = 3, hour = 4, min = 5, sec = 6 }';").get();
        res = e.execute_cql("SELECT my_func5(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({
            {serialized(db_clock::time_point(db_clock::duration(int64_t(0x12de9b1e550))))}
        });

        // Different boost versions support different year ranges, but no version supports year 10001.
        e.execute_cql("CREATE FUNCTION my_func6(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return {year = 10001, month = 2, day = 3, hour = 4, min = 5, sec = 6 }';").get();
        auto fut = e.execute_cql("SELECT my_func6(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), boost::gregorian::bad_year, message_contains("Year is out of valid range:"));

        e.execute_cql("CREATE FUNCTION my_func3(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return \"abc\"';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        // FIXME: the exception message is redundant.
        BOOST_REQUIRE_EXCEPTION(fut.get(), marshal_exception, message_equals("marshaling error: unable to parse date 'abc': marshaling error: Unable to parse timestamp from 'abc'"));

        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('bar', 9223372036854775808);").get();
        fut = e.execute_cql("SELECT my_func(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("timestamp value must fit in signed 64 bits"));

        e.execute_cql("CREATE FUNCTION my_func4(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return 42.2';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("timestamp must be a string, integer or date table"));
    });
}

SEASTAR_TEST_CASE(test_user_function_uuid_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS uuid LANGUAGE Lua AS 'return \"982e9b0f-1df7-4425-ba04-e99d808b8940\"';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({
            {serialized(utils::UUID("982e9b0f-1df7-4425-ba04-e99d808b8940"))}
        });
    });
}

SEASTAR_TEST_CASE(test_user_function_timeuuid_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS timeuuid LANGUAGE Lua AS 'return \"d18648bc-cf83-11e9-9820-107b4493b787\"';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({
            {serialized(timeuuid_native_type{utils::UUID("d18648bc-cf83-11e9-9820-107b4493b787")})}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS timeuuid LANGUAGE Lua AS 'return \"d18648bc-cf83-21e9-9820-107b4493b787\"';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), marshal_exception, message_equals("marshaling error: Unsupported UUID version (2)"));
    });
}

SEASTAR_TEST_CASE(test_user_function_tuple_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS tuple<int, double, text> LANGUAGE Lua AS 'return {1,2.4,\"foo\"}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        auto tuple_type = tuple_type_impl::get_instance({int32_type, double_type, utf8_type});
        assert_that(res).is_rows().with_rows({
            {make_tuple_value(tuple_type, {1, 2.4, "foo"}).serialize()}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS tuple<int, double, text> LANGUAGE Lua AS 'return {1.2, 1.2, \"foo\"}';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not an integer"));

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS tuple<int, double, text> LANGUAGE Lua AS 'return {1,2.4,\"foo\", 42}';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("key 4 is not valid for a sequence of size 3"));

        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS tuple<int, double, text> LANGUAGE Lua AS 'return {[1] = 1, [3] = \"foo\"}';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("key 2 missing in sequence of size 3"));
    });
}

SEASTAR_TEST_CASE(test_user_function_list_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE Lua AS 'return {1,2,3}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        auto list_type = list_type_impl::get_instance(int32_type, false);
        assert_that(res).is_rows().with_rows({
            {make_list_value(list_type, {1, 2, 3}).serialize()}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE Lua AS 'return {1.2}';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not an integer"));

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE Lua AS 'return \"foo\"';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not a table"));

        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE Lua AS 'return {foo = 42}';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not a number"));

        e.execute_cql("CREATE FUNCTION my_func5(val int) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE Lua AS 'return {[1] = 42, [3] = 43}';").get();
        fut = e.execute_cql("SELECT my_func5(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("table is not a sequence"));
    });
}

SEASTAR_TEST_CASE(test_user_function_set_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS set<int> LANGUAGE Lua AS 'return {[1] = true, [42] = true}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        auto set_type = set_type_impl::get_instance(int32_type, false);
        assert_that(res).is_rows().with_rows({
            {make_set_value(set_type, {1, 42}).serialize()}
        });


        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS set<int> LANGUAGE Lua AS 'return {[1] = false}';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("sets are represented with tables with true values"));
    });
}

SEASTAR_TEST_CASE(test_user_function_nested_types) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS map<int, frozen<set<frozen<list<frozen<tuple<text, bigint>>>>>>> LANGUAGE Lua AS  \
                      'return {[42] = {[{{\"foo\", 41}, {\"bar\", 40}}] = true, [{{\"bar\", 40}}] = true}, [39] = {}}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        auto tuple_type = tuple_type_impl::get_instance({utf8_type, long_type});
        auto tuple_value1 = make_tuple_value(tuple_type, {"foo", int64_t(41)});
        auto tuple_value2 = make_tuple_value(tuple_type, {"bar", int64_t(40)});
        auto list_type = list_type_impl::get_instance(tuple_type, false);
        data_value list_value1 = make_list_value(list_type, {tuple_value1, tuple_value2});
        data_value list_value2 = make_list_value(list_type, {tuple_value2});
        auto set_type = set_type_impl::get_instance(list_type, false);
        data_value set_value1 = make_set_value(set_type, {list_value2, list_value1});
        data_value set_value2 = make_set_value(set_type, {});
        auto map_type = map_type_impl::get_instance(int32_type, set_type, false);
        data_value map_value = make_map_value(map_type, {{39, set_value2}, {42, set_value1}});
        assert_that(res).is_rows().with_rows({{map_value.serialize()}});
    });
}

SEASTAR_TEST_CASE(test_user_function_duration_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return {months = 1, days = 2147483647, nanoseconds = 3}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({
            {serialized(cql_duration(months_counter(1), days_counter(2147483647), nanoseconds_counter(3)))}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return {months = 1, days = 2147483648, nanoseconds = 3}';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func2(val) FROM my_table;").get(), ire, message_equals("2147483648 days doesn't fit in a 32 bit integer"));
        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return {months = 2147483648, days = 2, nanoseconds = 3}';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func3(val) FROM my_table;").get(), ire, message_equals("2147483648 months doesn't fit in a 32 bit integer"));
        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return {months = 1, days = 2, nanoseconds = \"9223372036854775808\"}';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func4(val) FROM my_table;").get(), ire, message_equals("9223372036854775808 nanoseconds doesn't fit in a 64 bit integer"));

        e.execute_cql("CREATE FUNCTION my_func5(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return \"1mo2d3ns\"';").get();
        res = e.execute_cql("SELECT my_func5(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({
            {serialized(cql_duration(months_counter(1), days_counter(2), nanoseconds_counter(3)))}
        });

        e.execute_cql("CREATE FUNCTION my_func6(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return 42.2';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func6(val) FROM my_table;").get(), ire, message_equals("a duration must be of the form { months = v1, days = v2, nanoseconds = v3 }"));

        e.execute_cql("CREATE FUNCTION my_func7(val int) CALLED ON NULL INPUT RETURNS duration LANGUAGE Lua AS 'return {foo = 42}';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func7(val) FROM my_table;").get(), ire, message_equals("invalid duration field: 'foo'"));
    });
}

SEASTAR_TEST_CASE(test_user_function_map_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS map<text, int> LANGUAGE Lua AS 'return {foo = 1, bar = 2}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        auto map_type = map_type_impl::get_instance(utf8_type, int32_type, false);
        assert_that(res).is_rows().with_rows({
            {make_map_value(map_type, {{"bar", 2}, {"foo", 1}}).serialize()}
        });
    });
}

SEASTAR_TEST_CASE(test_user_function_udt_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TYPE my_type (my_int int, my_double double);").get();
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();

        // FIXME: Maybe instead of coercing tables to UDTs we should
        // require that the user constructs a corresponding usertype
        // explicitly:
        //   v = my_type:new()
        //   v.field1 = val1
        //   v.field2 = val2
        //   return v
        // or:
        //  return my_type:new({field1 = val1, field2 = val2})

        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {my_int = 1, my_double = 2.5}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        auto user_type =
                user_type_impl::get_instance("ks", "my_type", {"my_int", "my_double"}, {int32_type, double_type}, false);
        assert_that(res).is_rows().with_rows({
            {make_user_value(user_type, {1, 2.5}).serialize()}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {my_int = 1, my_float = 2.5}';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get0(), ire, message_equals("invalid UDT field 'my_float'"));

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {[true] = 2.5}';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get0(), ire, message_equals("unexpected value"));

        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {my_int = 1}';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get0(), ire, message_equals("key my_double missing in udt my_type"));
    });
}

SEASTAR_TEST_CASE(test_user_function_called_on_null) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(2)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_return_null_on_null) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{std::nullopt}});
    });
}

SEASTAR_TEST_CASE(test_user_function_lua_error) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 42);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * bar';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get0(), ire, message_equals("lua execution failed: ?:-1: attempt to perform arithmetic on a nil value (field 'bar')"));

    });
}

SEASTAR_TEST_CASE(test_user_function_timeout) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 42);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'while true do end';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get0(), ire, message_contains("lua execution timeout: "));
    });
}

SEASTAR_TEST_CASE(test_user_function_compilation) {
    return with_udf_enabled([] (cql_test_env& e) {
        auto create = e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 @ val';");
        BOOST_REQUIRE_EXCEPTION(create.get(), ire, message_equals("could not compile: [string \"<internal>\"]:2: <eof> expected near '@'"));
    });
}

SEASTAR_TEST_CASE(test_user_function_bad_language) {
    return with_udf_enabled([] (cql_test_env& e) {
        auto create = e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Java AS 'return 2 * val';");
        BOOST_REQUIRE_EXCEPTION(create.get(), ire, message_equals("Language 'java' is not supported"));
    });
}

SEASTAR_TEST_CASE(test_user_function) {
    return with_udf_enabled([] (cql_test_env& e) {
        auto create = e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get0();
        auto change = get_schema_change(create);
        using sc = cql_transport::event::schema_change;
        BOOST_REQUIRE(change->change == sc::change_type::CREATED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);
        BOOST_REQUIRE_EQUAL(change->keyspace, "ks");
        std::vector<sstring> args{"my_func", "int"};
        BOOST_REQUIRE_EQUAL(change->arguments, args);
        auto msg = e.execute_cql("SELECT * FROM system_schema.functions;").get0();
        auto str_list = list_type_impl::get_instance(utf8_type, false);
        assert_that(msg).is_rows()
            .with_rows({
                {
                  serialized("ks"),
                  serialized("my_func"),
                  make_list_value(str_list, {"int"}).serialize(),
                  make_list_value(str_list, {"val"}).serialize(),
                  serialized("return 2 * val"),
                  serialized(false),
                  serialized("lua"),
                  serialized("int"),
                }
             });

        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 10 );").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('bar', 10 );").get();

        assert_that(e.execute_cql("SELECT my_func(val) FROM my_table;").get0()).is_rows().with_size(2);

        e.execute_cql("CREATE FUNCTION my_func2(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get0())
            .is_rows()
            .with_size(2);

        e.execute_cql("CREATE FUNCTION my_func2(val bigint) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get0())
            .is_rows()
            .with_size(3);

        e.execute_cql("CREATE FUNCTION my_func2(val double) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get0())
            .is_rows()
            .with_size(4);

        msg = e.execute_cql("DROP FUNCTION my_func2(bigint);").get0();
        change = get_schema_change(msg);
        BOOST_REQUIRE(change->change == sc::change_type::DROPPED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);
        BOOST_REQUIRE_EQUAL(change->keyspace, "ks");
        std::vector<sstring> drop_args{"my_func2", "bigint"};
        BOOST_REQUIRE_EQUAL(change->arguments, drop_args);

        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get0())
            .is_rows()
            .with_size(3);
    });
}

SEASTAR_THREAD_TEST_CASE(test_user_function_db_init) {
    tmpdir data_dir;
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    db_cfg.data_file_directories({data_dir.path().string()}, db::config::config_source::CommandLine);
    db_cfg.enable_user_defined_functions({true}, db::config::config_source::CommandLine);
    db_cfg.experimental_features({db::experimental_features_t::UDF}, db::config::config_source::CommandLine);

    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE FUNCTION my_func(a int, b float) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
    }, db_cfg_ptr).get();

    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("DROP FUNCTION my_func;").get();
    }, db_cfg_ptr).get();
}

SEASTAR_TEST_CASE(test_user_function_mixups) {
    return with_udf_enabled([] (cql_test_env& e) {
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION system.now;").get(), ire, message_equals("'system.now : () -> timeuuid' is not a user defined function"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION system.now();").get(), ire, message_equals("'system.now : () -> timeuuid' is not a user defined function"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE OR REPLACE FUNCTION system.now() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get(),
                                ire, message_equals("Cannot replace 'system.now : () -> timeuuid' which is not a user defined function"));

        e.execute_cql("CREATE FUNCTION my_func1(a int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
        e.execute_cql("CREATE FUNCTION my_func2() CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
    });
}

SEASTAR_TEST_CASE(test_user_function_errors) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE FUNCTION my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get();
        auto msg = e.execute_cql("CREATE FUNCTION IF NOT EXISTS my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get0();
        BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::void_message>(msg));

        msg = e.execute_cql("CREATE OR REPLACE FUNCTION my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get0();
        auto change = get_schema_change(msg);
        using sc = cql_transport::event::schema_change;
        BOOST_REQUIRE(change->change == sc::change_type::CREATED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE OR REPLACE FUNCTION IF NOT EXISTS my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get(),
                                exceptions::syntax_exception, message_equals("line 1:27 no viable alternative at input 'IF'"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE FUNCTION my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get(),
                                ire, message_equals("The function 'ks.my_func : (int, float) -> int' already exists"));

        msg = e.execute_cql("DROP FUNCTION IF EXISTS no_such_func(int);").get0();
        BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::void_message>(msg));

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION no_such_func(int);").get(), ire, message_equals("User function ks.no_such_func(int) doesn't exist"));

        e.execute_cql("DROP FUNCTION IF EXISTS no_such_func;").get();

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION no_such_func;").get(), ire, message_equals("No function named ks.no_such_func found"));

        e.execute_cql("CREATE FUNCTION my_func(a int, b double) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get();

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION my_func").get(), ire, message_equals("There are multiple functions named ks.my_func"));
    });
}

SEASTAR_TEST_CASE(test_user_function_invalid_type) {
    return with_udf_enabled([] (cql_test_env& e) {
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS not_a_type LANGUAGE Lua AS 'return 2 * val';").get(), ire, message_equals("Unknown type ks.not_a_type"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE FUNCTION my_func(val not_a_type) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get(), ire, message_equals("Unknown type ks.not_a_type"));

        e.execute_cql("CREATE TYPE my_type (my_int int);").get();

        auto fut = e.execute_cql("CREATE FUNCTION my_func(val frozen<my_type>) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("User defined argument and return types should not be frozen"));

        e.execute_cql("CREATE FUNCTION my_func(val my_type) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        auto msg = e.execute_cql("SELECT * FROM system_schema.functions;").get0();
        auto str_list = list_type_impl::get_instance(utf8_type, false);
        assert_that(msg).is_rows()
            .with_rows({
                {
                  serialized("ks"),
                  serialized("my_func"),
                  make_list_value(str_list, {"frozen<my_type>"}).serialize(),
                  make_list_value(str_list, {"val"}).serialize(),
                  serialized("return 2 * val"),
                  serialized(false),
                  serialized("lua"),
                  serialized("int"),
                }
             });
   });
}

SEASTAR_TEST_CASE(test_user_function_filtering) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int, t timestamp);").get();
        e.execute_cql("INSERT INTO my_table (key, val, t) VALUES ('foo', 7, toTimestamp(now()) );").get();
        e.execute_cql("INSERT INTO my_table (key, val, t) VALUES ('bar', 10, toTimestamp(now()) );").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) \
                RETURNS NULL ON NULL INPUT \
                RETURNS int \
                LANGUAGE Lua \
                AS 'return 2 * val';").get0();
        // Expect error until UDFs can be used for filtering.
        // See #5607
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT val FROM my_table WHERE my_func(val) > 10;").get(),
                                exceptions::syntax_exception, message_equals("line 1:31 no viable alternative at input 'my_func'"));

        // Reproduce #7977 and verify the internal error exception
        e.execute_cql("CREATE FUNCTION minutesAgo(ago int, now bigint) \
                RETURNS NULL ON NULL INPUT \
                RETURNS bigint \
                LANGUAGE Lua \
                AS 'return now - 60000 * ago';").get0();
        set_abort_on_internal_error(false);
        auto reset_on_internal_abort = defer([] {
            set_abort_on_internal_error(true);
        });
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT val FROM my_table WHERE key = 'foo' AND t < toTimestamp(now()) AND t >= minutesAgo(1, toTimestamp(now())) ALLOW FILTERING;").get(),
                                std::runtime_error, message_contains("User function cannot be executed in this context"));
    });
}
