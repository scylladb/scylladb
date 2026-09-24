/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "types/list.hh"
#include "transport/messages/result_message.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "types/vector.hh"
#include "types/user.hh"
#include "utils/big_decimal.hh"
#include "db/config.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/test_utils.hh"
#include <boost/date_time/gregorian/gregorian_types.hpp>

BOOST_AUTO_TEST_SUITE(user_function_test)

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
    db_cfg.experimental_features({db::experimental_features_t::feature::UDF}, db::config::config_source::CommandLine);
    return do_with_cql_env_thread(std::forward<Func>(func), db_cfg_ptr);
}

SEASTAR_TEST_CASE(test_user_function_time_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val varint);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 9223372036854775807);").get();
        e.execute_cql("CREATE FUNCTION my_func(val varint) CALLED ON NULL INPUT RETURNS time LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        assert_that(res).is_rows().with_rows({{serialized(time_native_type{int64_t(9223372036854775807)})}});

        e.execute_cql("CREATE FUNCTION my_func2(val varint) CALLED ON NULL INPUT RETURNS time LANGUAGE Lua AS 'return \"08:12:54.123\"';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        assert_that(res).is_rows().with_rows({
            {serialized(db_clock::time_point(db_clock::duration(int64_t(9223372036854775807))))}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return \"2011-02-03 04:05:06+0000\"';").get();
        res = e.execute_cql("SELECT my_func2(val) FROM my_table;").get();
        assert_that(res).is_rows().with_rows({{serialized(db_clock::time_point(db_clock::duration(int64_t(0x12de9b1e550))))}});

        e.execute_cql("CREATE FUNCTION my_func5(val varint) CALLED ON NULL INPUT RETURNS timestamp LANGUAGE Lua AS 'return {year = 2011, month = 2, day = 3, hour = 4, min = 5, sec = 6 }';").get();
        res = e.execute_cql("SELECT my_func5(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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

SEASTAR_TEST_CASE(test_user_function_vector_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS vector<int, 3> LANGUAGE Lua AS 'return {1,2,3}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        auto vector_type = vector_type_impl::get_instance(int32_type, 3);
        assert_that(res).is_rows().with_rows({
            {make_vector_value(vector_type, {1, 2, 3}).serialize()}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS vector<int, 3> LANGUAGE Lua AS 'return {1, 2.1, 3}';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not an integer"));

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS vector<int, 3> LANGUAGE Lua AS 'return {1, 2, 3, 4}';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("key 4 is not valid for a sequence of size 3"));

        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS vector<int, 3> LANGUAGE Lua AS 'return {[1] = 1, [3] = 3}';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("key 2 missing in sequence of size 3"));
    });
}

SEASTAR_TEST_CASE(test_user_function_list_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE Lua AS 'return {1,2,3}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS map<int, frozen<set<frozen<list<frozen<tuple<text, vector<bigint, 2>>>>>>>> LANGUAGE Lua AS  \
                      'return {[42] = {[{{\"foo\", {41, 43}}, {\"bar\", {40, 44}}}] = true, [{{\"bar\", {40, 44}}}] = true}, [39] = {}}';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        auto vector_type = vector_type_impl::get_instance(long_type, 2);
        auto vector_value1 = make_vector_value(vector_type, {int64_t(41), int64_t(43)});
        auto vector_value2 = make_vector_value(vector_type, {int64_t(40), int64_t(44)});
        auto tuple_type = tuple_type_impl::get_instance({utf8_type, vector_type});
        auto tuple_value1 = make_tuple_value(tuple_type, {"foo", vector_value1});
        auto tuple_value2 = make_tuple_value(tuple_type, {"bar", vector_value2});
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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
        res = e.execute_cql("SELECT my_func5(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
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
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        auto user_type =
                user_type_impl::get_instance("ks", "my_type", {"my_int", "my_double"}, {int32_type, double_type}, false);
        assert_that(res).is_rows().with_rows({
            {make_user_value(user_type, {1, 2.5}).serialize()}
        });

        e.execute_cql("CREATE FUNCTION my_func2(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {my_int = 1, my_float = 2.5}';").get();
        auto fut = e.execute_cql("SELECT my_func2(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("invalid UDT field 'my_float'"));

        e.execute_cql("CREATE FUNCTION my_func3(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {[true] = 2.5}';").get();
        fut = e.execute_cql("SELECT my_func3(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("unexpected value"));

        e.execute_cql("CREATE FUNCTION my_func4(val int) CALLED ON NULL INPUT RETURNS my_type LANGUAGE Lua AS 'return {my_int = 1}';").get();
        fut = e.execute_cql("SELECT my_func4(val) FROM my_table;");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("key my_double missing in udt my_type"));
    });
}

SEASTAR_TEST_CASE(test_user_function_called_on_null) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        assert_that(res).is_rows().with_rows({{serialized(2)}});
    });
}

SEASTAR_TEST_CASE(test_user_function_return_null_on_null) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', null);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get();
        assert_that(res).is_rows().with_rows({{std::nullopt}});
    });
}

SEASTAR_TEST_CASE(test_user_function_lua_error) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 42);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * bar';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get(), ire, message_contains("attempt to perform arithmetic on a nil value (field 'bar')"));

    });
}

SEASTAR_TEST_CASE(test_user_function_timeout) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 42);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'while true do end';").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT my_func(val) FROM my_table;").get(), ire, message_contains("lua execution timeout: "));
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
        auto create = e.execute_cql("CREATE FUNCTION my_func(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        auto change = get_schema_change(create);
        using sc = cql_transport::event::schema_change;
        BOOST_REQUIRE(change->change == sc::change_type::CREATED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);
        BOOST_REQUIRE_EQUAL(change->keyspace, "ks");
        std::vector<sstring> args{"my_func", "int"};
        BOOST_REQUIRE_EQUAL(change->arguments, args);
        auto msg = e.execute_cql("SELECT * FROM system_schema.functions;").get();
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

        assert_that(e.execute_cql("SELECT my_func(val) FROM my_table;").get()).is_rows().with_size(2);

        e.execute_cql("CREATE FUNCTION my_func2(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get())
            .is_rows()
            .with_size(2);

        e.execute_cql("CREATE FUNCTION my_func2(val bigint) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get())
            .is_rows()
            .with_size(3);

        e.execute_cql("CREATE FUNCTION my_func2(val double) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get())
            .is_rows()
            .with_size(4);

        msg = e.execute_cql("DROP FUNCTION my_func2(bigint);").get();
        change = get_schema_change(msg);
        BOOST_REQUIRE(change->change == sc::change_type::DROPPED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);
        BOOST_REQUIRE_EQUAL(change->keyspace, "ks");
        std::vector<sstring> drop_args{"my_func2", "bigint"};
        BOOST_REQUIRE_EQUAL(change->arguments, drop_args);

        assert_that(e.execute_cql("SELECT * FROM system_schema.functions;").get())
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
    db_cfg.experimental_features({db::experimental_features_t::feature::UDF}, db::config::config_source::CommandLine);

    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE FUNCTION my_func(a int, b float) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
    }, db_cfg_ptr).get();

    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("DROP FUNCTION my_func;").get();
    }, db_cfg_ptr).get();
}

SEASTAR_TEST_CASE(test_user_function_mixups) {
    return with_udf_enabled([] (cql_test_env& e) {
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION system.now;").get(), exceptions::unauthorized_exception,
                message_contains("system keyspace is not user-modifiable"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("DROP FUNCTION system.now();").get(), exceptions::unauthorized_exception,
                message_contains("system keyspace is not user-modifiable"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE OR REPLACE FUNCTION system.now() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get(),
                exceptions::unauthorized_exception,
                message_contains("system keyspace is not user-modifiable"));

        e.execute_cql("CREATE FUNCTION my_func1(a int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
        e.execute_cql("CREATE FUNCTION my_func2() CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';").get();
    });
}

SEASTAR_TEST_CASE(test_user_function_errors) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE FUNCTION my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get();
        auto msg = e.execute_cql("CREATE FUNCTION IF NOT EXISTS my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get();
        BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::void_message>(msg));

        msg = e.execute_cql("CREATE OR REPLACE FUNCTION my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get();
        auto change = get_schema_change(msg);
        using sc = cql_transport::event::schema_change;
        BOOST_REQUIRE(change->change == sc::change_type::CREATED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE OR REPLACE FUNCTION IF NOT EXISTS my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get(),
                                exceptions::syntax_exception, message_equals("line 1:27 no viable alternative at input 'IF'"));
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE FUNCTION my_func(a int, b float) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * a';").get(),
                                ire, message_equals("The function 'ks.my_func : (int, float) -> int' already exists"));

        msg = e.execute_cql("DROP FUNCTION IF EXISTS no_such_func(int);").get();
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
        auto msg = e.execute_cql("SELECT * FROM system_schema.functions;").get();
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
                AS 'return 2 * val';").get();
        // Expect error until UDFs can be used for filtering.
        // See #5607
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT val FROM my_table WHERE my_func(val) > 10;").get(),
                                ire, message_contains("Only the token function and scoring functions are supported in function-call restrictions"));

        // Reproduce #7977 and verify the internal error exception
        e.execute_cql("CREATE FUNCTION minutesAgo(ago int, now bigint) \
                RETURNS NULL ON NULL INPUT \
                RETURNS bigint \
                LANGUAGE Lua \
                AS 'return now - 60000 * ago';").get();
        set_abort_on_internal_error(false);
        auto reset_on_internal_abort = defer([] noexcept {
            set_abort_on_internal_error(true);
        });
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("SELECT val FROM my_table WHERE key = 'foo' AND t <= toTimestamp(now()) AND t >= minutesAgo(1, toTimestamp(now())) ALLOW FILTERING;").get(),
                                std::runtime_error, message_contains("User function cannot be executed in this context"));
    });
}

BOOST_AUTO_TEST_SUITE_END()
