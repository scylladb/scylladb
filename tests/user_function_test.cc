/*
 * Copyright (C) 2019 ScyllaDB
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
#include "tests/cql_assertions.hh"
#include "tests/cql_test_env.hh"
#include "types/list.hh"
#include "transport/messages/result_message.hh"
#include "db/config.hh"
#include "tmpdir.hh"
#include "exception_utils.hh"

using ire = exceptions::invalid_request_exception;
using exception_predicate::message_equals;
using exception_predicate::message_contains;

static shared_ptr<cql_transport::event::schema_change> get_schema_change(
        shared_ptr<cql_transport::messages::result_message> msg) {
    auto schema_change_msg = dynamic_pointer_cast<cql_transport::messages::result_message::schema_change>(msg);
    return schema_change_msg->get_change();
}

template<typename T>
static bytes serialized(T v) {
    return data_value(v).serialize();
}

SEASTAR_TEST_CASE(test_user_function_disabled) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto fut = e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2';");
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("User defined functions are disabled. Set enable_user_defined_functions to enable them"));
    });
}

template<typename Func>
static future<> with_udf_enabled(Func&& func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.enable_user_defined_functions({true}, db::config::config_source::CommandLine);
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
    });
}

SEASTAR_TEST_CASE(test_user_function_tinyint_return) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int);").get();
        e.execute_cql("INSERT INTO my_table (key, val) VALUES ('foo', 3);").get();
        e.execute_cql("CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS tinyint LANGUAGE Lua AS 'return val';").get();
        auto res = e.execute_cql("SELECT my_func(val) FROM my_table;").get0();
        assert_that(res).is_rows().with_rows({{serialized(int8_t(3))}});
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
        BOOST_REQUIRE_EXCEPTION(fut.get(), ire, message_equals("value is not valid utf8"));
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
