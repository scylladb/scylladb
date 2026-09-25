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
#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message.hh"
#include "db/config.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/test_utils.hh"

BOOST_AUTO_TEST_SUITE(user_function_test)

using ire = exceptions::invalid_request_exception;
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

// The rest of this test was ported to test/cqlpy/test_lua_udf.py. The schema
// change events checked here aren't visible through the Python driver.
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

        e.execute_cql("CREATE FUNCTION my_func2(val bigint) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 2 * val';").get();
        auto msg = e.execute_cql("DROP FUNCTION my_func2(bigint);").get();
        change = get_schema_change(msg);
        BOOST_REQUIRE(change->change == sc::change_type::DROPPED);
        BOOST_REQUIRE(change->target == sc::target_type::FUNCTION);
        BOOST_REQUIRE_EQUAL(change->keyspace, "ks");
        std::vector<sstring> drop_args{"my_func2", "bigint"};
        BOOST_REQUIRE_EQUAL(change->arguments, drop_args);
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

// The rest of this test was ported to test/cqlpy/test_lua_udf.py. The result
// message types checked here aren't visible through the Python driver.
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

        msg = e.execute_cql("DROP FUNCTION IF EXISTS no_such_func(int);").get();
        BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::void_message>(msg));
    });
}

// The rest of this test was ported to test/cqlpy/test_lua_udf.py. This part
// triggers an internal error, which would abort a server run by cqlpy.
SEASTAR_TEST_CASE(test_user_function_filtering) {
    return with_udf_enabled([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE my_table (key text PRIMARY KEY, val int, t timestamp);").get();
        e.execute_cql("INSERT INTO my_table (key, val, t) VALUES ('foo', 7, toTimestamp(now()) );").get();
        e.execute_cql("INSERT INTO my_table (key, val, t) VALUES ('bar', 10, toTimestamp(now()) );").get();

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
