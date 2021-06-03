/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "test/lib/cql_test_env.hh"
#include <seastar/core/manual_clock.hh>
#include <seastar/testing/test_case.hh>
#include "utils/error_injection.hh"
#include "db/timeout_clock.hh"
#include "test/lib/cql_assertions.hh"
#include "types/list.hh"
#include "log.hh"
#include <chrono>

using namespace std::literals::chrono_literals;

static logging::logger flogger("error_injection_test");

using milliseconds = std::chrono::milliseconds;
using minutes = std::chrono::minutes;
using steady_clock = std::chrono::steady_clock;

constexpr milliseconds sleep_msec(10); // Injection time sleep 10 msec
constexpr minutes future_mins(10);     // Far in future        10 mins

SEASTAR_TEST_CASE(test_inject_noop) {
    utils::error_injection<false> errinj;

    BOOST_REQUIRE_NO_THROW(errinj.inject("noop1",
            [] () { throw std::runtime_error("shouldn't happen"); }));

    errinj.enable("error");
    BOOST_ASSERT(errinj.enabled_injections().empty());
    BOOST_ASSERT(errinj.enter("error") == false);

    auto start_time = steady_clock::now();
    return errinj.inject("noop2", sleep_msec).then([start_time] {
        auto wait_time = std::chrono::duration_cast<milliseconds>(steady_clock::now() - start_time);
        BOOST_REQUIRE_LT(wait_time.count(), sleep_msec.count());
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_is_enabled) {
    utils::error_injection<true> errinj;

    // Test enable and disable
    errinj.enable("is_enabled_test", false);
    errinj.disable("is_enabled_test");
    BOOST_ASSERT(errinj.enabled_injections().size() == 0);

    // Test enable with one_shot=true and enter
    errinj.enable("is_enabled_test", true);
    BOOST_ASSERT(errinj.enabled_injections().size() == 1);
    BOOST_ASSERT(errinj.enter("is_enabled_test"));
    BOOST_ASSERT(errinj.enabled_injections().size() == 0);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_lambda) {
    utils::error_injection<true> errinj;

    errinj.enable("lambda");
    BOOST_REQUIRE_THROW(errinj.inject("lambda",
            [] () -> void { throw std::runtime_error("test"); }),
            std::runtime_error);
    errinj.disable("lambda");
    BOOST_REQUIRE_NO_THROW(errinj.inject("lambda",
            [] () -> void { throw std::runtime_error("test"); }));
    errinj.enable("lambda");
    BOOST_REQUIRE_THROW(errinj.inject("lambda",
            [] () -> void { throw std::runtime_error("test"); }),
            std::runtime_error);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_sleep_duration) {
    utils::error_injection<true> errinj;

    auto start_time = steady_clock::now();
    errinj.enable("future_sleep");
    return errinj.inject("future_sleep", sleep_msec).then([start_time] {
        auto wait_time = std::chrono::duration_cast<milliseconds>(steady_clock::now() - start_time);
        BOOST_REQUIRE_GE(wait_time.count(), sleep_msec.count());
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_steady_clock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = steady_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        errinj.inject("future_deadline", deadline).then([deadline] {
            BOOST_REQUIRE_GE(std::chrono::duration_cast<std::chrono::milliseconds>(steady_clock::now() - deadline).count(), 0);
            return make_ready_future<>();
        }).get();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_manual_clock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = seastar::manual_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        auto f = errinj.inject("future_deadline", deadline).then([deadline] {
            BOOST_REQUIRE_GE(std::chrono::duration_cast<std::chrono::milliseconds>(seastar::manual_clock::now() - deadline).count(), 0);
            return make_ready_future<>();
        });
        manual_clock::advance(sleep_msec);
        f.get();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_manual_clock_lambda) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = seastar::manual_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        auto f = errinj.inject("future_deadline", deadline, [deadline] {
            BOOST_REQUIRE_GE(std::chrono::duration_cast<std::chrono::milliseconds>(seastar::manual_clock::now() - deadline).count(), 0);
            return make_ready_future<>();
        });
        manual_clock::advance(sleep_msec);
        f.get();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_db_clock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = db::timeout_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        errinj.inject("future_deadline", deadline).then([deadline] {
            BOOST_REQUIRE_GE(std::chrono::duration_cast<std::chrono::milliseconds>(db::timeout_clock::now() - deadline).count(), 0);
            return make_ready_future<>();
        }).get();
    });
}

SEASTAR_TEST_CASE(test_inject_future_disabled) {
    utils::error_injection<true> errinj;

    auto start_time = steady_clock::now();
    return errinj.inject("futid", sleep_msec).then([start_time] {
        auto wait_time = std::chrono::duration_cast<milliseconds>(steady_clock::now() - start_time);
        BOOST_REQUIRE_LT(wait_time.count(), sleep_msec.count());
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_error_exceptions) {

    auto exc = std::make_exception_ptr(utils::injected_error("test"));
    BOOST_TEST(!is_timeout_exception(exc));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_exception) {
    utils::error_injection<true> errinj;

    errinj.enable("exc");
    return errinj.inject("exc", [] () -> std::exception_ptr {
        return std::make_exception_ptr(std::runtime_error("test"));
    }).then_wrapped([] (auto f) {
        BOOST_REQUIRE_THROW(f.get(), std::runtime_error);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_two) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    errinj.enable("one");
    errinj.enable("two");

    std::vector<sstring> expected = { "one", "two" };
    auto enabled_injections = errinj.enabled_injections();
    std::sort(enabled_injections.begin(), enabled_injections.end());
    BOOST_TEST(enabled_injections == expected);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_disable_all) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    errinj.enable("one");
    errinj.enable("two");
    errinj.disable_all();
    auto enabled_injections = errinj.enabled_injections();
    BOOST_TEST(enabled_injections == std::vector<sstring>());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_once) {
    utils::error_injection<true> errinj;

    errinj.enable("first", true);

    std::vector<sstring> expected1 = { "first" };
    auto enabled_injections1 = errinj.enabled_injections();
    BOOST_TEST(enabled_injections1 == expected1);
    BOOST_REQUIRE_THROW(errinj.inject("first", [] { throw std::runtime_error("test"); }),
        std::runtime_error);

    std::vector<sstring> expected_empty;
    auto enabled_injections2 = errinj.enabled_injections();
    BOOST_TEST(enabled_injections2 == expected_empty);
    BOOST_REQUIRE_NO_THROW(errinj.inject("first", [] { throw std::runtime_error("test"); }));

    return make_ready_future<>();
}

// Test error injection CQL API
// NOTE: currently since functions can't get terminals an auxiliary table
//       with error injection names and one shot parameters
SEASTAR_TEST_CASE(test_inject_cql) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&e] {
            // Type of returned list of error injections cql3/functions/error_injcetion_fcts.cc
            const auto my_list_type = list_type_impl::get_instance(ascii_type, false);
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
            auto row_empty = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{}}));
            auto row_test1 = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{"test1"}}));
            auto row_test2 = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{"test2"}}));
#else
            auto row_empty = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{}}));
            auto row_test1 = row_empty;
            auto row_test2 = row_empty;
#endif

            // Auxiliary table with terminals
            cquery_nofail(e, "create table error_name (name ascii primary key, one_shot ascii)");

            // Enable (test1,one_shot=true)
            cquery_nofail(e, "insert into  error_name (name, one_shot) values ('test1', 'true')");

            // Check no error injections before injecting
            auto ret0 = e.execute_cql("select enabled_injections() from error_name limit 1").get0();
            assert_that(ret0).is_rows().with_rows({
                {row_empty}
            });

            cquery_nofail(e, "select enable_injection(name, one_shot)  from error_name where name = 'test1'");
            // enabled_injections() returns a list all injections in one call, so limit 1
            auto ret1 = e.execute_cql("select enabled_injections() from error_name limit 1").get0();
            assert_that(ret1).is_rows().with_rows({
                {row_test1}
            });
            utils::get_local_injector().inject("test1", [] {}); // Noop one-shot injection
            auto ret2 = e.execute_cql("select enabled_injections() from error_name limit 1").get0();
            assert_that(ret2).is_rows().with_rows({
                // Empty list after one shot executed
                {row_empty}
            });

            // Again (test1,one_shot=true) but disable with CQL API
            cquery_nofail(e, "select enable_injection(name, one_shot)  from error_name where name = 'test1'");
            // enabled_injections() returns a list all injections in one call, so limit 1
            auto ret3 = e.execute_cql("select enabled_injections() from error_name limit 1").get0();
            assert_that(ret3).is_rows().with_rows({
                {row_test1}
            });
            // Disable
            cquery_nofail(e, "select disable_injection(name) from error_name where name = 'test1'");
            auto ret4 = e.execute_cql("select enabled_injections() from error_name limit 1").get0();
            assert_that(ret4).is_rows().with_rows({
                // Empty list after one shot disabled
                {row_empty}
            });

            cquery_nofail(e, "insert into  error_name (name, one_shot) values ('test2', 'false')");
            cquery_nofail(e, "select enable_injection(name, one_shot)  from error_name where name = 'test2'");
            utils::get_local_injector().inject("test2", [] {}); // Noop injection, doesn't disable
            auto ret5 = e.execute_cql("select enabled_injections() from error_name limit 1").get0();
            assert_that(ret5).is_rows().with_rows({
                {row_test2}
            });
        });
    });
}
