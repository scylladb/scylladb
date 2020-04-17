/*
 * Copyright (C) 2020 ScyllaDB
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

    BOOST_ASSERT(errinj.enabled_injections().empty());

    auto start_time = steady_clock::now();
    return errinj.inject("noop2", sleep_msec).then([start_time] {
        auto wait_time = std::chrono::duration_cast<milliseconds>(steady_clock::now() - start_time);
        BOOST_REQUIRE_LT(wait_time.count(), sleep_msec.count());
        return make_ready_future<>();
    });
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
