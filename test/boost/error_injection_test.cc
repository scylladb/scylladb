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
#include <seastar/testing/test_case.hh>
#include "utils/error_injection.hh"
#include "log.hh"
#include <chrono>

using namespace std::literals::chrono_literals;

static logging::logger flogger("error_injection_test");


SEASTAR_TEST_CASE(test_inject_noop) {
    utils::error_injection<false> errinj;

    BOOST_REQUIRE_NO_THROW(errinj.inject("noop1",
            [] () { throw std::runtime_error("shouldn't happen"); }));

    BOOST_ASSERT(errinj.enabled_injections().empty());

    auto f = make_ready_future<>();
    auto start_time = std::chrono::steady_clock::now();
    errinj.inject("noop2", 100, f);
    return f.then([start_time] {
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        BOOST_REQUIRE_LE(wait_time.count(), 10);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_lambda) {
    utils::error_injection<true> errinj;

    errinj.enable("lambda");
    BOOST_REQUIRE_THROW(errinj.inject("lambda",
            [] () { throw std::runtime_error("test"); }),
            std::runtime_error);
    errinj.disable("lambda");
    BOOST_REQUIRE_NO_THROW(errinj.inject("lambda",
            [] () { throw std::runtime_error("test"); }));
    errinj.enable("lambda");
    BOOST_REQUIRE_THROW(errinj.inject("lambda",
            [] () { throw std::runtime_error("test"); }),
            std::runtime_error);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_future) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    auto start_time = std::chrono::steady_clock::now();
    errinj.enable("futi");
    errinj.inject("futi", 100, f);
    return f.then([start_time] {
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        BOOST_REQUIRE_GE(wait_time.count(), 100);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_future_sleep) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    auto start_time = std::chrono::steady_clock::now();
    errinj.enable("futi");
    errinj.inject("futi", 100, f);
    return f.then([start_time] {
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        BOOST_REQUIRE_GE(wait_time.count(), 100);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_future_disabled) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    auto start_time = std::chrono::steady_clock::now();
    errinj.inject("futid", 100, f);
    return f.then([start_time] {
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        BOOST_REQUIRE_LE(wait_time.count(), 10);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_exception) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    errinj.enable("exc");
    errinj.inject("exc", [] () { return std::make_exception_ptr(std::runtime_error("test")); }, f);
    return f.then_wrapped([] (auto f) {
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
