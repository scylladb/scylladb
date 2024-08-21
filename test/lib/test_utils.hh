/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/util/source_location-compat.hh>
#include <string>
#include <boost/test/unit_test.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

using namespace seastar;

// Thread safe alternatives to BOOST_REQUIRE_*, BOOST_CHECK_* and BOOST_FAIL().
// Use these if instead of the BOOST provided macros if you want to use them on
// multiple shards, to avoid problems due to the BOOST versions not being thread
// safe.

namespace tests {

[[nodiscard]] bool do_check(bool condition, seastar::compat::source_location sl, std::string_view msg);

[[nodiscard]] inline bool check(bool condition, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    return do_check(condition, sl, {});
}

template <typename LHS, typename RHS>
[[nodiscard]] bool check_equal(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    const auto condition = (lhs == rhs);
    return do_check(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void do_require(bool condition, seastar::compat::source_location sl, std::string_view msg);

inline void require(bool condition, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require(condition, sl, {});
}

template <typename LHS, typename RHS, typename Compare>
void do_require_relation(
        const LHS& lhs,
        const RHS& rhs,
        const Compare& relation,
        const char* relation_operator,
        seastar::compat::source_location sl) {
    const auto condition = relation(lhs, rhs);
    do_require(condition, sl, fmt::format("assertion {} {} {} failed", lhs, relation_operator, rhs));
}

template <typename LHS, typename RHS>
void require_less(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require_relation(lhs, rhs, std::less<LHS>{}, "<", sl);
}

template <typename LHS, typename RHS>
void require_less_equal(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require_relation(lhs, rhs, std::less_equal<LHS>{}, "<=", sl);
}

template <typename LHS, typename RHS>
void require_equal(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require_relation(lhs, rhs, std::equal_to<LHS>{}, "==", sl);
}

template <typename LHS, typename RHS>
void require_not_equal(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require_relation(lhs, rhs, std::not_equal_to<LHS>{}, "!=", sl);
}

template <typename LHS, typename RHS>
void require_greater_equal(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require_relation(lhs, rhs, std::greater_equal<LHS>{}, ">=", sl);
}

template <typename LHS, typename RHS>
void require_greater(const LHS& lhs, const RHS& rhs, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    do_require_relation(lhs, rhs, std::greater<LHS>{}, ">", sl);
}

void fail(std::string_view msg, seastar::compat::source_location sl = seastar::compat::source_location::current());

inline std::string getenv_safe(std::string_view name) {
    auto v = ::getenv(name.data());
    if (!v) {
        throw std::logic_error(fmt::format("Environment variable {} not set", name));
    }
    return std::string(v);
}

extern boost::test_tools::assertion_result has_scylla_test_env(boost::unit_test::test_unit_id);
future<bool> compare_files(std::string fa, std::string fb);
future<> touch_file(std::string name);

extern std::mutex boost_logger_mutex;

#define THREADSAFE_BOOST_CHECK( BOOST_CHECK_EXPR ) {                    \
        std::lock_guard<std::mutex> guard(tests::boost_logger_mutex);   \
        BOOST_CHECK_EXPR;                                               \
    }

// Thread-safe variants of BOOST macros for unit tests
// This is required to address lack of synchronization in boost test logger, which is susceptible to races
// in multi-threaded tests. Boost's XML printer (see boost/test/utils/xml_printer.hpp) can generate
// unreadable XML files, and therefore cause failure when parsing its content.
#define THREADSAFE_BOOST_REQUIRE( P ) THREADSAFE_BOOST_CHECK(BOOST_REQUIRE( P ))
#define THREADSAFE_BOOST_REQUIRE_EQUAL( L, R ) THREADSAFE_BOOST_CHECK(BOOST_REQUIRE_EQUAL( L, R ))

}

namespace internal {

template<typename Lhs, typename Rhs>
concept has_left_shift = requires(Lhs& lhs, const Rhs& rhs) {
    { lhs << rhs } -> std::same_as<Lhs&>;
};

}

namespace std {

template <typename T>
requires (fmt::is_formattable<T>::value &&
          !::internal::has_left_shift<std::ostream, T>)
std::ostream& boost_test_print_type(std::ostream& os, const T& p) {
    fmt::print(os, "{}", p);
    return os;
}

}
