/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <source_location>
#include "utils/source_location-compat.hh"
#include <string>
#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

using namespace seastar;

// Thread safe alternatives to BOOST_REQUIRE_*, BOOST_CHECK_* and BOOST_FAIL().
// Use these if instead of the BOOST provided macros if you want to use them on
// multiple shards, to avoid problems due to the BOOST versions not being thread
// safe.

namespace tests {

[[nodiscard]] bool do_check(bool condition, std::source_location sl, std::string_view msg);

[[nodiscard]] inline bool check(bool condition, std::source_location sl = std::source_location::current()) {
    return do_check(condition, sl, {});
}

template <typename LHS, typename RHS>
[[nodiscard]] bool check_equal(const LHS& lhs, const RHS& rhs, std::source_location sl = std::source_location::current()) {
    const auto condition = (lhs == rhs);
    return do_check(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void do_require(bool condition, std::source_location sl, std::string_view msg);

inline void require(bool condition, std::source_location sl = std::source_location::current()) {
    do_require(condition, sl, {});
}

template <typename LHS, typename RHS>
void require_equal(const LHS& lhs, const RHS& rhs, std::source_location sl = std::source_location::current()) {
    const auto condition = (lhs == rhs);
    do_require(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void fail(std::string_view msg, std::source_location sl = std::source_location::current());

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

}
