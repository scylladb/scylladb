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

#pragma once

#include <experimental/source_location>
#include <string>

#include <fmt/format.h>

// Thread safe alternatives to BOOST_REQUIRE_*, BOOST_CHECK_* and BOOST_FAIL().
// Use these if instead of the BOOST provided macros if you want to use them on
// multiple shards, to avoid problems due to the BOOST versions not being thread
// safe.

namespace tests {

void do_check(bool condition, std::experimental::source_location sl, std::string_view msg);

inline void check(bool condition, std::experimental::source_location sl = std::experimental::source_location::current()) {
    do_check(condition, sl, {});
}

template <typename LHS, typename RHS>
void check_equal(const LHS& lhs, const RHS& rhs, std::experimental::source_location sl = std::experimental::source_location::current()) {
    const auto condition = (lhs == rhs);
    do_check(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void do_require(bool condition, std::experimental::source_location sl, std::string_view msg);

inline void require(bool condition, std::experimental::source_location sl = std::experimental::source_location::current()) {
    do_require(condition, sl, {});
}

template <typename LHS, typename RHS>
void require_equal(const LHS& lhs, const RHS& rhs, std::experimental::source_location sl = std::experimental::source_location::current()) {
    const auto condition = (lhs == rhs);
    do_require(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void fail(std::string_view msg, std::experimental::source_location sl = std::experimental::source_location::current());

}
