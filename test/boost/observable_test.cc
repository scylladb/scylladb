/*
 * Copyright (C) 2018-present ScyllaDB
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

#define BOOST_TEST_MODULE observable_test

#include <boost/test/unit_test.hpp>
#include <variant>
#include <numeric>

#include "utils/observable.hh"
#include "utils/updateable_value.hh"

using namespace utils;

BOOST_AUTO_TEST_CASE(test_basic_functionality) {
    observable<int> pub;
    int v1 = 0, v2 = 0;
    observer<int> sub1 = pub.observe([&] (int x) { v1 = x; });
    observer<int> sub2 = pub.observe([&] (int x) { v2 = x; });
    pub(7);
    BOOST_REQUIRE_EQUAL(v1, 7);
    BOOST_REQUIRE_EQUAL(v2, 7);
    sub1.disconnect();
    pub(3);
    BOOST_REQUIRE_EQUAL(v1, 7);
    BOOST_REQUIRE_EQUAL(v2, 3);
    sub1 = std::move(sub2);
    pub(4);
    BOOST_REQUIRE_EQUAL(v1, 7);
    BOOST_REQUIRE_EQUAL(v2, 4);
    pub = observable<int>();
    pub(5);
    BOOST_REQUIRE_EQUAL(v1, 7);
    BOOST_REQUIRE_EQUAL(v2, 4);
}

BOOST_AUTO_TEST_CASE(test_exceptions) {
    observable<> pub;
    bool saw1 = false;
    observer<> sub1 = pub.observe([&] { saw1 = true; });
    observer<> sub2 = pub.observe([&] { throw 2; });
    bool saw3 = false;
    observer<> sub3 = pub.observe([&] { saw3 = true; });
    observer<> sub4 = pub.observe([&] { throw 4; });
    bool caught = false;
    try {
        pub();
    } catch (int v) {
        BOOST_REQUIRE(saw1);
        BOOST_REQUIRE(saw3);
        BOOST_REQUIRE(v == 2 || v == 4);
        caught = true;
    }
    BOOST_REQUIRE(caught);
}

BOOST_AUTO_TEST_CASE(test_disconnect_fully_disconnects) {
    std::variant<observable<>, std::array<char, 100>> pub = observable<>();
    observer<> sub = std::get<observable<>>(pub).observe([] {});
    sub.disconnect();
    auto x = std::array<char, 100>{};
    std::iota(x.begin(), x.end(), 'X');
    // Once upon a time, disconnect() still remembered the observable's address.
    // Simulate a the observable being freed and its memory reused for something
    // else by assigning garbage to the variant that holds its data
    pub = x;
    // Would have accessed the overwritten observable before the bug fix.
    sub.disconnect();
}

