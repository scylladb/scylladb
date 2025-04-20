/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "map_difference.hh"

#include <map>
#include <set>

using std::map;
using std::set;

BOOST_AUTO_TEST_CASE(both_empty) {
    map<int, int> left;
    map<int, int> right;

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left.empty());
    BOOST_REQUIRE(diff.entries_only_on_right.empty());
    BOOST_REQUIRE(diff.entries_in_common.empty());
    BOOST_REQUIRE(diff.entries_differing.empty());
}

BOOST_AUTO_TEST_CASE(left_empty) {
    map<int, int> left;
    map<int, int> right;

    right.emplace(1, 100);
    right.emplace(2, 200);

    set<int> keys;
    keys.emplace(1);
    keys.emplace(2);

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left.empty());
    BOOST_REQUIRE(diff.entries_only_on_right == keys);
    BOOST_REQUIRE(diff.entries_in_common.empty());
    BOOST_REQUIRE(diff.entries_differing.empty());
}

BOOST_AUTO_TEST_CASE(right_empty) {
    map<int, int> left;
    map<int, int> right;

    left.emplace(1, 100);
    left.emplace(2, 200);

    set<int> keys;
    keys.emplace(1);
    keys.emplace(2);

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left == keys);
    BOOST_REQUIRE(diff.entries_only_on_right.empty());
    BOOST_REQUIRE(diff.entries_in_common.empty());
    BOOST_REQUIRE(diff.entries_differing.empty());
}

BOOST_AUTO_TEST_CASE(both_same) {
    map<int, int> left;
    map<int, int> right;

    left.emplace(1, 100);
    left.emplace(2, 200);

    right.emplace(1, 100);
    right.emplace(2, 200);

    set<int> keys;
    keys.emplace(1);
    keys.emplace(2);

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left.empty());
    BOOST_REQUIRE(diff.entries_only_on_right.empty());
    BOOST_REQUIRE(diff.entries_in_common == keys);
    BOOST_REQUIRE(diff.entries_differing.empty());
}

BOOST_AUTO_TEST_CASE(differing_values) {
    map<int, int> left;
    map<int, int> right;

    left.emplace(1, 100);
    left.emplace(2, 200);

    right.emplace(1, 1000);
    right.emplace(2, 2000);

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left.empty());
    BOOST_REQUIRE(diff.entries_only_on_right.empty());
    BOOST_REQUIRE(diff.entries_in_common.empty());
    BOOST_REQUIRE(diff.entries_differing.size() == 2);
}
