/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "map_difference.hh"

#include <map>

using namespace std;

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

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left.empty());
    BOOST_REQUIRE(diff.entries_only_on_right == right);
    BOOST_REQUIRE(diff.entries_in_common.empty());
    BOOST_REQUIRE(diff.entries_differing.empty());
}

BOOST_AUTO_TEST_CASE(right_empty) {
    map<int, int> left;
    map<int, int> right;

    left.emplace(1, 100);
    left.emplace(2, 200);

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left == left);
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

    auto diff = difference(left, right, [](int x, int y) -> bool {
        return x == y;
    });

    BOOST_REQUIRE(diff.entries_only_on_left.empty());
    BOOST_REQUIRE(diff.entries_only_on_right.empty());
    BOOST_REQUIRE(diff.entries_in_common == left);
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
