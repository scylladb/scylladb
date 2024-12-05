/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE utils

#include <boost/test/unit_test.hpp>

#include <vector>
#include <list>
#include <string>
#include <ranges>

#include "utils/ranges_concat.hh"

BOOST_AUTO_TEST_CASE(test_join_two_integer_vectors) {
    std::vector<int> v1 = {1, 2, 3};
    std::vector<int> v2 = {4, 5, 6};

    auto joined = utils::concat_view(v1, v2);

    auto result = joined | std::ranges::to<std::vector>();

    std::vector<int> expected = {1, 2, 3, 4, 5, 6};
    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_join_empty_rcanges) {
    std::vector<int> v1;
    std::vector<int> v2;

    auto joined = utils::concat_view(v1, v2);

    auto result = joined | std::ranges::to<std::vector>();

    BOOST_CHECK(result.empty());
}


BOOST_AUTO_TEST_CASE(test_join_one_empty_range) {
    std::vector<int> v1 = {1, 2, 3};
    std::vector<int> v2;

    auto joined = utils::views::concat(v1, v2);

    auto result = joined | std::ranges::to<std::vector>();

    std::vector<int> expected = {1, 2, 3};
    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_join_different_container_types) {
    std::vector<std::string> v1 = {"Hello", "World"};
    std::list<std::string> v2 = {"Boost", "Test"};

    auto joined = utils::views::concat(v1, v2);

    auto result = joined | std::ranges::to<std::vector>();

    std::vector<std::string> expected = {"Hello", "World", "Boost", "Test"};
    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_iterator_comparison) {
    std::vector<int> v1 = {1, 2, 3};
    std::vector<int> v2 = {4, 5, 6};

    auto joined = utils::views::concat(v1, v2);

    auto it1 = joined.begin();
    auto it2 = joined.begin();

    BOOST_CHECK(it1 == it2);

    ++it1;
    BOOST_CHECK(it1 != it2);
}

BOOST_AUTO_TEST_CASE(test_range_based_for_loop) {
    std::vector<int> v1 = {1, 2, 3};
    std::vector<int> v2 = {4, 5, 6};

    auto joined = utils::views::concat(v1, v2);

    int expected[] = {1, 2, 3, 4, 5, 6};
    int index = 0;

    for (int x : joined) {
        BOOST_CHECK_EQUAL(x, expected[index++]);
    }

    BOOST_CHECK_EQUAL(index, 6);
}
