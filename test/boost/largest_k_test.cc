/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include "utils/largest_k.hh"

#include <functional>
#include <memory>
#include <ranges>
#include <string>
#include <vector>

using utils::largest_k;

BOOST_AUTO_TEST_SUITE(largest_k_test)

BOOST_AUTO_TEST_CASE(keeps_the_largest_elements_in_ascending_order) {
    largest_k<int> top(3);
    BOOST_REQUIRE(top.push(3));
    BOOST_REQUIRE(top.push(5));
    BOOST_REQUIRE(top.push(1));
    // 4 replaces 1, the smallest element kept.
    BOOST_REQUIRE(top.push(4));
    // 2 is smaller than all the elements kept.
    BOOST_REQUIRE(!top.push(2));
    BOOST_REQUIRE_EQUAL(top.size(), 3u);
    BOOST_REQUIRE(std::move(top).take_sorted() == (std::vector<int>{3, 4, 5}));
}

BOOST_AUTO_TEST_CASE(keeps_all_elements_below_capacity) {
    largest_k<int> top(5);
    top.push(2);
    top.push(1);
    BOOST_REQUIRE_EQUAL(top.size(), 2u);
    BOOST_REQUIRE_EQUAL(top.capacity(), 5u);
    BOOST_REQUIRE(std::move(top).take_sorted() == (std::vector<int>{1, 2}));
}

BOOST_AUTO_TEST_CASE(does_not_add_an_element_equal_to_the_smallest) {
    struct item {
        int key;
        std::string name;
    };
    auto names = [] (const std::vector<item>& items) {
        return items | std::views::transform(&item::name) | std::ranges::to<std::vector>();
    };

    largest_k<item, std::less<>, int item::*> top(2, {}, &item::key);
    top.push({1, "a"});
    top.push({2, "b"});
    BOOST_REQUIRE(!top.push({1, "c"}));
    BOOST_REQUIRE(names(std::move(top).take_sorted()) == (std::vector<std::string>{"a", "b"}));
}

BOOST_AUTO_TEST_CASE(keeps_nothing_at_zero_capacity) {
    largest_k<int> top(0);
    BOOST_REQUIRE(!top.push(1));
    BOOST_REQUIRE(std::move(top).take_sorted().empty());
}

BOOST_AUTO_TEST_CASE(orders_by_the_comparator) {
    // With std::greater, the largest elements are the smallest numbers.
    largest_k<int, std::greater<>> top(2);
    top.push(5);
    top.push(1);
    top.push(4);
    top.push(2);
    BOOST_REQUIRE(std::move(top).take_sorted() == (std::vector<int>{2, 1}));
}

BOOST_AUTO_TEST_CASE(accepts_a_mutable_comparator) {
    // The call operator is not const, as std::indirect_strict_weak_order allows.
    struct mutable_less {
        bool operator()(int lhs, int rhs) { return lhs < rhs; }
    };
    largest_k<int, mutable_less> top(2);
    top.push(3);
    top.push(1);
    top.push(2);
    BOOST_REQUIRE(std::move(top).take_sorted() == (std::vector<int>{2, 3}));
}

BOOST_AUTO_TEST_CASE(keeps_move_only_elements) {
    auto value_of = [] (const std::unique_ptr<int>& p) { return *p; };
    largest_k<std::unique_ptr<int>, std::less<>, decltype(value_of)> top(2, {}, value_of);
    top.push(std::make_unique<int>(3));
    top.push(std::make_unique<int>(1));
    top.push(std::make_unique<int>(2));
    auto kept = std::move(top).take_sorted();
    BOOST_REQUIRE_EQUAL(kept.size(), 2u);
    BOOST_REQUIRE_EQUAL(*kept[0], 2);
    BOOST_REQUIRE_EQUAL(*kept[1], 3);
}

BOOST_AUTO_TEST_SUITE_END()
