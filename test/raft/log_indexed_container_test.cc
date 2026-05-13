/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "raft/log_indexed_container.hh"
#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>

using namespace raft;

BOOST_AUTO_TEST_CASE(test_empty_container) {
    log_indexed_container<int> c;
    BOOST_CHECK(c.empty());
    BOOST_CHECK(c.find(index_t{0}) == nullptr);
    BOOST_CHECK(c.find(index_t{100}) == nullptr);
}

BOOST_AUTO_TEST_CASE(test_emplace_and_find) {
    log_indexed_container<int> c;
    c.emplace(index_t{5}, 42);
    BOOST_CHECK(!c.empty());
    BOOST_CHECK_EQUAL(c.base_index(), index_t{5});

    auto* p = c.find(index_t{5});
    BOOST_REQUIRE(p != nullptr);
    BOOST_CHECK_EQUAL(*p, 42);

    BOOST_CHECK(c.find(index_t{4}) == nullptr);
    BOOST_CHECK(c.find(index_t{6}) == nullptr);
}

BOOST_AUTO_TEST_CASE(test_emplace_with_gap) {
    log_indexed_container<int> c;
    c.emplace(index_t{10}, 1);
    c.emplace(index_t{13}, 2);

    BOOST_CHECK_EQUAL(*c.find(index_t{10}), 1);
    BOOST_CHECK(c.find(index_t{11}) == nullptr);
    BOOST_CHECK(c.find(index_t{12}) == nullptr);
    BOOST_CHECK_EQUAL(*c.find(index_t{13}), 2);
}

BOOST_AUTO_TEST_CASE(test_emplace_before_base) {
    log_indexed_container<int> c;
    c.emplace(index_t{5}, 50);
    c.emplace(index_t{7}, 70);

    // Emplace before the current base index, prepending empty slots.
    c.emplace(index_t{3}, 30);
    BOOST_CHECK_EQUAL(c.base_index(), index_t{3});
    BOOST_CHECK_EQUAL(*c.find(index_t{3}), 30);
    BOOST_CHECK(c.find(index_t{4}) == nullptr);
    BOOST_CHECK_EQUAL(*c.find(index_t{5}), 50);
    BOOST_CHECK(c.find(index_t{6}) == nullptr);
    BOOST_CHECK_EQUAL(*c.find(index_t{7}), 70);
}

BOOST_AUTO_TEST_CASE(test_clear_at_and_trim) {
    log_indexed_container<int> c;
    c.emplace(index_t{5}, 1);
    c.emplace(index_t{6}, 2);
    c.emplace(index_t{7}, 3);

    c.clear_at(index_t{5});
    c.trim_front();
    // Front was cleared, trim_front should advance base_idx.
    BOOST_CHECK_EQUAL(c.base_index(), index_t{6});
    BOOST_CHECK(c.find(index_t{5}) == nullptr);
    BOOST_CHECK_EQUAL(*c.find(index_t{6}), 2);

    c.clear_at(index_t{7});
    c.trim_front();
    // Index 7 cleared but 6 is still at front, no trimming.
    BOOST_CHECK_EQUAL(c.base_index(), index_t{6});
    BOOST_CHECK(c.find(index_t{7}) == nullptr);

    c.clear_at(index_t{6});
    c.trim_front();
    // All cleared, container should be empty.
    BOOST_CHECK(c.empty());
}

BOOST_AUTO_TEST_CASE(test_clear_at_middle_then_front) {
    log_indexed_container<int> c;
    c.emplace(index_t{1}, 10);
    c.emplace(index_t{2}, 20);
    c.emplace(index_t{3}, 30);

    // Clear middle — no trimming.
    c.clear_at(index_t{2});
    c.trim_front();
    BOOST_CHECK_EQUAL(c.base_index(), index_t{1});
    BOOST_CHECK(c.find(index_t{2}) == nullptr);

    // Clear front — trims past the gap.
    c.clear_at(index_t{1});
    c.trim_front();
    BOOST_CHECK_EQUAL(c.base_index(), index_t{3});
    BOOST_CHECK_EQUAL(*c.find(index_t{3}), 30);
}

BOOST_AUTO_TEST_CASE(test_for_each) {
    log_indexed_container<int> c;
    c.emplace(index_t{2}, 20);
    c.emplace(index_t{4}, 40);
    c.emplace(index_t{5}, 50);

    std::vector<std::pair<uint64_t, int>> visited;
    c.for_each([&](index_t idx, int& val) {
        visited.emplace_back(idx.value(), val);
    });

    BOOST_REQUIRE_EQUAL(visited.size(), 3);
    BOOST_CHECK_EQUAL(visited[0].first, 2);
    BOOST_CHECK_EQUAL(visited[0].second, 20);
    BOOST_CHECK_EQUAL(visited[1].first, 4);
    BOOST_CHECK_EQUAL(visited[1].second, 40);
    BOOST_CHECK_EQUAL(visited[2].first, 5);
    BOOST_CHECK_EQUAL(visited[2].second, 50);
}

BOOST_AUTO_TEST_CASE(test_for_each_with_clear_at) {
    log_indexed_container<int> c;
    c.emplace(index_t{1}, 10);
    c.emplace(index_t{2}, 20);
    c.emplace(index_t{3}, 30);

    // Clear some entries during iteration.
    c.for_each([&](index_t idx, int& val) {
        if (idx == index_t{1} || idx == index_t{3}) {
            c.clear_at(idx);
        }
    });

    // After for_each, trim_front should have cleaned up.
    BOOST_CHECK(c.find(index_t{1}) == nullptr);
    BOOST_CHECK_EQUAL(*c.find(index_t{2}), 20);
    BOOST_CHECK(c.find(index_t{3}) == nullptr);
}

BOOST_AUTO_TEST_CASE(test_clear) {
    log_indexed_container<int> c;
    c.emplace(index_t{10}, 1);
    c.emplace(index_t{20}, 2);

    c.clear();
    BOOST_CHECK(c.empty());
    BOOST_CHECK(c.find(index_t{10}) == nullptr);

    // Can reuse after clear.
    c.emplace(index_t{5}, 99);
    BOOST_CHECK_EQUAL(*c.find(index_t{5}), 99);
}

BOOST_AUTO_TEST_CASE(test_move_only_type) {
    log_indexed_container<std::unique_ptr<int>> c;
    c.emplace(index_t{1}, std::make_unique<int>(42));

    auto* p = c.find(index_t{1});
    BOOST_REQUIRE(p != nullptr);
    BOOST_CHECK_EQUAL(**p, 42);

    c.clear_at(index_t{1});
    c.trim_front();
    BOOST_CHECK(c.empty());
}
