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

BOOST_AUTO_TEST_CASE(test_extract_trims_front) {
    log_indexed_container<int> c;
    c.emplace(index_t{5}, 1);
    c.emplace(index_t{6}, 2);
    c.emplace(index_t{7}, 3);

    // extract returns the stored value and advances base_idx past the
    // now-empty front.
    BOOST_CHECK_EQUAL(c.extract(index_t{5}), 1);
    BOOST_CHECK_EQUAL(c.base_index(), index_t{6});
    BOOST_CHECK(c.find(index_t{5}) == nullptr);
    BOOST_CHECK_EQUAL(*c.find(index_t{6}), 2);

    // Index 7 extracted but 6 is still at front, no trimming.
    BOOST_CHECK_EQUAL(c.extract(index_t{7}), 3);
    BOOST_CHECK_EQUAL(c.base_index(), index_t{6});
    BOOST_CHECK(c.find(index_t{7}) == nullptr);

    // All extracted, container should be empty.
    BOOST_CHECK_EQUAL(c.extract(index_t{6}), 2);
    BOOST_CHECK(c.empty());
}

BOOST_AUTO_TEST_CASE(test_extract_middle_then_front) {
    log_indexed_container<int> c;
    c.emplace(index_t{1}, 10);
    c.emplace(index_t{2}, 20);
    c.emplace(index_t{3}, 30);

    // Extract the middle — no trimming.
    c.extract(index_t{2});
    BOOST_CHECK_EQUAL(c.base_index(), index_t{1});
    BOOST_CHECK(c.find(index_t{2}) == nullptr);

    // Extract the front — trims past the gap.
    c.extract(index_t{1});
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

BOOST_AUTO_TEST_CASE(test_for_each_mutates_values) {
    log_indexed_container<int> c;
    c.emplace(index_t{2}, 20);
    c.emplace(index_t{4}, 40);

    c.for_each([](index_t, int& val) {
        val += 1;
    });

    BOOST_CHECK_EQUAL(*c.find(index_t{2}), 21);
    BOOST_CHECK_EQUAL(*c.find(index_t{4}), 41);
}

BOOST_AUTO_TEST_CASE(test_erase_if_in_range) {
    log_indexed_container<int> c;
    for (uint64_t i = 2; i <= 6; ++i) {
        c.emplace(index_t{i}, int(i * 10));
    }

    // Only the slots within the range are visited, and only those the
    // callback rejects are erased.
    std::vector<uint64_t> visited;
    c.erase_if_in_range(index_t{3}, index_t{5}, [&](index_t idx, int& val) {
        visited.push_back(idx.value());
        return val != 40;
    });

    BOOST_REQUIRE_EQUAL(visited.size(), 3);
    BOOST_CHECK_EQUAL(visited[0], 3);
    BOOST_CHECK_EQUAL(visited[1], 4);
    BOOST_CHECK_EQUAL(visited[2], 5);

    BOOST_CHECK_EQUAL(c.base_index(), index_t{2});
    BOOST_CHECK(c.find(index_t{4}) == nullptr);
    for (uint64_t i : {2, 3, 5, 6}) {
        BOOST_REQUIRE(c.find(index_t{i}) != nullptr);
        BOOST_CHECK_EQUAL(*c.find(index_t{i}), int(i * 10));
    }
}

BOOST_AUTO_TEST_CASE(test_erase_if_in_range_trims_front) {
    log_indexed_container<int> c;
    c.emplace(index_t{2}, 20);
    c.emplace(index_t{3}, 30);
    c.emplace(index_t{7}, 70);

    // Erasing the front elements moves the base index up to the next
    // occupied slot, preserving invariant (1).
    c.erase_if_in_range(index_t{0}, index_t{3}, [](index_t, int&) { return false; });

    BOOST_CHECK_EQUAL(c.base_index(), index_t{7});
    BOOST_REQUIRE(c.peek_front() != nullptr);
    BOOST_CHECK_EQUAL(*c.peek_front(), 70);

    c.erase_if_in_range(index_t{7}, index_t{7}, [](index_t, int&) { return false; });
    BOOST_CHECK(c.empty());
}

BOOST_AUTO_TEST_CASE(test_erase_if_in_range_out_of_bounds) {
    log_indexed_container<int> c;
    c.emplace(index_t{5}, 50);

    size_t visits = 0;
    auto count = [&](index_t, int&) { ++visits; return true; };

    // Ranges entirely below or above the container are no-ops, and a range
    // that only overlaps it visits just the overlap.
    c.erase_if_in_range(index_t{0}, index_t{4}, count);
    c.erase_if_in_range(index_t{6}, index_t{100}, count);
    BOOST_CHECK_EQUAL(visits, 0);

    c.erase_if_in_range(index_t{0}, index_t{100}, count);
    BOOST_CHECK_EQUAL(visits, 1);

    log_indexed_container<int> empty;
    empty.erase_if_in_range(index_t{0}, index_t{10}, count);
    BOOST_CHECK_EQUAL(visits, 1);
}

BOOST_AUTO_TEST_CASE(test_erase_leading_above) {
    log_indexed_container<int> c;
    for (uint64_t i = 2; i <= 5; ++i) {
        c.emplace(index_t{i}, int(i * 10));
    }

    // Erases from `first` upward and stops at the first slot it keeps, so the
    // one it stopped on and everything above it survive.
    std::vector<uint64_t> visited;
    c.erase_leading_above(index_t{3}, [&](index_t idx, int& val) {
        visited.push_back(idx.value());
        return val == 40;
    });

    BOOST_REQUIRE_EQUAL(visited.size(), 2);
    BOOST_CHECK_EQUAL(visited[0], 3);
    BOOST_CHECK_EQUAL(visited[1], 4);
    BOOST_CHECK(c.find(index_t{3}) == nullptr);
    BOOST_CHECK(c.find(index_t{4}) != nullptr);
    BOOST_CHECK(c.find(index_t{5}) != nullptr);
    // Below `first` is never touched.
    BOOST_CHECK(c.find(index_t{2}) != nullptr);

    // Keeping the very first slot visited leaves everything in place.
    size_t visits = 0;
    c.erase_leading_above(index_t{0}, [&](index_t, int&) { ++visits; return true; });
    BOOST_CHECK_EQUAL(visits, 1);
    BOOST_CHECK(c.find(index_t{2}) != nullptr);
    BOOST_CHECK(c.find(index_t{4}) != nullptr);
    BOOST_CHECK(c.find(index_t{5}) != nullptr);

    // A first index below the base index still starts at the front, and
    // erasing everything trims the container away.
    c.erase_leading_above(index_t{0}, [](index_t, int&) { return false; });
    BOOST_CHECK(c.empty());

    // And on an empty container it is a no-op.
    c.erase_leading_above(index_t{0}, [](index_t, int&) {
        BOOST_FAIL("callback must not be called on an empty container");
        return true;
    });
}

BOOST_AUTO_TEST_CASE(test_erase_if_in_range_moves_value_out) {
    log_indexed_container<std::unique_ptr<int>> c;
    c.emplace(index_t{1}, std::make_unique<int>(11));
    c.emplace(index_t{2}, std::make_unique<int>(22));

    std::vector<std::unique_ptr<int>> taken;
    c.erase_if_in_range(index_t{1}, index_t{2}, [&](index_t idx, std::unique_ptr<int>& val) {
        if (idx == index_t{1}) {
            taken.push_back(std::move(val));
            return false;
        }
        return true;
    });

    BOOST_REQUIRE_EQUAL(taken.size(), 1);
    BOOST_CHECK_EQUAL(*taken[0], 11);
    BOOST_CHECK_EQUAL(c.base_index(), index_t{2});
    BOOST_CHECK_EQUAL(**c.peek_front(), 22);
}

BOOST_AUTO_TEST_CASE(test_peek_front_and_consume) {
    log_indexed_container<int> c;
    BOOST_CHECK(c.peek_front() == nullptr);

    c.emplace(index_t{4}, 40);
    c.emplace(index_t{2}, 20); // before base — becomes the new front

    BOOST_CHECK_EQUAL(c.base_index(), index_t{2});
    BOOST_REQUIRE(c.peek_front() != nullptr);
    BOOST_CHECK_EQUAL(*c.peek_front(), 20);

    // Consume the front: extracting it trims past the gap at index 3.
    BOOST_CHECK_EQUAL(c.extract(c.base_index()), 20);
    BOOST_CHECK_EQUAL(c.base_index(), index_t{4});
    BOOST_REQUIRE(c.peek_front() != nullptr);
    BOOST_CHECK_EQUAL(*c.peek_front(), 40);

    BOOST_CHECK_EQUAL(c.extract(c.base_index()), 40);
    BOOST_CHECK(c.peek_front() == nullptr);
    BOOST_CHECK(c.empty());
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

    // extract moves the value out to the caller.
    auto extracted = c.extract(index_t{1});
    BOOST_REQUIRE(extracted != nullptr);
    BOOST_CHECK_EQUAL(*extracted, 42);
    BOOST_CHECK(c.empty());
}
