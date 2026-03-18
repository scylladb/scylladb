/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE rolling_max_tracker

#include <boost/test/unit_test.hpp>
#include <algorithm>

#include <seastar/core/bitops.hh>

#include "utils/rolling_max_tracker.hh"

// Helper: compute the expected current_max for a given raw value.
// Mirrors the tracker's internal rounding: clamp to 1, take log2ceil,
// then raise back to a power of two.
static size_t rounded(size_t v) {
    return size_t(1) << seastar::log2ceil(std::max(v, size_t(1)));
}

BOOST_AUTO_TEST_CASE(test_empty_tracker_returns_zero) {
    utils::rolling_max_tracker tracker(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 0u);
}

BOOST_AUTO_TEST_CASE(test_single_sample) {
    utils::rolling_max_tracker tracker(10);
    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));
}

BOOST_AUTO_TEST_CASE(test_max_tracks_largest_in_window) {
    utils::rolling_max_tracker tracker(10);
    tracker.add_sample(5);
    tracker.add_sample(20);
    tracker.add_sample(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(20));
}

BOOST_AUTO_TEST_CASE(test_increasing_samples) {
    utils::rolling_max_tracker tracker(5);
    for (size_t i = 1; i <= 10; ++i) {
        tracker.add_sample(i);
        BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(i));
    }
}

BOOST_AUTO_TEST_CASE(test_decreasing_samples) {
    utils::rolling_max_tracker tracker(5);
    tracker.add_sample(100);
    tracker.add_sample(90);
    tracker.add_sample(80);
    tracker.add_sample(70);
    tracker.add_sample(60);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));

    tracker.add_sample(50);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(90));

    tracker.add_sample(40);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(80));

    tracker.add_sample(30);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(70));

    tracker.add_sample(20);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(60));
}

BOOST_AUTO_TEST_CASE(test_max_expires_from_window) {
    utils::rolling_max_tracker tracker(3);
    tracker.add_sample(100);
    tracker.add_sample(1);
    tracker.add_sample(2);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));

    tracker.add_sample(3);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(3));
}

BOOST_AUTO_TEST_CASE(test_new_max_replaces_smaller_entries) {
    utils::rolling_max_tracker tracker(5);
    tracker.add_sample(10);
    tracker.add_sample(5);
    tracker.add_sample(3);
    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(10));

    tracker.add_sample(50);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(50));

    tracker.add_sample(1);
    tracker.add_sample(1);
    tracker.add_sample(1);
    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(50));

    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(1));
}

BOOST_AUTO_TEST_CASE(test_window_size_one) {
    utils::rolling_max_tracker tracker(1);
    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));

    tracker.add_sample(5);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(5));

    tracker.add_sample(200);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(200));
}

BOOST_AUTO_TEST_CASE(test_window_size_two) {
    utils::rolling_max_tracker tracker(2);
    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));

    tracker.add_sample(5);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));

    tracker.add_sample(200);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(200));

    tracker.add_sample(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(200));

    tracker.add_sample(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(10));
}

BOOST_AUTO_TEST_CASE(test_equal_values) {
    utils::rolling_max_tracker tracker(5);
    tracker.add_sample(42);
    tracker.add_sample(42);
    tracker.add_sample(42);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(42));

    for (int i = 0; i < 20; ++i) {
        tracker.add_sample(42);
        BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(42));
    }

    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(100));
}

BOOST_AUTO_TEST_CASE(test_staircase_pattern) {
    utils::rolling_max_tracker tracker(6);

    for (size_t i = 1; i <= 5; ++i) {
        tracker.add_sample(i);
    }
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(5));

    tracker.add_sample(4);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(5));

    tracker.add_sample(3);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(5));

    tracker.add_sample(2);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(5));

    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), rounded(5));
}

BOOST_AUTO_TEST_CASE(test_zero_sample_clamped_to_one) {
    utils::rolling_max_tracker tracker(3);
    tracker.add_sample(0);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 1);

    tracker.add_sample(0);
    tracker.add_sample(0);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 1);
}

BOOST_AUTO_TEST_CASE(test_current_max_is_upper_bound) {
    // For any value, current_max() >= value (never underestimates).
    utils::rolling_max_tracker tracker(1);
    for (size_t v = 1; v <= 1024; ++v) {
        tracker.add_sample(v);
        BOOST_REQUIRE_GE(tracker.current_max(), v);
        // And at most 2x the value
        BOOST_REQUIRE_LE(tracker.current_max(), 2 * v);
    }
}

BOOST_AUTO_TEST_CASE(test_sliding_window_correctness) {
    const size_t window = 7;
    const size_t n = 100;
    utils::rolling_max_tracker tracker(window);
    std::vector<size_t> values;
    values.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        values.push_back((i * 37 + 13) % 50);
    }

    for (size_t i = 0; i < n; ++i) {
        tracker.add_sample(values[i]);

        size_t start = (i + 1 > window) ? (i + 1 - window) : 0;
        size_t expected_max = 0;
        for (size_t j = start; j <= i; ++j) {
            expected_max = std::max(expected_max, rounded(values[j]));
        }
        BOOST_REQUIRE_EQUAL(tracker.current_max(), expected_max);
    }
}
