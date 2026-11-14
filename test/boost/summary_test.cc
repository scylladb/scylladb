/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/histogram.hh"
#include <algorithm>
#include <sstream>

using duration = utils::time_estimated_histogram::duration;

std::string vector_to_string(const std::vector<double>& v) {
    std::stringstream ss;
    ss << "{";
    std::copy(v.begin(), v.end(),
        std::ostream_iterator<uint64_t>(ss, ","));
    ss << "}";
    return ss.str();
}

void fill_range(utils::summary_calculator& summary, size_t count, const duration& from, const duration& to) {
    auto delta = (to-from)/count;
    for (size_t i = 0; i < count; i++) {
        summary.mark(from + i*delta);
    }
}

BOOST_AUTO_TEST_CASE(test_summary_api) {
    utils::summary_calculator summary;
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0,0}));
    BOOST_CHECK_EQUAL(vector_to_string(summary.quantiles()), vector_to_string({0.5,0.95,0.99}));
    summary.set_quantiles({0.7, 0.8});
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0}));
    BOOST_CHECK_EQUAL(vector_to_string(summary.quantiles()), vector_to_string({0.7, 0.8}));
    summary.mark(std::chrono::microseconds(520));
    summary.mark(std::chrono::microseconds(770));
    BOOST_CHECK_EQUAL(summary.histogram().count(), 2);
    BOOST_CHECK_EQUAL(summary.histogram().get(0), 1);
    BOOST_CHECK_EQUAL(summary.histogram().get(2), 1);
}

BOOST_AUTO_TEST_CASE(test_summary_calculation) {
    utils::summary_calculator summary;
    fill_range(summary, 1000, std::chrono::microseconds(520), std::chrono::seconds(1));
    fill_range(summary, 900, std::chrono::seconds(1), std::chrono::seconds(2));
    fill_range(summary, 50, std::chrono::seconds(2), std::chrono::seconds(3));
    fill_range(summary, 40, std::chrono::seconds(3), std::chrono::seconds(4));
    fill_range(summary, 10, std::chrono::seconds(4), std::chrono::seconds(8));
    BOOST_CHECK_EQUAL(summary.histogram().count(), 2000);
    summary.update();
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({1048576, 2097152, 4194304}));
    summary.update();
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0,0}));
}

// The snapshot is array<uint32,65> + uint64, not a full histogram; 65*4 = 260
// isn't 8-aligned, so 4 B tail padding makes the net saving 256 (not 260) B.
// Guard against the footprint regressing: summary_calculator must stay at least
// 256 B smaller than the old two-full-histograms layout. We keep
// sizeof(std::vector<double>) symbolic on both sides so the two vectors embedded
// in summary_calculator cancel the two on the right, making the check independent
// of the stdlib's vector layout (e.g. _GLIBCXX_DEBUG).
static_assert(sizeof(utils::summary_calculator) + 256 <= 2 * sizeof(std::vector<double>) /* two vectors */ + 2 * sizeof(utils::time_estimated_histogram),
        "summary_calculator previous-snapshot footprint regressed");

// Two consecutive non-empty intervals: the second summary must reflect only the
// second interval, proving the snapshot isolates per-interval deltas.
BOOST_AUTO_TEST_CASE(test_summary_multi_interval) {
    utils::summary_calculator summary;
    fill_range(summary, 1000, std::chrono::microseconds(520), std::chrono::seconds(1));
    summary.update();
    auto first = summary.summary();
    BOOST_CHECK_GT(first[0], 0);
    BOOST_CHECK_LT(first[2], 2000000); // p99 of the [520us, 1s) interval stays sub-2s

    // Second, non-empty interval entirely in the [2s, 3s) band.
    fill_range(summary, 1000, std::chrono::seconds(2), std::chrono::seconds(3));
    summary.update();
    auto second = summary.summary();
    // Must land in the [2s, 3s) band, i.e. above the first interval's p99.
    BOOST_CHECK_GT(second[0], first[2]);
    BOOST_CHECK_GE(second[0], 2000000);

    // Empty interval => zeros.
    summary.update();
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0,0}));
}

// A capacity-0 sample buffer must accept mark()'s push_back as a no-op while
// still maintaining the scalar stats. ihistogram has no timer (unlike the
// timed_rate_* wrappers), so it can be exercised in a plain unit test.
BOOST_AUTO_TEST_CASE(test_minimal_sample_buffer) {
    for (size_t cap : {size_t(0), size_t(1)}) {
        utils::ihistogram h(cap);
        BOOST_CHECK_EQUAL(h.sample.capacity(), cap);
        for (int i = 1; i <= 1000; i++) {
            h.mark(std::chrono::microseconds(i));
        }
        BOOST_CHECK_EQUAL(h.sample.size(), cap); // never exceeds capacity
        BOOST_CHECK_EQUAL(h.count, 1000);
        BOOST_CHECK_EQUAL(h.min, 1);
        BOOST_CHECK_EQUAL(h.max, 1000);
    }
}
