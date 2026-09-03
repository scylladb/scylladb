/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Regression test: default_aws_retry_strategy's backoff must be jittered, so
// callers that fail together don't resume in lockstep. Samples
// compute_sleep_duration() directly instead of timing real sleeps.

#include "test/lib/scylla_test_case.hh"
#include "utils/s3/default_aws_retry_strategy.hh"
#include <algorithm>
#include <vector>

using namespace std::chrono_literals;

namespace {

// Spread (max - min) across n compute_sleep_duration() samples.
std::chrono::milliseconds sample_spread(const aws::default_aws_retry_strategy& strategy, unsigned n, unsigned attempted_retries) {
    std::vector<std::chrono::milliseconds> samples;
    samples.reserve(n);
    for (unsigned i = 0; i < n; ++i) {
        samples.push_back(strategy.compute_sleep_duration(attempted_retries));
    }
    auto [min_it, max_it] = std::minmax_element(samples.begin(), samples.end());
    return *max_it - *min_it;
}

} // anonymous namespace

// ladder(3) = 200ms; samples should spread over a good fraction of it, not collapse to one value.
BOOST_AUTO_TEST_CASE(test_retry_wave_should_disperse) {
    aws::default_aws_retry_strategy strategy;
    constexpr unsigned attempted_retries = 3;
    constexpr auto expected_sleep = std::chrono::milliseconds((1UL << attempted_retries) * 25);

    auto spread = sample_spread(strategy, 2000, attempted_retries);
    fmt::print("2000 samples spread = {}ms (sleep = {}ms)\n", spread.count(), expected_sleep.count());

    BOOST_REQUIRE_GT(spread, expected_sleep / 4);
}

// ladder(15) would be ~13 minutes uncapped; max_sleep_time must cap it.
BOOST_AUTO_TEST_CASE(test_backoff_sleep_is_capped) {
    constexpr auto cap = 200ms;
    aws::default_aws_retry_strategy strategy(/* max_retries */ 100, cap);
    constexpr unsigned attempted_retries = 15; // (1<<15)*25ms would be ~13 minutes uncapped

    auto sleep_time = strategy.compute_sleep_duration(attempted_retries);
    fmt::print("retry#{} sleep = {}ms (cap = {}ms)\n", attempted_retries, sleep_time.count(), cap.count());

    BOOST_REQUIRE_LE(sleep_time, cap);
}

// A single retry must not resume near-instantly.
BOOST_AUTO_TEST_CASE(test_backoff_sleep_has_floor) {
    aws::default_aws_retry_strategy strategy(/* max_retries */ 100);
    constexpr unsigned attempted_retries = 1;

    auto sleep_time = strategy.compute_sleep_duration(attempted_retries);
    fmt::print("retry#{} sleep = {}ms\n", attempted_retries, sleep_time.count());

    BOOST_REQUIRE_GE(sleep_time, 10ms); // the default min_sleep_time floor
}

// Below the natural ladder, the floor must still jitter, not collapse to a fixed value.
BOOST_AUTO_TEST_CASE(test_backoff_sleep_jitters_below_natural_ladder) {
    constexpr auto min_sleep_time = 1000ms; // above ladder(1)=50ms and ladder(2)=100ms
    aws::default_aws_retry_strategy strategy(/* max_retries */ 10, /* max_sleep_time */ 5000ms, min_sleep_time);
    constexpr unsigned attempted_retries = 1;

    auto spread = sample_spread(strategy, 2000, attempted_retries);
    fmt::print("2000 samples spread below natural ladder = {}ms\n", spread.count());

    BOOST_REQUIRE_GT(spread, 0ms);
}
