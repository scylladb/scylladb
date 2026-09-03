/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Reproducer: default_aws_retry_strategy's backoff has no jitter, so any
// number of independent callers that fail at the same instant (same
// attempted_retries) resume at the same instant too - a synchronized
// retry wave against the same bucket/endpoint.

#include "test/lib/scylla_test_case.hh"
#include "utils/s3/default_aws_retry_strategy.hh"
#include "utils/s3/aws_error.hh"
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/when_all.hh>
#include <vector>
#include <algorithm>

using namespace seastar;
using namespace std::chrono_literals;

namespace {

std::exception_ptr make_retryable_error() {
    return std::make_exception_ptr(aws::aws_exception(aws::aws_error(aws::aws_error_type::INTERNAL_FAILURE, utils::http::retryable::yes)));
}

// Simulates N independent shards/nodes that all hit a retryable error at
// the same moment and are on their Kth retry already (attempted_retries).
// Returns the spread (max - min) of their resume timestamps.
future<std::chrono::microseconds> measure_resume_spread(unsigned n, unsigned attempted_retries) {
    std::vector<aws::default_aws_retry_strategy> strategies;
    strategies.reserve(n);
    for (unsigned i = 0; i < n; ++i) {
        strategies.emplace_back(attempted_retries + 10); // never exhaust retries
    }

    auto t0 = lowres_clock::now();
    std::vector<future<std::chrono::microseconds>> futs;
    futs.reserve(n);
    for (auto& s : strategies) {
        futs.push_back(s.should_retry(make_retryable_error(), attempted_retries).then([t0](bool) {
            return std::chrono::duration_cast<std::chrono::microseconds>(lowres_clock::now() - t0);
        }));
    }
    auto results = co_await when_all_succeed(futs.begin(), futs.end());
    auto [min_it, max_it] = std::minmax_element(results.begin(), results.end());
    co_return *max_it - *min_it;
}

} // anonymous namespace

// attempted_retries=3 => (1<<3)*25ms == 200ms sleep. If backoff were
// jittered, N independent callers would resume spread out over a good
// fraction of that 200ms window. With no jitter they should all resume
// within a few ms of each other (pure scheduler noise), regardless of N.
// Contract a retry strategy is expected to uphold (e.g. AWS SDKs' "full
// jitter"): independent callers retrying at the same attempted_retries
// should resume spread out over a good fraction of the backoff window,
// not in lockstep. These assertions FAIL against the current
// default_aws_retry_strategy because it has no jitter term at all.
SEASTAR_TEST_CASE(test_retry_wave_should_disperse_small) {
    constexpr unsigned attempted_retries = 3;
    constexpr auto expected_sleep = std::chrono::milliseconds((1UL << attempted_retries) * 25);

    auto spread = co_await measure_resume_spread(10, attempted_retries);
    fmt::print("N=10 resume spread = {}us (sleep = {}ms)\n", spread.count(), expected_sleep.count());

    BOOST_REQUIRE_GT(spread, expected_sleep / 4);
}

SEASTAR_TEST_CASE(test_retry_wave_should_disperse_at_scale) {
    constexpr unsigned attempted_retries = 3;
    constexpr auto expected_sleep = std::chrono::milliseconds((1UL << attempted_retries) * 25);

    // Scale N up by two orders of magnitude (models more concurrent
    // objects/shards/nodes hitting the same bucket). If anything, a
    // correct jittered implementation disperses *more* visibly as N
    // grows (the resume histogram fills out the window); this codebase's
    // implementation stays glued together regardless of N.
    auto spread = co_await measure_resume_spread(2000, attempted_retries);
    fmt::print("N=2000 resume spread = {}us (sleep = {}ms)\n", spread.count(), expected_sleep.count());

    BOOST_REQUIRE_GT(spread, expected_sleep / 4);
}
