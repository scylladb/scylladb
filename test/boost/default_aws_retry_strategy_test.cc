/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Regression test: default_aws_retry_strategy's backoff must be jittered, so
// independent callers that fail at the same instant (same attempted_retries)
// don't all resume at the same instant and re-synchronize the retry wave.

#include "test/lib/scylla_test_case.hh"
#include "utils/s3/default_aws_retry_strategy.hh"
#include "utils/s3/aws_error.hh"
#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/sleep.hh>
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

// attempted_retries=3 => ladder = (1<<3)*25ms == 200ms. Full jitter picks
// uniformly from [floor, min(cap, ladder)], so N independent callers should
// resume spread out over a good fraction of that window, not in lockstep.
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
    // objects/shards/nodes hitting the same bucket). A correct jittered
    // implementation disperses at least as well as N grows.
    auto spread = co_await measure_resume_spread(2000, attempted_retries);
    fmt::print("N=2000 resume spread = {}us (sleep = {}ms)\n", spread.count(), expected_sleep.count());

    BOOST_REQUIRE_GT(spread, expected_sleep / 4);
}

// The ladder for a high attempt count would climb past a minute with the old
// (1<<n)*25ms formula; max_sleep_time must cap it at a few seconds.
SEASTAR_TEST_CASE(test_backoff_sleep_is_capped) {
    constexpr auto cap = 200ms;
    aws::default_aws_retry_strategy strategy(/* max_retries */ 100, cap);
    constexpr unsigned attempted_retries = 15; // (1<<15)*25ms would be ~13 minutes uncapped

    auto t0 = lowres_clock::now();
    co_await strategy.should_retry(make_retryable_error(), attempted_retries);
    auto elapsed = lowres_clock::now() - t0;

    fmt::print("retry#{} elapsed = {}ms (cap = {}ms)\n",
               attempted_retries,
               std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
               cap.count());
    BOOST_REQUIRE_LE(elapsed, cap + 100ms); // allow scheduler slack
}

// A single retry must not resume near-instantly; a small floor keeps the
// backoff meaningful even at low attempt counts.
SEASTAR_TEST_CASE(test_backoff_sleep_has_floor) {
    aws::default_aws_retry_strategy strategy(/* max_retries */ 100);
    constexpr unsigned attempted_retries = 1;

    auto t0 = lowres_clock::now();
    co_await strategy.should_retry(make_retryable_error(), attempted_retries);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(lowres_clock::now() - t0);

    fmt::print("retry#{} elapsed = {}ms\n", attempted_retries, elapsed.count());
    BOOST_REQUIRE_GE(elapsed, 5ms); // well above scheduler noise, below the 10ms floor's own margin
}

// An in-progress backoff sleep must be cancellable via abort_source, so
// shutdown/close doesn't wait out a stuck retry's full backoff window.
SEASTAR_TEST_CASE(test_backoff_sleep_is_abortable) {
    constexpr auto cap = 60s; // long enough that only the abort ends the wait
    abort_source as;
    aws::default_aws_retry_strategy strategy(/* max_retries */ 100, cap, &as);
    constexpr unsigned attempted_retries = 10;

    auto fut = strategy.should_retry(make_retryable_error(), attempted_retries);
    co_await sleep(50ms);
    as.request_abort();

    BOOST_REQUIRE_THROW(co_await std::move(fut), seastar::sleep_aborted);
}
