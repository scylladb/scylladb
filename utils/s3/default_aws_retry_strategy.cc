/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "default_aws_retry_strategy.hh"
#include "aws_error.hh"
#include <seastar/core/sleep.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include "utils/assert.hh"
#include "utils/log.hh"
#include <algorithm>
#include <random>

namespace seastar::http {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http;

namespace aws {

// Full jitter (AWS's own recommended scheme): sleep for a random duration in
// [floor, min(cap, base*2^attempt)] so independent callers that fail together
// don't resume together and re-synchronize the load they just got throttled for.
static seastar::future<> sleep_before_retry(size_t attempted_retries, std::chrono::milliseconds max_sleep_time, seastar::abort_source* as) {
    constexpr size_t scale_factor = 25;
    constexpr auto floor_sleep_time = std::chrono::milliseconds(10);
    // thread_local: each shard runs its own reactor thread, so this is independently seeded per shard.
    static thread_local std::default_random_engine engine{std::random_device{}()};

    auto shift = std::min<size_t>(attempted_retries, 20); // avoid overflowing the shift
    auto ladder = std::chrono::milliseconds((1UL << shift) * scale_factor);
    // don't let the floor exceed a caller-configured cap below it (e.g. max_sleep_time=0)
    auto floor = std::min(floor_sleep_time, max_sleep_time);
    auto cap = std::max(std::min(ladder, max_sleep_time), floor);
    std::uniform_int_distribution<int64_t> dist(floor.count(), cap.count());
    auto sleep_time = std::chrono::milliseconds(dist(engine));

    if (as) {
        return seastar::sleep_abortable(sleep_time, *as);
    }
    return seastar::sleep(sleep_time);
}

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, std::chrono::milliseconds max_sleep_time, seastar::abort_source* as)
    : _max_retries(max_retries), _max_sleep_time(max_sleep_time), _as(as) {
    // a negative cap would make sleep_before_retry() compute a negative sleep, skipping backoff entirely
    SCYLLA_ASSERT(max_sleep_time >= std::chrono::milliseconds(0));
}

seastar::future<bool> default_aws_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    if (attempted_retries >= _max_retries) {
        rs_logger.warn("Retries exhausted. Retry# {}", attempted_retries);
        co_return false;
    }
    auto err = aws_error::from_exception_ptr(error);
    bool should_retry = err.is_retryable() == utils::http::retryable::yes;
    if (should_retry) {
        rs_logger.debug("AWS HTTP client request failed. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
        co_await sleep_before_retry(attempted_retries, _max_sleep_time, _as);
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}

} // namespace aws
