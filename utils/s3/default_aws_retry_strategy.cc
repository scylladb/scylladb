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

constexpr unsigned max_shiftable_retries = 20; // 1UL << shift must stay well clear of overflow

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, std::chrono::milliseconds max_sleep_time, std::chrono::milliseconds min_sleep_time)
    : _max_retries(std::min(max_retries, max_shiftable_retries))
    , _min_sleep_time(std::min(min_sleep_time, max_sleep_time)) // don't let the floor exceed a caller-configured cap below it (e.g. max_sleep_time=0)
    , _max_sleep_time(max_sleep_time) {
    // a negative cap would make compute_sleep_duration() compute a negative sleep, skipping backoff entirely
    SCYLLA_ASSERT(max_sleep_time >= std::chrono::milliseconds(0));
    SCYLLA_ASSERT(min_sleep_time >= std::chrono::milliseconds(0));
}

// Full jitter (AWS's own recommended scheme: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
// The ladder is the jitter *width* above min_sleep_time, not an absolute value compared
// against it -- so early attempts still jitter even when min_sleep_time exceeds the raw
// ladder value, instead of collapsing to min_sleep_time exactly every time.
std::chrono::milliseconds default_aws_retry_strategy::compute_sleep_duration(unsigned attempted_retries) const {
    if (attempted_retries == 0) {
        return std::chrono::milliseconds(0);
    }
    constexpr size_t scale_factor = 25;
    // thread_local: each shard runs its own reactor thread, so this is independently seeded per shard.
    static thread_local std::default_random_engine engine{std::random_device{}()};

    // safe: should_retry() only calls with attempted_retries < _max_retries <= max_shiftable_retries
    auto ladder = std::chrono::milliseconds((1UL << attempted_retries) * scale_factor);
    auto jitter_width = std::min(ladder, _max_sleep_time - _min_sleep_time); // never negative: ctor sanitizes min <= max
    std::uniform_int_distribution<int64_t> dist(0, jitter_width.count());
    return _min_sleep_time + std::chrono::milliseconds(dist(engine));
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
        auto sleep_time = compute_sleep_duration(attempted_retries);
        if (sleep_time.count() > 0) {
            co_await seastar::sleep(sleep_time);
        }
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}

} // namespace aws
