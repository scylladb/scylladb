/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "default_aws_retry_strategy.hh"
#include "aws_error.hh"
#include "throttling_controller.hh"

#include <seastar/core/sleep.hh>

#include <algorithm>
#include <random>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include "utils/log.hh"

namespace seastar::http {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http;

namespace aws {

// Full jitter, uniform in [0, 1). Without it every stream refused at the same
// instant retries at the same instant, over and over.
static double jitter() {
    static thread_local std::mt19937 engine{std::random_device{}()};
    static thread_local std::uniform_real_distribution<double> dist{0.0, 1.0};
    return dist(engine);
}

// Delay before a retry, by error class: a dropped connection can be retried almost
// immediately, while a 503 means the endpoint is over its limit already. A throttle
// also waits before its first retry, which is where the larger base pays off.
static seastar::future<> sleep_before_retry(size_t attempted_retries, bool is_throttling) {
    constexpr double transient_base_sec = 0.05;
    constexpr double throttling_base_sec = 1.0;
    // The cap, not the base, sets the retry window at this depth: with base 1.0 the
    // exponential passes 20 s at attempt 5, so attempts 5-9 would all park on a 20 s
    // cap. Total window per request, over ten retries:
    //
    //   cap 20:  1, 2, 4, 8, 16, 20, 20, 20, 20, 20  = 131 s
    //   cap 60:  1, 2, 4, 8, 16, 32, 60, 60, 60, 60  = 303 s
    //
    // Coupled to max_retries -- changing that means re-deriving this.
    constexpr double cap_sec = 60.0;

    if (attempted_retries == 0 && !is_throttling) {
        return seastar::make_ready_future();
    }
    const double base = is_throttling ? throttling_base_sec : transient_base_sec;
    const double exponential = base * static_cast<double>(1UL << std::min(attempted_retries, size_t{30}));
    const double delay_sec = std::min(exponential, cap_sec) * jitter();
    return seastar::sleep(std::chrono::milliseconds(static_cast<int64_t>(delay_sec * 1000.0)));
}

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, s3::throttling_controller* controller)
    : _max_retries(max_retries), _controller(controller) {
}

// Errors that indicate the S3 endpoint is throttling us (429/503-class).
// These drive the adaptive send-rate limiter's back-off.
static bool is_throttling_error(aws::aws_error_type type) {
    using enum aws::aws_error_type;
    switch (type) {
    case SLOW_DOWN:
    case THROTTLING:
    case SERVICE_UNAVAILABLE:
    case HTTP_TOO_MANY_REQUESTS:
    case HTTP_SERVICE_UNAVAILABLE:
    case HTTP_BANDWIDTH_LIMIT_EXCEEDED:
        return true;
    default:
        return false;
    }
}

seastar::future<bool> default_aws_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    auto err = aws_error::from_exception_ptr(error);

    // Report before the caps are consulted, so that the response which exhausts a
    // request still reaches the control loop.
    const bool is_throttling = is_throttling_error(err.get_error_type());
    if (_controller && is_throttling) {
        _controller->on_throttled();
    }

    if (attempted_retries >= _max_retries) {
        rs_logger.warn("Retries exhausted. Retry# {}", attempted_retries);
        co_return false;
    }
    bool should_retry = err.is_retryable() == utils::http::retryable::yes;
    if (should_retry) {
        rs_logger.debug("AWS HTTP client request failed. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
        co_await sleep_before_retry(attempted_retries, is_throttling);
        if (_controller) {
            // After the backoff, so the token is spent close to the re-dispatch.
            // should_retry() has no abort_source, so this wait is not abortable.
            co_await _controller->acquire(nullptr);
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
