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
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include "utils/log.hh"

namespace seastar::http {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http;

namespace aws {

// Report an aborted wait the way a pre-request abort_requested() check would, so
// callers don't have to special-case a sleep_aborted from inside a retry.
static seastar::future<> map_abort(seastar::future<> f, seastar::abort_source* as) {
    if (!as) {
        return f;
    }
    return f.handle_exception_type([as](const seastar::sleep_aborted&) {
        return seastar::make_exception_future<>(as->abort_requested_exception_ptr());
    });
}

// Delay before a retry, by error class: a dropped connection can be retried almost
// immediately, while a 503 means the endpoint is over its limit already. A throttle
// also waits before its first retry, which is where the larger base pays off.
static seastar::future<> sleep_before_retry(size_t attempted_retries, bool is_throttling, seastar::abort_source* as) {
    // The transient base is what the client always used, so a dropped connection
    // still backs off exactly as before. Only the throttling class is reshaped.
    constexpr double transient_base_sec = 0.025;
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
    const double delay_sec = std::min(exponential, cap_sec);
    const auto delay = std::chrono::milliseconds(static_cast<int64_t>(delay_sec * 1000.0));
    if (!as) {
        return seastar::sleep(delay);
    }
    return map_abort(seastar::sleep_abortable(delay, *as), as);
}

s3::throttling_controller& default_aws_retry_strategy::no_throttling() {
    static thread_local s3::noop_throttling_controller instance;
    return instance;
}

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, s3::throttling_controller& controller, seastar::abort_source* as)
    : _max_retries(max_retries), _controller(controller), _as(as) {
}

// Errors that indicate the S3 endpoint is throttling us (429/503-class).
// These are reported to the send brake and pick the longer backoff base below.
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
    // request is still counted by the brake.
    const bool is_throttling = is_throttling_error(err.get_error_type());
    if (is_throttling) {
        _controller.on_throttled();
    } else {
        _controller.on_not_throttled();
    }

    if (attempted_retries >= _max_retries) {
        rs_logger.warn("Retries exhausted. Retry# {}", attempted_retries);
        co_return false;
    }
    bool should_retry = err.is_retryable() == utils::http::retryable::yes;

    if (should_retry) {
        rs_logger.debug("AWS HTTP client request failed. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
        co_await sleep_before_retry(attempted_retries, is_throttling, _as);
        // After the backoff, so the brake is checked close to the re-dispatch.
        co_await map_abort(_controller.acquire(_as), _as);
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}

} // namespace aws
