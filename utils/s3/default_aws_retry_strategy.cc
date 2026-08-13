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

#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include "utils/log.hh"

namespace seastar::http {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http;

namespace aws {

static seastar::future<> sleep_before_retry(size_t attempted_retries) {
    if (attempted_retries == 0) {
        return seastar::make_ready_future();
    }
    constexpr size_t scale_factor = 25;
    return seastar::sleep(std::chrono::milliseconds((1UL << attempted_retries) * scale_factor));
}

s3::throttling_controller& default_aws_retry_strategy::no_throttling() {
    static thread_local s3::noop_throttling_controller instance;
    return instance;
}

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, s3::throttling_controller& controller)
    : _max_retries(max_retries), _controller(controller) {
}

// Errors that indicate the S3 endpoint is throttling us (429/503-class).
// These pick the longer backoff base and trip the send brake.
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
    // request still trips the brake.
    if (is_throttling_error(err.get_error_type())) {
        _controller.on_throttled();
    }

    if (attempted_retries >= _max_retries) {
        rs_logger.warn("Retries exhausted. Retry# {}", attempted_retries);
        co_return false;
    }
    bool should_retry = err.is_retryable() == utils::http::retryable::yes;

    if (should_retry) {
        rs_logger.debug("AWS HTTP client request failed. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
        co_await sleep_before_retry(attempted_retries);
        // After the backoff, so the brake is checked close to the re-dispatch.
        // should_retry() has no abort_source, so this wait is not abortable.
        co_await _controller.acquire(nullptr);
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}

} // namespace aws
