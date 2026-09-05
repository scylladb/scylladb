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
#include "utils/log.hh"

namespace seastar::http {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http;

namespace aws {

static seastar::future<> sleep_before_retry(size_t attempted_retries, seastar::abort_source* as) {
    if (attempted_retries == 0) {
        return seastar::make_ready_future();
    }
    constexpr size_t scale_factor = 25;
    auto d = std::chrono::milliseconds((1UL << attempted_retries) * scale_factor);
    if (!as) {
        return seastar::sleep(d);
    }
    // Report the abort the same way a pre-request abort_requested() check would,
    // so callers don't have to special-case a mid-backoff sleep_aborted.
    return seastar::sleep_abortable(d, *as).handle_exception_type([as] (const seastar::sleep_aborted&) {
        return seastar::make_exception_future<>(as->abort_requested_exception_ptr());
    });
}

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, seastar::abort_source* as) : _max_retries(max_retries), _as(as) {
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
        co_await sleep_before_retry(attempted_retries, _as);
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}

} // namespace aws
