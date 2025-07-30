/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "default_aws_retry_strategy.hh"
#include "aws_error.hh"
#include "seastar/http/exception.hh"
#include "seastar/util/short_streams.hh"
#include "utils/log.hh"

namespace seastar::http::experimental {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http::experimental;

namespace aws {

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries, unsigned scale_factor) : _max_retries(max_retries), _scale_factor(scale_factor) {
}

seastar::future<bool> default_aws_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    if (attempted_retries >= _max_retries) {
        rs_logger.warn("Retries exhausted. Retry# {}", attempted_retries);
        co_return false;
    }
    auto err = aws_error::from_exception_ptr(error);
    bool should_retry = err.is_retryable() == retryable::yes;
    if (should_retry) {
        rs_logger.debug("AWS HTTP client request failed. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}
seastar::future<seastar::input_stream<char>> default_aws_retry_strategy::analyze_reply(std::optional<seastar::http::reply::status_type> expected,
                                                                                       const seastar::http::reply& rep,
                                                                                       seastar::input_stream<char>&& in) const {
    auto _in = std::move(in);
    auto status_class = seastar::http::reply::classify_status(rep._status);

    if (status_class != seastar::http::reply::status_class::informational && status_class != seastar::http::reply::status_class::success) {
        std::optional<aws_error> possible_error = aws_error::parse(co_await seastar::util::read_entire_stream_contiguous(_in));
        if (possible_error) {
            throw aws_exception(std::move(possible_error.value()));
        }
        throw aws_exception(aws_error::from_http_code(rep._status));
    }

    if (expected.has_value() && rep._status != *expected) {
        throw seastar::httpd::unexpected_status_error(rep._status);
    }
    co_return std::move(_in);
}

} // namespace aws
