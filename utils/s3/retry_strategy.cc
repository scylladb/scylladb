/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "retry_strategy.hh"
#include "aws_error.hh"
#include "utils/log.hh"

using namespace std::chrono_literals;

namespace aws {

static logging::logger rs_logger("default_retry_strategy");

default_retry_strategy::default_retry_strategy(unsigned max_retries, unsigned scale_factor) : _max_retries(max_retries), _scale_factor(scale_factor) {
}

bool default_retry_strategy::should_retry(const aws_error& error, unsigned attempted_retries) const {
    if (attempted_retries >= _max_retries) {
        rs_logger.error("Retries exhausted. Retry# {}", attempted_retries);
        return false;
    }
    bool should_retry = error.is_retryable() == retryable::yes;
    if (should_retry) {
        rs_logger.debug("S3 client request failed. Reason: {}. Retry# {}", error.get_error_message(), attempted_retries);
    } else {
        rs_logger.error("S3 client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                        error.get_error_message(),
                        std::to_underlying(error.get_error_type()),
                        attempted_retries);
    }
    return should_retry;
}

std::chrono::milliseconds default_retry_strategy::delay_before_retry(const aws_error&, unsigned attempted_retries) const {
    if (attempted_retries == 0) {
        return 0ms;
    }

    return std::chrono::milliseconds((1UL << attempted_retries) * _scale_factor);
}

} // namespace aws
