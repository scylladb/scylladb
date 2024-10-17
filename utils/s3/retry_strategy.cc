/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "retry_strategy.hh"
#include "aws_error.hh"

using namespace std::chrono_literals;

namespace aws {

default_retry_strategy::default_retry_strategy(uint32_t max_retries, uint32_t scale_factor)
    : _max_retries(max_retries)
    , _scale_factor(scale_factor) {
}

bool default_retry_strategy::should_retry(const aws_error& error, uint32_t attempted_retries) const {
    if (attempted_retries >= _max_retries) {
        return false;
    }

    return error.is_retryable() == retryable::yes;
}

std::chrono::milliseconds default_retry_strategy::delay_before_retry(const aws_error&, uint32_t attempted_retries) const {
    if (attempted_retries == 0) {
        return 0ms;
    }

    return std::chrono::milliseconds((1UL << attempted_retries) * _scale_factor);
}

} // namespace aws
