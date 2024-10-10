/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <cstdint>
#include <seastar/util/bool_class.hh>

namespace aws {
using retryable = seastar::bool_class<struct is_retryable>;
class aws_error;
class retry_strategy {
public:
    virtual ~retry_strategy() = default;
    // Returns true if the error can be retried given the error and the number of times already tried.
    [[nodiscard]] virtual retryable should_retry(const aws_error& error, uint32_t attempted_retries) const = 0;

    // Calculates the time in milliseconds the client should wait before attempting another request based on the error and attemptedRetries count.
    [[nodiscard]] virtual uint32_t delay_before_retry(const aws_error& error, uint32_t attempted_retries) const = 0;

    [[nodiscard]] virtual uint32_t get_max_retries() const = 0;
};

class default_retry_strategy : public retry_strategy {
    uint32_t _max_retries;
    uint32_t _scale_factor;

public:
    explicit default_retry_strategy(uint32_t max_retries = 10, uint32_t scale_factor = 25);

    [[nodiscard]] retryable should_retry(const aws_error& error, uint32_t attempted_retries) const override;

    [[nodiscard]] uint32_t delay_before_retry(const aws_error& error, uint32_t attempted_retries) const override;

    [[nodiscard]] uint32_t get_max_retries() const override { return _max_retries; }
};
} // namespace aws
