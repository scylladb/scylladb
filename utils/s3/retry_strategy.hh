/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <chrono>

namespace aws {

class aws_error;

class retry_strategy {
public:
    virtual ~retry_strategy() = default;
    // Returns true if the error can be retried given the error and the number of times already tried.
    [[nodiscard]] virtual bool should_retry(const aws_error& error, unsigned attempted_retries) const = 0;

    // Calculates the time in milliseconds the client should wait before attempting another request based on the error and attemptedRetries count.
    [[nodiscard]] virtual std::chrono::milliseconds delay_before_retry(const aws_error& error, unsigned attempted_retries) const = 0;

    [[nodiscard]] virtual unsigned get_max_retries() const = 0;
};

class default_retry_strategy : public retry_strategy {
    unsigned _max_retries;
    unsigned _scale_factor;

public:
    explicit default_retry_strategy(unsigned max_retries = 10, unsigned scale_factor = 25);

    [[nodiscard]] bool should_retry(const aws_error& error, unsigned attempted_retries) const override;

    [[nodiscard]] std::chrono::milliseconds delay_before_retry(const aws_error& error, unsigned attempted_retries) const override;

    [[nodiscard]] unsigned get_max_retries() const override { return _max_retries; }
};

} // namespace aws
