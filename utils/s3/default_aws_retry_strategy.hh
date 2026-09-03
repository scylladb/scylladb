/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/http/retry_strategy.hh>
#include <chrono>

namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::retry_strategy {
protected:
    const unsigned _max_retries;
    const std::chrono::milliseconds _min_sleep_time;
    const std::chrono::milliseconds _max_sleep_time;

public:
    // sleep times must be non-negative (asserts); min_sleep_time and max_retries are clamped down, not asserted.
    explicit default_aws_retry_strategy(unsigned max_retries = 10,
            std::chrono::milliseconds max_sleep_time = std::chrono::milliseconds(5000),
            std::chrono::milliseconds min_sleep_time = std::chrono::milliseconds(10));

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;

    // should_retry()'s sleep duration, computed without sleeping. Exposed for unit testing.
    std::chrono::milliseconds compute_sleep_duration(unsigned attempted_retries) const;
};

} // namespace aws
