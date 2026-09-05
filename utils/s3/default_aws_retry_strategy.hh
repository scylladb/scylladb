/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/core/abort_source.hh>
#include <seastar/http/retry_strategy.hh>
#include <chrono>

namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::retry_strategy {
protected:
    unsigned _max_retries;
    std::chrono::milliseconds _max_sleep_time;
    seastar::abort_source* _as;

public:
    // max_sleep_time must be non-negative (0 disables backoff sleep); asserts otherwise.
    explicit default_aws_retry_strategy(unsigned max_retries = 10,
            std::chrono::milliseconds max_sleep_time = std::chrono::milliseconds(5000),
            seastar::abort_source* as = nullptr);

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
