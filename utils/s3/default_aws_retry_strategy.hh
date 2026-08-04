/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/http/retry_strategy.hh>

namespace s3 {
class throttling_controller;
}
namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::retry_strategy {
protected:
    unsigned _max_retries;
    s3::throttling_controller* _controller;

public:
    // Named rather than a bare literal in the default argument, so that anything
    // wanting to honour the same ceiling can refer to it.
    static constexpr unsigned default_max_retries = 10;

    default_aws_retry_strategy(unsigned max_retries = default_max_retries, s3::throttling_controller* controller = nullptr);

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
