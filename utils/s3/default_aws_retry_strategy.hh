/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/core/abort_source.hh>
#include <seastar/http/retry_strategy.hh>

#include "utils/s3/noop_throttling_controller.hh"

namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::retry_strategy {
protected:
    unsigned _max_retries;
    s3::throttling_controller& _controller;
    // Bound at construction because should_retry() takes no abort_source of its own.
    seastar::abort_source* _as;

public:
    // Named rather than a bare literal in the default argument, so that anything
    // wanting to honour the same ceiling can refer to it.
    static constexpr unsigned default_max_retries = 10;

    // Defaults to a controller that does nothing, so the strategy never has to
    // ask whether it has one.
    static s3::throttling_controller& no_throttling();

    default_aws_retry_strategy(unsigned max_retries = default_max_retries, s3::throttling_controller& controller = no_throttling(), seastar::abort_source* as = nullptr);

    unsigned max_retries() const { return _max_retries; }
    s3::throttling_controller& controller() const { return _controller; }

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
