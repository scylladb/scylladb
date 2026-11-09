/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/http/retry_strategy.hh>

#include "utils/s3/noop_throttling_controller.hh"

namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::retry_strategy {
protected:
    unsigned _max_retries;
    s3::throttling_controller& _controller;

public:
    // Named rather than a bare literal in the default argument, so that anything
    // wanting to honour the same ceiling can refer to it.
    static constexpr unsigned default_max_retries = 10;

    // Defaults to a controller that does nothing, so the strategy never has to
    // ask whether it has one.
    static s3::throttling_controller& no_throttling();

    default_aws_retry_strategy(unsigned max_retries = default_max_retries, s3::throttling_controller& controller = no_throttling());

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

// Keeps the connection after an error reply the handler parsed, which means it read the
// whole body. A transport failure maps to NETWORK_CONNECTION, or to UNKNOWN when it is
// not retryable.
class aws_connection_reset_policy final : public seastar::http::connection_reset_policy {
public:
    [[nodiscard]] bool should_reset(std::exception_ptr error) const override;
};

} // namespace aws
