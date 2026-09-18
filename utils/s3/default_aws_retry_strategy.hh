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

// Reports, backs off and waits on the send brake exactly as default_aws_retry_strategy
// does, and then refuses to retry: the chunked download fiber consumes the reply body
// as it arrives, so its request cannot be replayed at the transport layer. The fiber
// resumes from the offset it has consumed instead.
//
// One instance per request, taking the ladder index from the fiber: seastar only
// advances the count it passes on the branch that retries, which this never takes. The
// base keeps its own ceiling, so the attempt that exhausts the budget is reported but
// not paced -- there is no dispatch left for a backoff to space out.
class chunked_download_pacing_strategy final : public default_aws_retry_strategy {
    unsigned _current_retry;

public:
    explicit chunked_download_pacing_strategy(unsigned current_retry, s3::throttling_controller& controller)
        : default_aws_retry_strategy(default_max_retries, controller), _current_retry(current_retry) {}

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
