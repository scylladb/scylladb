/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <functional>
#include <memory>

namespace s3 {

// Per-shard client-side send-rate limiter interface.
//
// A throttling controller sits in front of request dispatch: acquire() is
// awaited before a request is sent, and update_client_sending_rate() feeds
// back whether the response was a throttling response (S3 503 SlowDown and
// friends) or a success. Implementations decide whether and how much to slow
// the client down.
//
// See aws_throttling_controller for the production AWS-CUBIC
// implementation, and noop_throttling_controller for a do-nothing variant
// used by tests that must not interfere with the client send rate.
class throttling_controller {
public:
    virtual ~throttling_controller() = default;

    // Acquire one token before dispatching a request. If an abort source is
    // provided, the wait (if any) is abortable and will resolve with
    // seastar::sleep_aborted when the source is triggered.
    virtual seastar::future<> acquire(seastar::abort_source* as = nullptr) = 0;

    // Feed back the outcome of a request. Pass true for throttling responses
    // (503 SlowDown etc.), false for success.
    virtual void update_client_sending_rate(bool is_throttling_response) = 0;

    // Observability accessors.
    virtual bool enabled() const = 0;
    virtual double fill_rate() const = 0;
    virtual double measured_tx_rate() const = 0;
};

// Factory used by the client to construct a throttling controller per
// group_client (per scheduling group). Defaults to the adaptive controller;
// tests may inject a factory producing noop_throttling_controller.
using throttling_controller_factory = std::function<std::unique_ptr<throttling_controller>()>;

} // namespace s3
