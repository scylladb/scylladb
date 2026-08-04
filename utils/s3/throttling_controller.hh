/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <cstdint>
#include <functional>
#include <memory>

namespace s3 {

// Per-shard client-side send-rate limiter. acquire() is awaited before a request
// is dispatched; the on_*() methods report how the response came back.
class throttling_controller {
public:
    virtual ~throttling_controller() = default;

    // Waits until the client may send. With an abort source the wait resolves
    // with seastar::sleep_aborted when it is triggered.
    virtual seastar::future<> acquire(seastar::abort_source* as) = 0;

    // Outcome of a completed request: the endpoint asked us to slow down, or the
    // request went through.
    virtual void on_throttled() = 0;
    virtual void on_success() = 0;

    // For metrics.
    virtual bool enabled() const = 0;
    virtual double fill_rate() const = 0;
    virtual double measured_tx_rate() const = 0;
    virtual uint64_t throttles() const = 0;
};

// Builds the controller a client owns. The client defaults to the adaptive one;
// tests inject a factory returning noop_throttling_controller.
using throttling_controller_factory = std::function<std::unique_ptr<throttling_controller>()>;

} // namespace s3
