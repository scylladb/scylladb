/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/s3/throttling_controller.hh"

#include <seastar/core/lowres_clock.hh>
#include <cstddef>

namespace s3 {

// Per-shard adaptive client-side send-rate limiter.
//
// This is a Seastar-native port of the AWS C++ SDK's adaptive retry token
// bucket (see aws-sdk-cpp AdaptiveRetryStrategy.cpp). It implements the CUBIC
// congestion-control algorithm on the client send rate: on a throttling
// response (S3 503 SlowDown and friends) the allowed request rate is cut
// multiplicatively; on success it grows back along a cubic curve toward the
// last-known-good rate, then probes beyond it.
//
// The limiter starts disabled and stays a no-op until the first throttling
// response is observed, so there is zero overhead until S3 actually pushes
// back. It is single-shard: there is no locking, and the blocking Acquire()
// of the original becomes a future-based wait.
//
// All AWS default constants are preserved verbatim.
class aws_throttling_controller final : public throttling_controller {
    // AWS SDK defaults (AdaptiveRetryStrategy.cpp).
    static constexpr double min_fill_rate = 0.5;
    static constexpr double min_capacity = 1.0;
    static constexpr double smooth = 0.8;
    static constexpr double beta = 0.7;
    static constexpr double scale_constant = 0.4;

    using clock = seastar::lowres_clock;

    double _fill_rate = 0.0;        // tokens (requests) added per second
    double _max_capacity = 0.0;     // token bucket ceiling
    double _current_capacity = 0.0; // available tokens
    clock::time_point _last_timestamp{};
    bool _has_last_timestamp = false;

    double _measured_tx_rate = 0.0; // EMA-smoothed measured send rate (rps)
    double _last_tx_rate_bucket = 0.0;
    size_t _request_count = 0;

    bool _enabled = false;
    double _last_max_rate = 0.0;
    clock::time_point _last_throttle_time{};

    void refill(clock::time_point now);
    void update_rate(double new_rps, clock::time_point now);
    void update_measured_rate(clock::time_point now);
    double calculate_time_window() const;
    double cubic_success(clock::time_point now, double time_window) const;
    double cubic_throttle(double rate_to_use) const;

    static double to_seconds(clock::time_point tp);

public:
    aws_throttling_controller() = default;

    seastar::future<> acquire(seastar::abort_source* as = nullptr) override;
    void update_client_sending_rate(bool is_throttling_response) override;

    bool enabled() const override { return _enabled; }
    double fill_rate() const override { return _fill_rate; }
    double measured_tx_rate() const override { return _measured_tx_rate; }
};

} // namespace s3
