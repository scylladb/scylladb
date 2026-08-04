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

// Adaptive send-rate limiter: a token bucket whose fill rate is driven by the
// CUBIC algorithm. A throttling response cuts the rate multiplicatively; a
// success grows it back along a cubic curve towards the last rate that worked,
// then probes above it.
//
// Default-constructed, the limiter admits everything until the first throttling
// response; constructed with a seed rate it paces from the first request.
// Single-shard, so no locking.
class aws_throttling_controller final : public throttling_controller {
    static constexpr double min_fill_rate = 0.5;
    static constexpr double min_capacity = 1.0;
    static constexpr double smooth = 0.8;
    static constexpr double beta = 0.7;
    static constexpr double scale_constant = 0.4;

    double _fill_rate = 0.0;        // tokens (requests) added per second
    double _max_capacity = 0.0;     // token bucket ceiling
    double _current_capacity = 0.0; // available tokens
    seastar::lowres_clock::time_point _last_timestamp{};

    double _measured_tx_rate = 0.0; // EMA-smoothed measured send rate (rps)
    double _last_tx_rate_bucket = 0.0;
    size_t _request_count = 0;

    bool _enabled = false;
    double _last_max_rate = 0.0;
    seastar::lowres_clock::time_point _last_throttle_time{};

    uint64_t _throttles = 0; // throttling responses observed, for metrics only

    void refill(seastar::lowres_clock::time_point now);
    void update_client_sending_rate(bool is_throttling_response);
    void update_rate(double new_rps, seastar::lowres_clock::time_point now);
    void update_measured_rate(seastar::lowres_clock::time_point now);
    double calculate_time_window() const;
    double cubic_success(seastar::lowres_clock::time_point now, double time_window) const;
    double cubic_throttle(double rate_to_use) const;


public:
    aws_throttling_controller();

    // Seeded start: begin enabled at the given send rate (requests/s) instead
    // of waiting for the first throttling response to arrive.
    explicit aws_throttling_controller(double seed_rate);

    seastar::future<> acquire(seastar::abort_source* as) override;
    void on_throttled() override;
    void on_success() override;

    bool enabled() const override { return _enabled; }
    double fill_rate() const override { return _fill_rate; }
    double measured_tx_rate() const override { return _measured_tx_rate; }
    uint64_t throttles() const override { return _throttles; }
};

} // namespace s3
