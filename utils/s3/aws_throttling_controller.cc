/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/s3/aws_throttling_controller.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <algorithm>
#include <chrono>
#include <cmath>

namespace s3 {

double aws_throttling_controller::to_seconds(clock::time_point tp) {
    return std::chrono::duration<double>(tp.time_since_epoch()).count();
}

// Token-bucket admission: one request costs one token. Refill first, then if
// fewer than one token is available, sleep for exactly the time needed to
// accrue the shortfall at the current fill rate: (1 - capacity) / fill_rate
// seconds. Loop because the sleep may wake early (abort/timer granularity).
seastar::future<> aws_throttling_controller::acquire(seastar::abort_source* as) {
    if (!_enabled) {
        co_return;
    }
    refill(clock::now());
    while (1.0 > _current_capacity) {
        std::chrono::duration<double> wait_time((1.0 - _current_capacity) / _fill_rate);
        auto d = std::chrono::duration_cast<clock::duration>(wait_time);
        if (as) {
            co_await seastar::sleep_abortable(d, *as);
        } else {
            co_await seastar::sleep(d);
        }
        refill(clock::now());
    }
    _current_capacity -= 1.0;
}

// Continuous (leaky-bucket) refill: add fill_rate * elapsed_seconds tokens
// since the last observation, capped at the bucket ceiling. The first call
// just seeds the timestamp with no accrual.
void aws_throttling_controller::refill(clock::time_point now) {
    if (!_has_last_timestamp) {
        _last_timestamp = now;
        _has_last_timestamp = true;
        return;
    }
    double elapsed = std::chrono::duration<double>(now - _last_timestamp).count();
    double fill_amount = std::abs(elapsed) * _fill_rate;
    _current_capacity = std::min(_max_capacity, _current_capacity + fill_amount);
    _last_timestamp = now;
}

// Apply a newly computed target rate: settle pending tokens at the old rate
// first, then set both the fill rate and the bucket ceiling to the new rate
// (clamped to AWS minimums), and shrink current tokens to the new ceiling.
// Fill rate == capacity means the bucket holds at most ~1 second of budget.
void aws_throttling_controller::update_rate(double new_rps, clock::time_point now) {
    refill(now);
    _fill_rate = std::max(new_rps, min_fill_rate);
    _max_capacity = std::max(new_rps, min_capacity);
    _current_capacity = std::min(_current_capacity, _max_capacity);
}

// Estimate the actual achieved request rate, bucketed into 0.5-second windows.
// Each request increments a counter; when the wall clock crosses into a new
// half-second bucket, the counter over the elapsed bucket span gives an
// instantaneous rate, which is folded into an exponential moving average
// (weight `smooth`) to damp noise. This EMA feeds the CUBIC calculations.
void aws_throttling_controller::update_measured_rate(clock::time_point now) {
    double t = to_seconds(now);
    double time_bucket = std::floor(t * 2.0) / 2.0;
    _request_count += 1;
    if (time_bucket > _last_tx_rate_bucket) {
        double current_rate = _request_count / (time_bucket - _last_tx_rate_bucket);
        _measured_tx_rate = (current_rate * smooth) + (_measured_tx_rate * (1 - smooth));
        _request_count = 0;
        _last_tx_rate_bucket = time_bucket;
    }
}

// CUBIC control loop, driven once per response:
//  - On throttling: record the current rate as the new "last max" (the point
//    where congestion occurred) and cut multiplicatively by beta. This also
//    arms the controller on the first ever throttle.
//  - On success: grow along the cubic curve back toward last_max and beyond.
// The final rate is clamped to at most 2x the measured rate so the allowance
// never runs far ahead of what the client is actually able to send.
void aws_throttling_controller::update_client_sending_rate(bool is_throttling_response) {
    auto now = clock::now();
    update_measured_rate(now);

    double calculated_rate = 0.0;
    if (is_throttling_response) {
        double rate_to_use = _measured_tx_rate;
        if (_enabled) {
            rate_to_use = std::min(rate_to_use, _fill_rate);
        }

        _last_max_rate = rate_to_use;
        _last_throttle_time = now;

        calculated_rate = cubic_throttle(rate_to_use);
        _enabled = true;
    } else {
        double time_window = calculate_time_window();
        calculated_rate = cubic_success(now, time_window);
    }

    double new_rate = std::min(calculated_rate, 2.0 * _measured_tx_rate);
    update_rate(new_rate, now);
}

// CUBIC inflection point W (in seconds): the time after a throttle at which
// the cubic curve returns to last_max_rate. Derived from the CUBIC equation
// by solving for the offset where growth is zero:
//   W = cbrt( (last_max_rate * (1 - beta)) / scale_constant ).
double aws_throttling_controller::calculate_time_window() const {
    return std::pow(((_last_max_rate * (1.0 - beta)) / scale_constant), (1.0 / 3));
}

// CUBIC growth curve: rate = scale_constant * (t - W)^3 + last_max_rate,
// where t is seconds elapsed since the last throttle and W is the inflection
// point. Concave (cautious) approach below last_max, convex (fast probing)
// growth above it.
double aws_throttling_controller::cubic_success(clock::time_point now, double time_window) const {
    double dt = std::chrono::duration<double>(now - _last_throttle_time).count();
    return scale_constant * std::pow(dt - time_window, 3.0) + _last_max_rate;
}

// Multiplicative decrease on throttling: rate = rate * beta (beta < 1).
double aws_throttling_controller::cubic_throttle(double rate_to_use) const {
    return rate_to_use * beta;
}

} // namespace s3
