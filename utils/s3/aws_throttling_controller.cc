/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/s3/aws_throttling_controller.hh"

#include "utils/log.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <random>

namespace s3 {

extern logging::logger s3l;

// Uniform in [lo, hi].
static seastar::lowres_clock::duration freeze_duration(seastar::lowres_clock::duration lo, seastar::lowres_clock::duration hi) {
    thread_local std::mt19937 engine{std::random_device{}()};
    std::uniform_int_distribution dist{lo.count(), hi.count()};
    return seastar::lowres_clock::duration(dist(engine));
}

static double to_seconds(seastar::lowres_clock::time_point tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count() / 1e3;
}

// Starts enabled at seed_rate, pacing from the first request. The state is set
// here rather than through update_client_sending_rate() because that clamps the
// rate to twice the measured one, which is zero before anything has been sent.
// The seed also stands in as the last-known-good rate and as the initial measured
// rate, so the CUBIC curve has somewhere to grow from.
aws_throttling_controller::aws_throttling_controller() : _last_timestamp(seastar::lowres_clock::now()) {
}

aws_throttling_controller::aws_throttling_controller(double seed_rate)
    : _fill_rate(std::max(seed_rate, min_fill_rate))
    , _max_capacity(std::max(seed_rate, min_capacity))
    , _current_capacity(_max_capacity)
    , _measured_tx_rate(_fill_rate)
    , _enabled(true)
    , _last_max_rate(_fill_rate) {
    auto now = seastar::lowres_clock::now();
    _last_timestamp = now;
    _last_throttle_time = now;
    // Open the current measurement bucket, so the first computed rate spans it
    // rather than all the time since the clock's epoch.
    _last_tx_rate_bucket = std::floor(to_seconds(now) * 2.0) / 2.0;
}

// One request costs one token. If fewer than one is available, sleep for the time
// needed to accrue the shortfall. Loops because a sleep may wake early.
seastar::future<> aws_throttling_controller::acquire(seastar::abort_source* as) {
    if (!_enabled) {
        co_return;
    }

    // Freeze first: it means no requests at all, which a fill rate cannot express.
    if (auto now = seastar::lowres_clock::now(); now < _frozen_until) {
        auto d = _frozen_until - now;
        if (as) {
            co_await seastar::sleep_abortable(d, *as);
        } else {
            co_await seastar::sleep(d);
        }
    }

    refill(seastar::lowres_clock::now());
    while (1.0 > _current_capacity) {
        std::chrono::duration<double> wait_time((1.0 - _current_capacity) / _fill_rate);
        auto d = std::chrono::duration_cast<seastar::lowres_clock::duration>(wait_time);
        if (as) {
            co_await seastar::sleep_abortable(d, *as);
        } else {
            co_await seastar::sleep(d);
        }
        refill(seastar::lowres_clock::now());
    }
    _current_capacity -= 1.0;
}

// Adds fill_rate * elapsed tokens, capped at the ceiling. Both constructors open
// the observation window and the clock is monotonic, so elapsed is never negative.
void aws_throttling_controller::refill(seastar::lowres_clock::time_point now) {
    double elapsed = std::chrono::duration<double>(now - _last_timestamp).count();
    double fill_amount = elapsed * _fill_rate;
    _current_capacity = std::min(_max_capacity, _current_capacity + fill_amount);
    _last_timestamp = now;
}

// Settles pending tokens at the old rate, then adopts the new one. Ceiling equals
// fill rate, so the bucket holds at most a second of budget.
void aws_throttling_controller::update_rate(double new_rps, seastar::lowres_clock::time_point now) {
    refill(now);
    _fill_rate = std::max(new_rps, min_fill_rate);
    _max_capacity = std::max(new_rps, min_capacity);
    _current_capacity = std::min(_current_capacity, _max_capacity);
}

// Achieved request rate, counted in half-second buckets and smoothed into an EMA.
void aws_throttling_controller::update_measured_rate(seastar::lowres_clock::time_point now) {
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

void aws_throttling_controller::on_throttled() {
    ++_throttles;
    update_client_sending_rate(true);

    // Stop admitting for a short interval; lowering the fill rate alone still sends.
    const auto now = seastar::lowres_clock::now();
    if (now < _frozen_until) {
        // Already frozen. Extending on every response would never let go.
        return;
    }
    if (_last_freeze_end != seastar::lowres_clock::time_point{} && now - _last_freeze_end < freeze_min_gap) {
        // Inside the quiet gap that bounds the duty cycle.
        return;
    }

    const auto d = freeze_duration(freeze_min, freeze_max);
    _frozen_until = now + d;
    _last_freeze_end = _frozen_until;
    ++_freezes;

    // Drop accrued tokens so the resume is paced by the reduced fill rate rather
    // than firing a full bucket.
    _current_capacity = 0.0;

    // warn, because the s3 logger runs at warn by default and this needs to be
    // countable in a run. freeze_min_gap bounds how often it can fire.
    s3l.warn("froze sending for {} ms after a throttling response (freeze #{}, fill_rate {:.2f}/s)",
             std::chrono::duration_cast<std::chrono::milliseconds>(d).count(),
             _freezes,
             _fill_rate);
}

void aws_throttling_controller::on_success() {
    // Returns exactly what an admitted retry spent, so a request that retried and
    // then succeeded is net zero and only retries that end in failure drain the pool.
    _retry_quota = std::min(_retry_quota + 1, initial_retry_quota);
    update_client_sending_rate(false);
}

void aws_throttling_controller::on_error_not_throttled() {
    // Not throttled, so the curve grows as on success -- but it is not a success,
    // so no quota is returned.
    update_client_sending_rate(false);
}

bool aws_throttling_controller::try_acquire_retry_quota() {
    if (_retry_quota == 0) {
        ++_quota_denials;
        return false;
    }
    --_retry_quota;
    return true;
}

// Recomputes the target rate from one response. A throttle records the current
// rate as the point where congestion occurred, cuts by beta and arms the limiter;
// a success grows along the cubic curve. Either way the result is clamped to twice
// the measured rate, so the allowance cannot run far ahead of what is being sent.
void aws_throttling_controller::update_client_sending_rate(bool is_throttling_response) {
    auto now = seastar::lowres_clock::now();
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

// Seconds after a throttle at which the curve returns to last_max_rate:
//   W = cbrt((last_max_rate * (1 - beta)) / scale_constant)
double aws_throttling_controller::calculate_time_window() const {
    return std::pow(((_last_max_rate * (1.0 - beta)) / scale_constant), (1.0 / 3));
}

// rate = scale_constant * (t - W)^3 + last_max_rate, for t seconds since the last
// throttle. Approaches last_max cautiously, then probes above it faster.
double aws_throttling_controller::cubic_success(seastar::lowres_clock::time_point now, double time_window) const {
    double dt = std::chrono::duration<double>(now - _last_throttle_time).count();
    return scale_constant * std::pow(dt - time_window, 3.0) + _last_max_rate;
}

// Multiplicative decrease on throttling: rate = rate * beta (beta < 1).
double aws_throttling_controller::cubic_throttle(double rate_to_use) const {
    return rate_to_use * beta;
}

} // namespace s3
