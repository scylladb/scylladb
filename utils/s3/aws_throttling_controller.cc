/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/s3/aws_throttling_controller.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <chrono>

namespace s3 {

bool aws_throttling_controller::answers_pre_freeze_request() const {
    return seastar::lowres_clock::now() < _frozen_until;
}

void aws_throttling_controller::close_sample() {
    if (_sample_outcomes < outcomes_per_sample) {
        return;
    }

    const double sample = static_cast<double>(_sample_throttles) / static_cast<double>(_sample_outcomes);
    _refused_ratio = _refused_ratio * ratio_ema_factor + sample * (1.0 - ratio_ema_factor);

    _sample_outcomes = 0;
    _sample_throttles = 0;
}

// Holds the request back for the remainder of a freeze, and otherwise admits it
// immediately. Loops rather than sleeping once, because a sleep may wake early and
// this wait is the only thing that holds a request back.
seastar::future<> aws_throttling_controller::acquire(seastar::abort_source* as) {
    while (true) {
        const auto now = seastar::lowres_clock::now();
        if (now >= _frozen_until) {
            co_return;
        }
        const auto d = _frozen_until - now;
        if (as) {
            co_await seastar::sleep_abortable(d, *as);
        } else {
            co_await seastar::sleep(d);
        }
    }
}

void aws_throttling_controller::on_not_throttled() {
    if (answers_pre_freeze_request()) {
        return;
    }

    ++_sample_outcomes;
    // Never arms a freeze, even when this sample carries the estimate over the
    // threshold: while the endpoint keeps refusing, the next refusal arrives and arms it
    // there, and if it has stopped there is no longer anything to brake.
    close_sample();
}

void aws_throttling_controller::on_throttled() {
    ++_throttles;

    if (answers_pre_freeze_request()) {
        return;
    }

    ++_sample_outcomes;
    ++_sample_throttles;
    close_sample();

    if (_refused_ratio <= ratio_threshold) {
        return;
    }

    _frozen_until = seastar::lowres_clock::now() + freeze_duration;
    ++_freezes;

    // Nothing is sent while frozen, so the estimate that justified this freeze cannot
    // be confirmed or contradicted until it lifts. Rebuild it from what happens then.
    _refused_ratio = 0.0;
    _sample_outcomes = 0;
    _sample_throttles = 0;
}

} // namespace s3
