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
#include <chrono>

namespace s3 {

extern logging::logger s3l;

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

void aws_throttling_controller::on_throttled() {
    ++_throttles;

    // Stop admitting for a short interval. One test covers the two cases that must
    // not re-arm: while the freeze runs, now - _frozen_until is negative and so below
    // the gap; once it ends, the gap keeps the next one away. Extending on every
    // response would never let go, and an episode delivers hundreds per second.
    const auto now = seastar::lowres_clock::now();
    if (_frozen_until != seastar::lowres_clock::time_point{} && now - _frozen_until < freeze_min_gap) {
        return;
    }

    _frozen_until = now + freeze_duration;
    ++_freezes;

    // Countable through the send_freezes metric, so this is context for a human
    // reading a log rather than the measurement itself.
    s3l.info("froze sending for {} ms after a throttling response (freeze #{})",
             std::chrono::duration_cast<std::chrono::milliseconds>(freeze_duration).count(),
             _freezes);
}

} // namespace s3
