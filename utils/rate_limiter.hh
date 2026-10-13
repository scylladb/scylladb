/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/timer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/lowres_clock.hh>
#include "seastarx.hh"

namespace utils {

/**
 * 100% naive rate limiter. Consider it a placeholder
 * Will let you process X "units" per second, then reset this every s.
 * Obviously, accuracy is virtually non-existent and steady rate will fluctuate.
 */
class rate_limiter {
private:
    timer<lowres_clock> _timer;
    size_t _units_per_s;
    basic_semaphore<semaphore_default_exception_factory, lowres_clock> _sem {0};

    void on_timer();
public:
    rate_limiter(size_t rate);
    // Waits until u units fit in the budget. Fails with semaphore_timed_out
    // if that has not happened by timeout.
    future<> reserve(size_t u, lowres_clock::time_point timeout = lowres_clock::time_point::max());
};

}
