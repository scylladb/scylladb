/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/timer.hh"
#include "core/semaphore.hh"
#include "core/reactor.hh"

namespace utils {

/**
 * 100% naive rate limiter. Consider it a placeholder
 * Will let you process X "units" per second, then reset this every s.
 * Obviously, accuracy is virtually non-existant and steady rate will fluctuate.
 */
class rate_limiter {
private:
    timer<lowres_clock> _timer;
    size_t _units_per_s;
    semaphore _sem;

    void on_timer();
public:
    rate_limiter(size_t rate);
    future<> reserve(size_t u);
};

}
