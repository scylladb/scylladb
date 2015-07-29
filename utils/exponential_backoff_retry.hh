
/*
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "core/sleep.hh"
#include <chrono>

// Implements retry policy that exponentially increases sleep time between retries.
class exponential_backoff_retry {
    std::chrono::milliseconds _base_sleep_time;
    std::chrono::milliseconds _sleep_time;
    std::chrono::milliseconds _max_sleep_time;
public:
    exponential_backoff_retry(std::chrono::milliseconds base_sleep_time, std::chrono::milliseconds max_sleep_time)
        : _base_sleep_time(std::min(base_sleep_time, max_sleep_time))
        , _sleep_time(_base_sleep_time)
        , _max_sleep_time(max_sleep_time) {}

    future<> retry() {
        auto old_sleep_time = _sleep_time;
        // calculating sleep time seconds for the next retry.
        _sleep_time = std::min(_sleep_time * 2, _max_sleep_time);

        return sleep(old_sleep_time);
    }

    // Return sleep time in seconds to be used for next retry.
    std::chrono::milliseconds sleep_time() const {
        return _sleep_time;
    }

    // Reset sleep time to base sleep time.
    void reset() {
        _sleep_time = _base_sleep_time;
    }
};
