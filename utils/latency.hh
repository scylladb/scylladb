/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <chrono>
/**
 * A helper class to keep track of latencies
 */
namespace utils {

class latency_counter {
public:
    using time_point = std::chrono::system_clock::time_point;
    using duration = std::chrono::system_clock::duration;
private:
    time_point _start;
    time_point _stop;
public:
    void start() {
        _start = now();
    }

    bool is_start() const {
        // if start is not set it is still zero
        return _start.time_since_epoch().count();
    }
    latency_counter& stop() {
        _stop = now();
        return *this;
    }

    bool is_stopped() const {
        // if stop was not set, it is still zero
        return _stop.time_since_epoch().count();
    }

    duration latency() const {
        return _stop - _start;
    }

    latency_counter& check_and_stop() {
        if (!is_stopped()) {
            return stop();
        }
        return *this;
    }

    int64_t latency_in_nano() const {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(latency()).count();
    }

    static time_point now() {
        return std::chrono::system_clock::now();
    }
};

}
