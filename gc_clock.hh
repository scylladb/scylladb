/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <chrono>
#include <experimental/optional>

// FIXME: wraps around in 2038
class gc_clock {
public:
    using base = std::chrono::system_clock;
    using rep = int32_t;
    using period = std::ratio<1, 1>; // seconds
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<gc_clock, duration>;

    static time_point now() {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch()));
    }
};


using ttl_opt = std::experimental::optional<gc_clock::time_point>;

