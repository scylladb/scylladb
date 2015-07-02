/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <atomic>
#include <chrono>
#include <experimental/optional>

extern std::atomic<int64_t> clocks_offset;

static inline std::chrono::seconds get_clocks_offset()
{
    auto off = clocks_offset.load(std::memory_order_relaxed);
    return std::chrono::seconds(off);
}

// FIXME: wraps around in 2038
class gc_clock {
public:
    using base = std::chrono::system_clock;
    using rep = int32_t;
    using period = std::ratio<1, 1>; // seconds
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<gc_clock, duration>;

    static time_point now() {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch())) + get_clocks_offset();
    }

    // Intended for tests. Allows testing time related event without need
    // for real time waits.
};


using expiry_opt = std::experimental::optional<gc_clock::time_point>;
using ttl_opt = std::experimental::optional<gc_clock::duration>;

// 20 years in seconds
static constexpr gc_clock::duration max_ttl = gc_clock::duration{20 * 365 * 24 * 60 * 60};

template<typename Duration>
static inline void forward_jump_clocks(Duration delta)
{
    auto d = std::chrono::duration_cast<std::chrono::seconds>(delta).count();
    clocks_offset.fetch_add(d, std::memory_order_relaxed);
}