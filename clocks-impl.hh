/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>

extern std::atomic<int64_t> clocks_offset;

template<typename Duration>
inline void forward_jump_clocks(Duration delta)
{
    auto d = std::chrono::duration_cast<std::chrono::seconds>(delta).count();
    clocks_offset.fetch_add(d, std::memory_order_relaxed);
}

// Unlike forward_jump_clocks(), which moves the clocks by a relative amount,
// this sets the offset to an absolute value - so the caller can also undo a
// previous jump, by setting the offset back to 0.
inline void set_clocks_offset(std::chrono::seconds off)
{
    clocks_offset.store(off.count(), std::memory_order_relaxed);
}

inline std::chrono::seconds get_clocks_offset()
{
    auto off = clocks_offset.load(std::memory_order_relaxed);
    return std::chrono::seconds(off);
}

// Returns a time point which is earlier from t by d, or minimum time point if it cannot be represented.
template<typename Clock, typename Duration, typename Rep, typename Period>
inline
auto saturating_subtract(std::chrono::time_point<Clock, Duration> t, std::chrono::duration<Rep, Period> d) -> decltype(t) {
    return std::max(t, decltype(t)::min() + d) - d;
}
