/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>

extern std::atomic<int64_t> clocks_offset;

template<typename Duration>
static inline void forward_jump_clocks(Duration delta)
{
    auto d = std::chrono::duration_cast<std::chrono::seconds>(delta).count();
    clocks_offset.fetch_add(d, std::memory_order_relaxed);
}

static inline std::chrono::seconds get_clocks_offset()
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
