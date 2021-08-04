/*
 * Copyright (C) 2021-present ScyllaDB
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

// A coarser and faster version of std::steady_clock, using
// CLOCK_MONOTONIC_COARSE instead of CLOCK_MONOTONIC.
//
// Intended for measuring time taken by synchronous code paths (where
// seastar::lowres_clock is not suitable).

#include <chrono>
#include <ctime>

namespace utils {

struct coarse_steady_clock {
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    using time_point = std::chrono::time_point<coarse_steady_clock, duration>;

    static constexpr bool is_steady = true;

    static time_point now() noexcept {
        timespec tp;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &tp);
        return time_point(std::chrono::seconds(tp.tv_sec) + std::chrono::nanoseconds(tp.tv_nsec));
    };
};

};
