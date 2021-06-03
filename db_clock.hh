/*
 * Copyright (C) 2014-present ScyllaDB
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

#include "clocks-impl.hh"
#include "gc_clock.hh"

#include <chrono>
#include <cstdint>
#include <ratio>
#include <type_traits>

// the database clock follows Java - 1ms granularity, 64-bit counter, 1970 epoch

class db_clock final {
public:
    using base = std::chrono::system_clock;
    using rep = int64_t;
    using period = std::ratio<1, 1000>; // milliseconds
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<db_clock, duration>;

    static constexpr bool is_steady = base::is_steady;
    static constexpr std::time_t to_time_t(time_point t) {
        return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
    }
    static constexpr time_point from_time_t(std::time_t t) {
        return time_point(std::chrono::duration_cast<duration>(std::chrono::seconds(t)));
    }
    static time_point now() {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch())) + get_clocks_offset();
    }
};

static inline
gc_clock::time_point to_gc_clock(db_clock::time_point tp) {
    // Converting time points through `std::time_t` means that we don't have to make any assumptions about the epochs
    // of `gc_clock` and `db_clock`, though we require that that the period of `gc_clock` is also 1 s like
    // `std::time_t` to avoid loss of information.
    {
        using second = std::ratio<1, 1>;
        static_assert(
                std::is_same<gc_clock::period, second>::value,
                "Conversion via std::time_t would lose information.");
    }

    return gc_clock::from_time_t(db_clock::to_time_t(tp));
}

/* For debugging and log messages. */
std::ostream& operator<<(std::ostream&, db_clock::time_point);
