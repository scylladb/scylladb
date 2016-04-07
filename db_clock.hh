/*
 * Copyright (C) 2014 ScyllaDB
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

#ifndef DB_CLOCK_HH_
#define DB_CLOCK_HH_

#include <chrono>
#include <cstdint>
#include "gc_clock.hh"

// the database clock follows Java - 1ms granularity, 64-bit counter, 1970 epoch

class db_clock {
public:
    using base = std::chrono::system_clock;
    using rep = int64_t;
    using period = std::ratio<1, 1000>; // milliseconds
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<db_clock, duration>;

    static constexpr bool is_steady = base::is_steady;
    static std::time_t to_time_t(time_point t) {
        return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
    }
    static time_point from_time_t(std::time_t t) {
        return time_point(std::chrono::duration_cast<duration>(std::chrono::seconds(t)));
    }
    static time_point now() {
        auto now_since_epoch = base::now() - base::from_time_t(0);
        return time_point(std::chrono::duration_cast<duration>(now_since_epoch)) + get_clocks_offset();
    }
    static uint64_t now_in_usecs() {
        return now().time_since_epoch().count() * 1000;
    }
};

static inline
gc_clock::time_point to_gc_clock(db_clock::time_point tp) {
    static_assert(std::is_same<db_clock::base, gc_clock::base>::value, "Below we assume that base is the same");
    return gc_clock::time_point(std::chrono::duration_cast<gc_clock::duration>(tp.time_since_epoch()));
}

#endif /* DB_CLOCK_HH_ */
