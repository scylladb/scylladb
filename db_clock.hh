/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DB_CLOCK_HH_
#define DB_CLOCK_HH_

#include <chrono>
#include <cstdint>

// the database clock follows Java - 1ms granularity, 64-bit counter, 1970 epoch

class db_clock {
    using base = std::chrono::system_clock;
public:
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
        return time_point(std::chrono::duration_cast<duration>(now_since_epoch));
    }
};

#endif /* DB_CLOCK_HH_ */
