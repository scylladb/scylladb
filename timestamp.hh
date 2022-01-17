/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <limits>
#include <chrono>
#include <string>
#include "clocks-impl.hh"

namespace api {

using timestamp_type = int64_t;
timestamp_type constexpr missing_timestamp = std::numeric_limits<timestamp_type>::min();
timestamp_type constexpr min_timestamp = std::numeric_limits<timestamp_type>::min() + 1;
timestamp_type constexpr max_timestamp = std::numeric_limits<timestamp_type>::max();

// Used for generating server-side mutation timestamps.
// Same epoch as Java's System.currentTimeMillis() for compatibility.
// Satisfies requirements of Clock.
class timestamp_clock final {
    using base = std::chrono::system_clock;
public:
    using rep = timestamp_type;
    using duration = std::chrono::microseconds;
    using period = typename duration::period;
    using time_point = std::chrono::time_point<timestamp_clock, duration>;

    static constexpr bool is_steady = base::is_steady;

    static time_point now() {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch())) + get_clocks_offset();
    }
};

static inline
timestamp_type new_timestamp() {
    return timestamp_clock::now().time_since_epoch().count();
}

}

/* For debugging and log messages. */
std::string format_timestamp(api::timestamp_type);
