/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "clocks-impl.hh"
#include "gc_clock.hh"

#include <chrono>
#include <cstdint>
#include <ratio>
#include <type_traits>
#include <fmt/chrono.h>

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
    static time_point now() noexcept {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch())) + get_clocks_offset();
    }
};

static inline
gc_clock::time_point to_gc_clock(db_clock::time_point tp) noexcept {
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
template <>
struct fmt::formatter<db_clock::time_point> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db_clock::time_point& tp, FormatContext& ctx) const {
        auto t = db_clock::to_time_t(tp);
        return fmt::format_to(ctx.out(), "{:%Y/%m/%d %T}", fmt::gmtime(t));
    }
};
