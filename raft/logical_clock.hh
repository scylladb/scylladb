/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <chrono>
#include <fmt/chrono.h>

namespace raft {

// Raft protocol state machine clock ticks at different speeds
// depending on the environment. A typical clock tick for
// a production system is 100ms, while a test system can
// tick it at the speed of the hardware clock.
//
// Every state machine has an own instance of logical clock,
// this enables tests when different state machines run at
// different clock speeds.
class logical_clock final {
public:
    using rep = int64_t;
    // There is no realistic period for a logical clock,
    // just use the smallest period possible.
    using period = std::chrono::nanoseconds::period;
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<logical_clock, duration>;

    static constexpr bool is_steady = true;

    void advance(duration diff = duration{1}) {
        _now += diff;
    }
    time_point now() const noexcept {
        return _now;
    }

    static constexpr time_point min() {
        return time_point(duration{0});
    }
private:
    time_point _now = min();
};

} // end of namespace raft

template <>
struct fmt::formatter<raft::logical_clock::time_point> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const raft::logical_clock::time_point& p, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", (p - raft::logical_clock::min()).count());
    }
};
