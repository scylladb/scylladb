/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <chrono>
#include <ostream>

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

inline std::ostream& operator<<(std::ostream& os, const logical_clock::time_point& p) {
    return os << (p - logical_clock::min()).count();
}

} // end of namespace raft

namespace std {

inline std::ostream& operator<<(std::ostream& os, const raft::logical_clock::duration& d) {
    return os << d.count();
}

} // end of namespace std
