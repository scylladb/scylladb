/*
 * Copyright (C) 2017 ScyllaDB
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

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <experimental/string_view>
#include <ostream>
#include <stdexcept>

// Wrapper for a value with a type-tag for differentiating instances.
template <class Value, class Tag>
class cql_duration_counter final {
public:
    using value_type = Value;

    explicit constexpr cql_duration_counter(value_type count) noexcept : _count(count) {}

    constexpr operator value_type() const noexcept { return _count; }
private:
    value_type _count;
};

using months_counter = cql_duration_counter<int32_t, struct month_tag>;
using days_counter = cql_duration_counter<int32_t, struct day_tag>;
using nanoseconds_counter = cql_duration_counter<int64_t, struct nanosecond_tag>;

class cql_duration_error : public std::invalid_argument {
public:
    explicit cql_duration_error(std::experimental::string_view what) : std::invalid_argument(what.data()) {}

    virtual ~cql_duration_error() = default;
};

//
// A duration of time.
//
// Three counters represent the time: the number of months, of days, and of nanoseconds. This is necessary because
// the number hours in a day can vary during daylight savings and because the number of days in a month vary.
//
// As a consequence of this representation, there can exist no total ordering relation on durations. To see why,
// consider a duration `1mo5s` (1 month and 5 seconds). In a month with 30 days, this represents a smaller duration of
// time than in a month with 31 days.
//
// The primary use of this type is to manipulate absolute time-stamps with relative offsets. For example,
// `"Jan. 31 2005 at 23:15" + 3mo5d`.
//
class cql_duration final {
public:
    using common_counter_type = int64_t;

    static_assert(
            (sizeof(common_counter_type) >= sizeof(months_counter::value_type)) &&
            (sizeof(common_counter_type) >= sizeof(days_counter::value_type)) &&
            (sizeof(common_counter_type) >= sizeof(nanoseconds_counter::value_type)),
            "The common counter type is smaller than one of the component counter types.");

    // A zero-valued duration.
    constexpr cql_duration() noexcept = default;

    // Construct a duration with explicit values for its three counters.
    constexpr cql_duration(months_counter m, days_counter d, nanoseconds_counter n) noexcept :
            months(m),
            days(d),
            nanoseconds(n) {}

    //
    // Parse a duration string.
    //
    // Three formats for durations are supported:
    //
    // 1. "Standard" format. This consists of one or more pairs of a count and a unit specifier. Examples are "23d1mo"
    //    and "5h23m10s". Components of the total duration must be written in decreasing order. That is, "5h2y" is
    //    an invalid duration string.
    //
    //    The allowed units are:
    //      - "y": years
    //      - "mo": months
    //      - "w": weeks
    //      - "d": days
    //      - "h": hours
    //      - "m": minutes
    //      - "s": seconds
    //      - "ms": milliseconds
    //      - "us" or "µs": microseconds
    //      - "ns": nanoseconds
    //
    //    Units are case-insensitive.
    //
    // 2. ISO-8601 format. "P[n]Y[n]M[n]DT[n]H[n]M[n]S" or "P[n]W". All specifiers are optional. Examples are
    //    "P23Y1M" or "P10W".
    //
    // 3. ISO-8601 alternate format. "P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]". All specifiers are mandatory. An example is
    //    "P2000-10-14T07:22:30".
    //
    // For all formats, a negative duration is indicated by beginning the string with the '-' symbol. For example,
    // "-2y10ns".
    //
    // Throws `cql_duration_error` in the event of a parsing error.
    //
    explicit cql_duration(std::experimental::string_view s);

    months_counter::value_type months{0};
    days_counter::value_type days{0};
    nanoseconds_counter::value_type nanoseconds{0};
};

//
// Pretty-print a duration using the standard format.
//
// Durations are simplified during printing so that `duration(24, 0, 0)` is printed as "2y".
//
std::ostream& operator<<(std::ostream& os, const cql_duration& d);

// See above.
seastar::sstring to_string(const cql_duration&);

//
// Note that equality comparison is based on exact counter matches. It is not valid to expect equivalency across
// counters like months and days. See the documentation for `duration` for more.
//

bool operator==(const cql_duration&, const cql_duration&) noexcept;
bool operator!=(const cql_duration&, const cql_duration&) noexcept;
