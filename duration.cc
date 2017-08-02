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

#include "duration.hh"

#include "stdx.hh"

#include <boost/lexical_cast.hpp>
#include <seastar/core/print.hh>

#include <cctype>
#include <experimental/optional>
#include <limits>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>

namespace {

//
// Helper for retrieving the counter based on knowing its type.
//
template<class Counter>
constexpr typename Counter::value_type& counter_ref(cql_duration &) noexcept;

template<>
constexpr months_counter::value_type& counter_ref<months_counter>(cql_duration &d) noexcept {
    return d.months;
}

template<>
constexpr days_counter::value_type& counter_ref<days_counter>(cql_duration &d) noexcept {
    return d.days;
}

template<>
constexpr nanoseconds_counter::value_type& counter_ref<nanoseconds_counter>(cql_duration &d) noexcept {
    return d.nanoseconds;
}

// Unit for a component of a duration. For example, years.
class duration_unit {
public:
    using index_type = uint8_t;
    using common_counter_type = cql_duration::common_counter_type;

    virtual ~duration_unit() = default;

    // Units with larger indicies are greater. For example, "months" have a greater index than "days".
    virtual index_type index() const noexcept = 0;

    virtual const char* short_name() const noexcept = 0;

    virtual const char* long_name() const noexcept = 0;

    // Increment the appropriate counter in the duration instance based on a count of this unit.
    virtual void increment_count(cql_duration&, common_counter_type) const noexcept = 0;

    // The remaining capacity (in terms of this unit) of the appropriate counter in the duration instance.
    virtual common_counter_type available_count(const cql_duration&) const noexcept = 0;
};

// `_index` is the assigned index of this unit.
// `Counter` is the counter type in the `cql_duration` instance that is used to store this unit.
// `_factor` is the conversion factor of one count of this unit to the corresponding count in `Counter`.
template <uint8_t _index, class Counter, cql_duration::common_counter_type _factor>
class duration_unit_impl : public duration_unit {
public:
    static constexpr auto factor = _factor;

    virtual ~duration_unit_impl() = default;

    index_type index() const noexcept override {
        return _index;
    }

    void increment_count(cql_duration &d, common_counter_type c) const noexcept override {
        counter_ref<Counter>(d) += (c * factor);
    }

    common_counter_type available_count(const cql_duration& d) const noexcept override {
        const auto limit = std::numeric_limits<typename Counter::value_type>::max();
        return {(limit - counter_ref<Counter>(const_cast<cql_duration&>(d))) / factor};
    }
};

struct nanosecond_unit final : public duration_unit_impl<0, nanoseconds_counter , 1> {
    const char* short_name() const noexcept override { return "ns"; }
    const char* long_name() const noexcept override { return "nanoseconds"; }
} const nanosecond{};

struct microsecond_unit final : public duration_unit_impl<1, nanoseconds_counter, 1000> {
    const char* short_name() const noexcept override { return "us"; }
    const char* long_name() const noexcept override { return "microseconds"; }
} const microsecond{};

struct millisecond_unit final : public duration_unit_impl<2, nanoseconds_counter, microsecond_unit::factor * 1000> {
    const char* short_name() const noexcept override { return "ms"; }
    const char* long_name() const noexcept override { return "milliseconds"; }
} const millisecond{};

struct second_unit final : public duration_unit_impl<3, nanoseconds_counter, millisecond_unit::factor * 1000> {
    const char* short_name() const noexcept override { return "s"; }
    const char* long_name() const noexcept override { return "seconds"; }
} const second{};

struct minute_unit final : public duration_unit_impl<4, nanoseconds_counter, second_unit::factor * 60> {
    const char* short_name() const noexcept override { return "m"; }
    const char* long_name() const noexcept override { return "minutes"; }
} const minute{};

struct hour_unit final : public duration_unit_impl<5, nanoseconds_counter, minute_unit::factor * 60> {
    const char* short_name() const noexcept override { return "h"; }
    const char* long_name() const noexcept override { return "hours"; }
} const hour{};

struct day_unit final : public duration_unit_impl<6, days_counter, 1> {
    const char* short_name() const noexcept override { return "d"; }
    const char* long_name() const noexcept override { return "days"; }
} const day{};

struct week_unit final : public duration_unit_impl<7, days_counter, 7> {
    const char* short_name() const noexcept override { return "w"; }
    const char* long_name() const noexcept override { return "weeks"; }
} const week{};

struct month_unit final : public duration_unit_impl<8, months_counter, 1> {
    const char* short_name() const noexcept override { return "mo"; }
    const char* long_name() const noexcept override { return "months"; }
} const month{};

struct year_unit final : public duration_unit_impl<9, months_counter, 12> {
    const char* short_name() const noexcept override { return "y"; }
    const char* long_name() const noexcept override { return "years"; }
} const year{};

const auto unit_table = std::unordered_map<stdx::string_view, std::reference_wrapper<const duration_unit>>{
        {year.short_name(), year},
        {month.short_name(), month},
        {week.short_name(), week},
        {day.short_name(), day},
        {hour.short_name(), hour},
        {minute.short_name(), minute},
        {second.short_name(), second},
        {millisecond.short_name(), millisecond},
        {microsecond.short_name(), microsecond}, {"µs", microsecond},
        {nanosecond.short_name(), nanosecond}
};

//
// Convenient helper to parse the indexed sub-expression from a match group as a duration counter.
//
// Throws `std::out_of_range` if a counter is out of range.
//
template <class Match, class Index = typename Match::size_type>
cql_duration::common_counter_type parse_count(const Match& m, Index group_index) {
    try {
        return boost::lexical_cast<cql_duration::common_counter_type>(m[group_index].str());
    } catch (const boost::bad_lexical_cast&) {
        throw std::out_of_range("duration counter");
    }
}

//
// Build up a duration unit-by-unit.
//
// We support overflow detection on construction for convenience and compatibility with Cassandra.
//
// We maintain some additional state over a `cql_duration` in order to track the order in which components are added when
// parsing the standard format.
//
class duration_builder final {
public:
    duration_builder& add(cql_duration::common_counter_type count, const duration_unit& unit) {
        validate_addition(count, unit);
        validate_and_update_order(unit);

        unit.increment_count(_duration, count);
        return *this;
    }

    template <class Match, class Index = typename Match::size_type>
    duration_builder& add_parsed_count(const Match& m, Index group_index, const duration_unit& unit) {
        cql_duration::common_counter_type count;

        try {
            count = parse_count(m, group_index);
        } catch (const std::out_of_range&) {
            throw cql_duration_error(sprint("Invalid duration. The count for the %s is out of range", unit.long_name()));
        }

        return add(count, unit);
    }

    cql_duration build() const noexcept {
        return _duration;
    }

private:
    const duration_unit* _current_unit{nullptr};

    cql_duration _duration{};

    //
    // Throws `cql_duration_error` if the addition of a quantity of the designated unit would overflow one of the
    // counters.
    //
    void validate_addition(typename cql_duration::common_counter_type count, const duration_unit& unit) const {
        const auto available = unit.available_count(_duration);

        if (count > available) {
            throw cql_duration_error(
                    sprint("Invalid duration. The number of %s must be less than or equal to %s",
                           unit.long_name(),
                           available));
        }
    }

    //
    // Validate that an addition of a quantity of the designated unit is not out of order. We require that units are
    // added in decreasing size.
    //
    // This function also updates the last-observed unit for the next invocation.
    //
    // Throws `cql_duration_error` for order violations.
    //
    void validate_and_update_order(const duration_unit& unit) {
        const auto index = unit.index();

        if (_current_unit != nullptr) {
            if (index == _current_unit->index()) {
                throw cql_duration_error(sprint("Invalid duration. The %s are specified multiple times", unit.long_name()));
            } else if (index > _current_unit->index()) {
                throw cql_duration_error(
                        sprint("Invalid duration. The %s should be after %s",
                               _current_unit->long_name(),
                               unit.long_name()));
            }
        }

        _current_unit = &unit;
    }
};

//
// These functions assume no sign information ('-). That is left to the `cql_duration` constructor.
//

stdx::optional<cql_duration> parse_duration_standard_format(stdx::string_view s) {

    //
    // We parse one component (pair of a count and unit) at a time in order to give more precise error messages when
    // units are specified multiple times or out of order rather than just "parse error".
    //
    // The other formats are more strict and complain less helpfully.
    //

    static const auto pattern =
            std::regex(u8"(\\d+)(y|Y|mo|MO|mO|Mo|w|W|d|D|h|H|s|S|ms|MS|mS|Ms|us|US|uS|Us|µs|µS|ns|NS|nS|Ns|m|M)");

    auto iter = s.cbegin();
    std::cmatch match;

    duration_builder b;

    // `match_continuous` ensures that the entire string must be included in a match.
    while (std::regex_search(iter, s.end(), match, pattern, std::regex_constants::match_continuous)) {
        iter += match.length();

        auto symbol = match[2].str();

        // Special case for mu.
        {
            auto view = stdx::string_view(symbol);
            view.remove_suffix(1);

            if (view == u8"µ") {
                b.add_parsed_count(match, 1, microsecond);
                continue;
            }
        }

        // Otherwise, we can just convert to lower-case for look-up.
        std::transform(symbol.begin(), symbol.end(), symbol.begin(), [](char ch) { return std::tolower(ch); });
        b.add_parsed_count(match, 1, unit_table.at(symbol));
    }

    if (iter != s.cend()) {
        // There is unconsumed input.
        return {};
    }

    return b.build();
}

stdx::optional<cql_duration> parse_duration_iso8601_format(stdx::string_view s) {
    static const auto pattern = std::regex("P((\\d+)Y)?((\\d+)M)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)S)?)?");

    std::cmatch match;
    if (!std::regex_match(s.data(), match, pattern)) {
        return {};
    }

    duration_builder b;

    if (match[1].matched) {
        b.add_parsed_count(match, 2, year);
    }

    if (match[3].matched) {
        b.add_parsed_count(match, 4, month);
    }

    if (match[5].matched) {
        b.add_parsed_count(match, 6, day);
    }

    // Optional, more granular, information.
    if (match[7].matched) {
        if (match[8].matched) {
            b.add_parsed_count(match, 9, hour);
        }

        if (match[10].matched) {
            b.add_parsed_count(match, 11, minute);
        }

        if (match[12].matched) {
            b.add_parsed_count(match, 13, second);
        }
    }

    return b.build();
}

stdx::optional<cql_duration> parse_duration_iso8601_alternative_format(stdx::string_view s) {
    static const auto pattern = std::regex("P(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})");

    std::cmatch match;
    if (!std::regex_match(s.data(), match, pattern)) {
        return {};
    }

    return duration_builder()
            .add_parsed_count(match, 1, year)
            .add_parsed_count(match, 2, month)
            .add_parsed_count(match, 3, day)
            .add_parsed_count(match, 4, hour)
            .add_parsed_count(match, 5, minute)
            .add_parsed_count(match, 6, second)
            .build();
}

stdx::optional<cql_duration> parse_duration_iso8601_week_format(stdx::string_view s) {
    static const auto pattern = std::regex("P(\\d+)W");

    std::cmatch match;
    if (!std::regex_match(s.data(), match, pattern)) {
        return {};
    }

    return duration_builder()
            .add_parsed_count(match, 1, week)
            .build();
}

// Parse a duration string without sign information assuming one of the supported formats.
stdx::optional<cql_duration> parse_duration(stdx::string_view s) {
    if (s.length() == 0u) {
        return {};
    }

    if (s.front() == 'P') {
        if (s.back() == 'W') {
            return parse_duration_iso8601_week_format(s);
        }

        if (s.find('-') != s.npos) {
            return parse_duration_iso8601_alternative_format(s);
        }

        return parse_duration_iso8601_format(s);
    }

    return parse_duration_standard_format(s);
}

}

cql_duration::cql_duration(stdx::string_view s) {
    const bool is_negative = (s.length() != 0) && (s[0] == '-');

    // Without any sign indicator ('-').
    const auto ps = (is_negative ? s.cbegin() + 1 : s.cbegin());

    const auto d = parse_duration(ps);
    if (!d) {
        throw cql_duration_error(sprint("Unable to convert '%s' to a duration", s));
    }

    *this = *d;

    if (is_negative) {
        months = -months;
        days = -days;
        nanoseconds = -nanoseconds;
    }
}

std::ostream& operator<<(std::ostream& os, const cql_duration& d) {
    if ((d.months < 0) || (d.days < 0) || (d.nanoseconds < 0)) {
        os << '-';
    }

    // If a non-zero integral component of the count can be expressed in `unit`, then append it to the stream with its
    // unit.
    //
    // Returns the remaining count.
    const auto append = [&os](cql_duration::common_counter_type count, auto&& unit) {
        const auto divider = unit.factor;

        if ((count == 0) || (count < divider)) {
            return count;
        }

        os << (count / divider) << unit.short_name();
        return count % divider;
    };

    const auto month_remainder = append(std::abs(d.months), year);
    append(month_remainder, month);

    append(std::abs(d.days), day);

    auto nanosecond_remainder = append(std::abs(d.nanoseconds), hour);
    nanosecond_remainder = append(nanosecond_remainder, minute);
    nanosecond_remainder = append(nanosecond_remainder, second);
    nanosecond_remainder = append(nanosecond_remainder, millisecond);
    nanosecond_remainder = append(nanosecond_remainder, microsecond);
    append(nanosecond_remainder, nanosecond);

    return os;
}

seastar::sstring to_string(const cql_duration& d) {
    std::ostringstream ss;
    ss << d;
    return ss.str();
}

bool operator==(const cql_duration& d1, const cql_duration& d2) noexcept {
    return (d1.months == d2.months) && (d1.days == d2.days) && (d1.nanoseconds == d2.nanoseconds);
}

bool operator!=(const cql_duration& d1, const cql_duration& d2) noexcept {
    return !(d1 == d2);
}
