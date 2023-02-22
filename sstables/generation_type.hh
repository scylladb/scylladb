/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/core.h>
#include <cstdint>
#include <compare>
#include <limits>
#include <iostream>
#include <seastar/core/sstring.hh>

namespace sstables {

class generation_type {
public:
    using value_type = int64_t;
private:
    value_type _value;
public:
    generation_type() noexcept
        : _value(0)
    {}

    explicit constexpr generation_type(value_type value) noexcept: _value(value) {}
    constexpr value_type value() const noexcept { return _value; }

    constexpr bool operator==(const generation_type& other) const noexcept { return _value == other._value; }
    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept { return _value <=> other._value; }

    generation_type& operator++() noexcept {
        ++_value;
        return *this;
    }

    generation_type operator++(int) noexcept {
        auto ret = *this;
        ++_value;
        return ret;
    }
};

constexpr generation_type generation_from_value(generation_type::value_type value) {
    return generation_type{value};
}
constexpr generation_type::value_type generation_value(generation_type generation) {
    return generation.value();
}

} //namespace sstables

namespace seastar {
template <typename string_type = sstring>
string_type to_sstring(sstables::generation_type generation) {
    return to_sstring(sstables::generation_value(generation));
}
} //namespace seastar

namespace std {
template <>
struct hash<sstables::generation_type> {
    size_t operator()(const sstables::generation_type& generation) const noexcept {
        return hash<sstables::generation_type::value_type>{}(generation.value());
    }
};

// for min_max_tracker
template <>
struct numeric_limits<sstables::generation_type> : public numeric_limits<int64_t> {
    static constexpr sstables::generation_type min() noexcept {
        return sstables::generation_type{numeric_limits<int64_t>::min()};
    }
    static constexpr sstables::generation_type max() noexcept {
        return sstables::generation_type{numeric_limits<int64_t>::max()};
    }
};
} //namespace std

template <>
struct fmt::formatter<sstables::generation_type> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const sstables::generation_type& generation, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", generation.value());
    }
};
