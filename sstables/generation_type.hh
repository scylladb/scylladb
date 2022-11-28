/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <compare>
#include <limits>
#include <iostream>
#include <seastar/core/sstring.hh>

namespace sstables {

class generation_type {
    int64_t _value;
public:
    generation_type() = delete;

    explicit constexpr generation_type(int64_t value) noexcept: _value(value) {}
    constexpr int64_t value() const noexcept { return _value; }

    constexpr bool operator==(const generation_type& other) const noexcept { return _value == other._value; }
    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept { return _value <=> other._value; }
};

constexpr generation_type generation_from_value(int64_t value) {
    return generation_type{value};
}
constexpr int64_t generation_value(generation_type generation) {
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
        return hash<int64_t>{}(generation.value());
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

[[maybe_unused]] static ostream& operator<<(ostream& s, const sstables::generation_type& generation) {
    return s << generation.value();
}
} //namespace std
