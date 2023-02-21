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
#include <variant>
#include <seastar/core/sstring.hh>
#include "utils/UUID.hh"

namespace sstables {

struct generation_type {
    using impl_type = std::variant<int64_t, utils::UUID>;
    impl_type value;

    generation_type() = delete;

    constexpr generation_type(const generation_type& gen) noexcept = default;
    constexpr generation_type& operator=(const generation_type& gen) noexcept = default;

    template <typename T>
    explicit constexpr generation_type(const T& _value) noexcept: value(_value) {}

    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept = default;
};

inline auto generation_value(const generation_type& generation) {
    assert(false && "unimplemented"); // TODO(JS): implement
    return 0;
}

template <typename T>
generation_type generation_from_value(const T& v) noexcept {
    return generation_type{v};
}

} //namespace sstables

namespace seastar {
sstring to_sstring(const sstables::generation_type& gen);
} //namespace seastar

namespace std {
template <>
struct hash<sstables::generation_type> {
    size_t operator()(const sstables::generation_type& generation) const noexcept {
        return hash<sstables::generation_type::impl_type>{}(generation.value);
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

[[maybe_unused]] inline ostream& operator<<(ostream& s, const sstables::generation_type& generation) {
     return s << to_sstring(generation);
}
} //namespace std
