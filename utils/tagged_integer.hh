/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <compare>
#include <iostream>
#include <type_traits>

namespace utils {

// Note: do not use directly, use utils::tagged_integer instead.
// The reason this double-tagged template exist
// is to distinguish between utils::tagged_integer
// and raft::internal::tagged_uint64 that have incompatible
// idl types and therefore must not be convertible to each other.
template <typename Final, typename Tag, std::integral ValueType>
class tagged_tagged_integer {
public:
    using value_type = ValueType;
private:
    value_type _value;
public:
    tagged_tagged_integer() noexcept : _value(0) {}
    explicit tagged_tagged_integer(value_type v) noexcept : _value(v) {}

    tagged_tagged_integer& operator=(value_type v) noexcept {
        _value = v;
        return *this;
    }

    value_type value() const noexcept { return _value; }
    operator value_type() const noexcept { return _value; }

    explicit operator bool() const { return _value != 0; }

    auto operator<=>(const tagged_tagged_integer& o) const = default;

    tagged_tagged_integer& operator++() noexcept {
        ++_value;
        return *this;
    }
    tagged_tagged_integer& operator--() noexcept {
        --_value;
        return *this;
    }

    tagged_tagged_integer operator++(int) noexcept {
        auto ret = *this;
        ++_value;
        return ret;
    }
    tagged_tagged_integer operator--(int) noexcept {
        auto ret = *this;
        --_value;
        return ret;
    }

    tagged_tagged_integer operator+(const tagged_tagged_integer& o) const {
        return tagged_tagged_integer(_value + o._value);
    }
    tagged_tagged_integer operator-(const tagged_tagged_integer& o) const {
        return tagged_tagged_integer(_value - o._value);
    }

    tagged_tagged_integer& operator+=(const tagged_tagged_integer& o) {
        _value += o._value;
        return *this;
    }
    tagged_tagged_integer& operator-=(const tagged_tagged_integer& o) {
        _value -= o._value;
        return *this;
    }
};

template <typename Tag, std::integral ValueType>
using tagged_integer = tagged_tagged_integer<struct final, Tag, ValueType>;

} // namespace utils

namespace std {

template <typename Final, typename Tag, std::integral ValueType>
struct hash<utils::tagged_tagged_integer<Final, Tag, ValueType>> {
    size_t operator()(const utils::tagged_tagged_integer<Final, Tag, ValueType>& x) const noexcept {
        return hash<ValueType>{}(x.value());
    }
};

template <typename Final, typename Tag, std::integral ValueType>
[[maybe_unused]] ostream& operator<<(ostream& s, const utils::tagged_tagged_integer<Final, Tag, ValueType>& x) {
    return s << x.value();
}

} // namespace std
