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

template <typename Tag, std::integral ValueType>
class tagged_integer {
public:
    using value_type = ValueType;
private:
    value_type _value;
public:
    tagged_integer() noexcept : _value(0) {}
    explicit tagged_integer(value_type v) noexcept : _value(v) {}

    tagged_integer& operator=(value_type v) noexcept {
        _value = v;
        return *this;
    }

    value_type value() const noexcept { return _value; }
    operator value_type() const noexcept { return _value; }

    explicit operator bool() const { return _value != 0; }

    auto operator<=>(const tagged_integer& o) const = default;

    tagged_integer& operator++() noexcept {
        ++_value;
        return *this;
    }
    tagged_integer& operator--() noexcept {
        --_value;
        return *this;
    }

    tagged_integer operator++(int) noexcept {
        auto ret = *this;
        ++_value;
        return ret;
    }
    tagged_integer operator--(int) noexcept {
        auto ret = *this;
        --_value;
        return ret;
    }

    tagged_integer operator+(const tagged_integer& o) const {
        return tagged_integer(_value + o._value);
    }
    tagged_integer operator-(const tagged_integer& o) const {
        return tagged_integer(_value - o._value);
    }

    tagged_integer& operator+=(const tagged_integer& o) const {
        _value += o._value;
        return *this;
    }
    tagged_integer& operator-=(const tagged_integer& o) const {
        _value -= o._value;
        return *this;
    }
};

} // namespace utils

namespace std {

template <typename Tag, std::integral ValueType>
struct hash<utils::tagged_integer<Tag, ValueType>> {
    size_t operator()(const utils::tagged_integer<Tag, ValueType>& x) const noexcept {
        return hash<ValueType>{}(x.value());
    }
};

template <typename Tag, std::integral ValueType>
[[maybe_unused]] ostream& operator<<(ostream& s, const utils::tagged_integer<Tag, ValueType>& x) {
    return s << x.value();
}

} // namespace std
