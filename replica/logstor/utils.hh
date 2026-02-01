/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <concepts>
#include "serializer.hh"

namespace replica::logstor {

// an unsigned integer that can be incremented and compared with wraparound semantics
template <std::unsigned_integral T>
class generation_base {
    T _value;

public:

    using underlying = T;

    constexpr generation_base() noexcept : _value(0) {}
    constexpr explicit generation_base(T value) noexcept : _value(value) {}

    constexpr T value() const noexcept { return _value; }

    constexpr generation_base& operator++() noexcept {
        ++_value;
        return *this;
    }

    constexpr generation_base operator++(int) noexcept {
        auto old = *this;
        ++_value;
        return old;
    }

    constexpr generation_base& operator+=(T delta) noexcept {
        _value += delta;
        return *this;
    }

    constexpr generation_base operator+(T delta) const noexcept {
        return generation_base(_value + delta);
    }

    constexpr bool operator==(const generation_base& other) const noexcept = default;

    /// Comparison using wraparound semantics.
    /// Returns true if this generation is less than other, accounting for wraparound.
    /// Assumes generations are within half the value space of each other.
    constexpr bool operator<(const generation_base& other) const noexcept {
        // Use signed comparison after converting difference to signed type
        // This handles wraparound: if diff > max/2, it's treated as negative
        using signed_type = std::make_signed_t<T>;
        auto diff = static_cast<signed_type>(_value - other._value);
        return diff < 0;
    }

    constexpr bool operator<=(const generation_base& other) const noexcept {
        return *this == other || *this < other;
    }

    constexpr bool operator>(const generation_base& other) const noexcept {
        return other < *this;
    }

    constexpr bool operator>=(const generation_base& other) const noexcept {
        return other <= *this;
    }
};

}

template <std::unsigned_integral T>
struct fmt::formatter<replica::logstor::generation_base<T>> : fmt::formatter<T> {
    template <typename FormatContext>
    auto format(const replica::logstor::generation_base<T>& gen, FormatContext& ctx) const {
        return fmt::formatter<T>::format(gen.value(), ctx);
    }
};

namespace ser {

template <std::unsigned_integral T>
struct serializer<replica::logstor::generation_base<T>> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::generation_base<T>& g) {
        serializer<typename replica::logstor::generation_base<T>::underlying>::write(out, g.value());
    }
    template <typename Input>
    static replica::logstor::generation_base<T> read(Input& in) {
        auto val = serializer<typename replica::logstor::generation_base<T>::underlying>::read(in);
        return replica::logstor::generation_base<T>(val);
    }
    template <typename Input>
    static void skip(Input& in) {
        serializer<typename replica::logstor::generation_base<T>::underlying>::skip(in);
    }
};

}
