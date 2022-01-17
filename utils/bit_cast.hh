/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstring>
#include <cstddef>
#include <type_traits>

template <class T> concept Trivial = std::is_trivial_v<T>;
template <class T> concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

// C++20 has std::bit_cast, which does the same thing,
// (but better, because it supports constexpr through compiler magic)
// but it hasn't made its way to our libstdc++ yet.
template <Trivial To, TriviallyCopyable From>
requires (sizeof(To) == sizeof(From))
To bit_cast(const From& src) noexcept {
    To dst;
    std::memcpy(&dst, &src, sizeof(To));
    return dst;
}

template <Trivial To>
To read_unaligned(const void* src) {
    To dst;
    std::memcpy(&dst, src, sizeof(To));
    return dst;
}

template <TriviallyCopyable From>
void write_unaligned(void* dst, const From& src) {
    std::memcpy(dst, &src, sizeof(From));
}
