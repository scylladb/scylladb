/*
 * Copyright (C) 2021-present ScyllaDB
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
