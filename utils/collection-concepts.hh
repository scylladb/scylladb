/*
 * Copyright (C) 2020-present ScyllaDB
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
#include <type_traits>
#include <seastar/util/concepts.hh>
#include <compare>

template <typename Func, typename T>
concept Disposer = requires (Func f, T* val) { 
    { f(val) } noexcept -> std::same_as<void>;
};

template <typename Key1, typename Key2, typename Less>
concept LessComparable = requires (const Key1& a, const Key2& b, Less less) {
    { less(a, b) } -> std::same_as<bool>;
    { less(b, a) } -> std::same_as<bool>;
};

template <typename Key1, typename Key2, typename Less>
concept LessNothrowComparable = LessComparable<Key1, Key2, Less> && std::is_nothrow_invocable_v<Less, Key1, Key2>;

// Temporary scaffolding for supporting trichotomic compares
// that return either int or std::strong_ordering
template <typename T>
concept int_or_strong_ordering = std::same_as<T, int> || std::same_as<T, std::strong_ordering>;

template <typename T1, typename T2, typename Compare>
concept Comparable = requires (const T1& a, const T2& b, Compare cmp) {
    // The Comparable is trichotomic comparator that should return 
    //   negative value when a < b
    //   zero when a == b
    //   positive value when a > b
    { cmp(a, b) } -> int_or_strong_ordering;
};