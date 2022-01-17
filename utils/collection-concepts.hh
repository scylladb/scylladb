/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

template <typename T1, typename T2, typename Compare>
concept Comparable = requires (const T1& a, const T2& b, Compare cmp) {
    // The Comparable is trichotomic comparator that should return 
    //   negative value when a < b
    //   zero when a == b
    //   positive value when a > b
    { cmp(a, b) } -> std::same_as<std::strong_ordering>;
};
