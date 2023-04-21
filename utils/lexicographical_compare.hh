// Copyright 2023-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <compare>
#include <cstdint>
#include <iterator>

// Specifies position in a lexicographically ordered sequence
// relative to some value.
//
// For example, if used with a value "bc" with lexicographical ordering on strings,
// each enum value represents the following positions in an example sequence:
//
//   aa
//   aaa
//   b
//   ba
// --> before_all_prefixed
//   bc
// --> before_all_strictly_prefixed
//   bca
//   bcd
// --> after_all_prefixed
//   bd
//   bda
//   c
//   ca
//
enum class lexicographical_relation : int8_t {
    before_all_prefixed,
    before_all_strictly_prefixed,
    after_all_prefixed
};

// Like std::lexicographical_compare but injects values from shared sequence (types) to the comparator
// Compare is an abstract_type-aware less comparator, which takes the type as first argument.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
bool lexicographical_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        if (comp(*types, *first1, *first2)) {
            return true;
        }
        if (comp(*types, *first2, *first1)) {
            return false;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return (first1 == last1) && (first2 != last2);
}

// Like std::lexicographical_compare but injects values from shared sequence
// (types) to the comparator. Compare is an abstract_type-aware trichotomic
// comparator, which takes the type as first argument.
template <std::input_iterator TypesIterator, std::input_iterator InputIt1, std::input_iterator InputIt2, typename Compare>
requires requires (TypesIterator types, InputIt1 i1, InputIt2 i2, Compare cmp) {
    { cmp(*types, *i1, *i2) } -> std::same_as<std::strong_ordering>;
}
std::strong_ordering lexicographical_tri_compare(TypesIterator types_first, TypesIterator types_last,
        InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2,
        Compare comp,
        lexicographical_relation relation1 = lexicographical_relation::before_all_strictly_prefixed,
        lexicographical_relation relation2 = lexicographical_relation::before_all_strictly_prefixed) {
    while (types_first != types_last && first1 != last1 && first2 != last2) {
        auto c = comp(*types_first, *first1, *first2);
        if (c != 0) {
            return c;
        }
        ++first1;
        ++first2;
        ++types_first;
    }
    bool e1 = first1 == last1;
    bool e2 = first2 == last2;
    if (e1 && e2) {
        return static_cast<int>(relation1) <=> static_cast<int>(relation2);
    }
    if (e2) {
        return relation2 == lexicographical_relation::after_all_prefixed ? std::strong_ordering::less : std::strong_ordering::greater;
    } else if (e1) {
        return relation1 == lexicographical_relation::after_all_prefixed ? std::strong_ordering::greater : std::strong_ordering::less;
    } else {
        return std::strong_ordering::equal;
    }
}

// A trichotomic comparator for prefix equality total ordering.
// In this ordering, two sequences are equal iff any of them is a prefix
// of the another. Otherwise, lexicographical ordering determines the order.
//
// 'comp' is an abstract_type-aware trichotomic comparator, which takes the
// type as first argument.
//
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
requires requires (TypesIterator ti, InputIt1 i1, InputIt2 i2, Compare c) {
    { c(*ti, *i1, *i2) } -> std::same_as<std::strong_ordering>;
}
std::strong_ordering prefix_equality_tri_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        auto c = comp(*types, *first1, *first2);
        if (c != 0) {
            return c;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return std::strong_ordering::equal;
}

// Returns true iff the second sequence is a prefix of the first sequence
// Equality is an abstract_type-aware equality checker which takes the type as first argument.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Equality>
bool is_prefixed_by(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Equality equality) {
    while (first1 != last1 && first2 != last2) {
        if (!equality(*types, *first1, *first2)) {
            return false;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return first2 == last2;
}

