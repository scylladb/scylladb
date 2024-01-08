/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <set>

template<typename Key>
struct map_difference {
    // Entries in left map whose keys don't exist in the right map.
    std::set<Key> entries_only_on_left;

    // Entries in right map whose keys don't exist in the left map.
    std::set<Key> entries_only_on_right;

    // Entries that appear in both maps with the same value.
    std::set<Key> entries_in_common;

    // Entries that appear in both maps but have different values.
    std::set<Key> entries_differing;

    map_difference()
        : entries_only_on_left{}
        , entries_only_on_right{}
        , entries_in_common{}
        , entries_differing{}
    { }
};

/**
 * Produces a map_difference between the two specified maps, with Key keys and
 * Tp values, using the provided equality function. In order to work with any
 * map type, such as std::map and std::unordered_map, Args holds the remaining
 * type parameters of the particular map type.
 */
template<template<typename...> class Map,
         typename Key,
         typename Tp,
         typename Eq = std::equal_to<Tp>,
         typename... Args>
inline
map_difference<Key>
difference(const Map<Key, Tp, Args...>& left,
           const Map<Key, Tp, Args...>& right,
           Eq equals = Eq())
{
    map_difference<Key> diff{};
    for (auto&& kv : right) {
        diff.entries_only_on_right.emplace(kv.first);
    }
    for (auto&& kv : left) {
        auto&& left_key = kv.first;
        auto&& it = right.find(left_key);
        if (it != right.end()) {
            diff.entries_only_on_right.erase(left_key);
            const Tp& left_value = kv.second;
            const Tp& right_value = it->second;
            if (equals(left_value, right_value)) {
                diff.entries_in_common.emplace(left_key);
            } else {
                diff.entries_differing.emplace(left_key);
            }
        } else {
            diff.entries_only_on_left.emplace(left_key);
        }
    }
    return diff;
}
