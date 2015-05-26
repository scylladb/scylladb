/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <map>

template<typename Tp>
struct value_difference {
    Tp left_value;
    Tp right_value;

    value_difference(const Tp& left_value_, const Tp& right_value_)
        : left_value(left_value_)
        , right_value(right_value_)
    { }
};

template<typename Key, typename Tp, typename Compare, typename Alloc>
struct map_difference {
    // Entries in left map whose keys don't exist in the right map.
    std::map<Key, Tp, Compare> entries_only_on_left;

    // Entries in right map whose keys don't exist in the left map.
    std::map<Key, Tp, Compare> entries_only_on_right;

    // Entries that appear in both maps with the same value.
    std::map<Key, Tp, Compare> entries_in_common;

    // Entries that appear in both maps but have different values.
    std::map<Key, value_difference<Tp>, Compare> entries_differing;

    map_difference(const Compare& cmp, const Alloc& alloc)
        : entries_only_on_left{cmp, alloc}
        , entries_only_on_right{cmp, alloc}
        , entries_in_common{cmp, alloc}
        , entries_differing{cmp, alloc}
    { }
};

template<typename Key,
         typename Tp,
         typename Compare = std::less<Key>,
         typename Eq = std::equal_to<Tp>,
         typename Alloc>
inline
map_difference<Key, Tp, Compare, Alloc>
difference(const std::map<Key, Tp, Compare, Alloc>& left,
           const std::map<Key, Tp, Compare, Alloc>& right,
           Compare key_comp,
           Eq equals = Eq(),
           Alloc alloc = Alloc())
{
    map_difference<Key, Tp, Compare, Alloc> diff{key_comp, alloc};
    diff.entries_only_on_right = right;
    for (auto&& kv : left) {
        auto&& left_key = kv.first;
        auto&& it = right.find(left_key);
        if (it != right.end()) {
            diff.entries_only_on_right.erase(left_key);
            const Tp& left_value = kv.second;
            const Tp& right_value = it->second;
            if (equals(left_value, right_value)) {
                diff.entries_in_common.emplace(kv);
            } else {
                value_difference<Tp> value_diff{left_value, right_value};
                diff.entries_differing.emplace(left_key, std::move(value_diff));
            }
        } else {
            diff.entries_only_on_left.emplace(kv);
        }
    }
    return diff;
}

template<typename Key, typename Tp, typename Compare, typename Eq, typename Alloc>
inline
map_difference<Key, Tp, Compare, Alloc>
difference(const std::map<Key, Tp, Compare, Alloc>& left, const std::map<Key, Tp, Compare, Alloc>& right, Eq equals) {
    return difference(left, right, left.key_comp(), equals);
}
