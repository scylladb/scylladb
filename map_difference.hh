/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include <map>
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

template<typename Key,
         typename Tp,
         typename Compare = std::less<Key>,
         typename Eq = std::equal_to<Tp>,
         typename Alloc>
inline
map_difference<Key>
difference(const std::map<Key, Tp, Compare, Alloc>& left,
           const std::map<Key, Tp, Compare, Alloc>& right,
           Compare key_comp,
           Eq equals = Eq(),
           Alloc alloc = Alloc())
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

template<typename Key, typename Tp, typename Compare, typename Eq, typename Alloc>
inline
map_difference<Key>
difference(const std::map<Key, Tp, Compare, Alloc>& left, const std::map<Key, Tp, Compare, Alloc>& right, Eq equals) {
    return difference(left, right, left.key_comp(), equals);
}
