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

#include <list>
#include <algorithm>
#include <seastar/core/thread.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include "utils/collection-concepts.hh"

namespace utils {


// Similar to std::merge but it does not stall. Must run inside a seastar
// thread. It merges items from list2 into list1. Items from list2 can only be copied.
template<class T, class Compare>
requires LessComparable<T, T, Compare>
void merge_to_gently(std::list<T>& list1, const std::list<T>& list2, Compare comp) {
    auto first1 = list1.begin();
    auto first2 = list2.begin();
    auto last1 = list1.end();
    auto last2 = list2.end();
    while (first2 != last2) {
        seastar::thread::maybe_yield();
        if (first1 == last1) {
            // Copy remaining items of list2 into list1
            std::copy_if(first2, last2, std::back_inserter(list1), [] (const auto&) { return true; });
            return;
        }
        if (comp(*first2, *first1)) {
            first1 = list1.insert(first1, *first2);
            ++first2;
        } else {
            ++first1;
        }
    }
}

template<class T>
seastar::future<> clear_gently(std::list<T>& list) {
    return repeat([&list] () mutable {
        if (list.empty()) {
            return seastar::stop_iteration::yes;
        }
        list.pop_back();
        return seastar::stop_iteration::no;
    });
}

}

