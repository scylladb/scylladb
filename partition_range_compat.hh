/*
 * Copyright 2016-present ScyllaDB
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

#include <vector>
#include "range.hh"
#include "dht/i_partitioner.hh"
#include "query-request.hh"

namespace compat {

using wrapping_partition_range = wrapping_range<dht::ring_position>;


// unwraps a vector of wrapping ranges into a vector of nonwrapping ranges
// if the vector happens to be sorted by the left bound, it remains sorted
template <typename T, typename Comparator>
std::vector<nonwrapping_range<T>>
unwrap(std::vector<wrapping_range<T>>&& v, Comparator&& cmp) {
    std::vector<nonwrapping_range<T>> ret;
    ret.reserve(v.size() + 1);
    for (auto&& wr : v) {
        if (wr.is_wrap_around(cmp)) {
            auto&& p = std::move(wr).unwrap();
            ret.insert(ret.begin(), nonwrapping_range<T>(std::move(p.first)));
            ret.emplace_back(std::move(p.second));
        } else {
            ret.emplace_back(std::move(wr));
        }
    }
    return ret;
}

// unwraps a vector of wrapping ranges into a vector of nonwrapping ranges
// if the vector happens to be sorted by the left bound, it remains sorted
template <typename T, typename Comparator>
std::vector<nonwrapping_range<T>>
unwrap(const std::vector<wrapping_range<T>>& v, Comparator&& cmp) {
    std::vector<nonwrapping_range<T>> ret;
    ret.reserve(v.size() + 1);
    for (auto&& wr : v) {
        if (wr.is_wrap_around(cmp)) {
            auto&& p = wr.unwrap();
            ret.insert(ret.begin(), nonwrapping_range<T>(p.first));
            ret.emplace_back(p.second);
        } else {
            ret.emplace_back(wr);
        }
    }
    return ret;
}

template <typename T>
std::vector<wrapping_range<T>>
wrap(const std::vector<nonwrapping_range<T>>& v) {
    // re-wrap (-inf,x) ... (y, +inf) into (y, x):
    if (v.size() >= 2 && !v.front().start() && !v.back().end()) {
        auto ret = std::vector<wrapping_range<T>>();
        ret.reserve(v.size() - 1);
        std::copy(v.begin() + 1, v.end() - 1, std::back_inserter(ret));
        ret.emplace_back(v.back().start(), v.front().end());
        return ret;
    }
    return boost::copy_range<std::vector<wrapping_range<T>>>(v);
}

template <typename T>
std::vector<wrapping_range<T>>
wrap(std::vector<nonwrapping_range<T>>&& v) {
    // re-wrap (-inf,x) ... (y, +inf) into (y, x):
    if (v.size() >= 2 && !v.front().start() && !v.back().end()) {
        auto ret = std::vector<wrapping_range<T>>();
        ret.reserve(v.size() - 1);
        std::move(v.begin() + 1, v.end() - 1, std::back_inserter(ret));
        ret.emplace_back(std::move(v.back()).start(), std::move(v.front()).end());
        return ret;
    }
    // want boost::adaptor::moved ...
    return boost::copy_range<std::vector<wrapping_range<T>>>(v);
}

inline
dht::token_range_vector
unwrap(const std::vector<wrapping_range<dht::token>>& v) {
    return unwrap(v, dht::token_comparator());
}

inline
dht::token_range_vector
unwrap(std::vector<wrapping_range<dht::token>>&& v) {
    return unwrap(std::move(v), dht::token_comparator());
}


class one_or_two_partition_ranges : public std::pair<dht::partition_range, std::optional<dht::partition_range>> {
    using pair = std::pair<dht::partition_range, std::optional<dht::partition_range>>;
public:
    explicit one_or_two_partition_ranges(dht::partition_range&& f)
        : pair(std::move(f), std::nullopt) {
    }
    explicit one_or_two_partition_ranges(dht::partition_range&& f, dht::partition_range&& s)
        : pair(std::move(f), std::move(s)) {
    }
    operator dht::partition_range_vector() const & {
        auto ret = dht::partition_range_vector();
        // not reserving, since ret.size() is likely to be 1
        ret.push_back(first);
        if (second) {
            ret.push_back(*second);
        }
        return ret;
    }
    operator dht::partition_range_vector() && {
        auto ret = dht::partition_range_vector();
        // not reserving, since ret.size() is likely to be 1
        ret.push_back(std::move(first));
        if (second) {
            ret.push_back(std::move(*second));
        }
        return ret;
    }
};

inline
one_or_two_partition_ranges
unwrap(wrapping_partition_range pr, const schema& s) {
    if (pr.is_wrap_around(dht::ring_position_comparator(s))) {
        auto unw = std::move(pr).unwrap();
        // Preserve ring order
        return one_or_two_partition_ranges(
                dht::partition_range(std::move(unw.second)),
                dht::partition_range(std::move(unw.first)));
    } else {
        return one_or_two_partition_ranges(dht::partition_range(std::move(pr)));
    }
}

// Unwraps `range` and calls `func` with its components, with an unwrapped
// range type, as a parameter (once or twice)
template <typename T, typename Comparator, typename Func>
void
unwrap_into(wrapping_range<T>&& range, const Comparator& cmp, Func&& func) {
    if (range.is_wrap_around(cmp)) {
        auto&& unw = range.unwrap();
        // Preserve ring order
        func(nonwrapping_range<T>(std::move(unw.second)));
        func(nonwrapping_range<T>(std::move(unw.first)));
    } else {
        func(nonwrapping_range<T>(std::move(range)));
    }
}

}
