/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#pragma once

#include <vector>
#include "interval.hh"
#include "dht/ring_position.hh"

namespace compat {

using wrapping_partition_range = wrapping_interval<dht::ring_position>;


// unwraps a vector of wrapping ranges into a vector of nonwrapping ranges
// if the vector happens to be sorted by the left bound, it remains sorted
template <typename T, typename Comparator>
std::vector<interval<T>>
unwrap(std::vector<wrapping_interval<T>>&& v, Comparator&& cmp) {
    std::vector<interval<T>> ret;
    ret.reserve(v.size() + 1);
    for (auto&& wr : v) {
        if (wr.is_wrap_around(cmp)) {
            auto&& p = std::move(wr).unwrap();
            ret.insert(ret.begin(), interval<T>(std::move(p.first)));
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
std::vector<interval<T>>
unwrap(const std::vector<wrapping_interval<T>>& v, Comparator&& cmp) {
    std::vector<interval<T>> ret;
    ret.reserve(v.size() + 1);
    for (auto&& wr : v) {
        if (wr.is_wrap_around(cmp)) {
            auto&& p = wr.unwrap();
            ret.insert(ret.begin(), interval<T>(p.first));
            ret.emplace_back(p.second);
        } else {
            ret.emplace_back(wr);
        }
    }
    return ret;
}

template <typename T>
std::vector<wrapping_interval<T>>
wrap(const std::vector<interval<T>>& v) {
    // re-wrap (-inf,x) ... (y, +inf) into (y, x):
    if (v.size() >= 2 && !v.front().start() && !v.back().end()) {
        auto ret = std::vector<wrapping_interval<T>>();
        ret.reserve(v.size() - 1);
        std::copy(v.begin() + 1, v.end() - 1, std::back_inserter(ret));
        ret.emplace_back(v.back().start(), v.front().end());
        return ret;
    }
    return boost::copy_range<std::vector<wrapping_interval<T>>>(v);
}

template <typename T>
std::vector<wrapping_interval<T>>
wrap(std::vector<interval<T>>&& v) {
    // re-wrap (-inf,x) ... (y, +inf) into (y, x):
    if (v.size() >= 2 && !v.front().start() && !v.back().end()) {
        auto ret = std::vector<wrapping_interval<T>>();
        ret.reserve(v.size() - 1);
        std::move(v.begin() + 1, v.end() - 1, std::back_inserter(ret));
        ret.emplace_back(std::move(v.back()).start(), std::move(v.front()).end());
        return ret;
    }
    // want boost::adaptor::moved ...
    return boost::copy_range<std::vector<wrapping_interval<T>>>(v);
}

inline
dht::token_range_vector
unwrap(const std::vector<wrapping_interval<dht::token>>& v) {
    return unwrap(v, dht::token_comparator());
}

inline
dht::token_range_vector
unwrap(std::vector<wrapping_interval<dht::token>>&& v) {
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
unwrap_into(wrapping_interval<T>&& range, const Comparator& cmp, Func&& func) {
    if (range.is_wrap_around(cmp)) {
        auto&& unw = range.unwrap();
        // Preserve ring order
        func(interval<T>(std::move(unw.second)));
        func(interval<T>(std::move(unw.first)));
    } else {
        func(interval<T>(std::move(range)));
    }
}

}
