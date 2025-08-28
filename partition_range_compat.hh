/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#pragma once

#include <vector>
#include "interval.hh"
#include "dht/ring_position.hh"

namespace compat {

using wrapping_partition_range = wrapping_interval<dht::ring_position>;


// unwraps a vector of wrapping ranges into a vector of nonwrapping ranges
// if the vector happens to be sorted by the left bound, it remains sorted
template <template <typename...> class Container, typename T, typename Comparator>
requires std::ranges::range<Container<interval<T>>>
        && requires (Container<interval<T>> c, size_t s, interval<T> i) {
    { c.reserve(s) };
    { c.emplace_back(std::move(i)) };
    { c.insert(c.begin(), std::move(i)) };
}
Container<interval<T>>
unwrap(Container<wrapping_interval<T>>&& v, Comparator&& cmp) {
    Container<interval<T>> ret;
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

template <typename T>
struct is_wrapping_interval : std::false_type {};

template <typename T>
struct is_wrapping_interval<wrapping_interval<T>> : std::true_type {};

template <typename T>
constexpr bool is_wrapping_interval_v = is_wrapping_interval<T>::value;

template <typename T>
struct is_nonwrapping_interval : std::false_type {};

template <typename T>
struct is_nonwrapping_interval<interval<T>> : std::true_type {};

template <typename T>
constexpr bool is_nonwrapping_interval_v = is_nonwrapping_interval<T>::value;

template <typename T>
struct rebind_wrapping_interval_to_nonwrapping_interval;

template <typename T>
struct rebind_wrapping_interval_to_nonwrapping_interval<wrapping_interval<T>> {
    using type = interval<T>;
};

template <typename T>
using rebind_wrapping_interval_to_nonwrapping_interval_t = typename rebind_wrapping_interval_to_nonwrapping_interval<T>::type;

template <typename T>
struct rebind_container_wrapping_to_nonwrapping_interval;

template <typename T>
using rebind_container_wrapping_to_nonwrapping_interval_t = rebind_container_wrapping_to_nonwrapping_interval<T>::type;

template <typename T>
struct rebind_container_wrapping_to_nonwrapping_interval<std::vector<wrapping_interval<T>>> {
    using type = std::vector<interval<T>>;
};

template <typename T>
struct rebind_container_wrapping_to_nonwrapping_interval<utils::chunked_vector<wrapping_interval<T>>> {
    using type = utils::chunked_vector<interval<T>>;
};

template <typename T>
struct rebind_container_nonwrapping_to_wrapping_interval;

template <typename T>
using rebind_container_nonwrapping_to_wrapping_interval_t = rebind_container_nonwrapping_to_wrapping_interval<T>::type;

template <typename T>
struct rebind_container_nonwrapping_to_wrapping_interval<std::vector<interval<T>>> {
    using type = std::vector<wrapping_interval<T>>;
};

template <typename T>
struct rebind_container_nonwrapping_to_wrapping_interval<utils::chunked_vector<interval<T>>> {
    using type = utils::chunked_vector<wrapping_interval<T>>;
};

// unwraps a vector of wrapping ranges into a vector of nonwrapping ranges
// if the vector happens to be sorted by the left bound, it remains sorted
template <typename Container, typename Comparator>
requires std::ranges::range<Container>
        && is_wrapping_interval_v<typename Container::value_type>
        && requires (Container c, size_t s, Container::value_type i) {
    { c.reserve(s) };
    { c.emplace_back(std::move(i)) };
    { c.insert(c.begin(), std::move(i)) };
}
rebind_container_wrapping_to_nonwrapping_interval_t<Container>
unwrap(const Container& v, Comparator&& cmp) {
    rebind_container_wrapping_to_nonwrapping_interval_t<Container> ret;
    using interval_t = rebind_wrapping_interval_to_nonwrapping_interval_t<typename Container::value_type>;
    ret.reserve(v.size() + 1);
    for (auto&& wr : v) {
        if (wr.is_wrap_around(cmp)) {
            auto&& p = wr.unwrap();
            ret.insert(ret.begin(), interval_t(p.first));
            ret.emplace_back(p.second);
        } else {
            ret.emplace_back(wr);
        }
    }
    return ret;
}

template <typename Container>
requires std::ranges::range<Container>
        && is_nonwrapping_interval_v<typename Container::value_type>
        && requires (Container c, size_t s, Container::value_type i) {
    { c.reserve(s) };
    { c.emplace_back(std::move(i)) };
    { c.push_back(std::move(i)) };
}
rebind_container_nonwrapping_to_wrapping_interval_t<Container>
wrap(const Container& v) {
    using ret_type = rebind_container_nonwrapping_to_wrapping_interval_t<Container>;
    // re-wrap (-inf,x) ... (y, +inf) into (y, x):
    if (v.size() >= 2 && !v.front().start() && !v.back().end()) {
        auto ret = ret_type();
        ret.reserve(v.size() - 1);
        std::copy(v.begin() + 1, v.end() - 1, std::back_inserter(ret));
        ret.emplace_back(v.back().start(), v.front().end());
        return ret;
    }
    return v | std::ranges::to<ret_type>();
}

template <typename Container>
requires std::ranges::range<Container>
        && is_nonwrapping_interval_v<typename Container::value_type>
        && requires (Container c, size_t s, Container::value_type i) {
    { c.reserve(s) };
    { c.emplace_back(std::move(i)) };
    { c.push_back(std::move(i)) };
}
rebind_container_nonwrapping_to_wrapping_interval_t<Container>
wrap(Container&& v) {
    using ret_type = rebind_container_nonwrapping_to_wrapping_interval_t<Container>;
    // re-wrap (-inf,x) ... (y, +inf) into (y, x):
    if (v.size() >= 2 && !v.front().start() && !v.back().end()) {
        auto ret = ret_type();
        ret.reserve(v.size() - 1);
        std::move(v.begin() + 1, v.end() - 1, std::back_inserter(ret));
        ret.emplace_back(std::move(v.back()).start(), std::move(v.front()).end());
        return ret;
    }
    return std::ranges::owning_view(std::move(v)) | std::ranges::to<ret_type>();
}

inline
dht::token_range_vector
unwrap(const utils::chunked_vector<wrapping_interval<dht::token>>& v) {
    return unwrap(v, dht::token_comparator());
}

inline
dht::token_range_vector
unwrap(utils::chunked_vector<wrapping_interval<dht::token>>&& v) {
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
