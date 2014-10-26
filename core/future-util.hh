/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CORE_FUTURE_UTIL_HH_
#define CORE_FUTURE_UTIL_HH_

#include "future.hh"

// parallel_for_each - run tasks in parallel
//
// Given a range [@begin, @end) of objects, run func(*i) for each i in
// the range, and return a future<> that resolves when all the functions
// complete.  @func should return a future<> that indicates when it is
// complete.
template <typename Iterator, typename Func>
inline
future<>
parallel_for_each(Iterator begin, Iterator end, Func&& func) {
    auto ret = make_ready_future<>();
    while (begin != end) {
        auto f = func(*begin++).then([ret = std::move(ret)] () mutable {
            return std::move(ret);
        });
        ret = std::move(f);
    }
    return ret;
}

#endif /* CORE_FUTURE_UTIL_HH_ */
