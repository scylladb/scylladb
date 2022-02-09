/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/result.hh"

namespace utils {

// A version of parallel_for_each which understands results.
// In case of a failure, it returns one of the failed results.
template<typename R, typename Iterator, typename Func>
requires
    ExceptionContainerResult<R>
    && std::is_void_v<typename R::value_type>
    && requires (Func f, Iterator i) { { f(*i++) } -> std::same_as<seastar::future<R>>; }
inline seastar::future<R> result_parallel_for_each(Iterator begin, Iterator end, Func&& func) {
    struct result_reducer {
        R res = bo::success();
        void operator()(R&& r) {
            if (res && !r) {
                res = std::move(r);
            }
        }
        R get() {
            return std::move(res);
        }
    };

    return seastar::map_reduce(std::move(begin), std::move(end), std::forward<Func>(func), result_reducer{});
}

// A version of parallel_for_each which understands results.
// In case of a failure, it returns one of the failed results.
template<typename R, typename Range, typename Func>
requires
    ExceptionContainerResult<R>
    && std::is_void_v<typename R::value_type>
inline seastar::future<R> result_parallel_for_each(Range&& range, Func&& func) {
    return result_parallel_for_each<R>(std::begin(range), std::end(range), std::forward<Func>(func));
}

}
