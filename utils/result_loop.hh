/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iterator>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include "utils/result.hh"

namespace utils {

namespace internal {

template<typename Iterator, typename IteratorCategory>
inline size_t iterator_range_estimate_vector_capacity(Iterator begin, Iterator end, IteratorCategory category) {
    // Return 0 in general case
    return 0;
}

template<typename Iterator, typename IteratorCategory>
inline size_t iterator_range_estimate_vector_capacity(Iterator begin, Iterator end, std::forward_iterator_tag category) {
    // May require linear scan, but it's better than reallocation
    return std::distance(begin, end);
}

}

// A version of parallel_for_each which understands results.
// In case of a failure, it returns one of the failed results.
template<typename R, typename Iterator, typename Func>
requires
    ExceptionContainerResult<R>
    && std::is_void_v<typename R::value_type>
    && requires (Func f, Iterator i) { { f(*i++) } -> std::same_as<seastar::future<R>>; }
inline seastar::future<R> result_parallel_for_each(Iterator begin, Iterator end, Func&& func) noexcept {
    std::vector<seastar::future<R>> futs;
    while (begin != end) {
        auto f = seastar::futurize_invoke(std::forward<Func>(func), *begin++);
        seastar::memory::scoped_critical_alloc_section _;
        if (f.available() && !f.failed()) {
            auto res = f.get();
            if (res) {
                continue;
            }
            f = seastar::make_ready_future<R>(std::move(res));
        }
        if (futs.empty()) {
            using itraits = std::iterator_traits<Iterator>;
            auto n = (internal::iterator_range_estimate_vector_capacity(begin, end, typename itraits::iterator_category()) + 1);
            futs.reserve(n);
        }
        futs.push_back(std::move(f));
    }

    if (futs.empty()) {
        return seastar::make_ready_future<R>(bo::success());
    }

    // Use a coroutine so that the waiting task is allocated only once
    return ([] (std::vector<seastar::future<R>> futs) noexcept -> seastar::future<R> {
        using error_type = typename R::error_type;
        std::variant<std::monostate, error_type, std::exception_ptr> result_state;
        while (!futs.empty()) {
            // TODO: Avoid try..catching here if seastar coroutines allow that
            // Or not? Explicit checks might be slower on the happy path
            try {
                auto res = co_await std::move(futs.back());
                if (!res) {
                    result_state = std::move(res).assume_error();
                }
            } catch (...) {
                result_state = std::current_exception();
            }
            futs.pop_back();
        }
        if (std::holds_alternative<std::monostate>(result_state)) {
            co_return bo::success();
        } else if (auto* error = std::get_if<error_type>(&result_state)) {
            co_return std::move(*error);
        } else {
            co_return seastar::coroutine::exception(
                    std::get<std::exception_ptr>(std::move(result_state)));
        }
    })(std::move(futs));
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
