/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iterator>
#include <seastar/core/coroutine.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <type_traits>
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

namespace internal {

template<typename Reducer, ExceptionContainer ExCont>
struct result_reducer_traits {
    using result_type = bo::result<void, ExCont, exception_container_throw_policy>;

    static seastar::future<result_type> maybe_call_get(Reducer&& r) {
        return seastar::make_ready_future<result_type>(bo::success());
    }
};

template<typename Reducer, ExceptionContainer ExCont>
requires requires (Reducer r) {
    { r.get() };
}
struct result_reducer_traits<Reducer, ExCont> {
    using original_type = seastar::futurize_t<decltype(std::declval<Reducer>().get())>;
    using result_type = bo::result<typename original_type::value_type, ExCont, exception_container_throw_policy>;

    static seastar::future<result_type> maybe_call_get(Reducer&& r) {
        auto x = r.get();
        if constexpr (seastar::Future<decltype(x)>) {
            return x.then([] (auto&& v) {
                return seastar::make_ready_future<result_type>(bo::success(std::move(v)));
            });
        } else {
            return seastar::make_ready_future<result_type>(bo::success(std::move(x)));
        }
    }
};

template<typename Reducer, ExceptionContainer ExCont>
struct result_map_reduce_unary_adapter {
private:
    // We could in theory just use result<Reducer> here and turn it into
    // a failed result on receiving an error, however that would destroy
    // the Reducer and some map_reduce usages may assume that the reducer
    // is alive until all mapper calls finish (e.g. it may hold a smart pointer
    // to some memory used by mappers), therefore it is safer to keep it
    // separate from the error and only destroy it when the wrapper
    // is destroyed.
    Reducer _reducer;
    ExCont _excont;

    using reducer_traits = result_reducer_traits<Reducer, ExCont>;

public:
    result_map_reduce_unary_adapter(Reducer&& reducer)
            : _reducer(std::forward<Reducer>(reducer))
    { }

    template<ExceptionContainerResult Arg>
    seastar::future<> operator()(Arg&& arg) {
        if (!_excont && arg) {
            return seastar::futurize_invoke(_reducer, std::move(arg).assume_value());
        }
        if (_excont) {
            // We already got an error before, so ignore the new one
            return seastar::make_ready_future<>();
        }
        // `arg` must be a failed result
        _excont = std::move(arg).assume_error();
        return seastar::make_ready_future<>();
    }

    seastar::future<typename reducer_traits::result_type> get() {
        if (_excont) {
            return seastar::make_ready_future<typename reducer_traits::result_type>(bo::failure(std::move(_excont)));
        }
        return reducer_traits::maybe_call_get(std::move(_reducer));
    }
};

}

template<typename Iterator, typename Mapper, typename Reducer>
requires requires (Iterator i, Mapper mapper, Reducer reduce) {
    *i++;
    { i != i } -> std::convertible_to<bool>;
    { mapper(*i) } -> ExceptionContainerResultFuture<>;
    { seastar::futurize_invoke(reduce, seastar::futurize_invoke(mapper, *i).get().value()) }
            -> std::same_as<seastar::future<>>;
}
inline
auto
result_map_reduce(Iterator begin, Iterator end, Mapper&& mapper, Reducer&& reducer) {
    using result_type = std::remove_reference_t<decltype(seastar::futurize_invoke(mapper, *begin).get())>;
    using exception_container_type = typename result_type::error_type;
    using adapter_type = internal::result_map_reduce_unary_adapter<Reducer, exception_container_type>;
    return seastar::map_reduce(
            std::move(begin),
            std::move(end),
            std::forward<Mapper>(mapper),
            adapter_type(std::forward<Reducer>(reducer)));
}

namespace internal {

template<ExceptionContainerResult Left, ExceptionContainerResult Right, typename Reducer>
requires ResultRebindableTo<Right, Left>
struct result_map_reduce_binary_adapter {
private:
    using left_value_type = typename Left::value_type;
    using right_value_type = typename Right::value_type;
    using return_type = std::invoke_result<Reducer, left_value_type&&, right_value_type&&>;

    Reducer _reducer;

public:
    result_map_reduce_binary_adapter(Reducer&& reducer)
            : _reducer(std::forward<Reducer>(reducer))
    { }

    Left operator()(Left&& left, Right&& right) {
        if (!left) {
            return std::move(left);
        }
        if (!right) {
            return std::move(right).as_failure();
        }
        return _reducer(std::move(left).assume_value(), std::move(right).assume_value());
    }
};

}

template<typename Iterator, typename Mapper, typename Initial, typename Reducer>
requires requires (Iterator i, Mapper mapper, Initial initial, Reducer reduce) {
    *i++;
    { i != i } -> std::convertible_to<bool>;
    { mapper(*i) } -> ExceptionContainerResultFuture<>;
    { reduce(std::move(initial), mapper(*i).get().value()) }
            -> std::convertible_to<rebind_result<Initial, std::remove_reference_t<decltype(mapper(*i).get())>>>;
}
inline
auto
result_map_reduce(Iterator begin, Iterator end, Mapper&& mapper, Initial initial, Reducer reduce)
        -> seastar::future<rebind_result<Initial, std::remove_reference_t<decltype(mapper(*begin).get())>>> {

    using right_type = std::remove_reference_t<decltype(mapper(*begin).get())>;
    using left_type = rebind_result<Initial, right_type>;

    return seastar::map_reduce(
            std::move(begin),
            std::move(end),
            std::forward<Mapper>(mapper),
            left_type(std::move(initial)),
            internal::result_map_reduce_binary_adapter<left_type, right_type, Reducer>(std::move(reduce)));
}

template<typename AsyncAction, typename StopCondition>
requires requires (StopCondition cond, AsyncAction act) {
    { cond() } -> std::same_as<bool>;
    { seastar::futurize_invoke(act) } -> ExceptionContainerResultFuture<>;
}
inline
auto
result_do_until(StopCondition stop_cond, AsyncAction action) {
    // TODO: Constrain the result of act() better
    using future_type = seastar::futurize_t<std::invoke_result_t<AsyncAction>>;
    using result_type = typename future_type::value_type;
    static_assert(std::is_void_v<typename result_type::value_type>,
            "The result type of the action must be future<result<void>>");
    for (;;) {
        try {
            if (stop_cond()) {
                return seastar::make_ready_future<result_type>(bo::success());
            }
        } catch (...) {
            return seastar::current_exception_as_future<result_type>();
        }
        auto f = seastar::futurize_invoke(action);
        if (f.available() && !seastar::need_preempt()) {
            if (f.failed()) {
                return f;
            }
            result_type&& res = f.get();
            if (!res) {
                return seastar::make_ready_future<result_type>(std::move(res));
            }
        } else {
            return ([] (future_type f, StopCondition stop_cond, AsyncAction action) -> seastar::future<result_type> {
                for (;;) {
                    // No need to manually maybe_yield because co_await does that for us
                    result_type res = co_await std::move(f);
                    if (!res) {
                        co_return res;
                    }
                    if (stop_cond()) {
                        co_return bo::success();
                    }
                    f = seastar::futurize_invoke(action);
                }
            })(std::move(f), std::move(stop_cond), std::move(action));
        }
    }
}

template<typename AsyncAction>
requires requires (AsyncAction act) {
    { seastar::futurize_invoke(act) } -> ExceptionContainerResultFuture<>;
}
inline
auto result_repeat(AsyncAction&& action) noexcept {
    using future_type = seastar::futurize_t<std::invoke_result_t<AsyncAction>>;
    using result_type = typename future_type::value_type;
    static_assert(std::is_same_v<seastar::stop_iteration, typename result_type::value_type>, "bad AsyncAction signature");
    using return_result_type = rebind_result<void, result_type>;
    for (;;) {
        auto f = seastar::futurize_invoke(action);

        if (f.available() && !seastar::need_preempt()) {
            if (f.failed()) {
                return seastar::make_exception_future<return_result_type>(f.get_exception());
            }
            result_type&& res = f.get();
            if (!res) {
                return seastar::make_ready_future<return_result_type>(std::move(res).as_failure());
            }
            if (res.value() == seastar::stop_iteration::yes) {
                return seastar::make_ready_future<return_result_type>(bo::success());
            }
        } else {
            return ([] (future_type f, AsyncAction action) -> seastar::future<return_result_type> {
                for (;;) {
                    // No need to manually maybe_yield because co_await does that for us
                    auto&& res = co_await std::move(f);
                    if (!res) {
                        co_return std::move(res).as_failure();
                    }
                    if (res.value() == seastar::stop_iteration::yes) {
                        co_return bo::success();
                    }
                    f = seastar::futurize_invoke(action);
                }
            })(std::move(f), std::move(action));
        }
    }
}

}
