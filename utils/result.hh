/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// A collection of utilities related to boost::outcome::result.

#include <utility>
#include <boost/outcome/policy/base.hpp>
#include <boost/outcome/result.hpp>
#include <seastar/core/future.hh>
#include "utils/exception_container.hh"

namespace bo = BOOST_OUTCOME_V2_NAMESPACE;

namespace utils {

// A policy which throws the container_error associated with the result
// if there was an attempt to access value while it was not present.
struct exception_container_throw_policy : bo::policy::base {
    template<class Impl> static constexpr void wide_value_check(Impl&& self) {
        if (!base::_has_value(self)) {
            base::_error(self).throw_me();
        }
    }

    template<class Impl> static constexpr void wide_error_check(Impl&& self) {
        if (!base::_has_error(self)) {
            throw bo::bad_result_access("no error");
        }
    }
};

template<typename R>
concept ExceptionContainerResult = bo::is_basic_result<R>::value && ExceptionContainer<typename R::error_type>;

template<typename F>
concept ExceptionContainerResultFuture = seastar::is_future<F>::value && ExceptionContainerResult<typename F::value_type>;

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

// Converts a result into either a ready or an exceptional future.
// Supports only results which has an exception_container as their error type.
template<typename R>
requires ExceptionContainerResult<R>
seastar::future<typename R::value_type> result_into_future(R&& res) {
    if (res) {
        if constexpr (std::is_void_v<typename R::value_type>) {
            return seastar::make_ready_future<>();
        } else {
            return seastar::make_ready_future<typename R::value_type>(std::move(res).assume_value());
        }
    } else {
        return std::move(res).assume_error().template into_exception_future<typename R::value_type>();
    }
}

// Converts a non-result future to return a successful result.
// It _does not_ convert exceptional futures to failed results.
template<typename R>
requires ExceptionContainerResult<R>
seastar::future<R> then_ok_result(seastar::future<typename R::value_type>&& f) {
    using T = typename R::value_type;
    if constexpr (std::is_void_v<T>) {
        return f.then([] {
            return seastar::make_ready_future<R>(bo::success());
        });
    } else {
        return f.then([] (T&& t) {
            return seastar::make_ready_future<R>(bo::success(std::move(t)));
        });
    }
}

namespace internal {

template<typename T, typename... Exs>
using result_with_exception = bo::result<T, exception_container<Exs...>, exception_container_throw_policy>;

template<typename C, typename Arg>
struct result_wrapped_call_traits {
    static_assert(ExceptionContainerResult<Arg>);
    using return_type = decltype(seastar::futurize_invoke(std::declval<C>(), std::declval<typename Arg::value_type>()));

    static auto invoke_with_value(C& c, Arg&& arg) {
        // Arg must have a value
        return seastar::futurize_invoke(c, std::move(arg).value());
    }
};

template<typename C, typename... Exs>
struct result_wrapped_call_traits<C, result_with_exception<void, Exs...>> {
    using return_type = decltype(seastar::futurize_invoke(std::declval<C>()));

    static auto invoke_with_value(C& c, result_with_exception<void, Exs...>&& arg) {
        return seastar::futurize_invoke(c);
    }
};

template<typename C>
struct result_wrapper {
    C c;

    result_wrapper(C&& c) : c(std::move(c)) {}

    template<typename InputResult>
    requires ExceptionContainerResult<InputResult>
    auto operator()(InputResult arg) {
        using traits = internal::result_wrapped_call_traits<C, InputResult>;
        using return_type = typename traits::return_type;
        static_assert(ExceptionContainerResultFuture<return_type>,
                "the return type of the call must be a future<result<T>> for some T");

        using return_result_type = typename return_type::value_type;
        static_assert(std::is_same_v<typename InputResult::error_type, typename return_result_type::error_type>,
                "the error type of accepted result and returned result are not the same");

        if (arg) {
            return traits::invoke_with_value(c, std::move(arg));
        } else {
            return seastar::make_ready_future<return_result_type>(bo::failure(std::move(arg).assume_error()));
        }
    }
};

}

// Converts a callable to accept a result<T> instead of T.
// Given a callable which can be called with following arguments and return type:
//
//   (T) -> future<result<U>>
//
// ...returns a new callable which can be called with the following signature:
//
//   (result<T>) -> future<result<U>>
//
// On call, the adapted callable is run only if the result<T> contains a value.
// Otherwise, the adapted callable is not called and the error is returned immediately.
// The resulting callable must receive result<> as an argument when being called,
// it won't be automatically converted to result<>.
template<typename C>
auto result_wrap(C&& c) {
    return internal::result_wrapper<C>(std::move(c));
}

}
