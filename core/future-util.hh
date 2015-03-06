/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CORE_FUTURE_UTIL_HH_
#define CORE_FUTURE_UTIL_HH_

#include "future.hh"
#include "shared_ptr.hh"
#include "reactor.hh"
#include <tuple>

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

// The AsyncAction concept represents an action which can complete later than
// the actual function invocation. It is represented by a function which
// returns a future which resolves when the action is done.

template<typename AsyncAction, typename StopCondition>
static inline
void do_until_continued(StopCondition&& stop_cond, AsyncAction&& action, promise<> p) {
    while (!stop_cond()) {
        try {
            auto&& f = action();
            if (!f.available()) {
                std::move(f).then_wrapped([action = std::forward<AsyncAction>(action),
                    stop_cond = std::forward<StopCondition>(stop_cond), p = std::move(p)](std::result_of_t<AsyncAction()> fut) mutable {
                    try {
                        fut.get();
                        do_until_continued(stop_cond, std::forward<AsyncAction>(action), std::move(p));
                    } catch(...) {
                        p.set_exception(std::current_exception());
                    }
                });
                return;
            }

            if (f.failed()) {
                f.forward_to(std::move(p));
                return;
            }
        } catch (...) {
            p.set_exception(std::current_exception());
            return;
        }
    }

    p.set_value();
}

// Invokes given action until it fails or given condition evaluates to true.
template<typename AsyncAction, typename StopCondition>
static inline
future<> do_until(StopCondition&& stop_cond, AsyncAction&& action) {
    promise<> p;
    auto f = p.get_future();
    do_until_continued(std::forward<StopCondition>(stop_cond),
        std::forward<AsyncAction>(action), std::move(p));
    return f;
}

// Invoke given action until it fails.
template<typename AsyncAction>
static inline
future<> keep_doing(AsyncAction&& action) {
    while (task_quota) {
        auto f = action();

        if (!f.available()) {
            return f.then([action = std::forward<AsyncAction>(action)] () mutable {
                return keep_doing(std::forward<AsyncAction>(action));
            });
        }

        if (f.failed()) {
            return std::move(f);
        }

        --task_quota;
    }

    promise<> p;
    auto f = p.get_future();
    schedule(make_task([action = std::forward<AsyncAction>(action), p = std::move(p)] () mutable {
        keep_doing(std::forward<AsyncAction>(action)).forward_to(std::move(p));
    }));
    return f;
}

template<typename Iterator, typename AsyncAction>
static inline
future<> do_for_each(Iterator begin, Iterator end, AsyncAction&& action) {
    if (begin == end) {
        return make_ready_future<>();
    }
    while (true) {
        auto f = action(*begin++);
        if (begin == end) {
            return f;
        }
        if (!f.available()) {
            return std::move(f).then([action = std::forward<AsyncAction>(action),
                    begin = std::move(begin), end = std::move(end)] () mutable {
                return do_for_each(std::move(begin), std::move(end), std::forward<AsyncAction>(action));
            });
        }
        if (f.failed()) {
            return std::move(f);
        }
    }
}

template <typename... Future>
future<std::tuple<Future...>> when_all(Future&&... fut);

template <>
inline
future<std::tuple<>>
when_all() {
    return make_ready_future<std::tuple<>>();
}

// gcc can't capture a parameter pack, so we need to capture
// a tuple and use apply.  But apply cannot accept an overloaded
// function pointer as its first parameter, so provide this instead.
struct do_when_all {
    template <typename... Future>
    future<std::tuple<Future...>> operator()(Future&&... fut) const {
        return when_all(std::move(fut)...);
    }
};

template <typename Future, typename... Rest>
inline
future<std::tuple<Future, Rest...>>
when_all(Future&& fut, Rest&&... rest) {
    return std::move(fut).then_wrapped(
            [rest = std::make_tuple(std::move(rest)...)] (Future&& fut) mutable {
        return apply(do_when_all(), std::move(rest)).then_wrapped(
                [fut = std::move(fut)] (future<std::tuple<Rest...>>&& rest) mutable {
            return make_ready_future<std::tuple<Future, Rest...>>(
                    std::tuple_cat(std::make_tuple(std::move(fut)), std::get<0>(rest.get())));
        });
    });
}

template <typename T>
struct reducer_with_get_traits {
    using result_type = decltype(std::declval<T>().get());
    using future_type = future<result_type>;
    static future_type maybe_call_get(future<> f, lw_shared_ptr<T> r) {
        return f.then([r = std::move(r)] () mutable {
            return make_ready_future<result_type>(std::move(*r).get());
        });
    }
};

template <typename T, typename V = void>
struct reducer_traits {
    using future_type = future<>;
    static future_type maybe_call_get(future<> f, lw_shared_ptr<T> r) {
        return f.then([r = std::move(r)] {});
    }
};

template <typename T>
struct reducer_traits<T, decltype(std::declval<T>().get(), void())> : public reducer_with_get_traits<T> {};

// @Mapper is a callable which transforms values from the iterator range
// into a future<T>. @Reducer is an object which can be called with T as
// parameter and yields a future<>. It may have a get() method which returns
// a value of type U which holds the result of reduction. This value is wrapped
// in a future and returned by this function. If the reducer has no get() method
// then this function returns future<>.
//
// TODO: specialize for non-deferring reducer
template <typename Iterator, typename Mapper, typename Reducer>
inline
auto
map_reduce(Iterator begin, Iterator end, Mapper&& mapper, Reducer&& r)
    -> typename reducer_traits<Reducer>::future_type
{
    auto r_ptr = make_lw_shared(std::forward<Reducer>(r));
    future<> ret = make_ready_future<>();
    while (begin != end) {
        ret = mapper(*begin++).then([ret = std::move(ret), r_ptr] (auto value) mutable {
            return ret.then([value = std::move(value), r_ptr] () mutable {
                return (*r_ptr)(std::move(value));
            });
        });
    }
    return reducer_traits<Reducer>::maybe_call_get(std::move(ret), r_ptr);
}

// Implements @Reducer concept. Calculates the result by
// adding elements to the accumulator.
template <typename Result, typename Addend = Result>
class adder {
private:
    Result _result;
public:
    future<> operator()(const Addend& value) {
        _result += value;
        return make_ready_future<>();
    }
    Result get() && {
        return std::move(_result);
    }
};

static inline
future<> now() {
    return make_ready_future<>();
}

#endif /* CORE_FUTURE_UTIL_HH_ */
