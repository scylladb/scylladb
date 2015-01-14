/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef APPLY_HH_
#define APPLY_HH_

#include <tuple>
#include <utility>

template <typename Func, typename Args, typename IndexList>
struct apply_helper;

template <typename Func, typename... T, size_t... I>
struct apply_helper<Func, std::tuple<T...>, std::index_sequence<I...>> {
    static auto apply(Func func, std::tuple<T...> args) {
        return func(std::get<I>(std::move(args))...);
    }
};

template <typename Func, typename... T>
auto apply(Func func, std::tuple<T...>&& args) {
    using helper = apply_helper<Func, std::tuple<T...>, std::index_sequence_for<T...>>;
    return helper::apply(std::move(func), std::move(args));
}

#endif /* APPLY_HH_ */
