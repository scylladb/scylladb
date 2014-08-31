/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef APPLY_HH_
#define APPLY_HH_

#include <tuple>

template <size_t... I>
struct index_list {
};

template <size_t Cur, size_t N, typename Result>
struct index_list_helper;

template <size_t N, size_t... I>
struct index_list_helper<N, N, index_list<I...>> {
    using type = index_list<I...>;
};

template <size_t Cur, size_t N, size_t... I>
struct index_list_helper<Cur, N, index_list<I...>> {
    using type = typename index_list_helper<Cur + 1, N, index_list<I..., Cur>>::type;
};

template <size_t N>
using make_index_list = typename index_list_helper<0, N, index_list<>>::type;

template <typename Func, typename Args, typename IndexList>
struct apply_helper;

template <typename Func, typename... T, size_t... I>
struct apply_helper<Func, std::tuple<T...>, index_list<I...>> {
    static auto apply(Func func, std::tuple<T...> args) {
        return func(std::get<I>(std::move(args))...);
    }
};

template <typename Func, typename... T>
auto apply(Func func, std::tuple<T...>&& args) {
    using helper = apply_helper<Func, std::tuple<T...>, make_index_list<sizeof...(T)>>;
    return helper::apply(std::move(func), std::move(args));
}

#endif /* APPLY_HH_ */
