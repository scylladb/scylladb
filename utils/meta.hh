/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

namespace meta {

// Wrappers that allows returning a list of types. All helpers defined in this
// file accept both unpacked and packed lists of types.
template<typename... Ts>
struct list { };

namespace internal {

template<bool... Vs>
constexpr ssize_t do_find_if_unpacked() {
    ssize_t i = -1;
    ssize_t j = 0;
    (..., ((Vs && i == -1) ? i = j : j++));
    return i;
}

template<ssize_t N>
struct negative_to_empty : std::integral_constant<size_t, N> { };

template<>
struct negative_to_empty<-1> { };

template<typename T>
struct is_same_as {
    template<typename U>
    using type = std::is_same<T, U>;
};

template<template<class> typename Predicate, typename... Ts>
struct do_find_if : internal::negative_to_empty<internal::do_find_if_unpacked<Predicate<Ts>::value...>()> { };

template<template<class> typename Predicate, typename... Ts>
struct do_find_if<Predicate, meta::list<Ts...>> : internal::negative_to_empty<internal::do_find_if_unpacked<Predicate<Ts>::value...>()> { };

}

// Returns the index of the first type in the list of types list of types Ts for
// which Predicate<T::value is true.
template<template<class> typename Predicate, typename... Ts>
constexpr size_t find_if = internal::do_find_if<Predicate, Ts...>::value;

// Returns the index of the first occurrence of type T in the list of types Ts.
template<typename T, typename... Ts>
constexpr size_t find = find_if<internal::is_same_as<T>::template type, Ts...>;

namespace internal {

template<size_t N, typename... Ts>
struct do_get_unpacked { };

template<size_t N, typename T, typename... Ts>
struct do_get_unpacked<N, T, Ts...> : do_get_unpacked<N - 1, Ts...> { };

template<typename T, typename... Ts>
struct do_get_unpacked<0, T, Ts...> {
    using type = T;
};

template<size_t N, typename... Ts>
struct do_get : do_get_unpacked<N, Ts...> { };

template<size_t N, typename... Ts>
struct do_get<N, meta::list<Ts...>> : do_get_unpacked<N, Ts...> { };

}

// Returns the Nth type in the provided list of types.
template<size_t N, typename... Ts>
using get = typename internal::do_get<N, Ts...>::type;

namespace internal {

template<size_t N, typename Result, typename... Ts>
struct do_take_unpacked { };

template<typename... Ts>
struct do_take_unpacked<0, list<Ts...>> {
    using type = list<Ts...>;
};

template<typename... Ts, typename U, typename... Us>
struct do_take_unpacked<0, list<Ts...>, U, Us...> {
    using type = list<Ts...>;
};

template<size_t N, typename... Ts, typename U, typename... Us>
struct do_take_unpacked<N, list<Ts...>, U, Us...> {
    using type = typename do_take_unpacked<N - 1, list<Ts..., U>, Us...>::type;
};

template<size_t N, typename Result, typename... Ts>
struct do_take : do_take_unpacked<N, Result, Ts...> { };


template<size_t N, typename Result, typename... Ts>
struct do_take<N, Result, meta::list<Ts...>> : do_take_unpacked<N, Result, Ts...> { };

}

// Returns a list containing N first elements of the provided list of types.
template<size_t N, typename... Ts>
using take = typename internal::do_take<N, list<>, Ts...>::type;

namespace internal {

template<typename... Ts>
struct do_for_each_unpacked {
    template<typename Function>
    static constexpr void run(Function&& fn) {
        (..., fn(static_cast<Ts*>(nullptr)));
    }
};

template<typename... Ts>
struct do_for_each : do_for_each_unpacked<Ts...> { };

template<typename... Ts>
struct do_for_each<meta::list<Ts...>> : do_for_each_unpacked<Ts...> { };

}

// Executes the provided function for each element in the provided list of
// types. For each type T the Function is called with an argument of type T*.
template<typename... Ts, typename Function>
constexpr void for_each(Function&& fn) {
    internal::do_for_each<Ts...>::run(std::forward<Function>(fn));
};

namespace internal {

template<typename... Ts>
struct get_size : std::integral_constant<size_t, sizeof...(Ts)> { };

template<typename... Ts>
struct get_size<meta::list<Ts...>> : std::integral_constant<size_t, sizeof...(Ts)> { };

}

// Returns the size of a list of types.
template<typename... Ts>
constexpr size_t size = internal::get_size<Ts...>::value;

template<template <class> typename Predicate, typename... Ts>
static constexpr const bool all_of = std::conjunction_v<Predicate<Ts>...>;

}
