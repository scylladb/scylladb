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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "apply.hh"
#include <utility>
#include <memory>
#include <tuple>

/// \addtogroup future-util
/// @{

/// do_with() holds an object alive for the duration until a future
/// completes, and allow the code involved in making the future
/// complete to have easy access to this object.
///
/// do_with() takes two arguments: The first is an temporary object (rvalue),
/// the second is a function returning a future (a so-called "promise").
/// The function is given (a moved copy of) this temporary object, by
/// reference, and it is ensured that the object will not be destructed until
/// the completion of the future returned by the function.
///
/// do_with() returns a future which resolves to whatever value the given future
/// (returned by the given function) resolves to. This returned value must not
/// contain references to the temporary object, as at that point the temporary
/// is destructed.
///
/// \param rvalue a temporary value to protect while \c f is running
/// \param f a callable, accepting an lvalue reference of the same type
///          as \c rvalue, that will be accessible while \c f runs
/// \return whatever \c f returns
template<typename T, typename F>
inline
auto do_with(T&& rvalue, F&& f) {
    auto obj = std::make_unique<T>(std::forward<T>(rvalue));
    auto fut = f(*obj);
    return fut.finally([obj = std::move(obj)] () {});
}

/// \cond internal
template <typename Tuple, size_t... Idx>
inline
auto
cherry_pick_tuple(std::index_sequence<Idx...>, Tuple&& tuple) {
    return std::make_tuple(std::get<Idx>(std::forward<Tuple>(tuple))...);
}
/// \endcond

/// Multiple argument variant of \ref do_with(T&& rvalue, F&& f).
///
/// This is the same as \ref do_with(T&& tvalue, F&& f), but accepts
/// two or more rvalue parameters, which are held in memory while
/// \c f executes.  \c f will be called with all arguments as
/// reference parameters.
template <typename T1, typename T2, typename T3_or_F, typename... More>
inline
auto
do_with(T1&& rv1, T2&& rv2, T3_or_F&& rv3, More&&... more) {
    auto all = std::forward_as_tuple(
            std::forward<T1>(rv1),
            std::forward<T2>(rv2),
            std::forward<T3_or_F>(rv3),
            std::forward<More>(more)...);
    constexpr size_t nr = std::tuple_size<decltype(all)>::value - 1;
    using idx = std::make_index_sequence<nr>;
    auto&& just_values = cherry_pick_tuple(idx(), std::move(all));
    auto&& just_func = std::move(std::get<nr>(std::move(all)));
    auto obj = std::make_unique<std::remove_reference_t<decltype(just_values)>>(std::move(just_values));
    auto fut = apply(just_func, *obj);
    return fut.finally([obj = std::move(obj)] () {});
}

/// @}
