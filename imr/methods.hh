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

#include "compound.hh"

namespace imr {
namespace methods {

template<template<class> typename Method>
struct trivial_method {
    template<typename... Args>
    static void run(Args&&...) noexcept { }
};

template<template<class> typename Method, typename T>
using has_trivial_method = std::is_base_of<trivial_method<Method>, Method<T>>;

namespace internal {

template<template<class> typename Method, typename...>
struct generate_method : trivial_method<Method> { };

template<template<class> typename Method, typename Structure, typename... Tags, typename... Types>
struct generate_method<Method, Structure, member<Tags, Types>...> {
    template<typename Context, typename... Args>
    static void run(uint8_t* ptr, const Context& context, Args&&... args) noexcept {
        auto view = Structure::make_view(ptr, context);
        meta::for_each<member<Tags, Types>...>([&] (auto member_type) {
            using member = std::remove_pointer_t<decltype(member_type)>;
            auto member_ptr = ptr + view.template offset_of<typename member::tag>();
            Method<typename member::type>::run(member_ptr,
                                               context.template context_for<typename member::tag>(member_ptr),
                                               std::forward<Args>(args)...);
        });
    }
};

template<template<class> typename Method, typename Tag, typename Type>
struct generate_method<Method, optional<Tag, Type>> {
    template<typename Context, typename... Args>
    static void run(uint8_t* ptr, const Context& context, Args&&... args) noexcept {
        if (context.template is_present<Tag>()) {
            Method<Type>::run(ptr,
                              context.template context_for<Tag>(ptr),
                              std::forward<Args>(args)...);
        }
    }
};

template<template<class> typename Method, typename Tag, typename... Members>
struct generate_method<Method, variant<Tag, Members...>> {
    template<typename Context, typename... Args>
    static void run(uint8_t* ptr, const Context& context, Args&&... args) noexcept {
        auto view = variant<Tag, Members...>::make_view(ptr, context);
        view.visit_type([&] (auto alternative_type) {
            using member = std::remove_pointer_t<decltype(alternative_type)>;
            Method<typename member::type>::run(ptr,
                                               context.template context_for<typename member::tag>(ptr),
                                               std::forward<Args>(args)...);
        }, context);
    }
};

template<template<class> typename Method>
struct member_has_trivial_method {
    template<typename T>
    struct type;
};
template<template<class> typename Method>
template<typename Tag, typename Type>
struct member_has_trivial_method<Method>::type<member<Tag, Type>> : has_trivial_method<Method, Type> { };

template<template<class> typename Method, typename T>
struct get_method;

template<template<class> typename Method, typename... Members>
struct get_method<Method, structure<Members...>>
    : std::conditional_t<meta::all_of<member_has_trivial_method<Method>::template type, Members...>,
                         trivial_method<Method>,
                         generate_method<Method, structure<Members...>, Members...>>
{ };

template<template<class> typename Method, typename Tag, typename Type>
struct get_method<Method, optional<Tag, Type>>
    : std::conditional_t<has_trivial_method<Method, Type>::value,
                         trivial_method<Method>,
                         generate_method<Method, optional<Tag, Type>>>
{ };

template<template<class> typename Method, typename Tag, typename... Members>
struct get_method<Method, variant<Tag, Members...>>
    : std::conditional_t<meta::all_of<member_has_trivial_method<Method>::template type, Members...>,
                         trivial_method<Method>,
                         generate_method<Method, variant<Tag, Members...>>>
{ };

template<template<class> typename Method, typename Tag, typename Type>
struct get_method<Method, tagged_type<Tag,Type>>
    : std::conditional_t<has_trivial_method<Method, Type>::value,
                         trivial_method<Method>,
                         Method<Type>>
{ };

}

template<typename T>
struct destructor : trivial_method<destructor> { };
using trivial_destructor = trivial_method<destructor>;

template<typename T>
using is_trivially_destructible = has_trivial_method<destructor, T>;

template<typename... Members>
struct destructor<structure<Members...>> : internal::get_method<destructor, structure<Members...>> { };

template<typename Tag, typename Type>
struct destructor<optional<Tag, Type>> : internal::get_method<destructor, optional<Tag, Type>> { };

template<typename Tag, typename... Members>
struct destructor<variant<Tag, Members...>> : internal::get_method<destructor, variant<Tag, Members...>> { };

template<typename T, typename Context = decltype(no_context)>
void destroy(uint8_t* ptr, const Context& context = no_context) {
    destructor<T>::run(ptr, context);
}

template<typename T>
struct mover : trivial_method<mover> { };
using trivial_mover = trivial_method<mover>;

template<typename T>
using is_trivially_movable = has_trivial_method<mover, T>;

template<typename... Members>
struct mover<structure<Members...>> : internal::get_method<mover, structure<Members...>> { };

template<typename Tag, typename Type>
struct mover<optional<Tag, Type>> : internal::get_method<mover, optional<Tag, Type>> { };

template<typename Tag, typename... Members>
struct mover<variant<Tag, Members...>> : internal::get_method<mover, variant<Tag, Members...>> { };

template<typename T, typename Context = decltype(no_context)>
void move(uint8_t* ptr, const Context& context = no_context) {
    mover<T>::run(ptr, context);
}

}
}
