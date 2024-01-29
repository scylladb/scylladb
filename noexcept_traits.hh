/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <type_traits>
#include <memory>
#include <seastar/core/future.hh>

#include "seastarx.hh"

#pragma once

//
// Utility for adapting types which are not nothrow move constructible into such
// by wrapping them if necessary.
//
// Example usage:
//
//   T val{};
//   using traits = noexcept_movable<T>;
//   auto f = make_ready_future<typename traits::type>(traits::wrap(std::move(val)));
//   T val2 = traits::unwrap(f.get());
//

template<typename T>
struct noexcept_movable;

template<typename T>
requires std::is_nothrow_move_constructible_v<T>
struct noexcept_movable<T> {
    using type = T;

    static type wrap(T&& v) {
        return std::move(v);
    }

    static future<T> wrap(future<T>&& v) {
        return std::move(v);
    }

    static T unwrap(type&& v) {
        return std::move(v);
    }

    static future<T> unwrap(future<type>&& v) {
        return std::move(v);
    }
};

template<typename T>
requires (!std::is_nothrow_move_constructible_v<T>)
struct noexcept_movable<T> {
    using type = std::unique_ptr<T>;

    static type wrap(T&& v) {
        return std::make_unique<T>(std::move(v));
    }

    static T unwrap(type&& v) {
        return std::move(*v);
    }
};

template<typename T>
using noexcept_movable_t = typename noexcept_movable<T>::type;
