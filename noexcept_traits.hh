/*
 * Copyright 2015-present ScyllaDB
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
//   T val2 = traits::unwrap(f.get0());
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
