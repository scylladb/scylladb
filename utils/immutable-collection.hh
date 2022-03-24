/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <type_traits>
#include <seastar/util/concepts.hh>
#include <utility>

namespace utils {

/*
 * Wraps a collection into immutable form.
 *
 * Immutability here means that the collection itself cannot be modified,
 * i.e. adding or removing elements is not possible. Read-only methods such
 * as find(), begin()/end(), lower_bound(), etc. are available and are
 * transparently forwarded to the underlying collection. Return values from
 * those methods are also returned as-is so it's pretty much like a const
 * reference on the collection.
 *
 * The important difference from the const reference is that obtained
 * elements or iterators are not necessarily const too, so it's possible
 * to modify the found or iterated over elements.
 */

template <typename Collection>
class immutable_collection {
    Collection* _col;

public:
    immutable_collection(Collection& col) noexcept : _col(&col) {}

#define DO_WRAP_METHOD(method, is_const)                                                                           \
    template <typename... Args>                                                                                    \
    auto method(Args&&... args) is_const noexcept(noexcept(std::declval<is_const Collection>().method(args...))) { \
        return _col->method(std::forward<Args>(args)...);                                                           \
    }

#define WRAP_CONST_METHOD(method)    \
    DO_WRAP_METHOD(method, const)

#define WRAP_METHOD(method)          \
    WRAP_CONST_METHOD(method)        \
    DO_WRAP_METHOD(method, )

    WRAP_METHOD(find)
    WRAP_METHOD(lower_bound)
    WRAP_METHOD(upper_bound)
    WRAP_METHOD(slice)
    WRAP_METHOD(lower_slice)
    WRAP_METHOD(upper_slice)

    WRAP_CONST_METHOD(empty)
    WRAP_CONST_METHOD(size)
    WRAP_CONST_METHOD(calculate_size)
    WRAP_CONST_METHOD(external_memory_usage)

    WRAP_METHOD(begin)
    WRAP_METHOD(end)
    WRAP_METHOD(rbegin)
    WRAP_METHOD(rend)
    WRAP_CONST_METHOD(cbegin)
    WRAP_CONST_METHOD(cend)
    WRAP_CONST_METHOD(crbegin)
    WRAP_CONST_METHOD(crend)

#undef WRAP_METHOD
#undef WRAP_CONST_METHOD
#undef DO_WRAP_METHOD
};

} // namespace utils
