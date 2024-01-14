/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <vector>

/// Represents a container which can preallocate space for future insertions
/// which can be used to reduce the number of overall memory re-allocation and item movement.
///
/// The number of items for which space is currently reserved is returned by capacity().
/// This includes items currently present in the container.
///
/// The number of items currently present is returned by size().
///
/// Invariant:
///
///   size() <= capacity()
///
/// Space is reserved by calling reserve(desired_capacity).
/// The post-condition of calling reserve() is:
///
///   capacity() >= desired_capacity
///
/// It is guaranteed insertion of (capacity() - size()) items does not
/// throw if T::value_type constructor and move constructor do not throw.
template <typename T>
concept ContainerWithCapacity = requires (T x, size_t desired_capacity, typename T::value_type e) {
    { x.reserve(desired_capacity) } -> std::same_as<void>;
    { x.capacity() } -> std::same_as<size_t>;
    { x.size() } -> std::same_as<size_t>;
};

static_assert(ContainerWithCapacity<std::vector<int>>);

/// Reserves space for at least desired_capacity - v.size() elements.
///
/// Amortizes space expansion so that a series of N calls to amortized_reserve(v, v.size() + 1)
/// starting from an empty container takes O(N) time overall.
///
/// Post-condition: v.capacity() >= desired_capacity
template <ContainerWithCapacity T>
void amortized_reserve(T& v, size_t desired_capacity) {
    if (desired_capacity > v.capacity()) {
        v.reserve(std::max(desired_capacity, v.capacity() * 2));
    }
}
