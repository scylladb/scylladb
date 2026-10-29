/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <utility>
#include <vector>

namespace utils {

// Keeps the `capacity` largest elements of a stream, ordered by `Compare` on the projection of each
// element. The elements are kept in a min heap, so an element that is not larger than the smallest
// one kept costs one comparison, and an element that is kept costs O(log capacity). An element
// that is only equal to the smallest one kept is not added.
template <typename T, typename Compare = std::less<>, typename Proj = std::identity>
requires std::indirect_strict_weak_order<Compare, std::projected<const T*, Proj>>
class largest_k {
    std::vector<T> _heap;
    size_t _capacity;
    [[no_unique_address]] Compare _compare;
    [[no_unique_address]] Proj _proj;

    bool smaller(const T& lhs, const T& rhs) {
        return std::invoke(_compare, std::invoke(_proj, lhs), std::invoke(_proj, rhs));
    }

    // The order of the heap, the reverse of smaller(), so that the front of the heap is the
    // smallest element kept.
    auto heap_order() {
        return [this] (const T& lhs, const T& rhs) { return smaller(rhs, lhs); };
    }

public:
    explicit largest_k(size_t capacity, Compare compare = {}, Proj proj = {})
        : _capacity(capacity)
        , _compare(std::move(compare))
        , _proj(std::move(proj)) {
        _heap.reserve(capacity);
    }

    // Adds `element` if it is one of the `capacity` largest elements so far. If there is no space,
    // removes the smallest element kept. Returns whether `element` was added.
    bool push(T element) {
        if (_heap.size() < _capacity) {
            _heap.push_back(std::move(element));
            std::ranges::push_heap(_heap, heap_order());
            return true;
        }
        if (_heap.empty() || !smaller(_heap.front(), element)) {
            return false;
        }
        std::ranges::pop_heap(_heap, heap_order());
        _heap.back() = std::move(element);
        std::ranges::push_heap(_heap, heap_order());
        return true;
    }

    size_t size() const noexcept {
        return _heap.size();
    }

    size_t capacity() const noexcept {
        return _capacity;
    }

    // Returns the elements kept in ascending order. Equal elements are in no specific order.
    std::vector<T> take_sorted() && {
        std::ranges::sort(_heap, [this] (const T& lhs, const T& rhs) { return smaller(lhs, rhs); });
        return std::move(_heap);
    }
};

} // namespace utils
