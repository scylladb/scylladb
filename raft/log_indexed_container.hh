/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>
#include <optional>
#include <type_traits>
#include <boost/container/deque.hpp>

#include "utils/assert.hh"
#include "raft/raft.hh"

namespace raft {

// A container that maps raft log indices to values, providing O(1)
// access by index. Internally uses a deque of optional<T> slots
// where slot position corresponds to (index - base_index), and
// base_index is the index of the first slot in the deque.
//
// The container does not require indices to be inserted in order.
// Gaps (nullopt slots) between occupied slots are allowed.
//
// Invariant (1): the slot with the smallest index (the front), if
// any, is always occupied. extract() maintains it by trimming
// leading empty slots, which also reclaims memory. Consequently
// peek_front() gives O(1) access to the smallest-index element,
// letting callers consume the container from the front and stop
// early.
//
// Exception safety: every public method is either noexcept or provides
// the basic exception guarantee.
template<std::move_constructible T>
class log_indexed_container {
    // Why boost::container::deque: it frees each fixed-size data block
    // as its last element is popped, so memory tracks the live element
    // count. std::deque frees those data blocks too; the difference is
    // its internal map (the array of pointers to the blocks), which
    // only grows and is recentered on push, never shrunk. That would
    // only bite on a one-off spike in size; for this steadily draining
    // queue the map should remain small either way, so boost is just
    // the more conservative choice.
    //
    // Possible memory optimization: std::optional<T> spends an extra
    // discriminator byte (plus padding) per slot on top of T. It is
    // negligible for the current users, but if a future,
    // space-sensitive T ever makes it matter, and T has a spare
    // invalid value, the slots could switch to
    // seastar::optimized_optional<T>, which reuses that value as the
    // empty state and drops the byte.
    boost::container::deque<std::optional<T>> _slots;
    // The index corresponding to slot 0.
    index_t _base_idx{0};

    // Removes empty slots from the front of the deque, advancing
    // _base_idx accordingly, restoring invariant (1).
    void trim_front() noexcept {
        while (!_slots.empty() && !_slots.front()) {
            _slots.pop_front();
            _base_idx = _base_idx + index_t{1};
        }
    }

    // Position of idx in _slots. idx must be at or after _base_idx.
    [[nodiscard]] size_t offset_of(index_t idx) const noexcept {
        SCYLLA_ASSERT(idx >= _base_idx);
        return (idx - _base_idx).value();
    }

public:
    // Returns a pointer to the value at the given index,
    // or nullptr if the slot is empty or out of range.
    T* find(index_t idx) noexcept {
        if (_slots.empty() || idx < _base_idx) {
            return nullptr;
        }
        auto off = offset_of(idx);
        if (off >= _slots.size()) {
            return nullptr;
        }
        auto& slot = _slots[off];
        return slot ? &*slot : nullptr;
    }

    // Inserts a value at the given index. The slot must not already be
    // occupied. Returns a reference to the inserted value. On a throw the
    // container is unchanged, except it might be left with some trailing
    // empty slots.
    T& emplace(index_t idx, T value) {
        if (_slots.empty()) {
            _slots.emplace_back(std::move(value));
            _base_idx = idx;
        } else if (idx < _base_idx) {
            // The new index is before the current base. Prepend empty
            // slots to accommodate it, then fill the new front slot.
            auto count = (_base_idx - idx).value();
            size_t added = 0;
            try {
                for (; added < count; ++added) {
                    _slots.emplace_front(std::nullopt);
                }
                _slots.front().emplace(std::move(value));
            } catch (...) {
                // Roll back the prepended slots so the container (and
                // invariant (1)) is left exactly as before.
                for (size_t i = 0; i < added; ++i) {
                    _slots.pop_front();
                }
                throw;
            }
            _base_idx = idx;
        } else {
            auto off = offset_of(idx);
            if (off >= _slots.size()) {
                // Grow to reach the index. If the fill below throws, the
                // trailing empty slots are left in place: they are
                // harmless (invariant (1) holds, the slots read as
                // empty) and reclaimed as the container drains.
                _slots.resize(off + 1);
            }
            SCYLLA_ASSERT(!_slots[off]);
            _slots[off].emplace(std::move(value));
        }
        return *_slots[offset_of(idx)];
    }

    // Removes the slot at the given index and returns its value. The
    // slot must be occupied. Trims leading empty slots afterwards, so
    // invariant (1) is preserved and freed memory is reclaimed.
    T extract(index_t idx) noexcept(std::is_nothrow_move_constructible_v<T>) {
        auto off = offset_of(idx);
        SCYLLA_ASSERT(off < _slots.size() && _slots[off]);
        T value = std::move(*_slots[off]);
        _slots[off].reset();
        trim_front();
        return value;
    }

    // By invariant (1), true iff the container holds no live elements:
    // an all-nullopt deque cannot occur, so an empty deque is the only
    // way to have zero elements.
    bool empty() const noexcept {
        return _slots.empty();
    }

    // Returns the base index (the index corresponding to slots[0]).
    // By invariant (1), when the container is non-empty this is also
    // the index of the smallest-index (front) element.
    index_t base_index() const noexcept {
        return _base_idx;
    }

    // Returns a pointer to the smallest-index (front) element, whose
    // index is base_index(), or nullptr if the container is empty.
    T* peek_front() noexcept {
        return _slots.empty() ? nullptr : &*_slots.front();
    }

    // Iterates over all occupied slots in increasing index order,
    // calling f(index_t, T&) for each. The callback may freely mutate
    // the value through the T& reference, but must not mutate the
    // container's structure: it must not call emplace(), extract() or
    // clear(), as any of those would invalidate the ongoing iteration.
    template<typename F>
    requires std::invocable<F&, index_t, T&>
    void for_each(F&& f) {
        for (size_t i = 0; i < _slots.size(); ++i) {
            if (_slots[i]) {
                f(_base_idx + index_t{i}, *_slots[i]);
            }
        }
    }

    // Clears all slots.
    void clear() noexcept {
        _slots.clear();
        _base_idx = index_t{0};
    }
};

} // namespace raft
