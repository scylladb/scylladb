/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>
#include <optional>
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
// After clearing slots with clear_at(), callers should call
// trim_front() to remove leading empty slots and reclaim memory.
// for_each() calls trim_front() automatically after iteration.
// clear_at() does not trim internally because callers may use it
// inside for_each(), where trimming would invalidate iteration.
template<std::move_constructible T>
class log_indexed_container {
    // boost::container::deque deallocates blocks when items are
    // removed, unlike std::deque which may retain them.
    boost::container::deque<std::optional<T>> _slots;
    index_t _base_idx{0};

public:
    // Returns a pointer to the value at the given index,
    // or nullptr if the slot is empty or out of range.
    T* find(index_t idx) {
        if (_slots.empty() || idx < _base_idx) {
            return nullptr;
        }
        auto off = (idx - _base_idx).value();
        if (off >= _slots.size()) {
            return nullptr;
        }
        auto& slot = _slots[off];
        return slot ? &*slot : nullptr;
    }

    // Inserts a value at the given index. The slot must not already
    // be occupied. Returns a reference to the inserted value.
    T& emplace(index_t idx, T value) {
        if (_slots.empty()) {
            _base_idx = idx;
            _slots.emplace_back(std::move(value));
        } else if (idx < _base_idx) {
            // The new index is before the current base. Prepend
            // empty slots to accommodate it.
            auto count = (_base_idx - idx).value();
            for (size_t i = 0; i < count; ++i) {
                _slots.emplace_front(std::nullopt);
            }
            _base_idx = idx;
            _slots.front().emplace(std::move(value));
        } else {
            auto off = (idx - _base_idx).value();
            if (off >= _slots.size()) {
                _slots.resize(off + 1);
            }
            SCYLLA_ASSERT(!_slots[off]);
            _slots[off].emplace(std::move(value));
        }
        return *_slots[(idx - _base_idx).value()];
    }

    // Clears the slot at the given index. The slot must be occupied.
    void clear_at(index_t idx) {
        SCYLLA_ASSERT(idx >= _base_idx);
        auto off = (idx - _base_idx).value();
        SCYLLA_ASSERT(off < _slots.size() && _slots[off]);
        _slots[off].reset();
    }

    // Removes empty slots from the front of the deque,
    // advancing _base_idx accordingly.
    void trim_front() {
        while (!_slots.empty() && !_slots.front()) {
            _slots.pop_front();
            _base_idx = _base_idx + index_t{1};
        }
    }

    bool empty() const {
        return _slots.empty();
    }

    // Returns the base index (the index corresponding to slots[0]).
    index_t base_index() const {
        return _base_idx;
    }

    // Iterates over all occupied slots, calling f(index_t, T&)
    // for each. The callback may call clear_at() on the current
    // element, but must not access it afterwards.
    template<typename F>
    requires std::invocable<F, index_t, T&>
    void for_each(F&& f) {
        for (size_t i = 0; i < _slots.size(); ++i) {
            if (_slots[i]) {
                f(_base_idx + index_t{i}, *_slots[i]);
            }
        }
        trim_front();
    }

    // Clears all slots.
    void clear() {
        _slots.clear();
        _base_idx = index_t{0};
    }
};

} // namespace raft
