/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/on_internal_error.hh"
#include <seastar/core/rwlock.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <vector>

// This class supports atomic inserts, removes, and iteration.
// All operations are synchronized using a read-write lock.
template <typename T>
class atomic_vector {
    std::vector<T> _vec;
    mutable seastar::rwlock _vec_lock;

public:
    void add(const T& value) {
        auto lock = _vec_lock.for_write().lock().get();
        auto unlock = seastar::defer([this] {
            _vec_lock.for_write().unlock();
        });
        _vec.push_back(value);
    }
    seastar::future<> remove(const T& value) {
        return with_lock(_vec_lock.for_write(), [this, value] {
            _vec.erase(std::remove(_vec.begin(), _vec.end(), value), _vec.end());
        });
    }

    // This must be called on a thread. The callback function must not
    // call remove or thread_for_each. If the callback function needs to
    // call this, use thread_for_each_nested instead.
    //
    // We would take callbacks that take a T&, but we had bugs in the
    // past with some of those callbacks holding that reference past a
    // preemption.
    void thread_for_each(seastar::noncopyable_function<void(T)> func) const {
        _vec_lock.for_read().lock().get();
        auto unlock = seastar::defer([this] {
            _vec_lock.for_read().unlock();
        });
        // Take a snapshot of the current contents while holding the read lock,
        // so that concurrent add() calls and possible reallocations won't
        // affect our iteration.
        auto snapshot = _vec;
        // We grab locks in both add() and remove(), so we iterate using
        // indexes on the snapshot to avoid concurrent modifications.
        for (size_t i = 0, n = snapshot.size(); i < n; ++i) {
            func(snapshot[i]);
        }
    }

    // This must be called on a thread. This should be used only from
    // the context of a thread_for_each callback, when the read lock is
    // already held. The callback function must not call remove or
    // thread_for_each.
    void thread_for_each_nested(seastar::noncopyable_function<void(T)> func) const {
        // When called in the context of thread_for_each, the read lock is
        // already held, so we don't need to acquire it again. Acquiring it
        // again could lead to a deadlock. This function must only be called
        // while holding the read lock on _vec_lock.

        // Take a snapshot of the current contents while the read lock is held,
        // so that concurrent add() calls and possible reallocations won't
        // affect our iteration.
        auto snapshot = _vec;
        // We grab locks in both add() and remove(), so we iterate using
        // indexes on the snapshot to avoid concurrent modifications.
        for (size_t i = 0, n = snapshot.size(); i < n; ++i) {
            func(snapshot[i]);
        }
    }

    // The callback function must not call remove.
    //
    // We would take callbacks that take a T&, but we had bugs in the
    // past with some of those callbacks holding that reference past a
    // preemption.
    seastar::future<> for_each(seastar::noncopyable_function<seastar::future<>(T)> func) const {
        auto holder = co_await _vec_lock.hold_read_lock();
        // Take a snapshot of the current contents while holding the read lock,
        // so that concurrent add() calls and possible reallocations won't
        // affect our iteration.
        auto snapshot = _vec;
        // We grab locks in both add() and remove(), so we iterate using
        // indexes on the snapshot to avoid concurrent modifications.
        for (size_t i = 0, n = snapshot.size(); i < n; ++i) {
            co_await func(snapshot[i]);
        }
    }
};
