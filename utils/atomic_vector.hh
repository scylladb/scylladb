/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/rwlock.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/coroutine.hh>

#include <vector>

// This class supports atomic removes (by using a lock and returning a
// future) and non atomic insert and iteration (by using indexes).
template <typename T>
class atomic_vector {
    std::vector<T> _vec;
    mutable seastar::rwlock _vec_lock;

public:
    void add(const T& value) {
        _vec.push_back(value);
    }
    seastar::future<> remove(const T& value) {
        return with_lock(_vec_lock.for_write(), [this, value] {
            _vec.erase(std::remove(_vec.begin(), _vec.end(), value), _vec.end());
        });
    }

    // This must be called on a thread. The callback function must not
    // call remove.
    //
    // We would take callbacks that take a T&, but we had bugs in the
    // past with some of those callbacks holding that reference past a
    // preemption.
    void thread_for_each(seastar::noncopyable_function<void(T)> func) const {
        _vec_lock.for_read().lock().get();
        auto unlock = seastar::defer([this] {
            _vec_lock.for_read().unlock();
        });
        // We grab a lock in remove(), but not in add(), so we
        // iterate using indexes to guard against the vector being
        // reallocated.
        for (size_t i = 0, n = _vec.size(); i < n; ++i) {
            func(_vec[i]);
        }
    }

    // The callback function must not call remove.
    //
    // We would take callbacks that take a T&, but we had bugs in the
    // past with some of those callbacks holding that reference past a
    // preemption.
    seastar::future<> for_each(seastar::noncopyable_function<seastar::future<>(T)> func) const {
        auto holder = co_await _vec_lock.hold_read_lock();
        // We grab a lock in remove(), but not in add(), so we
        // iterate using indexes to guard against the vector being
        // reallocated.
        for (size_t i = 0, n = _vec.size(); i < n; ++i) {
            co_await func(_vec[i]);
        }
    }
};
