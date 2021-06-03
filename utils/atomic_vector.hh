/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <seastar/core/rwlock.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <vector>

// This class supports atomic removes (by using a lock and returning a
// future) and non atomic insert and iteration (by using indexes).
template <typename T>
class atomic_vector {
    std::vector<T> _vec;
    seastar::rwlock _vec_lock;

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
    void for_each(seastar::noncopyable_function<void(T)> func) {
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
};
