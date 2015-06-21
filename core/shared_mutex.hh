/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "future.hh"
#include "circular_buffer.hh"

namespace seastar {

/// \addtogroup fiber-module
/// @{

/// \brief Shared/exclusive mutual exclusion.
///
/// Similar to \c std::shared_mutex, this class provides protection
/// for a shared resource, with two levels of access protection: shared
/// and exclusive.  Shared access allows multiple tasks to access the
/// shared resource concurrently, while exclusive access allows just
/// one task to access the resource at a time.
///
/// Note that many seastar tasks do not require protection at all,
/// since the seastar scheduler is not preemptive; however tasks that do
/// (by waiting on a future) may require explicit locking.
///
/// The \ref with_shared(shared_mutex&, Func&&) and
/// \ref with_lock(shared_mutex&, Func&&) provide exception-safe
/// wrappers for use with \c shared_mutex.
///
/// \see semaphore simpler mutual exclusion
class shared_mutex {
    unsigned _readers = 0;
    bool _writer = false;
    struct waiter {
        waiter(promise<>&& pr, bool for_write) : pr(std::move(pr)), for_write(for_write) {}
        promise<> pr;
        bool for_write;
    };
    circular_buffer<waiter> _waiters;
public:
    shared_mutex() = default;
    shared_mutex(shared_mutex&&) = default;
    shared_mutex& operator=(shared_mutex&&) = default;
    shared_mutex(const shared_mutex&) = delete;
    void operator=(const shared_mutex&) = delete;
    /// Lock the \c shared_mutex for shared access
    ///
    /// \return a future that becomes ready when no exclusive access
    ///         is granted to anyone.
    future<> lock_shared() {
        if (!_writer && _waiters.empty()) {
            ++_readers;
            return make_ready_future<>();
        }
        _waiters.emplace_back(promise<>(), false);
        return _waiters.back().pr.get_future();
    }
    /// Unlocks a \c shared_mutex after a previous call to \ref lock_shared().
    void unlock_shared() {
        --_readers;
        wake();
    }
    /// Lock the \c shared_mutex for exclusive access
    ///
    /// \return a future that becomes ready when no access, shared or exclusive
    ///         is granted to anyone.
    future<> lock() {
        if (!_readers && !_writer) {
            _writer = true;
            return make_ready_future<>();
        }
        _waiters.emplace_back(promise<>(), true);
        return _waiters.back().pr.get_future();
    }
    /// Unlocks a \c shared_mutex after a previous call to \ref lock().
    void unlock() {
        _writer = false;
        wake();
    }
private:
    void wake() {
        while (!_waiters.empty()) {
            auto& w = _waiters.front();
            // note: _writer == false in wake()
            if (w.for_write) {
                if (!_readers) {
                    _writer = true;
                    w.pr.set_value();
                    _waiters.pop_front();
                }
                break;
            } else { // for read
                ++_readers;
                w.pr.set_value();
                _waiters.pop_front();
            }
        }
    }
};

/// Executes a function while holding shared access to a resource.
///
/// Executes a function while holding shared access to a resource.  When
/// the function returns, the mutex is automatically unlocked.
///
/// \param sm a \ref shared_mutex guarding access to the shared resource
/// \param func callable object to invoke while the mutex is held for shared access
/// \return whatever \c func returns, as a future
///
/// \relates shared_mutex
template <typename Func>
inline
futurize_t<std::result_of_t<Func ()>>
with_shared(shared_mutex& sm, Func&& func) {
    return sm.lock_shared().then([func = std::forward<Func>(func)] () mutable {
        return func();
    }).finally([&sm] {
        sm.unlock_shared();
    });
}

/// Executes a function while holding exclusive access to a resource.
///
/// Executes a function while holding exclusive access to a resource.  When
/// the function returns, the mutex is automatically unlocked.
///
/// \param sm a \ref shared_mutex guarding access to the shared resource
/// \param func callable object to invoke while the mutex is held for shared access
/// \return whatever \c func returns, as a future
///
/// \relates shared_mutex
template <typename Func>
inline
futurize_t<std::result_of_t<Func ()>>
with_lock(shared_mutex& sm, Func&& func) {
    return sm.lock().then([func = std::forward<Func>(func)] () mutable {
        return func();
    }).finally([&sm] {
        sm.unlock();
    });
}

/// @}

}
