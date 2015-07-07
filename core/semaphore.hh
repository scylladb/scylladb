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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CORE_SEMAPHORE_HH_
#define CORE_SEMAPHORE_HH_

#include "future.hh"
#include <list>
#include <stdexcept>
#include <exception>
#include "timer.hh"

/// \addtogroup fiber-module
/// @{

/// Exception thrown when a semaphore is broken by
/// \ref semaphore::broken().
class broken_semaphore : public std::exception {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept {
        return "Semaphore broken";
    }
};

/// Exception thrown when a semaphore wait operation
/// times out.
///
/// \see semaphore::wait(typename timer<>::duration timeout, size_t nr)
class semaphore_timed_out : public std::exception {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept {
        return "Semaphore timedout";
    }
};

/// \brief Counted resource guard.
///
/// This is a standard computer science semaphore, adapted
/// for futures.  You can deposit units into a counter,
/// or take them away.  Taking units from the counter may wait
/// if not enough units are available.
///
/// To support exceptional conditions, a \ref broken() method
/// is provided, which causes all current waiters to stop waiting,
/// with an exceptional future returned.  This allows causing all
/// fibers that are blocked on a semaphore to continue.  This is
/// similar to POSIX's `pthread_cancel()`, with \ref wait() acting
/// as a cancellation point.
class semaphore {
private:
    size_t _count;
    struct entry {
        promise<> pr;
        size_t nr;
        timer<> tr;
        entry(promise<>&& pr_, size_t nr_) : pr(std::move(pr_)), nr(nr_) {}
    };
    std::list<entry> _wait_list;
public:
    /// Constructs a semaphore object with a specific number of units
    /// in its internal counter.  The default is 1, suitable for use as
    /// an unlocked mutex.
    ///
    /// \param count number of initial units present in the counter (default 1).
    semaphore(size_t count = 1) : _count(count) {}
    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future that becomes ready when sufficient units are availble
    ///         to satisfy the request.  If the semaphore was \ref broken(), may
    ///         contain an exception.
    future<> wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return make_ready_future<>();
        }
        promise<> pr;
        auto fut = pr.get_future();
        _wait_list.push_back(entry(std::move(pr), nr));
        return fut;
    }
    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  If the request
    /// cannot be satisfied in time, the request is aborted.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param timeout how long to wait.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future that becomes ready when sufficient units are availble
    ///         to satisfy the request.  On timeout, the future contains a
    ///         \ref semaphore_timed_out exception.  If the semaphore was
    ///         \ref broken(), may contain an exception.
    future<> wait(typename timer<>::duration timeout, size_t nr = 1) {
        auto fut = wait(nr);
        if (!fut.available()) {
            auto& e = _wait_list.back();
            e.tr.set_callback([&e, this] {
                e.pr.set_exception(semaphore_timed_out());
                e.nr = 0;
                signal(0);
            });
            e.tr.arm(timeout);
        }
        return std::move(fut);
    }
    /// Deposits a specified number of units into the counter.
    ///
    /// The counter is incremented by the specified number of units.
    /// If the new counter value is sufficient to satisfy the request
    /// of one or more waiters, their futures (in FIFO order) become
    /// ready, and the value of the counter is reduced according to
    /// the amount requested.
    ///
    /// \param nr Number of units to deposit (default 1).
    void signal(size_t nr = 1) {
        _count += nr;
        while (!_wait_list.empty() && _wait_list.front().nr <= _count) {
            auto& x = _wait_list.front();
            if (x.nr) {
               _count -= x.nr;
               x.pr.set_value();
               x.tr.cancel();
            }
            _wait_list.pop_front();
        }
    }
    /// Attempts to reduce the counter value by a specified number of units.
    ///
    /// If sufficient units are available in the counter, and if no
    /// other fiber is waiting, then the counter is reduced.  Otherwise,
    /// nothing happens.  This is useful for "opportunistic" waits where
    /// useful work can happen if the counter happens to be ready, but
    /// when it is not worthwhile to wait.
    ///
    /// \param nr number of units to reduce the counter by (default 1).
    /// \return `true` if the counter had sufficient units, and was decremented.
    bool try_wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return true;
        } else {
            return false;
        }
    }
    /// Returns the number of units available in the counter.
    ///
    /// Does not take into account any waiters.
    size_t current() const { return _count; }

    /// Signal to waiters that an error occurred.  \ref wait() will see
    /// an exceptional future<> containing a \ref broken_semaphore exception.
    /// The future is made available immediately.
    ///
    /// This may only be used once per semaphore; after using it the
    /// semaphore is in an indeterminate state and should not be waited on.
    void broken() { broken(std::make_exception_ptr(broken_semaphore())); }

    /// Signal to waiters that an error occurred.  \ref wait() will see
    /// an exceptional future<> containing the provided exception parameter.
    /// The future is made available immediately.
    ///
    /// This may only be used once per semaphore; after using it the
    /// semaphore is in an indeterminate state and should not be waited on.
    template <typename Exception>
    void broken(const Exception& ex) {
        broken(std::make_exception_ptr(ex));
    }

    /// Signal to waiters that an error occurred.  \ref wait() will see
    /// an exceptional future<> containing the provided exception parameter.
    /// The future is made available immediately.
    ///
    /// This may only be used once per semaphore; after using it the
    /// semaphore is in an indeterminate state and should not be waited on.
    void broken(std::exception_ptr ex);
};

inline
void
semaphore::broken(std::exception_ptr xp) {
    while (!_wait_list.empty()) {
        auto& x = _wait_list.front();
        x.pr.set_exception(xp);
        x.tr.cancel();
        _wait_list.pop_front();
    }
}

/// @}

#endif /* CORE_SEMAPHORE_HH_ */
