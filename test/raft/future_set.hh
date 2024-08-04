/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/condition-variable.hh>
#include "test/raft/logical_timer.hh"

using namespace seastar;

// A set of futures that can be polled to obtain the result of some ready future in the set.
//
// Note: the set must be empty on destruction. Call `release` to ensure emptiness.
template <typename T>
class future_set {
    struct cond_var_container : public seastar::weakly_referencable<cond_var_container> {
        seastar::condition_variable v;
    };

    std::vector<future<T>> _futures;
    cond_var_container _container;

public:
    // Polling the set returns the value of one of the futures which became available
    // or `std::nullopt` if the logical duration `d` passes (according to `timer`),
    // whichever event happens first.
    //
    // Cannot be called in parallel.
    // TODO: we could probably lift this restriction by using `broadcast()` instead of `signal()`. Think about it.
    future<std::optional<T>> poll(logical_timer& timer, raft::logical_clock::duration d) {
        auto timeout = timer.now() + d;

        auto wake_condition = [this, &timer, timeout] {
            return std::any_of(_futures.begin(), _futures.end(), std::mem_fn(&future<T>::available)) || timer.now() >= timeout;
        };

        if (timer.now() < timeout) { // i.e. d > 0
            // Wake ourselves up when the timeout passes (if we're still waiting at that point).
            // If nothing else wakes us, this will.
            timer.schedule(timeout, [ptr = _container.weak_from_this()] {
                if (ptr) {
                    ptr->v.signal();
                }
            });

            co_await _container.v.wait(wake_condition);
        }

        SCYLLA_ASSERT(wake_condition());

        for (auto& f : _futures) {
            if (f.available()) {
                std::swap(f, _futures.back());
                auto ff = std::move(_futures.back());
                _futures.pop_back();
                co_return std::move(ff).get();
            }
        }

        // No future was available, so `wake_condition()` implies:
        SCYLLA_ASSERT(timer.now() >= timeout);
        co_return std::nullopt;
    }

    void add(future<T> f) {
        _futures.push_back(std::move(f).finally([ptr = _container.weak_from_this()] {
            if (ptr) {
                ptr->v.signal();
            }
        }));
    }

    // Removes all futures from the set and returns them (even if they are not ready yet).
    // The user must ensure that there are no futures in the set when it's destroyed; this is a good way to do so.
    std::vector<future<T>> release() {
        return std::exchange(_futures, {});
    }

    bool empty() const {
        return _futures.empty();
    }

    ~future_set() {
        SCYLLA_ASSERT(_futures.empty());
    }
};
