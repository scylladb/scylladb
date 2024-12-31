/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/alien.hh>
#include <seastar/core/reactor.hh>

#include <queue>

namespace seastar {
    class logger;
} // namespace seastar

namespace utils {

// Spawns a new OS thread, which can be used as a worker for running nonpreemptible 3rd party code.
// Callables can be sent to the thread for execution via submit().
class alien_worker {
    bool _running = true;
    std::mutex _mut;
    std::condition_variable _cv;
    std::queue<seastar::noncopyable_function<void() noexcept>> _pending;
    // Note: initialization of _thread uses other fields, so it must be performed last.
    std::thread _thread;

    std::thread spawn(seastar::logger&, int niceness);
public:
    alien_worker(seastar::logger&, int niceness);
    ~alien_worker();
    // The worker captures `this`, so `this` must have a stable address.
    alien_worker(const alien_worker&) = delete;
    alien_worker(alien_worker&&) = delete;

    // Submits a new callable to the thread for execution.
    // This callable will run on a different OS thread,
    // concurrently with the current thread, so be careful not to cause a data race.
    // Avoid capturing references in the callable if possible, and if you do,
    // be extremely careful about their concurrent uses.
    template <typename T>
    seastar::future<T> submit(seastar::noncopyable_function<T()> f) {
        auto p = seastar::promise<T>();
        auto fut = p.get_future();
        auto wrapper = [p = std::move(p), f = std::move(f), shard = seastar::this_shard_id(), &alien = seastar::engine().alien()] () mutable noexcept {
            try {
                auto v = f();
                seastar::alien::run_on(alien, shard, [v = std::move(v), p = std::move(p)] () mutable noexcept {
                    p.set_value(std::move(v));
                });
            } catch (...) {
                seastar::alien::run_on(alien, shard, [p = std::move(p), ep = std::current_exception()] () mutable noexcept {
                    p.set_exception(ep);
                });
            }
        };
        {
            std::unique_lock lk(_mut);
            _pending.push(std::move(wrapper));
        }
        _cv.notify_one();
        return fut;
    }
};

} // namespace utils
