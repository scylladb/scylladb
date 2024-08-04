/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <atomic>
#include <vector>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

using namespace seastar;

namespace utils {

class barrier_aborted_exception : public std::exception {
public:
    virtual const char* what() const noexcept override {
        return "barrier aborted";
    }
};

// Shards-coordination mechanism that allows shards to wait each other at
// certain points. The barrier should be copied to each shard, then when
// each shard calls .arrive_and_wait()-s it will be blocked and woken up
// after all other shards do the same. The call to .arrive_and_wait() is
// not one-shot but is re-entrable. Every time a shard calls it it gets
// blocked until the corresponding step from others.
//
// Calling the arrive_and_wait() by one shard in one "phase" must be done
// exactly one time. If not called other shards will be blocked for ever,
// the second call will trigger the respective assertion.
//
// A recommended usage is inside sharded<> service. For example
//
//   class foo {
//       cross_shard_barrier barrier;
//       foo(cross_shard_barrier b) : barrier(std::move(b)) {}
//   };
//
//   sharded<foo> f;
//
//   // Start a sharded service and spread the barrier between instances
//   co_await f.start(cross_shard_barrier());
//
//   // On each shard start synchronizing instances with each-other
//   f.invoke_on_all([] (auto& f) {
//      co_await f.do_something();
//      co_await f.barrier.arrive_and_wait();
//      co_await f.do_something_else();
//      co_await f.barrier.arrive_and_wait();
//      co_await f.cleanup();
//   });
//
// In the above example each shard will only call the do_something_else()
// after _all_ other shards complete their do_something()s. Respectively,
// the cleanup() on each shard will only start after do_something_else()
// completes on _all_ of them.

class cross_shard_barrier {
    struct barrier {
        std::atomic<int> counter;
        std::atomic<bool> alive;
        std::vector<std::optional<promise<>>> wakeup;

        barrier() : counter(smp::count), alive(true) {
            wakeup.reserve(smp::count);
            for (unsigned i = 0; i < smp::count; i++) {
                wakeup.emplace_back();
            }
        }

        barrier(const barrier&) = delete;
    };

    std::shared_ptr<barrier> _b;

public:
    cross_shard_barrier() : _b(std::make_shared<barrier>()) {}

    // The 'solo' mode turns all the synchronization off, calls to
    // arrive_and_wait() never block. Eliminates the need to mess
    // with conditional usage in callers.
    struct solo {};
    cross_shard_barrier(solo) {}

    future<> arrive_and_wait() {
        if (!_b) {
            return make_ready_future<>();
        }

        // The barrier assumes that each shard arrives exactly once
        // (per phase). At the same time we cannot ban copying the
        // barrier, because it will likely be copied between sharded
        // users on sharded::start. The best check in this situation
        // is to make sure the local promise is not set up.
        SCYLLA_ASSERT(!_b->wakeup[this_shard_id()].has_value());
        auto i = _b->counter.fetch_add(-1);
        return i == 1 ? complete() : wait();
    }

    /**
     * Wakes up all arrivals with the barrier_aborted_exception() and
     * returns this exception itself. Once called the barrier becomes
     * unusable, any subsequent arrive_and_wait()s can (and actually
     * will) hang forever.
     */
    void abort() noexcept {
        // We can get here from shards that had already visited the
        // arrive_and_wait() and got the exceptional future. In this
        // case the counter would be set to smp::count and none of the
        // fetch_add(-1)s below will make it call complete()
        _b->alive.store(false);
        auto i = _b->counter.fetch_add(-1);
        if (i == 1) {
            (void)complete().handle_exception([] (std::exception_ptr ignored) {});
        }
    }

private:
    future<> complete() {
        _b->counter.fetch_add(smp::count);
        bool alive = _b->alive.load(std::memory_order_relaxed);
        return smp::invoke_on_all([b = _b, sid = this_shard_id(), alive] {
            if (this_shard_id() != sid) {
                std::optional<promise<>>& w = b->wakeup[this_shard_id()];
                if (alive) {
                    SCYLLA_ASSERT(w.has_value());
                    w->set_value();
                    w.reset();
                } else if (w.has_value()) {
                    w->set_exception(barrier_aborted_exception());
                    w.reset();
                }
            }

            return alive ? make_ready_future<>()
                         : make_exception_future<>(barrier_aborted_exception());
        });
    }

    future<> wait() {
        return _b->wakeup[this_shard_id()].emplace().get_future();
    }
};

} // namespace utils
