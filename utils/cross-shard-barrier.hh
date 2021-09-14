/*
 * Copyright 2021-present ScyllaDB
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

#include <atomic>
#include <vector>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

using namespace seastar;

namespace utils {

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
        std::vector<std::optional<promise<>>> wakeup;

        barrier() : counter(smp::count) {
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
        assert(!_b->wakeup[this_shard_id()].has_value());
        auto i = _b->counter.fetch_add(-1);
        return i == 1 ? complete() : wait();
    }

private:
    future<> complete() {
        _b->counter.fetch_add(smp::count);
        return smp::invoke_on_all([this, sid = this_shard_id()] {
            if (this_shard_id() != sid) {
                std::optional<promise<>>& w = _b->wakeup[this_shard_id()];
                assert(w.has_value());
                w->set_value();
                w.reset();
            }
        });
    }

    future<> wait() {
        return _b->wakeup[this_shard_id()].emplace().get_future();
    }
};

} // namespace utils
