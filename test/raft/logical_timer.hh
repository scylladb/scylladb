/*
 * Copyright (C) 2021 ScyllaDB
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

#include <seastar/core/timed_out_error.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include "raft/logical_clock.hh"

using namespace seastar;

constexpr raft::logical_clock::duration operator "" _t(unsigned long long ticks) {
    return raft::logical_clock::duration{ticks};
}

// A wrapper around `raft::logical_clock` that allows scheduling events to happen after a certain number of ticks.
class logical_timer {
    struct scheduled_impl {
    private:
        bool _resolved = false;

        virtual void do_resolve() = 0;

    public:
        virtual ~scheduled_impl() { }

        void resolve() {
            if (!_resolved) {
                do_resolve();
                _resolved = true;
            }
        }

        void mark_resolved() { _resolved = true; }
        bool resolved() { return _resolved; }
    };

    struct scheduled {
        raft::logical_clock::time_point _at;
        seastar::shared_ptr<scheduled_impl> _impl;

        void resolve() { _impl->resolve(); }
        bool resolved() { return _impl->resolved(); }
    };

    raft::logical_clock _clock;

    // A min-heap of `scheduled` events sorted by `_at`.
    std::vector<scheduled> _scheduled;

    // Comparator for the `_scheduled` min-heap.
    static bool cmp(const scheduled& a, const scheduled& b) {
        return a._at > b._at;
    }

public:
    logical_timer() = default;

    logical_timer(const logical_timer&) = delete;
    logical_timer(logical_timer&&) = default;

    // Returns the current logical time (number of `tick()`s since initialization).
    raft::logical_clock::time_point now() {
        return _clock.now();
    }

    // Tick the internal clock.
    // Resolve all events whose scheduled times arrive after that tick.
    void tick() {
        _clock.advance();
        while (!_scheduled.empty() && _scheduled.front()._at <= _clock.now()) {
            _scheduled.front().resolve();
            std::pop_heap(_scheduled.begin(), _scheduled.end(), cmp);
            _scheduled.pop_back();
        }
    }

    // Given a future `f` and a logical time point `tp`, returns a future that resolves
    // when either `f` resolves or the time point `tp` arrives (according to the number of `tick()` calls),
    // whichever comes first.
    //
    // Note: if `tp` comes first, that doesn't cancel any in-progress computations behind `f`.
    // `f` effectively becomes a discarded future.
    // Note: there is a possibility that the internal heap will grow unbounded if we call `with_timeout`
    // more often than we `tick`, so don't do that. It is recommended to call at least one
    // `tick` per one `with_timeout` call (on average in the long run).
    template <typename... T>
    future<T...> with_timeout(raft::logical_clock::time_point tp, future<T...> f) {
        if (f.available()) {
            return f;
        }

        struct sched : public scheduled_impl {
            promise<T...> _p;

            virtual ~sched() override { }
            virtual void do_resolve() override {
                _p.set_exception(std::make_exception_ptr(timed_out_error()));
            }
        };

        auto s = make_shared<sched>();
        auto res = s->_p.get_future();

        _scheduled.push_back(scheduled{
            ._at = tp,
            ._impl = s
        });
        std::push_heap(_scheduled.begin(), _scheduled.end(), cmp);

        (void)f.then_wrapped([s = std::move(s)] (auto&& f) mutable {
            if (s->resolved()) {
                f.ignore_ready_future();
            } else {
                f.forward_to(std::move(s->_p));
                s->mark_resolved();
                // tick() will (eventually) clear the `_scheduled` entry.
            }
        });

        return res;
    }

    // Returns a future that resolves after a number of `tick()`s represented by `d`.
    // Example usage: `sleep(20_t)` resolves after 20 `tick()`s.
    // Note: analogous remark applies as for `with_timeout`, i.e. make sure to call at least one `tick`
    // per one `sleep` call on average.
    future<> sleep(raft::logical_clock::duration d) {
        if (d == raft::logical_clock::duration{0}) {
            return make_ready_future<>();
        }

        struct sched : public scheduled_impl {
            promise<> _p;
            virtual ~sched() override {}
            virtual void do_resolve() override { _p.set_value(); }
        };

        auto s = make_shared<sched>();
        auto f = s->_p.get_future();
        _scheduled.push_back(scheduled{
            ._at = now() + d,
            ._impl = std::move(s)
        });
        std::push_heap(_scheduled.begin(), _scheduled.end(), cmp);

        return f;
    }
};

