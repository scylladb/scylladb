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
    template <typename... T>
    class timed_out : public raft::error {
        // lw_shared_ptr to make the exception copyable
        lw_shared_ptr<seastar::future<T...>> _fut;

    public:
        timed_out(seastar::future<T...> f) : error("timed out"), _fut(make_lw_shared(std::move(f))) {}
        timed_out(const timed_out&) = default;
        timed_out(timed_out&&) = default;

        seastar::future<T...>& get_future() {
            return *_fut;
        }
    };

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
    // If `tp` comes first, the returned future is resolved with the `timed_out` exception.
    // The exception contains a future equivalent to the original future (i.e. when the original future
    // resolves, the future inside the exception will contain the original future's result).
    //
    // Note: it is highly recommended to pass in futures that always resolve eventually
    // since we attach continuations to these futures, allocating memory.
    //
    // Note: there is a possibility that the internal heap will grow unbounded if we call `with_timeout`
    // more often than we `tick`, so don't do that. It is recommended to call at least one
    // `tick` per one `with_timeout` call (on average in the long run).
    template <typename... T>
    future<T...> with_timeout(raft::logical_clock::time_point tp, future<T...> f) {
        if (f.available()) {
            return f;
        }

        struct sched : public scheduled_impl {
            // The original future (the `f` argument), when it resolves, will set value on `_p`.
            // Before timeout, `_p` is connected to the future returned to the user (`res` below).
            // After timeout, `_p` is a new promise connected to the future returned inside the timed_out exception.
            // Therefore:
            // 1. if `f` resolves before timeout, it will resolve `res`,
            // 2. otherwise it will resolve the future we return inside `timed_out` which is returned through `res`.
            promise<T...> _p;

            virtual ~sched() override { }
            virtual void do_resolve() override {
                promise<T...> new_p;
                _p.set_exception(timed_out<T...>{new_p.get_future()});
                std::swap(new_p, _p);
            }
        };

        auto s = make_shared<sched>();
        auto res = s->_p.get_future();

        _scheduled.push_back(scheduled{
            ._at = tp,
            ._impl = s
        });
        std::push_heap(_scheduled.begin(), _scheduled.end(), cmp);

        (void)f.then_wrapped([s = std::move(s)] (future<T...> f) mutable {
            // If we're before timeout, `s->_p` is connected to `res` so we're returning the result of `f` to our caller.
            // Otherwise `s->_p` is connected to a future which was previously returned to our caller inside `timed_out`.
            f.forward_to(std::move(s->_p));
            s->mark_resolved();
            // tick() will (eventually) clear the `_scheduled` entry
            // (unless we're already past timeout, in which case the entry has already been cleared).
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

    // Schedule `f` to be called at logical time point `tp` (according to this timer's clock).
    template <typename F>
    void schedule(raft::logical_clock::time_point tp, F f) {
        if (tp <= now()) {
            f();
            return;
        }

        struct sched : public scheduled_impl {
            sched(F f) : _f(std::move(f)) {}
            virtual ~sched() override {}
            virtual void do_resolve() override { _f(); }

            F _f;
        };

        _scheduled.push_back(scheduled {
            ._at = tp,
            ._impl = make_shared<sched>(std::move(f))
        });
        std::push_heap(_scheduled.begin(), _scheduled.end(), cmp);
    }
};

