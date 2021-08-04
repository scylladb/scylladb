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

#include <map>
#include <iosfwd>
#include <exception>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/condition-variable.hh>

#include "seastarx.hh"

namespace utils {

/**
 * Simple counter type wrapper, that allows
 * waiting for it to reach a given value.
 * 
 * Actual content must be monotonically 
 * increasing. 
 * 
 * Waiting using future<> wait routines
 * is less efficient than using coroutine
 * when(..) calls, as the former can cause
 * spurious wakeups wheras the latter will 
 * not.
 */
template<typename T, typename Cmp = std::less<T>>
class waitable_counter {
private:
    T _value;
    condition_variable _cond;
    Cmp _cmp;

    template<std::invocable<> Func>
    waitable_counter& set(T v, Func&& f) {
        if (_cmp(v, _value)) {
            throw std::invalid_argument("Values must be monotonically increasing");
        }
        _value = std::move(v);
        f();
        return *this;
    }

public:
    waitable_counter(T&& t = {}, Cmp cmp = {})
        : _value(t)
        , _cmp(std::move(cmp))
    {}

    /**
     * Waits for the counter to reach pos, or timeout
     */
    template<typename Clock = typename timer<>::clock, typename Duration = typename Clock::duration>
    future<> wait(const T& pos, std::chrono::time_point<Clock, Duration> timeout) {
        return _cond.wait(timeout, [this, pos] { return !_cmp(_value, pos); });
    }

    /**
     * Waits for the counter to reach pos
     */
    future<> wait(const T& pos) {
        return _cond.wait([this, pos] { return !_cmp(_value, pos); });
    }

    /**
     * Waits for the counter to reach pos, or timeout (co-routine only)
     * Note: the coroutine versions are supoerior in every sense, since 
     * they are immune to suprious wakeups (go condition-var::when).
     */
    template<typename Clock = typename timer<>::clock, typename Duration = typename Clock::duration>
    auto when(const T& pos, std::chrono::time_point<Clock, Duration> timeout) {
        return _cond.when(timeout, [this, pos] { return !_cmp(_value, pos); });
    }

    /**
     * Waits for the counter to reach pos (co-routine only)
     */
    auto when(const T& pos) {
        return _cond.when([this, pos] { return !_cmp(_value, pos); });
    }

    /**
     * Sets the new counter value, notifying
     * any waiting entities (for values <= v)
     * 
     * v must be >= current value.
     */
    waitable_counter& set(T v) {
        return set(std::move(v), [&] {
            _cond.broadcast();
        });
    }

    /**
     * Signals all waiters at v or below
     * with exception. Current value is updated 
     * to v, thus waits after this will pass
     * as if successful.
     */
    waitable_counter& set_exception(T v, std::exception_ptr e) {
        return set(std::move(v), [&] {
            _cond.broken(e);
        });
    }

    operator const T&() const {
        return _value;
    }
};

template<typename T>
std::ostream& operator<<(std::ostream& os, const waitable_counter<T>& t) {
    const T& v = t;
    return os << v;
}

}