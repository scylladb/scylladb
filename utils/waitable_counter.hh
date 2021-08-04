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
#include <seastar/core/shared_future.hh>

namespace utils {

/**
 * Simple counter type wrapper, that allows
 * waiting for it to reach a given value.
 * 
 * Actual content must be monotonically 
 * increasing, and unless you want a lot
 * of entries, waiting should be either
 * sparse, or at least quantized to "typical"
 * values (like disk blocks or similar, hint hint)
 */
template<typename T, typename Cmp = std::less<T>>
class waitable_counter {
private:
    using shared_promise_type = seastar::shared_promise<>;

    T _value;
    std::map<T, shared_promise_type, Cmp> _waiters;

    using value_type = typename decltype(_waiters)::value_type;

    template<std::invocable<value_type&> Func>
    waitable_counter& set(T v, Func&& f) {
        if (_waiters.key_comp()(v, _value)) {
            throw std::invalid_argument("Values must be monotonically increasing");
        }
        _value = std::move(v);
        auto i = _waiters.upper_bound(_value);
        std::for_each(_waiters.begin(), i, f);
        _waiters.erase(_waiters.begin(), i);
        return *this;
    }

public:
    using clock = typename shared_promise_type::clock;
    using time_point = typename shared_promise_type::time_point;

    waitable_counter(T&& t = {}, Cmp cmp = {})
        : _value(t)
        , _waiters(std::move(cmp))
    {}
    /**
     * Waits for the counter to reach pos
     */
    future<> wait(const T& pos, time_point timeout = time_point::max()) {
        // if value held is already greater or 
        // equal, just go ahead.
        if (!_waiters.key_comp()(_value, pos)) {
            return make_ready_future<>();
        }
        // else create/use the shared_promise
        // at this pos.
        auto& p = _waiters[pos];
        return p.get_shared_future(timeout);
    }
    /**
     * Sets the new counter value, notifying
     * any waiting entities (for values <= v)
     * 
     * v must be >= current value.
     */
    waitable_counter& set(T v) {
        return set(std::move(v), [](value_type& p) {
            p.second.set_value();
        });
    }

    /**
     * Signals all waiters at v or below
     * with exception. Current value is updated 
     * to v, thus waits after this will pass
     * as if successful.
     */
    waitable_counter& set_exception(T v, std::exception_ptr e) {
        return set(std::move(v), [&](value_type& p) {
            p.second.set_exception(e);
        });
    }

    future<> wait_and_set_range(const T& s, T v) {
        co_await wait(s);
        set(std::move(v));
    }

    future<> wait_and_set_range_exception(const T& s, T v, std::exception_ptr e) {
        co_await wait(s);
        set_exception(std::move(v), std::move(e));
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