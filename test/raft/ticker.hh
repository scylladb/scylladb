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

#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

using namespace seastar;

// Given a set of functions and associated positive natural numbers,
// calls the functions with periods defined by their corresponding numbers,
// yielding in between.
//
// Define a "tick" to be a (possibly empty) set of calls to some of the given functions
// followed by a yield. The ticker executes a sequence of ticks. Given {n, f}, where n
// is a number and f is a function, f will be called each nth tick.
//
// For example, suppose the ticker was started with the set {{2, f}, {4, g}, {4, h}}.
// Then the functions called in each tick are:
// tick 1: f, g, h tick 2: none, tick 3: f, tick 4: none, tick 5: f, g, h tick 2: none, and so on.
//
// The order of calls within a single tick is unspecified.
//
// The number of ticks can be limited. We crash if the ticker reaches the limit before it's `abort()`ed.
//
// Call `start` to provide the distribution and start the ticking.
class ticker {
    bool _stop = false;
    std::optional<future<>> _ticker;
    seastar::logger& _logger;

public:
    ticker(seastar::logger& l) : _logger(l) {}
    ticker(const ticker&) = delete;
    ticker(ticker&&) = delete;

    ~ticker() {
        assert(!_ticker);
    }

    using on_tick_t = noncopyable_function<void()>;

    template <size_t N>
    void start(std::pair<size_t, on_tick_t> (&&tick_funs)[N], uint64_t limit = std::numeric_limits<uint64_t>::max()) {
        assert(!_ticker);
        _ticker = tick(std::move(tick_funs), limit);
    }

    future<> abort() {
        if (_ticker) {
            _stop = true;
            co_await *std::exchange(_ticker, std::nullopt);
        }
    }

private:
    template <size_t N>
    future<> tick(std::pair<size_t, on_tick_t> (&&tick_funs)[N], uint64_t limit) {
        static_assert(N > 0);

        auto funs = std::to_array(std::move(tick_funs));
        for (uint64_t tick = 0; tick < limit; ++tick) {
            if (_stop) {
                _logger.info("ticker: finishing after {} ticks", tick);
                co_return;
            }

            for (auto& [n, f] : funs) {
                if (tick % n == 0) {
                    f();
                }
            }

            co_await seastar::later();
        }

        _logger.error("ticker: limit reached");
        assert(false);
    }
};
