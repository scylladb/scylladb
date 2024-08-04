/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

using namespace seastar;

// Calls the given function as fast as the Seastar reactor allows and waits on each call.
// The function is passed an incrementing integer (incremented by one for each call, starting at 0).
// The number of ticks can be limited. We crash if the ticker reaches the limit before it's `abort()`ed.
// Call `start()` to start the ticking.
class ticker {
    bool _stop = false;
    std::optional<future<>> _ticker;
    seastar::logger& _logger;

public:
    ticker(seastar::logger& l) : _logger(l) {}
    ticker(const ticker&) = delete;
    ticker(ticker&&) = delete;

    ~ticker() {
        SCYLLA_ASSERT(!_ticker);
    }

    using on_tick_t = noncopyable_function<future<>(uint64_t)>;

    void start(on_tick_t fun, uint64_t limit = std::numeric_limits<uint64_t>::max()) {
        SCYLLA_ASSERT(!_ticker);
        _ticker = tick(std::move(fun), limit);
    }

    future<> abort() {
        if (_ticker) {
            _stop = true;
            co_await *std::exchange(_ticker, std::nullopt);
        }
    }

private:
    future<> tick(on_tick_t fun, uint64_t limit) {
        for (uint64_t tick = 0; tick < limit; ++tick) {
            if (_stop) {
                _logger.info("ticker: finishing after {} ticks", tick);
                co_return;
            }

            co_await fun(tick);
        }

        _logger.error("ticker: limit reached");
        SCYLLA_ASSERT(false);
    }
};
