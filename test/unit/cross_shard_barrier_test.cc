/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <fmt/core.h>
#include "utils/cross-shard-barrier.hh"

static constexpr unsigned phases_scale = 11;

class worker : public seastar::peering_sharded_service<worker> {
    utils::cross_shard_barrier _barrier;
    std::atomic<unsigned> _phase;
    std::random_device _rndgen;
    timer<> _watchdog;
    unsigned _last_wdog_phase;

    void watchdog_tick() {
        auto this_phase = _phase.load();
        if (_last_wdog_phase == this_phase) {
            fmt::print("loop stuck on shard.{} at phase {}\n", this_shard_id(), _last_wdog_phase);
            abort();
        } else {
            fmt::print("shard.{} alive at phase {}\n", this_shard_id(), this_phase);
        }
        _last_wdog_phase = this_phase;
    }

public:
    worker(utils::cross_shard_barrier b) noexcept
            : _barrier(std::move(b))
            , _phase(0)
            , _watchdog([this] { watchdog_tick(); })
            , _last_wdog_phase(0)
    {
        // Give each shard a good chance to sleep for up to 100ms
        auto period = std::chrono::milliseconds(smp::count * 100);
        // Don't make them fire all at once
        auto first = period + this_shard_id() * period / smp::count;
        _watchdog.arm(std::chrono::steady_clock::now() + first, {period});
    }

    future<> loop() {
        for (unsigned i = 0; i < smp::count * phases_scale; i++) {
            unsigned phase = _phase.fetch_add(1);
            co_await _barrier.arrive_and_wait();
            if (this_shard_id() == (i % smp::count)) {
                co_await container().invoke_on_all([phase] (auto& w) {
                    auto this_phase = w._phase.load();
                    if (this_phase != phase + 1) {
                        fmt::print("ERROR {}.{} != {}\n", this_shard_id(), this_phase, phase + 1);
                        abort();
                    }
                });
            }
            co_await _barrier.arrive_and_wait();
            co_await seastar::sleep(std::chrono::milliseconds(_rndgen() % 100));
        }
    }
};

int main(int argc, char **argv) {
    app_template app;
    return app.run(argc, argv, [&app] {
        if (smp::count < 2) {
            std::cerr << "Cannot run test with smp::count < 2";
            return make_ready_future<>();
        }

        return seastar::async([] {
            sharded<worker> w;
            w.start(utils::cross_shard_barrier()).get();
            w.invoke_on_all(&worker::loop).get();
            w.stop().get();
        });
    });
}
