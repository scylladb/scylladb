/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
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
        auto period = std::chrono::seconds(smp::count * 100);
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

    future<> loop_with_error() {
        auto b = seastar::defer([this] { _barrier.abort(); });

        while (true) {
            if (_rndgen() % 3 == 0) {
                throw std::runtime_error("exception");
            }

            co_await _barrier.arrive_and_wait();
            _phase.fetch_add(1);
        }
    }

    future<> error() {
        _barrier.abort();
        return make_ready_future<>();
    }

    unsigned get_phase() const noexcept { return _phase.load(); }
};

int main(int argc, char **argv) {
    app_template app;
    return app.run(argc, argv, [] {
        if (smp::count < 2) {
            std::cerr << "Cannot run test with smp::count < 2";
            return make_ready_future<>();
        }

        return seastar::async([] {
            sharded<worker> w;
            w.start(utils::cross_shard_barrier()).get();
            w.invoke_on_all(&worker::loop).get();
            w.stop().get();

            for (size_t i = 0; i < smp::count * phases_scale; i++) {
                sharded<worker> w;
                w.start(utils::cross_shard_barrier()).get();
                try {
                    w.invoke_on_all(&worker::loop_with_error).get();
                } catch (...) {
                    auto ph = w.invoke_on(0, [] (auto& w) { return w.get_phase(); }).get();
                    for (size_t c = 1; c < smp::count; c++) {
                        auto ph_2 = w.invoke_on(c, [] (auto& w) { return w.get_phase(); }).get();
                        if (ph_2 != ph) {
                            fmt::print("aborted barrier passed shard through\n");
                            abort();
                        }
                    }
                }
                w.stop().get();
            }

            std::vector<int> count(64);
            parallel_for_each(count, [] (auto& cnt) -> future<> {
                std::vector<sharded<worker>> w(32);
                co_await parallel_for_each(w, [] (auto &sw) -> future<> {
                    co_await sw.start(utils::cross_shard_barrier());
                    co_await sw.invoke_on_all(&worker::error);
                    co_await sw.stop();
                });
            }).get();
        });
    });
}
