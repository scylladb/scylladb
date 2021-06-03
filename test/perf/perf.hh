/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/print.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/weak_ptr.hh>
#include "seastarx.hh"
#include "utils/extremum_tracking.hh"
#include "utils/estimated_histogram.hh"
#include "linux-perf-event.hh"

#include <chrono>
#include <iosfwd>
#include <boost/range/irange.hpp>

template <typename Func>
static
void time_it(Func func, int iterations = 5, int iterations_between_clock_readings = 1000) {
    using clk = std::chrono::steady_clock;

    for (int i = 0; i < iterations; i++) {
        auto start = clk::now();
        auto end_at = start + std::chrono::seconds(1);
        uint64_t count = 0;

        while (clk::now() < end_at) {
            for (int i = 0; i < iterations_between_clock_readings; i++) { // amortize clock reading cost
                func();
                count++;
            }
        }

        auto end = clk::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        std::cout << format("{:.2f}", (double)count / duration) << " tps\n";
    }
}

struct executor_shard_stats {
    uint64_t invocations = 0;
    uint64_t allocations = 0;
    uint64_t tasks_executed = 0;
    uint64_t instructions_retired = 0;
};

inline
executor_shard_stats
operator+(executor_shard_stats a, executor_shard_stats b) {
    a.invocations += b.invocations;
    a.allocations += b.allocations;
    a.tasks_executed += b.tasks_executed;
    a.instructions_retired += b.instructions_retired;
    return a;
}

inline
executor_shard_stats
operator-(executor_shard_stats a, executor_shard_stats b) {
    a.invocations -= b.invocations;
    a.allocations -= b.allocations;
    a.tasks_executed -= b.tasks_executed;
    a.instructions_retired -= b.instructions_retired;
    return a;
}

uint64_t perf_tasks_processed();
uint64_t perf_mallocs();


// Drives concurrent and continuous execution of given asynchronous action
// until a deadline. Counts invocations and collects statistics.
template <typename Func>
class executor {
    const Func _func;
    const lowres_clock::time_point _end_at;
    const uint64_t _end_at_count;
    const unsigned _n_workers;
    uint64_t _count;
    linux_perf_event _instructions_retired_counter = linux_perf_event::user_instructions_retired();
private:
    executor_shard_stats executor_shard_stats_snapshot();
    future<> run_worker() {
        return do_until([this] {
            return _end_at_count ? _count == _end_at_count : lowres_clock::now() >= _end_at;
        }, [this] () mutable {
            ++_count;
            return _func();
        });
    }
public:
    executor(unsigned n_workers, Func func, lowres_clock::time_point end_at, uint64_t end_at_count = 0)
            : _func(std::move(func))
            , _end_at(end_at)
            , _end_at_count(end_at_count)
            , _n_workers(n_workers)
            , _count(0)
    { }

    // Returns the number of invocations of @func
    future<executor_shard_stats> run() {
        auto stats_start = executor_shard_stats_snapshot();
        _instructions_retired_counter.enable();
        auto idx = boost::irange(0, (int)_n_workers);
        return parallel_for_each(idx.begin(), idx.end(), [this] (auto idx) mutable {
            return this->run_worker();
        }).then([this, stats_start] {
            _instructions_retired_counter.disable();
            auto stats_end = executor_shard_stats_snapshot();
            return stats_end - stats_start;
        });
    }

    future<> stop() {
        return make_ready_future<>();
    }
};

template <typename Func>
executor_shard_stats
executor<Func>::executor_shard_stats_snapshot() {
    return executor_shard_stats{
        .invocations = _count,
        .allocations = perf_mallocs(),
        .tasks_executed = perf_tasks_processed(),
        .instructions_retired = _instructions_retired_counter.read(),
    };
}

struct perf_result {
    double throughput;
    double mallocs_per_op;
    double tasks_per_op;
    double instructions_per_op;
};

std::ostream& operator<<(std::ostream& os, const perf_result& result);

/**
 * Measures throughput of an asynchronous action. Executes the action on all cores
 * in parallel, with given number of concurrent executions per core.
 *
 * Runs many iterations. Prints partial total throughput after each iteraton.
 *
 * Returns a vector of throughputs achieved in each iteration.
 */
template <typename Func>
static
std::vector<perf_result> time_parallel(Func func, unsigned concurrency_per_core, int iterations = 5, unsigned operations_per_shard = 0) {
    using clk = std::chrono::steady_clock;
    if (operations_per_shard) {
        iterations = 1;
    }
    std::vector<perf_result> results;
    for (int i = 0; i < iterations; ++i) {
        auto start = clk::now();
        auto end_at = lowres_clock::now() + std::chrono::seconds(1);
        distributed<executor<Func>> exec;
        exec.start(concurrency_per_core, func, std::move(end_at), operations_per_shard).get();
        auto stats = exec.map_reduce0(std::mem_fn(&executor<Func>::run),
                executor_shard_stats(), std::plus<executor_shard_stats>()).get0();
        auto end = clk::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        auto result = perf_result{
            .throughput = static_cast<double>(stats.invocations) / duration,
            .mallocs_per_op = double(stats.allocations) / stats.invocations,
            .tasks_per_op = double(stats.tasks_executed) / stats.invocations,
            .instructions_per_op = double(stats.instructions_retired) / stats.invocations,
        };
        std::cout << result << "\n";
        results.emplace_back(result);
        exec.stop().get();
    }
    return results;
}

template<typename Func>
auto duration_in_seconds(Func&& f) {
    using clk = std::chrono::steady_clock;
    auto start = clk::now();
    f();
    auto end = clk::now();
    return std::chrono::duration_cast<std::chrono::duration<float>>(end - start);
}

class scheduling_latency_measurer : public weakly_referencable<scheduling_latency_measurer> {
    using clk = std::chrono::steady_clock;
    clk::time_point _last = clk::now();
    utils::estimated_histogram _hist{300};
    min_max_tracker<clk::duration> _minmax;
    bool _stop = false;
private:
    void schedule_tick();
    void tick() {
        auto old = _last;
        _last = clk::now();
        auto latency = _last - old;
        _minmax.update(latency);
        _hist.add(latency.count());
        if (!_stop) {
            schedule_tick();
        }
    }
public:
    void start() {
        schedule_tick();
    }
    void stop() {
        _stop = true;
        later().get(); // so that the last scheduled tick is counted
    }
    const utils::estimated_histogram& histogram() const {
        return _hist;
    }
    clk::duration min() const { return _minmax.min(); }
    clk::duration max() const { return _minmax.max(); }
};

std::ostream& operator<<(std::ostream& out, const scheduling_latency_measurer& slm);
