/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "perf.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include "seastarx.hh"
#include "reader_concurrency_semaphore.hh"
#include "schema/schema.hh"
#include "utils/logalloc.hh"


uint64_t perf_mallocs() {
    return memory::stats().mallocs();
}

uint64_t perf_logallocs() {
    return logalloc::shard_tracker().statistics().num_allocations;
}

uint64_t perf_tasks_processed() {
    return engine().get_sched_stats().tasks_processed;
}

void scheduling_latency_measurer::schedule_tick() {
    seastar::schedule(make_task(default_scheduling_group(), [self = weak_from_this()] () mutable {
        if (self) {
            self->tick();
        }
    }));
}

auto fmt::formatter<scheduling_latency_measurer>::format(const scheduling_latency_measurer& slm, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto to_ms = [] (int64_t nanos) {
        return float(nanos) / 1e6;
    };
    return fmt::format_to(ctx.out(), "{{count: {}, "
                         //"min: {:.6f} [ms], "
                         //"50%: {:.6f} [ms], "
                         //"90%: {:.6f} [ms], "
                         "99%: {:.6f} [ms], "
                         "max: {:.6f} [ms]}}",
        slm.histogram().count(),
        //to_ms(slm.min().count()),
        //to_ms(slm.histogram().percentile(0.5)),
        //to_ms(slm.histogram().percentile(0.9)),
        to_ms(slm.histogram().percentile(0.99)),
        to_ms(slm.max().count()));
}

auto fmt::formatter<perf_result>::format(const perf_result& result, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{:.2f} tps ({:5.1f} allocs/op, {:5.1f} logallocs/op, {:5.1f} tasks/op, {:7.0f} insns/op, {:7.0f} cycles/op, {:8} errors)",
            result.throughput, result.mallocs_per_op, result.logallocs_per_op, result.tasks_per_op, result.instructions_per_op, result.cpu_cycles_per_op, result.errors);
}

aggregated_perf_results::aggregated_perf_results(std::vector<perf_result>& results) {
    stats["throughput"] = calculate_stats(results, std::mem_fn(&perf_result::throughput));
    median_by_throughput = results[results.size() / 2];
    stats["instructions_per_op"] = calculate_stats(results, std::mem_fn(&perf_result::instructions_per_op));
    stats["cpu_cycles_per_op"] = calculate_stats(results, std::mem_fn(&perf_result::cpu_cycles_per_op));
}

aggregated_perf_results::stats_t aggregated_perf_results::calculate_stats(std::vector<perf_result>& results, std::function<double(const perf_result&)> get_stat) const {
    stats_t ret;
    std::sort(results.begin(), results.end(), [&] (const perf_result& a, const perf_result& b) {
        return get_stat(a) < get_stat(b);
    });
    ret.min = get_stat(results[0]);
    ret.median = get_stat(results[results.size() / 2]);
    ret.max = get_stat(results[results.size() - 1]);

    ret.mean = 0;
    for (const auto& pr : results) {
        ret.mean += get_stat(pr);
    }
    ret.mean /= results.size();
    double var = 0;
    std::vector<double> abs_deviations;
    abs_deviations.reserve(results.size());
    for (const auto& pr : results) {
        auto dev = get_stat(pr) - ret.mean;
        abs_deviations.emplace_back(std::abs(dev));
        var += dev * dev;
    }
    // Since the results are samples of the total population
    // devide the sum of squared deviations by (n - 1) rather than by n
    ret.stdev = std::sqrt(results.size() > 1 ? var / (results.size() - 1) : var);

    std::sort(abs_deviations.begin(), abs_deviations.end());
    ret.median_absolute_deviation = abs_deviations[abs_deviations.size() / 2];

    return ret;
}

std::ostream&
operator<<(std::ostream& os, const aggregated_perf_results& result) {
    for (const auto& s : {"throughput", "instructions_per_op", "cpu_cycles_per_op"}) {
        auto& t = result.stats.at(s);
        fmt::print(os, "\n{:>19}: mean={:.2f} standard-deviation={:.2f} median={:.2f} median-absolute-deviation={:.2f} maximum={:.2f} minimum={:.2f}",
                s, t.mean, t.stdev, t.median, t.median_absolute_deviation, t.max, t.min);
    }
    return os;
}

aio_writes_result_mixin::aio_writes_result_mixin()
    : aio_writes(engine().get_io_stats().aio_writes)
    , aio_write_bytes(engine().get_io_stats().aio_write_bytes)
{}

void aio_writes_result_mixin::update(aio_writes_result_mixin& result, const executor_shard_stats& stats) {
    result.aio_writes = (engine().get_io_stats().aio_writes - result.aio_writes) / stats.invocations;
    result.aio_write_bytes = (engine().get_io_stats().aio_write_bytes - result.aio_write_bytes) / stats.invocations;
}

auto fmt::formatter<perf_result_with_aio_writes>::format(const perf_result_with_aio_writes& result, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{:.2f} tps ({:5.1f} allocs/op, {:5.1f} logallocs/op, {:5.1f} tasks/op, {:7.0f} insns/op, {:7.0f} cycles/op, {:8} errors, {:7.2f} bytes/op, {:5.1f} writes/op)",
            result.throughput, result.mallocs_per_op, result.logallocs_per_op, result.tasks_per_op, result.instructions_per_op, result.cpu_cycles_per_op, result.errors, result.aio_write_bytes, result.aio_writes);
}

namespace perf {

reader_concurrency_semaphore_wrapper::reader_concurrency_semaphore_wrapper(sstring name)
    : _semaphore(std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, std::move(name),
                reader_concurrency_semaphore::register_metrics::no))
{
}

reader_concurrency_semaphore_wrapper::~reader_concurrency_semaphore_wrapper() {
    (void)_semaphore->stop().finally([sem = std::move(_semaphore)] { });
}

reader_permit reader_concurrency_semaphore_wrapper::make_permit() {
    return _semaphore->make_tracking_only_permit(nullptr, "perf", db::no_timeout, {});
}

} // namespace perf
