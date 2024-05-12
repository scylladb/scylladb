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
    auto compare_throughput = [] (perf_result a, perf_result b) { return a.throughput < b.throughput; };
    std::sort(results.begin(), results.end(), compare_throughput);
    median_by_throughput = results[results.size() / 2];
    throughput.median = median_by_throughput.throughput;
    throughput.min = results[0].throughput;
    throughput.max = results[results.size() - 1].throughput;
    auto absolute_deviations = boost::copy_range<std::vector<double>>(
            results
            | boost::adaptors::transformed(std::mem_fn(&perf_result::throughput))
            | boost::adaptors::transformed([&] (double r) { return abs(r - throughput.median); }));
    std::sort(absolute_deviations.begin(), absolute_deviations.end());
    throughput.median_absolute_deviation = absolute_deviations[results.size() / 2];
}

std::ostream&
operator<<(std::ostream& os, const aggregated_perf_results& result) {
    auto& t = result.throughput;
    fmt::print(os, "\nmedian {}\nmedian absolute deviation: {:.2f}\nmaximum: {:.2f}\nminimum: {:.2f}", t.median, t.median_absolute_deviation, t.max, t.min);
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
