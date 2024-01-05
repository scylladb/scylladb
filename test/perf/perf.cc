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


uint64_t perf_mallocs() {
    return memory::stats().mallocs();
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

std::ostream& operator<<(std::ostream& out, const scheduling_latency_measurer& slm) {
    auto to_ms = [] (int64_t nanos) {
        return float(nanos) / 1e6;
    };
    return out << fmt::format("{{count: {}, "
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

std::ostream&
operator<<(std::ostream& os, const perf_result& result) {
    fmt::print(os, "{:.2f} tps ({:5.1f} allocs/op, {:5.1f} tasks/op, {:7.0f} insns/op, {:8} errors)",
            result.throughput, result.mallocs_per_op, result.tasks_per_op, result.instructions_per_op, result.errors);
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

std::ostream& operator<<(std::ostream& os, const perf_result_with_aio_writes& result) {
    fmt::print(os, "{:.2f} tps ({:5.1f} allocs/op, {:5.1f} tasks/op, {:7.0f} insns/op, {:8} errors, {:7.2f} bytes/op, {:5.1f} writes/op)",
            result.throughput, result.mallocs_per_op, result.tasks_per_op, result.instructions_per_op, result.errors, result.aio_write_bytes, result.aio_writes);
    return os;
}

namespace perf {

reader_concurrency_semaphore_wrapper::reader_concurrency_semaphore_wrapper(sstring name)
    : _semaphore(std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, std::move(name)))
{
}

reader_concurrency_semaphore_wrapper::~reader_concurrency_semaphore_wrapper() {
    (void)_semaphore->stop().finally([sem = std::move(_semaphore)] { });
}

reader_permit reader_concurrency_semaphore_wrapper::make_permit() {
    return _semaphore->make_tracking_only_permit(nullptr, "perf", db::no_timeout, {});
}

} // namespace perf
