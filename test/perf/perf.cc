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

#include "perf.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include "seastarx.hh"
#include "reader_concurrency_semaphore.hh"


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
    return out << sprint("{count: %d, "
                         //"min: %.6f [ms], "
                         //"50%%: %.6f [ms], "
                         //"90%%: %.6f [ms], "
                         "99%%: %.6f [ms], "
                         "max: %.6f [ms]}",
        slm.histogram().count(),
        //to_ms(slm.min().count()),
        //to_ms(slm.histogram().percentile(0.5)),
        //to_ms(slm.histogram().percentile(0.9)),
        to_ms(slm.histogram().percentile(0.99)),
        to_ms(slm.max().count()));
}

std::ostream&
operator<<(std::ostream& os, const perf_result& result) {
    fmt::print(os, "{:.2f} tps ({:5.1f} allocs/op, {:5.1f} tasks/op, {:7.0f} insns/op)",
            result.throughput, result.mallocs_per_op, result.tasks_per_op, result.instructions_per_op);
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
    return _semaphore->make_tracking_only_permit(nullptr, "perf");
}

} // namespace perf
