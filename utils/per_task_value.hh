/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>
#include <optional>
#include <seastar/core/reactor.hh>

namespace utils {

/// A value recomputed at most once per reactor task. Seastar evaluates all the metric
/// functions of a shard in one task, so the gauges of one scrape read the same value.
template <std::invocable Action>
class per_task_value {
public:
    using value_type = std::invoke_result_t<Action>;
private:
    Action _action;
    std::optional<value_type> _value;
    uint64_t _task = 0;
public:
    explicit per_task_value(Action action) : _action(std::move(action)) {}

    const value_type& get() {
        const auto task = seastar::engine().get_sched_stats().tasks_processed;
        if (!_value || _task != task) {
            _value = _action();
            _task = task;
        }
        return *_value;
    }
};

} // namespace utils
