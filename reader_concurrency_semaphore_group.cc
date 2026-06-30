/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "reader_concurrency_semaphore_group.hh"
#include "reader_concurrency_semaphore_group.hh"
#include <seastar/core/metrics.hh>
#include <algorithm>

namespace sm = seastar::metrics;

static const sm::label class_label("class");

void reader_concurrency_semaphore_shared_pool::register_metrics(const sstring& name) {
    _metrics.add_group("database", {
        sm::make_gauge("reads_shared_pool_available_memory",
                       [this] { return _available_memory; },
                       sm::description("Holds the current amount of available memory in the shared reader concurrency semaphore pool."),
                       {class_label(name)}),
        sm::make_gauge("reads_shared_pool_total_memory",
                       [this] { return _total_memory; },
                       sm::description("Holds the total memory of the shared reader concurrency semaphore pool."),
                       {class_label(name)}),
    });
}

void reader_concurrency_semaphore_shared_pool::wake_front_waiter() noexcept {
    auto& sem = _notify_list.front().get();
    _notify_list.pop_front();
    sem._on_shared_pool_notify_list = false;
    sem.maybe_wake_execution_loop();
}

void reader_concurrency_semaphore_shared_pool::repay(ssize_t amount) noexcept {
    _available_memory += amount;
    if (_available_memory > _total_memory)  [[unlikely]] {
        static thread_local logger::rate_limit rate_limit(std::chrono::seconds(30));
        rcslog.log(log_level::warn, rate_limit,
                "shared reader concurrency semaphore pool over-repaid memory: available={}, total={}, repaid={}; clamping available memory to total",
                _available_memory, _total_memory, amount);
        _available_memory = _total_memory;
    }
    // Wake a single semaphore rather than all of them.  We pop from the front
    // (FIFO) so that every registered semaphore gets a turn: each woken
    // semaphore re-registers at the back if it still has blocked waiters,
    // giving round-robin fairness.  If the woken semaphore borrows some of the
    // returned memory it hands the rest off to the next waiter via
    // maybe_wake_next_waiter(), so a single large return can satisfy several
    // waiters in order without skipping the head of the line.
    if (!_notify_list.empty()) {
        wake_front_waiter();
    }
}

void reader_concurrency_semaphore_shared_pool::maybe_wake_next_waiter() noexcept {
    if (available_memory() > 0 && !_notify_list.empty()) {
        wake_front_waiter();
    }
}

void reader_concurrency_semaphore_shared_pool::request_wakeup(reader_concurrency_semaphore& sem) noexcept {
    if (!sem._on_shared_pool_notify_list) {
        sem._on_shared_pool_notify_list = true;
        _notify_list.emplace_back(sem);
    }
}

void reader_concurrency_semaphore_shared_pool::unregister_wakeup(reader_concurrency_semaphore& sem) noexcept {
    if (sem._on_shared_pool_notify_list) {
        sem._on_shared_pool_notify_list = false;
        std::erase_if(_notify_list, [&sem](std::reference_wrapper<reader_concurrency_semaphore> ref) {
            return &ref.get() == &sem;
        });
    }
}

// Calling adjust is serialized since 2 adjustments can't happen simultaneously,
// if they did the behaviour would be undefined.
future<> reader_concurrency_semaphore_group::adjust() {
    return with_semaphore(_operations_serializer, 1, [this] () {
        // Clamp out-of-range values rather than rejecting them, so an invalid
        // configured value degrades gracefully instead of failing.
        const double shared_pool_fraction = std::clamp(_shared_pool_fraction, 0.0, 1.0);
        const ssize_t shared_memory = _total_memory * shared_pool_fraction;
        const ssize_t dedicated_memory = _total_memory - shared_memory;
        _shared_pool.set_total_memory(shared_memory);

        ssize_t distributed_memory = 0;
        for (auto& [sg, wsem] : _semaphores) {
            const ssize_t memory_share = std::floor((double(wsem.weight) / double(_total_weight)) * dedicated_memory);
            const ssize_t unreduced_memory_share = std::floor((double(wsem.weight) / double(_total_weight)) * _total_memory);
            wsem.sem.set_resources({_max_concurrent_reads, memory_share}, unreduced_memory_share);
            distributed_memory += memory_share;
        }
        // Slap the remainder on one of the semaphores.
        // This will be a few bytes, doesn't matter where we add it.
        auto& sem = _semaphores.begin()->second.sem;
        sem.set_resources(sem.initial_resources() + reader_resources{0, dedicated_memory - distributed_memory}, sem.unreduced_memory());
    });
}

// The call to change_weight is serialized as a consequence of the call to adjust.
future<> reader_concurrency_semaphore_group::change_weight(weighted_reader_concurrency_semaphore& sem, size_t new_weight) {
    auto diff = new_weight - sem.weight;
    if (diff) {
        sem.weight += diff;
        _total_weight += diff;
        return adjust();
    }
    return make_ready_future<>();
}

future<> reader_concurrency_semaphore_group::wait_adjust_complete() {
    return with_semaphore(_operations_serializer, 1, [] {
        return make_ready_future<>();
    });
}

future<> reader_concurrency_semaphore_group::stop() noexcept {
    return parallel_for_each(_semaphores, [] (auto&& item) {
        return item.second.sem.stop();
    }).then([this] {
        _semaphores.clear();
    });
}

reader_concurrency_semaphore& reader_concurrency_semaphore_group::get(scheduling_group sg) {
    return _semaphores.at(sg).sem;
}
reader_concurrency_semaphore* reader_concurrency_semaphore_group::get_or_null(scheduling_group sg) {
    auto it = _semaphores.find(sg);
    if (it == _semaphores.end()) {
        return nullptr;
    } else {
        return &(it->second.sem);
    }
}

reader_concurrency_semaphore& reader_concurrency_semaphore_group::add_or_update(scheduling_group sg, size_t shares) {
    auto result = _semaphores.try_emplace(
            sg,
            0,
            _max_concurrent_reads,
            _name_prefix ? format("{}_{}", *_name_prefix, sg.name()) : sg.name(),
            _max_queue_length,
            _serialize_limit_multiplier,
            _kill_limit_multiplier,
            _cpu_concurrency,
            _preemptive_abort_factor,
            _shared_pool
        );
    auto&& it = result.first;
    // since we serialize all group changes this change wait will be queues and no further operations
    // will be executed until this adjustment ends.
    (void)change_weight(it->second, shares);
    return it->second.sem;
}

future<> reader_concurrency_semaphore_group::remove(scheduling_group sg) {
    auto node_handle =  _semaphores.extract(sg);
    if (!node_handle.empty()) {
        weighted_reader_concurrency_semaphore& sem = node_handle.mapped();
        return sem.sem.stop().then([this, &sem] {
            return change_weight(sem, 0);
        }).finally([node_handle = std::move(node_handle)] () {
            // this holds on to the node handle until we destroy it only after the semaphore
            // is stopped properly.
        });
    }
    return make_ready_future();
}

size_t reader_concurrency_semaphore_group::size() {
    return _semaphores.size();
}

void reader_concurrency_semaphore_group::foreach_semaphore(std::function<void(scheduling_group, reader_concurrency_semaphore&)> func) {
    for (auto& [sg, wsem] : _semaphores) {
        func(sg, wsem.sem);
    }
}

future<>
reader_concurrency_semaphore_group::foreach_semaphore_async(std::function<future<> (scheduling_group, reader_concurrency_semaphore&)> func) {
    auto units = co_await get_units(_operations_serializer, 1);
    for (auto& [sg, wsem] : _semaphores) {
        co_await func(sg, wsem.sem);
    }
}
