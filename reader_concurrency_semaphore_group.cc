/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "reader_concurrency_semaphore_group.hh"

// Calling adjust is serialized since 2 adjustments can't happen simultaneously,
// if they did the behaviour would be undefined.
future<> reader_concurrency_semaphore_group::adjust() {
    return with_semaphore(_operations_serializer, 1, [this] () {
        ssize_t distributed_memory = 0;
        for (auto& [sg, wsem] : _semaphores) {
            const ssize_t memory_share = std::floor((double(wsem.weight) / double(_total_weight)) * _total_memory);
            wsem.sem.set_resources({_max_concurrent_reads, memory_share});
            distributed_memory += memory_share;
        }
        // Slap the remainder on one of the semaphores.
        // This will be a few bytes, doesn't matter where we add it.
        auto& sem = _semaphores.begin()->second.sem;
        sem.set_resources(sem.initial_resources() + reader_resources{0, _total_memory - distributed_memory});
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
            _cpu_concurrency
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
