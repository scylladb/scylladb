/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

/*
 * Copyright (C) 2021-present ScyllaDB
 */

#pragma once

#include <unordered_map>
#include <optional>
#include "reader_concurrency_semaphore.hh"

// A shared pool of memory that can be used by multiple reader_concurrency_semaphores.
// When a semaphore exhausts its dedicated memory, it can borrow from this pool.
class shared_memory_pool {
    ssize_t _available_memory;
    ssize_t _total_memory;
public:
    explicit shared_memory_pool(ssize_t memory) noexcept
        : _available_memory(memory)
        , _total_memory(memory) {}

    bool try_consume(ssize_t amount) noexcept {
        if (_available_memory >= amount) {
            _available_memory -= amount;
            return true;
        }
        return false;
    }

    void signal(ssize_t amount) noexcept {
        _available_memory += amount;
    }

    ssize_t available_memory() const noexcept {
        return _available_memory;
    }

    ssize_t total_memory() const noexcept {
        return _total_memory;
    }

    void set_total_memory(ssize_t memory) noexcept {
        auto diff = memory - _total_memory;
        _total_memory = memory;
        _available_memory += diff;
    }
};

// The reader_concurrency_semaphore_group is a group of semaphores that shares a common pool of memory,
// the memory is dynamically divided between them according to a relative slice of shares each semaphore
// is given.
// All of the mutating operations on the group are asynchronic and serialized. The semaphores are created
// and managed by the group.

class reader_concurrency_semaphore_group {
    size_t _total_memory;
    size_t _total_weight;
    size_t _max_concurrent_reads;
    size_t _max_queue_length;
    utils::updateable_value<uint32_t> _serialize_limit_multiplier;
    utils::updateable_value<uint32_t> _kill_limit_multiplier;
    utils::updateable_value<uint32_t> _cpu_concurrency;
    utils::updateable_value<float> _preemptive_abort_factor;

    friend class database_test_wrapper;

    struct weighted_reader_concurrency_semaphore {
        size_t weight;
        ssize_t memory_share;
        reader_concurrency_semaphore sem;
        weighted_reader_concurrency_semaphore(size_t shares, int count, sstring name, size_t max_queue_length,
                utils::updateable_value<uint32_t> serialize_limit_multiplier,
                utils::updateable_value<uint32_t> kill_limit_multiplier,
                utils::updateable_value<uint32_t> cpu_concurrency,
                utils::updateable_value<float> preemptive_abort_factor)
                : weight(shares)
                , memory_share(0)
                , sem(utils::updateable_value(count), 0, name, max_queue_length, std::move(serialize_limit_multiplier), std::move(kill_limit_multiplier),
                      std::move(cpu_concurrency), std::move(preemptive_abort_factor), reader_concurrency_semaphore::register_metrics::yes) {}
    };

    std::unordered_map<scheduling_group, weighted_reader_concurrency_semaphore> _semaphores;
    seastar::semaphore _operations_serializer;
    std::optional<sstring> _name_prefix;

    future<> change_weight(weighted_reader_concurrency_semaphore& sem, size_t new_weight);

public:
    reader_concurrency_semaphore_group(size_t memory, size_t max_concurrent_reads, size_t max_queue_length,
            utils::updateable_value<uint32_t> serialize_limit_multiplier,
            utils::updateable_value<uint32_t> kill_limit_multiplier,
            utils::updateable_value<uint32_t> cpu_concurrency,
            utils::updateable_value<float> preemptive_abort_factor,
            std::optional<sstring> name_prefix = std::nullopt)
            : _total_memory(memory)
            , _total_weight(0)
            , _max_concurrent_reads(max_concurrent_reads)
            ,  _max_queue_length(max_queue_length)
            , _serialize_limit_multiplier(std::move(serialize_limit_multiplier))
            , _kill_limit_multiplier(std::move(kill_limit_multiplier))
            , _cpu_concurrency(std::move(cpu_concurrency))
            , _preemptive_abort_factor(std::move(preemptive_abort_factor))
            , _operations_serializer(1)
            , _name_prefix(std::move(name_prefix)) { }

    ~reader_concurrency_semaphore_group() {
        assert(_semaphores.empty());
    }
    future<> adjust();
    future<> wait_adjust_complete();

    future<> stop() noexcept;
    reader_concurrency_semaphore& get(scheduling_group sg);
    reader_concurrency_semaphore* get_or_null(scheduling_group sg);
    reader_concurrency_semaphore& add_or_update(scheduling_group sg, size_t shares);
    future<> remove(scheduling_group sg);
    size_t size();
    void foreach_semaphore(std::function<void(scheduling_group, reader_concurrency_semaphore&)> func);

    future<> foreach_semaphore_async(std::function<future<> (scheduling_group, reader_concurrency_semaphore&)> func);

    auto sum_read_concurrency_sem_var(std::invocable<reader_concurrency_semaphore&> auto member) {
        using ret_type = std::invoke_result_t<decltype(member), reader_concurrency_semaphore&>;
        return std::ranges::fold_left(_semaphores | std::views::values | std::views::transform([=] (weighted_reader_concurrency_semaphore& wrcs) { return std::invoke(member, wrcs.sem); }), ret_type(0), std::plus{});
    }
};
