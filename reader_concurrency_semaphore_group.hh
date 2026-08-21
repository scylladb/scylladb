/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * Copyright (C) 2021-present ScyllaDB
 */

#pragma once

#include <unordered_map>
#include <optional>
#include "reader_concurrency_semaphore.hh"
#include "utils/serialized_action.hh"

// The reader_concurrency_semaphore_group is a group of semaphores that shares a common pool of memory.
// The total memory is divided based on the shared_pool_fraction parameter:
// - Dedicated portion (1 - shared_pool_fraction): divided between scheduling groups according to their relative weight/shares
// - Shared portion (shared_pool_fraction): a pool accessible by all scheduling groups when their dedicated memory is exhausted
// All of the mutating operations on the group are asynchronic and serialized. The semaphores are created
// and managed by the group.

class reader_concurrency_semaphore_group {
    size_t _total_memory;
    size_t _total_weight;
    size_t _max_concurrent_reads;
    size_t _max_queue_length;
    utils::updateable_value<double> _shared_pool_fraction;
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
                utils::updateable_value<float> preemptive_abort_factor,
                reader_concurrency_semaphore_shared_pool& shared_pool)
                : weight(shares)
                , memory_share(0)
                , sem(utils::updateable_value(count), 0, name, max_queue_length, std::move(serialize_limit_multiplier), std::move(kill_limit_multiplier),
                      std::move(cpu_concurrency), std::move(preemptive_abort_factor), reader_concurrency_semaphore::register_metrics::yes, shared_pool) {}
    };

    std::unordered_map<scheduling_group, weighted_reader_concurrency_semaphore> _semaphores;
    reader_concurrency_semaphore_shared_pool _shared_pool;
    seastar::semaphore _operations_serializer;
    std::optional<sstring> _name_prefix;
    // Re-splits the memory between the dedicated and shared portions whenever
    // the shared pool fraction changes at runtime. Must be declared before
    // its observer so the observer is destroyed first.
    serialized_action _shared_pool_fraction_action;
    utils::observer<double> _shared_pool_fraction_observer;

    future<> change_weight(weighted_reader_concurrency_semaphore& sem, size_t new_weight);

public:
    reader_concurrency_semaphore_group(size_t memory, size_t max_concurrent_reads, size_t max_queue_length,
            utils::updateable_value<uint32_t> serialize_limit_multiplier,
            utils::updateable_value<uint32_t> kill_limit_multiplier,
            utils::updateable_value<uint32_t> cpu_concurrency,
            utils::updateable_value<float> preemptive_abort_factor,
            utils::updateable_value<double> shared_pool_fraction,
            std::optional<sstring> name_prefix = std::nullopt)
            : _total_memory(memory)
            , _total_weight(0)
            , _max_concurrent_reads(max_concurrent_reads)
            ,  _max_queue_length(max_queue_length)
            , _shared_pool_fraction(std::move(shared_pool_fraction))
            , _serialize_limit_multiplier(std::move(serialize_limit_multiplier))
            , _kill_limit_multiplier(std::move(kill_limit_multiplier))
            , _cpu_concurrency(std::move(cpu_concurrency))
            , _preemptive_abort_factor(std::move(preemptive_abort_factor))
            , _shared_pool(0)
            , _operations_serializer(1)
            , _name_prefix(std::move(name_prefix))
            , _shared_pool_fraction_action([this] { return adjust(); })
            // adjust() clamps out-of-range values, so the action can't fail on
            // a bad update.
            , _shared_pool_fraction_observer(_shared_pool_fraction.observe(_shared_pool_fraction_action.make_observer()))
    {
        // Register metrics unconditionally so they remain valid when the shared
        // pool is enabled at runtime via a live config update (the gauges report
        // 0 while the pool is disabled).
        _shared_pool.register_metrics(_name_prefix.value_or("unnamed"));
    }

    ~reader_concurrency_semaphore_group() {
        assert(_semaphores.empty());
    }
    future<> adjust();
    future<> wait_adjust_complete();

    future<> stop() noexcept;
    reader_concurrency_semaphore& get(scheduling_group sg);
    reader_concurrency_semaphore* get_or_null(scheduling_group sg);
    reader_concurrency_semaphore& add_or_update(scheduling_group sg, size_t shares);
    reader_concurrency_semaphore_shared_pool& get_shared_pool() noexcept { return _shared_pool; }
    future<> remove(scheduling_group sg);
    size_t size();
    void foreach_semaphore(std::function<void(scheduling_group, reader_concurrency_semaphore&)> func);

    future<> foreach_semaphore_async(std::function<future<> (scheduling_group, reader_concurrency_semaphore&)> func);

    auto sum_read_concurrency_sem_var(std::invocable<reader_concurrency_semaphore&> auto member) {
        using ret_type = std::invoke_result_t<decltype(member), reader_concurrency_semaphore&>;
        return std::ranges::fold_left(_semaphores | std::views::values | std::views::transform([=] (weighted_reader_concurrency_semaphore& wrcs) { return std::invoke(member, wrcs.sem); }), ret_type(0), std::plus{});
    }
};
