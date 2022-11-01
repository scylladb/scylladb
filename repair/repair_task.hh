/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "repair/repair.hh"
#include "tasks/task_manager.hh"

class repair_task_impl : public tasks::task_manager::task::impl {
public:
    repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, unsigned sequence_number, std::string keyspace, std::string table, std::string type, std::string entity, tasks::task_id parent_id)
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(type), std::move(entity), parent_id) {
        _status.progress_units = "ranges";
    }
protected:
    repair_uniq_id get_repair_uniq_id() const noexcept {
        return repair_uniq_id{
            .id = _status.sequence_number,
            .task_info = tasks::task_info(_status.id, _status.shard)
        };
    }

    virtual future<> run() override = 0;
};

// The repair_module tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
class repair_module : public tasks::task_manager::module {
private:
    repair_service& _rs;
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id <= repair_module::_sequence_number
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> _status;
    // Map repair id into repair_info.
    std::unordered_map<int, lw_shared_ptr<repair_info>> _repairs;
    std::unordered_set<tasks::task_id> _pending_repairs;
    std::unordered_set<tasks::task_id> _aborted_pending_repairs;
    // The semaphore used to control the maximum
    // ranges that can be repaired in parallel.
    named_semaphore _range_parallelism_semaphore;
    static constexpr size_t _max_repair_memory_per_range = 32 * 1024 * 1024;
    seastar::condition_variable _done_cond;
    void start(repair_uniq_id id);
    void done(repair_uniq_id id, bool succeeded);
public:
    repair_module(tasks::task_manager& tm, repair_service& rs, size_t max_repair_memory) noexcept;

    repair_service& get_repair_service() noexcept {
        return _rs;
    }

    repair_uniq_id new_repair_uniq_id() noexcept {
        return repair_uniq_id{
            .id = new_sequence_number(),
            .task_info = tasks::task_info(tasks::task_id::create_random_id(), this_shard_id())
        };
    }

    repair_status get(int id) const;
    void check_in_shutdown();
    void add_repair_info(int id, lw_shared_ptr<repair_info> ri);
    void remove_repair_info(int id);
    lw_shared_ptr<repair_info> get_repair_info(int id);
    std::vector<int> get_active() const;
    size_t nr_running_repair_jobs();
    void abort_all_repairs();
    named_semaphore& range_parallelism_semaphore();
    static size_t max_repair_memory_per_range() { return _max_repair_memory_per_range; }
    future<> run(repair_uniq_id id, std::function<void ()> func);
    future<repair_status> repair_await_completion(int id, std::chrono::steady_clock::time_point timeout);
    float report_progress(streaming::stream_reason reason);
    bool is_aborted(const tasks::task_id& uuid);
};
