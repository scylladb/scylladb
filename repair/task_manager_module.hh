/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

#include "gc_clock.hh"
#include "repair/repair.hh"
#include "tasks/task_manager.hh"

namespace repair {

// The repair::task_manager_module tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
class task_manager_module : public tasks::task_manager::module {
private:
    repair_service& _rs;
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id <= repair_module::_sequence_number
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> _status;
    // Map repair id into repair_info.
    std::unordered_map<int, lw_shared_ptr<repair_info>> _repairs;
    std::unordered_set<tasks::task_id> _pending_repairs;
    // The semaphore used to control the maximum
    // ranges that can be repaired in parallel.
    named_semaphore _range_parallelism_semaphore;
    seastar::condition_variable _done_cond;
    void start(repair_uniq_id id);
    void done(repair_uniq_id id, bool succeeded);
public:
    static constexpr size_t max_repair_memory_per_range = 32 * 1024 * 1024;

    task_manager_module(tasks::task_manager& tm, repair_service& rs, size_t max_repair_memory) noexcept;

    repair_service& get_repair_service() noexcept {
        return _rs;
    }

    repair_uniq_id new_repair_uniq_id() noexcept {
        return repair_uniq_id{
            .id = new_sequence_number(),
            .task_id = tasks::task_id::create_random_id(),
        };
    }

    repair_uniq_id get_repair_uniq_id(tasks::task_manager::task::impl& task) const noexcept;

    // Creates and starts a task repairing the given repair_info's ranges on
    // this shard. Binds the task's abort source to ri when the task starts.
    future<tasks::task_manager::task_ptr> start_shard_repair_task(tasks::task_info parent_data, lw_shared_ptr<repair_info> ri, gc_clock::time_point flush_time);

    repair_status get(int id) const;
    void check_in_shutdown();
    void add_repair_info(int id, lw_shared_ptr<repair_info> ri);
    void remove_repair_info(int id);
    std::vector<int> get_active() const;
    size_t nr_running_repair_jobs();
    void abort_all_repairs();
    // Aborts user-requested repair jobs whose effective_replication_map pins
    // a token metadata version older than current_version. Such repairs block
    // topology barriers (raft_topology_cmd::barrier_and_drain) for their whole
    // duration, which is unbounded.
    void abort_repairs_pinning_stale_versions(locator::token_metadata::version_t current_version);
    named_semaphore& range_parallelism_semaphore();
    future<> run(repair_uniq_id id, std::function<void ()> func);
    future<repair_status> repair_await_completion(int id, std::chrono::steady_clock::time_point timeout);
    float report_progress();
    future<bool> is_aborted(const tasks::task_id& uuid, shard_id shard);
};

}
