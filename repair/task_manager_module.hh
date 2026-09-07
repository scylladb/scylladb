/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "node_ops/node_ops_ctl.hh"
#include "repair/repair.hh"
#include "service/topology_guard.hh"
#include "streaming/stream_reason.hh"
#include "tasks/task_manager.hh"

namespace repair {

class repair_task_impl : public tasks::task_manager::task::impl {
protected:
    streaming::stream_reason _reason;
public:
    repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, unsigned sequence_number, std::string scope, std::string keyspace, std::string table, std::string entity, tasks::task_id parent_id, streaming::stream_reason reason) noexcept
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
        , _reason(reason) {
        _status.progress_units = "ranges";
    }

    virtual std::string type() const override {
        return format("{}", _reason);
    }
protected:
    repair_uniq_id get_repair_uniq_id() const noexcept {
        return repair_uniq_id{
            .id = _status.sequence_number,
            .task_id = _status.id,
        };
    }

    virtual future<> run() override = 0;
};

class shard_repair_task_impl : public repair_task_impl {
public:
    repair_info info;
private:
    std::optional<sstring> _failed_because;
    gc_clock::time_point _flush_time;
public:
    shard_repair_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            sstring keyspace,
            repair_service& repair,
            locator::effective_replication_map_ptr erm_,
            dht::token_range_vector ranges_,
            std::vector<table_id> table_ids_,
            repair_uniq_id parent_id_,
            std::vector<sstring> data_centers_,
            std::vector<sstring> hosts_,
            std::unordered_set<locator::host_id> ignore_nodes_,
            std::unordered_map<dht::token_range, repair_neighbors> neighbors_,
            streaming::stream_reason reason_,
            bool hints_batchlog_flushed,
            bool small_table_optimization,
            std::optional<int> ranges_parallelism,
            gc_clock::time_point flush_time,
            service::frozen_topology_guard topo_guard,
            tablet_repair_sched_info sched_info = tablet_repair_sched_info(),
            size_t small_table_optimization_ranges_reduced_factor_ = 1);
    void check_failed_ranges();
    gc_clock::time_point get_flush_time() const { return _flush_time; }

    virtual future<> release_resources() noexcept override;
protected:
    virtual future<tasks::task_manager::task::progress> get_progress() const override;
    future<> run() override;
};

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
    std::unordered_map<int, tasks::task_id> _repairs;
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

    repair_status get(int id) const;
    void check_in_shutdown();
    void add_shard_task_id(int id, tasks::task_id ri);
    void remove_shard_task_id(int id);
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
