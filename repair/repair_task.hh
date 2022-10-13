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
    repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, unsigned sequence_number = 0, std::string keyspace = "", std::string table = "", std::string type = "", std::string entity = "", tasks::task_id parent_id = tasks::task_id::create_null_id())
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(type), std::move(entity), parent_id) {
        _status.progress_units = "ranges";
    }
protected:
    repair_uniq_id get_repair_uniq_id() const noexcept {
        return repair_uniq_id{
            .id = _status.sequence_number,
            .task_data = tasks::task_info(_status.id, _status.shard)
        };
    }

    future<> do_repair_ranges(lw_shared_ptr<repair_info> ri);
    future<> repair_ranges(lw_shared_ptr<repair_info> ri);

    virtual future<> run() override = 0;
};

class user_requested_repair_task_impl : public repair_task_impl {
private:
    std::vector<sstring> _cfs;
    dht::token_range_vector _ranges;
    std::vector<sstring> _hosts;
    std::vector<sstring> _data_centers;
    std::unordered_set<gms::inet_address> _ignore_nodes;
public:
    user_requested_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string type, std::string entity, std::vector<sstring> cfs, dht::token_range_vector ranges, std::vector<sstring> hosts, std::vector<sstring> data_centers, std::unordered_set<gms::inet_address> ignore_nodes)
        : repair_task_impl(module, id.uuid(), id.id, std::move(keyspace), "", std::move(type), std::move(entity))
        , _cfs(std::move(cfs))
        , _ranges(std::move(ranges))
        , _hosts(std::move(hosts))
        , _data_centers(std::move(data_centers))
        , _ignore_nodes(std::move(ignore_nodes))
    {}
protected:
    future<> run() override;

    // TODO: implement abort and progress for user-requested repairs
};

class data_sync_repair_task_impl : public repair_task_impl {
private:
    dht::token_range_vector _ranges;
    std::unordered_map<dht::token_range, repair_neighbors> _neighbors;
    streaming::stream_reason _reason;
    std::optional<node_ops_id> _ops_uuid;
public:
    data_sync_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string type, std::string entity, dht::token_range_vector ranges, std::unordered_map<dht::token_range, repair_neighbors> neighbors, streaming::stream_reason reason, std::optional<node_ops_id> ops_uuid)
        : repair_task_impl(module, id.uuid(), id.id, std::move(keyspace), "", std::move(type), std::move(entity))
        , _ranges(std::move(ranges))
        , _neighbors(std::move(neighbors))
        , _reason(reason)
        , _ops_uuid(ops_uuid) {}
protected:
    future<> run() override;

    // TODO: implement abort and progress for data-sync repairs
};

class shard_repair_task_impl : public repair_task_impl {
private:
    lw_shared_ptr<repair_info> _ri;
    std::exception_ptr _ex;
public:
    shard_repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, std::string keyspace, std::string type, tasks::task_id parent_id, lw_shared_ptr<repair_info> ri, std::exception_ptr ex)
        : repair_task_impl(module, id, 0, std::move(keyspace), "", std::move(type), "", parent_id)
        , _ri(ri)
        , _ex(ex)
    {}
protected:
    future<> run() override;
};

// The repair_module tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
class repair_module : public tasks::task_manager::module {
private:
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id < _sequence_number
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> _status;
    // Map repair id into repair_info.
    std::unordered_map<int, lw_shared_ptr<repair_info>> _repairs; // TODO: This may be replaced with ptrs to shard tasks. 
    std::unordered_set<tasks::task_id> _pending_repairs;
    std::unordered_set<tasks::task_id> _aborted_pending_repairs;
    named_semaphore _range_parallelism_semaphore;
    static constexpr size_t _max_repair_memory_per_range = 32 * 1024 * 1024;
    seastar::condition_variable _done_cond;
    repair_service& _rs;

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
            .task_data = tasks::task_info(tasks::task_id::create_random_id(), this_shard_id())
        };
    }

    void check_in_shutdown();
    repair_status get(int id) const;
    future<repair_status> repair_await_completion(int id, std::chrono::steady_clock::time_point timeout);
    void add_repair_info(int id, lw_shared_ptr<repair_info> ri);
    void remove_repair_info(int id);
    lw_shared_ptr<repair_info> get_repair_info(int id);
    std::vector<int> get_active() const;
    size_t nr_running_repair_jobs();
    void abort_all_repairs();
    named_semaphore& range_parallelism_semaphore();
    static size_t max_repair_memory_per_range() { return _max_repair_memory_per_range; }
    future<> run(repair_uniq_id id, std::function<void ()> func);
    float report_progress(streaming::stream_reason reason);
    void abort_repair_node_ops(node_ops_id ops_uuid);
    bool is_aborted(const tasks::task_id& uuid);
};
