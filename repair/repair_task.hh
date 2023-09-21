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
protected:
    streaming::stream_reason _reason;
public:
    repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, unsigned sequence_number, std::string keyspace, std::string table, std::string entity, tasks::task_id parent_id, streaming::stream_reason reason) noexcept
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(entity), parent_id)
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
            .task_info = tasks::task_info(_status.id, _status.shard)
        };
    }

    virtual future<> run() override = 0;
};

class user_requested_repair_task_impl : public repair_task_impl {
private:
    lw_shared_ptr<locator::global_effective_replication_map> _germs;
    std::vector<sstring> _cfs;
    dht::token_range_vector _ranges;
    std::vector<sstring> _hosts;
    std::vector<sstring> _data_centers;
    std::unordered_set<gms::inet_address> _ignore_nodes;
public:
    user_requested_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string entity, lw_shared_ptr<locator::global_effective_replication_map> germs, std::vector<sstring> cfs, dht::token_range_vector ranges, std::vector<sstring> hosts, std::vector<sstring> data_centers, std::unordered_set<gms::inet_address> ignore_nodes) noexcept
        : repair_task_impl(module, id.uuid(), id.id, std::move(keyspace), "", std::move(entity), tasks::task_id::create_null_id(), streaming::stream_reason::repair)
        , _germs(germs)
        , _cfs(std::move(cfs))
        , _ranges(std::move(ranges))
        , _hosts(std::move(hosts))
        , _data_centers(std::move(data_centers))
        , _ignore_nodes(std::move(ignore_nodes))
    {}

    virtual tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable::yes;
    }
protected:
    future<> run() override;

    // TODO: implement progress for user-requested repairs
};

class data_sync_repair_task_impl : public repair_task_impl {
private:
    dht::token_range_vector _ranges;
    std::unordered_map<dht::token_range, repair_neighbors> _neighbors;
    optimized_optional<abort_source::subscription> _abort_subscription;
public:
    data_sync_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string entity, dht::token_range_vector ranges, std::unordered_map<dht::token_range, repair_neighbors> neighbors, streaming::stream_reason reason, shared_ptr<node_ops_info> ops_info)
        : repair_task_impl(module, id.uuid(), id.id, std::move(keyspace), "", std::move(entity), tasks::task_id::create_null_id(), reason)
        , _ranges(std::move(ranges))
        , _neighbors(std::move(neighbors))
    {
        if (ops_info && ops_info->as) {
            _abort_subscription = ops_info->as->subscribe([this] () noexcept {
                (void)abort();
            });
        }
    }

    virtual tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable(!_abort_subscription);
    }
protected:
    future<> run() override;

    // TODO: implement progress for data-sync repairs
};

class shard_repair_task_impl : public repair_task_impl {
public:
    repair_service& rs;
    seastar::sharded<replica::database>& db;
    seastar::sharded<netw::messaging_service>& messaging;
    sharded<db::system_distributed_keyspace>& sys_dist_ks;
    sharded<db::view::view_update_generator>& view_update_generator;
    service::migration_manager& mm;
    gms::gossiper& gossiper;
    const dht::sharder& sharder;
    locator::effective_replication_map_ptr erm;
    dht::token_range_vector ranges;
    std::vector<sstring> cfs;
    std::vector<table_id> table_ids;
    repair_uniq_id id;
    std::vector<sstring> data_centers;
    std::vector<sstring> hosts;
    std::unordered_set<gms::inet_address> ignore_nodes;
    std::unordered_map<dht::token_range, repair_neighbors> neighbors;
    size_t total_rf;
    uint64_t nr_ranges_finished = 0;
    uint64_t nr_ranges_total;
    size_t nr_failed_ranges = 0;
    bool aborted = false;
    int ranges_index = 0;
    repair_stats _stats;
    std::unordered_set<sstring> dropped_tables;
    bool _hints_batchlog_flushed = false;
public:
    shard_repair_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            const sstring& keyspace,
            repair_service& repair,
            locator::effective_replication_map_ptr erm_,
            const dht::token_range_vector& ranges_,
            std::vector<table_id> table_ids_,
            repair_uniq_id parent_id_,
            const std::vector<sstring>& data_centers_,
            const std::vector<sstring>& hosts_,
            const std::unordered_set<gms::inet_address>& ignore_nodes_,
            streaming::stream_reason reason_,
            bool hints_batchlog_flushed);
    virtual tasks::is_internal is_internal() const noexcept override {
        return tasks::is_internal::yes;
    }
    void check_failed_ranges();
    void check_in_abort_or_shutdown();
    repair_neighbors get_repair_neighbors(const dht::token_range& range);
    void update_statistics(const repair_stats& stats) {
        _stats.add(stats);
    }
    const std::vector<sstring>& table_names() {
        return cfs;
    }
    const std::string& get_keyspace() const noexcept {
        return _status.keyspace;
    }
    streaming::stream_reason reason() const noexcept {
        return _reason;
    }

    bool hints_batchlog_flushed() const {
        return _hints_batchlog_flushed;
    }

    future<> repair_range(const dht::token_range& range, table_id);

    size_t ranges_size();

    virtual void release_resources() noexcept override;
protected:
    future<> do_repair_ranges();
    future<> run() override;
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
    std::unordered_map<int, tasks::task_id> _repairs;
    std::unordered_set<tasks::task_id> _pending_repairs;
    std::unordered_set<tasks::task_id> _aborted_pending_repairs;
    // The semaphore used to control the maximum
    // ranges that can be repaired in parallel.
    named_semaphore _range_parallelism_semaphore;
    seastar::condition_variable _done_cond;
    void start(repair_uniq_id id);
    void done(repair_uniq_id id, bool succeeded);
public:
    static constexpr size_t max_repair_memory_per_range = 32 * 1024 * 1024;

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
    void add_shard_task_id(int id, tasks::task_id ri);
    void remove_shard_task_id(int id);
    tasks::task_manager::task_ptr get_shard_task_ptr(int id);
    std::vector<int> get_active() const;
    size_t nr_running_repair_jobs();
    void abort_all_repairs();
    named_semaphore& range_parallelism_semaphore();
    future<> run(repair_uniq_id id, std::function<void ()> func);
    future<repair_status> repair_await_completion(int id, std::chrono::steady_clock::time_point timeout);
    float report_progress(streaming::stream_reason reason);
    bool is_aborted(const tasks::task_id& uuid);
};
