/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "node_ops/node_ops_ctl.hh"
#include "repair/repair.hh"
#include "service/topology_guard.hh"
#include "streaming/stream_reason.hh"
#include "tasks/task_manager.hh"
#include <cstddef>

namespace repair {

class remote_metas;

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
            .task_info = tasks::task_info(_status.id, _status.shard)
        };
    }

    virtual future<> run() override = 0;
};

class user_requested_repair_task_impl : public repair_task_impl {
private:
    lw_shared_ptr<locator::global_vnode_effective_replication_map> _germs;
    std::vector<sstring> _cfs;
    dht::token_range_vector _ranges;
    std::vector<sstring> _hosts;
    std::vector<sstring> _data_centers;
    std::unordered_set<locator::host_id> _ignore_nodes;
    bool _small_table_optimization;
    std::optional<int> _ranges_parallelism;
    gms::gossiper& _gossiper;
public:
    user_requested_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string entity, lw_shared_ptr<locator::global_vnode_effective_replication_map> germs, std::vector<sstring> cfs, dht::token_range_vector ranges, std::vector<sstring> hosts, std::vector<sstring> data_centers, std::unordered_set<locator::host_id> ignore_nodes, bool small_table_optimization, std::optional<int> ranges_parallelism, gms::gossiper& gossiper) noexcept
        : repair_task_impl(module, id.uuid(), id.id, "keyspace", std::move(keyspace), "", std::move(entity), tasks::task_id::create_null_id(), streaming::stream_reason::repair)
        , _germs(germs)
        , _cfs(std::move(cfs))
        , _ranges(std::move(ranges))
        , _hosts(std::move(hosts))
        , _data_centers(std::move(data_centers))
        , _ignore_nodes(std::move(ignore_nodes))
        , _small_table_optimization(small_table_optimization)
        , _ranges_parallelism(ranges_parallelism)
        , _gossiper(gossiper)
    {}

    virtual tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable::yes;
    }

    tasks::is_user_task is_user_task() const noexcept override;
protected:
    future<> run() override;

    virtual future<std::optional<double>> expected_total_workload() const override;
};

class data_sync_repair_task_impl : public repair_task_impl {
private:
    dht::token_range_vector _ranges;
    std::unordered_map<dht::token_range, repair_neighbors> _neighbors;
    optimized_optional<abort_source::subscription> _abort_subscription;
    size_t _cfs_size = 0;
public:
    data_sync_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string entity, dht::token_range_vector ranges, std::unordered_map<dht::token_range, repair_neighbors> neighbors, streaming::stream_reason reason, shared_ptr<node_ops_info> ops_info)
        : repair_task_impl(module, id.uuid(), id.id, "keyspace", std::move(keyspace), "", std::move(entity), tasks::task_id::create_null_id(), reason)
        , _ranges(std::move(ranges))
        , _neighbors(std::move(neighbors))
    {
        if (ops_info && ops_info->as) {
            _abort_subscription = ops_info->as->subscribe([this] () noexcept {
                abort();
            });
        }
    }

    virtual tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable(!_abort_subscription);
    }
protected:
    future<> run() override;

    virtual future<std::optional<double>> expected_total_workload() const override;
};

struct hosts_and_tables {
    std::unordered_set<locator::host_id> hosts;
    std::unordered_set<table_id> tables;
};

struct remote_data {
    utils::chunked_vector<tablet_repair_task_meta> metas;
};

class remote_metas {
public:
    using remote_data_ptr = foreign_ptr<lw_shared_ptr<remote_data>>;
private:
    std::vector<remote_data_ptr> _metas_on_shards;
    size_t _metas_count = 0;

    remote_metas() : _metas_on_shards(smp::count) {}
public:
    future<> for_each_local_meta(std::function<future<>(const tablet_repair_task_meta&)> func) const;
    size_t size() const;
    void clear();
    future<hosts_and_tables> get_hosts_and_tables() const;

    friend class remote_metas_builder;
};

class remote_metas_builder {
private:
    remote_metas _remote_metas;
    std::unordered_map<size_t, std::vector<tablet_repair_task_meta>> _pending_metas;
    constexpr static size_t max_pending_metas_per_shard = 32;
public:
    remote_metas_builder() : _remote_metas() {}

    future<> add_on_shard(size_t shard_id, tablet_repair_task_meta meta);
    future<remote_metas> build() &&;
private:
    future<> flush(size_t shard_id);
    future<> allocate_on_shard(size_t shard_id);
};

class tablet_repair_task_impl : public repair_task_impl {
private:
    sstring _keyspace;
    std::vector<sstring> _tables;
    remote_metas _metas;
    optimized_optional<abort_source::subscription> _abort_subscription;
    std::optional<int> _ranges_parallelism;
    size_t _metas_size = 0;
    gc_clock::time_point _flush_time;
    service::frozen_topology_guard _topo_guard;
    utils::chunked_vector<locator::effective_replication_map_ptr> _erms;
    bool _skip_flush;
public:
    bool sched_by_scheduler = false;
public:
    tablet_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, sstring keyspace, std::vector<sstring> tables, streaming::stream_reason reason, remote_metas metas, std::optional<int> ranges_parallelism, service::frozen_topology_guard topo_guard, utils::chunked_vector<locator::effective_replication_map_ptr> erms, bool skip_flush = false)
        : repair_task_impl(module, id.uuid(), id.id, "keyspace", keyspace, "", "", tasks::task_id::create_null_id(), reason)
        , _keyspace(std::move(keyspace))
        , _tables(std::move(tables))
        , _metas(std::move(metas))
        , _ranges_parallelism(ranges_parallelism)
        , _topo_guard(topo_guard)
        , _erms(std::move(erms))
        , _skip_flush(skip_flush)
    {
    }

    virtual tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable(!_abort_subscription);
    }

    gc_clock::time_point get_flush_time() const { return _flush_time; }

    tasks::is_user_task is_user_task() const noexcept override;
    virtual future<> release_resources() noexcept override;
private:
    size_t get_metas_size() const noexcept;
protected:
    future<> run() override;

    virtual future<std::optional<double>> expected_total_workload() const override;
};

class shard_repair_task_impl : public repair_task_impl {
public:
    repair_service& rs;
    seastar::sharded<replica::database>& db;
    seastar::sharded<netw::messaging_service>& messaging;
    service::migration_manager& mm;
    gms::gossiper& gossiper;
private:
    locator::effective_replication_map_ptr erm;
public:
    dht::token_range_vector ranges;
    std::vector<sstring> cfs;
    std::vector<table_id> table_ids;
    repair_uniq_id global_repair_id;
    std::vector<sstring> data_centers;
    std::vector<sstring> hosts;
    std::unordered_set<locator::host_id> ignore_nodes;
    std::unordered_map<dht::token_range, repair_neighbors> neighbors;
    uint64_t nr_ranges_finished = 0;
    size_t nr_failed_ranges = 0;
    int ranges_index = 0;
    repair_stats _stats;
    std::unordered_set<sstring> dropped_tables;
    bool _hints_batchlog_flushed = false;
    std::unordered_set<locator::host_id> nodes_down;
    bool _small_table_optimization = false;
    size_t small_table_optimization_ranges_reduced_factor = 1;
private:
    bool _aborted = false;
    std::optional<sstring> _failed_because;
    std::optional<semaphore> _user_ranges_parallelism;
    uint64_t _ranges_complete = 0;
    gc_clock::time_point _flush_time;
    service::frozen_topology_guard _frozen_topology_guard;
    service::topology_guard _topology_guard = {service::null_topology_guard};
public:
    bool sched_by_scheduler = false;
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
            const std::unordered_set<locator::host_id>& ignore_nodes_,
            streaming::stream_reason reason_,
            bool hints_batchlog_flushed,
            bool small_table_optimization,
            std::optional<int> ranges_parallelism,
            gc_clock::time_point flush_time,
            service::frozen_topology_guard topo_guard,
            bool sched_by_scheduler = false);
    void check_failed_ranges();
    void check_in_abort_or_shutdown();
    repair_neighbors get_repair_neighbors(const dht::token_range& range);
    gc_clock::time_point get_flush_time() const { return _flush_time; }
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

    locator::effective_replication_map_ptr get_erm();

    size_t get_total_rf() {
        return get_erm()->get_replication_factor();
    }

    future<> repair_range(const dht::token_range& range, table_info table);

    size_t ranges_size() const noexcept;

    virtual future<> release_resources() noexcept override;
protected:
    future<> do_repair_ranges();
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
    float report_progress();
    future<bool> is_aborted(const tasks::task_id& uuid, shard_id shard);
};

}
