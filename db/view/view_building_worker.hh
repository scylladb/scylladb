/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/shared_future.hh>
#include <unordered_map>
#include <unordered_set>
#include "locator/tablets.hh"
#include "seastar/core/gate.hh"
#include "db/view/view_building_state.hh"
#include "sstables/shared_sstable.hh"
#include "utils/UUID.hh"
#include "service/migration_listener.hh"

namespace replica {
class database;
}

namespace netw {
class messaging_service;
}

namespace service {
class raft_group0_client;
}

namespace db {

class system_keyspace;

namespace view {

class view_update_generator;

// The view_building_worker is a service responsible for executing
// tasks scheduled by view_building_coordinator.
// The service observes `view_building_state` and handles `work_on_view_building_tasks()` RPC.
// In response to the RPC, the worker start the tasks (check view_building_state.hh)
// and executes them until they are finished or aborted.
// If the RPC connection is broken (for example, because the raft leader was changed
// and current view building coordinator was aborted), the work is continued in background
// and the new coordinator will attach itself to the ongoing tasks.
class view_building_worker : public seastar::peering_sharded_service<view_building_worker>, public service::migration_listener::only_view_notifications {
    /*
     * View building coordinator may send multiple tasks at once to be executed together.
     * This is possible only if all of the tasks:
     * - are of the same type
     * - their base_id is the same
     * - their last_token is the same
     * - their shard_id is the same
     *
     * Moreover, any of the task can be aborted during execution. When this happens:
     * - the batch is not aborted unless it was the last alive task
     * - entry is removed from _tasks_ids
     *
     * When `work` future is finished, it means all tasks in `tasks_ids` are done.
     *
     * The batch lives on shard 0 exclusively.
     * When the batch starts to execute its tasks, it firstly copies all necessary data
     * to the designated shard, then the work is done on the local copy of the data only.
     */

    enum class batch_state {
        idle,
        in_progress,
        finished,
    };

    class batch {
    public:
        batch_state state = batch_state::idle;
        table_id base_id;
        locator::tablet_replica replica;
        std::unordered_map<utils::UUID, view_building_task> tasks;

        shared_future<> work;
        condition_variable batch_done_cv;
        sharded<abort_source> abort_sources;

        batch(sharded<view_building_worker>& vbw, std::unordered_map<utils::UUID, view_building_task> tasks, table_id base_id, locator::tablet_replica replica);
        future<> start();
        future<> abort_task(utils::UUID id);
        future<> abort();

    private:
        sharded<view_building_worker>& _vbw;

        future<> do_work();
    };

    friend class batch;

    struct local_state {
        std::optional<table_id> processing_base_table = std::nullopt;
        // Stores ids of views for which the flush was done.
        // When a new view is created, we need to flush the base table again,
        // as data might be inserted.
        std::unordered_set<table_id> flushed_views;
        std::unordered_map<utils::UUID, shared_ptr<batch>> tasks_map;

        std::unordered_set<utils::UUID> finished_tasks;
        std::unordered_set<utils::UUID> aborted_tasks;

        condition_variable state_updated_cv;

        // Clears completed/aborted tasks and creates batches (without starting them) for started tasks.
        // Returns a map of tasks per shard to execute.
        future<> update(view_building_worker& vbw);

        future<> finish_completed_tasks();

        // The state can be aborted if, for example, a view is dropped, then all its tasks
        // are aborted and the coordinator may choose new base table to process.
        // This method aborts all batches as we stop to processing the current base table.
        future<> clear_state();

        // Flush table with `table_id` on all shards.
        // This method should be used only on currently processing base table and
        // it updates `flushed_views` field.
        future<> flush_table(view_building_worker& vbw, table_id table_id);
    };

    class consumer;

private:
    replica::database& _db;
    db::system_keyspace& _sys_ks;
    service::migration_notifier& _mnotifier;
    service::raft_group0_client& _group0_client;
    view_update_generator& _vug;
    netw::messaging_service& _messaging;
    view_building_state_machine& _vb_state_machine;
    abort_source _as;
    named_gate _gate;

    local_state _state;
    std::unordered_set<table_id> _views_in_progress;
    future<> _view_building_state_observer = make_ready_future<>();

    condition_variable _sstables_to_register_event;
    semaphore _staging_sstables_mutex = semaphore(1);
    std::unordered_map<table_id, std::vector<sstables::shared_sstable>> _sstables_to_register;
    std::unordered_map<table_id, std::unordered_map<dht::token, std::vector<sstables::shared_sstable>>> _staging_sstables;
    future<> _staging_sstables_registrator = make_ready_future<>();

public:
    view_building_worker(replica::database& db, db::system_keyspace& sys_ks, service::migration_notifier& mnotifier,
            service::raft_group0_client& group0_client, view_update_generator& vug, netw::messaging_service& ms,
            view_building_state_machine& vbsm);
    void start_backgroud_fibers();

    future<> register_staging_sstable_tasks(std::vector<sstables::shared_sstable> ssts, lw_shared_ptr<replica::table> table);
    
    future<> drain();
    future<> stop();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override {};
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {};
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;

private:
    future<> run_view_building_state_observer();
    future<> update_built_views();
    future<> update_building_state();
    bool is_shard_free(shard_id shard);

    dht::token_range get_tablet_token_range(table_id table_id, dht::token last_token);
    future<> do_build_range(table_id base_id, std::vector<table_id> views_ids, dht::token last_token, abort_source& as);
    future<> do_process_staging(table_id base_id, dht::token last_token);

    future<> run_staging_sstables_registrator();
    // Caller must hold units from `_staging_sstables_mutex`
    future<> create_staging_sstable_tasks();
    future<> discover_existing_staging_sstables();
    std::unordered_map<table_id, std::vector<sstables::shared_sstable>> discover_local_staging_sstables(building_tasks building_tasks);

    void init_messaging_service();
    future<> uninit_messaging_service();
    future<std::vector<view_task_result>> work_on_tasks(std::vector<utils::UUID> ids);
};

}

}
