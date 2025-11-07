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
#include "locator/abstract_replication_strategy.hh"
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
     * The batch lives on shard, where its executing its work exclusively.
     */
    class batch {
    public:
        table_id base_id;
        locator::tablet_replica replica;
        std::unordered_map<utils::UUID, view_building_task> tasks;

        shared_promise<> promise;
        future<> work = make_ready_future();
        abort_source as;

        batch(sharded<view_building_worker>& vbw, std::unordered_map<utils::UUID, view_building_task> tasks, table_id base_id, locator::tablet_replica replica);
        void start();
        future<> abort_task(utils::UUID id);
        future<> abort();

    private:
        sharded<view_building_worker>& _vbw;

        future<> do_work();
    };

    friend class batch;

    struct state {
        std::optional<table_id> processing_base_table = std::nullopt;
        std::unordered_set<utils::UUID> completed_tasks;
        std::unique_ptr<batch> _batch = nullptr;
        std::unordered_set<table_id> flushed_views;

        semaphore _mutex = semaphore(1);
        // All of the methods below should be executed while holding `_mutex` unit!
        future<> update_processing_base_table(replica::database& db, const view_building_state& building_state, abort_source& as);
        future<> flush_base_table(replica::database& db, table_id base_table_id, abort_source& as);
        future<> clean_up_after_batch();
        future<> clear();
    };

    // Wrapper which represents information needed to create
    // `process_staging` view building task.
    struct staging_sstable_task_info {
        table_id table_id;
        shard_id shard;
        dht::token last_token;
        foreign_ptr<sstables::shared_sstable> sst_foreign_ptr;
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

    state _state;
    std::unordered_set<table_id> _views_in_progress;
    future<> _view_building_state_observer = make_ready_future<>();

    condition_variable _sstables_to_register_event;
    semaphore _staging_sstables_mutex = semaphore(1);
    std::unordered_map<table_id, std::vector<staging_sstable_task_info>> _sstables_to_register;
    std::unordered_map<table_id, std::vector<sstables::shared_sstable>> _staging_sstables;
    future<> _staging_sstables_registrator = make_ready_future<>();

public:
    view_building_worker(replica::database& db, db::system_keyspace& sys_ks, service::migration_notifier& mnotifier,
            service::raft_group0_client& group0_client, view_update_generator& vug, netw::messaging_service& ms,
            view_building_state_machine& vbsm);
    future<> init();

    future<> register_staging_sstable_tasks(std::vector<sstables::shared_sstable> ssts, table_id table_id);

    future<> drain();
    future<> stop();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override {};
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {};
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;

    // Used ONLY to load staging sstables migrated during intra-node tablet migration.
    void load_sstables(table_id table_id, std::vector<sstables::shared_sstable> ssts);
    // Used in cleanup/cleanup-target tablet transition stage
    void cleanup_staging_sstables(locator::effective_replication_map_ptr erm, table_id table_id, locator::tablet_id tid);

private:
    future<view_building_state> get_latest_view_building_state();
    future<> check_for_aborted_tasks();

    future<> run_view_building_state_observer();
    future<> update_built_views();

    dht::token_range get_tablet_token_range(table_id table_id, dht::token last_token);
    future<> do_build_range(table_id base_id, std::vector<table_id> views_ids, dht::token last_token, abort_source& as);
    future<> do_process_staging(table_id base_id, dht::token last_token);

    future<> run_staging_sstables_registrator();
    // Caller must hold units from `_staging_sstables_mutex`
    future<> create_staging_sstable_tasks();
    future<> discover_existing_staging_sstables();
    std::unordered_map<table_id, std::vector<staging_sstable_task_info>> discover_local_staging_sstables(building_tasks building_tasks);

    void init_messaging_service();
    future<> uninit_messaging_service();
    future<std::vector<utils::UUID>> work_on_tasks(std::vector<utils::UUID> ids);
};

}

}
