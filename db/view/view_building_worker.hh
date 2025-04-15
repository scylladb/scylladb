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
#include "service/view_building_state.hh"
#include "utils/UUID.hh"

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

namespace view {

class view_update_generator;

class view_building_worker : public seastar::peering_sharded_service<view_building_worker> {
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
     * The batch lives on shard 0 but it might execute its work on a different shard.
     */

    enum class batch_state {
        idle,
        in_progress,
        finished,
        failed,
    };

    class batch {
    public:
        batch_state state = batch_state::idle;
        std::unordered_map<utils::UUID, service::view_building::view_building_task> tasks;
        table_id base_id;
        locator::tablet_replica replica;
        
        shared_future<> work;
        condition_variable batch_done_cv;
        sharded<abort_source> abort_sources;

        batch(view_building_worker& vbw, std::unordered_map<utils::UUID, service::view_building::view_building_task> tasks, table_id base_id, locator::tablet_replica replica);
        future<> start();
        future<> abort_task(utils::UUID id);
        future<> abort();

    private:
        view_building_worker& _vbw;

        future<> do_work();
        future<> do_build_range(view_building_worker& local_vbw);
        future<> do_process_staging(view_building_worker& local_vbw);
    };

    friend class batch;

    struct local_state {
        std::optional<table_id> processing_base_table = std::nullopt;
        std::unordered_map<utils::UUID, shared_ptr<batch>> tasks_map;
        std::unordered_set<utils::UUID> finished_tasks;
        std::unordered_set<utils::UUID> failed_tasks;

        // Clears completed/aborted tasks and creates batches (without starting them) for started tasks.
        // Returns a map of tasks per shard to execute.
        future<> update(view_building_worker& vbw);

        future<> finish_completed_tasks();

        // The state can be aborted if, for example, a view is dropped, then all its tasks
        // are aborted and the coordinator may choose new base table to process.
        // This method aborts all batches as we stop to processing the current base table.
        future<> clear_state();
    };

    class consumer;

private:
    replica::database& _db;
    service::raft_group0_client& _group0_client;
    view_update_generator& _vug;
    netw::messaging_service& _messaging;
    service::view_building::view_building_state_machine& _vb_state_machine;
    abort_source _as;

    local_state _state;
    future<> _view_building_state_observer = make_ready_future<>();
    
public:
    view_building_worker(replica::database& db, service::raft_group0_client& group0_client, view_update_generator& vug, netw::messaging_service& ms, service::view_building::view_building_state_machine& vbsm);
    void start_state_observer();

    future<> drain();
    future<> stop();

private:
    future<> run_view_building_state_observer();
    future<> update_building_state();
    bool is_shard_free(shard_id shard);
    
    dht::token_range get_tablet_token_range(table_id table_id, dht::token last_token);

    void init_messaging_service();
    future<> uninit_messaging_service();
    future<std::vector<service::view_building::view_task_result>> get_tasks_results(std::vector<utils::UUID> ids);
};

}

}
