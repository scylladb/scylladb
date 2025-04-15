/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include "locator/tablets.hh"
#include "raft/raft.hh"
#include "service/topology_state_machine.hh"
#include "service/view_building_state.hh"

namespace netw {
class messaging_service;
}

namespace raft {
class server;
}

namespace service {

class group0_guard;
class raft_group0;


namespace view_building {

class view_building_coordinator {
    replica::database& _db;
    raft::server& _raft;
    raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    netw::messaging_service& _messaging;
    view_building_state_machine& _vb_sm;
    const topology_state_machine& _topo_sm;
    const raft::term_t _term;
    abort_source& _as;

    std::unordered_map<locator::tablet_replica, shared_future<>> _remote_work;

public:
    view_building_coordinator(replica::database& db, raft::server& raft, raft_group0& group0,
            db::system_keyspace& sys_ks, netw::messaging_service& ms,
            view_building_state_machine& vb_sm, const topology_state_machine& topo_sm,
            raft::term_t term, abort_source& as);

    future<> run();
    future<> stop();

private:
    future<group0_guard> start_operation();
    future<> await_event();
    future<> commit_mutations(group0_guard guard, std::vector<mutation> mutations, std::string_view description);
    void handle_coordinator_error(std::exception_ptr eptr);

    future<std::optional<group0_guard>> update_state(group0_guard guard);
    // Returns if any new tasks were started
    future<bool> work_on_view_building(group0_guard guard);

    future<> mark_view_build_status_started(const group0_guard& guard, table_id view_id, std::vector<mutation>& out);
    future<> mark_all_remaining_view_build_statuses_started(const group0_guard& guard, table_id base_id, std::vector<mutation>& out);
    future<> update_views_statuses(const group0_guard& guard, table_id base_id, std::vector<mutation>& out);

    std::set<locator::tablet_replica> get_replicas_with_tasks();
    std::vector<utils::UUID> get_started_tasks(locator::tablet_replica replica);
    std::vector<utils::UUID> select_tasks_for_replica(locator::tablet_replica replica);
    
    future<std::vector<mutation>> start_tasks(const group0_guard& guard, std::vector<utils::UUID> tasks);
    void attach_to_started_tasks(const locator::tablet_replica& replica, std::vector<utils::UUID> tasks);
    future<> work_on_tasks(locator::tablet_replica replica, std::vector<utils::UUID> tasks);
    future<> update_state_after_work_is_done(const locator::tablet_replica& replica, std::vector<utils::UUID> tasks, std::vector<view_task_result> results);
};

}

}
