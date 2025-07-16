/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include "db/system_keyspace.hh"
#include "locator/tablets.hh"
#include "mutation/canonical_mutation.hh"
#include "raft/raft.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "schema/schema_fwd.hh"
#include "service/topology_state_machine.hh"
#include "db/view/view_building_state.hh"

namespace netw {
class messaging_service;
}

namespace raft {
class server;
}

namespace gms {
class gossiper;
}

namespace service {
class group0_guard;
class raft_group0;
}

namespace db {

namespace view {

class view_building_coordinator : public service::endpoint_lifecycle_subscriber {
    replica::database& _db;
    raft::server& _raft;
    service::raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    gms::gossiper& _gossiper;
    netw::messaging_service& _messaging;
    db::view::view_building_state_machine& _vb_sm;
    const service::topology_state_machine& _topo_sm;
    const raft::term_t _term;
    abort_source& _as;


    using remote_work_results = std::vector<std::pair<utils::UUID, db::view::view_task_result>>;
    std::unordered_map<locator::tablet_replica, shared_future<std::optional<remote_work_results>>> _remote_work;

public:
    view_building_coordinator(replica::database& db, raft::server& raft, service::raft_group0& group0,
            db::system_keyspace& sys_ks, gms::gossiper& gossiper, netw::messaging_service& ms,
            db::view::view_building_state_machine& vb_sm, const service::topology_state_machine& topo_sm,
            raft::term_t term, abort_source& as);

    future<> run();
    future<> stop();

    void generate_tablet_migration_updates(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, const locator::tablet_map& tmap, locator::global_tablet_id gid, const locator::tablet_transition_info& trinfo);
    void generate_tablet_resize_updates(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id, const locator::tablet_map& old_tmap, const locator::tablet_map& new_tmap);

    void abort_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id);
    void abort_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id, locator::tablet_replica replica, dht::token last_token);
    void rollback_aborted_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id);
    void rollback_aborted_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id, locator::tablet_replica replica, dht::token last_token);

    virtual void on_up(const gms::inet_address& endpoint, locator::host_id host_id) override;

private:
    future<service::group0_guard> start_operation();
    future<> await_event();
    future<> commit_mutations(service::group0_guard guard, utils::chunked_vector<mutation> mutations, std::string_view description);
    void handle_coordinator_error(std::exception_ptr eptr);

    future<std::optional<service::group0_guard>> update_state(service::group0_guard guard);
    // Returns if any new tasks were started
    future<bool> work_on_view_building(service::group0_guard guard);

    future<> mark_view_build_status_started(const service::group0_guard& guard, table_id view_id, utils::chunked_vector<mutation>& out);
    future<> mark_all_remaining_view_build_statuses_started(const service::group0_guard& guard, table_id base_id, utils::chunked_vector<mutation>& out);
    future<> update_views_statuses(const service::group0_guard& guard, table_id base_id, utils::chunked_vector<mutation>& out);

    std::set<locator::tablet_replica> get_replicas_with_tasks();
    std::vector<utils::UUID> select_tasks_for_replica(locator::tablet_replica replica);

    future<utils::chunked_vector<mutation>> start_tasks(const service::group0_guard& guard, std::vector<utils::UUID> tasks);
    void attach_to_started_tasks(const locator::tablet_replica& replica, std::vector<utils::UUID> tasks);
    future<std::optional<remote_work_results>> work_on_tasks(locator::tablet_replica replica, std::vector<utils::UUID> tasks);
    future<utils::chunked_vector<mutation>> update_state_after_work_is_done(const service::group0_guard& guard, const locator::tablet_replica& replica, remote_work_results results);
};

void abort_view_building_tasks(const db::view::view_building_state_machine& vb_sm,
        utils::chunked_vector<canonical_mutation>& out, api::timestamp_type write_timestamp, table_id table_id, const locator::tablet_replica& replica, dht::token last_token);

}

}
