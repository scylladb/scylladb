/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_group_registry.hh"
#include "cql3/query_processor.hh"

namespace service {

class sc_raft_server;

class sc_groups_manager: public peering_sharded_service<sc_groups_manager> {
    class state_machine_impl;
    class rpc_impl;

    friend class sc_raft_server;

    struct leader_state {
        raft::term_t term;
        api::timestamp_type last_timestamp;
    };

    struct raft_group_state {
        bool has_tablet = false;
        lw_shared_ptr<gate> gate = nullptr;
        raft::server* server = nullptr;
        shared_future<> server_op = make_ready_future<>();
        std::optional<leader_state> leader_state = std::nullopt;
        condition_variable leader_state_cond = condition_variable();
        future<> leader_state_fiber = make_ready_future<>();
    };

    netw::messaging_service& _ms;
    raft_group_registry& _raft_gr;
    cql3::query_processor& _qp;
    replica::database& _db;
    gms::feature_service& _features;
    std::unordered_map<raft::group_id, raft_group_state> _raft_groups = {};
    locator::token_metadata_ptr _pending_tm = nullptr;
    bool _started = false;
    gms::feature::listener_registration _feature_listener{};

    future<> start_raft_group(locator::global_tablet_id tablet,
        raft::group_id group_id,
        locator::token_metadata_ptr tm);

    void schedule_raft_group_deletion(raft::group_id group_id, raft_group_state& group_state);

    void schedule_raft_groups_deletion(bool all);

    future<> leader_state_fiber(raft_group_state& state, locator::global_tablet_id tablet, raft::group_id gid);

    future<> wait_for_groups_to_start();

public:
    sc_groups_manager(netw::messaging_service& ms, raft_group_registry& raft_gr, 
        cql3::query_processor& qp, replica::database& _db,
        gms::feature_service& features);

    // Called whenever a new token_metadata is published on this shard.
    // Starts raft::server instances for all strongly consistent tablets now
    // residing on this shard, and schedules removal of servers for tablets
    // that have moved away.
    //
    // Note that the method is synchronous: it only initiates these operations
    // and does not wait for their completion.
    void update(locator::token_metadata_ptr new_tm);

    // Used to submit write commands and perform read_barrier() before reads.
    future<sc_raft_server> acquire_server(raft::group_id group_id);

    // Called during node boot. Waits for all raft::server instances corresponding
    // to the latest group0 state to start.
    future<> start();

    // Called during node shutdown. Waits for all raft::server instances to stop.
    future<> stop();
};

class sc_raft_server {
private:
    sc_groups_manager::raft_group_state& _state;
    gate::holder _holder;

public:
    sc_raft_server(sc_groups_manager::raft_group_state& state, gate::holder holder);

    raft::server& server() {
        return *_state.server;
    }

    // Waits for a leader. Returns the timestamp to be used for the new mutation,
    // or nullopt if the current server is not the leader for the current term.
    using begin_mutate_result = std::pair<raft::term_t, std::optional<api::timestamp_type>>;
    future<begin_mutate_result> begin_mutate();
};

}