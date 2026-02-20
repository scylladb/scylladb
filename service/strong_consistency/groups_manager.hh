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

namespace service::strong_consistency {

class coordinator;
class raft_server;

/// A sharded service (currently pinned to shard 0) responsible for the lifecycle and access
/// management of all Raft groups for strongly consistent tablets hosted on this node.
///
/// Listens for token_metadata updates to automatically start Raft servers for tablets newly
/// assigned to this node and schedule the deletion of Raft servers for tablets that have moved away.
///
/// It serves as the entry point for read and write requests via acquire_server() method. It is guaranteed
/// that the raft::server instance and its associated state managed by groups_manager cannot be
/// stopped or destroyed while the returned raft_server object is alive.
///
/// Runs a background fiber (leader_info_updater) per group that monitors the raft::server state
/// and computes the next write timestamp as soon as the server becomes leader.
/// This allows write requests to proceed without waiting for read_barrier(),
/// which would otherwise be needed to compute the timestamp.
class groups_manager : public peering_sharded_service<groups_manager> {
    class state_machine_impl;
    class rpc_impl;

    friend class raft_server;

    struct leader_info {
        // The Raft term this structure describes.
        raft::term_t term;

        // The last timestamp used for mutations in this term.
        api::timestamp_type last_timestamp;
    };

    struct raft_group_state {
        // Invariants:
        // * This is equal to true iff the corresponding tablet has a replica
        //   on this (node, shard).
        bool has_tablet = false;

        lw_shared_ptr<gate> gate = nullptr;
        raft::server* server = nullptr;

        // Serialized chain of raft::server control operations (start/stop).
        // This serialization handles (rare) cases where a tablet is migrated out
        // before the raft::server has finished initializing, or conversely,
        // when a tablet is migrated back to this node before deinitialization completes.
        // Subsequent operations wait for the previous one to complete.
        //
        // Invariants:
        // * The future is resolved iff the chain of starting and stopping tasks
        //   of this Raft group is empty.
        // * The future is resolved iff the Raft group has started and exists.
        //   As a result, if the group has been removed, this raft_group_state
        //   must no longer exist.
        // * Subsequent control operations are ordered by their scheduling, i.e.
        //   if an operation A was scheduled before operation B, then
        //   operation B starts ONLY after operation A is completed.
        //
        // Exceptions:
        // * If the corresponding future throws an exception, it's critical,
        //   e.g. starting the Raft group failed. Under normal circumstances,
        //   this shouldn't throw anything.
        shared_future<> server_control_op = make_ready_future<>();

        // Responsible for controlling the ongoing Raft operations.
        //
        // The entity responsible for triggering it is groups_manager.
        // No other code should trigger it directly.
        //
        // Invariants:
        // * Triggered iff the Raft group is being removed.
        abort_source raft_ops_as;

        // Populated only when this node thinks it's a tablet raft group leader.
        std::optional<leader_info> leader_info = std::nullopt;
        condition_variable leader_info_cond = condition_variable();
        // Exceptions:
        // * If the future throws an exception, it's critical.
        //   Under normal circumstances, this shouldn't throw anything.
        future<> leader_info_updater = make_ready_future<>();
    };

private:
    netw::messaging_service& _ms;
    raft_group_registry& _raft_gr;
    cql3::query_processor& _qp;
    replica::database& _db;
    gms::feature_service& _features;
    std::unordered_map<raft::group_id, raft_group_state> _raft_groups = {};
    locator::token_metadata_ptr _pending_tm = nullptr;
    bool _started = false;

private:
    future<> start_raft_group(locator::global_tablet_id tablet,
        raft::group_id group_id,
        locator::token_metadata_ptr tm);

    void abort_raft_group_operations(raft::group_id group_id) noexcept;

    void schedule_raft_group_deletion(raft::group_id group_id, raft_group_state& group_state);

    void schedule_raft_groups_deletion(bool all);

    future<> leader_info_updater(raft_group_state& state, locator::global_tablet_id tablet, raft::group_id gid);

    future<> wait_for_groups_to_start();

public:
    groups_manager(netw::messaging_service& ms, raft_group_registry& raft_gr, 
        cql3::query_processor& qp, replica::database& _db,
        gms::feature_service& features);

    // Called whenever a new token_metadata is published on this shard.
    // Starts raft::server instances for all strongly consistent tablets now
    // residing on this shard, and schedules removal of servers for tablets
    // that have moved away.
    //
    // Note that the method is synchronous: it only initiates these operations
    // and does not wait for their completion.
    //
    // Exceptions:
    // * The function doesn't throw any exceptions.
    void update(locator::token_metadata_ptr new_tm);

    // The raft_server instance is used to submit write commands
    // and perform read_barrier() before reads.
    //
    // Preconditions:
    // * The group corresponding to the passed group_id must exist.
    //
    // Exceptions:
    // * If this function throws an exception, it's critical and unexpected.
    //   Under normal circumstances, it shouldn't throw any exceptions.
    future<raft_server> acquire_server(raft::group_id group_id);

    // Called during node boot. Waits for all raft::server instances corresponding
    // to the latest group0 state to start.
    future<> start();

    // Called during node shutdown. Waits for all raft::server instances to stop.
    future<> stop();

    // This is a temporary solution before we have tablet migrations
    // and proper shutdown management for strongly consistent tablets.
    //
    // It should only be called at shutdown to cancel all remaining
    // ongoing Raft operations to prevent getting stuck (see the comment
    // in main.cc where I elaborated on that).
    void schedule_groups_removal();
};

/// A temporary, RAII-style handle to an active Raft group server instance,
/// used to safely submit commands or perform consistency barriers.
///
/// The holder guarantees that the underlying raft::server and its associated state
/// managed by groups_manager cannot be stopped or destroyed while this raft_server object is alive.
/// It ensures that even if a topology change triggers the deletion of the Raft group,
/// the shutdown sequence will wait until this handle is destroyed, preventing use-after-free
/// errors during ongoing operations.
class raft_server {
private:
    friend class coordinator;

private:
    groups_manager::raft_group_state& _state;
    gate::holder _holder;

public:
    raft_server(groups_manager::raft_group_state& state, gate::holder holder);

    raft::server& server() {
        return *_state.server;
    }

    // Possible results:
    //   timestamp_with_term - timestamp to use for a new mutation request
    //   raft::not_a_leader - this node is not a leader
    //   need_wait_for_leader - the caller needs to wait on the specified future and then retry `begin_mutate`
    struct timestamp_with_term {
        api::timestamp_type timestamp;
        raft::term_t term;
    };
    struct need_wait_for_leader {
        future<> future;
    };
    using begin_mutate_result = std::variant<timestamp_with_term, raft::not_a_leader, need_wait_for_leader>;
    // Exceptions:
    // * raft::request_aborted: If the result is need_wait_for_leader,
    //      the corresponding future may throw an exception if the Raft
    //      group started being removed before the operation finishes.
    // * No other exceptions.
    begin_mutate_result begin_mutate();
};

}
