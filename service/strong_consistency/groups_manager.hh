/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_group_registry.hh"
#include "cql3/query_processor.hh"
#include "db/commitlog/raft_commitlog_replay_buffer.hh"

namespace db {
class system_keyspace;
class raft_commitlog_replay_buffer;
}

namespace gms {
class gossiper;
}

namespace service {
class migration_manager;
}

namespace service::strong_consistency {

class raft_server;
class raft_resize_tracker;

// conditional_variable::wait doesn't have an overload taking an abort_source.
// This is a temporary workaround until we extend the interface.
// See: scylladb/seastar#3292.
future<> wait_with_abort_source(condition_variable& cv, abort_source& as);

/// A cache of leader locations for raft groups where this node is not a replica.
/// Populated by the CQL transport layer after a redirect reveals the actual leader.
///
/// Uses a sweep-based eviction strategy tied to token_metadata updates:
/// begin_sweep() before iterating tablets, mark_seen() for each existing group,
/// end_sweep() to evict entries whose groups no longer exist.
class tablet_group_leader_cache {
    struct entry {
        locator::host_id leader;
        bool seen = false;
    };
    std::unordered_map<raft::group_id, entry> _entries;

public:
    void put(raft::group_id group, locator::host_id leader) {
        auto [it, inserted] = _entries.try_emplace(group, entry{leader});
        if (!inserted) {
            it->second.leader = leader;
        }
    }

    std::optional<locator::host_id> get(raft::group_id group) const {
        auto it = _entries.find(group);
        if (it != _entries.end()) {
            return it->second.leader;
        }
        return std::nullopt;
    }

    void erase(raft::group_id group) {
        _entries.erase(group);
    }

    void begin_sweep() {
        for (auto& [_, e] : _entries) {
            e.seen = false;
        }
    }

    void mark_seen(raft::group_id group) {
        auto it = _entries.find(group);
        if (it != _entries.end()) {
            it->second.seen = true;
        }
    }

    void end_sweep() {
        std::erase_if(_entries, [](const auto& p) { return !p.second.seen; });
    }
};

/// A sharded service responsible for the lifecycle and access
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

    // What a parent needs while its children's leaders are being kept co-located with its own.
    // Held behind a pointer and only for the duration of a resize: raft_group_state exists for
    // every group on every shard, while a resize concerns a handful of them at a time, so the
    // condition variable, the fiber and the abort source below would otherwise be paid for by
    // every group whether or not anything is being resized.
    struct resize_colocation_state {
        resize_colocation_state(locator::global_tablet_id tablet, std::vector<raft::group_id> new_gids)
            : tablet(tablet)
            , new_gids(std::move(new_gids))
        {}

        locator::global_tablet_id tablet;

        // The children whose leaders follow this group's. The colocator iterates them across
        // preemption points, so it keeps its own copy rather than reaching into the tracker,
        // whose vector a later token metadata change may replace.
        std::vector<raft::group_id> new_gids;

        // Signalled whenever the raft state of this group, or of one of its children, changes on
        // this replica. Lets the colocator re-check the leaders without polling.
        condition_variable leader_changed;

        // Bumped together with every leader_changed broadcast. A waiter which samples it before
        // checking the leaders can tell whether a change it hasn't accounted for happened in the
        // meantime, and therefore must not go to sleep.
        uint64_t leader_change_seq = 0;

        // Runs until `as` is aborted. Joined while the group is torn down, next to
        // leader_info_updater, so it never outlives the raft servers it drives.
        future<> colocator = make_ready_future<>();

        // The fiber spends most of its life waiting outside of a raft server, so aborting the
        // servers taking part in the resize doesn't stop it.
        abort_source as;
    };

    struct raft_group_state : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
        bool has_tablet = false;
        lw_shared_ptr<gate> gate = nullptr;
        raft::server* server = nullptr;

        // Serialized chain of raft::server control operations (start/stop).
        // This serialization handles (rare) cases where a tablet is migrated out
        // before the raft::server has finished initializing, or conversely,
        // when a tablet is migrated back to this node before deinitialization completes.
        // Subsequent operations wait for the previous one to complete.
        shared_future<> server_control_op = make_ready_future<>();

        // Populated only when this node thinks it's a tablet raft group leader.
        std::optional<leader_info> leader_info = std::nullopt;
        condition_variable leader_info_cond = condition_variable();
        future<> leader_info_updater = make_ready_future<>();

        // Set on a parent for as long as it is being resized on this replica.
        std::unique_ptr<resize_colocation_state> resize_colocation;
    };

    netw::messaging_service& _ms;
    raft_group_registry& _raft_gr;
    cql3::query_processor& _qp;
    replica::database& _db;
    service::migration_manager& _mm;
    db::system_keyspace& _sys_ks;
    gms::feature_service& _features;
    gms::gossiper& _gossiper;
    db::raft_commitlog_replay_buffer& _raft_replay_buffer;
    raft_resize_tracker& _resize_tracker;
    std::unordered_map<raft::group_id, raft_group_state> _raft_groups = {};
    boost::intrusive::list<raft_group_state, boost::intrusive::constant_time_size<false>> _starting_groups;
    locator::token_metadata_ptr _pending_tm = nullptr;
    bool _started = false;

    tablet_group_leader_cache _leader_cache;

    // Should be called on the shard that hosts the Raft group
    // If the group is created as a result of a resize, the parent id is the group_id
    // of the original tablet.
    future<> start_raft_group(locator::global_tablet_id tablet,
        raft::group_id group_id,
        locator::token_metadata_ptr tm,
        std::optional<raft::group_id> parent_gid = std::nullopt);

    void schedule_raft_group_deletion(raft::group_id group_id, raft_group_state& group_state);

    void schedule_raft_groups_deletion(bool all);

    // `token` is a token the group owns, used to find the tablet it serves in the current tablet
    // map; a tablet id would not do, see stable_token_of_group().
    future<> leader_info_updater(raft_group_state& state, table_id table, raft::group_id gid,
        dht::token token);

    // The outcome of a colocate_leaders() round. Tells the caller how to wait before re-checking.
    enum class colocation_status {
        // Every child is led by the leader of its parent.
        colocated,
        // The parent's leader is unknown, an election is in progress there.
        parent_leader_unknown,
        // Nothing to do on this replica: an election is in progress in one of the children, or a
        // diverged one is led by another replica, which is the one that has to hand its
        // leadership over.
        awaiting_leader_change,
        // A leadership transfer was carried out. It only makes the target start an election,
        // which it may lose, so the outcome has to be re-checked.
        transfer_done,
        // A leadership transfer was needed but did not complete.
        transfer_failed,
    };

    // Makes sure that the leader of every group in `new_gids` is co-located with the leader of
    // their parent `parent_gid`, which is required for writes to be handed off to them (see
    // coordinator::mutate()).
    //
    // If this replica leads one of the children and it is not co-located, transfers that group's
    // leadership to the parent's leader. Never throws on a failed transfer - the caller is
    // expected to retry.
    future<colocation_status> colocate_leaders(raft_server& parent, raft::group_id parent_gid,
        const std::vector<raft::group_id>& new_gids);

    // Background fiber of a parent being replaced by its children during a tablet resize. It
    // keeps the children's leaders co-located with the parent's, which is a precondition for the
    // writes redirected to them to be served (see colocate_leaders()). Runs until the resize is
    // over on this replica.
    future<> leader_colocator(raft_group_state& state, raft::group_id parent_gid);

    // Starts the colocator of `parent_gid` unless it is already running. Idempotent: update()
    // re-records a resize on every token metadata change, and a repeated call must not start a
    // second fiber nor replace the state a running one refers to.
    void start_leader_colocator(raft_group_state& state, locator::global_tablet_id tablet,
        raft::group_id parent_gid, std::vector<raft::group_id> new_gids);

    // Signals that the raft state of `gid` - which may be either a parent being resized or one of
    // its children - changed. A no-op if `gid` is not taking part in a resize.
    void notify_leader_change(raft::group_id gid);

    void init_messaging_service();
    future<> uninit_messaging_service();

    // Returns the shard hosting the raft server of the given tablet, or nullopt if this node does
    // not own a replica of it, or if the tablet is not served by `expected_gid` here - which is
    // how a tablet id coming from a replica with a different tablet map is rejected.
    std::optional<shard_id> find_shard_for_tablet(locator::global_tablet_id tablet, raft::group_id expected_gid) const;

    // A non-blocking, non-throwing variant of acquire_server(): returns nullopt if the group
    // is not hosted here, hasn't started yet or is being stopped.
    std::optional<raft_server> try_acquire_server(raft::group_id group_id);

public:
    groups_manager(netw::messaging_service& ms, raft_group_registry& raft_gr,
        cql3::query_processor& qp, replica::database& _db, service::migration_manager& mm, db::system_keyspace& sys_ks,
        gms::feature_service& features, gms::gossiper& gossiper, db::raft_commitlog_replay_buffer& raft_replay_buffer,
        sharded<raft_resize_tracker>& resize_tracker);

    // Called whenever a new token_metadata is published on this shard.
    // Starts raft::server instances for all strongly consistent tablets now
    // residing on this shard, and schedules removal of servers for tablets
    // that have moved away.
    //
    // Note that the method is synchronous: it only initiates these operations
    // and does not wait for their completion.
    void update(locator::token_metadata_ptr new_tm);

    // The raft_server instance is used to submit write commands and perform read_barrier() before reads.
    future<raft_server> acquire_server(table_id table_id, raft::group_id group_id, abort_source& as);

    // Whether the requests of the given group are handed off to its children during a resize,
    // and which of those children covers a given token.
    bool should_handoff_writes(raft::group_id group_id) const;
    raft::group_id group_for_handoff(schema_ptr schema, const dht::token& token) const;

    // Seals the raft group `parent_gid`, which is being replaced by the groups `new_gids`.
    // Returns true once start_resize and end_resize have been committed in the parent group, or,
    // if wait_only is true, once end_resize has been applied on this replica.
    // Returns false if the call has to be retried, which covers every case where this replica
    // cannot make progress yet: it has not observed the resize, does not host the groups involved
    // yet, is not the leader of the parent group, or the leaders are not co-located yet.
    future<bool> handle_process_raft_resize(raft::group_id parent_gid,
        const std::vector<raft::group_id>& new_gids, bool wait_only, abort_source& as);

    // Called during node boot. Starts all raft::server instances corresponding
    // to the latest group0 state in the background.
    void start();

    // Called during node shutdown. Waits for all raft::server instances to stop.
    future<> stop();

    future<> wait_for_groups_to_start(lowres_clock::time_point timeout);

    // Sends an RPC to every host that holds a tablet replica of the given table, asking it to wait
    // until the raft groups for those tablets are started and ready to serve queries.
    // For the local node, waits directly without an RPC.
    future<> wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout);

    tablet_group_leader_cache& leader_cache() { return _leader_cache; }

    std::optional<locator::tablet_routing_info_v2> check_tablet_version(
        const replica::table&,
        const dht::token&,
        const locator::tablet_version_block) const;
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
    begin_mutate_result begin_mutate(abort_source&);

    // Possible results:
    //   ok - this node is the leader, proceed with read_barrier() locally
    //   raft::not_a_leader - this node is not a leader, redirect to the leader
    //   need_wait_for_leader - the leader is unknown, the caller needs to wait and retry
    struct ok {};
    using begin_read_result = std::variant<ok, raft::not_a_leader, need_wait_for_leader>;
    begin_read_result begin_read(abort_source&);
    void advance_leader_timestamp(api::timestamp_type ts);
};

} // namespace service::strong_consistency
