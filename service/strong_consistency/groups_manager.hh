/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "locator/tablet_metadata_guard.hh"
#include "service/strong_consistency/raft_resize_tracker.hh"
#include "service/topology_guard.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_group_registry.hh"
#include "cql3/query_processor.hh"
#include "db/commitlog/raft_commitlog_replay_buffer.hh"

#include <seastar/util/noncopyable_function.hh>

#include <source_location>

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
class tablet_state_machine;

// What separates a raft group's live configuration from the one its tablet's current
// migration stage implies. Defined in groups_manager.cc.
struct config_sync_work;

/// Thrown by acquire_server() when this replica does not serve the group any more. The group's
/// tablet has left this shard: the table was dropped, the resize was finalized and replaced the
/// tablet with the ones it was split into, or the tablet was migrated away. The request has to be
/// resolved again against the current tablet map, which names the group serving the token now.
/// Retryable: the caller re-enters create_operation_ctx() rather than failing.
struct group_not_served : public std::exception {
    const char* what() const noexcept override {
        return "The raft group is no longer served by this replica";
    }
};

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
    // Held behind a pointer, and only for the duration of a resize. raft_group_state exists for
    // every group on every shard, while a resize concerns a handful at a time. The condition
    // variable, the fiber and the abort source below would otherwise be paid for by every group,
    // resize or no resize.
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

        // Runs until `as` is aborted, which detach_resize_colocation() does as soon as this
        // replica learns that the resize is over. Parked in _draining_colocators then, and
        // joined by stop().
        future<> colocator = make_ready_future<>();

        // The fiber spends most of its life waiting outside of a raft server, so aborting the
        // servers taking part in the resize doesn't stop it.
        abort_source as;
    };

    // A group can be deleted and started again while its entry exists (the
    // tablet leaves the shard and returns before the deletion finishes), so
    // the fields may describe different incarnations of the raft::server:
    //  - `gate` is replaced by every start. A holder only defers the deletion
    //    that closed that very gate, i.e. the destruction of that incarnation.
    //  - `server` is the last started server until its deletion resets it.
    //    Once a restart is queued behind a pending deletion, `gate` is already
    //    the new incarnation's while `server` is still the old one's.
    //  - `server_control_op` chains starts and deletions (chain_control_op()).
    //    If it is available, the last operation was a start: a finished
    //    deletion either erased the entry or has a start queued behind it.
    // So `server` is the live server of the current `gate` iff
    // `server_control_op` is available. Dereference it only via
    // try_acquire_server(), acquire_server(), or from a control operation.
    struct raft_group_state : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
        bool has_tablet = false;
        lw_shared_ptr<gate> gate = nullptr;
        raft::server* server = nullptr;
        // Owned by `server`, valid for as long as it is.
        tablet_state_machine* state_machine = nullptr;
        shared_future<> server_control_op = make_ready_future<>();

        // Populated only when this node thinks it's a tablet raft group leader.
        std::optional<leader_info> leader_info = std::nullopt;
        // A floor under the timestamps a leader of this group on this node hands out, raised by
        // raft_server::advance_leader_timestamp(). It outlives leader_info so that a term whose
        // leader_info is populated after the advance starts above the floor too. The other floor,
        // the end_resize timestamp of the group this one replaces, lives in the resize tracker.
        api::timestamp_type min_leader_timestamp = api::min_timestamp;

        condition_variable leader_info_cond = condition_variable();
        future<> leader_info_updater = make_ready_future<>();

        // At most one raft configuration change attempt is in flight per group.
        // Deliberately kept out of server_control_op: a configuration change must
        // never delay starting or stopping the server, nor be queued behind a
        // deletion that is waiting for it. The attempt never resolves to an
        // exception, so waiters don't have to handle one.
        shared_future<> config_sync = make_ready_future<>();

        // Ends the in-flight configuration change attempt, so that whoever stops
        // waiting for one can also stop it.
        //
        // Owned here rather than by the fiber that scheduled the attempt, and handed to
        // that attempt as a shared pointer, because the attempt outlives the fiber: an
        // abort source must never be destroyed while something is still subscribed to
        // it, and the attempt subscribes for as long as it runs.
        //
        // Replaced by every scheduled attempt, so a fiber that means to end the attempt
        // it was waiting for has to hold on to the pointer it saw rather than read this
        // back later.
        lw_shared_ptr<abort_source> config_sync_as;

        // Set on a parent once its data has been flushed for the seal, so that the rounds of
        // process_raft_resize which follow do not flush again. See handle_process_raft_resize().
        bool seal_flushed = false;

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

    // Colocators detached from their groups (see detach_resize_colocation()), each keeping the
    // state it is bound to alive until its fiber exits. The fibers are aborted when they are
    // detached and exit on their own; the drain is only joined by stop(), so that no fiber
    // outlives the manager.
    future<> _draining_colocators = make_ready_future<>();

    // The lowest timestamp a leader of group `gid`, whose state is `state`, may hand out next on
    // this node: see raft_group_state::min_leader_timestamp and the resize tracker's end_resize
    // timestamp of the group it replaces. Used when a leader's clock is seeded and on every write
    // it stamps.
    static api::timestamp_type leader_timestamp_floor(raft::group_id gid, const raft_group_state& state, const raft_resize_tracker& resize_tracker);

    // Should be called on the shard that hosts the Raft group. Returns the group's state machine,
    // owned by the raft server it started.
    // If the group is created as a result of a resize, the parent id is the group_id
    // of the original tablet.
    future<tablet_state_machine*> start_raft_group(locator::global_tablet_id tablet,
        raft::group_id group_id,
        locator::token_metadata_ptr tm,
        std::optional<raft::group_id> parent_gid = std::nullopt);

    void schedule_raft_group_deletion(raft::group_id group_id, raft_group_state& group_state);

    void schedule_raft_groups_deletion(bool all);

    // Queues a start or deletion of the group's raft::server behind the
    // previous one; at most one control operation runs per group. `op` is
    // kept alive until it completes.
    //
    // Control operations must not fail; a failure aborts the node regardless
    // of abort_on_internal_error, since the chain can't recover: a failed
    // start can't be retried (the first attempt consumes the raft log entries
    // replayed from the commitlog) and can't be left in place (every later
    // operation would inherit the failure). Readers rely on this: an
    // available `server_control_op` means "started".
    static void chain_control_op(raft_group_state& state, raft::group_id id,
            noncopyable_function<future<>()> op,
            std::source_location loc = std::source_location::current());

    // Handle to the group's server, or nullopt if the group is being deleted
    // or (re)started. Unlike acquire_server(), doesn't wait for a start.
    std::optional<raft_server> try_acquire_server(raft::group_id gid, raft_group_state& state);

    future<> leader_info_updater(raft_group_state& state, table_id table, raft::group_id gid,
        dht::token token);

    // The outcome of a colocate_leaders() round. Tells the caller how to wait before re-checking.
    enum class colocation_status {
        // Every child is led by the leader of its parent.
        colocated,
        // Nothing to do on this replica: an election is in progress in one of the children, or a
        // diverged child is led by another replica. That replica is the one which has to hand the
        // leadership over.
        awaiting_leader_change,
        // A leadership transfer was carried out. It only makes the target start an election,
        // which it may lose, so the outcome has to be re-checked.
        transfer_done,
        // A leadership transfer was needed but did not complete.
        transfer_failed,
    };

    // Makes sure that the leader of every group in `new_gids` is `parent_leader`, the leader of
    // their parent `parent_gid`. Writes are handed off to a child only once that holds.
    //
    // If this replica leads one of the children and it is not co-located, transfers that group's
    // leadership to the parent's leader. Never throws on a failed transfer - the caller retries.
    //
    // Takes the parent's leader rather than the parent itself, and holds no group for longer than
    // it operates on it. A group's deletion waits for such holders, and this function can wait out
    // a leadership transfer; the deletion it would block is the one which ends the resize it runs
    // for. The leader is a snapshot, which is why the caller re-checks.
    future<colocation_status> colocate_leaders(raft::server_id parent_leader, raft::group_id parent_gid,
        const std::vector<raft::group_id>& new_gids);

    // Background fiber of a parent being replaced by its children during a tablet resize. It
    // keeps the children's leaders co-located with the parent's, which is a precondition for
    // requests redirected to them to be served. Runs until the resize is over on this replica.
    future<> leader_colocator(resize_colocation_state& colocation, raft::group_id parent_gid);

    // Starts the colocator of `parent_gid` unless it is already running. A still-installed
    // colocator of an earlier resize of the same parent is detached first.
    void start_leader_colocator(raft_group_state& state, locator::global_tablet_id tablet,
        raft::group_id parent_gid, std::vector<raft::group_id> new_gids);

    // Detaches the group's colocation state and aborts its fiber, parking both in
    // _draining_colocators. Called as soon as this replica learns that the resize the colocator was
    // serving is over. Never waits for the fiber. Until it exits, the raft servers it may still
    // touch are protected by the gate holders it acquires per access, which the gate drains of
    // their deletions wait out. state.resize_colocation must be set.
    void detach_resize_colocation(raft_group_state& state);

    // Signals that the raft state of `gid` - which may be either a parent being resized or one of
    // its children - changed. A no-op if `gid` is not taking part in a resize.
    void notify_leader_change(raft::group_id gid);

    void init_messaging_service();
    future<> uninit_messaging_service();

    // Schedules a single attempt to move the group's raft configuration to the one
    // the tablet's current migration stage implies, unless an attempt is already in
    // flight or this node can't drive one.
    //
    // Cheap, synchronous and best-effort: a skipped attempt is never a correctness
    // problem, because converge_group_config() both observes whether the group
    // converged and reschedules until its deadline.
    void maybe_schedule_config_sync(raft_group_state& state, locator::global_tablet_id tablet,
        raft::group_id group_id, const locator::tablet_map& tmap);

    // Performs the configuration change scheduled by maybe_schedule_config_sync().
    // Runs only on the group leader and is internally bounded; errors are logged and
    // never propagated.
    //
    // `as` is the attempt's own abort source, kept alive by this coroutine's frame for
    // as long as the attempt runs. Ending it is how the fiber that scheduled the
    // attempt stops one it doesn't want anymore.
    future<> run_config_sync(raft_group_state& state, locator::global_tablet_id tablet,
        raft::group_id group_id, config_sync_work work, gate::holder holder,
        lw_shared_ptr<abort_source> as);

    // Drives one raft group's configuration to the one the tablet's current migration
    // stage implies and doesn't return until it got there. On behalf of
    // sync_raft_group_config(); see there for what a failure means.
    //
    // The tablet's stage and replica set are read from `guard` on every pass, so the
    // configuration this drives towards is always the one the coordinator currently
    // wants. Waiting for an attempt is done under `as`, which the guard aborts as soon
    // as the stage moves, and giving up on an attempt aborts that attempt too - so no
    // proposal is left running towards a configuration this stopped wanting.
    future<> converge_group_config(locator::global_tablet_id tablet, raft::group_id group_id,
        locator::tablet_metadata_guard& guard, lowres_clock::time_point deadline, abort_source& as);

    // Waits until the raft server of a group this node is no longer a member of is torn
    // down. On behalf of local_topology_barrier(); see there for what a failure means.
    future<> drain_group_deletion(locator::global_tablet_id tablet, raft::group_id group_id,
        lowres_clock::time_point deadline);

    // Enters the topology session the finalization resizing `parent_gid` runs under, or returns nullopt if this
    // shard does not have it: the token metadata change which opens it has not been applied here
    // yet, or the one which closes it has. Either way the caller cannot act on the resize here.
    std::optional<service::topology_guard> try_enter_resize_session(service::session_id session, raft::group_id parent_gid) const;

    // Returns the shard hosting the raft server of the given tablet. Nullopt if the table is gone.
    // The caller must hold the resize session of the tablet map it took `tablet` and
    // `expected_gid` from; a map here which disagrees with them is then an internal error.
    std::optional<shard_id> find_shard_for_tablet(locator::global_tablet_id tablet, raft::group_id expected_gid) const;

    // try_acquire_server() for a group named by id: nullopt also if the group is not hosted here.
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
    //
    // group_for_handoff() returns nullopt if the current tablet map no longer shows the tablet
    // resizing. It can answer that even right after should_handoff_writes() answered yes, and the
    // caller must then retry against the current map rather than hand the request anywhere.
    bool should_handoff_writes(raft::group_id group_id) const;
    std::optional<raft::group_id> group_for_handoff(schema_ptr schema, const dht::token& token) const;

    // Seals the raft group `parent_gid` of `tablet`, which is being replaced by the groups
    // `new_gids`.
    // Returns true once start_resize and end_resize have been committed in the parent group. With
    // wait_only, returns true once end_resize has been applied on this replica and the tablet's
    // data has been flushed.
    // Returns false if the call has to be retried, which covers every case where this replica
    // cannot make progress yet. It has not observed the resize (so does not have `session`), does
    // not host the groups involved, does not lead the parent, or the leaders are not co-located.
    future<bool> handle_process_raft_resize(locator::global_tablet_id tablet, raft::group_id parent_gid,
        const std::vector<raft::group_id>& new_gids, bool wait_only, service::session_id session, abort_source& as);

    // Called during node boot. Starts all raft::server instances corresponding
    // to the latest group0 state in the background.
    void start();

    // Called during node shutdown. Waits for all raft::server instances to stop.
    future<> stop();

    // Hands Raft leadership over to another replica for every strongly consistent
    // tablet group hosted on this shard which this node currently leads.
    //
    // Transferring leadership takes a round of Raft RPCs with the other replicas,
    // so it must run while messaging_service is still up. stop() runs late in the
    // shutdown sequence, after storage_service::do_drain() has already shut the
    // transport down, so we need a separate entry point to trigger stepdown().
    future<> stepdown_leaders();

    future<> wait_for_groups_to_start(lowres_clock::time_point timeout);

    future<> wait_for_snapshot_transfer(locator::global_tablet_id tablet, raft::group_id group_id, service::session_id session_id);

    // Is a raft server for the group running on this shard? Note that a group being
    // deleted counts as running until its raft server is destroyed.
    bool is_group_running(raft::group_id group_id) const;

    // Deletes everything this shard persists about the group, so that a tablet migrated
    // back here later rejoins the group with no history of its previous membership.
    //
    // Called by tablet cleanup, which is the point at which a replica has definitively
    // left: the raft server is already gone by then, and the tablet's storage is about
    // to be. Refuses while a raft server for the group is still running, because that
    // would pull the state out from under a live group.
    future<> erase_raft_group_state(raft::group_id group_id);

    // Drives the raft group of one tablet in transition to the configuration its
    // current migration stage implies, and doesn't return until it got there.
    //
    // This is the only place where a configuration mismatch becomes an error, and it
    // is per tablet: the topology coordinator receives it as a failed background
    // action of that one tablet, and the stage logic decides what to do about it -
    // retry, exclude a replica, or roll back. No other tablet's migration and no node
    // operation is held up by a group that can't converge.
    //
    // Called through the sync_raft_group_config RPC, whose stale invocations are fenced
    // the same way streaming's are: the stage that asked for the change is checked
    // against live tablet metadata here, the configuration to drive towards is derived
    // from that metadata on every pass rather than from the request, and the barrier of
    // the next stage waits for the tablet_metadata_guard this holds, which aborts it as
    // soon as the stage moves on.
    //
    // The guard bounds this call, not the raft proposal it schedules: that runs in the
    // background holding the group's gate. Ending the wait for one also aborts it, and
    // what keeps an aborted proposal from landing a configuration of an abandoned stage
    // is spelled out at the end of sync_raft_group_config().
    //
    // Called on every shard; a shard that runs no raft server for the group at the
    // tablet's current stage returns immediately.
    future<> sync_raft_group_config(locator::global_tablet_id tablet, raft::group_id group_id,
        lowres_clock::time_point deadline);

    // Makes sure the raft server of a group whose membership the tablet's current
    // migration stage ended is torn down - before the tablet cleanup of the same
    // migration removes the tablet's storage on this node.
    //
    // This is all the topology barrier does for strongly consistent tablets. The wait
    // is local and short: the deletion was scheduled when the stage was published,
    // which this barrier already synchronized with. Establishing a stage's raft
    // configuration is not part of it - that is sync_raft_group_config()'s job, driven
    // per tablet by the coordinator.
    //
    // Bounded by `as` as well, so that a barrier nobody is waiting for anymore - the
    // node is shutting down, or the topology command was superseded - returns instead
    // of waiting out its deadline.
    future<> local_topology_barrier(locator::token_metadata_ptr tm, abort_source& as);

    // Sends an RPC to every host that holds a tablet replica of the given table, asking it to wait
    // until the raft groups for those tablets are started and ready to serve queries.
    // For the local node, waits directly without an RPC.
    future<> wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout);

    tablet_group_leader_cache& leader_cache() { return _leader_cache; }

    std::optional<locator::tablet_routing_info_v2> check_tablet_version(
        const replica::table&,
        const dht::token&,
        const locator::tablet_version_block);
};

/// A temporary, RAII-style handle to an active Raft group server instance,
/// used to safely submit commands or perform consistency barriers.
///
/// Holds the gate of the server's incarnation: its deletion waits for the
/// handle before destroying the raft::server. The server may still be aborted
/// meanwhile; operations then fail with raft::stopped_error.
///
/// Obtain via groups_manager::acquire_server() or try_acquire_server().
class raft_server {
private:
    groups_manager::raft_group_state& _state;
    gate::holder _holder;
    raft::group_id _gid;
    raft_resize_tracker& _resize_tracker;

public:
    raft_server(raft::group_id gid, groups_manager::raft_group_state& state, gate::holder holder, raft_resize_tracker& resize_tracker);

    raft::server& server() const {
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
