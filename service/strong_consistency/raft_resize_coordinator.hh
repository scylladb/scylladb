/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/shared_future.hh>
#include "locator/tablets.hh"
#include "raft/raft.hh"
#include "schema/schema_fwd.hh"

namespace db {
class system_keyspace;
}

namespace replica {
class database;
}

class frozen_mutation;


namespace service::strong_consistency {

class groups_manager;
class raft_server;

// Returns true if the mutation targets system.raft_groups_metadata, i.e. it carries the
// progress of a strongly-consistent tablet resize rather than user data.
bool is_resize_mutation(const schema& s);

// Applies a system.raft_groups_metadata mutation.
//
// Unlike the tablet's own data, this table is a regular sharded system table, so its rows live
// on the shard chosen by the table's sharder, whereas a tablet raft group lives on the shard
// owning the tablet. Applying it locally would therefore write to a shard which
// reload_group_resize_state() - a routed CQL read - never looks at. Route the apply to the
// sharder's shard so that both sides agree.
//
// NOTE: db::rp_handle is shard-local and cannot be moved across shards, so the raft commitlog
// handle of the corresponding entry is released by the caller instead of pinning the remote
// memtable. The raft commitlog segment holding the entry may therefore be recycled while the
// row is still unflushed on the target shard. This is a narrow window (two writes per group per
// resize) and does not affect the replay path, which already applies with an empty handle.
// FIXME: make this durable, either by applying through the target shard's commitlog or by
// keeping the segment pinned until the remote memtable is flushed.
future<> apply_resize_mutation(replica::database& db, const frozen_mutation& m, schema_ptr s);

// Per-parent-group state tracking the progress of a strongly-consistent
// tablet split. Persisted in system.raft_groups_metadata and driven by
// applying the redirect_writes/groups_resized mutations to the parent's Raft
// log.
//
// Committing those two markers is what "sealing" the group being resized means: after
// redirect_writes new writes go to the groups replacing it, and after groups_resized its log
// is final. A replica has sealed the group once it applied groups_resized.
//
// The state exists on a replica from the moment it learns that the group is being resized -
// whichever comes first out of observing the resize in the tablet metadata, starting one of the
// groups replacing it, or replaying its log after a restart. Code which already established
// that a group is resizing can therefore take it by reference and never has to handle its
// absence; see raft_resize_coordinator::get_resize_state().
//
// It is dropped once the resize is over, when deleting parent or child group server.
struct raft_resize_state {
    // The groups replacing the one being resized, as announced in the tablet metadata. Fixed
    // for the lifetime of a resize.
    std::vector<raft::group_id> new_gids;

    // Set once the redirect_writes marker has been committed in the parent's log, i.e. once new
    // writes are routed to the group(s) replacing it. Never cleared.
    bool redirect_writes = false;

    // Resolved once the groups_resized mutation has been applied on this replica. The appliers
    // of the new groups block on this promise until it is set, at which point all the entries
    // of the group being replaced are known to be applied and the new groups may start applying
    // their own entries.
    shared_promise<> groups_resized;

    // Signalled whenever the raft state of the group being resized, or of one of the groups
    // replacing it, changes on this replica. Lets leader_colocator() re-check
    // the co-location of their leaders without polling.
    condition_variable leader_changed;

    // Bumped together with every leader_changed broadcast. A waiter which samples it before
    // checking the leaders can tell whether a change it hasn't accounted for happened in the
    // meantime, and therefore must not go to sleep.
    uint64_t leader_change_seq = 0;

    // Background fiber which keeps the leaders of the groups replacing this one co-located with
    // its leader, which is a precondition for the writes redirected to them to be served.
    // Started as soon as the resize is announced in the tablet metadata (see
    // raft_resize_coordinator::leader_colocator()); exits on its own once the resize completes.
    //
    // The fiber outlives neither this state nor the raft servers it drives: it is joined by
    // erase_resize_state(), which groups_manager runs while tearing a group down, so every
    // colocator is gone by the time the coordinator is stopped and the states erased.
    future<> leader_colocator = make_ready_future<>();

    // Aborted to stop leader_colocator. The fiber spends most of its life waiting outside of a
    // raft server, so aborting the servers taking part in the resize doesn't stop it.
    std::optional<abort_source> leader_colocator_as;
};

// Owns everything a replica has to do about the tablet resizes it takes part in: the state of
// each resize (see raft_resize_state), and the background fibers which keep the leaders of the
// groups replacing a resized one co-located with its leader.
//
// A sharded service, one instance per shard, holding the state of the resizes of the groups
// hosted on that shard. It is started before groups_manager and stopped after it, so it can be
// referenced by anything which needs the resize state - the state machines and the commitlog
// replay - without depending on the raft servers being up.
class raft_resize_coordinator {
    std::unordered_map<raft::group_id, raft::group_id> _child_to_parent;
    std::unordered_map<raft::group_id, raft_resize_state> _resize_states;
    db::system_keyspace& _sys_ks;

    // Background fiber of a group which is being replaced by other groups during a tablet
    // resize. It keeps the leaders of the replacing groups co-located with this group's leader,
    // which is a precondition for the writes redirected to them to be served (see
    // colocate_leaders()). Runs until aborted, which happens either when the parent group
    // is deleted (successful resize) or the child group is deleted (resize rolled back).
    future<> leader_colocator(locator::global_tablet_id tablet, raft::group_id parent_gid,
        groups_manager& gm, abort_source& as);

public:
    raft_resize_coordinator(db::system_keyspace& sys_ks)
        : _sys_ks(sys_ks)
    {}

    future<> stop();

    // Records that `parent_gid` is being replaced by `new_gids` on this replica, creating its
    // state if this is the first time we hear about the resize.
    // Must be called before any calling any of the other methods for `parent_gid`,
    // so that they can assume the state exists.
    void announce_resize(locator::global_tablet_id tablet, raft::group_id parent_gid, std::vector<raft::group_id> new_gids, groups_manager& gm);

    // Reloads the state of a resize from system.raft_groups_metadata, which is needed when
    // a replica restarts and the resize is already in progress. The state is created if
    // this is the first time we hear about the resize, but the colocation fiber is not
    // started until the resize is announced in the tablet metadata (see announce_resize()).
    future<> reload_group_resize_state(raft::group_id parent_gid);

    // Drops the state of `parent_gid`, after stopping and joining its colocator. The caller
    // must ensure that the resize is over.
    future<> erase_resize_state(raft::group_id parent_gid);

    // Returns true if this replica knows that the given group is being resized. False either
    // because the group is not being resized, or because this replica hasn't learnt about the
    // resize yet - the caller cannot tell the two apart and must treat both as "not ready".
    bool is_resizing(raft::group_id parent_gid) const;

    // The state of a resize this replica already knows about. It is an internal error to call
    // this for a group which is_resizing() doesn't hold for.
    raft_resize_state& get_resize_state(raft::group_id parent_gid);

    // Used to "apply" the redirect_writes mutation before the applier fiber reaches it.
    // This is safe if the redirect_writes mutation is already committed in the parent's log, 
    // because it will never be rolled back - we apply all committed entries on restart and
    // no other operation can remove the redirect_writes marker from the parent's log.
    void fast_forward_redirect_writes(raft::group_id parent_gid);

    // Returns the parent group of `child_gid`, or nullopt if it is not one of the groups
    // replacing another one.
    std::optional<raft::group_id> get_parent_group(raft::group_id child_gid) const;

    // Returns true once the writes of the given group are redirected to the groups replacing
    // it. False if it is not being resized at all.
    bool should_redirect_writes(raft::group_id parent_gid) const;

    // TODO(merge): a tablet merge has two parents per child, so this will have to return a
    // future which resolves once *all* parents applied groups_resized.
    // Resolves once the parent of the given group has applied groups_resized, or nullopt if
    // the group is not one of the groups replacing another one.
    std::optional<shared_future<>> get_parent_finished_future(raft::group_id child_gid) const;


    // Signals that the raft state of `gid` - which may be either a group being resized or one
    // of the groups replacing it - changed. A no-op if `gid` is not taking part in a resize.
    void notify_leader_change(raft::group_id gid);

    // The outcome of a colocate_leaders() round. Tells the caller how to wait before
    // re-checking.
    enum class colocation_status {
        // Every replacing group is led by the leader of the group being replaced.
        colocated,
        // The leader of the group being replaced is unknown, an election is in progress there.
        parent_leader_unknown,
        // Nothing to do on this replica: an election is in progress in one of the replacing
        // groups, or a diverged one is led by another replica, which is the one that has to
        // hand its leadership over.
        awaiting_leader_change,
        // A leadership transfer was carried out. It only makes the target start an election,
        // which it may lose, so the outcome has to be re-checked.
        transfer_done,
        // A leadership transfer was needed but did not complete.
        transfer_failed,
    };

    // Makes sure that the leader of every group in `new_gids` is co-located with the leader of
    // the group `parent_gid` they are replacing, which is required for writes to be redirected
    // to them (see coordinator::mutate()).
    //
    // If this replica leads one of the new groups and it is not co-located, transfers that
    // group's leadership to the parent leader. Never throws on a failed transfer - the caller
    // is expected to retry.
    future<colocation_status> colocate_leaders(raft_server& parent, raft::group_id parent_gid,
        const std::vector<raft::group_id>& new_gids, groups_manager& gm);
};

} // namespace service::strong_consistency
