/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_future.hh>
#include "raft/raft.hh"
#include "schema/schema_fwd.hh"
#include "service/strong_consistency/state_machine.hh"

namespace db {
class system_keyspace;
}

namespace service::strong_consistency {

// Per-parent state tracking the progress of a strongly consistent tablet resize. Rebuilt from the
// markers persisted in the parent's own system.raft_groups row, and advanced as those markers are
// applied from the parent's Raft log. See resize_marker_kind for the terminology.
//
// A replica has sealed the parent once it applied end_resize.
//
// The state exists on a replica from the moment it learns that the parent is being resized -
// whichever comes first out of observing the resize in the tablet metadata, starting one of its
// children, or reloading the markers after a restart. Every entry point which may be the first
// therefore creates the state on demand, see raft_resize_tracker::state_for().
//
// It is dropped once the resize is over, when the raft server of the parent or of one of its
// children is deleted.
struct raft_resize_state {
    // The parent's children, as recorded in the tablet metadata. Empty until
    // set_replacement_groups() runs, which a state created by a marker or by a reload precedes.
    std::vector<raft::group_id> new_gids;

    // Set once the start_resize marker has been applied on this replica, i.e. once the parent's
    // writes are handed off to its children. Never cleared.
    bool start_resize = false;

    // Resolved once the end_resize marker has been applied on this replica. The appliers of the
    // children block on this promise until it is set, at which point every entry the parent
    // committed is known to be applied and the children may start applying their own.
    shared_promise<> end_resize;

};

// Owns the state of every tablet resize the groups hosted on this shard take part in (see
// raft_resize_state).
//
// A sharded service, one instance per shard. It is started before groups_manager and stopped
// after it, so it can be referenced by anything which needs the resize state - the state
// machines and the commitlog replay - without depending on the raft servers being up.
class raft_resize_tracker {
    std::unordered_map<raft::group_id, raft::group_id> _child_to_parent;
    std::unordered_map<raft::group_id, raft_resize_state> _resize_states;
    db::system_keyspace& _sys_ks;

    // The state of `parent_gid`, created if this replica hasn't heard about the resize yet. Any
    // of the entry points below may be the first to learn about it, in any order, so each of them
    // goes through here rather than assuming the state is already there.
    raft_resize_state& state_for(raft::group_id parent_gid);


public:
    raft_resize_tracker(db::system_keyspace& sys_ks)
        : _sys_ks(sys_ks)
    {}

    future<> stop();

    // Records the children which replace `parent_gid` on this replica, as read from the tablet
    // metadata. Creates the state if this is the first time we hear about the resize.
    void set_replacement_groups(raft::group_id parent_gid, std::vector<raft::group_id> new_gids);

    // Restores which markers the parent has already applied from its system.raft_groups row,
    // which is needed when a replica restarts while a resize is in progress. Creates the state if
    // this is the first time we hear about the resize.
    future<> restore_applied_markers(raft::group_id parent_gid);

    // Records that the parent `parent_gid` reached the resize phase `kind`, creating its state if
    // this is the first time we hear about the resize - a replica may learn that a group is
    // being resized by applying a marker before it observes the resize in the tablet metadata.
    //
    // Called by the applier fiber once the corresponding marker has been applied, and by
    // groups_manager to fast-forward start_resize ahead of its own apply. The latter is safe
    // because the marker is already committed in the parent's log by then, so it can never be
    // rolled back: we apply all committed entries on restart and no operation removes a marker
    // from the log.
    void mark_resize_phase(raft::group_id parent_gid, resize_marker_kind kind);

    // Drops the state of `parent_gid`. The caller must ensure that the resize is over.
    void erase_resize_state(raft::group_id parent_gid);

    // Returns true once the parent has been sealed on this replica, i.e. once it applied
    // end_resize. False if the group is not being resized at all.
    bool has_applied_end_resize(raft::group_id parent_gid) const;

    // Returns true if this replica knows that the given group is being resized. False either
    // because the group is not being resized, or because this replica hasn't learnt about the
    // resize yet - the caller cannot tell the two apart and must treat both as "not ready".
    bool is_resizing(raft::group_id parent_gid) const;

    // Returns the parent of `child_gid`, or nullopt if it is not a child of a resize.
    std::optional<raft::group_id> get_parent_group(raft::group_id child_gid) const;

    // Returns true once the parent's writes are handed off to its children. False if the group
    // is not being resized at all.
    bool should_handoff_writes(raft::group_id parent_gid) const;

    // Resolves once the parent has been sealed on this replica. It is an internal error to call
    // this for a group which is_resizing() doesn't hold for.
    future<> wait_for_end_resize(raft::group_id parent_gid, abort_source& as);

    // Resolves once the parent of the given group has applied end_resize, or nullopt if the group
    // is not a child of a resize.
    // FIXME: a tablet merge has two parents per child, so this will have to return a future
    // which resolves once *all* parents applied end_resize.
    std::optional<shared_future<>> get_parent_finished_future(raft::group_id child_gid) const;

};

} // namespace service::strong_consistency
