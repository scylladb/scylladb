/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
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
// The state exists on a replica from the moment it learns that the parent is being resized. It
// learns either by observing the resize in the tablet metadata, or by reloading the markers after
// a restart. Both create the state on demand; applying a marker does not. The finalization publishes
// the metadata in a barrier of its own before committing any marker, so update() has always
// recorded the resize by the time a marker arrives. A marker for a group with no state here is
// therefore one left over from a resize which is already over.
//
// It is dropped by the teardown of the parent's raft server, which the tablet map replacement
// ending the resize brings about. A child's own mapping goes earlier, dropped by
// groups_manager::update() once the group serves a tablet of its own.
struct raft_resize_state {
    // Set once the start_resize marker has been applied on this replica, i.e. once the parent's
    // writes are handed off to its children. Never cleared.
    bool start_resize = false;

    // Resolved once the end_resize marker has been applied on this replica.
    shared_promise<> end_resize;
};

// Owns the state of every tablet resize the groups hosted on this shard take part in (see
// raft_resize_state).
//
// A sharded service, one instance per shard, started before groups_manager and stopped after it.
// Anything needing the resize state - the state machines, the commitlog replay - can therefore
// reach it without depending on the raft servers being up.
class raft_resize_tracker {
    // Maps every child recorded on this replica to its parent. Each entry is owned by the child
    // group alone. It is dropped by the child's own teardown, or by update() observing that the
    // group now serves a tablet of its own, i.e. that the resize was finalized. Erasing a parent's
    // state leaves the mappings of its children to their owners.
    std::unordered_map<raft::group_id, raft::group_id> _child_to_parent;
    std::unordered_map<raft::group_id, raft_resize_state> _resize_states;
    db::system_keyspace& _sys_ks;

    void erase_resize_state(raft::group_id parent_gid);

public:
    raft_resize_tracker(db::system_keyspace& sys_ks)
        : _sys_ks(sys_ks)
    {}

    future<> stop();

    // Records the children which replace `parent_gid` on this replica, as read from the tablet
    // metadata. Creates the state if this is the first time we hear about the resize.
    void set_replacement_groups(raft::group_id parent_gid, const utils::small_vector<raft::group_id, 2>& new_gids);

    // Restores which markers the parent has already applied from its system.raft_groups row,
    // which a replica restarting mid-resize needs. Creates the state if this is the first we hear
    // of the resize. It is, for the commitlog replay: that runs before the tablet metadata records
    // the resize at all.
    future<> restore_applied_markers(raft::group_id parent_gid);

    // Records that the parent `parent_gid` reached the resize phase `kind`. A marker of a resize
    // with no state here is ignored rather than creating one. The state is absent only once the
    // resize has ended on this replica, and resurrecting it would leave an entry nothing removes.
    //
    // Called by the applier fiber once the corresponding marker has been applied.
    void mark_resize_phase(raft::group_id parent_gid, resize_marker_kind kind);

    // Drops the record that `gid` - a parent, or a child of one - has in the resize it took part
    // in: the parent's whole state, or just the child's own mapping.
    // Called by groups_manager while the group's raft server is being torn down, and from update()
    // for the one ending no teardown covers - a finalization, which the children's mappings must
    // not outlive.
    void erase_group(raft::group_id gid);

    // Returns true if this replica knows that the given group is being resized. False either
    // because the group is not being resized, or because this replica has not learnt of the resize
    // yet. The caller cannot tell the two apart and must treat both as "not ready".
    bool is_resizing(raft::group_id parent_gid) const;

    // Returns the parent of `child_gid`, or nullopt if it is not a child of a resize.
    std::optional<raft::group_id> get_parent_group(raft::group_id child_gid) const;

    // Returns true once the parent's writes are handed off to its children. False if the group
    // is not being resized at all.
    bool should_handoff_writes(raft::group_id parent_gid) const;

    // Resolves once the parent of the given group has applied end_resize, or with
    // abort_requested_exception if the parent is torn down first.
    // A waiter needs no abort source of its own. A parent is always torn down together with its
    // children or after them, so the parent's teardown ends the wait of a child being torn down.
    // Nullopt if the group is not a child of a resize, and also if the parent's state is already
    // gone, which means the resize is over on this replica and nothing is left to wait for.
    // FIXME: a tablet merge has two parents per child, so this will have to return a future
    // which resolves once *all* parents applied end_resize.
    std::optional<shared_future<>> get_parent_finished_future(raft::group_id child_gid) const;
};

} // namespace service::strong_consistency
