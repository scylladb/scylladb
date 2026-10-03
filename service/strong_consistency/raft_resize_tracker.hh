/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
#include "raft/raft.hh"
#include "schema/schema_fwd.hh"
#include "service/strong_consistency/state_machine.hh"

namespace db {
class system_keyspace;
}

namespace service::strong_consistency {

// Per-parent state tracking the progress of a strongly consistent tablet resize on this replica:
// which of the parent's resize markers have been applied here.
//
// Created when the replica learns that the parent is being resized - from the tablet metadata, or
// by reloading the markers after a restart - and advanced as the parent applies the markers.
// Dropped by the teardown of the parent's raft server.
struct raft_resize_state {
    // Set once the start_resize marker has been applied on this replica, i.e. once the parent's
    // writes are handed off to its children. Never cleared.
    bool start_resize = false;

    // Set once the end_resize marker has been applied on this replica, i.e. once the parent's
    // log is final and applied here. Never cleared.
    bool end_resize = false;
    // The timestamp end_resize was stamped with, taken from the parent leader's clock, so it is
    // above every write in the parent's log. Set with end_resize.
    api::timestamp_type end_resize_timestamp = api::min_timestamp;

    // The highest timestamp of a marker applied here. The markers' cells carry it, and a deletion
    // of the parent's row has to be stamped above it.
    api::timestamp_type max_marker_timestamp = api::min_timestamp;
};

// Owns the state of every tablet resize the groups hosted on this shard take part in.
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
    // metadata. Creates the state if there is none yet.
    void set_replacement_groups(raft::group_id parent_gid, const utils::small_vector<raft::group_id, 2>& new_gids);

    // Restores which markers the parent has already applied, and the timestamp of end_resize, from
    // its system.raft_groups row, which a replica restarting mid-resize needs. Creates the state if
    // there is none yet.
    future<> restore_applied_markers(raft::group_id parent_gid);

    // Records that the parent `parent_gid` reached the resize phase `marker.kind`, stamped with
    // `marker.timestamp`. A marker of a resize with no state here is ignored rather than creating
    // one: the state is absent only once the resize has ended on this replica, and resurrecting it
    // would leave an entry nothing removes.
    //
    // Called by the applier fiber once the corresponding marker has been applied.
    void mark_resize_phase(raft::group_id parent_gid, const resize_marker& marker);

    // Drops `gid`'s part in its resize: a parent's whole state, or a child's mapping to its parent.
    // Called by the group's teardown, and by groups_manager::update() once a child serves a tablet
    // of its own.
    void erase_group(raft::group_id gid);

    // Returns true if this replica knows that the given group is being resized. False either
    // because the group is not being resized, or because this replica has not learnt of the resize
    // yet. The caller cannot tell the two apart and must treat both as "not ready".
    bool is_resizing(raft::group_id parent_gid) const;

    // Returns the highest timestamp of a marker `parent_gid` has applied here, or api::min_timestamp
    // if it applied none or is not being resized.
    api::timestamp_type max_marker_timestamp(raft::group_id parent_gid) const;

    // Returns the parent of `child_gid`, or nullopt if it is not a child of a resize.
    std::optional<raft::group_id> get_parent_group(raft::group_id child_gid) const;
};

} // namespace service::strong_consistency
