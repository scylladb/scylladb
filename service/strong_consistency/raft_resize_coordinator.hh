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

namespace db {
class system_keyspace;
}

namespace replica {
class database;
}

class frozen_mutation;


namespace service::strong_consistency {

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

// Per-parent-group state tracking the progress of a strongly-consistent tablet resize.
// Persisted in system.raft_groups_metadata and driven by applying the resize phase markers to
// the parent's Raft log.
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
};

// Owns the state of each tablet resize a replica takes part in (see raft_resize_state).
//
// A sharded service, one instance per shard, holding the state of the resizes of the groups
// hosted on that shard. It is started before groups_manager and stopped after it, so it can be
// referenced by anything which needs the resize state - the state machines and the commitlog
// replay - without depending on the raft servers being up.
class raft_resize_coordinator {
    std::unordered_map<raft::group_id, raft::group_id> _child_to_parent;
    std::unordered_map<raft::group_id, raft_resize_state> _resize_states;
    db::system_keyspace& _sys_ks;

public:
    raft_resize_coordinator(db::system_keyspace& sys_ks)
        : _sys_ks(sys_ks)
    {}

    future<> stop();

    // Records that `parent_gid` is being replaced by `new_gids` on this replica, creating its
    // state if this is the first time we hear about the resize.
    // Must be called before any calling any of the other methods for `parent_gid`,
    // so that they can assume the state exists.
    void announce_resize(raft::group_id parent_gid, std::vector<raft::group_id> new_gids);

    // Reloads the state of a resize from system.raft_groups_metadata, which is needed when
    // a replica restarts and the resize is already in progress. The state is created if
    // this is the first time we hear about the resize.
    future<> reload_group_resize_state(raft::group_id parent_gid);

    // Drops the state of `parent_gid`. The caller must ensure that the resize is over.
    void erase_resize_state(raft::group_id parent_gid);

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
};

} // namespace service::strong_consistency
