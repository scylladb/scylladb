/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/raft_storage.idl.hh"
#include "idl/uuid.idl.hh"

verb [[with_timeout]] wait_for_raft_groups_to_start(raft::server_id dst_id, table_id table);

// Seals the raft group `parent_gid` on the target replica, i.e. makes it stop accepting writes
// and finish its log, so that the groups replacing it can take over. See raft_resize_state.
// `new_gids` are the groups replacing it: for a split the left and right child in this order,
// for a merge the single group the parents are merged into.
// With `wait_only`, only waits until the group has been sealed on the target replica.
// Returns false if the group was not sealed and the call has to be retried.
verb [[with_timeout]] process_raft_resize(raft::server_id dst_id, table_id table_id, raft::group_id parent_gid, std::vector<raft::group_id> new_gids, bool wait_only) -> bool;
