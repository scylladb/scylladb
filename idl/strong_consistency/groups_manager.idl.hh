/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/raft_storage.idl.hh"
#include "idl/storage_service.idl.hh"
#include "idl/uuid.idl.hh"

verb [[with_timeout]] wait_for_raft_groups_to_start(raft::server_id dst_id, table_id table);
verb [[cancellable]] wait_for_snapshot_transfer(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id, utils::UUID session_id);
verb [[with_timeout, cancellable]] sync_raft_group_config(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id);


// Seals the raft group `parent_gid` of `tablet` on the target replica, which is being replaced by
// `new_gids` - for a split the left and right child in that order. `session` is the topology
// session of the resize finalization (system.topology `session`), opened by the group0 write which
// records `new_gids` in the tablet map and closed by the one which replaces the map; the receiver
// acts only while it has that session open, i.e. while its own tablet map names the same ids.
// With `wait_only`, only waits. Returns false if the call has to be retried.
verb [[with_timeout]] process_raft_resize(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id parent_gid, std::vector<raft::group_id> new_gids, bool wait_only, service::session_id session) -> bool;
