/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/raft_storage.idl.hh"
#include "idl/uuid.idl.hh"

verb [[with_timeout]] wait_for_raft_groups_to_start(raft::server_id dst_id, table_id table);
verb [[cancellable]] wait_for_snapshot_transfer(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id, utils::UUID session_id);
