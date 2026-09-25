/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/strong_consistency/truncate_tablet_result.hh"

#include "idl/raft_storage.idl.hh"
#include "idl/storage_service.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/storage_service.idl.hh"

namespace service {
namespace strong_consistency {

struct truncate_tablet_result {
    bool committed;
    std::optional<raft::server_id> leader;
};

} // namespace strong_consistency
} // namespace service

verb [[with_timeout]] wait_for_raft_groups_to_start(raft::server_id dst_id, table_id table);
verb [[cancellable]] wait_for_snapshot_transfer(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id, utils::UUID session_id);
verb [[with_timeout, cancellable]] sync_raft_group_config(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id);
verb [[with_timeout]] truncate_tablet(raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id, utils::UUID request_id, service::frozen_topology_guard guard) -> service::strong_consistency::truncate_tablet_result;
