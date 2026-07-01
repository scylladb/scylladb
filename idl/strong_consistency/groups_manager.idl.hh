/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/raft_storage.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/storage_service.idl.hh"

verb [[with_timeout]] wait_for_raft_groups_to_start(raft::server_id dst_id, table_id table);
verb [[cancellable]] get_local_applied_timestamps(raft::server_id dst_id, service::fencing_token fence, std::vector<locator::global_tablet_id> tablets) -> std::unordered_map<locator::global_tablet_id, api::timestamp_type>;
