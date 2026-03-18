/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "raft/raft.hh"

namespace raft {

verb [[with_client_info, with_timeout]] raft_read_barrier (raft::group_id, raft::server_id from_id, raft::server_id dst_id);

} // namespace raft
