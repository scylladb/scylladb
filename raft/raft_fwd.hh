/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/internal.hh"

namespace raft {

// This is user provided id for a snapshot
using snapshot_id = internal::tagged_id<struct snapshot_id_tag>;
// Unique identifier of a server in a Raft group
using server_id = internal::tagged_id<struct server_id_tag>;
// Unique identifier of a Raft group
using group_id = raft::internal::tagged_id<struct group_id_tag>;

// This type represents the raft term
using term_t = raft::internal::tagged_uint64<struct term_tag>;
// This type represensts the index into the raft log
using index_t = raft::internal::tagged_uint64<struct index_tag>;
// Identifier for a read barrier request
using read_id = raft::internal::tagged_uint64<struct read_id_tag>;

} // namespace raft
