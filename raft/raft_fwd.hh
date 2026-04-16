/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

// Lightweight forward-declaration header for commonly used raft types.
// Include this instead of raft/raft.hh when only the basic ID/index types
// are needed (e.g. in other header files), to avoid pulling in the full
// raft machinery (futures, abort_source, bytes_ostream, etc.).

#include "internal.hh"

namespace raft {

using server_id = internal::tagged_id<struct server_id_tag>;
using group_id = internal::tagged_id<struct group_id_tag>;
using term_t = internal::tagged_uint64<struct term_tag>;
using index_t = internal::tagged_uint64<struct index_tag>;
using read_id = internal::tagged_uint64<struct read_id_tag>;

class server;

} // namespace raft
