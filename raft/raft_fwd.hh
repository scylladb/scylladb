/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/internal.hh"

namespace raft {

using server_id = internal::tagged_id<struct server_id_tag>;
using group_id = raft::internal::tagged_id<struct group_id_tag>;

} // namespace raft
