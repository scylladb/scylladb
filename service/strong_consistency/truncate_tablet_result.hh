/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "raft/raft.hh"

#include <optional>

namespace service::strong_consistency {

// Answer of the truncate_tablet verb. `leader` is set when the replica does not lead the group
// and knows who does.
struct truncate_tablet_result {
    bool committed = false;
    std::optional<raft::server_id> leader;
};

} // namespace service::strong_consistency
