/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iostream>

namespace service {

enum class group0_upgrade_state : uint8_t {
    // In recovery state group 0 is disabled.
    // The node does not attempt any Raft/group 0 related operations, including upgrade.
    recovery = 0,

    // In `use_pre_raft_procedures` state the node performs schema changes without using group 0,
    // using the 'old ways': by updating its local schema tables and attempting to push the change to others
    // through direct RPC. If necessary, changes are later propagated with gossip.
    use_pre_raft_procedures = 1,

    // In `synchronize` state the node rejects any attempts for changing the schema.
    // Schema changes may still arrive from other nodes for some time. However, if no failures occur
    // during the upgrade procedure, eventually all nodes should enter `synchronize` state. Then
    // the nodes ensure that schema is synchronized across the entire cluster before entering `use_post_raft_procedures`.
    synchronize = 2,

    // In `use_post_raft_procedures` state the upgrade is finished. The node performs schema changes
    // using group 0, i.e. by constructing appropriate Raft commands and sending them to the Raft group 0 cluster.
    use_post_raft_procedures = 3,
};

inline constexpr uint8_t group0_upgrade_state_last = 3;

std::ostream& operator<<(std::ostream&, group0_upgrade_state);

} // namespace service
