/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "service/raft/raft_state_machine.hh"
#include "mutation/frozen_mutation.hh"
#include "locator/tablets.hh"

namespace service::strong_consistency {

struct raft_command {
    utils::chunked_vector<frozen_mutation> mutations;
};
std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db);

}