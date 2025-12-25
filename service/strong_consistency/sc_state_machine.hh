/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "service/raft/raft_state_machine.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema_fwd.hh"
#include "locator/tablets.hh"

namespace db {
    class system_keyspace;
}

namespace service {
    struct sc_raft_command {
        frozen_mutation mutation;
    };
    std::unique_ptr<raft_state_machine> make_sc_state_machine(locator::global_tablet_id tablet,
        raft::group_id gid,
        replica::database& db);
}