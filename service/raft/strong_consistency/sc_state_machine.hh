/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "service/raft/raft_state_machine.hh"
#include "mutation/canonical_mutation.hh"
#include "utils/chunked_vector.hh"
#include "schema/schema_fwd.hh"
#include "locator/tablets.hh"

namespace db {
    class system_keyspace;
}

namespace service {
    struct sc_raft_command {
        utils::chunked_vector<canonical_mutation> mutations;
        utils::UUID prev_state_id;
        utils::UUID new_state_id;
    };
    std::unique_ptr<raft_state_machine> make_sc_state_machine(table_id table_id,
        locator::tablet_id tablet_id,
        raft::group_id gid,
        replica::database& db, 
        db::system_keyspace& sys_ks);
}