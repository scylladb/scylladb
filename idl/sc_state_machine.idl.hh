/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/chunked_vector.hh"

#include "idl/frozen_schema.idl.hh"
#include "idl/uuid.idl.hh"

namespace service {
struct sc_raft_command {
    utils::chunked_vector<canonical_mutation> mutations;
    utils::UUID prev_state_id;
    utils::UUID new_state_id;
};

} // namespace service
