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

namespace service {
    std::unique_ptr<raft_state_machine> make_sc_state_machine();
}