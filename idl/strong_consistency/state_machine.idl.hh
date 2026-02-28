/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/frozen_mutation.idl.hh"
#include "idl/uuid.idl.hh"

namespace service {
namespace strong_consistency {

struct raft_command {
    utils::chunked_vector<frozen_mutation> mutations;
};

} // namespace strong_consistency
} // namespace service