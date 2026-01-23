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
    frozen_mutation mutation;
};

} // namespace strong_consistency
} // namespace service