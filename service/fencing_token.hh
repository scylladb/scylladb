/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "service/topology_state_machine_fwd.hh"

namespace service {

// This class is used in RPC's signatures to hold the topology_version of the caller.
// The reason why we wrap the topology_version in this class is that we anticipate
// other versions to occur in the future, such as the schema version.
struct fencing_token {
    topology_version_t topology_version{0};
    // topology_version == 0 means the caller is not aware about
    // the fencing or doesn't use it for some reason.
    explicit operator bool() const {
        return topology_version != 0;
    }
};

} // namespace service
