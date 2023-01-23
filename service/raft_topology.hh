/*
 *
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <stdint.h>
#include <iostream>

// This a temporary header for raft-topology related declarations,
// it will be merged into a more appropriate header when the main
// raft-topology patch is merged.

namespace service {

struct fencing_token {
    int64_t topology_version;
    bool allow_previous;
};

inline std::ostream& operator<<(std::ostream& os, const fencing_token& fencing_token) {
    return os << "fencing_token(" << fencing_token.topology_version << ',' << fencing_token.allow_previous << ")";
}

}
