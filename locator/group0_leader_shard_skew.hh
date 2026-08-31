/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "locator/host_id.hh"

#include <optional>

namespace locator {

// Deweights shard 0 of the group0 leader, see apply_group0_leader_shard_ratio().
struct group0_leader_shard_skew {
    std::optional<host_id> leader; // unknown leader: no skew
    unsigned ratio = 100;

    unsigned ratio_for(host_id host) const {
        return leader == host ? ratio : 100u;
    }
};

}
