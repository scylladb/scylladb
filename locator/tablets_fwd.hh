/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Lightweight header exposing only locator::tablet_replica and
// locator::tablet_replica_set — the two types needed in function
// signatures across widely-included headers — without pulling in the
// heavy locator/tablets.hh (which includes seastar/core/reactor.hh,
// raft/raft.hh, etc.)

#include "locator/host_id.hh"
#include "utils/small_vector.hh"
#include <seastar/core/shard_id.hh>
#include "dht/token.hh"
#include <utility>

namespace locator {

struct tablet_replica {
    host_id host;
    seastar::shard_id shard;

    auto operator<=>(const tablet_replica&) const = default;
};

using tablet_replica_set = utils::small_vector<tablet_replica, 3>;

struct tablet_routing_info {
    tablet_replica_set tablet_replicas;
    std::pair<dht::token, dht::token> token_range;
};

} // namespace locator
