/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <utility>
#include <seastar/core/shard_id.hh>
#include "schema/schema.hh"
#include "locator/abstract_replication_strategy.hh"
#include "dht/token.hh"

namespace service {
// A helper class for the storage_proxy::cas API.
//
// Represents the correct shard on which cas() must be called.
// The appropriate shard is selected based on the current topology,
// and a pinning an effective_replication_map_ptr is acquired to ensure that the shard
// decision remains valid while cas_shard instance is alive.
class cas_shard {
    locator::effective_replication_map_ptr _erm;
    unsigned _shard;
public:
    cas_shard(const schema& s, dht::token token);
    unsigned shard() const {
        return _shard;
    }
    bool this_shard() const {
        return _shard == this_shard_id();
    }

    decltype(auto) get_erm(this auto&& self) {
        return std::forward_like<decltype(self)>(self._erm);
    }
};
}