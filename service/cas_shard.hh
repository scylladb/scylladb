/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "locator/tablet_metadata_guard.hh"

namespace service {
// A helper class for the storage_proxy::cas API.
//
// Represents the correct shard on which cas() must be called.
// The appropriate shard is selected based on the current topology,
// and a token_metadata_guard is acquired to ensure that the shard
// decision remains valid while cas_shard instance is alive.
class cas_shard {
    locator::token_metadata_guard _token_guard;
    unsigned _shard;
public:
    cas_shard(const schema& s, dht::token token);
    unsigned shard() const {
        return _shard;
    }
    bool this_shard() const {
        return _shard == this_shard_id();
    }
    locator::token_metadata_guard token_guard() && {
        return std::move(_token_guard);
    }
};
}