/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Lightweight header exposing only the small tablet types (such as
// locator::tablet_replica, locator::tablet_replica_set and the tablet
// routing info) needed in function signatures across widely-included
// headers — without pulling in the heavy locator/tablets.hh (which
// includes seastar/core/reactor.hh, raft/raft.hh, etc.)

#include "locator/host_id.hh"
#include "utils/small_vector.hh"
#include "utils/tagged_integer.hh"
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

/// A 64-bit hash computed from a tablet's replica list after ordering it.
/// Used by TABLETS_ROUTING_V2 to detect when a driver's cached routing is stale.
///
/// Tablet version is computed differently for eventually-consistent and
/// strongly-consistent tablets. For more details, see: docs/dev/tablets-routing-v2.md.
using tablet_version = utils::tagged_integer<struct tablet_version_tag, uint64_t>;

/// A single-byte encoding of one 4-bit block of a tablet_version.
/// The high nibble is the block index (0-15), the low nibble is the block's value.
/// Used in EXECUTE requests to minimize network usage.
///
/// Blocks are indexed from the least significant bits to the most significant ones.
/// For more details, see: docs/dev/tablets-routing-v2.md.
using tablet_version_block = utils::tagged_integer<struct tablet_version_block_tag, uint8_t>;

inline bool compare_tablet_version_block(tablet_version hash, tablet_version_block block) noexcept {
    uint64_t hash_value = hash.value();

    // Extract the value of the block.
    const uint8_t block_value = block.value() & 0x0F;

    // Extract the index of the block.
    const uint8_t block_index = (block.value() & 0xF0) >> 4;

    // Shift the hash so that the block corresponds to the 4 least significant bits.
    hash_value >>= (block_index * 4);

    // Extract the block value.
    const uint8_t hash_block = static_cast<uint8_t>(hash_value & 0x0F);

    return hash_block == block_value;
}

struct tablet_routing_info {
    tablet_replica_set tablet_replicas;
    std::pair<dht::token, dht::token> token_range;
};

struct tablet_routing_info_v2 {
    tablet_replica_set tablet_replicas;
    std::pair<dht::token, dht::token> token_range;
    tablet_version hash;
};

} // namespace locator
