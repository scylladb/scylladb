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
#include "schema/schema_fwd.hh"
#include "utils/small_vector.hh"
#include "utils/tagged_integer.hh"
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include "dht/token.hh"
#include <utility>
#include <vector>

namespace locator {

class tablet_map;

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

// Full definition in locator/tablets.hh.
struct tablet_routing_info_v2;

/// A 64-bit hash computed from a tablet's replica list after ordering it.
/// Used by TABLETS_ROUTING_V2 to detect when a driver's cached routing is stale.
///
/// Tablet version is computed differently for eventually-consistent and
/// strongly-consistent tablets. For more details, see: docs/dev/tablets-routing-v2.md.
using tablet_version = utils::tagged_integer<struct tablet_version_tag, uint64_t>;

// Kept in sync with locator/topology.hh's own definition (a plain alias
// redeclaration with an identical underlying type is not an ODR violation).
// Duplicated here so headers using it in declaration-only signatures (e.g.
// locator/tablets.hh's get_allowed_racks()) don't need the full locator/topology.hh.
using rack_list = std::vector<seastar::sstring>;

// Identifies tablet within the scope of a single tablet_map,
// which has a scope of (table_id, token metadata version).
// Different tablets of different tables can have the same tablet_id.
// Different tablets in subsequent token metadata version can have the same tablet_id.
// When splitting a tablet, one of the new tablets (in the new token metadata version)
// will have the same tablet_id as the old one.
struct tablet_id {
    size_t id;
    explicit tablet_id(size_t id) : id(id) {}
    size_t value() const { return id; }
    explicit operator size_t() const { return id; }
    auto operator<=>(const tablet_id&) const = default;
};

/// Identifies tablet (not be confused with tablet replica) in the scope of the whole cluster.
struct global_tablet_id {
    table_id table;
    tablet_id tablet;

    auto operator<=>(const global_tablet_id&) const = default;
};

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

enum class tablet_task_type {
    none,
    user_repair,
    auto_repair,
    migration,
    intranode_migration,
    split,
    merge
};

seastar::sstring tablet_task_type_to_string(tablet_task_type);
tablet_task_type tablet_task_type_from_string(const seastar::sstring&);

// - incremental (incremental repair): The incremental repair logic is enabled.
//   Unrepaired sstables will be included for repair. Repaired sstables will be
//   skipped. The incremental repair states will be updated after repair.

// - full (full repair): The incremental repair logic is enabled.
//   Both repaired and unrepaired sstables will be included for repair. The
//   incremental repair states will be updated after repair.

// - disabled (non incremental repair): The incremental repair logic is disabled
//   completely. The incremental repair states, e.g., repaired_at in sstables and
//   sstables_repaired_at in system.tablets table, will not be updated after
//   repair.
enum class tablet_repair_incremental_mode : uint8_t {
    incremental,
    full,
    disabled,
};

constexpr tablet_repair_incremental_mode default_tablet_repair_incremental_mode{tablet_repair_incremental_mode::incremental};

seastar::sstring tablet_repair_incremental_mode_to_string(tablet_repair_incremental_mode);
tablet_repair_incremental_mode tablet_repair_incremental_mode_from_string(const seastar::sstring&);

} // namespace locator
