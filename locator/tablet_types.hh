/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Lightweight header with the small tablet types (such as tablet_id,
// global_tablet_id, tablet_replica and the tablet routing info) and
// forward declarations of tablet_map and tablet_metadata. Widely-included
// headers should include this instead of the heavy locator/tablets.hh,
// which every change to the tablet implementation touches.

#include "locator/host_id.hh"
#include "schema/schema_fwd.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "utils/small_vector.hh"
#include "utils/tagged_integer.hh"
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"
#include "dht/token.hh"
#include <fmt/core.h>
#include <utility>

namespace locator {

using tablet_task_id = utils::tagged_uuid<struct tablet_task_id_tag>;

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

class tablet_map;
class tablet_metadata;
struct combined_load_stats;
struct rf_rack_topology_operation;

enum class tablet_task_type {
    none,
    user_repair,
    auto_repair,
    migration,
    intranode_migration,
    split,
    merge
};

sstring tablet_task_type_to_string(tablet_task_type);
tablet_task_type tablet_task_type_from_string(const sstring&);


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

sstring tablet_repair_incremental_mode_to_string(tablet_repair_incremental_mode);
tablet_repair_incremental_mode tablet_repair_incremental_mode_from_string(const sstring&);

// Describes if a given token is located at either left or right side of a tablet's range
enum tablet_range_side {
    left = 0,
    right = 1,
};

} // namespace locator

namespace std {

template<>
struct hash<locator::tablet_id> {
    size_t operator()(const locator::tablet_id& id) const {
        return std::hash<size_t>()(id.value());
    }
};

template<>
struct hash<locator::tablet_replica> {
    size_t operator()(const locator::tablet_replica& r) const {
        return utils::hash_combine(
                std::hash<locator::host_id>()(r.host),
                std::hash<shard_id>()(r.shard));
    }
};

template<>
struct hash<locator::global_tablet_id> {
    size_t operator()(const locator::global_tablet_id& id) const {
        return utils::hash_combine(
                std::hash<table_id>()(id.table),
                std::hash<locator::tablet_id>()(id.tablet));
    }
};

} // namespace std

template <>
struct fmt::formatter<locator::global_tablet_id> : fmt::formatter<string_view> {
    auto format(const locator::global_tablet_id&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_id> : fmt::formatter<string_view> {
    auto format(locator::tablet_id  id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.value());
    }
};

template <>
struct fmt::formatter<locator::tablet_replica> : fmt::formatter<string_view> {
    auto format(const locator::tablet_replica& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}:{}", r.host, r.shard);
    }
};

template <>
struct fmt::formatter<locator::tablet_task_type> : fmt::formatter<string_view> {
    auto format(const locator::tablet_task_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_repair_incremental_mode> : fmt::formatter<string_view> {
    auto format(const locator::tablet_repair_incremental_mode&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
