/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <unordered_map>
#include "bytes.hh"
#include "utils/managed_bytes.hh"
#include "schema/schema_fwd.hh"
#include "utils/hash.hh"
#include "utils/updateable_value.hh"
#include "utils/small_vector.hh"

class schema;
class rows_entry;
class column_definition;
class atomic_cell_or_collection;

namespace db {

// Memtable-level guardrail cache keys.
//
// Each key has two flavours:
//   *_key       — owns its clustering key as a (possibly fragmented)
//                 managed_bytes, stored in the cache.
//   *_key_view  — non-owning views into the keys of the mutation being
//                 checked, used for heterogeneous, allocation-free lookups.
//
// Hashing and equality are defined once, on the view types, in terms of their
// fields; the owning keys expose view() so the shared transparent functors
// (cache_key_hash / cache_key_eq) treat an owning key and a view with the same
// content as equal.
//
// IMPORTANT (allocator safety): these caches are plain std::unordered_maps that
// outlive the LSA allocator context in which they are populated (on_row_merged()
// runs inside the memtable's allocating_section) and are cleared in
// on_flush()/rebuild() under the standard allocator.  managed_bytes picks its
// allocator from current_allocator(), so every construction and destruction of
// an *owning* key (inserts and clears) MUST happen under
// with_allocator(standard_allocator(), ...); otherwise an LSA-allocated key
// would be freed under the wrong allocator and corrupt memory.  Lookups use the
// non-owning views and so neither copy nor allocate.

struct row_key_view {
    bytes_view pk;
    managed_bytes_view ck;
    bool operator==(const row_key_view&) const noexcept = default;
    size_t hash() const noexcept {
        return utils::hash_combine(
            std::hash<bytes_view>{}(pk),
            std::hash<managed_bytes_view>{}(ck));
    }
};

struct collection_key_view {
    bytes_view pk;
    managed_bytes_view ck;
    column_id col;
    bool operator==(const collection_key_view&) const noexcept = default;
    size_t hash() const noexcept {
        return utils::hash_combine(
            row_key_view{pk, ck}.hash(),
            std::hash<column_id>{}(col));
    }
};

struct row_key {
    bytes pk;
    managed_bytes ck;
    row_key_view view() const noexcept { return { bytes_view(pk), managed_bytes_view(ck)}; }
};

struct collection_key {
    bytes pk;
    managed_bytes ck;
    column_id col;
    collection_key_view view() const noexcept { return { bytes_view(pk), managed_bytes_view(ck), col}; }
};

inline row_key_view as_key_view(const row_key& k) noexcept { return k.view(); }
inline row_key_view as_key_view(const row_key_view& k) noexcept { return k; }
inline collection_key_view as_key_view(const collection_key& k) noexcept { return k.view(); }
inline collection_key_view as_key_view(const collection_key_view& k) noexcept { return k; }

// Transparent hash/equality shared by both caches; heterogeneous lookups with
// a *_key_view are allocation-free.
struct cache_key_hash {
    using is_transparent = void;
    template <typename K>
    size_t operator()(const K& k) const noexcept { return as_key_view(k).hash(); }
};

struct cache_key_eq {
    using is_transparent = void;
    template <typename A, typename B>
    bool operator()(const A& a, const B& b) const noexcept { return as_key_view(a) == as_key_view(b); }
};

using memtable_row_cache = std::unordered_map<row_key, uint64_t, cache_key_hash, cache_key_eq>;
using memtable_collection_cache = std::unordered_map<collection_key, uint64_t, cache_key_hash, cache_key_eq>;

struct guardrail_config {
    // Partition thresholds (replica-side only — checked via SSTable metadata index)
    utils::updateable_value<uint32_t> partition_size_fail_threshold_mb;
    utils::updateable_value<uint32_t> partition_size_warn_threshold_mb;
    utils::updateable_value<uint32_t> rows_count_fail_threshold;
    utils::updateable_value<uint32_t> rows_count_warn_threshold;
    // Row thresholds (shared: replica checks on-disk size, coordinator checks mutation memory)
    utils::updateable_value<uint32_t> row_size_fail_threshold_mb;
    utils::updateable_value<uint32_t> row_size_warn_threshold_mb;
    // Collection thresholds (shared: replica checks SSTable metadata, coordinator checks mutation)
    utils::updateable_value<uint32_t> collection_elements_fail_threshold;
    utils::updateable_value<uint32_t> collection_elements_warn_threshold;
    // Cell thresholds (coordinator-side only — checked from mutation content)
    utils::updateable_value<uint32_t> cell_size_fail_threshold_mb;
    utils::updateable_value<uint32_t> cell_size_warn_threshold_mb;
    // When true, coordinator-side soft limit violations are surfaced to the
    // client as CQL warnings in the response frame (see large_data_cql_warnings).
    utils::updateable_value<bool> cql_warnings_enabled;
};

// Tracker for mutation_partition_v2::apply_monotonically().
// Populates the memtable-level row/collection caches after each row
// merge so that subsequent writes to the same row can be rejected
// by the guardrail before an SSTable flush.
class large_data_cache_tracker {
public:
    large_data_cache_tracker(guardrail_config& cfg,
            memtable_row_cache& row_cache,
            memtable_collection_cache& collection_cache) noexcept
        : _cfg(cfg), _row_cache(row_cache), _collection_cache(collection_cache) {}

    void set_partition_key(bytes pk) noexcept { _pk_bytes = std::move(pk); }
    void on_row_merged(const schema& s, const rows_entry& row) noexcept;
    void on_collection_merged(const column_definition& cdef,
            const atomic_cell_or_collection& merged_cell) noexcept;

private:
    struct pending_collection {
        column_id id;
        uint32_t count;
    };

    guardrail_config& _cfg;
    memtable_row_cache& _row_cache;
    memtable_collection_cache& _collection_cache;
    bytes _pk_bytes;
    utils::small_vector<pending_collection, 4> _pending_collections;
};

} // namespace db
