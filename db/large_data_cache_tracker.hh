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
#include "schema/schema_fwd.hh"
#include "utils/hash.hh"
#include "utils/updateable_value.hh"
#include "utils/small_vector.hh"

class schema;
class rows_entry;
class column_definition;
class atomic_cell_or_collection;

namespace db {

struct row_key {
    bytes pk;
    bytes ck;
    bool operator==(const row_key&) const = default;
    struct hash {
        size_t operator()(const row_key& k) const noexcept;
    };
};

struct collection_key {
    bytes pk;
    bytes ck;
    column_id col;
    bool operator==(const collection_key&) const = default;
    struct hash {
        size_t operator()(const collection_key& k) const noexcept;
    };
};

using memtable_row_cache = std::unordered_map<row_key, uint64_t, row_key::hash>;
using memtable_collection_cache = std::unordered_map<collection_key, uint64_t, collection_key::hash>;

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
