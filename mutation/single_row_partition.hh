/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "mutation/partition_version.hh"
#include "reader_permit.hh"
#include "mutation_partition.hh"
#include "utils/lru.hh"

#include <seastar/util/backtrace.hh>

#include <deque>
#include <fmt/format.h>

/// Determines format of partition entry in memtable and row_cache.
/// For a given table, all partitions use the same format within a given scylla process.
/// Different servers may use different format.
enum class partition_format {
    // Handles large partitions with many rows well.
    // With MVCC.
    // rows entries linked into LRU, partition entry evicted when the last row is evicted.
    // Partitions may be incomplete in cache (tracks clustering range continuity).
    generic,

    // For schema without clustering columns.
    // No MVCC.
    // partition entry (single_row_partition) linked into LRU.
    // Partitions always complete in cache (no continuity tracking).
    single_row
};

inline
partition_format get_partition_format(const schema& s) {
    return s.clustering_key_size() ? partition_format::generic : partition_format::single_row;
}

// A simplified mutation_partition object for schemas without a clustering key.
// LSA-managed object.
// Mutators should run under owning allocator.
class single_row_partition final : public evictable {
    schema_ptr _s;

    // Indirect so that cache_entry doesn't increase in size for generic storage.
    managed_ref<deletable_row> _row;
public:
    using fragments_vector = utils::small_vector<mutation_fragment_v2, 3>;

    single_row_partition(schema_ptr);
    single_row_partition(schema_ptr, const mutation_partition&);
    single_row_partition(schema_ptr, mutation_partition&&);
    single_row_partition(single_row_partition&&) noexcept = default;
    single_row_partition(const single_row_partition&) = delete;

    // Memory usage excluding memory taken directly by this object.
    // Run under owning allocator.
    size_t external_memory_usage() const {
        return _row->cells().external_memory_usage(*_s, column_kind::regular_column);
    }

    void on_evicted() noexcept override;
    void on_evicted_shallow() noexcept override {};

    // Frees elements of this entry.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently(cache_tracker*) noexcept;

    deletable_row& row() { return *_row; }
    const deletable_row& row() const { return *_row; }
    const schema_ptr& get_schema() const { return _s; }
    fragments_vector as_fragments(const dht::decorated_key& key, reader_permit) const;

    // No exception guarantees.
    void apply(const schema& mp_schema, const mutation_partition& mp, mutation_application_stats& app_stats);

    // No exception guarantees.
    void apply(const schema& mp_schema, mutation_partition&& mp, mutation_application_stats& app_stats);

    // Weak exception guarantees. After exception, p may be partially applied, but both this and p will
    // commute to the same value as they would, should the exception not happen.
    void apply_monotonically(single_row_partition&& p);

    // Strong exception guarantees.
    void upgrade(schema_ptr new_schema);
};

/// A run-time dependent partition storage with a layout chosen based
/// on partition_format which is known externally.
///
/// Starts with uninitialized storage, must initialize manually.
/// If initialized, must call destroy() before this instance is destroyed.
union partition_storage final {
    partition_entry pe;       // partition_format::generic
    single_row_partition srp; // partition_format::single_row

    partition_storage() noexcept {}
    ~partition_storage() noexcept {}
    partition_storage(const partition_storage&) = delete;
    partition_storage(partition_storage&&) = delete;

    static size_t size_of(partition_format f) {
        switch (f) {
            case partition_format::generic: return sizeof(partition_entry);
            case partition_format::single_row: return sizeof(single_row_partition);
        }
        abort();
    }

    template <typename Visitor>
    auto accept(this auto& self, partition_format f, Visitor&& v) {
        switch (f) {
            case partition_format::generic: return v(self.pe);
            case partition_format::single_row: return v(self.srp);
        }
        throw_with_backtrace<std::invalid_argument>(fmt::format("Unsupported format: {}", static_cast<int>(f)));
    }

    partition_storage(partition_format f, partition_storage&& o) noexcept {
        switch (f) {
            case partition_format::single_row:
                new (&srp) single_row_partition(std::move(o.srp));
                break;
            case partition_format::generic:
                new (&pe) partition_entry(std::move(o.pe));
                break;
            default:
                abort();
        }
    }

    void destroy(partition_format f) noexcept {
        switch (f) {
            case partition_format::single_row:
                srp.~single_row_partition();
                break;
            case partition_format::generic:
                pe.~partition_entry();
                break;
            default:
                abort();
        }
    }
};
