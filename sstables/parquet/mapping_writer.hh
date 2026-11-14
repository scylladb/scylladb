/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The write direction of the mapping: mutation fragments to Parquet rows.
//
// The format is specified in mapping.hh. This file holds the writer, and
// the policy that picks the row defaults.
//
// Use, for each partition, in sstable order:
//
//     mapping_writer w(schema, header, file_writer, cfg);
//     w.start_partition(key);                     // appends to the next row
//     w.partition_tombstone(t);                   // optional, before any row
//     w.write_static_row(sr);                     // a row, unless sr is empty
//     for each clustering fragment:
//         w.write_clustering_row(cr);             // one row
//         for (auto b : range_tombstone_bounds(open, rtc)) {
//             w.write_range_tombstone_bound(rtc.position().key(), b.kind, b.tomb);   // one row
//         }
//     w.end_partition();                          // a row of kind "none" if the partition has none
//     ...
//     w.finish();
//
// Every call that ends a Parquet row returns a row_info.

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "keys/clustering_bounds_comparator.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "sstables/parquet/mapping.hh"
#include "sstables/parquet/writer.hh"
#include "utils/chunked_vector.hh"
#include "utils/small_vector.hh"

namespace sstables {
class file_writer;
struct serialization_header;
}

namespace sstables::parquet {

/// What a Parquet row of the mapping writer reported.
struct row_info {
    /// The ordinal of the row in the file.
    uint64_t ordinal;
    /// The raw size of the row (writer::current_row_raw_size()). The first
    /// row of a partition includes the partition key and the partition tombstone.
    uint64_t raw_size;
    /// What writer::end_row() returned: a page set ended at this row, so
    /// the next row starts a page set.
    bool page_set_ended;
};

/// Picks the defaults of a row that has no live row marker to supply them.
///
/// The writer calls it once per static row and per clustering row without
/// a marker (pick_timestamp() and pick_expiration()), and once per
/// clustering row with a dead marker (pick_expiration() only). Any choice
/// produces a correct file. The choice only affects the size of the file.
class row_defaults_policy {
public:
    virtual ~row_defaults_policy() = default;
    /// `candidates` holds the timestamps of the row: of its cells,
    /// collection elements, collection tombstones, and row tombstones. It
    /// may be empty. The result may be any timestamp.
    virtual api::timestamp_type pick_timestamp(const utils::chunked_vector<api::timestamp_type>& candidates) = 0;
    /// `candidates` holds, for each live cell and live collection element
    /// of the row except counters, its expiration, or nothing if it does
    /// not expire. It may be empty. The result may be any expiration with a
    /// non-zero TTL, or nothing for a row without a default TTL.
    virtual std::optional<expiration> pick_expiration(const utils::chunked_vector<std::optional<expiration>>& candidates) = 0;
};

/// The default policy. It picks the majority timestamp and the majority
/// expiration of the row with the Boyer-Moore majority vote, in time
/// linear in the number of candidates and without allocation. Without a
/// majority, it picks one of the candidates. A row without timestamps gets
/// the timestamp of the previous row that had some, which keeps the `ts`
/// column in runs.
std::unique_ptr<row_defaults_policy> make_default_row_defaults_policy();

/// A policy for tests. It picks a random candidate or a random value, so
/// that the reader is tested against defaults that no cell has, and against
/// timestamp differences that wrap around.
std::unique_ptr<row_defaults_policy> make_random_row_defaults_policy(uint64_t seed);

/// A range tombstone bound row: its kind and the tombstone that it holds.
struct range_tombstone_bound {
    bound_kind kind;
    tombstone tomb;
};

/// The bound rows of a range tombstone change at its position, in order.
/// `open` is the range tombstone open before the change, or an empty one.
///
/// A change that opens a tombstone gives a start bound. A change that
/// closes one gives an end bound. A change from one tombstone to another (a
/// boundary) gives an end bound followed by a start bound. A change from no
/// tombstone to no tombstone gives nothing. The key of every bound is
/// `rtc.position().key()`.
///
/// The split is a separate step so that the sstable writer sees each row
/// on its own: it adds row index entries and tracks the open tombstone per
/// row, and a page set may end between the two rows of a boundary.
utils::small_vector<range_tombstone_bound, 2> range_tombstone_bounds(tombstone open, const range_tombstone_change& rtc);

/// Writes mutation fragments as Parquet rows. See the top of this file.
///
/// The cost of a row is proportional to its cells, collection elements,
/// and key components, plus the fixed set of row-level columns. Columns are
/// found by position, and the common case allocates nothing per row.
///
/// The methods that end a row may write to the file, so they must run in a
/// seastar thread, as must finish().
class mapping_writer {
    struct defaults {
        api::timestamp_type ts;
        std::optional<expiration> exp;
    };
    struct cell_metadata;

    const ::schema& _schema;
    table_mapping _mapping;
    writer _writer;
    std::unique_ptr<row_defaults_policy> _policy;
    // The mapping of each static and regular column, by column_id.
    std::vector<const cell_columns*> _static_by_id;
    std::vector<const cell_columns*> _regular_by_id;
    bool _in_partition = false;
    bool _partition_has_row = false;
    // Scratch space for the policy, reused across rows. A row with many
    // cells or collection elements makes them large, so they are chunked.
    utils::chunked_vector<api::timestamp_type> _timestamps;
    utils::chunked_vector<std::optional<expiration>> _expirations;

    levels value_levels(column_id c) const;
    void append_key(const key_columns& k, managed_bytes_view v);
    void write_clustering_prefix(const clustering_key_prefix& prefix);
    void collect_candidates(const row& cells, column_kind kind, bool timestamps);
    defaults pick_defaults(const row& cells, column_kind kind, const row_marker* marker, const row_tombstone* tomb);
    void write_row_level(fragment_kind kind, const defaults& d);
    void write_cells(const row& cells, column_kind kind, const defaults& d);
    void write_atomic_cell(const cell_metadata_columns& meta, column_id value, value_type vt, atomic_cell_view cell, const defaults& d);
    void write_counter_cell(const cell_columns& c, atomic_cell_view cell, const defaults& d);
    void write_collection(const cell_columns& c, collection_mutation_view cmv, const defaults& d);
    void write_udt(const cell_columns& c, collection_mutation_view cmv, const defaults& d);
    void write_tombstone(column_id ts, column_id deletion_time, tombstone t, api::timestamp_type base);
    const cell_columns& column(column_kind kind, column_id id) const;
    void check_in_partition() const;
    row_info end_row();
public:
    /// `s` is the table schema, and `header` the serialization header that
    /// the sstable stores, which make_serialization_header() built from `s`.
    /// The writer keeps a reference to `s`. Compact storage tables are
    /// rejected. A null `policy` means the default policy.
    mapping_writer(const ::schema& s, const serialization_header& header, sstables::file_writer& out, writer_config cfg,
            std::unique_ptr<writer_memory_handle> memory = nullptr, std::unique_ptr<row_defaults_policy> policy = nullptr);

    const table_mapping& mapping() const noexcept { return _mapping; }
    /// The file writer, for its size measures (rows(), raw_size(),
    /// estimated_size(), buffered_size(), ...) and its statistics.
    const writer& file() const noexcept { return _writer; }

    /// Starts a partition. The key goes into the partition's first row,
    /// which the next call that writes a row ends. Its ordinal is file().rows().
    void start_partition(const partition_key& key);
    /// Records the partition tombstone in the partition's first row. Must
    /// come before any row of the partition. An empty tombstone is ignored.
    void partition_tombstone(tombstone t);
    /// Writes the static row, which must come before any other row of the
    /// partition. Returns nothing, and writes nothing, if it has no cells.
    std::optional<row_info> write_static_row(const static_row& sr);
    /// Writes a clustering row. Its key must be full.
    row_info write_clustering_row(const clustering_row& cr);
    /// Writes a range tombstone bound row. See range_tombstone_bounds().
    /// `t` must not be empty.
    row_info write_range_tombstone_bound(const clustering_key_prefix& prefix, bound_kind kind, tombstone t);
    /// Ends the partition. If the partition has no row yet, writes a row of
    /// kind "none" and returns it.
    std::optional<row_info> end_partition();

    /// Ends the file with the key-value metadata of the format, followed by
    /// `extra`. Must come after end_partition().
    writer_result finish(std::vector<key_value> extra = {});
};

} // namespace sstables::parquet
