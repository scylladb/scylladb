/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The read direction of the mapping: Parquet rows to mutation fragments.
//
// The format is specified in mapping.hh. This file has three parts:
//
// - open_mapping() computes the mapping of a file from the serialization
//   header, and checks the file's schema and layout against it. It runs
//   once per file and shard, when the sstable is opened.
// - read_plan holds what a read needs for one table schema: the leaves to
//   read (the projection), and the columns of the current schema that the
//   cells of the file belong to. It applies column_translation as the `mx`
//   reader does.
// - row_decoder decodes the current row of a codec reader built with the
//   plan's projection.
//
// The row decoder does not care in which direction the rows are read.
// position() moves all cursors to a row ordinal, and the decode methods
// read the row there. A step to the neighbouring row in the decoder's
// direction is cheap, any other move is a seek.
//
// Cursors of cell columns that are in a run of nulls are parked until the
// run ends (PLAN.md, "CPU per row"). The parked cursors are ordered by the
// ordinal at which they wake. A step therefore moves only the cursors that
// are not parked, and decoding visits only the cell columns that have a
// non-null leaf in the row.
//
// Every inconsistency that mapping.hh calls corruption, and every other
// value that the writer cannot produce, raises the malformed sstable error.

#include <cstdint>
#include <optional>
#include <vector>

#include "mutation/mutation_fragment.hh"
#include "mutation/position_in_partition.hh"
#include "schema/schema_fwd.hh"
#include "sstables/column_translation.hh"
#include "sstables/parquet/fetcher.hh"
#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/mapping.hh"

namespace sstables {
struct serialization_header;
}

namespace sstables::parquet {

class reader;
class column_cursor;

/// The mapping of a file, opened for reading.
struct opened_mapping {
    table_mapping mapping;
    /// The leaf of the file that holds each column of mapping.get_schema(),
    /// by column_id, or nothing if the column is absent from the file.
    std::vector<std::optional<leaf_index>> leaves;
};

/// Computes the mapping of a file from the serialization header, with the
/// key names that the file's schema gives to its first top-level fields.
/// Checks that the file's schema is the one that the mapping implies for
/// the columns that the file has, and that the file records the layout of
/// this format. Raises the malformed sstable error otherwise.
opened_mapping open_mapping(const serialization_header& header, const file_summary& summary);

/// What a read of a file needs for one table schema. See the top of this file.
///
/// The plan reads every column of the file that the table schema has. A
/// static or regular column that is absent from the table schema is never
/// read: the reader skips it without the check that `mx` makes for it
/// (PLAN.md, "Types").
class read_plan {
public:
    /// A leaf that the read needs. Its index in the plan is its index in
    /// the projection, so the cursor of slot i is reader::cursor(i).
    struct slot {
        leaf_index leaf;
        /// The levels of the leaf (leaf_column::max_def), and for a list
        /// leaf the definition level from which an entry is an element
        /// (leaf_column::repeated_def[0]), 0 otherwise.
        uint8_t max_def;
        uint8_t element_def;
        /// Whether the decoder may park the cursor in a run of nulls.
        bool parkable;
        /// The cell column that the leaf belongs to (an index of cells()),
        /// or -1 for a row-level or key leaf.
        int32_t owner;
    };
    /// A static or regular column of the file that the table schema has.
    struct cell_plan {
        const cell_columns* columns;
        column_kind kind;
        /// The column in the table schema.
        column_id id;
        /// The table schema's type cannot read the values of the file.
        /// Raises the malformed sstable error at the first cell, as `mx` does.
        bool schema_mismatch;
    };
private:
    const table_mapping& _mapping;
    table_schema_version _version;
    std::vector<slot> _slots;
    // The slot of each column of the mapping's schema, or -1.
    std::vector<int32_t> _slot_of;
    // Static columns, then regular columns, each by increasing id.
    std::vector<cell_plan> _cells;
public:
    /// `leaves` is opened_mapping::leaves of `m`. The plan refers to `m`,
    /// which must outlive it.
    read_plan(const opened_mapping& m, const file_summary& summary, const ::schema& s, const column_translation& ct);

    const table_mapping& mapping() const noexcept { return _mapping; }
    table_schema_version version() const noexcept { return _version; }
    const std::vector<slot>& slots() const noexcept { return _slots; }
    const std::vector<cell_plan>& cells() const noexcept { return _cells; }
    /// The slot of a column of the mapping's schema, or -1 if it is not read.
    int32_t slot_of(column_id c) const noexcept { return _slot_of[c]; }
    /// The projection to pass to the codec reader.
    std::vector<std::optional<leaf_index>> projection() const;
};

/// A range tombstone bound row.
struct decoded_bound {
    clustering_key_prefix prefix;
    bound_kind kind;
    tombstone tomb;
};

/// Decodes the rows of a codec reader that was built with a plan's projection.
/// See the top of this file.
///
/// After position(r) completes, the decode methods read row r. They may
/// be called in any order and more than once. Values are copied out of the
/// cursors, so the results stay valid after the decoder moves.
class row_decoder {
    struct parked {
        bool is = false;
        // The rows [lo, hi) are null.
        uint64_t lo = 0;
        uint64_t hi = 0;
    };

    const ::schema& _schema;
    const read_plan& _plan;
    reader& _reader;
    const read_direction _direction;
    const uint64_t _rows;
    // The cursors that are not parked are at _row.
    bool _positioned = false;
    uint64_t _row = 0;
    // The slots that are never parked.
    std::vector<uint32_t> _fixed;
    // The parkable slots that are positioned at _row.
    std::vector<uint32_t> _active;
    utils::chunked_vector<parked> _parked;
    // The parked slots, as a heap of (wake ordinal, slot) whose top wakes first.
    utils::chunked_vector<std::pair<uint64_t, uint32_t>> _wake;
    // The slots that try_position() could not move without I/O.
    std::vector<uint32_t> _pending;
    // Scratch space of decode_cells().
    std::vector<uint32_t> _owners;
    std::vector<uint64_t> _owner_epoch;
    uint64_t _epoch = 0;
    std::vector<bytes> _components;

    bool wakes_before(const std::pair<uint64_t, uint32_t>& a, const std::pair<uint64_t, uint32_t>& b) const noexcept;
    void push_wake(uint64_t wake, uint32_t slot);
    void try_park(uint32_t slot);
    void park_nulls();
    bool step(uint64_t row);
    bool jump(uint64_t row);

    const column_cursor* cursor(column_id c) const noexcept;
    // The slot of a leaf, or null if the read does not project it.
    const read_plan::slot* slot_of_column(column_id c) const noexcept;
    void decode_cells(::row& cells, column_kind kind, api::timestamp_type ts, std::optional<expiration> exp);
    std::optional<atomic_cell_or_collection> decode_cell(const read_plan::cell_plan& p, api::timestamp_type ts, std::optional<expiration> exp);
    std::optional<atomic_cell_or_collection> decode_collection(const read_plan::cell_plan& p, const column_definition& cdef,
            api::timestamp_type ts, std::optional<expiration> exp);
    std::optional<atomic_cell_or_collection> decode_udt(const read_plan::cell_plan& p, const column_definition& cdef,
            api::timestamp_type ts, std::optional<expiration> exp);
    std::optional<atomic_cell_or_collection> decode_counter(const read_plan::cell_plan& p, const column_definition& cdef, api::timestamp_type ts);
    api::timestamp_type default_timestamp() const;
    std::optional<expiration> default_expiration() const;
    void check_null(column_id c, std::string_view what) const;
    void check_no_cells();
    void check_no_partition_tombstone() const;
    void decode_clustering_components(bool full);
public:
    /// `r` must be built with `plan.projection()`, and `s` must be the
    /// schema of the plan. The rows are read in `direction`.
    row_decoder(const ::schema& s, const read_plan& plan, reader& r, read_direction direction);

    /// An estimate of the memory of a decoder of `plan`: its state per slot
    /// and per cell column. The keys of the row that it decodes are not
    /// counted.
    static size_t fixed_memory(const read_plan& plan) noexcept;

    /// Positions the decoder at `row`, which must be below the file's row
    /// count, without I/O. Returns false if that needs I/O. Then
    /// finish_position() must complete the move before the decoder is used.
    bool try_position(uint64_t row);
    /// Completes the move that try_position() started.
    future<> finish_position();
    /// Positions the decoder at `row`.
    future<> position(uint64_t row);
    /// Drops the pages that the cursors hold and the pages fetched ahead,
    /// and unpositions the decoder. The next position() reads again.
    void release_pages() noexcept;
    uint64_t current_row() const noexcept { return _row; }

    // The current row.

    fragment_kind kind() const;
    /// Whether the row starts a partition. Its first partition key column
    /// has a value.
    bool starts_partition() const;
    /// The partition key, of a row that starts a partition.
    partition_key decode_partition_key();
    /// The partition tombstone, of a row that starts a partition.
    tombstone decode_partition_tombstone() const;
    /// Checks a row of kind none.
    void check_none_row();
    /// The static row, of a row of kind static_row. Cells of columns that
    /// were dropped after they were written are left out.
    static_row decode_static_row();
    /// The key of a clustering row.
    clustering_key decode_clustering_key();
    /// The marker, the tombstones, and the cells of a clustering row,
    /// applied to `cr`.
    void decode_clustering_row(clustering_row& cr);
    /// A range tombstone bound row.
    decoded_bound decode_bound();
};

} // namespace sstables::parquet
