/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Layer 2: the Scylla-facing half of the Parquet storage format.
//
// Turns a stream of rows-with-cells into Parquet columns at a chosen metadata
// folding level, and reconstructs them losslessly on the way back. This is the
// piece that decides whether the format is viable at all -- see
// docs/dev/parquet-storage-format.md section 5.3 and the 26.8x measurement in
// section 10.3.
//
// NOTE ON TYPES. The cell/row structs below mirror the shape of Scylla's
// atomic_cell and clustering_row, not their implementation. Wiring this to the
// real mutation_fragment stream is the remaining Phase 2 work; keeping the
// shredder expressed against a small local model lets the folding logic be
// tested and fuzzed on its own, which is where the risk actually is.

#include "format/parquet_writer.hh"
#include "format/parquet_metadata.hh"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace sstables::parquet {

using namespace sstables::parquet::format;

// ---------------------------------------------------------------- value model
using value = std::variant<int32_t, int64_t, double, std::string>;

enum class cql_type { int32, bigint, dbl, text, blob, timestamp };

inline phys_type phys_of(cql_type t) {
    switch (t) {
    case cql_type::int32:     return phys_type::int32;
    case cql_type::bigint:    return phys_type::int64;
    case cql_type::timestamp: return phys_type::int64;
    case cql_type::dbl:       return phys_type::dbl;
    case cql_type::text:      return phys_type::byte_array;
    case cql_type::blob:      return phys_type::byte_array;
    }
    return phys_type::byte_array;
}

inline std::optional<int32_t> converted_of(cql_type t) {
    switch (t) {
    case cql_type::text:      return int32_t(converted::utf8);
    // MILLIS, not MICROS. Two different things are easy to conflate here: a cell's
    // *write* timestamp (`USING TIMESTAMP`, and our `__ts` leaf) is microseconds
    // since epoch, but a value of CQL type `timestamp` is **milliseconds** -- that
    // is what timestamp_type serialises. Annotating the column MICROS while writing
    // millisecond values makes every external reader report a date in 1970: the
    // 2023-01-01 value 1672531200000 reads back as 1970-01-20T08:35:31.2Z.
    //
    // Our own reader is unaffected either way, because it inverts the mapping from
    // `cql_type` and never consults the annotation -- which is exactly why this
    // survived a round-trip suite and only showed up when pyarrow read the file.
    // See docs/dev/parquet-storage-format.md section 10.1g.
    case cql_type::timestamp: return int32_t(converted::timestamp_millis);
    default:                  return std::nullopt;
    }
}

enum class column_kind { partition_key, clustering_key, regular };

struct cql_column {
    std::string name;
    cql_type    type;
    column_kind kind;
    // A non-frozen collection. It becomes a Dremel MAP group rather than a
    // single leaf (design doc 5.2); frozen ones are already opaque blobs and
    // travel as ordinary BYTE_ARRAY values.
    bool        multi_cell = false;
    // A counter column. It shares the non-frozen-collection shape -- one map element per replica
    // shard -- so `multi_cell` is true for it too, and nothing downstream could previously tell
    // the two apart. The distinction matters because a counter's element *values* are not opaque
    // blobs: they are two big-endian int64s, the shard's value and its logical clock, and an
    // external reader has no way to know that from the schema alone.
    bool        counter = false;
};

// A cell as the storage layer sees it: a value plus its own metadata. Key
// columns have no cell metadata -- they are part of the row's identity.
struct cell {
    bool                   live = true;
    std::optional<value>   v;                 // nullopt => the column is absent
    int64_t                timestamp = 0;
    std::optional<int32_t> ttl;               // seconds
    std::optional<int32_t> local_deletion_time;
};

// A deletion: when it happened, and when it becomes collectable.
struct deletion_info {
    int64_t timestamp = 0;
    int32_t local_deletion_time = 0;
    bool operator==(const deletion_info&) const = default;
};

// The CQL row marker -- what makes a row exist even with every column null.
// Almost every INSERT creates one, so it is stored as a delta against the row's
// own timestamp, which is nearly always zero and costs nothing after zstd.
struct marker_info {
    int64_t                timestamp = 0;
    std::optional<int32_t> ttl;
    std::optional<int32_t> expiry;
    bool operator==(const marker_info&) const = default;
};

// A range tombstone change: not a row, but carried as one so that it keeps its
// place in the clustering order without a second stream. The clustering-key
// columns hold the bound's prefix, padded past `prefix_len` with values that
// mean nothing.
struct rtc_info {
    int32_t weight = 0;       // bound_weight
    int32_t region = 0;       // partition_region
    int32_t prefix_len = 0;   // clustering components the bound actually sets
    std::optional<deletion_info> tomb;   // nullopt: the change closes a range
    bool operator==(const rtc_info&) const = default;
};

// Two big-endian int64s in sixteen bytes.
//
// Counter cells reuse the collection representation -- one element per shard --
// and this is how a shard's two halves are packed: the key holds its id (the
// most and least significant bits of the UUID) and the value holds its counter
// value and logical clock. Big-endian so the bytes sort like the numbers and the
// encoding does not depend on the host.
inline std::string pack_i64_pair(int64_t a, int64_t b) {
    std::string s(16, '\0');
    const uint64_t ua = uint64_t(a), ub = uint64_t(b);
    for (int i = 0; i < 8; ++i) {
        s[size_t(i)]     = char(uint8_t(ua >> (56 - 8 * i)));
        s[size_t(8 + i)] = char(uint8_t(ub >> (56 - 8 * i)));
    }
    return s;
}

inline bool unpack_i64_pair(std::string_view s, int64_t& a, int64_t& b) {
    if (s.size() != 16) { return false; }
    uint64_t ua = 0, ub = 0;
    for (int i = 0; i < 8; ++i) {
        ua = (ua << 8) | uint8_t(s[size_t(i)]);
        ub = (ub << 8) | uint8_t(s[size_t(8 + i)]);
    }
    a = int64_t(ua);
    b = int64_t(ub);
    return true;
}

// One entry of a non-frozen collection. Scylla stores these as (key, cell)
// pairs: for a set the key is the element and the cell carries only liveness,
// for a map the key is the map key, for a list a timeuuid. All serialised, so
// both sides are opaque bytes here.
struct collection_element {
    std::string                key;
    std::optional<std::string> value;   // nullopt => the element is dead
    int64_t                    timestamp = 0;
    std::optional<int32_t>     ttl;
    std::optional<int32_t>     local_deletion_time;
    bool operator==(const collection_element&) const = default;
};

struct collection_cell {
    std::optional<deletion_info>    tomb;       // collection-wide tombstone
    std::vector<collection_element> elements;
    bool operator==(const collection_cell&) const = default;
};

// How much heap a buffered row costs, for the write-side memory budget (R-13).
//
// This is an *estimate that errs high*, on purpose: its only job is to make the writer
// cut a row group before the shard runs out of memory, so under-counting is the
// dangerous direction and over-counting merely cuts a little early.
//
// The structural part (sizeof + string contents) accounts for roughly 3/4 of what is
// actually resident. The rest is allocator overhead: every std::map node is its own
// malloc with a header and size-class rounding, and vectors carry capacity slack. The
// per-entry constants below are therefore deliberately larger than a red-black tree
// node needs -- calibrated against measured RSS, which came out at ~1.77 kB/row for a
// 10-column time-series table (design doc 5.5a).
inline constexpr size_t map_entry_overhead = 96;   // node + malloc header + rounding
inline constexpr size_t vec_slack_num = 3, vec_slack_den = 2;   // assume 1.5x capacity

inline size_t heap_bytes(const value& v) {
    const auto* s = std::get_if<std::string>(&v);
    return s ? s->capacity() : 0;
}

inline size_t heap_bytes(const cell& c) {
    return sizeof(cell) + (c.v ? heap_bytes(*c.v) : 0);
}

struct row {
    std::vector<value>       key;             // partition key then clustering key
    std::map<size_t, cell>   cells;           // index into the regular columns
    std::optional<marker_info>   marker;      // row marker, if the row has one
    // A row tombstone has two halves and the shadowable one is always >= the
    // regular one. Collapsing them into a single value turns a shadowable
    // tombstone into a regular one, which deletes cells that should survive.
    std::optional<deletion_info> row_del;          // shadowable
    std::optional<deletion_info> row_del_regular;  // regular
    // The partition's tombstone, repeated on every row of that partition. It
    // costs nothing -- a column that is constant within a partition and usually
    // absent entirely compresses away -- and it keeps a row self-describing,
    // which is what lets the reader work a window at a time.
    std::optional<deletion_info> part_del;
    // True for a placeholder row standing in for a partition that has no
    // clustering rows -- a static row or a bare partition tombstone. Its
    // clustering-key values are meaningless. Marking the row is cheaper than
    // making every clustering-key column nullable for every table.
    bool no_ck = false;
    // Set when this "row" is really a range tombstone change.
    std::optional<rtc_info> rtc;
    // Non-frozen collections, keyed in the same index space as `cells`.
    std::map<size_t, collection_cell> collections;
};

// See the note on map_entry_overhead above: errs high, by design.
inline size_t heap_bytes(const collection_cell& cc) {
    size_t n = sizeof(collection_cell)
             + cc.elements.capacity() * sizeof(collection_element);
    for (const auto& e : cc.elements) {
        n += e.key.capacity() + (e.value ? e.value->capacity() : 0);
    }
    return n;
}

inline size_t heap_bytes(const row& r) {
    size_t n = sizeof(row);
    n += r.key.capacity() * sizeof(value) * vec_slack_num / vec_slack_den;
    for (const auto& v : r.key) { n += heap_bytes(v); }
    for (const auto& [k, c] : r.cells) {
        n += map_entry_overhead + sizeof(k) + heap_bytes(c);
    }
    for (const auto& [k, cc] : r.collections) {
        n += map_entry_overhead + sizeof(k) + heap_bytes(cc);
    }
    return n;
}

// ---------------------------------------------------------------- folding
enum class folding_level {
    verbatim,     // L0: five leaves per regular column. The 2020 mapping.
    row_folded,   // L1: one leaf per column, one __ts per row, plus a sparse
                  //     side-channel for cells that disagree with their row.
    uniform,      // L2: as L1 but the whole row group shares one timestamp,
                  //     which then lives in the file's key/value metadata.
    logical,      // L3: the user's CQL schema and nothing else -- no cell
                  //     metadata at all. LOSSY: write times, TTLs and deletions
                  //     are discarded, so this can never be a storage format.
                  //     Export only; reassemble() refuses it.
};

// True for levels that can be read back into the rows they came from. L3 cannot.
inline bool folding_is_lossless(folding_level l) {
    return l != folding_level::logical;
}

// How L1 records cells whose timestamp differs from their row's.
//
//   per_column -- one optional __tsx_<col> leaf per regular column. Simple, but
//                 measured badly: at row-group scale "some row diverges in this
//                 column" is near-certain, so every leaf materialises even at 1%
//                 divergence while carrying almost nothing (43 -> 83 leaves; see
//                 docs/dev/parquet-storage-format.md section 10.3a).
//   sparse     -- two leaves total regardless of table width: a per-row bitmap of
//                 which columns diverge, and a blob of zigzag-varint deltas from
//                 the row timestamp. Both null for rows with no exception, which
//                 costs a definition-level bit and compresses away.
enum class exception_encoding { per_column, sparse };

const char* to_string(folding_level);

// The physical Parquet schema a (cql schema, folding level) pair produces, plus
// the bookkeeping a reader needs to invert it.
struct mapped_schema {
    std::vector<column_spec> columns;         // physical Parquet leaves
    // The schema tree the leaves came from, depth-first with the root at index 0.
    // Flat for a schema without collections; a collection contributes a MAP group.
    // Kept so the writer always takes one path, nested or not.
    std::vector<format::schema_element> tree;
    folding_level            level{};
    size_t                   n_key = 0;       // leading key columns
    size_t                   n_regular = 0;
    // Index of the __ts column, if the level materialises one.
    std::optional<size_t>    ts_index;
    // L1/per_column: index of the exception leaf for each regular column.
    std::vector<std::optional<size_t>> ts_exc_index;
    // L1/sparse: the two side-channel leaves.
    exception_encoding    exc_encoding = exception_encoding::sparse;
    std::optional<size_t> tsx_mask_index;
    std::optional<size_t> tsx_vals_index;
    // For L0: index of the first of the four metadata leaves per column.
    std::vector<std::optional<size_t>> meta_base_index;
    // For L1: the per-column TTL and local-deletion-time leaves. Recorded
    // individually rather than as base + k, because the groups are skipped for
    // collection columns (their per-element metadata lives inside the group) and
    // because writing only one of the two silently produced a ragged row group.
    //
    // `l1_ldt_index` now carries only the *expiry of a live cell* -- a cell that has a
    // value and a TTL. A **dead** cell's deletion time goes to the folded channel below.
    // The two are disjoint by construction: the discriminator is whether the cell has a
    // value, so no cell writes to both.
    std::vector<std::optional<size_t>> l1_ttl_index, l1_ldt_index;
    // L1, the folded deletion channel. The same trick as __ts/__tsx, applied to the
    // other half of the cell metadata.
    //
    // Before this existed, every dead cell wrote its deletion time into its own column's
    // `__ldt_<col>` leaf: one leaf per column, 195 of them on the Backblaze corpus slice,
    // 60.1 MB for 42 448 175 tombstones, where the same rows' *write* times cost 1.9 MB in
    // a single folded `__ts`. Same information shape, ~32x the cost, and the reason `pq`
    // paid ~63 MB where the row format paid ~10 MB for the same tombstones. The fold was
    // never extended; the data itself is not the defect.
    //
    // Three leaves, independent of table width:
    //
    //   __ldt        the row's deletion time -- the one most of its dead cells agree on.
    //   __dmask      a per-row bitmap of which regular columns are dead in this row. This
    //                also carries what `__ldt_<col>`'s *presence* used to mean, and it has
    //                to: "dead" and "absent" are different states (a dead cell must keep
    //                shadowing older data on another replica, so it cannot be written as a
    //                Parquet null), and the value leaf's definition level cannot tell them
    //                apart. Losing this distinction resurrects deleted data.
    //   __ldtx_mask  which of those dead cells disagree with the row's deletion time, and
    //   __ldtx_vals  their zigzag-varint deltas from it. Both null for a row whose dead
    //                cells all share one deletion time -- which is exactly what a
    //                bind-NULL INSERT produces, since one statement carries one timestamp.
    //
    // NOT applied to collections. A non-frozen collection's per-element `__ldt` lives
    // inside its MAP group, where there is no row-level leaf to fold into and the element
    // count varies per row; `__dmask` indexes regular columns, and a collection column
    // never sets its bit. Same exclusion, for the same reason, as the statistics-based
    // leaf elision in design doc 10.26: a repeated slot that is "present but empty" is not
    // the same as absent.
    std::optional<size_t> ldt_index, dmask_index, ldtx_mask_index, ldtx_vals_index;
    // Row marker, row tombstone and partition tombstone leaves. Each group is
    // materialised only when the data needs it.
    std::optional<size_t> rm_index, rm_ttl_index, rm_ldt_index;
    std::optional<size_t> rt_ts_index, rt_ldt_index;
    std::optional<size_t> rtr_ts_index, rtr_ldt_index;
    std::optional<size_t> pt_ts_index, pt_ldt_index;
    std::optional<size_t> no_ck_index;
    std::optional<size_t> rtc_w_index, rtc_reg_index, rtc_len_index,
                          rtc_ts_index, rtc_ldt_index;
    // Per regular column: where its value lives, and whether that is a scalar
    // leaf or the first of the five leaves a collection group contributes.
    // Recorded rather than computed, because arithmetic over leaf positions is
    // exactly what broke when the metadata groups were added.
    std::vector<size_t> value_leaf;
    std::vector<bool>   value_is_collection;
    // A counter column: a collection whose element value is typed rather than opaque. Its group
    // has six leaves instead of five, and the extra `clock` is appended after __ldt rather than
    // sitting next to `value`, so every existing vcol+N offset keeps its meaning. Schema order is
    // cosmetic here -- a reader sees `value` and `clock` as siblings of the same group either way
    // -- and the alternative was renumbering offsets throughout the shred and reassemble paths,
    // which is the most delicate code in this file.
    std::vector<bool>   value_is_counter;
    // Row-level collection tombstone leaves, per regular column.
    std::vector<std::optional<size_t>> ct_ts_index, ct_ldt_index;
    // For L2: the single timestamp shared by every cell.
    std::optional<int64_t>   uniform_ts;

    size_t leaf_count() const { return columns.size(); }
};

// Decides which optional metadata leaves are actually needed for this batch of
// rows, then builds the schema. Materialising a leaf that is never used is the
// entire cost of the 2020 mapping, so this inspects the data first.
// What map_schema decides by looking at the data: which optional leaf groups a
// batch of rows actually needs. Separated from the schema builder so a reader
// can recover the same answers from a file it did not write.
struct schema_flags {
    bool any_ttl = false;
    // The two halves of what used to be one `any_deletion` flag. They drive different
    // leaves now, so a table with TTLs but no tombstones no longer materialises a
    // deletion channel, and the far more common reverse -- tombstones but no TTLs, which
    // is every bind-NULL load -- no longer materialises one leaf per column.
    //
    //   any_live_expiry -- some cell has a value *and* a local_deletion_time, i.e. a live
    //                      cell with a TTL, whose ldt is its expiry time. Per column, so
    //                      it keeps the `__ldt_<col>` leaves.
    //   any_dead_cell   -- some cell has no value but does have a local_deletion_time,
    //                      i.e. a tombstone. Folded into the row-level channel.
    bool any_live_expiry = false;
    bool any_dead_cell = false;
    bool all_same_ts = true;
    bool any_marker = false;
    bool any_marker_ttl = false;
    bool any_row_del = false;
    bool any_part_del = false;
    bool any_no_ck = false;
    bool any_rtc = false;
    std::vector<bool> col_diverges;      // per regular column
    std::optional<int64_t> single_ts;
};

// Whether the leaf set may be derived from the rows, or has to cover every case up
// front.
//
// Parquet fixes one leaf set for a whole file, before the first row group is written.
// `derived` inspects every row and emits only the metadata leaves the data actually
// needs, which is the smallest file and is correct as long as all rows are in hand --
// i.e. as long as the sstable becomes a single row group. `conservative` emits every
// optional metadata leaf regardless of use, which is what an incremental writer is
// forced into: at its first flush it cannot know whether row ten million carries a TTL.
//
// The unused leaves are all-null, so they cost definition levels that RLE away plus a
// fixed ~225 B per leaf. Measured: +2.52 % on a narrow table and +7.2 % on a small wide
// one, falling to +0.55 % once the file is large -- which is exactly when cutting is
// needed. See design doc 5.5a.
enum class leaf_set { derived, conservative };

schema_flags scan_rows(const std::vector<cql_column>& cols, const std::vector<row>& rows,
                       leaf_set = leaf_set::derived);

// The single schema builder. Both map_schema (write side) and
// recover_mapped_schema (read side) go through this, so there is exactly one
// definition of the leaf layout.
// `encoding_overrides` maps a *CQL column name* to the encoding an operator asked for through
// `parquet = {'encoding.<column>': ...}`. It is consulted after the structural rules below and wins
// over them, because the operator knows things the schema cannot: that a table is scan-only, or that
// a partition key happens to be ordered. Names that match no column are impossible here -- the DDL
// layer rejects those (cf_prop_defs::apply_to_builder) -- so an unmatched entry is simply inert.
//
// Only leaves that correspond to a CQL column can be overridden. The synthetic leaves (`__ts`,
// deletion and TTL columns) keep their own encodings, since they are not something a user names.
mapped_schema build_mapped_schema(const std::vector<cql_column>& cols,
                                  folding_level requested,
                                  const schema_flags&,
                                  exception_encoding = exception_encoding::sparse,
                                  const std::map<std::string, format::encoding>& encoding_overrides = {});

mapped_schema map_schema(const std::vector<cql_column>& cols,
                         folding_level requested,
                         const std::vector<row>& rows,
                         exception_encoding = exception_encoding::sparse,
                         leaf_set = leaf_set::derived,
                         const std::map<std::string, format::encoding>& encoding_overrides = {});

// Rebuild the mapped_schema of a file we did not write, from its footer. The
// folding level comes from the scylla.folding_level key/value entry; which
// optional leaf groups exist is read off the leaf names. Throws if the file was
// not written by this mapping, or if the recovered layout does not match the
// file's own leaves -- a mismatch means a silent misread, so it must not be
// tolerated.
mapped_schema recover_mapped_schema(const file_metadata&,
                                    const std::vector<cql_column>& cols);

// Which leaves a projection may skip: one byte per leaf, non-zero meaning "do not read".
//
// `want_regular` has one entry per regular column, in the order `mapped_schema::value_leaf` uses.
//
// Only *per-column* leaves of unwanted columns are skipped. Every key leaf and every **shared**
// metadata channel is kept, however narrow the projection, and that is not conservatism for its own
// sake: `__dmask` is what distinguishes a dead cell from an absent one, and the comment on it in
// this header says plainly that losing the distinction resurrects deleted data. A projection is
// supposed to make a read cheaper, not change what the surviving columns mean, so the channels that
// every column depends on are not the projection's to drop.
//
// Skipping a leaf makes its column_data come back flagged `skipped`, which reassemble() treats as
// "no value in this leaf for any row of this window" -- the right answer for a column the caller
// did not ask for, and the reason this needs no new decode path.
std::vector<uint8_t> projection_skip_mask(const mapped_schema& ms,
                                          const std::vector<bool>& want_regular);

// Shred rows into Parquet columns according to `ms`.
std::vector<column_data> shred(const mapped_schema& ms,
                               const std::vector<cql_column>& cols,
                               const std::vector<row>& rows);

// Rebuild the rows. Must be exactly equal to the input for L0 and L1, and for
// L2 whenever the uniform precondition held.
std::vector<row> reassemble(const mapped_schema& ms,
                            const std::vector<cql_column>& cols,
                            const std::vector<column_data>& colsdata,
                            size_t nrows);

// Convenience: schema + shred + write a complete Parquet file.
// Declares the counter convention in the footer for any counter columns present. Exposed so the
// storage writer, which drives the file writer itself rather than going through write_rows(),
// emits the same metadata.
void add_counter_metadata(format::parquet_file_writer&, const std::vector<cql_column>&);

std::vector<uint8_t> write_rows(const std::vector<cql_column>& cols,
                                const std::vector<row>& rows,
                                folding_level level,
                                writer_options opt = {},
                                exception_encoding = exception_encoding::sparse,
                                const std::map<std::string, format::encoding>&
                                        encoding_overrides = {});

} // namespace sstables::parquet
