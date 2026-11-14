/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The mapping between CQL data and Parquet columns: the part that the
// writer and the reader share.
//
// The mapping turns mutation fragments into Parquet rows and back. It knows
// nothing about pages or encodings. This file describes the columns of a
// file (table_mapping) and the conversion of single values. The writer is
// in mapping_writer.hh.
//
// The rest of this comment is the specification of the format. It is
// written for the reader: it states how to decode every column.
//
//
// Columns
// =======
//
// The serialization header in Statistics.db is the authority for the
// names and the types of the static and regular columns, and for the types
// of the key columns. Both sides derive the Parquet schema from it with
// table_mapping. Each CQL name is turned into a Parquet name by
// parquet_name(): the CQL name itself, unless it starts with `_scylla`, is
// not valid UTF-8, or is empty. Such a name becomes `_scylla_x` followed by
// the lowercase hexadecimal form of its bytes.
//
// The top-level fields of the Parquet schema are, in order:
//
// - The partition key columns, then the clustering key columns, in key
//   order. They are always in the file's schema, even when they are null
//   in the whole file. The reader matches them by position. Their names
//   come from the table schema at write time, and the reader does not
//   compare them with the current schema.
// - The value columns of the static columns, then those of the regular
//   columns, in the order of the serialization header.
// - The reserved group `_scylla`. It is required, so it adds no definition level.
//
// `_scylla` holds the row-level columns (see "Row-level columns"), then the
// required group `_scylla.cells`. That group holds one required subgroup per
// CQL column that has cell metadata, under the column's Parquet name: key
// columns first, then static, then regular columns, in the same orders as above.
//
// Any column that is null in the whole file, except a key column, is
// absent from the file's schema. The reader treats an absent column as
// entirely null. A group without columns is absent as well.
//
// All leaves are optional unless stated otherwise. In this comment, "null"
// means that the row has no value in the leaf (or, for a list, that the
// whole list is null).
//
//
// Value types
// ===========
//
// Every CQL value is stored in the type that value_type_of() picks for the
// write-time type of the column, with reversed types replaced by their
// underlying type. The reader restores the exact CQL serialized bytes.
//
//   CQL type            Parquet type           Annotation (logical / converted)    Stored value
//   ------------------  ---------------------  ----------------------------------  ---------------------------------------
//   boolean             BOOLEAN                -                                   byte != 0; restored as 0 or 1
//   tinyint             INT32                  INT(8, signed) / INT_8              the value, sign-extended
//   smallint            INT32                  INT(16, signed) / INT_16            the value, sign-extended
//   int                 INT32                  INT(32, signed) / INT_32            the value
//   date                INT32                  DATE / DATE                         the unsigned day number minus 2^31,
//                                                                                  which is the day relative to the epoch
//   bigint              INT64                  INT(64, signed) / INT_64            the value
//   timestamp           INT64                  TIMESTAMP(UTC, MILLIS) /            milliseconds since the epoch
//   (and the legacy                            TIMESTAMP_MILLIS
//   DateType)
//   time                INT64                  TIME(local, NANOS) / -              nanoseconds since midnight
//   float               FLOAT                  -                                   the IEEE 754 bits
//   double              DOUBLE                 -                                   the IEEE 754 bits
//   uuid, timeuuid      FIXED_LEN_BYTE_ARRAY   UUID / -                            the 16 CQL bytes (big-endian,
//                       (16)                                                       as UUID requires)
//   ascii, text         BYTE_ARRAY             STRING / UTF8                       the CQL bytes
//   everything else     BYTE_ARRAY             -                                   the CQL serialized bytes
//
// "Everything else" covers blob, varint, decimal, duration, inet, the empty
// type, vectors, and all frozen collections, tuples, and UDTs. A varint is
// not annotated as DECIMAL, because DECIMAL requires a fixed precision. A
// duration is not annotated as INTERVAL, which is unsigned and has another
// layout.
//
// Empty values. A CQL value can be empty (zero bytes) in any type. A value
// is never stored as a null. For the types stored as BOOLEAN, INT32, INT64,
// FLOAT, DOUBLE, or FIXED_LEN_BYTE_ARRAY, an empty value stores a
// placeholder and sets a boolean empty marker. The placeholder is zero:
// false, 0, 0.0 (all bits zero), or 16 zero bytes. The reader ignores the
// stored value when the marker is set. A marker is true or null; the writer
// never writes false, and the reader treats false like null. Types stored
// as BYTE_ARRAY have no marker, since a zero-length value is natural there.
//
// Where the empty markers are:
//
// - Key column k: `_scylla.cells.<k>.empty`.
// - Cell values and collection elements: see "Cells".
//
//
// Row-level columns
// =================
//
// Every Parquet row is one mutation fragment in sstable order, except that
// a partition start has no row of its own. Row-level columns are leaves of
// `_scylla`:
//
//   Leaf                            Type    Meaning
//   ------------------------------  ------  ------------------------------------------------------------
//   kind                            INT32   fragment_kind; non-null on every row
//   partition_tomb_ts               INT64   absolute timestamp of the partition tombstone
//   partition_tomb_deletion_time    INT64   deletion time of the partition tombstone
//   ts                              INT64   the row's default timestamp; null exactly on kind "none"
//   ttl                             INT32   the row's default TTL, in seconds
//   expiry                          INT64   the row's default expiry time, in seconds since the epoch
//   marker_deletion_time            INT64   deletion time of a dead row marker
//   tomb_ts                         INT64   row tombstone timestamp minus `ts`
//   tomb_deletion_time              INT64   deletion time of the row tombstone or the range tombstone
//   shadowable_tomb_ts              INT64   shadowable tombstone timestamp minus `ts`
//   shadowable_tomb_deletion_time   INT64   deletion time of the shadowable tombstone
//
// Times. A deletion time or an expiry time is an absolute count of seconds
// since the epoch (gc_clock), in every column. A TTL is a count of seconds.
// A timestamp is a difference from `ts`, except `ts` itself and
// `partition_tomb_ts`. A difference is computed and applied with 64-bit
// wraparound: stored = int64(uint64(value) - uint64(ts)), and value =
// int64(uint64(ts) + uint64(stored)). A null difference means "no
// difference". The writer never stores a difference of zero, and the reader
// treats zero like null.
//
// A tombstone exists exactly when its deletion time is non-null. A
// timestamp difference or a timestamp without its deletion time is
// corruption.
//
// The partition. The partition key and the partition tombstone are on the
// partition's first row, and null on its other rows. The first partition key
// column is non-null exactly on the rows that start a partition. A
// partition whose first fragment would otherwise be missing, because it has
// no static row with content, no clustering row, and no range tombstone,
// gets one row of kind "none". That row holds the key and the partition
// tombstone if there is one. A row of kind "none" therefore stands for a
// partition with no fragments, with or without a partition tombstone.
// The static row comes first if it has content; a static row without cells
// has no row. There is no row for the partition end.
//
// Kinds (fragment_kind):
//
// - none (0): see above. Every other row-level column, every clustering
//   key column, and every cell column is null. `ts` is null.
// - static row (1): `ts`, `ttl`, and `expiry` are the row's defaults for its
//   cells. The writer picks them freely; the reader does not depend on how.
//   Clustering key columns, `marker_deletion_time`, and the row tombstone
//   columns are null.
// - clustering row without a marker (2): like a static row, with the
//   clustering key and an optional row tombstone and shadowable tombstone.
// - clustering row with a marker (3): the row marker exists. Its timestamp
//   is `ts`. If `marker_deletion_time` is non-null, the marker is dead with
//   that deletion time, and `ttl` and `expiry` serve only as defaults for
//   the cells. Otherwise the marker is live, and it expires with `ttl` and
//   `expiry` if they are non-null.
// - inclusive start (4), exclusive start (5), inclusive end (6), exclusive
//   end (7): a range tombstone bound at the clustering prefix. `ts` and
//   `tomb_deletion_time` hold the range tombstone: an end bound holds the
//   tombstone that ends, a start bound holds the one that starts. Every
//   other row-level column of the row is null, and so is every cell column.
//   A boundary between two adjacent range tombstones is stored as an end
//   bound row followed by a start bound row at the same prefix.
//
// `ttl` and `expiry` are both null or both non-null. When non-null, `ttl`
// is not zero. A row tombstone's timestamp is `ts` plus `tomb_ts`, and a
// shadowable tombstone's is `ts` plus `shadowable_tomb_ts`. The shadowable
// tombstone of a clustering row is the shadowable tombstone column if it is
// present, and the regular row tombstone otherwise (row_tombstone's rule).
//
// Clustering keys. A clustering prefix is stored as nulls in the trailing
// clustering key columns. A non-null component after a null one is
// corruption. Clustering rows have full keys.
//
//
// Cells
// =====
//
// Defaults. Let D be the row's defaults: the timestamp `ts`, and the pair
// (`ttl`, `expiry`), which may be absent. A cell, a collection element, or a
// UDT field has these metadata leaves, scalar for a cell or a field, and
// lists with one entry per element for a collection:
//
//   ts             INT64    timestamp minus D.ts; null means no difference
//   ttl            INT32    null means "D.ttl"; 0 means "does not expire"
//   expiry         INT64    null means "D.expiry"
//   deletion_time  INT64    non-null exactly when the cell is dead
//   empty          BOOLEAN  the value is empty (fixed-width types only)
//
// Writer, per cell (the truth table):
//
//   Cell state                     D has no TTL           D has (T, E)
//   -----------------------------  ---------------------  -------------------------------------
//   live, does not expire          ttl, expiry null       ttl = 0, expiry null
//   live, expires with (t, e)      ttl = t, expiry = e    ttl = (t == T ? null : t),
//                                                         expiry = (e == E ? null : e)
//   dead with deletion time d      deletion_time = d; ttl and expiry null; no value
//
// In every state, `ts` is the cell's timestamp minus D.ts, or null if they
// are equal. A live cell always has a value, possibly empty.
//
// Reader, per cell:
//
// - The cell exists if its value or its deletion time is non-null.
//   (Collections and counters: see below.)
// - Timestamp: D.ts plus `ts`.
// - If `deletion_time` is non-null, the cell is dead. Its `ttl`, `expiry`,
//   value, and empty marker must be null.
// - Otherwise the cell is live. Let ttl' be `ttl` if non-null, else D.ttl
//   (possibly absent). If ttl' is absent or 0, the cell does not expire, and
//   `expiry` must be null. Otherwise the cell expires with TTL ttl' and the
//   expiry `expiry` if non-null, else D.expiry, which must exist.
// - An expiring cell has a non-zero TTL. The writer rejects an expiring
//   cell with a TTL of zero, which it could not represent.
//
// Atomic column c (a native type or a frozen type):
//
//   c                                    the value, typed as above; null for a dead cell
//   _scylla.cells.<c>.ts, .ttl, .expiry, .deletion_time, .empty
//
// Non-frozen collection c. The value is a LIST or a MAP under c. Each
// metadata leaf of the elements is a LIST of optional entries, which is
// either null for the row or has exactly one entry per element, in element
// order. A null list means that the leaf is null for every element. A null
// entry means the same for one element. The writer never writes an empty
// list: a collection without elements has a null value list.
//
//   map<K,V>   c: MAP, required key K, optional value V.
//              A dead entry has a null value.
//              .key_empty: empty markers of the keys (if K is fixed-width).
//              .empty: empty markers of the values (if V is fixed-width).
//   set<K>     c: LIST of required K. A dead element differs from a live one
//              only by its deletion time.
//              .empty: empty markers of the elements (if K is fixed-width).
//   list<V>    c: LIST of optional V. A dead element is a null.
//              .key: LIST of required FIXED_LEN_BYTE_ARRAY(16) UUID, the
//              timeuuid cell keys, one per element. Non-null exactly when c is.
//              .empty: empty markers of the values (if V is fixed-width).
//
// Every non-frozen collection also has `_scylla.cells.<c>.ts`, `.ttl`,
// `.expiry`, `.deletion_time` as element lists, and the scalar collection
// tombstone leaves `.tomb_ts` (timestamp minus D.ts) and
// `.tomb_deletion_time`. The collection cell exists if its value list or
// its tombstone deletion time is non-null. An empty collection without a
// tombstone is not written.
//
// Non-frozen UDT c. The value is a required group c with one optional leaf
// per field, in field order, under the field's Parquet name. The reader
// matches fields by position. Each field has the metadata of an atomic
// column under `_scylla.cells.<c>.fields.<field>`. The collection tombstone
// leaves are `_scylla.cells.<c>.tomb_ts` and `.tomb_deletion_time`.
//
// Counter column c. The value is a LIST of required INT64 under c: the
// shard values. `_scylla.cells.<c>.shard_id` (LIST of required
// FIXED_LEN_BYTE_ARRAY(16) UUID) and `.clock` (LIST of required INT64) hold
// the shard ids and the logical clocks, in shard order, and are non-null
// exactly when c is. `.ts` and `.deletion_time` are scalar, as for an atomic
// cell. A counter never expires: it has no `ttl` or `expiry` leaves, and the
// row's TTL defaults do not apply to it. A live counter cell has a non-null
// value list, which is empty if the cell has no shards. A dead counter cell
// has a null value list.
//
//
// Key-value metadata
// ==================
//
// Keys that start with `scylla.` are reserved for Scylla. The file records
// its cell metadata layout under `scylla.layout`, and the reader rejects
// any value other than the one it knows ("1" for this layout). Keys under
// `scylla.zstd_dictionary` are reserved for trained compression
// dictionaries. A key or a value has at most
// max_key_value_size bytes (128 KiB, metadata.hh), so a dictionary that is
// larger once encoded must be split over several keys under the prefix.

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "sstables/parquet/metadata.hh"
#include "sstables/parquet/schema.hh"
#include "types/types.hh"
#include "utils/chunked_vector.hh"
#include "utils/managed_bytes.hh"

class schema;

namespace sstables {
struct serialization_header;
}

namespace sstables::parquet {

class writer;

/// The name of the reserved group, and the prefix that parquet_name() escapes.
inline constexpr std::string_view reserved_name = "_scylla";
/// The prefix of escaped names, followed by the hexadecimal form of the CQL name.
inline constexpr std::string_view escaped_name_prefix = "_scylla_x";

/// The reserved prefix of key-value metadata keys.
inline constexpr std::string_view metadata_key_prefix = "scylla.";
/// The key of the cell metadata layout, and the layout of this format.
inline constexpr std::string_view layout_key = "scylla.layout";
inline constexpr std::string_view layout_v1 = "1";
/// Reserved for trained compression dictionaries.
inline constexpr std::string_view zstd_dictionary_key_prefix = "scylla.zstd_dictionary";

/// The key-value metadata that every file carries.
std::vector<key_value> file_key_value_metadata();

/// The Parquet name of a CQL column or UDT field name. See the top of this file.
sstring parquet_name(bytes_view cql_name);

/// The value of the row-level `kind` column.
enum class fragment_kind : int32_t {
    none = 0,
    static_row = 1,
    clustering_row = 2,
    clustering_row_with_marker = 3,
    incl_start = 4,
    excl_start = 5,
    incl_end = 6,
    excl_end = 7,
};

/// How a CQL value is stored in a leaf. See the table at the top of this file.
enum class value_type : uint8_t {
    boolean,
    tinyint,
    smallint,
    int32,
    date,
    bigint,
    timestamp,
    time,
    float_,
    double_,
    uuid,
    string,
    bytes,
};

/// The value type for CQL type `t`. Reversed types map like their
/// underlying type. Throws std::invalid_argument if `t` is not atomic.
value_type value_type_of(const abstract_type& t);
physical_type physical_type_of(value_type t) noexcept;
/// The leaf options (type length and annotations) of a value column.
column_options column_options_of(value_type t);
/// The width of the CQL serialized form of a non-empty value, for the types
/// that have an empty marker. Unset for byte arrays.
std::optional<uint32_t> cql_fixed_width(value_type t) noexcept;
/// Whether a column of this type has an empty marker.
inline bool has_empty_marker(value_type t) noexcept {
    return physical_type_of(t) != physical_type::byte_array;
}

/// Appends the CQL serialized value `v` of type `t` to leaf `c` at levels
/// `l`. An empty value of a type with an empty marker appends the zero
/// placeholder and returns true; the caller then sets the marker.
/// A non-empty value of the wrong width is an internal error.
bool append_value(writer& w, column_id c, levels l, value_type t, managed_bytes_view v);

// The inverse of append_value(), for the reader: the CQL serialized form of
// a stored value. The stored type must match physical_type_of(t). Values
// stored as byte arrays are the CQL bytes themselves.
bytes to_cql_value(value_type t, bool v);
bytes to_cql_value(value_type t, int32_t v);
bytes to_cql_value(value_type t, int64_t v);
bytes to_cql_value(value_type t, float v);
bytes to_cql_value(value_type t, double v);

/// The column ids of the metadata leaves of a cell, a UDT field, or the
/// elements of a collection. For a collection they are list leaves.
struct cell_metadata_columns {
    column_id ts;
    /// Absent for counters.
    std::optional<column_id> ttl;
    std::optional<column_id> expiry;
    column_id deletion_time;
    /// Present for fixed-width values only.
    std::optional<column_id> empty;
};

/// The shape of a static or regular column.
enum class column_shape : uint8_t {
    /// A native or frozen type: one value per cell.
    atomic,
    map,
    set,
    list,
    udt,
    counter,
};

/// A field of a non-frozen UDT.
struct field_columns {
    bytes name;
    sstring parquet_name;
    value_type type;
    column_id value;
    cell_metadata_columns meta;
};

/// The leaves of a static or regular column.
struct cell_columns {
    bytes name;
    sstring parquet_name;
    /// The write-time type, from the serialization header.
    data_type type;
    column_shape shape;
    /// The type of the atomic value, of a map's values, or of the elements
    /// of a set or a list. Unused for UDTs and counters.
    value_type values = value_type::bytes;
    /// The type of a map's keys.
    value_type keys = value_type::bytes;
    /// The value leaf of an atomic column, the value leaf of a map, the
    /// element leaf of a set or a list, the shard value leaf of a counter.
    /// Unused for UDTs.
    column_id value = 0;
    /// The key leaf of a map.
    std::optional<column_id> map_key;
    /// The metadata of the cell (atomic, counter) or of the elements
    /// (collections, as lists). Unused for UDTs.
    cell_metadata_columns meta{};
    /// The empty markers of a map's keys.
    std::optional<column_id> key_empty;
    /// The timeuuid keys of a list.
    std::optional<column_id> list_key;
    /// The collection tombstone, for collections and UDTs.
    std::optional<column_id> tomb_ts;
    std::optional<column_id> tomb_deletion_time;
    /// The shard ids and logical clocks of a counter.
    std::optional<column_id> shard_id;
    std::optional<column_id> clock;
    /// The fields of a UDT, in field order.
    std::vector<field_columns> fields;
};

/// The leaves of a key column.
struct key_columns {
    sstring parquet_name;
    /// The write-time type, without reversal.
    data_type type;
    value_type values;
    column_id value;
    std::optional<column_id> empty;
};

/// The row-level leaves.
struct row_columns {
    column_id kind;
    column_id partition_tomb_ts;
    column_id partition_tomb_deletion_time;
    column_id ts;
    column_id ttl;
    column_id expiry;
    column_id marker_deletion_time;
    column_id tomb_ts;
    column_id tomb_deletion_time;
    column_id shadowable_tomb_ts;
    column_id shadowable_tomb_deletion_time;
};

/// The Parquet columns of an sstable: the schema tree, and the leaves of
/// each CQL column.
///
/// The writer passes schema() to the file writer and appends to the leaves
/// by their column ids. The reader computes the same mapping from the
/// sstable's serialization header, finds the leaves in the file by their
/// paths (schema().path(id)), and checks the file's schema against
/// schema().to_elements() for the leaves that the file has.
class table_mapping {
    schema _schema;
    row_columns _row{};
    utils::chunked_vector<key_columns> _partition_key;
    utils::chunked_vector<key_columns> _clustering_key;
    utils::chunked_vector<cell_columns> _static;
    utils::chunked_vector<cell_columns> _regular;
    node_id _reserved = 0;
    node_id _cells = 0;

    table_mapping() = default;
    key_columns add_key_value(const sstring& name, data_type type);
    void add_key_metadata(key_columns& k);
    void add_value(cell_columns& c);
    void add_metadata(cell_columns& c);
    cell_columns make_cell_columns(bytes_view name, data_type type);
public:
    /// Computes the mapping from the serialization header.
    ///
    /// `key_names` holds the Parquet names of the partition key columns,
    /// then of the clustering key columns. The header has no key names. The
    /// writer computes them from the table schema with key_parquet_names().
    /// The reader takes them from the file's schema, matching by position.
    /// Throws std::invalid_argument if their number does not match the
    /// header, if a type in the header does not parse, or if a type cannot
    /// be stored (such as a non-frozen collection inside a collection).
    static table_mapping from_header(const serialization_header& h, std::span<const sstring> key_names);

    const schema& get_schema() const noexcept { return _schema; }
    const row_columns& row() const noexcept { return _row; }
    const utils::chunked_vector<key_columns>& partition_key() const noexcept { return _partition_key; }
    const utils::chunked_vector<key_columns>& clustering_key() const noexcept { return _clustering_key; }
    const utils::chunked_vector<cell_columns>& static_columns() const noexcept { return _static; }
    const utils::chunked_vector<cell_columns>& regular_columns() const noexcept { return _regular; }
};

/// The Parquet names of the key columns of `s`: the partition key columns,
/// then the clustering key columns.
std::vector<sstring> key_parquet_names(const ::schema& s);

/// Whether the Parquet name of every column of `s`, and of every field of
/// its non-frozen UDTs, is at most max_name_size bytes long. The Parquet
/// schema rejects longer names, so a table for which this is false cannot
/// be written as Parquet.
bool parquet_names_fit(const ::schema& s) noexcept;

/// The types of the partition key columns in a serialization header, which
/// stores one type for a single-column key and a CompositeType otherwise.
std::vector<data_type> partition_key_types(const serialization_header& h);

} // namespace sstables::parquet
