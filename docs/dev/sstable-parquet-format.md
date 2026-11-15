# File format of Parquet sstables (format `pq`, version `aa`)

This document specifies the on-disk format of an sstable whose Data file is a
Parquet file. Such an sstable has the format `pq` and the version `aa`. Its
files are named `aa-<generation>-pq-<component>`.

The document describes the format as the code implements it. The sources are
the header comments of `sstables/parquet/`, mainly `mapping.hh`, `writer.hh`,
and `sstable_writer.hh`. The tests that pin the format are
`test/boost/parquet_mapping_test.cc`, `test/boost/parquet_writer_test.cc`, and
`test/boost/parquet_sstable_writer_test.cc`. The user documentation is
`docs/architecture/sstable/sstable-parquet.rst`.

Terms used throughout:

- A *row* is a Parquet row. The *ordinal* of a row is its number in the file,
  counted from 0.
- A *leaf* is a leaf column of the Parquet schema. Its *path* is the list of
  names from the top-level field down to the leaf, joined with dots.
- *Null* means that a row has no value in a leaf. For a list leaf, it means
  that the whole list is null.
- A *fixed-width type* is a CQL type that is stored as `BOOLEAN`, `INT32`,
  `INT64`, `FLOAT`, `DOUBLE`, or `FIXED_LEN_BYTE_ARRAY` (see "Value types").

## 1. Components

Version `aa` has the trie index and the other properties of `mt`, except for
the Data file and its checksums (`sstables/version.hh`).

An `aa` sstable writes these components:

| Component | Content |
|---|---|
| `Data.db` | A Parquet file. See sections 2 and 3. |
| `Partitions.db`, `Rows.db` | The trie index. Positions are row ordinals. See section 4. |
| `Filter.db` | The bloom filter, as for `mt`. It is absent when `bloom_filter_fp_chance` is 1.0. |
| `Statistics.db` | As for `mt`, including the serialization header. Some values use the raw size. See section 6. |
| `Scylla.db` | As for `mt`, plus the `parquet_data_file` entry. See section 5. |
| `Digest.crc32` | The CRC-32 of the whole Data file, as for `mt`. |
| `TOC.txt` | The list of components. |

An `aa` sstable has no `CompressionInfo.db`, no `CRC.db`, no `Index.db`, and
no `Summary.db`. The pages of the Data file carry their own checksums and
compression. The component map of `aa` is the map of `ms` and `mt` without
`CompressionInfo.db` and `CRC.db` (`sstables/sstable_version.cc`).

Encryption at rest wraps the Data file as a whole, as for the other formats.
The Parquet format applies to the plaintext. All sizes and offsets in this
document are plaintext positions.

A Parquet sstable cannot store a table with compact storage. The writer and
the reader both reject such a table.

## 2. The mapping of CQL data to Parquet

### 2.1 Rows

Each row holds one mutation fragment, in sstable order. A partition start has
no row of its own. There is no row for a partition end.

- The partition key and the partition tombstone are stored on the first row of
  the partition. They are null on every other row of the partition.
- The first partition key column is non-null exactly on the rows that start a
  partition. All partition key columns agree on this.
- The static row is stored only if it has at least one cell. If it is stored,
  it is the first row of its partition.
- A partition that has no stored static row, no clustering row, and no range
  tombstone gets one row of kind `none`. This row holds the key, and the
  partition tombstone if there is one. So a row of kind `none` stands for a
  partition with no fragments, with or without a partition tombstone.
- The token is not stored.

### 2.2 Schema layout

The serialization header in `Statistics.db` is the authority for the names and
types of the static and regular columns, and for the types of the key columns.
The writer and the reader both derive the Parquet schema from it
(`table_mapping::from_header`).

The root schema element is named `schema`. Its children are, in this order:

1. The partition key columns, then the clustering key columns, in key order.
2. The value columns of the static columns, then those of the regular columns,
   in the order of the serialization header.
3. The required group `_scylla`.

`_scylla` holds the row-level leaves (section 2.3), followed by the required
group `_scylla.cells`. `_scylla.cells` holds one required subgroup per CQL
column that has cell metadata, under the column's Parquet name. Key columns
come first, then static columns, then regular columns, in the orders above.

A required group adds no definition level. So `_scylla`, `_scylla.cells`, and
their subgroups add no definition level.

All leaves are optional, except where this document says otherwise.

`test_schema_layout` in `parquet_mapping_test.cc` lists every leaf of a table
with every column shape, in schema order.

### 2.3 Row-level leaves

| Leaf | Type | Meaning |
|---|---|---|
| `_scylla.kind` | `INT32` | The fragment kind (section 2.4). Non-null on every row. |
| `_scylla.partition_tomb_ts` | `INT64` | The absolute timestamp of the partition tombstone. |
| `_scylla.partition_tomb_deletion_time` | `INT64` | The deletion time of the partition tombstone. |
| `_scylla.ts` | `INT64` | The row's default timestamp. Null exactly on kind `none`. On a bound row, the range tombstone's timestamp. |
| `_scylla.ttl` | `INT32` | The row's default TTL, in seconds. |
| `_scylla.expiry` | `INT64` | The row's default expiry time. |
| `_scylla.marker_deletion_time` | `INT64` | The deletion time of a dead row marker. |
| `_scylla.tomb_ts` | `INT64` | The row tombstone's timestamp minus `ts`. |
| `_scylla.tomb_deletion_time` | `INT64` | The deletion time of the row tombstone, or of the range tombstone on a bound row. |
| `_scylla.shadowable_tomb_ts` | `INT64` | The shadowable tombstone's timestamp minus `ts`. |
| `_scylla.shadowable_tomb_deletion_time` | `INT64` | The deletion time of the shadowable tombstone. |

Rules for times and tombstones:

- A deletion time or an expiry time is an absolute count of seconds since the
  epoch (`gc_clock`). A TTL is a count of seconds.
- Every timestamp in a row is stored as a difference from `ts`, except `ts`
  itself and `partition_tomb_ts`.
- A difference uses 64-bit wraparound. The writer stores
  `int64(uint64(value) - uint64(ts))`. The reader computes
  `int64(uint64(ts) + uint64(stored))`.
- A null difference means "no difference". The writer never stores a
  difference of zero. The reader treats zero like null.
- A tombstone exists exactly when its deletion time is non-null. A timestamp,
  or a timestamp difference, without its deletion time is corruption.
- `partition_tomb_ts` and `partition_tomb_deletion_time` are both null or both
  non-null. They may be non-null only on the first row of a partition.
- `ttl` and `expiry` are both null or both non-null. When they are non-null,
  `ttl` is not zero.

### 2.4 Fragment kinds

| Value | Kind | Clustering key | Other leaves |
|---|---|---|---|
| 0 | `none` | null | Every row-level leaf except `kind` and the partition tombstone is null. Every cell leaf is null. |
| 1 | static row | null | `marker_deletion_time` and the row tombstone leaves are null. |
| 2 | clustering row without a marker | full | Optional row tombstone and shadowable tombstone. `marker_deletion_time` is null. |
| 3 | clustering row with a marker | full | As kind 2, and the row marker exists (section 2.5). |
| 4 | inclusive start bound | prefix | Range tombstone bound (section 2.6). |
| 5 | exclusive start bound | prefix | Range tombstone bound. |
| 6 | inclusive end bound | prefix | Range tombstone bound. |
| 7 | exclusive end bound | prefix | Range tombstone bound. |

Any other value of `kind`, or a null `kind`, is corruption.

### 2.5 Row defaults and row markers

Static rows and clustering rows have defaults: the timestamp `ts`, and an
optional pair (`ttl`, `expiry`). Cells, collection elements, and collection
tombstones are stored against these defaults (section 2.8).

| Row marker | `kind` | `ts` | `ttl`, `expiry` | `marker_deletion_time` |
|---|---|---|---|---|
| Missing (and every static row) | 2 (1 for a static row) | Chosen by the writer | Chosen by the writer, or null | null |
| Live, does not expire | 3 | The marker's timestamp | null | null |
| Live, expires with TTL t and expiry e | 3 | The marker's timestamp | t, e | null |
| Dead with deletion time d | 3 | The marker's timestamp | Chosen by the writer, or null | d |

For a dead marker, `ttl` and `expiry` serve only as defaults for the cells.

Kind 2 with a non-null `marker_deletion_time` is corruption.

Where the writer chooses the defaults, any choice produces a correct file, and
the reader does not depend on it. The default policy takes the majority
timestamp of the row's cells, elements, and tombstones, and the majority
expiration of its live cells and elements (`mapping_writer.hh`).

The row tombstone is `ts + tomb_ts` with `tomb_deletion_time`. The shadowable
tombstone is `ts + shadowable_tomb_ts` with `shadowable_tomb_deletion_time`.
The writer writes the shadowable leaves only when the row tombstone is
shadowable. The reader applies the rule of `row_tombstone`: the shadowable
tombstone is the shadowable leaves if they are present, and the regular row
tombstone otherwise.

### 2.6 Range tombstones

A range tombstone change is stored as bound rows, one tombstone per row
(`range_tombstone_bounds` in `mapping_writer.hh`):

| Change | Rows |
|---|---|
| Opens a tombstone | One start bound. |
| Closes a tombstone | One end bound. |
| Replaces one tombstone with another (a boundary) | An end bound, then a start bound, at the same prefix. |
| From no tombstone to no tombstone | No row. |

The bound kind of each row follows from the position of the change. A bound
row stores the clustering prefix, `kind` (4 to 7), the tombstone's absolute
timestamp in `ts`, and its deletion time in `tomb_deletion_time`, which is
never null on a bound row. An end bound holds the tombstone that ends. A start
bound holds the tombstone that starts. Every other row-level leaf of a bound row is null, and every cell
leaf is null. A bound row may also be the first row of a partition. It then
holds the partition key and the partition tombstone as well.

### 2.7 Clustering keys

A clustering prefix is stored as nulls in the trailing clustering key leaves.
This is unambiguous, because an empty value is never a null (section 2.10).

- A non-null component after a null one is corruption.
- An empty marker on a null component is corruption.
- Clustering rows have full keys.
- Rows of kind `none` and static rows have no clustering key.

Key columns use the write-time type without reversal.

### 2.8 Cells

Let D be the row's defaults: `ts`, and the optional pair (`ttl`, `expiry`).
Each cell, collection element, and UDT field has these metadata leaves. They
are scalars for a cell or a field. They are lists, with one entry per element,
for a collection.

| Leaf | Type | Meaning |
|---|---|---|
| `ts` | `INT64` | The timestamp minus D.ts. Null means no difference. |
| `ttl` | `INT32` | Null means D.ttl. 0 means "does not expire". |
| `expiry` | `INT64` | Null means D.expiry. |
| `deletion_time` | `INT64` | Non-null exactly when the cell is dead. |
| `empty` | `BOOLEAN` | The value is empty. Present for fixed-width types only. |

The writer encodes each cell by this truth table:

| Cell state | D has no TTL | D has TTL T and expiry E |
|---|---|---|
| Live, does not expire | `ttl` null, `expiry` null | `ttl` = 0, `expiry` null |
| Live, expires with TTL t and expiry e | `ttl` = t, `expiry` = e | `ttl` = t if t differs from T, else null; `expiry` = e if e differs from E, else null |
| Dead with deletion time d | `deletion_time` = d; `ttl`, `expiry`, and the value null | the same |

In every state, `ts` is the cell's timestamp minus D.ts, or null if they are
equal. A live cell always has a value, possibly an empty one. The writer
rejects an expiring cell whose TTL is zero, or whose TTL does not fit in a
signed 32-bit integer.

The reader decodes each cell as follows:

1. The cell exists if its value or its deletion time is non-null.
2. Its timestamp is D.ts plus `ts`.
3. If `deletion_time` is non-null, the cell is dead. Its `ttl`, `expiry`,
   value, and empty marker must be null.
4. Otherwise the cell is live. Let ttl' be `ttl` if it is non-null, and D.ttl
   otherwise. If ttl' is absent or 0, the cell does not expire, and `expiry`
   must be null. Otherwise the cell expires with TTL ttl', and with `expiry` if
   it is non-null, or D.expiry otherwise. D.expiry must then exist.

An atomic column `c` (a native type or a frozen type) has these leaves:

    c                                  the value; null for a dead cell
    _scylla.cells.<c>.ts, .ttl, .expiry, .deletion_time, .empty

### 2.9 Non-frozen collections, UDTs, and counters

Keys, values, elements, and UDT fields must have atomic types (native or
frozen). The mapping rejects any other type, such as a non-frozen collection
inside a collection.

Lists and maps use the three-level shapes of the Parquet `LogicalTypes.md`.
A list is an optional `LIST` group with a repeated group `list` and a leaf
`element`. A map is an optional `MAP` group with a repeated group `key_value`,
a required leaf `key`, and an optional leaf `value`. So the leaf paths end in
`.list.element`, or in `.key_value.key` and `.key_value.value`.

**Element metadata lists.** For a map, a set, or a list column `c`, the
leaves `_scylla.cells.<c>.ts`, `.ttl`, `.expiry`, and `.deletion_time` are
lists of optional elements. A metadata list is either null for the row, or has
exactly one entry per element, in element order.

A null list means that the leaf is null for every element. A null entry means
that the leaf is null for that element. The writer writes a metadata list for
a row only if at least one element has a non-null entry in it. The writer
never writes an empty list, except for a counter (see below). A collection
without elements has a null value list.

The value leaves, by collection type:

| CQL type | Value leaves under `c` | Dead element | Extra metadata lists |
|---|---|---|---|
| `map<K,V>` | `MAP`: required key K, optional value V | The value is null. | `.empty` for values if V is fixed-width; `.key_empty` for keys if K is fixed-width. |
| `set<K>` | `LIST` of required K | Stays in the list. Only its deletion time tells it apart. | `.empty` if K is fixed-width. |
| `list<V>` | `LIST` of optional V | The element is null. | `.empty` if V is fixed-width; `.key` (see below). |

- `_scylla.cells.<c>.key` of a list column is a `LIST` of required
  `FIXED_LEN_BYTE_ARRAY(16)` with the `UUID` annotation. It holds the timeuuid
  cell keys, one per element. It is non-null exactly when the value list is.
- A set element is the cell key itself.
- An empty map key or set element of a fixed-width type stores the placeholder
  and sets its marker in `.key_empty` (map) or `.empty` (set).

**Collection tombstone.** Every non-frozen collection column, including a
UDT, has two scalar leaves: `_scylla.cells.<c>.tomb_ts`, the tombstone's
timestamp minus D.ts, and `_scylla.cells.<c>.tomb_deletion_time`, its deletion
time. The collection cell exists if its value list, or its tombstone deletion time,
is non-null. The writer does not write an empty collection without a
tombstone.

**Non-frozen UDT.** The value of a UDT column `c` is a required group `c`. It
has one optional leaf per field, in field order, under the field's Parquet
name. The group adds no definition level. Each field has the scalar metadata
of an atomic column under `_scylla.cells.<c>.fields.<field>`. The collection
tombstone leaves are `_scylla.cells.<c>.tomb_ts` and `.tomb_deletion_time`.
The reader matches fields by position. A field without a cell is null in all
its leaves.

**Counter.** A counter column `c` has these leaves:

| Leaf | Type | Content |
|---|---|---|
| `c` | `LIST` of required `INT64` | The shard values. |
| `_scylla.cells.<c>.shard_id` | `LIST` of required `FIXED_LEN_BYTE_ARRAY(16)`, `UUID` | The shard ids: the most significant 64 bits, then the least significant 64 bits, each big-endian. |
| `_scylla.cells.<c>.clock` | `LIST` of required `INT64` | The logical clocks. |
| `_scylla.cells.<c>.ts` | `INT64`, scalar | The timestamp minus D.ts. |
| `_scylla.cells.<c>.deletion_time` | `INT64`, scalar | Non-null exactly when the cell is dead. |

The three lists are in shard order, have the same length, and are non-null
exactly together. A live counter cell has non-null lists, which are empty if
the cell has no shards. A dead counter cell has null lists. A counter has no
`ttl` or `expiry` leaf: it never expires, and the row's TTL defaults do not
apply to it. The writer rejects a counter update and an expiring counter cell.

### 2.10 Empty values and placeholders

A CQL value can be empty (zero bytes) in any type. A value is never stored as
a null.

Types stored as `BYTE_ARRAY` store a zero-length value and have no empty
marker. Fixed-width types store a placeholder and set an empty marker. The
placeholder is zero: `false`, 0, 0.0 (all bits zero), or 16 zero bytes. The
reader ignores the stored value when the marker is set. A marker is true or
null. The writer never writes false, and the reader treats false like null.

Where the markers are:

| Value | Marker leaf |
|---|---|
| Key column `k` | `_scylla.cells.<k>.empty` |
| Atomic cell of column `c` | `_scylla.cells.<c>.empty` |
| UDT field `f` of column `c` | `_scylla.cells.<c>.fields.<f>.empty` |
| Map value, set element, list element | `_scylla.cells.<c>.empty`, a list |
| Map key | `_scylla.cells.<c>.key_empty`, a list |

A key column of a `BYTE_ARRAY` type has no `_scylla.cells.<k>` subgroup.

### 2.11 Column names

Value leaves and `_scylla.cells` subgroups use the Parquet name of the CQL
column or UDT field. `parquet_name()` computes it:

- The Parquet name is the CQL name, with three exceptions: a name that starts
  with `_scylla`, a name that is not valid UTF-8, and an empty name.
- An exception is written as `_scylla_x` followed by the lowercase hexadecimal
  form of the name's bytes.

Examples (from `test_parquet_names`):

| CQL name | Parquet name |
|---|---|
| `v` | `v` |
| `x_scylla` | `x_scylla` |
| `_scylla` | `_scylla_x5f7363796c6c61` |
| empty | `_scylla_x` |
| bytes `61 ff` | `_scylla_x61ff` |

The serialization header has no key column names. The writer takes them from
the table schema and applies the same rule. The reader takes the key names
from the first top-level fields of the file's schema, matching by position. It
does not compare them with the current schema, so a renamed key column has no
effect.

### 2.12 Value types

Each value is stored in the type that `value_type_of()` picks for the
write-time type of the column. A reversed type maps like its underlying type.
The reader restores the exact CQL serialized bytes.

| CQL type | Physical type | Logical type / converted type | Stored value |
|---|---|---|---|
| `boolean` | `BOOLEAN` | none | The byte is not 0. Restored as 0 or 1. |
| `tinyint` | `INT32` | `INT(8, signed)` / `INT_8` | The value, sign-extended. |
| `smallint` | `INT32` | `INT(16, signed)` / `INT_16` | The value, sign-extended. |
| `int` | `INT32` | `INT(32, signed)` / `INT_32` | The value. |
| `date` | `INT32` | `DATE` / `DATE` | The unsigned day number minus 2^31, which is the day relative to the epoch. |
| `bigint` | `INT64` | `INT(64, signed)` / `INT_64` | The value. |
| `timestamp`, legacy `DateType` | `INT64` | `TIMESTAMP(UTC, MILLIS)` / `TIMESTAMP_MILLIS` | Milliseconds since the epoch. |
| `time` | `INT64` | `TIME(local, NANOS)` / none | Nanoseconds since midnight. |
| `float` | `FLOAT` | none | The IEEE 754 bits. |
| `double` | `DOUBLE` | none | The IEEE 754 bits. |
| `uuid`, `timeuuid` | `FIXED_LEN_BYTE_ARRAY(16)` | `UUID` / none | The 16 CQL bytes. |
| `ascii`, `text` | `BYTE_ARRAY` | `STRING` / `UTF8` | The CQL bytes. |
| Every other type | `BYTE_ARRAY` | none | The CQL serialized bytes. |

"Every other type" covers `blob`, `varint`, `decimal`, `duration`, `inet`, the
empty type, vectors, and all frozen collections, tuples, and UDTs.

Metadata leaves have no annotation, except the `UUID` annotation of list keys
and counter shard ids.

### 2.13 Absent columns

A leaf that is null in the whole file is absent from the file's schema. A group
without leaves is absent as well. Key columns are the exception: they are
always in the file's schema, because the reader matches them by position. The
reader treats an absent leaf as entirely null.

A leaf counts as non-null from its first entry that is a value, or a null with
a definition level above 0. For example, an empty counter list makes its
leaves present.

For example, take a table with plain inserts, no TTLs, no deletions, and no
empty values, where the cells of each row share one timestamp. Its file has
only these leaves: the key columns, the value columns, `_scylla.kind`, and
`_scylla.ts`.

### 2.14 Key-value metadata

Keys that start with `scylla.` are reserved for Scylla.

| Key | Value | Meaning |
|---|---|---|
| `scylla.layout` | `1` | The cell metadata layout of this format. |
| `scylla.zstd_dictionary...` | not written | Reserved for trained compression dictionaries. |

The writer writes `scylla.layout` as the first key-value entry. The reader
rejects a file where `scylla.layout` is missing or has another value. The
reader ignores other keys.

A key or a value has at most 128 KiB. The writer rejects a larger one. The
reader rejects a file with a larger key, and skips a larger value without
allocating memory for it: it keeps the key without a value. A trained
dictionary that is larger once encoded must be split over several keys
under the `scylla.zstd_dictionary` prefix.

### 2.15 Range of times

The Data file stores every deletion time and expiry time as a signed 64-bit
count of seconds, and every TTL as a signed 32-bit count of seconds. The
mapping does not cap them.

Other components use the rules of `mt`. The partition tombstone in the trie
index and the range tombstone of each row index block go through
`writer_impl::to_deletion_time()`. That function caps a deletion time above
2^31 - 2 at 2^31 - 2, and counts the cap. `Statistics.db` stores 32-bit local
deletion times, which the metadata collector caps the same way. So for a
deletion time above 2^31 - 2, the index and the Data file hold different
values.

The Data file is the authority. The reader never takes a tombstone from the
index whose deletion time is 2^31 - 2. It reads that tombstone from the Data
file instead: after an index skip whose open range tombstone has such a
deletion time, it does not skip, and reads on from where it is. Validation
compares a partition tombstone of the index with the tombstone of the Data
file after applying the cap.

## 3. Physical layout of the Data file

### 3.1 File structure

    PAR1
    row group 0: in-place chunks in schema order, then their OffsetIndexes in the same order
    row group 1: the same
    ...
    late chunks (entirely null, without an OffsetIndex)
    FileMetaData (the footer)
    footer length (4 bytes, little-endian)
    PAR1

A *column chunk* (chunk) holds the pages of one leaf in one row group. A chunk
is *in place* if it lies in its row group's region. A leaf's chunks are in
place from the first row group in which it is non-null (section 2.13) to the
last row group. The earlier chunks of the leaf are *late*.

### 3.2 Layout invariants

A reader may rely on these invariants (`writer.hh`). The reader checks them
when it parses the footer (`file_summary.hh`) and when it decodes an
OffsetIndex block (`page_decoder.hh`). It rejects a file that breaks them.

- The first in-place chunk of row group 0 starts after the 4 magic bytes. The
  first in-place chunk of any other row group starts where the OffsetIndex
  block of the previous row group ends.
- The in-place chunks of a row group are adjacent and in schema order. Each one
  starts at the end of the previous one's last data page.
- The OffsetIndex block of a row group follows its last in-place chunk
  directly. It holds one OffsetIndex per in-place chunk, in the same order,
  with no gaps.
- The late chunks are adjacent, in schema order and then in row group order.
  They start at the end of the last row group's OffsetIndex block and end at
  the footer.
- A key column that is null in the whole file has only late chunks.
- All chunks of a leaf use the same compression codec.
- Every chunk has at least one data page.
- Every chunk records its null count in the footer: the entries with a
  definition level below the maximum.
- The footer has no minimum, maximum, or sort order. There is no ColumnIndex.

### 3.3 Row groups

- The writer buffers a whole row group in memory and writes it at the end.
- A row group ends only at a page set boundary (section 3.4). So it holds at
  least one page set.
- A row group ends at the first page set boundary at which the buffered size
  reaches the row group target. The buffered size is the size of the encoded
  pages, plus the raw size of the values not yet encoded.
- The row group target is the smaller of `parquet_row_group_size_in_kb`
  (default 8 MiB) and the writer's share of the per-shard writer memory
  budget. The budget is `parquet_writer_memory_fraction` (default 0.05) of the
  shard's memory. The share is the budget divided by the number of active
  Parquet writers on the shard, and is never below the page set size.

The sizes are writer policies. The reader handles any values.

### 3.4 Page sets and page cutting

A *page set* is a range of rows at whose boundaries every leaf may start a new
page. Every page starts at a page set boundary. A leaf may skip a boundary, so
one page may span several page sets.

The writer ends a page set at the end of a row when one of these holds:

- The raw size appended since the previous boundary reaches the page set size,
  `parquet_page_set_size_in_kb` (default 128 KiB).
- The page set has 16384 rows (`max_page_set_rows`).
- A leaf's pending page reached 65536 entries (`max_page_values`).

At a boundary, each leaf that received an entry since the previous boundary
ends its pending page if one of these holds:

- The page's size reaches `parquet_min_page_size_in_kb` (default 8 KiB). The
  size is the PLAIN size of its values plus the size of its levels.
- The page has at least 32768 entries (half of `max_page_values`).

A leaf that received no entry keeps its page open. A long run of nulls is
split at page set boundaries, so that a page does not exceed
`max_page_values` entries by more than the rows of one page set. A row of a
repeated leaf can have any number of entries, because a page holds whole rows.

The end of a row group ends a page set and every page.

### 3.5 Data pages

Every data page is a Data Page V2:

- The repetition levels, then the definition levels, then the values.
- The levels use the RLE/bit-packed hybrid encoding, with the bit width of the
  leaf's maximum level. They are never compressed. A leaf with a maximum level
  of 0 has no levels of that kind.
- `is_compressed` is false when compression did not make the values smaller,
  or when the page has no values. The values are then stored as they are.
- The header carries a CRC: the CRC-32 (`crc32_utils`) of all page bytes after
  the header, that is the levels and the stored values.
- `num_values` counts all entries, nulls included. `num_rows` counts the rows
  of the page.
- A page holds whole values, so a large value makes a large page. A page above
  2 GiB is an internal error of the writer.
- The uncompressed size in a page header is at most the
  `total_uncompressed_size` of its chunk. The reader rejects a page that
  declares more than the largest chunk of its row group, before it
  allocates anything for the page.
- When `is_compressed` is false, or the compression codec is
  `UNCOMPRESSED`, the uncompressed size equals the compressed size.
  Otherwise the uncompressed size is larger. The reader rejects a page
  header that breaks this.
- A page has value bytes exactly when it has values (`num_nulls` is below
  `num_values`). A page without values is `PLAIN`. A leaf with a maximum
  definition level of 0 has no nulls. The reader rejects a page header
  that breaks this, or whose encoding is not in the table of section 3.6.

Page headers have no checksum. Checksum validation checks each header as
the reader does. It also checks that the headers of each row group add up
to the totals of the footer: the sum of `total_uncompressed_size` (headers
included), of `num_values`, and of the null counts of the row group's
chunks. So damage to most header fields is detected without the digest.
The length of the levels and the choice of encoding can change to another
valid value. Only the digest detects that.

### 3.6 Encodings

The writer chooses an encoding per page, among these:

| Physical type | Data page encodings |
|---|---|
| `BOOLEAN` | `PLAIN`, `RLE` |
| `INT32`, `INT64` | `PLAIN`, `DELTA_BINARY_PACKED`, `RLE_DICTIONARY` |
| `FLOAT`, `DOUBLE` | `PLAIN`, `RLE_DICTIONARY` |
| `BYTE_ARRAY` | `PLAIN`, `DELTA_LENGTH_BYTE_ARRAY`, `RLE_DICTIONARY` |
| `FIXED_LEN_BYTE_ARRAY` | `PLAIN`, `RLE_DICTIONARY` |

`DELTA_BINARY_PACKED` uses blocks of 128 values in 4 miniblocks. A page
without values (all null) is marked `PLAIN`.

How the writer chooses:

- When a page ends, the writer computes the size of each candidate encoding.
  The size is exact for `PLAIN` and the delta encodings, and estimated for the
  RLE encodings. Sizes are compared before compression. The smallest wins.
- The size of a dictionary-encoded page is the size of its indices, plus the
  growth of the dictionary page since the previous page. So each dictionary
  entry is charged once, to the first page that uses it.
- A chunk starts with a dictionary. It falls back to the best other encoding
  for the rest of the chunk at the first page where the dictionary does not
  win, or when the dictionary page would exceed
  `parquet_dictionary_page_size_limit_in_kb` (default 4 KiB). A limit of 0
  disables dictionaries. `BOOLEAN` never uses a dictionary.
- A page that ends after a fallback uses the other encoding as a whole.

Dictionary pages:

- A chunk has a dictionary page if at least one of its pages uses the
  dictionary. The dictionary page then holds every entry added before the
  fallback, including entries that no page used.
- The dictionary page is the first page of its chunk and starts at the chunk's
  start. The first data page follows it directly.
- The dictionary page is `PLAIN`. It carries a CRC of the stored bytes.
- A dictionary page holds at least one entry. For a fixed-width physical
  type, its uncompressed size is the entry count times the width. For
  `BYTE_ARRAY`, it is at least 4 bytes per entry. The reader rejects a
  dictionary page header that breaks this.
- A dictionary page has no flag for an uncompressed body. So it is always
  compressed with the file's compression codec, unless the codec is
  `UNCOMPRESSED`.

The `encodings` list of a chunk holds every encoding of its data pages, `RLE`
if the leaf has levels, and `PLAIN` if the chunk has a dictionary page.

### 3.7 Late chunks

A late chunk covers the rows of its row group, in pages of at most 65536 rows.
Every entry is at definition level 0, and the pages have no values. A late
chunk has no dictionary page and no OffsetIndex. Its `encodings` list is
`PLAIN` and `RLE`, and its null count equals its row count.

### 3.8 Compression

The compression codec follows the table's `compression` option
(`make_writer_config` in `sstable_writer.cc`):

| Table compression | Parquet compression codec |
|---|---|
| none | `UNCOMPRESSED` |
| `LZ4Compressor`, `LZ4WithDictsCompressor` | `LZ4_RAW` |
| `SnappyCompressor` | `SNAPPY` |
| `DeflateCompressor` | `GZIP`, at zlib's default level |
| `ZstdCompressor`, `ZstdWithDictsCompressor` | `ZSTD`, at the table's compression level |

- The variants with dictionaries use the same codec without a dictionary.
- `chunk_length_in_kb` has no effect.
- All leaves use the same codec.
- `ZSTD` frames have a window of 2^17 bytes and no content checksum. The
  writer compresses with the parameters that zstd picks for an input of
  2^17 bytes at the level, whatever the page size. The reader rejects a
  frame with a larger window as malformed.
- The reader supports all five codecs.

### 3.9 OffsetIndex

Each in-place chunk has one OffsetIndex. It has one page location per data
page, not for the dictionary page:

- `offset`: the file position of the page header.
- `compressed_page_size`: the page size on disk, header included.
- `first_row_index`: the first row of the page, relative to the row group.

The reader finds a page by row ordinal through the OffsetIndex. It derives a
chunk's start, and so the extent of its dictionary page, from the layout
invariants.

### 3.10 Footer

The footer is a Thrift compact protocol `FileMetaData`. Its `version` is 2,
and the reader accepts 1 and 2. Its `schema` is the file's schema (section
2.2) without the absent leaves. `num_rows` is the total row count.
`key_value_metadata` is described in section 2.14. `created_by` is `scylla`.
There are no column orders.

- Each `RowGroup` has `columns`, `total_byte_size` (the sum of the chunks'
  uncompressed sizes), `num_rows`, `file_offset` (the start of the first
  in-place chunk), `total_compressed_size`, and `ordinal` (only when the
  index is at most 32767).
- Each `ColumnChunk` has `file_offset` set to 0, and `offset_index_offset`
  and `offset_index_length` for an in-place chunk.
- Each `ColumnMetaData` has `type`, `encodings`, `path_in_schema`, `codec`,
  `num_values`, the total sizes (page headers and dictionary page included),
  `data_page_offset`, `dictionary_page_offset` if there is a dictionary page,
  and `statistics` with `null_count` only.

The writer serializes the footer in pieces. It computes the CRC-32
(`crc32_utils`) of the serialized `FileMetaData` and stores it with the
footer's size in `Scylla.db` (section 5). The CRC covers neither the 4-byte
footer length nor the trailing magic bytes. The reader verifies it while it
parses the footer, when the sstable is opened.

## 4. The index

The index is the trie index of `ms` and `mt` (`Partitions.db` and `Rows.db`).
Its writers and its reader treat positions as opaque integers. In an `aa`
sstable, every position is a row ordinal, not a byte offset
(`sstable_writer.hh`).

- **Partition position.** A partition's position is the ordinal of its first
  row. That is the static row if it is stored, else the first clustering row
  or bound row, else the row of kind `none`. Partitions without a row index
  store it in `Partitions.db` as for `mt`.
- **Block offset.** A row index block's offset is the ordinal of its first row
  minus the partition's position. The first block starts at the partition's
  first clustering row or bound row, after the static row. So its offset is 1
  when the partition has a stored static row, and 0 otherwise.
- **Partition end.** The partition end in the row index (the final separator)
  is the ordinal one past the partition's last row.
- **End of file.** The total row count takes the place of the Data file size.
  The index reader receives it as `data_end_position`, and returns it when a
  bound is at the end of the file. `data_file_positions()` is a half-open
  range of ordinals.

Row index blocks:

- A block ends where a page set ends, so every block starts at a page set
  boundary.
- There is no auto-scaling. Every page set boundary inside a partition ends a
  block, subject to the rule below.
- The static row is in no block. A page set that ends at the static row does
  not end a block.
- A partition gets a row index only if it has at least two blocks.

The same-position rule. The trie separates two blocks by a key that is larger
than the last entry of the first block and not larger than the first entry of
the second. So two consecutive rows at the same position stay in one block,
even if a page set ends between them. Two rows have the same position when
their clustering prefixes are equal and their bound weights are equal
(`begin_entry` in `sstable_writer.cc`). This happens between the end bound row
and the start bound row of a boundary, and between range tombstone changes at
one position. Such a page set boundary ends no block. The block ends at the
next page set boundary instead.

Each block records the range tombstone that is open before its first row, as
for `ms` and `mt`. A start bound row opens its tombstone, and an end bound row
closes it.

## 5. The `Scylla.db` entry

`Scylla.db` of an `aa` sstable has the subcomponent `parquet_data_file`
(tag 15, `scylla_metadata_type::ParquetDataFile`). It is present exactly in
sstables with a Parquet Data file. `sstable-scylla-format.md` gives its
serialization.

| Field | Type | Content |
|---|---|---|
| `rows` | be64 | The number of rows in the Data file. It is the end-of-file position of the index. |
| `raw_size` | be64 | The total raw size of the rows (section 6). |
| `file_size` | be64 | The size of the Data file. For an encrypted sstable, the size of the plaintext. |
| `footer_size` | be64 | The size of the serialized `FileMetaData`. |
| `footer_crc32` | be32 | The CRC-32 (`crc32_utils`) of the serialized `FileMetaData`. |

When the sstable is opened, the reader checks that the entry exists, that the
Data file has `file_size` bytes, that the footer's CRC-32 is `footer_crc32`,
and that the footer's row count is `rows`. A mismatch raises the malformed
sstable error.

The per-component digests in `Scylla.db` include the Data file, as for `mt`.

## 6. Size measures

Byte offsets in the Data file do not measure the data, so the format uses two
other measures.

**Raw size.** The raw size of a row is the sum of the raw sizes of the values
appended to its leaves. A value's raw size is its width for a fixed-width
physical type (1 for a `BOOLEAN`), or its length for a byte array.

Nulls count 0. Placeholders and empty markers count like other values. The
raw size covers value leaves and metadata leaves, so the first row of a
partition includes the partition key and the partition tombstone. It does not
depend on the encoding or the compression.

The raw size is used for:

| Use | Measure |
|---|---|
| Large partition detection, and the partition size histogram in `Statistics.db` | The raw size of the partition's rows. |
| Large row detection | The raw size of the row. |
| Write progress (`writer_offset_tracker`) | The raw size written so far. |
| `sstable::data_size()` | `raw_size` from `Scylla.db`. So compaction strategies, the compaction backlog, and the tablet size use it. |
| Compression ratio in `Statistics.db` | The file size divided by the total raw size, for a table with compression. It stays unset (-1) without compression, as for `mt`. |

Large cell detection uses the size of the value: the CQL value of an atomic
cell (0 for a dead cell), or the sum of the keys and values of a collection's
elements. `mt` measures the serialized cell instead, which adds a few bytes of
cell header. So a cell whose value is exactly at the threshold is a large
cell in `mt`, and not in `aa`.

**On-disk estimate.** The maximum sstable size and the split of a large
partition use an estimate of the file size (`writer::estimated_size()`). It is
the bytes written so far, plus the encoded pages of the current row group,
plus the raw size not yet encoded times the ratio of encoded bytes to raw size
so far. The ratio is 1 before the first page. The footer, dictionaries,
OffsetIndexes, and late chunks are not counted. The error is less than one
row group.

- The writer ends the sstable after a partition when the estimate reaches the
  maximum sstable size.
- The writer splits a partition after a clustering row when the partition has
  at least two row index blocks and the estimate exceeds the maximum sstable
  size plus 10%.

**Read progress.** The read position is `data_size()` times the ordinal of
the next row, divided by the total row count. A reverse read counts the rows
that it consumed or skipped, so its progress grows too.

**Key estimate.** `estimated_keys_for_range()` advances the index to the token
range and takes the half-open ordinal range [start, end) from
`data_file_positions()`. An open end is the total row count. The estimate is
`ceil((end - start) / rows * estimated key count)`. It is 0 when the range is
empty, or when the token range does not overlap the sstable.

**Statistics.** `Statistics.db` counts rows as `mt` does for the same data. A
partition of a table with static columns counts one static row, even when
the Data file has no row for it. A range tombstone boundary counts as one row
and one range tombstone.
