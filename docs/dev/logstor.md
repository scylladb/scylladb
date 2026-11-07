# Logstor

## Introduction

Logstor is a log-structured storage engine for ScyllaDB optimized for key-value workloads. It provides an alternative storage backend for key-value tables - tables with a partition key only, with no clustering columns, and where rows are written whole, as a single value (individual
columns are usually not updated separately).

Unlike the traditional LSM-tree based storage, logstor uses a log-structured approach with in-memory indexing, making it particularly suitable for workloads with frequent overwrites and point lookups.

## Architecture

Logstor consists of several key components:

### Components

#### Primary Index

The primary index is entirely in memory and it maps a partition key to its location in the log segments. It consists of a B-tree per each table that is ordered by token.

An index entry does not hold the partition key itself but a `primary_index_key`: the token and a 128-bit XXH3 hash of the key's internal representation, seeded with a value drawn at random when the process starts. This keeps every entry the same small size regardless of the key length. Keys that share a token are ordered by their hash rather than by ring order; the range reader restores ring order after reading the records, which carry the full key. The hash exists only in memory: records store the partition key, and recovery recomputes the hash from it, so the seed is never persisted. A read verifies the key found in the record against the requested key, so a hash collision surfaces as an error rather than as another partition's data.

#### Segment Manager

The `segment_manager` handles the allocation and management of fixed-size segments (default 128KB). Segments are grouped into large files (default 32MB). Key responsibilities include:

- **Segment allocation**: Provides segments for writing new data
- **Space reclamation**: Tracks free space in each segment
- **Compaction**: Copies live data from sparse segments to reclaim space
- **Recovery**: Scans segments on startup to rebuild the index
- **Separator**: Writes to all _compaction groups_ (tablets and even tables) go to a single active segment. The separator splits these mixed segments, which have records from different compaction groups, into segments that each has a single compaction group. This separation is useful when migrating tablets.

The data in the segments consists of records of type `log_record`. Each record contains the value for some key as an encoded partition (`record_value`) and additional metadata. On disk a record is stored as a **record frame**, described under [Record Frames](#record-frames) below, which is also the unit the index points at.

The `segment_manager` receives new writes via a `write_buffer`, and appends each buffer to the active segment as a 4k-block-aligned chunk (see [Chunks](#chunks)).

#### Write Buffer

The `write_buffer` manages a buffer of log records and handles the serialization of the records including headers and alignment. It can be used to write multiple records to the buffer, then seal it into a chunk and write the chunk to a segment through the segment manager.

The `buffered_writer` manages multiple write buffers for user writes, an active buffer and multiple flushing ones, to batch writes and manage backpressure.

### Data Flow

**Write Path:**
1. Application writes mutation to logstor
2. Mutation is converted to a log record
3. Record is written to write buffer
4. The buffer is switched, sealed into a chunk, and the chunk is appended to the active segment.
5. Index is updated with new record locations
6. Old record locations (for overwrites) are marked as free

Only step 4 writes to the disk - all other steps only update in-memory metadata.

**Read Path:**
1. Application requests data for a partition key
2. Index lookup returns the cached partition, if the cache has it, or the record location
3. Segment manager reads record from disk
4. Record is decoded into a partition, which is added to the cache and returned

**Separator:**
1. When a record is written to the active segment, it is also written to its compaction group's separator buffer. The separator buffer holds a reference to the original segment.
2. The separator buffer is flushed when it's full, or requested to flush for other reason. It is written into a new segment in the compaction group, and it updates the location of the records from the original mixed segments to the new segments in the compaction group.
3. After the separator buffer is flushed and all records from the original segment are moved, it releases the reference of the segment. When there are no more reference to the segment it is freed.

**Compaction:**
1. The amount of live data is tracked for each segment in its (in-memory) segment_descriptor. The segment descriptors are stored in a histogram by live data. This histogram is called a `segment_set` and each compaction group owns one.
2. A segment set from a single compaction group is submitted for compaction.
3. Compaction picks segments for compaction from the segment set. It chooses segments with the lowest utilization such that compacting them results in net gain of free segments.
4. It reads the segments, finding all live records, and writing them into a write buffer. When the buffer is full it is flushed into a new segment, and for each recording updating the index location to the new location.
5. After all live records are rewritten the old segments are freed.

## Usage

### Enabling Logstor

To use logstor, enable the experimental feature in the configuration:

```yaml
experimental_features:
  - logstor
```

### Creating Tables

Tables using logstor must have no clustering columns, and created with the `storage_engine` property equals to 'logstor':

```cql
CREATE TABLE keyspace.user_profiles (
    user_id uuid PRIMARY KEY,
    name text,
    email text,
    metadata frozen<map<text, text>>
) WITH storage_engine = 'logstor';
```

### Basic Operations

**Insert/Update:**

```cql
INSERT INTO keyspace.table_name (pk, v) VALUES (1, 'value1');
INSERT INTO keyspace.table_name (pk, v) VALUES (2, 'value2');

-- Overwrite with new value
INSERT INTO keyspace.table_name (pk, v) VALUES (1, 'updated_value');
```

Currently, updates must write the full row. Updating individual columns is not yet supported. Each write replaces the entire partition.

**Select:**

```cql
SELECT * FROM keyspace.table_name WHERE pk = 1;
-- Returns: (1, 'updated_value')

SELECT pk, v FROM keyspace.table_name WHERE pk = 2;
-- Returns: (2, 'value2')

SELECT * FROM keyspace.table_name;
-- Returns: (1, 'updated_value'), (2, 'value2')
```

**Delete:**

```cql
DELETE FROM keyspace.table_name WHERE pk = 1;
```

### Inspecting a Table

`SELECT ... FROM MUTATION_FRAGMENTS()` dumps the raw mutations of a logstor table as the
node they are read on stores them, bypassing the storage proxy. A logstor table has two
mutation sources:

| Source | Content |
|--------|---------|
| `logstor-cache` | The partition as held in the logstor cache, if it is cached |
| `logstor-log:${file path}:${segment}` | The log record, named after the segment holding it |

```cql
SELECT * FROM MUTATION_FRAGMENTS(keyspace.table_name) WHERE pk = 1;
```

## Measuring Performance

Two tests measure the cost of a logstor operation on one node, at two levels.

**`scylla perf-simple-query --logstor`** measures the whole read and write path - CQL, the
coordinator, the replica and logstor - against a logstor table. It is the same test that measures an
sstable backed table, so the baseline is the same command without the flag. Use it for the cost of
an operation as a workload sees it, and for the two storage engines side by side.

**`test/perf/perf_logstor`** drives a logstor directly - no CQL, no coordinator, no replica - and
measures the steps of a read and of a write both together and one at a time, so that a change to the
hot path can be attributed to the step it moved. Its tests, its options, how its steps add up and
what a run has to get right are in the comment at the top of
[test/perf/perf_logstor.cc](../../test/perf/perf_logstor.cc).

## On-Disk Format

This section describes the parts of the format and the purpose of each part. See these files for
the exact layout, the field types and the encodings:

- [replica/logstor/ondisk.hh](../../replica/logstor/ondisk.hh) for the headers and the record
  frame.
- the `raw_write_buffer` comment in
  [replica/logstor/write_buffer.hh](../../replica/logstor/write_buffer.hh) for the layout of a
  chunk.

### Files

Each shard keeps its segments in large files (default 32MB), named
`ls_{shard_id}-{file_id}-Data.db`. A file holds a fixed number of segments, and logstor
pre-formats the file (fills it with zeros) before use.

### Segments

A segment is a fixed-size region of a file (default 128KB), and it is the unit of space that
logstor allocates, compacts and frees. A `log_segment_id` identifies a segment, and the id is an
index that maps to a file and to an offset in that file. A segment holds one or more chunks, and it
has one of two kinds:

- A **mixed** segment holds the records of user writes, from any table and compaction group. Each
  flushed write buffer appends one chunk to the segment.
- A **full** segment holds the records that compaction or the separator writes, from one table and
  token range, in a single chunk.

### Chunks

A chunk is the unit that logstor writes to a segment. A write buffer builds the chunk in memory,
and the chunk starts at a 4KB block boundary and is padded to a multiple of 4KB. A chunk holds
these parts, in this order:

- the **chunk header**, which identifies a valid chunk of a given format version, gives the segment
  kind and the sequence number of the segment, and holds the sizes and the CRCs of the parts that
  follow. Recovery uses the sequence number to order segments. After its first chunk, a segment can
  still hold the chunks of an earlier use of the segment, and recovery identifies such a chunk by
  its sequence number before recovery checks the version and the flags.
- the **segment header**, in a full segment only, which gives the table and the token range of the
  records.
- the **record frames**, one after the other.
- zero padding to the block boundary.

The record frames are the body of the chunk. The body CRC lets a reader find a chunk whose header
is durable but whose body was only partly written.

### Record Frames

A record frame holds one record and has these parts, in this order:

- the **frame header**, which gives the sizes of the parts that follow and the flags of the record.
- the **record header**, which holds the token, the timestamp, the table and the partition key.
- the **record value**, which is the encoded partition. The value also holds the schema version
  that the record was written under and the column mapping of that version, so that logstor can
  decode the record after an `ALTER TABLE`.
- zero padding to a multiple of `ondisk::record_alignment`.

The value of a record is encoded and decoded by `encode_record_value()` and `decode_record_value()`
in [replica/logstor/record_value.hh](../../replica/logstor/record_value.hh). All other code copies
the value as bytes, for example compaction, the separator and segment streaming.

For each record, the index holds a `record_location`, which points at the start of the record frame
and gives the frame size, so one read at that location returns the whole record. The position of a
chunk is a `segment_position`, which holds the segment and the offset but no size. To get the
location of a record, logstor adds the offset of the record frame in the chunk to the
`segment_position` of the chunk.

The sizes of the parts of a frame are named consistently throughout the code, since several of
them are otherwise easy to confuse:

| Name | Bytes |
|------|-------|
| `header_size`  | The serialized `record_header`: its fixed part plus `key_size`. |
| `value_size`   | The serialized `record_value`. Stored in the frame header. |
| `record_size`  | `header_size + value_size`, the record without its frame header. This is what `max_record_size()` bounds. |
| `frame_size`   | `record_frame_header_size + record_size`, the frame without its padding. This is what `record_location::size` holds. |
| `records_size` | All the frames of one chunk, their padding included. Stored in the chunk header. |
| `record_bytes` | The frame sizes of a set of records summed, their padding excluded. What a `segment_descriptor` and the `live_record_bytes` metric count. |
