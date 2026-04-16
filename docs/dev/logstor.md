# Logstor

## Introduction

Logstor is a log-structured storage engine for ScyllaDB optimized for key-value workloads. It provides an alternative storage backend for key-value tables - tables with a partition key only, with no clustering columns.

Unlike the traditional LSM-tree based storage, logstor uses a log-structured approach with in-memory indexing, making it particularly suitable for workloads with frequent overwrites and point lookups.

## Architecture

Logstor consists of several key components:

### Components

#### Primary Index

The primary index is entirely in memory and it maps a partition key to its location in the log segments. It consists of a B-tree per each table that is ordered token.

#### Segment Manager

The `segment_manager` handles the allocation and management of fixed-size segments (default 128KB). Segments are grouped into large files (default 32MB). Key responsibilities include:

- **Segment allocation**: Provides segments for writing new data
- **Space reclamation**: Tracks free space in each segment
- **Compaction**: Copies live data from sparse segments to reclaim space
- **Recovery**: Scans segments on startup to rebuild the index
- **Separator**: Rewrites segments that have records from different compaction groups into new segments that are separated by compaction group.

The data in the segments consists of records of type `log_record`. Each record contains the value for some key as a `canonical_mutation` and additional metadata.

The `segment_manager` receives new writes via a `write_buffer` and writes them sequentially to the active segment with 4k-block alignment.

#### Write Buffer

The `write_buffer` manages a buffer of log records and handles the serialization of the records including headers and alignment. It can be used to write multiple records to the buffer and then write the buffer to the segment manager.

The `buffered_writer` manages multiple write buffers for user writes, an active buffer and multiple flushing ones, to batch writes and manage backpressure.

### Data Flow

**Write Path:**
1. Application writes mutation to logstor
2. Mutation is converted to a log record
3. Record is written to write buffer
4. The buffer is switched and written to the active segment.
5. Index is updated with new record locations
6. Old record locations (for overwrites) are marked as free

**Read Path:**
1. Application requests data for a partition key
2. Index lookup returns record location
3. Segment manager reads record from disk
4. Record is deserialized into a mutation and returned

**Separator:**
1. When a record is written to the active segment, it is also written to its compaction group's separator buffer. The separator buffer holds a reference to the original segment.
2. The separator buffer is flushed when it's full, or requested to flush for other reason. It is written into a new segment in the compaction group, and it updates the location of the records from the original mixed segments to the new segments in the compaction group.
3. After the separator buffer is flushed and all records from the original segment are moved, it releases the reference of the segment. When there are no more reference to the segment it is freed.

**Compaction:**
1. The amount of live data is tracked for each segment in its segment_descriptor. The segment descriptors are stored in a histogram by live data.
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

## On-Disk Format

### Files

Segments are stored in large pre-allocated files on disk. Each file holds a fixed number of segments determined by the `file_size / segment_size` ratio. File names follow the pattern:

```
ls_{shard_id}-{file_id}-Data.db
```

Files are pre-formatted (zero-filled) before use.

### Segments

Each segment is a contiguous fixed-size region within a file (default 128KB). A segment is identified by a `log_segment_id` (a 32-bit integer index), which maps to a file and offset within that file.

A segment contains one or more **buffers** written sequentially. Each buffer is 4KB-block-aligned. A segment has one of two kinds:

- **mixed**: written by normal user writes; may contain records from multiple tables and compaction groups. Contains multiple buffers.
- **full**: written by compaction or the separator; contains records from a single table and token range. Contains exactly one buffer.

### Buffer Layout

A buffer within a segment has the following layout:

```
buffer_header
(segment_header)?       -- present only when kind == full
record_1
record_2
...
record_n
zero_padding            -- to align the entire buffer to block_alignment (4096 bytes)
```

buffer_header, segment_header, and records are aligned by `record_alignment` (8 bytes).

#### Buffer Header

A serialized form of `write_buffer::buffer_header`.

| Offset | Size | Field       | Description |
|--------|------|-------------|-------------|
| 0      | 4    | `magic`     | `0x4C475342` ("LGSB"). Used to detect valid buffers during recovery. |
| 4      | 4    | `data_size` | Size in bytes of all record data following the header(s). |
| 8      | 2    | `seg_gen`   | Segment generation number. Incremented each time the segment slot is reused. Used during recovery to discard stale data. |
| 10     | 1    | `kind`      | Segment kind: `0` = mixed, `1` = full. |
| 11     | 1    | `version`   | Version of the write buffer format. |
| 12     | 4    | `crc`       | CRC32 of all other fields in the buffer header. Used for validating the header. |

#### Segment Header (full segments only)

Immediately follows the buffer header when `kind == full`.

A serialized form of `write_buffer::segment_header`.

| Offset | Size | Field         | Description |
|--------|------|---------------|-------------|
| 16     | 16   | `table`       | UUID of the table this segment belongs to. |
| 32     | 8    | `first_token` | Minimum token of all records in the segment (raw token number). |
| 40     | 8    | `last_token`  | Maximum token of all records in the segment (raw token number). |

#### Records

Each record within the buffer is structured as:

```
record_header (4 bytes)
record_data   (variable)
zero_padding  -- to align to record_alignment (8 bytes)
```

**Record Header:**

| Offset | Size | Field       | Description |
|--------|------|-------------|-------------|
| 0      | 4    | `data_size` | Size in bytes of the serialized `log_record` that follows. |

**Record Data:**

The record data is the serialized form of a `log_record`, which contains:
- `key`: the partition key (`primary_index_key`), including a `decorated_key` with a token and partition key bytes.
- `generation`: a 16-bit write generation number, used during recovery to resolve conflicts when the same key appears in multiple segments.
- `mut`: the full partition value as a `canonical_mutation`.
