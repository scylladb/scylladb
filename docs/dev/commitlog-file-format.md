ScyllaDB commitlog segment file format
======================================

**Note:** Commitlog file formats are subject to change between scylla versions. Users should not make assumptions about nor rely on them.
Commitlog files should *never* be used across ScyllaDB updates. This information is provided mainly for ScyllaDB contributors.

File descriptor structure
-------------------------

ScyllaDB commitlog segment files are named with a versioned, time-indexed scheme, as

```
        <Prefix><version>-<id>.log
```

Where `<Prefix>` is application specific, but typically "Commitlog-", or in case of 
files being recycled "Recycled-Commitlog-", `<version>` is the file format version,
and `<id>` is the id part of a replay position (timestamp + shard).

Segment file data structure
---------------------------

All control data is written in network byte order.

The file consists of a file header, followed by any number of chunks. Each chunk has 
its own header + a marker to the start of next chunk, to allow skipping it more easily,
should any data corruption be present in the chunk's data.

Chunks contain data entries, with a small header, stored data + checksums to verify its integrity.

An entry can be a "multi-entry", i.e. several entries written as one.

Version 2
---------

(Format used in ScyllaDB 1.0 to as of this writing - named '2' because it is a slight deviation on the format used in cassandra)

```

        Segment file header

        magic           : uint32_t - ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';
        version         : uint32_t - same as descriptor
        id              : uint64_t - same as descriptor
        crc             : uint32_t - CRC32 of version, low 32 of id, high 32 of id. 

        Chunk header

        file_pos        : uint32_t - the file position of next chunk
        crc             : uint32_t - CRC32 of low 32 of segment id, high 32 of id and file offset of end of this header.

        Entry

        size            : uint32_t - size of entry (data + full headers). Must be smaller than MAX_UINT32.
        crc1            : uint32_t - CRC32 of size
        data            : bytes - actual entry data
        crc2            : uint32_t - CRC32 of size, data

        Multi-entry

        magic           : multi marker - 0xffffffff (MAX_UINT32)
        size            : size of all entries in this multi-entry + headers
        crc             : CRC32 of magic, size
        <entries> * N
        crc2            : CRC32 of magic, size and data in each entry
```

Version 3
---------

Modified from v2 to improve error detection/false positive elimination.
This version does CRC per written disk block instead of actual entry.
Every block is also tagged with which file is being written, this to
be able to better distinguish new data from that left over from recycling
(reusing old files) and actual disk/file corruption.

```

        Segment file header

        magic           : uint32_t - ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';
        version         : uint32_t - same as descriptor
        id              : uint64_t - same as descriptor
        alignment       : uint32_t - disk block size
        crc             : uint32_t - CRC32 of version, low 32 of id, high 32 of id, alignment. 

        Chunk header

        file_pos        : uint32_t - the file position of next chunk
        crc             : uint32_t - CRC32 of low 32 of segment id, high 32 of id and file offset of end of this header.

        Entry

        size            : uint32_t - size of entry (data + full headers). Must be smaller than MAX_UINT32.
        crc             : uint32_t - CRC32 of size
        data            : bytes - actual entry data

        Multi-entry

        magic           : multi marker - 0xffffffff (MAX_UINT32)
        size            : size of all entries in this multi-entry + headers
        crc             : CRC32 of magic, size
        <entries> * N

        Disk block (block size = `alignment`)

        0 - <bs - 12>   : Interleaved file data, i.e. the content above
        <bs - 12>       : uint64_t - same as descriptor.
        <bs - 4>        : uint32_t - CRC32 of block data up until crc (bs - 4), including segment id

The main benefit of the tagged and CRC:ed block is that if the CRC is broken, we know the part
of this file is corrupt. If the CRC is correct, but segment ID does not match, we can assume
the file is not fully written/prematurely ended. Both cases can mean data loss, depending on 
how writing is done as well as OS and hardware.

```

Version 4
---------

Modified from v3 to allow fragmented data entries, i.e. writing a single data entry
as a stream across several segments.
A fragmented entry is written by splitting data into sub-parts that will fit into
the normal restrictions of a write (i.e. smaller than max mutation size, but also
trying to fit into existing buffers as best we can to avoid wasting alignment slack).

```
        Fragmented entry

        magic           : fragmented marker - 0xfffffffe (MAX_UINT32-1)
        size            : size of this fragmented entry part + headers
        id              : the stream id
        offset          : offset of this entry in the data stream
        remaining       : stream data remaining to write after this entry 
        crc             : CRC32 of magic, size, id, offset and remaining

```

Each stream has a unique id (monotonic counter). We must handle reading and 
assembling streams out of order and interleaved because we can't always 
guarantee order replaying will be performed in.

The replayer needs to be called with a state storage (replay_state) to handle
fragmented entries. When encountering one, we store the data into the state
buffer for the id, and once we have all fragments (as defined by id, offset and
remaining), we can report the full entry back to caller.

Manifest
--------

The data commitlog and the schema commitlog each keep a manifest: the set of segment
files that may hold data. Hints commitlogs keep none. A segment listed in the manifest
but missing from disk was removed outside the commitlog, so its data is lost. A node
that finds such a segment refuses to start.

### Layout

```
commitlog/
  CommitLog-4-<id>.log              segments
  Recycled-CommitLog-4-<id>.log     recycled segments, never listed
  CommitLog-manifest-<gen>/         one directory per manifest generation
      0                             shard 0's manifest
      1                             shard 1's manifest
      ...
      record                        written last
  schema/
      SchemaLog-4-<id>.log
      SchemaLog-manifest-<gen>/0, record
```

A per-shard file is JSON, `{"version":1,"shard":k,"segments":["CommitLog-4-123.log", ...]}`,
with sorted basenames. `record` is `{"version":1,"shard_count":N}`. Every file is written
atomically: write `<name>.tmp`, flush, close, rename, sync the directory. Readers ignore
`*.tmp`. Older binaries skip the generation directories, because segment listing only
looks at regular files.

### Invariants

- A segment name is added to the manifest, and the manifest is written, before the
  segment can hold data. A new file is listed only after the commitlog directory is
  synced.
- A segment name is removed from the manifest, and the manifest is written, before the
  file is renamed to `Recycled-*` or unlinked. If that write fails, the files stay on
  disk and listed, and the next disposal pass retries.
- A shard writes only its own file and lists only segments it allocated.

Unlisted segments on disk hold no data, or only data that is already flushed. They are
replayed with a warning.

### Generations and record

Each start that enables manifests creates a new generation, numbered above every
generation found on disk. Every shard writes its file, then shard 0 writes `record`
with the shard count. A generation with `record` is sealed and must contain a file for
every shard. A generation without `record` is an activation that did not finish: its
names are expected on disk, but the generation is not checked for completeness.

After the commitlog is replayed and flushed, and before the replayed segments are
deleted, shard 0 removes the old generations. It removes `record` first, so a partially
removed generation reads as unsealed. Changing the shard count needs nothing extra: the
old generations are dropped and the new one is written by the current shards.

Once both manifests are sealed, the node sets `commitlog_manifest` in
`system.scylla_local` and flushes the table, so the marker survives a wiped
commitlog directory.

### When the node refuses to start

- A listed segment is missing from disk.
- A sealed generation has no file for one of its shards.
- A manifest file cannot be parsed, has an unknown version, or `record` has a shard
  count outside [1, 1024].
- The `commitlog_manifest` marker is set, but the data or schema commitlog directory
  has no manifest file at all. An empty generation directory, as left by
  `find commitlog -type f -delete`, does not count. A new, empty `commitlog_directory`
  trips the same check; start once with `unsafe_ignore_commitlog_manifest`.

Manifests are created only once the `COMMITLOG_MANIFEST` cluster feature is enabled. A
node that already has manifests keeps maintaining them.

The `unsafe_ignore_commitlog_manifest` option turns every refusal into an error log
line. The data in the missing segments is lost. A node serving strongly consistent
tables may have acknowledged writes it no longer holds, so replace such a node instead.
The next start drops the old generations and writes a new one, so remove the option
after the first start.

### Crash windows

| crash point | next start sees | outcome |
|---|---|---|
| after file create or recycle rename, before the name is added | unlisted file, no data | warn, replay (skipped by header check) |
| after the removal is written, before rename or unlink | unlisted file, flushed data only | warn, replay, harmless |
| unlink failed | same as above | same |
| during activation, before `record` | unsealed generation | names expected, not completeness-checked |
| after `record`, before the marker | sealed generation, no marker | verified; activation repeats, marker written |
| while dropping old generations, before the segments are deleted | unsealed or absent old generation, old segments present | replayed again, as without a manifest |
| while deleting replayed segments | unlisted old segments | replayed again, as without a manifest |
| file removed by an operator | listed, absent | refuse |
| per-shard manifest removed | sealed generation incomplete | refuse |
| `rm -rf commitlog/*` | marker, no generation | refuse |
| `find commitlog -type f -delete` | marker, empty generation directories | refuse |
| `record` removed together with a per-shard file | unsealed generation | not detected (known limit) |
