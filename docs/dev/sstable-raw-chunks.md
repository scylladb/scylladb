# Raw chunks in compressed SSTables

## Overview

The `min_compression_saving_percent` cluster config option (1-99, 0 or unset
means off) makes the writer store a `Data.db` chunk uncompressed ("raw") when
compressing it saves less than that percentage of its size. It targets data
that doesn't compress, such as encrypted or already-compressed values: such
chunks cost compressor CPU on every flush and compaction, and decompressor CPU
plus a buffer allocation on every read, for no space gain. Cassandra has a
similar feature (CASSANDRA-10520) with the same length rule; here the threshold
is kept in a Scylla-specific CompressionInfo option.

## On-disk format

There is no per-chunk flag and no new sstable format version. The rule is:

```
max_compressed_len = ceil(chunk_len * (100 - percent) / 100)
```

A chunk whose stored length (excluding the 4-byte checksum) is at least
`max_compressed_len` is raw; anything shorter is compressed. A compressed
chunk is only kept when it is shorter than `max_compressed_len`, so the two
can't be confused.

A raw chunk normally holds exactly `chunk_len` bytes. The last chunk of the
file may be shorter; if it is stored raw, it is zero-padded up to
`max_compressed_len` so the rule still identifies it, and the reader trims it
using the uncompressed data length. A raw chunk of any other length is
rejected as malformed.

Checksums and the digest cover the stored bytes exactly as for compressed
chunks, so checksum validation, file-based streaming and backup need no
changes.

## Where the setting lives

`min_compression_saving_percent` is a native cluster config option (see
`docs/dev/cluster_config.md`) at table, keyspace and cluster scope, not a
`compression` sub-option. The writer resolves it once per sstable through
`sstables_manager::min_compression_saving_percent()`, which `main` plugs into
the user sstables manager; unplugged managers (system tables, `scylla sstable`,
most tests) use 0. The resolved value is written to the options map in
`CompressionInfo.db`. On load, the threshold is derived from the sstable's own
value, so a change only affects sstables written afterwards; `upgradesstables`
rewrites existing ones with the current setting.

The on-disk option is not passed to `compression_parameters`:
`options_from_compression()` skips it and `compression` reads it directly.

## Compatibility

Older versions build `compression_parameters` from the on-disk options, don't
know this one and refuse to open such sstables (unknown compression option)
rather than misread raw chunks as compressed. The option can be set only once
every node advertises its `CLUSTER_CONFIG_MIN_COMPRESSION_SAVING_PERCENT`
feature, i.e. all nodes are upgraded; after that, an older node can't join.
Downgrading after sstables were written with the option requires rewriting
them without it.

## Writer: skipping hopeless compression

Trying to compress every chunk of incompressible data would keep most of the
CPU cost. The writer therefore tracks a streak of raw chunks
(`raw_chunk_heuristic` in `sstables/compress.hh`):

- After 2 consecutive chunks that didn't compress enough, following chunks are
  stored raw without trying.
- Every 16th chunk during such a streak is compressed anyway as a probe, so a
  switch to compressible data is noticed.
- After a partition of at least one chunk ends, the first chunk starting at or
  after that boundary is also probed, since a new large partition may hold
  different data. Smaller partitions don't trigger a probe, so a streak still
  forms when many of them share a chunk.

A probe that compresses well enough ends the streak. A short last chunk is
always compressed, since storing it raw would pad it.

## Reader

A raw chunk is returned in place, as a slice of the read buffer, instead of
being decompressed into a separately allocated buffer. This saves the
decompression and an allocation and copy per chunk. The returned slice keeps
the whole read buffer alive, so a reader may pin one more read-ahead buffer
than before.

The slice is charged to the read's permit with `request_memory()`, exactly like
a decompressed chunk. On local disk the tracked file already charges the read
buffer, so this double-counts by up to one chunk; on object storage the read
buffer isn't charged at all, and a chunk that straddles two read-ahead buffers
is assembled by `read_exactly()` into an untracked buffer, so the charge is
needed there.
