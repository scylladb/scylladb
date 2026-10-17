# Raw chunks in compressed SSTables

## Overview

The `min_compression_saving_percent` compression option (1-99, 0 or absent
means off) makes the writer store a `Data.db` chunk uncompressed ("raw") when
compressing it saves less than that percentage of its size. It targets data
that doesn't compress, such as encrypted or already-compressed values: such
chunks cost compressor CPU on every flush and compaction, and decompressor CPU
plus a buffer allocation on every read, for no space gain. Cassandra has the
same feature (CASSANDRA-10520) and the on-disk encoding follows it.

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

The writer adds `min_compression_saving_percent` to the options map in
`CompressionInfo.db`. On load, the threshold is derived from the sstable's own
value, not from the table schema, so `ALTER TABLE` only affects sstables
written afterwards; `upgradesstables` rewrites existing ones with the current
setting.

## Compatibility

Older versions don't know the option and refuse to open such sstables
(unknown compression option) rather than misread raw chunks as compressed. To
keep mixed-version clusters safe, setting the option requires the
`SSTABLE_RAW_CHUNKS` cluster feature, i.e. all nodes must be upgraded first.
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

A probe that compresses well enough ends the streak.

## Reader

A raw chunk is returned in place, as a slice of the read buffer, instead of
being decompressed into a separately allocated, permit-accounted buffer. This
saves the decompression and two allocations per chunk. The trade-off is that
the returned slice keeps the whole read buffer alive, so a reader may pin one
more read-ahead buffer than before. That buffer is charged to the read's permit
by the tracked file, so no extra accounting is done for the slice.

One exception: a chunk that straddles two read-ahead buffers is assembled by
`read_exactly()` into a fresh buffer of at most one chunk, which is not charged
to the permit (the decompressing path charges its output via
`request_memory()`).
