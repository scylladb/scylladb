# Shared-dictionary compression for SSTables

## Overview

Scylla now supports dictionary-based compression for SSTables, which improves
compression ratios by sharing compression dictionaries across compression
chunks.

## Background

Traditional SSTable compression in Scylla works on a chunk-by-chunk basis. Each
chunk is compressed independently, which means patterns that occur across chunks
cannot be effectively leveraged for better compression.

Dictionary-based compression addresses this limitation by training a dictionary
on representative data samples and using it across all compression chunks,
providing the compression algorithm with additional context for referencing.

## How it works

1. **Dictionary training**: Scylla samples data chunks from across the cluster
to build an optimized compression dictionary for a specific table.

2. **Dictionary distribution**: Dictionaries are stored in the `system.dicts`
table (managed by group0). Each table has its own (possibly absent) row there.

3. **Shared Compression**: When opening an SSTable for writing, if the table
has compression dictionaries enabled, the current
recommended dictionary for a table (i.e. the one in `system.dicts`)
is used to compress the data, and is written into the header of
`CompressionInfo.db`.

4. **Decompression**: When opening an SSTable for reading, the dictionary blob
is loaded from `CompressionInfo.db` and used to decompress the data.

## Implementation details

### New persistent data structures

There are two new persistent data structures involved:
- An extension to the SSTable format. `CompressionInfo.db` gains two new
  compressor IDs (lz4 with dicts, zstd with dicts) and new "compressor options"
  which store the dictionary blob used by this SSTable.
- An extension to `system.dicts`, which (in addition to the RPC compression
  dict) now also stores the current recommended SSTable compression dict
  for each table.

### SSTable format extension

The *structure* of the format isn't affected. Instead, we add two new compressor
identifiers (`LZ4WithDictsCompressor` and `ZstdWithDictsCompressor`), which
use the "compressor options" map in CompressionInfo.db to store the dict.

Since the structure isn't affected, we don't increment the SSTable version for
this. Naturally, the dict-compressed SSTables won't be readable by older
versions of Scylla (or by Cassandra), but they should complain about an unknown
compressor rather than consider the SSTable malformed.

If a downgrade is necessary, it can be done by disabling dictionaries
(through schema, or by setting `sstable_compression_dictionaries_enable_writing`
to `false` on all nodes) and rewriting the SSTables
(with `nodetool upgradesstables -a` or similar).

The extension is hidden behind the `SSTABLE_COMPRESSION_DICTS` cluster feature.

#### New entries in CompressionInfo.db

We store the dictionary blob in the "options" map in the header of
`CompressionInfo.db`, under the keys `.dictionary.00000000`,
`.dictionary.00000001`, ...

(It's split into several parts, because the "options" have 16-bit lengths,
and dictionaries are usually bigger than that).

### `system.dicts` extension

If a `system.dicts` partition with key `sstables/{table_uuid}` exists,
it provides the current recommended dict for this table, which is used
to compress new SSTables.

If a table doesn't have a matching row in `system.dicts`, then there's no
current dictionary for this table, and new SSTables should fall back to
dictionaryless compression.

### Compressor factory

With "traditional" compression, a compressor was just a function in the code,
not involving any data. This meant that the creation of compressors was
cheap and easy.

But with dictionaries involved, each unique compressor has its own RAM and cache
footprint. Therefore we want to deduplicate compressors as much as possible.

For this, we create new compressors through a central "compressor factory"
which contacts other shards and ensures that there are no redundant copies
of dictionaries in memory.

### Automatic training

To create a dictionary, some training data is needed.
This means that the dictionary can't be created immediately for a new table,
some data must accumulate in it first.

Also, the dataset can change over time, and a dictionary might become outdated.
In this case, it could be good to retrain it.

But it would be impractical to manually pick the right moments to train new
dicts. So there's `sstable_dict_autotrainer`, which periodically trains
new dicts, if it seems that the given dict-aware table deserves one.
Refer to the implementation for up-to-date details.

### New interfaces

- To enable dictionaries for a given table, the user sets its
  `sstable_compression` entry in the schema to one of the new compressor IDs.
  (The autotrainer will eventually train a dict for it.)
- REST API `storage_service/retrain_dict` can be used to trigger a dictionary
  training for a table manually, without waiting for the automatic training.
- REST API `storage_service/estimate_compression_ratios` can be used to generate
  a report with estimations of compression ratios (on the given table) for
  various compression configs (algorithm, level, chunk size), to guide the
  choice of configuration.

### New RPCs

- `SAMPLE_SSTABLES` is used by a dictionary-training node to gather SSTable
  samples from other nodes.
- `ESTIMATE_SSTABLE_VOLUME` is a helper RPC used by a dictionary-training node
  to find out how much data other nodes have, so that it can later request
  the right (i.e. proportional) amount of samples from each node.
  It's also used by the autotrainer to find out if the table is big enough for
  dictionary training.

### New config entries

There are several new config knobs related to this feature, all named like
`sstable_compression_dictionaries_*`.
Refer to `config.hh` for up-to-date details.
