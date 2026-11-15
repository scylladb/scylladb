/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The writer of an sstable whose Data file is a Parquet file
// (has_parquet_data_file(), version `aa`).
//
// It drives the mapping writer (mapping_writer.hh), which turns mutation
// fragments into Parquet rows, and writes the other components as the `mt`
// writer does: the trie index, Filter.db, Statistics.db, Scylla.db, and
// Digest.crc32. There is no CRC.db and no CompressionInfo.db, because the
// pages of the Data file carry their own checksums and compression.
//
// Positions in the trie index
// ===========================
//
// The index stores row ordinals in place of Data file offsets. The ordinal
// of a row is its number in the file, from 0.
//
// - A partition's position is the ordinal of its first row. That is the
//   static row if it has cells, else the first clustering row or range
//   tombstone bound, else the row of kind "none" that a partition without
//   other rows gets.
// - A row index block's offset is the ordinal of its first row minus the
//   partition's position. The first block starts at the partition's first
//   clustering row or range tombstone bound, after the static row.
// - The partition end that the row index stores (the final separator) is
//   the ordinal one past the partition's last row.
// - The end of the file is the total row count, which Scylla.db stores
//   (scylla_metadata::parquet_data_file::rows). The index reader receives
//   it in place of the Data file size.
//
// Row index blocks
// ================
//
// A block ends where a page set ends, so every block starts at a page set
// boundary. There is no auto-scaling: every page set boundary inside a
// partition ends a block. A partition gets a row index only if that gives
// it at least two blocks. The static row is in no block. A page set that
// ends at the static row does not end a block.
//
// There is one exception. The trie separates two blocks by a key that is
// larger than the last entry of the first block and not larger than the
// first entry of the second. So two consecutive rows at the same clustering
// position stay in one block, even if a page set ends between them. This
// happens between the end bound row and the start bound row of a range
// tombstone boundary, and between range tombstone changes at one position.
// Such a page set boundary gets no block, and the block ends at the next
// page set boundary instead.
//
// Each block records the range tombstone open before its first row, as for
// `ms` and `mt`. Bound rows open and close range tombstones one row at a
// time: a start bound opens its tombstone, and an end bound closes it.
//
// Size measures
// =============
//
// - The raw size (writer::raw_size()) measures partitions and rows for the
//   large data records, the partition size histogram in Statistics.db, and
//   write progress (writer_offset_tracker). Scylla.db stores the file's
//   total raw size.
// - The size of a cell for large cell detection is the size of its value:
//   for an atomic cell, the CQL value (zero for a dead cell); for a
//   collection, the sum of its element keys and values.
// - The maximum sstable size and the split of a large partition use the
//   estimate of the file size (writer::estimated_size()).
// - The compression ratio in Statistics.db is the file size divided by the
//   total raw size, for a table with compression. It stays unset (-1) for
//   a table without compression, as for `mt`.
// - data_file_position_for_tests() is the ordinal of the next row: the
//   position that the index would store for it.

#include <cstdint>
#include <memory>

#include "encoding_stats.hh"
#include "sstables/parquet/writer.hh"
#include "sstables/writer_impl.hh"

class compression_parameters;

namespace sstables::parquet {

/// The configuration of the Parquet file of an sstable: the tunables of
/// `cfg`, and the compression codec that the table's compression option
/// gives. The dictionary variants of LZ4 and Zstd use the same codec
/// without a dictionary. Zstd uses the table's compression level. The
/// chunk length has no effect.
writer_config make_writer_config(const sstable_writer_config& cfg, const compression_parameters& compression);

/// The table compression that gives pages of `codec`: the inverse of the
/// choice of compression codec in make_writer_config(). The variants with
/// dictionaries give the same codec, so they map to the algorithms without
/// dictionaries. A Parquet file does not record the Zstd level, so Zstd
/// gets the default level. A codec that our writer does not use gives no
/// compression.
compression_parameters compression_parameters_of(compression_codec codec);

/// Creates the writer. `sst` must have a Parquet Data file, and `s` must
/// not be a compact storage table.
std::unique_ptr<sstable_writer::writer_impl> make_sstable_writer(sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard);

} // namespace sstables::parquet
