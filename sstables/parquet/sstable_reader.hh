/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The reader of an sstable whose Data file is a Parquet file
// (has_parquet_data_file(), version `aa`).
//
// It has the entry points of the `mx` reader, with the same arguments:
// make_reader() and make_full_scan_reader(). sstable::make_reader() and
// sstable::make_full_scan_reader() choose by format.
//
// What a read needs from the file comes from three places:
//
// - The file summary (file_summary.hh). It is parsed once per sstable,
//   when the sstable is opened, and kept in shareable_components, so a
//   load on another shard reuses it.
// - The read state of this file below, one per sstable object, so one
//   per shard: the mapping of the file, the cache of decoded OffsetIndexes
//   and dictionaries, and the read plan of the latest table schema.
// - The trie index, whose positions are row ordinals (see sstable_writer.hh).
//   data_file_positions() is a half-open range of ordinals.
//
// A read builds a codec reader (reader.hh) over the Data file, tracked by
// the read's permit, and a row decoder (mapping_reader.hh) on top of it.
// The mutation reader walks rows by ordinal and turns each row into
// mutation fragments, in the order of the `mx` reader.
//
// Reverse reads
// =============
//
// A reversed slice is read natively (PLAN.md, "Reverse reads"). As for
// `mx`, the slice is in the native reversed format, and the schema passed
// to the reader is the reversed schema. Partitions come in forward order,
// and the fragments of each partition in reverse clustering order, after
// its partition start and static row. Unlike `mx`, a reverse read may span
// several partitions, and supports fast_forward_to() of both kinds.
//
// For each partition, the reader first reads the head forward: the first
// row, which holds the key and the partition tombstone, and the static row
// if there is one. It then walks backward by ordinal, from the end of the
// partition down to its first clustering row or range tombstone bound
// row. The codec reads the pages backward, and read-ahead runs backward.
// See sstable_reader.cc for the details.

#include <array>
#include <functional>
#include <memory>

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

#include "readers/mutation_reader.hh"
#include "readers/mutation_reader_fwd.hh"
#include "sstables/parquet/mapping_reader.hh"
#include "sstables/parquet/reader_cache.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/types_fwd.hh"
#include "tracing/trace_state.hh"

namespace utils {
struct hashed_key;
}

namespace sstables {
class abstract_index_reader;
}

namespace sstables::parquet {

/// What one shard keeps of an opened Parquet Data file, besides its summary.
/// It must be destroyed on the shard that created it.
class sstable_read_state {
    opened_mapping _mapping;
    metadata_cache _cache;
    // The plans of the two latest schemas that reads used, the latest
    // first. A table has a forward and a reversed schema, and reads in
    // both directions should not evict each other's plan.
    std::array<lw_shared_ptr<const read_plan>, 2> _plans;
public:
    /// The cache's entries live in `region` and are linked in `lru`, which
    /// belong to the sstables' cache_tracker, with `stats`.
    sstable_read_state(opened_mapping m, logalloc::region& region, lru& lru, metadata_cache_stats& stats)
        : _mapping(std::move(m))
        , _cache(region, lru, stats)
    {}

    const opened_mapping& mapping() const noexcept { return _mapping; }
    metadata_cache& cache() noexcept { return _cache; }
    /// The read plan for table schema `s`, whose column_translation is `ct`.
    lw_shared_ptr<const read_plan> plan_for(const ::schema& s, const column_translation& ct, const file_summary& summary);
};

/// A reader of a range of partitions, as mx::make_reader(). The index
/// reader `ir` must be the sstable's.
///
/// `integrity` adds nothing. For `mx`, it adds the chunk checksums of an
/// uncompressed Data file, and the digest at the end of a read of the whole
/// file. Here, every byte that the output depends on is checked on every
/// read: each page against its CRC, the footer against the checksum in
/// Scylla.db when the sstable is opened, and each OffsetIndex against the
/// footer and the page headers. The bytes that a read skips are the
/// chunks that the footer says are entirely null, so they cannot change
/// the output. A digest check would need a second pass over the whole
/// file, because the reader does not read it in order. Checksum validation
/// and the `validate` mode of scrub make that pass (file_verifier.hh, and
/// validate() below). Scrub in its other modes does not, because the pass
/// would double its I/O without checking anything that it outputs.
mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor,
        integrity_check integrity,
        std::unique_ptr<abstract_index_reader> ir,
        const utils::hashed_key* single_partition_read_murmur_hash);

/// A reader of the whole sstable that does not use the index, as
/// mx::make_full_scan_reader(). It does not support fast-forwarding or
/// next_partition(). `integrity` adds nothing, as for make_reader().
mutation_reader make_full_scan_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor,
        integrity_check integrity);

/// Validates the sstable, as mx::validate() (PLAN.md, "Scrub and
/// validation"). Reports each error through `error_handler`, and returns
/// their number. Stops early when `abort` is requested. It checks:
///
/// - The whole Data file, in order, with file_verifier.hh: the checksum of
///   every page, the OffsetIndexes against the footer and the page headers,
///   the late chunks, the footer checksum, and the digest if the sstable
///   has read it (read_digest()).
/// - A full scan, with the order of fragments validated as sstable::validate()
///   does for any format. A malformed sstable error ends the scan.
/// - The structure of each row: the partition key columns and the partition
///   tombstone are set exactly on the rows that start a partition.
/// - The trie index against the rows: partition positions in both
///   directions, partition keys and tombstones where the index has them,
///   and row index blocks (see index_checker in sstable_reader.cc).
future<uint64_t> validate(shared_sstable sst, reader_permit permit, abort_source& abort,
        std::function<void(sstring)> error_handler, read_monitor& monitor);

} // namespace sstables::parquet
