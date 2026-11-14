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

#include <memory>

#include <seastar/core/shared_ptr.hh>

#include "readers/mutation_reader.hh"
#include "readers/mutation_reader_fwd.hh"
#include "sstables/parquet/mapping_reader.hh"
#include "sstables/parquet/reader_cache.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/shared_sstable.hh"
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
    // The plan of the latest table schema that a read used.
    lw_shared_ptr<const read_plan> _plan;
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
/// FIXME: a reversed slice reads forward and reverses the result in
/// memory. The next step replaces this with native reverse reads.
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
        std::unique_ptr<abstract_index_reader> ir,
        const utils::hashed_key* single_partition_read_murmur_hash);

/// A reader of the whole sstable that does not use the index, as
/// mx::make_full_scan_reader(). It does not support fast-forwarding or
/// next_partition(). Page checksums are always verified.
mutation_reader make_full_scan_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor);

} // namespace sstables::parquet
