/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// C6 of the tiering decision: how much smaller would this data actually be as
// Parquet?
//
// Measured, not modelled. The corpus in docs/dev/parquet-storage-format.md
// section 10.1 spans a 50.1 % ratio on a wide web-analytics table and a 77.3 %
// one on a sensor table with the same folding and the same codec, so no formula
// over column counts and type widths is going to predict a given table's number.
// The only estimator worth having is the real writer run over some of the real
// data, which is what this does.
//
// The cost is one extra pass over a bounded sample of one input sstable, taken
// once per compaction that could convert, and only for tables in 'hybrid' mode.

#include "sstables/parquet/writer_impl.hh"
#include "sstables/shared_sstable.hh"
#include "schema/schema_fwd.hh"
#include "reader_permit.hh"

#include <seastar/core/future.hh>

#include <optional>
#include <span>
#include <vector>

namespace sstables::parquet {

struct gain_sample_limits {
    // Rows to shred before deciding we have seen enough. 100k rows is about two
    // row groups at the 35k default, which is enough for the encoders to have
    // built representative dictionaries.
    size_t max_rows = 100'000;
    // Hard ceiling on shredder memory, in case the rows are very wide. The
    // shredder holds the sample in a decoded form, which measures roughly two
    // orders of magnitude larger than the bytes it will write.
    size_t max_bytes = 256u << 20;
};

// What one pass of sample_sstable() saw and produced.
struct sstable_sample {
    // Encoded Parquet bytes for the sample, one entry per writer config passed in,
    // in the same order.
    std::vector<size_t> encoded_bytes;
    uint64_t partitions = 0;
    // Clustering rows fed to the shredder. Zero does not mean the sample is empty:
    // a partition holding only a static row, a partition tombstone or range
    // tombstones still produces storage rows, counted in `rows`.
    uint64_t clustering_rows = 0;
    // Rows the shredder emitted, which is what the Parquet file actually holds.
    uint64_t rows = 0;
    // Row groups the sample was encoded as (see sample_sstable() on how they are cut).
    uint64_t row_groups = 0;
    // Peak shredder memory the sample held at once, for the write-side memory budget.
    size_t peak_buffered_bytes = 0;
    // True when a limit stopped the scan before the sstable's end.
    bool truncated = false;
};

// Reads `sst` from its start, in token order, through the *whole* mutation fragment
// protocol -- partition tombstones, static rows, clustering rows, range tombstone
// changes and end_partition -- and encodes what it read with each of `cfgs`.
//
// The result is meant to predict what the storage writer would produce, so the sample
// is cut into row groups the way pq_writer_impl cuts them: at a partition boundary,
// once `cfgs[0].rows_per_row_group` rows or `cfgs[0].row_group_buffer_bytes` of shredder
// memory have accumulated. Encoding the whole sample as one row group would be
// optimistic -- one dictionary and one set of column-chunk headers where the file will
// have one per group -- and an estimator that is optimistic converts tables it should
// have declined. Each group is encoded as its own image and the sizes are summed, which
// counts the small per-file footer once per group rather than once; that error is a few
// hundred bytes per group and is in the pessimistic direction, which is the safe one.
//
// The scan stops only at a partition boundary, once either limit is reached: that is the
// one place where abandoning the read leaves the fragment stream well-formed and the
// shredder's partition state closed. A reader failure propagates; callers decide whether
// that is fatal.
future<sstable_sample> sample_sstable(schema_ptr,
                                      reader_permit,
                                      sstables::shared_sstable,
                                      std::span<const pq_writer_config> cfgs,
                                      gain_sample_limits = {});

// Fraction of on-disk bytes Parquet would save, e.g. 0.42 for "42 % smaller".
// Negative means Parquet is bigger. std::nullopt means the sample was not usable
// (empty sstable, no rows read, or a read error), which the policy reads as "not
// measured" and therefore as a rejection -- failing to measure must never be a
// reason to convert.
future<std::optional<double>> estimate_parquet_gain(schema_ptr,
                                                    reader_permit,
                                                    const std::vector<sstables::shared_sstable>& inputs,
                                                    const pq_writer_config&,
                                                    gain_sample_limits = {});

} // namespace sstables::parquet
