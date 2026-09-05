/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// A scan that yields Parquet's own columnar batches instead of mutation fragments.
//
// Why: design doc 10.44. Decoding every row group of a file and touching every value costs 30.4 ms
// where the same scan through `mutation_reader` costs 246.1 ms -- 8.1x. Two thirds of a scan is
// building a mutation per row (10.38), and that cost is the same on the row format, so it is not a
// parquet problem to fix in the decoder: the ceiling from optimising parquet decode is 1.19x. The
// only way past it is an interface that does not build a mutation, which is what this is.
//
// What it is *not*: a replacement for `make_full_scan_reader`. It returns columns, so a consumer
// that needs rows, tombstone resolution, cross-sstable merging or clustering-order guarantees still
// wants the mutation path. This exists for consumers whose shape is already columnar --
// aggregates, projections, and a pq-to-pq rewrite -- and it exists now because it is the one such
// consumer that can be *proved* correct without touching the query path: the same file read both
// ways must yield the same values.
//
// Deliberate limits of this first version, each of which is a follow-up rather than a design
// choice:
//   * unencrypted files only -- it throws on a PARE footer rather than silently reading nothing;
//   * it does not use the sstable's footer cache, so it parses the footer itself once per reader;
//   * whole row groups only, so no row-range seeking and no page-index use;
//   * every leaf is decoded, so there is no projection pushdown yet. That one is worth noting
//     twice: 10.30 measured that the *mutation* path does not push projection down either, and a
//     columnar interface is what would finally make it possible.

#include "sstables/shared_sstable.hh"
#include "sstables/parquet/format/parquet_reader.hh"
#include "sstables/parquet/schema_mapping.hh"
#include "schema/schema_fwd.hh"
#include "reader_permit.hh"

#include <seastar/core/future.hh>

#include <memory>
#include <optional>
#include <vector>

namespace sstables::parquet {

// One row group's worth of decoded columns.
struct column_batch {
    // One entry per leaf, in schema order -- the same order as mapped_schema's leaves, so a
    // consumer can pair them up without consulting the file.
    std::vector<format::column_data> columns;
    // Row ordinal, within the file, that this batch starts at. Batches are contiguous and in file
    // order, which for a pq file is partition order.
    int64_t first_row = 0;
    int64_t rows = 0;
};

class batch_reader {
public:
    virtual ~batch_reader() = default;
    // Reads and parses the footer. Idempotent, and `next()` calls it anyway -- it is public
    // because `schema_mapping()` and `columns()` describe the *file*, so they are empty until the
    // footer has been read, and a consumer that wants to know the shape before reading (to build a
    // projection, say) needs a way to ask.
    virtual future<> init() = 0;
    // The next batch, or nullopt at end of file.
    virtual future<std::optional<column_batch>> next() = 0;
    virtual future<> close() = 0;
    // What the columns mean. Valid after init(); stable thereafter.
    virtual const mapped_schema& schema_mapping() const = 0;
    virtual const std::vector<cql_column>& columns() const = 0;
    // Bytes fetched so far. Exists so a projection's saving is measured rather than claimed.
    virtual uint64_t bytes_read() const = 0;
};

// Which regular columns the consumer wants, in mapped_schema::value_leaf order. Empty means all of
// them, which is what a reader created without a projection gets.
//
// This is the thing a columnar format is supposed to be able to do and neither path could: §10.30
// measured that selecting one column of five moved what a pq scan reads by 1.6 %, in the wrong
// direction. A projection here skips both the decode *and* the read of the leaves it does not want,
// because a column chunk is a contiguous extent -- so a narrow scan finally costs less than a wide
// one.
struct projection {
    std::vector<bool> want_regular;
};

// Throws if the sstable is not `pq`, or if it is encrypted.
std::unique_ptr<batch_reader> make_batch_reader(shared_sstable, schema_ptr, reader_permit,
                                                std::optional<projection> = std::nullopt);

} // namespace sstables::parquet
