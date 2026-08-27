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
