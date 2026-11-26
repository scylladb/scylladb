/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <stdexcept>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include "encoding_stats.hh"
#include "mutation/mutation.hh"
#include "reader_permit.hh"
#include "schema/schema_fwd.hh"
#include "sstables/column_translation.hh"

namespace sstables {

class sstable;
class sstables_stats;

namespace mx {

// What the mx data-file parser needs from the source of the bytes.
// An sstable is one source; an encoded partition held in memory is another.
struct data_source_descriptor {
    // Delta-encoding bases of timestamps, TTLs and local deletion times.
    encoding_stats bases;
    // Column names in translation point into storage the source keeps alive.
    column_translation translation;
    bool has_shadowable_tombstones;
    // Null means no read accounting.
    sstables_stats* stats;
};

data_source_descriptor make_data_source_descriptor(const schema& s, sstable& sst);

// Well-formed input holds data the decoding schema cannot take: the column is
// gone and the data is newer than its drop, or has no drop record, or the column's
// type changed incompatibly. Data at or before the column's drop is skipped.
class column_mismatch_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Decodes exactly one partition from size bytes of mx data-file encoding, all of
// which the partition must use. Malformed input throws malformed_sstable_exception;
// data the schema cannot take throws column_mismatch_error.
future<mutation> decode_partition(schema_ptr s, const data_source_descriptor& desc, input_stream<char> input, uint64_t size, reader_permit permit);

} // namespace mx
} // namespace sstables
