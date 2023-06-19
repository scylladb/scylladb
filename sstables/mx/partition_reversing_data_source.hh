/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/iostream.hh>
#include "reader_permit.hh"
#include "sstables/index_reader.hh"
#include "sstables/shared_sstable.hh"

namespace tracing { class trace_state_ptr; }

namespace sstables {
namespace mx {

struct partition_reversing_data_source {
    seastar::data_source the_source;

    // Underneath, the data source is iterating over the sstable file in reverse order.
    // This points to the current position of the source over the underlying sstable file;
    // either the end of partition or the beginning of some row (never in the middle of a row).
    // The reference is valid as long as the data source is alive.
    const uint64_t& current_position_in_sstable;
};

// Returns a single partition retrieved from an sstable data file as a sequence of buffers
// but with the clustering order of rows reversed.
//
// `pos` is where the partition starts.
// `len` is the length of the partition.
// `ir` provides access to an index over the sstable.
//
// `ir.data_file_positions().end` may decrease below `current_position_in_sstable`,
// informing us that the user wants us to skip the sequence of rows between `ir.data_file_positions().end` and `current_position_in_sstable`.
// `ir.data_file_positions().end`, if engaged, must always point at the end of partition (pos + len) or the beginning of some row.
// We ignore the value of `ir.data_file_positions().start`.
//
// We assume that `ir.current_clustered_cursor()`, if engaged, is of type `sstables::mc::bsearch_clustered_cursor*`.
//
// The source must be closed before destruction unless `get()` was never called.
partition_reversing_data_source make_partition_reversing_data_source(
    const schema& s, shared_sstable sst, index_reader& ir, uint64_t pos, size_t len,
    reader_permit permit, tracing::trace_state_ptr trace_state);

}
}
