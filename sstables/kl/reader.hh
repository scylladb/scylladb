/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "sstables/progress_monitor.hh"

namespace sstables {
namespace kl {

mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor);

// Same as above but the slice is moved and stored inside the reader.
mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        query::partition_slice&& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor);

// A reader which doesn't use the index at all. It reads everything from the
// sstable and it doesn't support skipping.
mutation_reader make_crawling_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor);

} // namespace kl
} // namespace sstables
