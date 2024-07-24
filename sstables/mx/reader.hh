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
namespace mx {

// Precondition: if the slice is reversed, the schema must be reversed as well
// and the range must be singular (`range.is_singular()`).
// Fast-forwarding is not supported in reversed queries (FIXME).
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

// Validate the content of the sstable with the mutation_fragment_stream_valdiator,
// additionally cross checking that the content is laid out as expected by the
// index and promoted index respectively.
future<uint64_t> validate(
        shared_sstable sstable,
        reader_permit permit,
        abort_source& abort,
        std::function<void(sstring)> error_handler,
        sstables::read_monitor& monitor);

} // namespace mx
} // namespace sstables
