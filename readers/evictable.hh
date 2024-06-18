/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/i_partitioner_fwd.hh"
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

class reader_permit;
class mutation_source;

namespace tracing {
class trace_state_ptr;
}

namespace query {
class partition_slice;
}

/// Make an auto-paused evictable reader.
///
/// The reader is paused after each use, that is after each call to any of its
/// members that cause actual reading to be done (`fill_buffer()` and
/// `fast_forward_to()`). When paused, the reader is made evictable, that it is
/// it is registered with reader concurrency semaphore as an inactive read.
/// The reader is resumed automatically on the next use. If it was evicted, it
/// will be recreated at the position it left off reading. This is all
/// transparent to its user.
/// Parameters passed by reference have to be kept alive while the reader is
/// alive.
mutation_reader make_auto_paused_evictable_reader_v2(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr);

class evictable_reader_v2;

class evictable_reader_handle_v2 {
    friend std::pair<mutation_reader, evictable_reader_handle_v2> make_manually_paused_evictable_reader_v2(mutation_source, schema_ptr, reader_permit,
            const dht::partition_range&, const query::partition_slice&, tracing::trace_state_ptr, mutation_reader::forwarding);

private:
    evictable_reader_v2* _r;

private:
    explicit evictable_reader_handle_v2(evictable_reader_v2& r);

public:
    void pause();
};

/// Make a manually-paused evictable reader.
///
/// The reader can be paused via the evictable reader handle when desired. The
/// intended usage is subsequent reads done in bursts, after which the reader is
/// not used for some time. When paused, the reader is made evictable, that is,
/// it is registered with reader concurrency semaphore as an inactive read.
/// The reader is resumed automatically on the next use. If it was evicted, it
/// will be recreated at the position it left off reading. This is all
/// transparent to its user.
/// Parameters passed by reference have to be kept alive while the reader is
/// alive.
std::pair<mutation_reader, evictable_reader_handle_v2> make_manually_paused_evictable_reader_v2(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr);
