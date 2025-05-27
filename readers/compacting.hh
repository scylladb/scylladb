/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "gc_clock.hh"
#include "readers/mutation_reader_fwd.hh"
#include "compaction/compaction_garbage_collector.hh"

namespace dht {
class decorated_key;
}

class tombstone_gc_state;
struct tombstone_purge_stats;

/// Creates a compacting reader.
///
/// The compaction is done with a \ref mutation_compactor, using compaction-type
/// compaction (`compact_for_sstables::yes`).
///
/// \param source the reader whose output to compact.
///
/// Params \c compaction_time and \c get_max_purgeable are forwarded to the
/// \ref mutation_compactor instance.
///
/// Inter-partition forwarding: `next_partition()` and
/// `fast_forward_to(const dht::partition_range&)` is supported if the source
/// reader supports it
/// Intra-partition forwarding: `fast_forward_to(position_range)` is supported
/// if the source reader supports it
mutation_reader make_compacting_reader(mutation_reader source, gc_clock::time_point compaction_time,
        max_purgeable_fn get_max_purgeable,
        const tombstone_gc_state& gc_state,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        tombstone_purge_stats* tombstone_stats = nullptr);
