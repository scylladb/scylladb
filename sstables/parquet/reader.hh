/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>

// Reader for the `pq` sstable format: the inverse of writer_impl.hh.
//
// The Data component of a pq sstable is a complete, externally-valid Parquet
// file. Reading it back means parsing the footer, decoding row groups into the
// layer-2 row model, and turning those rows into mutation fragments -- the
// exact reverse of what fragment_shredder does on the way in.
//
// Scope, and the honest limitation: this reader materialises the whole Parquet
// image and every mutation in it before emitting anything. That mirrors the
// writer, which also assembles the whole image in memory, and it is what makes
// the format testable end to end today. It does NOT satisfy R-13 (bounded
// memory over arbitrarily large sstables) -- a seastar-native streaming reader
// that decodes one row group at a time, driven by the OffsetIndex, is tracked
// separately in docs/dev/parquet-storage-format.md section 11.
//
// The partition index is still Scylla's: index entries carry a row ordinal
// (design doc 5.4 option A), and the OffsetIndex turns that ordinal into a
// page. A streaming reader will need both. This one does not, because it has
// every row in hand -- but the index is written, verified, and ready for it.

#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/progress_monitor.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner.hh"
#include "query/query-request.hh"
#include "tracing/trace_state.hh"

namespace sstables::parquet {

mutation_reader make_reader(
        sstables::shared_sstable sst,
        schema_ptr query_schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        sstables::read_monitor& mon);

// Data-file-order scan of the whole sstable, used by compaction, scrub and
// validation. For pq that is just a full-range read: the Parquet row order is
// the write order, which is partition order.
mutation_reader make_full_scan_reader(
        sstables::shared_sstable sst,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        sstables::read_monitor& mon);

// Per-phase reader timings, for attributing point-read cost. Opt-in at runtime via
// PQ_READER_PROFILE=1; the instrumentation is always compiled, because a profile taken from
// a differently-built binary is a guess rather than a measurement.
//
// Exists because external sweeps ran out of resolution: page size and row-group count between
// them explain only 29 % of a point read, and the remaining 586 us could be any of four things
// (design doc 10.4g). Guessing which cost a wrong answer once already -- the footer looked
// obvious and turned out to be 7 %.
std::string reader_profile_report();
void reader_profile_reset();

} // namespace sstables::parquet
