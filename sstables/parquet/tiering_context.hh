/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Builds the tiering_inputs for a candidate compaction from things the
// compaction layer already knows.
//
// Kept apart from tiering_policy.hh on purpose: the policy stays a pure function
// over plain numbers with no Scylla dependencies, so it can be unit-tested
// exhaustively. This is the only file that has to know about sstables and
// schemas, and all it does is fill in a struct.

#include "sstables/parquet/tiering_policy.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/version.hh"
#include "schema/schema_fwd.hh"

#include <optional>
#include <vector>

namespace sstables::parquet {

// True when the schema can be shredded without losing information: no counters,
// and no multi-cell (non-frozen) collections, which the shredder does not
// handle yet.
bool schema_is_parquet_eligible(const ::schema&);

// True when every sstable this table writes should be Parquet, with no per-compaction decision.
//
// Two cases resolve to this:
//
//  - `storage_format = 'parquet'`, the explicit opt-in.
//  - `storage_format = 'hybrid'` **on a TWCS table**. Hybrid tiering exists to keep Parquet out of
//    the levels that get rewritten, because re-encoding and recompressing a Parquet run is the
//    expensive thing this format does. TWCS does not have those levels: a window is compacted and
//    then closed, so there is no repeated rewrite to protect against and no reason to leave part of
//    the table in the row format. Under TWCS, hybrid and parquet mean the same thing (design doc
//    6.4), so the criteria are skipped entirely -- which also skips C6's data sample, by far the
//    most expensive part of the decision.
//
// One consequence to be aware of: skipping C6 skips the measurement that catches a schema Parquet
// stores *worse* than the row format. A 197-column sparse-telemetry table measured at 208 % of its
// SSTable (section 10.4), and on TWCS + hybrid it would now be converted and grow rather than being
// declined. `storage_format = 'sstable'` is the way to keep such a table in the row format.
//
// The single home for this rule matters: it is consulted by compaction, by memtable flush and by
// streaming, and a table whose flushes disagree with its compactions never converges.
bool writes_parquet_unconditionally(const ::schema&);

// The version that reshard and reshape **on load** must write for this table.
//
// `native_choice` is what the native machinery picked -- in practice
// `sstables_manager::get_safe_sstable_version_for_rewrites()`, which chooses among the native
// versions from config and knows nothing about `pq`. Left to itself it would silently rewrite a
// Parquet table's sstables as native during boot-time reshard/reshape, so the decision is
// wrapped here instead.
//
// This exists as a named function rather than an `if` at the call site for one reason: the call
// site is `table_populator::process_subdir()`, a class local to `distributed_loader.cc`, so the
// version it picks is not observable from any test. Reshape-on-load is the one write path with
// no seam -- flush, streaming and compaction all pick their version through a function a test
// can call -- and it is exactly the path that drifted, gating on `storage_format == parquet`
// while the other three asked `writes_parquet_unconditionally()`. That made hybrid + TWCS write
// native here and `pq` everywhere else.
//
// Note that needing no tiering context is the whole point: both unconditional cases are decided
// by schema properties alone (`storage_format` and the compaction strategy), which is why "we are
// on the load path and know nothing about tiering yet" does not argue for a different answer here.
sstables::sstable_version_types version_for_rewrite_on_load(const ::schema&,
                                                            sstables::sstable_version_types native_choice);

// CQL columns, which is what C5 bounds. Deliberately not the Parquet leaf count: that is
// data-dependent (per-column deletion and TTL leaves materialise only when cells carry them),
// so it cannot be derived from a schema, and a criterion should not rest on a guess.
size_t column_count(const ::schema&);

tiering_inputs make_tiering_inputs(const std::vector<sstables::shared_sstable>& inputs,
                                   const ::schema&,
                                   const compaction_context&);

// Convenience: build the inputs and evaluate in one step.
tiering_decision decide_output_format(const std::vector<sstables::shared_sstable>& inputs,
                                      const ::schema&,
                                      const compaction_context&,
                                      const tiering_thresholds& = {});

} // namespace sstables::parquet
