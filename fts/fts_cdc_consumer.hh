/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

// =========================================================================
// FTS CDC Consumer — Header
// =========================================================================
//
// Per-shard service that polls the implicit CDC log tables for tables that
// carry an FTS index and forwards typed FieldValue batches to the Tantivy
// shard index running on a Seastar alien thread.
//
// Lifecycle:
//   fts_cdc_consumer is constructed once per ScyllaDB shard via the
//   seastar::sharded<> machinery.  `start()` opens existing indexes and
//   arms the periodic poll timer; `stop()` flips the stopping flag, cancels
//   the timer and waits for in-flight work via a `seastar::gate`.
//
// Write path (high level):
//   1. Any mutation on an FTS-indexed table is augmented by the CDC service
//      (because `cdc_enabled()` now returns true for FTS tables) and
//      `cdc.postimage` is enforced at CREATE INDEX time.
//   2. The CDC service writes one or more rows to the `_scylla_cdc_log`
//      shadow table; for non-delete operations a `post_image` row is also
//      emitted with the complete post-state of the row.
//   3. On each timer tick, `poll_cdc()` issues a CQL query against every
//      tracked `_scylla_cdc_log` table, reading rows written after the last
//      checkpoint timestamp.
//   4. Each CDC row is forwarded to `process_cdc_row()`, which routes:
//        - `post_image` rows to a Tantivy upsert with the full row state,
//        - `row_delete` / `partition_delete` rows to the corresponding
//          Tantivy delete call,
//        - delta rows (`insert`, `update`) are skipped — the post-image
//          row carries authoritative state for the same operation.
//   5. After processing a batch the local Tantivy index is committed and the
//      checkpoint is advanced.
//
// Read path:
//   The `search()` method is called by `fts_indexed_table_select_statement`
//   and dispatches a per-shard search via `container().map_reduce0(...)`.
//   The merged hit list is re-sorted by BM25 score and truncated to the
//   caller-requested limit before returning.
//
// Thread model:
//   - All Seastar code runs on the normal reactor thread.
//   - Each shard owns one `alien_thread_runner` that executes blocking
//     Tantivy operations on a dedicated OS thread.  The runner takes the
//     public `seastar::engine().alien()` reference at construction so it
//     never reaches into the `internal::` namespace.

#include <memory>
#include <unordered_map>
#include <utility>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

#include "cdc/log.hh"
#include "cql3/untyped_result_set.hh"
#include "schema/schema_fwd.hh"
#include "utils/UUID.hh"

// Forward-declare the CXX-bridge types so we avoid including the generated
// header from every translation unit that includes this header.
namespace fts {
struct FtsSearchResponse;
struct FieldMapping;
} // namespace fts

namespace replica {
class database;
} // namespace replica

namespace service {
class storage_proxy;
class query_state;
} // namespace service

namespace cql3 {
class query_processor;
} // namespace cql3

namespace db::fts {

// =========================================================================
// Alien thread runner
// =========================================================================
//
// Wraps a `seastar::alien::run_on` helper to execute blocking Tantivy
// operations without stalling Seastar reactor threads.  The Tantivy
// `ShardIndex` is NOT Send across shards, so one `alien_thread_runner` is
// kept per-shard.
class alien_thread_runner;

// =========================================================================
// Per-table index state
// =========================================================================
//
// Stores all open shard indexes for one base table, keyed by index name.
// The opaque `ShardIndex*` pointer is managed by `rust::Box<fts::ShardIndex>`
// on the alien side; we keep only the raw pointer for FFI calls from the
// Seastar thread.
struct fts_table_state {
    // Map: index_name → owning Box<ShardIndex> (held as void* to avoid
    // pulling in cxx.h from this header).
    std::unordered_map<sstring, std::unique_ptr<void, void(*)(void*)>> indexes;

    // Last `cdc$time` timeuuid consumed for this table.
    // Used as a strict (exclusive) lower bound for the next CDC poll query,
    // expressed directly in CQL as `cdc$time > <uuid>` to preserve full
    // sub-millisecond resolution.  See H3 in the review plan.
    utils::UUID checkpoint_uuid;

    fts_table_state() = default;
};

// =========================================================================
// fts_cdc_consumer
// =========================================================================
class fts_cdc_consumer
    : public seastar::peering_sharded_service<fts_cdc_consumer>
{
public:
    fts_cdc_consumer(seastar::sharded<cql3::query_processor>& qp, seastar::sharded<replica::database>& db);
    ~fts_cdc_consumer();

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Arm the poll timer and open indexes for all existing FTS tables.
    seastar::future<> start();

    /// Cancel timer and wait for any in-flight poll to complete.
    seastar::future<> stop();

    // ── Schema change notifications ──────────────────────────────────────

    /// Called when a table schema changes.
    /// - If the table gained an FTS index: open (or create) shard indexes.
    /// - If the table lost an FTS index: close and remove shard indexes.
    /// - If a column was added: trigger a full rebuild.
    seastar::future<> on_schema_change(schema_ptr old_schema, schema_ptr new_schema);

    /// Filesystem and in-memory cleanup for a dropped column family.
    /// Invoked from the `migration_listener` adapter registered in main.cc.
    seastar::future<> on_drop_column_family(const sstring& ks_name, const sstring& cf_name);

    // ── Read path ────────────────────────────────────────────────────────

    /// Execute a full-text search across all shard indexes and return a
    /// merged, score-sorted hit list truncated to `limit` results.
    ///
    /// Implemented by fanning out to every shard via `container().map_reduce0()`
    /// and merging the per-shard responses on the calling shard.  The
    /// per-shard work runs in `search_local()`.
    seastar::future<std::vector<std::pair<sstring, float>>>
    search(const sstring& keyspace,
           const sstring& table,
           const sstring& index_name,
           const sstring& query,
           const sstring& default_field,
           uint32_t limit);

    /// Per-shard search entry point invoked by `map_reduce0` from `search()`.
    /// Returns at most `limit` hits on this shard; callers merge and sort.
    seastar::future<std::vector<std::pair<sstring, float>>>
    search_local(sstring keyspace,
                 sstring table,
                 sstring index_name,
                 sstring query,
                 sstring default_field,
                 uint32_t limit);

private:
    // ── Internal implementation ──────────────────────────────────────────

    /// One poll tick: read CDC rows since last checkpoint and index them.
    seastar::future<> poll_cdc();

    /// Scan the database for FTS-indexed tables not yet tracked and open
    /// their shard indexes.  Called at the start of every poll cycle so
    /// that tables created after startup are picked up without needing a
    /// separate schema-change notification path.
    seastar::future<> scan_for_new_fts_tables();

    /// Process a single CDC log table for the given base table.
    seastar::future<> poll_cdc_table(const sstring& keyspace,
                                     const sstring& table,
                                     table_id tid,
                                     fts_table_state& ts);

    /// One prune tick: call `fts::prune_expired` for every open shard
    /// index, reclaiming on-disk space for TTL-expired documents.
    seastar::future<> prune_expired_all();

    /// Decode one CDC result-set row and forward writes/deletes to Tantivy.
    seastar::future<> process_cdc_row(schema_ptr base_schema,
                                      const cql3::untyped_result_set_row& row,
                                      fts_table_state& ts);

    /// Open Tantivy shard indexes for a newly created (or found on restart)
    /// FTS index on `s`.  Creates new index directories if they do not
    /// exist yet.
    seastar::future<> open_indexes_for_table(schema_ptr s);

    /// Close and release all shard indexes for a table.
    seastar::future<> close_indexes_for_table(table_id tid);

    // ── Data members ─────────────────────────────────────────────────────

    cql3::query_processor& _qp;
    replica::database& _db;

    // Per-table state, keyed by `table_id`.
    std::unordered_map<table_id, fts_table_state> _tables;

    // Alien thread that owns all `ShardIndex` objects.
    std::unique_ptr<alien_thread_runner> _alien;

    // Periodic CDC poll timer (default interval: 5 s).
    seastar::timer<seastar::lowres_clock> _poll_timer;

    // Periodic Tantivy `prune_expired` timer (default interval: 1 h).
    // Removes documents whose TTL has elapsed so on-disk segments do not
    // grow indefinitely with tombstone-only entries.
    seastar::timer<seastar::lowres_clock> _prune_timer;

    // Gate held by every in-flight poll or prune tick.  `stop()` flips
    // `_stopping`, cancels both timers and awaits the gate close so no
    // callback races with destruction of the alien thread runner.
    seastar::gate _poll_gate;

    // Set to true once `stop()` has been called, so freshly fired timer
    // callbacks skip any further work.
    bool _stopping{false};

    // Guard against concurrent poll ticks if the previous one is still
    // running (e.g. under very heavy CDC log load).
    bool _poll_running{false};
};

} // namespace db::fts
