# SESSION — FTS review follow-ups

Items discovered or deliberately left open while applying the FTS commits
review remediation plan (see
`.cursor/plans/fts_commits_review_fixes_8dc2cca3.plan.md`).  Each entry
is small enough to be picked up as a separate follow-up commit.

## Correctness / completeness

### FTS checkpoint persistence (H1)
The per-table CDC poll checkpoint is now stored as a `utils::UUID`
(`fts_table_state::checkpoint_uuid`) and used as a strict-greater-than
lower bound on every poll, which preserves sub-millisecond resolution
(H3 in the plan).  It is still **in-memory only** — a process restart
discards every checkpoint and replays the CDC log from the start,
capped by the CDC log TTL (24 hours by default).

Deferred because adding a `system.fts_index_checkpoints` table would
require:

  * a new schema declaration in `db::system_keyspace::all_tables()`,
  * a versioned migration path so older nodes can tolerate the new table,
  * the read on startup to happen after `system_keyspace` is fully
    initialised but before the first `fts_cdc_consumer::start()` runs.

A cheaper interim option is to use
`system_keyspace::set_scylla_local_param_as<utils::UUID>(...)` with
keys of the form `fts_checkpoint:<table_uuid>`.  This avoids the schema
work entirely; the caller would need a reference to
`db::system_keyspace`, which means threading one more constructor arg
through `fts_cdc_consumer`.

### FTS index coverage vs. CREATE INDEX targets list
`fts_index::validate` now rejects non-indexable, primary-key and
static-column targets with clear error messages, but the runtime indexer
in `fts_cdc_consumer::open_indexes_for_table` still maps every
FTS-indexable regular column — the `(col1, col2)` list in `CREATE INDEX
... ON tbl (col1, col2)` is consulted only for MATCH eligibility
(`has_fts_index_on_column`).

This is documented in the header comment of `index/fts_index.hh`.  A
future change should either:

  * persist a `field_mapping` JSON option at CREATE INDEX time and
    consume it at open time, restricting the index to the explicit
    targets list, OR
  * remove the targets list from FTS CREATE INDEX syntax altogether and
    document that an FTS index covers the whole row.

### FTS index options plumbing (M7)
`commit_interval_ms` and `prune_interval_ms` are parsed and validated at
CREATE INDEX time but the consumer uses hardcoded constants
(`POLL_INTERVAL_MS = 5 s`, `PRUNE_INTERVAL_MS = 1 h`).  The OPTION
values are silently ignored.  Either thread the OPTIONS through to the
consumer or remove them from `check_index_options` so the documentation
matches the behaviour.

### FTS static-column support (H4)
`fts_index::validate` now rejects static columns at CREATE INDEX time
because the consumer only iterates `regular_columns()`.  Supporting
static targets requires extending `open_indexes_for_table` and
`process_cdc_row` to also walk `static_columns()`, plus a story for
single-shard static-only updates (which would only ever arrive on one
shard's CDC log).

### Configurable poll/prune intervals (M7 / perf)
The 5-second minimum indexing latency is currently baked into the
consumer.  Make `POLL_INTERVAL_MS` an index OPTION once M7 is wired so
that latency-sensitive workloads can opt into faster polling.

## Performance / design notes

  * **Per-shard OS thread for `alien_thread_runner`** — one extra
    pthread per shard.  Probably fine, but document in the FTS user
    guide once we publish one.
  * **Sequential per-table CDC poll** — one slow table blocks the next
    table's poll within the same tick.  Could become
    `coroutine::parallel_for_each` once we are confident the alien
    worker can absorb the higher concurrency.
  * **Hex-encoded doc IDs are 2× key size**; base64 encoding would
    halve the on-disk Tantivy term size for sound keys.
  * **Index path under `<data_dir>/fts_indexes/`** ignores any per-disk
    SSTable layout; tablet migration will not move the index along
    with the data.  Needs to integrate with the multi-disk allocator
    once tablet-aware FTS becomes a goal.
  * **`alien::run_on` `set_value` after a cancelled `seastar::promise`** —
    if the shard is shutting down while a slow Tantivy call is still
    running we currently swallow the `broken_promise`.  Consider
    propagating shutdown as a cancellation token instead.

## Test debt

  * `test/cqlpy/test_fts.py::test_match_returns_hits_across_shards` is
    marked `xfail` because it needs a multi-shard cqlpy harness;
    enable in the appropriate CI lane.
  * No Rust unit test for the IPv4/IPv6 IP-address ingestion path yet;
    schema.rs handles the fall-through but the FFI-level conversion
    has no coverage.
  * No coverage for the BatchStatement path: bulk `BEGIN BATCH ... APPLY
    BATCH` writes against an FTS-indexed table should be smoke-tested
    once batch CDC semantics are documented.
