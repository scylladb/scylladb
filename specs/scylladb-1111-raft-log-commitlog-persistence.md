# Feature Specification: SCYLLADB-1111 Commitlog-Backed Raft Log Persistence for Strongly Consistent Tablet Groups

## 0. Metadata
- Ticket: SCYLLADB-1111
- Status: done
- Authors: Core Cluster Team (drafted by OpenCode)
- Created: 2026-04-08
- Updated: 2026-04-08

## 1. Problem and Goals
- Current behavior:
  - Raft persistence for strongly consistent tablet groups uses `service/strong_consistency/raft_groups_storage.cc`, which stores Raft log entries in `system.raft_groups` table rows and stores vote/term, commit index, and snapshot metadata in `system.raft_groups*` tables.
  - This path serializes persistence operations via `_pending_op_fut` and reloads persisted log state from tables during server startup.
  - Group0 uses a different storage path (`service/raft/raft_sys_table_storage.cc`) and is not part of this ticket scope.
- Problem:
  - Persisting per-tablet Raft log entries in system tables adds avoidable write amplification and startup/query overhead for a high-cardinality set of Raft groups.
  - The target design direction for tablet Raft groups is commitlog-backed durable log persistence/replay; table-backed log-entry storage is an interim implementation.
  - SCYLLADB-1111 requires durable, ordered, replayable Raft log persistence across restarts/failures without introducing temporary table-storage fallback behavior.
- Goals:
  - Move strongly consistent tablet-group Raft **log entry** persistence from `system.raft_groups` rows to a dedicated Raft commitlog domain.
  - Reduce write-path I/O by avoiding table-backed Raft log-entry writes and reusing commitlog append durability.
  - Recover tablet-group Raft logs through standard commitlog replay at startup rather than table scans.
  - Preserve existing Raft persistence ordering/linearization guarantees (`store_log_entries` vs `truncate_log`) and startup recovery correctness.
  - Keep vote/term, commit index, and snapshot descriptor/config persistence in existing `system.raft_groups*` metadata tables for this iteration.
  - Provide one-way upgrade safety from existing table-backed data via one-time import (migration), not long-lived fallback.
- Non-goals:
  - Migrating group0 (`system.raft*`) log-entry persistence in this ticket.
  - Implementing commitlog segment compaction and advanced memory-pressure reclamation policies (tracked separately).
  - Introducing a temporary dual-write or runtime fallback mode that keeps table-backed log-entry persistence active after cutover.

## 2. Scope, Compatibility, and Lineage
- In scope:
  - Replace `raft_groups_storage::store_log_entries/load_log/truncate_log` log-entry backend with commitlog-based append/replay/release logic for strongly consistent tablet groups.
  - Extend commitlog entry format handling with segment-versioned decoding:
    - segment v4 (`legacy`): `mutation_entry` only
    - segment v5 (`variant`): `variant<raft_commit_log_entry, mutation_entry>`
  - Define the commitlog versioning transition explicitly:
    - new segments are written with v5 headers (global `current_version` after this change)
    - replay accepts both v4 and v5 segment headers and dispatches decoding by descriptor version
  - Keep `store_term_and_vote`, `store_commit_idx`, `store_snapshot_descriptor`, and corresponding loads table-backed in `system.raft_groups*`.
  - Introduce per-shard Raft commitlog domain lifecycle (init/start/stop/replay) for strongly consistent groups and wire it from groups-manager startup/shutdown.
  - Add one-time import of legacy table-backed log rows (`system.raft_groups` data rows) into commitlog-backed state during node startup, then stop using table rows for log-entry persistence.
  - Add/update tests for restart recovery, truncation ordering, and migration behavior.
- Out of scope:
  - Group0 persistence backend conversion.
  - Long-term segment packing/compaction and low-memory rewriting optimizations.
  - Bidirectional downgrade compatibility after commitlog cutover.
- Implementation authority:
  - `this-spec-only`
- Compatibility impact:
  - Protocol/API: None (no external CQL/native protocol or Raft RPC interface changes).
  - Schema/storage format: `system.raft_groups` log-entry rows (`term/index/data`) are no longer the authoritative Raft log source for strongly consistent groups after cutover; vote/term, commit index, and snapshot metadata remain in `system.raft_groups*` tables. Commitlog introduces v5 variant payload support (`raft_commit_log_entry` + `mutation_entry`) and keeps replay compatibility with legacy v4 mutation-only segments.
  - Configuration/flags: No new user-facing flags in this ticket. Emission of Raft variant payloads is gated by `STRONGLY_CONSISTENT_TABLES`; non-SC domains continue emitting mutation-only commitlog entries.
  - Mixed-version behavior (upgrade/downgrade): Rolling upgrade is supported one-way via startup import from legacy table log rows into commitlog-backed state on upgraded nodes. Replay accepts v4 and v5 segments by descriptor version during transition. Downgrade to binaries that require table-backed log-entry persistence is not supported after a node has cut over.
  - Observability/metrics/logging: Add/extend metrics and logs for Raft commitlog append/replay/import/release activity, replay filtering counters (snapshot-covered/superseded/duplicate), and startup replay/import summaries per shard.
- Contract lineage and overlap resolution:
  - `specs/customer-268-raft-tables-enctyption.md` -> no conflict -> encryption coverage for Raft system tables; this spec changes persistence medium for strongly consistent Raft log entries.
  - Confluence page `228163595` (Tablet Raft Group Persistence in Tables) -> supersede -> table-backed log-entry persistence is replaced by commitlog-backed persistence for strongly consistent groups.
  - Confluence pages `206831685` and `218169442` -> refine -> this ticket implements the first production cutover slice (durable append/replay + legacy import) without deferred compaction/reclaimer enhancements.

### 2.1 Deferred Edge Cases and Out-of-Scope Constraints
- Commitlog segment compaction for Raft-group entries -> deferred to SCYLLADB-670 to keep SCYLLADB-1111 focused on correctness/cutover.
- Memory reclamation for commitlog-backed Raft log entries under pressure -> deferred to SCYLLADB-668; memtable-driven `rp_handle` reclamation has no equivalent for uncommitted Raft log ownership in this ticket.
- Replay memory footprint reduction -> deferred to SCYLLADB-668/672; this ticket buffers replayed Raft entries per group before post-processing, which can be memory-heavy on large logs.
- Replay fuzzing and negative-scenario hardening breadth expansion -> deferred to SCYLLADB-672; this ticket includes deterministic recovery/truncation regression coverage only.
- Group0 commitlog persistence conversion -> deferred; this ticket targets strongly consistent tablet-group persistence only.
- Downgrade-safe reverse migration from commitlog back to table rows -> deferred/not supported in this phase.

### 2.2 Documentation Impact
- User-visible behavior docs (`README.md` or equivalent): not required -> no end-user API change; behavior remains internal to strongly consistent Raft persistence.
- Architecture docs (`docs/ARCHITECTURE.md` and topic docs): required -> update `docs/dev/strong_consistency.md` to describe commitlog-backed tablet Raft log persistence, startup replay/import flow, and table-metadata-only role of `system.raft_groups*`.
- Other docs/tests docs: update `docs/dev/system_keyspace.md` Raft-group section to clarify that `system.raft_groups` log-entry rows are no longer the authoritative source after cutover; no separate test-doc file changes required.

## 3. Design and Contracts
- Key files/modules:
  - `service/strong_consistency/raft_groups_storage.hh`
  - `service/strong_consistency/raft_groups_storage.cc`
  - `service/strong_consistency/groups_manager.hh`
  - `service/strong_consistency/groups_manager.cc`
  - `db/commitlog/commitlog.hh`
  - `db/commitlog/commitlog.cc`
  - `db/commitlog/commitlog_entry.hh`
  - `db/commitlog/commitlog_entry.cc`
  - `db/commitlog/commitlog_replayer.hh`
  - `db/commitlog/commitlog_replayer.cc`
  - `idl/commitlog.idl.hh`
  - `replica/database.hh`
  - `replica/database.cc`
  - `main.cc`
  - `test/raft/raft_sys_table_storage_test.cc`
  - `test/cluster/test_strong_consistency.py`
- Architecture/data flow:
  - A dedicated Raft commitlog domain is created per shard for strongly consistent tablet groups (separate filename prefix from main/schema commitlogs).
  - Commitlog segment format is versioned:
    - v4 segments carry `mutation_entry` payloads only.
    - v5 segments carry variant payloads with either `raft_commit_log_entry` or `mutation_entry`.
  - Version transition contract:
    - commitlog segment writer uses v5 for newly created segments after rollout,
    - replay path accepts both v4 and v5 segment headers,
    - decoding is strictly version-driven (no payload heuristics).
  - Variant payload writing for strongly consistent tablet-group log persistence is enabled only when `STRONGLY_CONSISTENT_TABLES` is enabled.
  - Schema commitlog and hints commitlog remain mutation-only payload producers in this ticket (they do not emit `raft_commit_log_entry`).
  - `raft_groups_storage::store_log_entries` serializes and appends each Raft log entry to the shard-local Raft commitlog, retaining returned `rp_handle`s keyed by Raft index for safe release.
  - `raft_groups_storage::truncate_log` and snapshot-tail truncation release affected handles in linearized order, allowing segment reclamation once entries are no longer needed.
  - Startup replay for strongly consistent Raft log entries is implemented in a dedicated strong-consistency persistence path (using `db::commitlog::read_log_file` with Raft-prefix files), not in the generic mutation-applying `db::commitlog_replayer` loop.
  - Startup replay scans Raft commitlog segments for the Raft prefix, decodes entries by segment version, buffers Raft entries per group, applies replay filtering, performs one-time import from legacy table rows when needed, and exposes the resulting log to `load_log()` before groups begin serving.
  - Replay post-processing filters:
    - drop entries covered by persisted snapshot index,
    - drop superseded entries for overwritten leader epochs (term/index conflict resolution),
    - drop duplicate entries (e.g., partial/overlapping segment replay cases).
  - One-time import authority and idempotency:
    - For each group, startup first computes commitlog-replayed log state and `replayed_max_idx`.
    - Startup also computes `legacy_max_idx` from legacy `system.raft_groups` rows for that group.
    - Legacy table-row import runs when `legacy_max_idx > replayed_max_idx` and appends only the missing legacy suffix (`idx > replayed_max_idx`) in index order.
    - After successful import append, legacy `system.raft_groups` log rows (`term/index/data`) for the group are deleted.
    - If crash happens during import append, the next startup recomputes both maxima and resumes from the remaining missing suffix; if crash happens after append but before cleanup, cleanup is retried opportunistically.
  - Shard/async boundaries:
    - Groups and persistence remain shard-local (`groups_manager` + `raft_groups_storage` per shard).
    - No cross-shard persistence RPCs are introduced.
    - Replay/import runs asynchronously during shard startup before strongly consistent group service readiness.
  - Object lifetime boundaries:
    - Raft commitlog domain lifetime is tied to strongly consistent groups-manager lifecycle on each shard.
    - `rp_handle` ownership is tied to `raft_groups_storage` instance lifetime and is released on truncation/snapshot/abort/destruction.
    - Replay/import caches are startup-scoped and cleared after groups are initialized.
- Interfaces changed/added:
  - `raft_groups_storage` constructor gains access to the shard-local Raft commitlog domain.
  - `raft_groups_storage::store_log_entries/load_log/truncate_log` internals switch from table-row CRUD to commitlog append/replay/release behavior.
  - `groups_manager` startup/shutdown adds initialization and teardown of the Raft commitlog domain and replay/import orchestration.
  - Commitlog segment format handling adds v5 variant decode/encode path while keeping v4 legacy decode support.
  - `raft_groups_storage` startup flow adds explicit legacy-import reconciliation and legacy-row cleanup logic with idempotent restart semantics.
  - No external API or RPC signature changes.
- Contract obligations (for changed behavior surfaces):
  - `POST-001`: For each invocation of `raft_groups_storage::store_log_entries(entries)`, every input entry is durably appended to the shard-local Raft commitlog before the future resolves successfully.
  - `POST-002`: `raft_groups_storage::load_log()` reconstructs an ordered, contiguous Raft log consistent with persisted append/truncate/snapshot effects and serves it to `raft::server::start()`.
  - `POST-003`: On upgraded nodes with legacy table-backed log rows, startup imports legacy rows into commitlog-backed state when replayed log is behind legacy rows, and appends only the missing suffix (`legacy_idx > replayed_max_idx`).
  - `POST-004`: Replay decoding is descriptor-version aware: v4 segments are parsed as legacy mutation-only entries, v5 segments are parsed as variant entries; parsing rules are deterministic and independent of runtime heuristics.
  - `POST-005`: Replay output for each group excludes snapshot-covered entries, superseded term/index entries, and duplicates before serving `load_log()`, with deterministic precedence: commitlog records are applied in descriptor-id order and in-file order; for repeated index values in the replay stream, the last applied record is authoritative (including truncate/truncate-prefix effects).
  - `POST-006`: After successful legacy import, legacy table log rows are cleaned up; restart after partial import append or cleanup failure resumes from the missing suffix and does not create duplicate entries.
  - `INV-001`: Vote/term, commit index, and snapshot descriptor/config persistence semantics remain unchanged and continue using `system.raft_groups*` metadata tables.
  - `INV-002`: After cutover for a strongly consistent group, table row writes/deletes for Raft log entries (`term/index/data` rows in `system.raft_groups`) are not part of normal persistence flow.
  - `INV-003`: Schema and hints commitlog domains remain mutation-only entry producers in this ticket; `raft_commit_log_entry` payload usage is confined to strongly consistent tablet-group persistence path.
  - `CONC-001`: `store_log_entries`, `truncate_log`, and snapshot-tail release operations remain linearized by invocation order for each group, preserving Raft persistence ordering guarantees from `raft::persistence` contract.
  - `CONC-002`: `abort()` waits for in-flight linearized persistence operations and releases retained handles owned by the storage instance without leaking segment pins.
- Simplicity and rejected alternatives:
  - Minimal design statement: convert only the high-volume Raft log-entry path for strongly consistent groups to commitlog, keep existing table-backed metadata path unchanged, and add one-way startup import for upgrade safety.
  - Rejected alternative: dual-write/dual-read temporary fallback to table-backed log persistence -> rejected because it materially increases correctness surface (precedence rules, rollback semantics, and mixed persistence arbitration) outside this ticket.
  - Rejected alternative: global shared-commitlog entry-format overhaul in this ticket -> rejected due larger cross-subsystem blast radius; this ticket limits itself to strongly consistent Raft persistence cutover.
- Concurrency/performance notes:
  - Hot-path persistence keeps shard-local execution and existing linearization approach.
  - Startup replay is done once per shard over Raft-prefixed segments; per-group startup consumes this replayed state, with per-group file rescan only as a fallback path when pre-replayed state is unavailable.
  - Segment reclamation efficiency improvements beyond correctness-preserving handle release are deferred (Section 2.1).

```cpp
// Contract boundary sketch: per-group linearization is preserved.
future<> raft_groups_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    return execute_with_linearization_point([this, &entries] {
        // append to raft commitlog domain, capture rp_handles by raft index
        return do_store_log_entries(entries);
    });
}
```

## 4. Acceptance Criteria
Concrete Given/When/Then scenarios that map directly to tests and contracts.

- Scenario 1: Durable append and restart recovery (covers: `POST-001`, `POST-002`, `INV-001`)
  - Given: A strongly consistent tablet Raft group with empty persisted log on shard S.
  - When: Commands are committed, node crashes, and node restarts.
  - Then: `load_log()` reconstructs the committed/uncommitted Raft entries in order from Raft commitlog replay and server startup proceeds without reading table-backed log rows.

- Scenario 2: Truncation ordering across conflicts (covers: `POST-002`, `CONC-001`)
  - Given: Persisted entries at indices 1..N and a subsequent `truncate_log(K)` due to leader conflict resolution.
  - When: New entries are appended after truncation and node restarts.
  - Then: Recovered log contains only post-truncation authoritative entries; truncated tail entries do not reappear.

- Scenario 3: Snapshot-tail release behavior (covers: `POST-002`, `CONC-001`, `CONC-002`)
  - Given: Persisted log entries and snapshot descriptor persisted with `preserve_log_entries = P`.
  - When: Snapshot persistence truncates eligible tail and node restarts.
  - Then: Recovered log retains only expected trailing entries and released handles allow old segments to become reclaimable.

- Scenario 4: Legacy table import once, no runtime fallback (covers: `POST-003`, `INV-002`)
  - Given: Upgraded node with legacy `system.raft_groups` log rows for a group and no existing commitlog-backed entries for that group.
  - When: Node starts with the new binary.
  - Then: Startup imports legacy rows into commitlog-backed state, subsequent persistence uses commitlog only, and later restarts do not depend on table log rows.

- Scenario 5: Versioned replay and filtering (covers: `POST-004`, `POST-005`)
  - Given: Replay input contains mixed segment versions (v4 and v5) and overlapping/superseded Raft entries for a group.
  - When: Startup replay completes.
  - Then: Decoding follows segment version contract and final loaded log excludes snapshot-covered, superseded, and duplicate entries.

- Scenario 6: Non-SC domain payload isolation (covers: `INV-003`)
  - Given: Node starts with strongly consistent tables enabled and both schema/hints commitlogs present.
  - When: Commitlog replay runs for all domains.
  - Then: Schema and hints domains remain mutation-only entry domains; `raft_commit_log_entry` payloads are handled only in the strongly consistent Raft path.

- Scenario 7: Import restart idempotency after partial import/cleanup failure (covers: `POST-003`, `POST-006`)
  - Given: Startup begins importing legacy rows for a group and crashes either (a) between import batches or (b) after import append before deleting legacy table log rows.
  - When: Node restarts and replay/import logic runs again.
  - Then: Import resumes from the missing suffix (`legacy_max_idx > replayed_max_idx`) without duplicating already imported entries, and legacy-row cleanup is retried.

## 5. Test Plan and Validation
- Contract/scenario to test mapping:
  - `POST-001` / Scenario 1 -> `test/raft/raft_sys_table_storage_test.cc::test_groups_store_load_log_entries`
  - `POST-002`, `CONC-001` / Scenario 2 -> `test/raft/raft_sys_table_storage_test.cc::test_groups_truncate_log`
  - `POST-002`, `CONC-001`, `CONC-002` / Scenario 3 -> `test/raft/raft_sys_table_storage_test.cc::test_groups_store_snapshot_truncate_log_tail`, `test/raft/raft_sys_table_storage_test.cc::test_groups_store_snapshot_preserve_tail_restart_recovery`
  - Crash-window regression for snapshot/truncate-prefix replay filtering (supports Scenario 3, `POST-002`) -> `test/raft/raft_sys_table_storage_test.cc::test_groups_filter_replayed_truncate_prefix_without_snapshot`
  - Underflow regression for snapshot preserve-tail math (supports Scenario 3, `POST-002`) -> `test/raft/raft_sys_table_storage_test.cc::test_groups_store_snapshot_preserve_entries_underflow_guard`
  - `INV-001` -> `test/raft/raft_sys_table_storage_test.cc::test_groups_store_load_term_and_vote`, `test/raft/raft_sys_table_storage_test.cc::test_groups_store_load_snapshot`, `test/raft/raft_sys_table_storage_test.cc::test_groups_storage_shard_isolation`
  - End-to-end crash/restart correctness -> `test/cluster/test_strong_consistency.py::test_sc_persistence_after_crash`
  - End-to-end restart with shard topology changes -> `test/cluster/test_strong_consistency.py::test_sc_persistence_restart_with_smp_increase`
  - `POST-003`, `POST-006` / Scenarios 4, 7 -> `test/raft/raft_sys_table_storage_test.cc::test_groups_storage_legacy_import_restart_idempotent` (to add)
  - `POST-004`, `POST-005` / Scenario 5 -> `test/boost/commitlog_test.cc::test_commitlog_mixed_v4_v5_replay_for_raft_entries` (to add)
  - `INV-002` / Scenario 4 -> `test/raft/raft_sys_table_storage_test.cc::test_groups_storage_legacy_table_import_once` (to add)
  - `INV-003` / Scenario 6 -> `test/boost/commitlog_test.cc::test_schema_and_hints_commitlog_remain_mutation_only_under_sc_feature` (to add)
- Unit tests (C++):
  - Update `test/raft/raft_sys_table_storage_test.cc` to cover commitlog-backed replay, truncate ordering, snapshot-tail behavior, and crash-window/underflow regressions.
  - Keep `test/boost/commitlog_test.cc` exercising commitlog reader/writer paths with segment-version threading (`buffer_and_replay_position.segment_version`) in existing add/read cases.
- Integration tests (Python, if needed):
  - Extend `test/cluster/test_strong_consistency.py` with a dedicated restart test that verifies Raft-group progress survives crash/restart with commitlog-backed persistence and that normal writes continue.
- Stability (if needed):
  - Run new crash/restart integration case with `--repeat 100 --max-failures 1`.
- Validation commands:
  ```bash
  ./configure.py --mode=dev
  ninja build/dev/scylla
  ninja build/dev/test/raft/raft_sys_table_storage_test
  ninja build/dev/test/boost/commitlog_test

  ./test.py --mode=dev test/raft/raft_sys_table_storage_test.cc
  ./test.py --mode=dev test/boost/commitlog_test.cc
  ./test.py --mode=dev test/cluster/test_strong_consistency.py::test_sc_persistence_after_crash -v
  ./test.py --mode=dev test/cluster/test_strong_consistency.py::test_sc_persistence_restart_with_smp_increase -v
  ./test.py --mode=dev test/cluster/test_strong_consistency.py::test_sc_persistence_after_crash --repeat 100 --max-failures 1 -v
  ```
- Known uncovered paths and rationale:
  - Dedicated automated coverage for `POST-003`/`POST-006` (legacy import once + restart-idempotent cleanup) is not yet present in this draft and must be added before moving spec status to `done`.
  - Dedicated automated coverage for explicit mixed v4/v5 Raft payload replay (`POST-004`) and schema/hints isolation assertion (`INV-003`) is not yet present as standalone named tests in this draft and must be added before moving spec status to `done`.
  - Extreme disk-full and long-tail segment-fragmentation behavior under sustained pressure is not fully covered here; these are addressed by deferred SCYLLADB-668/670 follow-up scopes.
  - Replay peak-memory behavior for very large per-group buffered Raft logs is not fully stress-tested in this ticket; deferred to follow-up work in Section 2.1.

## 6. Risks and Rollout (Optional - include for behavior changes or risky refactors)
- Risks:
  - Replay/import bugs could cause dropped or duplicated Raft entries during restart.
  - Incorrect handle release ordering could keep segments pinned longer than expected.
  - Replay buffering of all Raft entries per group before post-processing can cause high memory usage and potential OOM in large log windows.
  - Startup replay/import time could increase on shards with large historical Raft commitlog volume.
  - One-way upgrade semantics can surprise operators if downgrade is attempted after cutover.
- Mitigations:
  - Enforce strict contract tests for replay ordering, truncate precedence, and one-time import behavior.
  - Enforce mixed-format v4/v5 replay tests and filtering correctness tests.
  - Keep per-group linearization logic unchanged and explicitly test `store_log_entries` vs `truncate_log` ordering.
  - Emit replay/import metrics and startup summary logs to support operability/debugging.
  - Document one-way upgrade semantics in architecture docs and release notes.
- Rollout:
  - Immediate rollout for strongly consistent tablet groups with one-time startup import on upgraded nodes.
  - No runtime fallback path; commitlog-backed persistence is authoritative after cutover.
- Rollback:
  - Binary downgrade after cutover is unsupported for affected nodes.
  - Operational rollback requires restoring node data from pre-cutover backup/snapshot or rolling forward with a fix.

## 8. Definition of Done
- [ ] Acceptance criteria implemented
- [ ] Tests added/updated and passing (`--repeat 100` for new tests when applicable)
- [ ] Validation commands executed successfully
- [ ] Compatibility impact in Section 2 verified
- [ ] Required documentation from Section 2.2 updated
- [ ] Spec status updated to `done`

## 9. Agent Scratchpad
<!-- Leave empty when authoring the spec. OpenCode fills this during execution. -->

- Same-chat fallback gate token:
  - `[SPEC_REVIEW_RESULT] path=specs/scylladb-1111-raft-log-commitlog-persistence.md verdict=APPROVE command=/spec-review`
- Implementation steps:
  - 1) Add commitlog v5 support and version-aware replay payload dispatch (v4/v5) in commitlog core readers.
  - 2) Extend commitlog payload model with Raft entry variant and keep mutation-only producers for non-SC domains.
  - 3) Introduce SC raft commitlog domain and replay path in strongly-consistency code.
  - 4) Rework `raft_groups_storage` log entry persistence/truncation/load to commitlog-backed data + `rp_handle` ownership.
  - 5) Implement legacy table import (missing suffix), cleanup, and idempotent restart semantics.
  - 6) Update tests in `test/raft/raft_sys_table_storage_test.cc` and `test/boost/commitlog_test.cc`.
  - 7) Update docs (`docs/dev/strong_consistency.md`, `docs/dev/system_keyspace.md`).
- File order:
  - `idl/commitlog.idl.hh`
  - `db/commitlog/commitlog.hh`
  - `db/commitlog/commitlog.cc`
  - `db/commitlog/commitlog_entry.hh`
  - `db/commitlog/commitlog_entry.cc`
  - `db/commitlog/commitlog_replayer.cc`
  - `db/hints/internal/hint_sender.cc`
  - `service/strong_consistency/groups_manager.hh`
  - `service/strong_consistency/groups_manager.cc`
  - `service/strong_consistency/raft_groups_storage.hh`
  - `service/strong_consistency/raft_groups_storage.cc`
  - `test/raft/raft_sys_table_storage_test.cc`
  - `test/boost/commitlog_test.cc`
  - `docs/dev/strong_consistency.md`
  - `docs/dev/system_keyspace.md`
