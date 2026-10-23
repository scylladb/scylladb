---
name: code-review
description: >-
  Review pending ScyllaDB changes (working-tree diff, staged changes, or a
  commit range) for correctness, performance, and adherence to project
  C++/Python conventions. Use when asked to "review", "code review", or check a
  diff/PR/branch before it is committed or merged.
---

# ScyllaDB Code Review

Review changed ScyllaDB code against project conventions and flag defects.

The instructions here do not override standard review practices for
correctness, performance, and readability. Rather they are additional points
to consider.

## When to use

- The user asks to review a diff, PR, branch, or commit range.
- Before committing or pushing, as a self-check.

## Prerequisites: load project conventions

Read the relevant instruction files before reviewing (do not load all at once):

- **Always:** `.github/copilot-instructions.md` — build/test commands, code &
  test philosophy, new-file rules (SPDX header, license ref).
- **If the diff touches `*.cc` / `*.hh`:** `.github/instructions/cpp.instructions.md`
  — memory management, Seastar patterns, error handling, naming, formatting,
  forbidden constructs.
- **If the diff touches `*.py`:** `.github/instructions/python.instructions.md`.

These files are the source of truth for style. This skill only adds review
*process* and *emphasis* on top of them.

## Determine the review scope

Identify what to review, in order of preference:

1. Explicit range/PR the user named.
2. Staged changes: `git diff --cached`.
3. Working-tree changes: `git diff`.
4. Branch vs. `master`: typically `git diff origin/master...HEAD` (for a whole feature branch).

Review per-commit when the branch has multiple commits — ScyllaDB requires
**bisectability**: every commit must build and pass tests on its own. Verify by
inspection, not by running the whole thing.

> Note: the working tree may contain a lot of untracked scratch files (cores,
> tarballs, benchmark logs). Review only relevant untracked files if present,
> ask for clarification if needed.

## Review dimensions

Rank findings most-severe first. Verify finding before reporting. Report by file:line,
group by severity and topic, suggest a fix if simple. Consider at least:

### Exposition and collateral
- Why vs what: each commit should explain why it is doing what it does, not just
  what (unless trivial).
- Note: single-patch series lose the cover letter when getting merged, so the commit
  message should include everything.
- Series title and patch titles should be descriptive, in imperative present tense.
- Series description (cover letter) should contain examples (for user-facing changes)
  and performance measurements (for hot-path changes). Performance collateral can include
  perf-simple-query run in relevant mode (or other microbenchmark). Prefer insn/op to
  throughput to isolate CPU frequency fluctuations. Note: ignore this for in-progress
  work that does not yet have a cover letter.
- New tests should come with a declaration of how long they take to run, and something
  about their stability.
- Changes that affect compatibility should explain how mixed-cluster operation and
  downgrades are supported.

### Compatibility
- Maintain dialect compatibility for CQL and Alternator with previous versions.
- Maintain mixed-version compatibility: IDL should be extended, not changed. New verbs
  should be guarded by cluster features. The guards should be at the entry point: the entry
  point checks cluster feature and sets a per-request flag to use the new feature, rather
  than every node checking for itself.
- Maintain disk-format compatibility, and system-schema compatibility.
- CQL should maintain compatibility with Cassandra where possible, and keep the spirit
  of SQL as much as possible. Where Cassandra made unreasonable choices in obscure corner
  cases, it is okay to diverge.

### Documentation
- New CQL features should be documented in the CQL reference and in docs/cql/cql-extensions.md.
- Protocol extensions should be documented in docs/dev/protocol-extensions.md.

### Correctness
- Logic errors, off-by-one, missing error paths.
- Exception safety across `co_await` suspension points; futures not discarded. Every asynchronous
  operation is eventually waited on (for example by service stop).
- Cross-shard safety: no shared mutable state without message passing; correct
  use of `foreign_ptr`.
- Resource accounting: limit memory consumption of asynchronous operations, by limiting concurrency
  (in simple cases) or accounting for resources (in complex cases with high variability).
- Bisectability: does each commit build and pass its own tests?

### Performance (weigh heavily in hot paths — data read/write, inner loops)
- Unnecessary copies of large objects; missing `const&` / `&&` / `string_view`.
- Containers that might host large amounts of data should use chunked variants
  to avoid exceeding 128kB allocations. Similarly managed_bytes vs contiguous blobs.
- Reactor stalls: long loops without `maybe_yield()`; blocking calls.
- "Expected exceptions" - e.g. timeouts. Should use boost::outcome.

### Cluster management
- The design point is hundreds of nodes, ten thousand shards, hundreds of thousands of
  tablets. Memory consumption for metadata should not affect a shard's normal role as
  a coordinator/replica shard.
- Every cluster operation should be recoverable. Register intent to perform, push the
  operation forward, mark the operation as complete. A failover to another leader will
  continue the work. The old leader will be fenced away.
- New configuration items should use the cluster config facility (docs/cql/cluster-config.rst),
  not scylla.yaml.

### Observability
- Major features need observability, as virtual tables, nodetool commands, or metrics
  (or a combination) depending on whether state evolution or current complex state
  needs to be presented. Metrics are an expensive resource so need to balance metric
  count.

### Build
- Keep header entanglements to a minimum.
- Keep configure.py and cmake build systems in sync.
- Use forward declarations to reduce rebuilds.
- Avoid implementations in headers for anything but trivial functions. LTO/PGO allows
  inlining across translation units.
- Avoid layering violations (note: a module layering document will be provided later).
- External dependencies should be pinned to a specific version (if downloaded dynamically)
  or frozen in the frozen toolchain image (if installed from Fedora).
- Avoid macros, with a view to future conversion of the tree to C++20 modules.

### Tests
- Bug fixes include a regression test that fails before / passes after, with the
  GitHub/JIRA issue referenced in a comment.
- New tests are stable (run `--repeat 100 --max-failures 1`), quick, no sleeps,
  deterministic (no unseeded random), minimal resources (single node if enough).
- Debug-mode iteration counts reduced where needed.

### New-file hygiene
- SPDX header with `LicenseRef-ScyllaDB-Source-Available-1.1`, current year.
- Files imported from other projects should retain attribution and license, add
  entry to licenses/.

