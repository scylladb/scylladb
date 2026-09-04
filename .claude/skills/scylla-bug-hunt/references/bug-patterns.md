# Bug pattern checklist

Every Research-stage agent should actively look for these, not just report
whatever it happens to notice while reading. The four reference scale
points below must be applied as explicit stress lenses to every candidate
in category 3 — "is this still fine at 1000 tables / 100,000 tablets /
100,000 sstables per node / 10,000 CQL connections?" is the question that
turns a vague "this could be faster" into a real finding.

## 1. Logical bugs

- Off-by-one or wrong-inclusivity on clustering-key / token ranges.
- Tombstone, TTL, or expiry handling that's correct in the common case but
  wrong at a boundary (just-expired, just-purged, concurrent write during
  compaction of the same partition).
- Races: state read across a `co_await` without re-validating it's still
  valid; a fiber holding a reference that outlives the object; a callback
  registered before the state it captures is fully initialized.
- Exceptions swallowed, downgraded, or logged-and-ignored where they
  should abort an operation or propagate.
- Incorrect merge/reduce logic when combining results from multiple
  shards/replicas/sstables (e.g. wrong tie-break, wrong precedence between
  live data and tombstone).
- Schema-version or topology-version mismatch handled inconsistently
  between two code paths that should agree.
- Retry logic that isn't actually idempotent, or idempotency assumed
  without a check.

## 2. Performance issues (not scale-dependent)

- Unnecessary copies of large structures (by value where a reference or
  move would do; `shared_ptr` copy in a hot loop).
- Redundant recomputation inside a loop that's invariant across
  iterations.
- Blocking or CPU-heavy work on a reactor thread that should have yielded.
- Serialization/deserialization done more than once for the same data on
  a single request path.
- Missing move semantics causing avoidable allocation.

## 3. Scale issues — the core of this skill

Apply all four lenses below to every candidate before deciding it's worth
a reproducer. A pattern that's technically O(n) but bounded by a small,
practically-fixed n (e.g. number of compaction strategies, not number of
tables) is not a finding — say so and move on.

- **1000+ tables**: anything iterated, locked, or broadcast once per table
  on a path that should be per-request or per-shard instead. A schema
  change, topology change, or periodic timer that does O(tables) work
  where O(1) or O(changed tables) would do.
- **100,000+ tablets**: tablet-map or tablet-metadata structures that are
  scanned linearly instead of indexed/hashed; gossip or control-plane
  messages whose size grows with tablet count instead of being paginated
  or delta-encoded; load-balancer or migration logic with per-tablet
  overhead that isn't amortized.
- **100,000+ sstables per node**: per-sstable bookkeeping (bloom filter
  registries, backlog trackers, compaction candidate selection) that
  rebuilds or fully rescans on every compaction decision instead of
  incrementally updating; index/summary structures whose lookup cost grows
  with sstable count where it shouldn't.
- **10,000+ CQL client connections**: per-connection state that isn't
  amortized (a full prepared-statement cache duplicated per connection
  instead of shared; a per-connection timer or buffer sized for one
  connection but multiplied unbounded); a global lock or single-shard
  bottleneck that every connection contends on regardless of which shard
  it's actually talking to.

General shapes to look for, then map onto the four lenses above:
- A container keyed by table/tablet/sstable/connection that's never
  trimmed and is walked in full on a hot or periodic path.
- A global mutex/lock whose hold time or acquisition frequency scales with
  one of the four counts.
- A config default, timeout, or fixed-size buffer that was sized assuming
  a small N and becomes wrong (too slow, too small, too many retries) once
  N is large.

## Reproducing a scale finding without a scale-sized cluster

Don't provision the literal scale point. Isolate the data structure or
algorithm in a unit/boost test and demonstrate the wrong growth curve
between a small N (fast to construct, e.g. 100) and a larger but still
test-feasible N (e.g. 5,000–20,000): assert that operation count or
wall-clock time grows roughly linearly (or worse) between the two when the
code's contract implies constant or logarithmic growth. A ratio check
(time(N2)/time(N1) vs N2/N1) is usually enough; it doesn't need to reach
the literal 100,000 to prove the shape is wrong.

## Environment notes carried over from prior work in this repo

- Boost test reproduction: run one case per process with the
  `test_config.yaml` args; `sstable_datafile_test` S3/GCS failures are
  environmental, not something you're reproducing.
- `test.py` needs `--no-gather-metrics`, or a cgroup `PermissionError`
  (rc=3) will look like a failure when it isn't.
- Worktree builds need a dedicated `SCCACHE_SERVER_PORT`: run
  `dbuild env SCCACHE_SERVER_PORT=<free port> ninja ...` inside the
  worktree, or sccache serializes on unhashable files and the build takes
  minutes longer than it should.
- Don't run this skill's reproduction stage concurrently with any other
  build or benchmark on the host — CPU contention from a second build
  reads as a fake regression in whichever one is actually measuring
  something.
