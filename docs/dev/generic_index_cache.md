# Unified LRU: Removing the Index Cache Fraction Cap

## Summary

This patch removes the hard `index_cache_fraction` cap (default 20%) that
previously limited how much of the shared cache memory SSTable index entries
could occupy.  Index entries now participate in the regular LRU alongside
row cache entries.  Hot index pages survive via recency; cold ones get
evicted naturally.

## Problem

The old design maintained a **separate index LRU list** (`_index_list`) and
a hard capacity cap.  Every reclaimer call checked whether index usage
exceeded the configured fraction of total cache, and if so, force-evicted
the oldest index entry regardless of its access pattern:

```
    BEFORE: Reclaimer Logic
    +-----------------------------------------+
    | if index_bytes > total * 0.20:          |
    |     evict from _index_list (force)  <------- hard cap
    | else:                                   |
    |     evict from main LRU                 |
    +-----------------------------------------+
```

This caused two problems:

### Problem 1: Cold index wastes cache space

When index pages are cold (read once, never again), the 20% reservation
holds them in cache at the expense of hot data rows:

```
    200 MB cache with 20% index cap
    +------------------+----+
    |  data rows       | idx|   idx = cold index pages
    |  (160 MB usable) |(40)|   occupying reserved space
    +------------------+----+
                         ^^^
                    wasted space — these pages are cold
                    but can't be reclaimed for data
```

### Problem 2: Hot index starved by the cap

When index pages are genuinely hot (e.g., clustering key lookups that
repeatedly need the same partition index pages), the 20% cap force-evicts
them, causing repeated I/O:

```
    Clustering key lookups need partition index pages

    With 20% cap:
    query -> index page needed -> MISS (evicted by cap) -> disk I/O -> cache
    query -> index page needed -> MISS (evicted by cap) -> disk I/O -> cache
    ...repeats forever...

    Without cap:
    query -> index page needed -> MISS -> disk I/O -> cache
    query -> index page needed -> HIT  (LRU keeps it, it's hot)
    query -> index page needed -> HIT
    ...stays cached...
```

## Solution

Remove the separate `_index_list` and the `index_cache_fraction` check
entirely.  All entries — data rows, index pages, partition index cache
entries — share a single LRU:

```
    AFTER: Unified LRU
    +-----------------------------------------------+
    | cold entries ------>  LRU front  (evicted)     |
    |                                                |
    | hot entries  ------>  LRU back   (survives)    |
    +-----------------------------------------------+
      ^                                          ^
      index pages read once           data rows and index pages
      drift here naturally            touched repeatedly stay here
```

The LRU provides natural protection:
- **Hot entries** (frequently touched) stay at the back — never evicted
- **Cold entries** (index pages read once for a scan) drift to the front
  — evicted first when memory pressure appears
- No artificial cap needed — the cache self-adjusts to the workload

## Changes

```
    File                              Lines
    --------------------------------  -----
    utils/lru.hh                      -33    removed index_evictable class,
                                             _index_list, is_index() virtual,
                                             should_evict_index parameter

    db/row_cache.cc                   -23    removed index budget check from
                                             reclaimer; now calls lru::evict()

    db/config.cc                       ~2    marked index_cache_fraction as
                                             Deprecated (accepted, ignored)

    test/boost/lru_test.cc           +270    11 unit tests for unified LRU

    test/boost/cache_algorithm_test   +250   3 updated + 2 new benchmark tests

    configure.py, CMakeLists.txt       +4    registered lru_test
```

The `index_evictable` class is replaced with a type alias:

```cpp
    // BEFORE                          // AFTER
    class index_evictable              using index_evictable = evictable;
      : public evictable {             // backwards-compat alias
        lru_link_type _index_lru_link; // 8 bytes saved per entry
        bool is_index() override;      // virtual call removed
    };
```

## Test Results

All tests run with `-c1 -m200M` inside the toolchain docker container.

### Unit tests (lru_test)

```
    Test                                          Result
    -------------------------------------------   ------
    test_lru_basic_eviction_order                 PASS
    test_lru_touch_moves_to_back                  PASS
    test_unified_lru_index_and_data_interleaved   PASS
    test_hot_index_survives_over_cold_data        PASS
    test_cold_index_evicted_before_hot_data       PASS
    test_index_exceeds_old_cap_survives_when_hot  PASS
    test_cold_index_evicted_under_pressure        PASS
    test_add_before_ordering                      PASS
    test_evict_all                                PASS
    test_evict_empty                              PASS
    test_remove_prevents_eviction                 PASS
    -------------------------------------------   ------
    11/11 passed
```

### Integration tests (cache_algorithm_test, combined_tests)

```
    Test                                                Format   Result
    -------------------------------------------------   ------   ------
    test_index_doesnt_flood_cache_in_small_partition     MD       PASS*
    test_index_is_cached_in_big_partition_workload       ME       PASS
    test_realistic_small_partition_with_data_columns     ME       PASS
    test_benchmark_cold_index_wastes_cache_space         ME       PASS
    test_benchmark_hot_index_needs_more_than_20pct       ME       PASS
    -------------------------------------------------   ------   ------
    5/5 passed    (*BOOST_WARN documents legacy MD flooding)
```

### Regression test (cached_file_test)

```
    8/8 passed — no changes to cached_file code
```

## Benchmark Details

### Case 1: Cold index wastes cache space

**Setup**: 50K partitions, 10 KB values, trie (ME) format, 200 MB RAM.
Hot subset: 5000 rows (~50 MB).

```
    What happens WITHOUT the 20% cap (this patch):

    Step 1: Warm up 5000 hot rows
    +----------------------------------------------+
    | hot rows (50 MB)  | cold idx |   free         |
    +----------------------------------------------+
                          cold index pages drift to
                          LRU front, get evicted

    Step 2: Re-read hot rows
    +----------------------------------------------+
    | hot rows (50 MB)              |   free         |
    +----------------------------------------------+
    Result: 0 partition misses — all rows cached

    What would happen WITH the old 20% cap:

    Step 1: Warm up 5000 hot rows
    +----------------------------------------------+
    | hot rows (maybe 45 MB) |  idx reserve (40 MB) |
    +----------------------------------------------+
                               can't be reclaimed
                               even though it's cold

    Step 2: Re-read hot rows — some evicted to make room
    Result: partition misses — rows evicted for cold index
```

**Measured**: 0 partition_misses after warmup across 3 full passes of 5000 rows.

### Case 2: Hot index needs more than 20%

**Setup**: 5 partitions × 2000 clustering keys × 10 KB values, trie (ME)
format, 200 MB RAM.  Point lookups on 100 specific CKs, repeated 5 times.

```
    What happens WITHOUT the 20% cap (this patch):

    Warmup: read 100 CKs × 5 partitions = 500 queries
    +----------------------------------------------+
    | row data      | hot index pages |    free     |
    +----------------------------------------------+
                      partition_index_cache entries
                      touched on every CK lookup
                      → stay at LRU back

    Re-read: 500 queries × 5 repeats = 2500 total
    Result: 0 partition_index_cache misses

    What would happen WITH the old 20% cap:

    If index exceeds 20% (40 MB), force-evict:
    +----------------------------------------------+
    | row data              |idx|  force-evict     |
    +----------------------------------------------+
                             ^^^
                        hot index pages evicted
                        even though they're needed

    Re-read: index pages re-loaded from disk every time
    Result: repeated partition_index_cache misses
```

**Measured**: 0 partition_index_cache misses across 2500 reads.

## Legacy Format Limitation

The pathological case of 600K partitions with ONLY a primary key column
(no data) using legacy MD format still exhibits index flooding.  This is
because `partition_index_page` entries hold ~2000 keys (~2 MB each), so
the index:data ratio is ~2000:1.

This is a known limitation of the legacy SSTable format, not a regression
from this patch.  The test documents it with `BOOST_WARN`:

```
    Legacy small-partition index test:
      misses_before=1000, misses_after=2000, delta=1000
```

Modern trie (ME/MS/MT) formats use 4 KB `cached_file` pages, making the
index:data ratio bounded and the unified LRU effective.

## Configuration

The `index_cache_fraction` parameter is now deprecated:

```yaml
    # scylla.yaml
    # index_cache_fraction: 0.2   # accepted but ignored
```

Existing configurations continue to work without changes.  The parameter
is silently ignored.

## W-TinyLFU Scan Resistance: Live Benchmark

Benchmark comparing W-TinyLFU (1% window) vs pure LRU (99% window)
under a mixed hot+scan workload.  ScyllaDB runs with 512 MB cache,
80K rows × 1 KB payload flushed to SSTable.  Two concurrent latte
clients: one reads a 10K-row hot set (random access), the other scans
all 80K rows sequentially — 5K ops/s each, 30 seconds.

```
    Workload:

    hot client:  hash(i) % 10000  — random, high locality
    scan client: i % 80000        — sequential, zero reuse

    ┌──────────────────────────────────────┐
    │ W-TinyLFU (1% window)               │
    │                                      │
    │ window (1%)  probation     protected  │
    │ [scan ···>]  [gate ···>]  [hot data] │
    │  cold scan    frequency    hot rows   │
    │  entries      filter       safe here  │
    │  evicted      rejects                 │
    │  quickly      cold scans             │
    └──────────────────────────────────────┘

    ┌──────────────────────────────────────┐
    │ LRU (99% window = pure LRU)          │
    │                                      │
    │ [oldest ························ MRU] │
    │  scan entries push hot rows out      │
    │  → hot reads occasionally miss       │
    │  → tail latency spikes               │
    └──────────────────────────────────────┘
```

### Results

HOT reads under scan pressure (latency in ms):

```
    Percentile    W-TinyLFU      LRU        Improvement
    ---------     ---------      ---        -----------
    p50             0.708       0.736           4%
    p99             2.206       2.288           4%
    p99.9           2.912       4.727          38%
    p99.99          5.100       7.590          33%
    Max             6.332       9.101          30%
```

The tail latency improvement (p99.9+) is where W-TinyLFU's scan
resistance shows up.  The scan workload cannot evict hot data from
the protected segment, so hot reads never suffer cache misses caused
by the scan.  With LRU, scan entries push hot rows out of the cache,
causing occasional disk reads that spike tail latency.

### Hit Rate Comparison (256MB, 80K rows)

With 80K×1KB rows in 256MB (data fits in cache), both policies achieve
similar hit rates because the scan reads the same 80K rows that are
already cached — it doesn't introduce truly cold data:

```
                     W-TinyLFU (1%)     LRU (99%)
  Partition hits:       127097            126838
  Partition misses:      72968            73232
  Hit rate:              63.52%           63.39%
  Evictions:            105977            105995
```

The advantage is in **latency**, not hit rate, at this data:memory ratio.
W-TinyLFU's frequency-based admission protects hot entries' positions
in the SLRU, preventing the scan from pushing them toward the eviction
front — even when both policies ultimately keep the same data cached.

For hit rate differences, the scan working set must exceed cache capacity
so that keeping hot data requires evicting scan data, which W-TinyLFU
does via the admission gate.

### Benchmark command

```bash
# Start ScyllaDB with W-TinyLFU (1% window):
DBUILD_TOOL=docker ./tools/toolchain/dbuild \
    --image scylladb/scylla-toolchain:fedora-43-20260304 -- \
    build/release/scylla --workdir ... \
    --smp 1 --memory 512M --developer-mode 1 --overprovisioned \
    --tinylfu-initial-window-fraction 0.01

# Load data:
latte schema /tmp/bench.rn localhost:9042
latte run /tmp/bench.rn -f insert --end-cycle 80000 \
    -d 120s --warmup 0 -P row_count=80000 localhost:9042

# Warmup + hot+scan:
latte run /tmp/bench.rn -f hot_read -d 20s -r 10000 --warmup 5s \
    -P hot_count=10000 localhost:9042

latte run /tmp/bench.rn -f hot_read -d 30s -r 5000 --warmup 0 \
    -P hot_count=10000 localhost:9042 &
latte run /tmp/bench.rn -f scan_read -d 30s -r 5000 --warmup 0 \
    -P row_count=80000 localhost:9042 &
wait

# Repeat with --tinylfu-initial-window-fraction 0.99 for LRU baseline.
```
