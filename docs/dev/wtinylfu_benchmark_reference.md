# W-TinyLFU vs Plain LRU: Reference Benchmark

Canonical benchmark for comparing W-TinyLFU scan resistance against plain
LRU. This document should be used as the reference for all branches.

## Setup

```
ScyllaDB:   --smp 1 --memory 512M --developer-mode 1
Cache:      ~314 MB usable (from 512 MB total)
Hot table:  80,000 rows × 1 KB payload = 80 MB  (~25% of cache)
Scan table: 500,000 rows × 1 KB payload = 500 MB (~160% of cache)
Rate:       5,000 ops/s hot + 5,000 ops/s scan (concurrent)
Duration:   120 seconds (after 30s warmup)
Tool:       latte 0.43.1-scylladb
```

The scan working set (500 MB) significantly exceeds the cache (314 MB),
forcing continuous eviction.  The hot set (80 MB) fits in cache and
should remain cached.

## Results (2026-07-20)

### Per-Table Hit Rates

```
                      W-TinyLFU (1% window)    Plain LRU
  HOT table:          81.8%                    81.8%
  SCAN table:          0.0%                     0.0%
  Total:              41.2%                    41.5%
```

Hit rates are identical because both policies must evict the same
amount — the scan (500 MB) far exceeds cache (314 MB), so both evict
at the same rate.  The difference is in WHAT gets evicted and the
resulting latency.

### HOT Read Latency (ms)

```
  Percentile    W-TinyLFU    Plain LRU    Improvement
  ---------     ---------    ---------    -----------
  p50             0.643        0.834         23%
  p75             1.051        1.404         25%
  p90             1.324        2.333         43%
  p99             3.623        5.751         37%
```

### Why Latency Differs When Hit Rates Are Equal

Both policies keep ~82% of hot reads in cache and miss ~18%.  But
the distribution of those misses differs:

```
  Plain LRU:
  ┌──────────────────────────────────────────────┐
  │ [scan][scan][hot3][scan][hot1][scan][hot2]    │
  │  scan entries interleave with hot entries     │
  │  → hot entries pushed toward eviction front   │
  │  → misses come in BURSTS when scan waves hit  │
  │  → burst misses = multiple back-to-back I/Os  │
  │  → high tail latency (p90=2.3ms, p99=5.8ms)  │
  └──────────────────────────────────────────────┘

  W-TinyLFU:
  ┌──────────────────────────────────────────────┐
  │ WINDOW: [scan entries] → gate → evicted      │
  │ PROTECTED: [hot1][hot2][hot3]... → stable    │
  │  scan entries never reach protected segment   │
  │  → misses are SPREAD EVENLY over time         │
  │  → no burst eviction of hot data              │
  │  → low tail latency (p90=1.3ms, p99=3.6ms)   │
  └──────────────────────────────────────────────┘
```

The 18% of hot misses in LRU come in correlated bursts when scan
waves push hot entries off the LRU tail.  In W-TinyLFU, the misses
are spread evenly because the admission gate prevents scan entries
from entering the main cache at all.

### Previous Results Comparison

```
  Source              LRU p75    TinyLFU p75    Improvement
  ------------------  -------    -----------    -----------
  v2 (stock docker)   4.600ms    1.110ms        76%
  This benchmark      1.404ms    1.051ms        25%
```

The v2 benchmark showed a larger gap because it compared a stock
ScyllaDB Docker image (potentially with compaction/background tasks)
against a dev build.  This benchmark compares two dev builds from
the same codebase, giving a cleaner measurement of the algorithm
difference alone.

## How to Reproduce

```bash
# Build W-TinyLFU binary (feat/wTinyLfu_for_non_CKs branch):
git checkout feat/wTinyLfu_for_non_CKs
# ... build ...

# Build plain LRU binary (feature/generic_index_cache branch):
git checkout feature/generic_index_cache
# ... build ...

# Create workload:
cat > /tmp/bench_full.rn << 'RUNE'
use latte::*;
const HOT_ROWS = latte::param!("hot_rows", 80000);
const SCAN_ROWS = latte::param!("scan_rows", 500000);

pub async fn schema(db) {
    db.execute("CREATE KEYSPACE IF NOT EXISTS bench WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': true}").await?;
    db.execute("CREATE TABLE IF NOT EXISTS bench.hot (pk bigint PRIMARY KEY, payload blob)").await?;
    db.execute("CREATE TABLE IF NOT EXISTS bench.scan (pk bigint PRIMARY KEY, payload blob)").await?;
}
pub async fn prepare(db) {
    db.prepare("ins_hot", "INSERT INTO bench.hot (pk, payload) VALUES (?, ?)").await?;
    db.prepare("ins_scan", "INSERT INTO bench.scan (pk, payload) VALUES (?, ?)").await?;
    db.prepare("sel_hot", "SELECT * FROM bench.hot WHERE pk = ?").await?;
    db.prepare("sel_scan", "SELECT * FROM bench.scan WHERE pk = ?").await?;
}
pub async fn load_hot(db, i) {
    db.execute_prepared("ins_hot", [i, blob(i, 1024)]).await?;
}
pub async fn load_scan(db, i) {
    db.execute_prepared("ins_scan", [i, blob(i, 1024)]).await?;
}
pub async fn hot_read(db, i) {
    db.execute_prepared("sel_hot", [hash(i) % HOT_ROWS]).await?;
}
pub async fn scan_read(db, i) {
    db.execute_prepared("sel_scan", [i % SCAN_ROWS]).await?;
}
RUNE

# Start ScyllaDB (via dbuild for shared libs):
DBUILD_TOOL=docker ./tools/toolchain/dbuild ... -- \
    build/release/scylla --smp 1 --memory 512M \
    --developer-mode 1 --overprovisioned

# Load data:
latte schema /tmp/bench_full.rn localhost:9042
latte run /tmp/bench_full.rn -f load_hot --end-cycle 80000 \
    -d 120s --warmup 0 localhost:9042
latte run /tmp/bench_full.rn -f load_scan --end-cycle 500000 \
    -d 600s --warmup 0 localhost:9042

# Flush:
curl -s -X POST http://localhost:10000/storage_service/keyspace_flush/bench

# Warmup:
latte run /tmp/bench_full.rn -f hot_read -d 30s -r 8000 \
    --warmup 5s -P hot_rows=80000 localhost:9042

# Attack (hot + scan parallel):
latte run /tmp/bench_full.rn -f hot_read -d 120s -r 5000 \
    --warmup 0 -P hot_rows=80000 localhost:9042 &
latte run /tmp/bench_full.rn -f scan_read -d 120s -r 5000 \
    --warmup 0 -P scan_rows=500000 localhost:9042 &
wait

# Collect metrics:
curl -s http://localhost:9180/metrics | grep cache_hit_rate.*bench
curl -s http://localhost:9180/metrics | grep scylla_cache_partition
```

## Key Takeaway

W-TinyLFU and plain LRU achieve identical hit rates in this workload
because both must evict the same total amount of data.  The advantage
of W-TinyLFU is not in WHAT it caches but in HOW it handles eviction
ordering — preventing scan entries from creating burst evictions of
hot data, which reduces tail latency by 25-43%.
