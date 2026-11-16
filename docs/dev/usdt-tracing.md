# Tracing Scylla with USDT probes

Scylla exposes USDT (user statically-defined tracing) probes under the
provider name `scylla`. A probe site compiles to a single `nop` instruction
plus an entry in the `.note.stapsdt` ELF section that records the probe's
name and where its arguments live. The only cost of an idle probe is the
evaluation of its argument expressions, which the conventions below keep
to integers and pointers to existing data, so probes are safe to keep
enabled in production builds. When a tracer such as bpftrace attaches, the
kernel patches the `nop` into a trap, and the tracer receives the arguments
on every hit.

Probes complement metrics and logs rather than replace them. Metrics
aggregate along dimensions fixed at development time and cannot carry
high-cardinality keys (a partition key, a client address). With a probe,
the filter and the aggregation are chosen at attach time, on a live node,
with no restart, no configuration change and no redeploy.

## Relationship to LTTng

Seastar's IO scheduler and reactor carry LTTng-UST tracepoints (providers
`seastar_io` and `seastar_reactor`, see Seastar's `doc/io-tracing.md`).
Scylla builds them with `Seastar_LTTNG=OFF` until the toolchain gets an
lttng-ust with the fix for https://bugs.lttng.org/issues/1438. The two
mechanisms are layers of one stack, not alternatives:

* An lttng-ust tracepoint is also a USDT probe. lttng-ust in the toolchain
  is built with SDT integration (`LTTNG_UST_HAVE_SDT_INTEGRATION`), so each
  `tracepoint()` emits the same `.note.stapsdt` entry that `SCYLLA_USDT`
  does, under the lttng provider name. bpftrace and perf attach to
  `seastar_io:*` and `scylla:*` probes in exactly the same way, and one
  bpftrace program can correlate both.
* On top of that, lttng-ust adds a recording backend: the tracepoint also
  branches on a per-tracepoint flag and, when an lttng session has enabled
  it, writes the event into lttng's per-thread ring buffers, from where
  `lttng-sessiond` collects a CTF trace for offline analysis (babeltrace2,
  Trace Compass). That backend needs liblttng-ust linked in and
  `lttng-sessiond` running before Scylla starts.

The probes in this document use plain `sys/sdt.h` so that they carry no
runtime dependency, cost a single `nop` plus argument evaluation when
idle, and work in every build regardless of `Seastar_LTTNG`. `SCYLLA_USDT`
is the single point of change: once LTTng is re-enabled, the macro can
expand to an lttng-ust tracepoint as well, so the same probe is available
to both a live bpftrace session and an lttng recording without touching
the probe sites or the tools.

## Listing the probes

The ELF notes are the source of truth for a given binary:

```bash
readelf -n build/release/scylla | grep -A3 'Provider: scylla'
```

`perf` can list them as SDT events once the binary is in its cache:

```bash
perf buildid-cache --add build/release/scylla
perf list | grep sdt_scylla
```

## Attaching

Attaching requires root (or CAP_BPF plus CAP_PERFMON) on the node.
bpftrace is the recommended frontend. To attach to a running process by pid:

```bash
bpftrace -p $(pgrep -x scylla | head -1) -e '
    usdt:*:scylla:query_done { @us[str(arg0), str(arg1)] = hist(arg2); }'
```

Or by binary path, which attaches to every process running that binary:

```bash
bpftrace -e 'usdt:/usr/bin/scylla:scylla:query_done /arg2 > 10000/ {
    printf("%s.%s %ld us\n", str(arg0), str(arg1), arg2); }'
```

Ready-made tools live in `tools/bpftrace/`. Each answers one question:

| tool | question |
|------|----------|
| `scylla-readlat.bt` | per-table read latency histograms, bytes read, failures |
| `scylla-slowquery.bt` | every read slower than 10ms, with table and result size |
| `scylla-repair-stream.bt` | repair range progress and per-file streaming bandwidth |
| `scylla-repair-tablet.bt` | where a tablet repair spends its time: per-tablet overheads, rounds, peers, stop (attach on the repair master) |
| `scylla-repair-rounds.bt` | one line per step of every repair round, for following a single slow range |
| `scylla-repair-follower.bt` | is this node slow to read, to write or waiting in the repairs it takes part in (attach on any replica) |
| `scylla-query-impact.bt` | read latency per shard, split by whether repair, a memtable flush, a compaction, an LSA reclaim or a cache update was active on the shard |
| `scylla-compaction-watch.bt` | every finished compaction with sizes and throughput, duration histograms per type |
| `scylla-memory-watch.bt` | every memtable flush with sizes and throughput, LSA reclaims that stall or run non-preemptibly |
| `scylla-largealloc.bt` | every allocation above Seastar's large allocation threshold, with size and stack (uprobe, see below) |
| `scylla-stalls.bt` | reactor stall reports per shard with the stalled stack (uprobe, see below) |
| `scylla-cpuprofile.bt` | on-CPU stack sampling of the reactor threads (no probe needed) |

Notes:

* A probe usually has several locations in the binary (the compiler clones
  coroutine bodies). Tracers attach to all locations of a name at once.
* Inside a container (for example under `tools/toolchain/dbuild`), attach
  from the host with `-p <pid>`. If the probes do not resolve, use the
  path form through the process root:
  `usdt:/proc/<pid>/root/<path to scylla>:scylla:<probe>`.
* A hit costs on the order of a microsecond. Attaching to a probe that
  fires hundreds of thousands of times per second slows the traced shards
  noticeably; keep the bpftrace program short and aggregate in maps.

## Probe catalog

All durations are in microseconds, all sizes in bytes. String arguments
are NUL-terminated and read with `str(argN)`.

### Data queries (`replica/database.cc`, `database::query()`)

| probe | arguments |
|-------|-----------|
| `query_start` | keyspace, table |
| `query_done` | keyspace, table, duration, result size, short read flag |
| `query_failed` | keyspace, table, duration |

### Repair ranges (`repair/repair.cc`, `repair_service::repair_range()`)

A tablet repair is one range, so these fire once per tablet too.

| probe | arguments |
|-------|-----------|
| `repair_range_start` | keyspace, table, range index, total ranges |
| `repair_range_done` | keyspace, table, duration |
| `repair_range_failed` | keyspace, table, duration |
| `repair_range_wait_done` | keyspace, table, wait for a range parallelism slot |
| `repair_sync_schema_done` | keyspace, table, duration of the schema sync with the peers |

### Tablet repair (`repair/repair.cc`, `repair_service::repair_tablet()`)

Fire on the replica the coordinator picked to run the repair (the master).
Mode: 0 full, 1 incremental, 2 rebuild repair.

| probe | arguments |
|-------|-----------|
| `tablet_repair_start` | keyspace, table, tablet id, peers, mode |
| `tablet_repair_done` | keyspace, table, tablet id, duration |
| `tablet_repair_failed` | keyspace, table, tablet id, duration |
| `repair_flush_hints_done` | keyspace, duration, needed flag, flushed flag |

### Row-level repair master (`repair/row_level.cc`, `row_level_repair`)

One row-level repair per range. All probes carry `repair_meta_id` first:
the master allocates it per range and sends it to the peers in every RPC,
so the `repair_meta_*` probes on the peers carry the same id. Node index 0
is the master itself, nodes 1.. are the peers in `repair_master_peer`
order. Host ids are passed as the two 64-bit halves of the UUID.

| probe | arguments |
|-------|-----------|
| `repair_master_start` | repair_meta_id, keyspace, table, peers, row buffer size, mode |
| `repair_master_peer` | repair_meta_id, node index, host id msb, host id lsb, shard |
| `repair_master_wait_done` | repair_meta_id, memory budget wait, reader permit wait |
| `repair_master_rpc_done` | repair_meta_id, node index, step, duration |
| `repair_round_start` | repair_meta_id, round |
| `repair_round_sync_boundary_done` | repair_meta_id, round, node index, rows read, bytes read, duration |
| `repair_round_combined_hash_done` | repair_meta_id, round, node index, duration |
| `repair_round_full_hashes_done` | repair_meta_id, round, node index, hashes, duration |
| `repair_round_get_diff_done` | repair_meta_id, round, node index, rows received, bytes received, duration |
| `repair_round_put_diff_done` | repair_meta_id, round, node index, rows sent, bytes sent, duration |
| `repair_round_flush_rows_done` | repair_meta_id, round, duration |
| `repair_round_done` | repair_meta_id, round, outcome, duration |
| `repair_master_done` | repair_meta_id, duration, rounds, fast rounds, slow rounds, failed flag |
| `repair_master_stats` | repair_meta_id, bytes read from disk on all nodes, bytes sent, bytes received, hashes sent, hashes received |

Step in `repair_master_rpc_done`: 1 `row_level_start`, 2
`get_estimated_partitions`, 3 `set_estimated_partitions`, 4
`row_level_stop` (includes the peer sealing its sstables). Outcome in
`repair_round_done`: 0 synced by the sync boundary (nothing compared), 1
synced by the combined hash, 2 slow path (hashes and rows exchanged), 3
no more data (last round).

### Repair participant (`repair/row_level.cc`, `repair_meta`)

Fire on every node taking part in a row-level repair, for its own
`repair_meta`: on the followers and on the master for its local reads and
writes. A `repair_meta_id` is unique per master only (each node counts
from zero), so these probes carry the master's host id msb as the second
argument and tools key on the pair. Read strategy: 0 local, 1 multishard
split, 2 multishard filter, 3 incremental repair.

| probe | arguments |
|-------|-----------|
| `repair_meta_start` | repair_meta_id, master host id msb, master host id lsb, keyspace, table, master flag |
| `repair_meta_permit_done` | keyspace, table, reader permit wait (followers) |
| `repair_meta_reader_created` | repair_meta_id, master msb, read strategy, sstable bytes selected, sstable bytes skipped, creation time |
| `repair_meta_read_rows_done` | repair_meta_id, master msb, rows, bytes, duration (one buffer read and hashed) |
| `repair_meta_apply_rows_done` | repair_meta_id, master msb, rows, bytes, duration (one batch received from the master and written; rows the master pulls are written in the round's flush_rows step) |
| `repair_meta_writer_done` | repair_meta_id, master msb, wait for the writer to seal its sstables at stop |
| `repair_meta_stop` | repair_meta_id, master msb, duration |
| `repair_meta_mark_repaired_done` | keyspace, table, duration (incremental repair, after stop, on the master and the followers) |
| `repair_writer_done` | keyspace, table, partitions written, writer lifetime |

### File-based streaming (`streaming/stream_blob.cc`)

The `tx` probes fire on the sending node, once per file, and the `rx`
probes on the receiving node.

| probe | arguments |
|-------|-----------|
| `file_stream_tx_start` | filename |
| `file_stream_tx_done` | filename, bytes, duration |
| `file_stream_tx_failed` | filename, bytes, duration |
| `file_stream_rx_start` | filename |
| `file_stream_rx_done` | filename, bytes, duration |
| `file_stream_rx_failed` | filename, bytes, duration |

### Compaction (`compaction/compaction.cc`, `compaction::run()`)

One triplet per compaction job of any type. The type string is the short
name from `to_string(compaction_type)`: `Compact`, `Major`, `Cleanup`,
`Scrub`, `Reshard`, `Reshape`, `Split`, `Upgrade`, `RewriteComponent`.
Sizes are bytes on disk of the input and output sstables.

| probe | arguments |
|-------|-----------|
| `compaction_start` | keyspace, table, type, input sstables, input size |
| `compaction_done` | keyspace, table, type, duration, input size, output size |
| `compaction_failed` | keyspace, table, type, duration |

### Memtable flush (`replica/table.cc`, `table::try_flush_memtable_to_sstable()`)

One triplet per memtable flushed to disk, whether triggered by memory
pressure, a timer, or an explicit flush. Memtable size is the LSA
occupancy of the memtable; sstable bytes are on-disk sizes of the output.

| probe | arguments |
|-------|-----------|
| `memtable_flush_start` | keyspace, table, memtable size, partitions |
| `memtable_flush_done` | keyspace, table, memtable size, duration, sstables written, sstable bytes |
| `memtable_flush_failed` | keyspace, table, memtable size, duration |

### LSA reclaim (`utils/logalloc.cc`, `reclaim_timer`)

One pair per memory reclaim operation of the log-structured allocator.
The kind is one of `reclaim` (release memory from LSA to the general
allocator), `reclaim_segments`, `compact` (compact regions to free
segments) and `evict` (evict from the cache or a memtable). The flags are
0 or 1. A reclaim is a stall when it ran for longer than
`--blocked-reactor-notify-ms`; the same condition produces the "took N us"
warning in the log.

| probe | arguments |
|-------|-----------|
| `lsa_reclaim_start` | kind, bytes requested, segments requested, preemptible |
| `lsa_reclaim_done` | kind, bytes requested, bytes released, duration, preemptible, stall |

### Row cache (`db/row_cache.cc`)

Argument-less markers around cache update phases: `row_cache_update_start`,
`row_cache_update_end`, `row_cache_update_one_batch_start`,
`row_cache_update_one_batch_end`, `row_cache_update_partition_start`,
`row_cache_update_partition_end`.

## Memory allocation and CPU scheduling: the Seastar boundary

Two frequent field questions, "what allocates these oversized buffers" and
"what is stalling the reactor", happen inside Seastar, where a `scylla:*`
probe cannot be placed. Until Seastar carries probes of its own, the tools
attach uprobes to two Seastar functions instead:

* `scylla-largealloc.bt` hooks `cpu_pages::warn_large_allocation`, the
  function that emits the "oversized allocation" log line, so it fires for
  exactly the same events but reports all of them, with a size histogram
  and the allocating stack, instead of one rate-limited log line.
* `scylla-stalls.bt` hooks `cpu_stall_detector::maybe_report_kernel_trace`,
  the last call of the function behind "Reactor stalled for N ms"
  (`generate_trace` itself is inlined in release builds), and records
  the stack that was running when the stall was detected.

Both take the file containing the Seastar code as their first argument
(`/usr/bin/scylla` for release builds, `libseastar.so` for dev builds,
prefixed with `/proc/<pid>/root` for a container). A uprobe depends on the
symbol surviving inlining, so these two tools are less stable than the
USDT-based ones and are kept out of the probe catalog; check with
`bpftrace -l 'uprobe:<file>:*maybe_report_kernel_trace*'` if one stops attaching.
Seastar's own reactor tracepoints (`seastar_reactor:task_start`,
`task_end` with the scheduling group) already exist as LTTng-UST
tracepoints and are USDT probes once `Seastar_LTTNG` is enabled, which
covers per-group scheduling questions.

`scylla-cpuprofile.bt` needs no probe: it samples the reactor threads'
user stacks with bpftrace's `profile` provider.

User stacks (`ustack()`) are walked through frame pointers, which the
default build omits. To get complete stacks from these three tools, build
with `./configure.py --cflags=-fno-omit-frame-pointer`; without it, only
the innermost frame or two are reliable, while counts, sizes and
per-thread attribution are unaffected.

## Diagnosing a slow tablet repair

1. On the node running the repair, attach `scylla-repair-tablet.bt`. Each
   range line splits the time into `pre` (memory and permit waits, start
   and estimated-partitions RPCs), `rounds` and `stop` (the peers sealing
   their sstables), and the exit histograms add the per-tablet overheads
   (hints flush, range slot wait, schema sync). If `pre`, `stop` and the
   overheads dominate, the repair is bound by per-tablet machinery, not
   data: look at the number of tablets and the coordinator.
2. If the rounds dominate, look at the rounds per range and the round
   duration by outcome. Many `synced_by_boundary` rounds mean the data
   agrees and each round costs a read of one buffer on every peer plus a
   network round trip; `slow_path` rounds with large rx/tx mean real
   differences. The sync-boundary histogram per peer shows whether one
   peer is slower than the others, and `scylla-repair-rounds.bt` follows
   a single range step by step.
3. On the slow peer, attach `scylla-repair-follower.bt`: read throughput
   per buffer (disk and hashing), reader permit wait (admission),
   apply time per batch (writes), writer wait at stop (sstable seal and
   the view update path) and the reader strategy (a multishard strategy
   means the peer reads across shards). `scylla-cpuprofile.bt` tells
   hashing CPU from IO wait.
4. To know what the repair costs the reads on a replica, attach
   `scylla-query-impact.bt` there: the read latency of each shard, and
   of the whole node under the key `all`, split into reads that ran
   while a repair was live on the shard and reads that did not, next to
   the same split for flushes, compactions, reclaims and cache updates.

## Adding a probe

Use `SCYLLA_USDT(name, args...)` from `utils/usdt.hh`. It expands to
nothing when `sys/sdt.h` is unavailable, so no other guard is needed.

Naming:

* `<subsystem>_<operation>_<event>`, all lower case, for example
  `file_stream_tx_done`.
* Instrument an operation as a triplet: `_start`, `_done` and `_failed`.
  The `_done` and `_failed` probes carry the duration, computed at the
  probe site from a `steady_clock` timestamp taken next to `_start`.
* Names are a stable interface: tools depend on them. Rename with the same
  care as a metric.

Arguments:

* Order: identifying strings first (keyspace, table, filename, peer), then
  counters (duration, bytes, counts), then flags.
* Pass at most 6 arguments (bpftrace reads `arg0` to `arg5` only on
  x86_64), and only integers, booleans and pointers to data that already
  exists: `sstring::c_str()` of a live string is fine,
  `format(...)` is not. Argument expressions run on every hit of the
  probe site even with no tracer attached, so they must be cheap and free
  of side effects.
* Prefer stable identifiers over addresses. A table name is useful in a
  bpftrace script; a `table*` is not.

Placement:

* Put probes at operation boundaries, next to the existing log statement
  for the same event, in the function that owns the operation's lifetime.
  Per-row or per-fragment probes belong in hot loops only with a USDT
  semaphore guarding the argument computation; discuss first.
* One probe site per event. Do not scatter the same probe name across
  several functions.

Housekeeping:

* Add the probe to the catalog above.
* `test/boost/usdt_test` checks the macro machinery (0 to 6 arguments,
  single evaluation, ELF note registration). It does not know about
  individual probes and does not need updating.
