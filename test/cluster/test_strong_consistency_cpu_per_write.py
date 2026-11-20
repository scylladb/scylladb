#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
CPU cost of a strongly consistent (SC) write, measured against an eventually
consistent (EC) write of the same row on the same cluster.

Why this test exists
--------------------
The cloud ladder test of SC tables (3 x i8g.4xlarge, 1 KiB rows, RF 3, batch
commitlog) found an SC write taking 2.3-2.7x the CPU of an EC write, two thirds
of it in the `gossip` scheduling group where the raft write path runs. A cloud
run gives one number per two hours and cannot say where the CPU goes. This
test gives the same ratio in three minutes on a dev build, with the CPU split
by scheduling group and the raft and commitlog counters that explain it, so a
change in the SC write path can be judged before it goes to the cloud. It found
that the 2.3-2.7x came from a CQL write of the raft commit index on every
replica per write (since removed) and from comparing SC at 100k writes/s with
EC at 500k, where EC batches commitlog syncs; on the fixed code, at the same
rate, SC costs 1.03-1.08x EC.

What it does
------------
3 nodes with 1 shard each, batch commitlog on both keyspaces so the durability
is equal. The EC and the SC keyspace get the same table with a 1 KiB value and
the same number of tablets. For each mode: a warm-up elects the raft leaders,
an idle baseline is taken with no load, then WRITES rows are inserted at
SC_CPU_RATE writes/s with at most CONCURRENCY in flight, and the cluster-wide
cumulative counters are read before and after. SC writes are sent to the node leading the row's tablet, as the
leader-aware stress tool of the cloud test does; the Python driver alone would
bounce two thirds of them to the leader through a replica and charge that to
SC. The idle rate of every counter is subtracted from the loaded run, so raft
heartbeats, gossip and the metrics scrapes are not charged to the writes.

What it prints and how to read it
---------------------------------
Per mode, in the pytest log (and in SC_CPU_REPORT if set):

  ec: 20000 writes in 1.8 s (10943 writes/s), 83.1 us CPU per write (raw 86.3)
  ec:   sl:default      56.2 us per write (raw 56.2)      <- statement execution
  ec:   main             8.5 us per write
  ec:   commitlog flushes/write 1.06, allocs/write 3.00, bytes/write 6485
  sc: 20000 writes in 1.8 s (10990 writes/s), 89.7 us CPU per write (raw 92.9)
  sc:   gossip          48.0 us per write      <- raft io fiber, apply, raft RPC handlers
  sc:   sl:default      12.0 us per write
  sc:   commitlog flushes/write 1.13, allocs/write 2.80, bytes/write 6613
  sc:   raft polls/write 4.06, persisted entries/poll 0.74, applied/write 2.99,
        node bounces/write 0.00, shard bounces/write 0.00
  SC/EC CPU per write: 1.08x

- "us CPU per write" is the cluster's reactor busy time (all three nodes,
  replication included) divided by the client writes; "raw" is before the idle
  baseline is subtracted.
- The scheduling-group lines split that time. In SC the memtable apply of all
  three replicas runs in `gossip` (the applier fiber), in EC it is in
  `sl:default`, so compare the totals, not `gossip` alone.
- "allocs/write" counts commitlog entries per client write: 3 for EC (one per
  replica); above 3 in SC means something besides the raft log is written
  durably per write. "flushes/write" counts fsyncs.
- "persisted entries/poll" is the raft batching degree: 1 and below means every
  write pays a whole raft round (commitlog write and fsync per replica, two
  appends, two replies, four polls). It is set by the write rate per raft group,
  i.e. by the tablet count: the cloud test at 768 tablets ran at 0.74-0.9;
  SC_CPU_TABLETS=128 reproduces that here, 4 gives full batching and SC ~= EC.
- "node bounces/write" must be 0; otherwise the leader lookup failed and the
  SC column includes the forwarding cost.
- The two "writes/s" must match: both modes are paced to SC_CPU_RATE, and the
  test warns when they differ by more than 10%, which means the cluster could
  not keep up and the comparison is at two rates, i.e. two batching degrees.
- A dev build on a laptop gives a ratio, not a production figure: the absolute
  numbers are 2-3x off an i8g release build and vary by 5% between runs. Judge
  changes by the SC/EC ratio and by the counters, and confirm an improvement
  that matters with a cloud ladder run.

When to run it
--------------
Not in CI (perf marker, so non_gating). Run it by hand, before and after a
change, whenever the SC write path changes: service/strong_consistency
(coordinator, state_machine, raft_commitlog, raft_groups_storage), raft/
(server, fsm), the raft RPC glue in service/raft, the commitlog's raft batch
format, or the routing of raft verbs in messaging_service. Also run it when a
cloud SC perf run reports a CPU regression, to attribute it.

How to run it
-------------
    ./test.py --mode dev test/cluster/test_strong_consistency_cpu_per_write.py
    # or plain pytest (see docs/dev/testing.md) with --mode dev

Knobs (environment): SC_CPU_TABLETS (default 128), SC_CPU_RATE (8000 writes/s,
both modes are paced to it; 0 for unpaced), SC_CPU_WRITES (20000),
SC_CPU_CONCURRENCY (32), SC_CPU_REPORT (a file the report lines are appended
to; pytest's captured log is cut in long runs), SC_CPU_PERF_DIR (record a perf
profile of the servers during each load phase into <dir>/perf-<mode>.data;
perf must be installed) and SC_CPU_PERF_CALLGRAPH (fp or dwarf, with the
profile). The test asserts nothing about the ratio; it fails only if the
cluster or the load does.
"""
import asyncio
import bisect
import logging
import os
import signal
import struct
import subprocess
import time
from collections import Counter

import pytest
from cassandra.metadata import Murmur3Token

from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import ScyllaMetricsLine
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {
    'experimental_features': ['strongly-consistent-tables'],
    'commitlog_sync': 'batch',
    'commitlog_sync_batch_window_in_ms': 100,
    'rf_rack_valid_keyspaces': False,
    # The raft counters of a table are off unless its metrics are reported.
    'enable_node_aggregated_table_metrics': True,
    # The scrape adds CPU of its own; keep the cluster from doing anything
    # else in the background.
    'tablet_load_stats_refresh_interval_in_seconds': 3600,
}
CMDLINE = ['--smp', '1']

TABLETS = int(os.environ.get('SC_CPU_TABLETS', 128))
VALUE_SIZE = 1024
WARMUP_WRITES = 2000
WRITES = int(os.environ.get('SC_CPU_WRITES', 20000))
CONCURRENCY = int(os.environ.get('SC_CPU_CONCURRENCY', 32))
# Both modes are paced to this rate, so that the comparison is at one rate and
# not at whatever each mode sustains: the rate sets the batching. 0 means no
# pacing, as fast as CONCURRENCY writers go.
RATE = int(os.environ.get('SC_CPU_RATE', 8000))
IDLE_SECONDS = 5
# Set to a directory to record a flat perf profile of the servers during each
# load phase into <dir>/perf-<mode>.data; perf has to be on the PATH.
PERF_DIR = os.environ.get('SC_CPU_PERF_DIR')
# Set to fp or dwarf to record call graphs with the profile.
PERF_CALLGRAPH = os.environ.get('SC_CPU_PERF_CALLGRAPH')
# The report lines also go to this file, if set: pytest's captured log is
# truncated in long runs.
REPORT_FILE = os.environ.get('SC_CPU_REPORT')


def stop_perf(perf: subprocess.Popen | None) -> int | None:
    """Stops a perf record, which then writes its file, and returns its status."""
    if perf is None:
        return None
    perf.send_signal(signal.SIGINT)
    try:
        return perf.wait(timeout=60)
    except subprocess.TimeoutExpired:
        perf.kill()
        return perf.wait()


def out(fmt, *args):
    logger.info(fmt, *args)
    if REPORT_FILE:
        with open(REPORT_FILE, 'a') as f:
            f.write((fmt % args) + '\n')


SCALAR_COUNTERS = {
    'cpu_busy_ms': 'scylla_reactor_cpu_busy_ms',
    'cl_flush': 'scylla_commitlog_flush',
    'cl_alloc': 'scylla_commitlog_alloc',
    'cl_bytes': 'scylla_commitlog_bytes_written',
    'raft_polls': 'scylla_strong_consistency_raft_polls',
    'raft_persisted': 'scylla_strong_consistency_raft_persisted_log_entries',
    'raft_applied': 'scylla_strong_consistency_raft_applied_entries',
    'sc_node_bounces': 'scylla_strong_consistency_coordinator_write_node_bounces',
    'sc_shard_bounces': 'scylla_strong_consistency_coordinator_write_shard_bounces',
}


class Snapshot:
    """Cluster-wide sums of the cumulative counters the test reads."""

    def __init__(self, scalars: dict, groups: dict, at: float):
        self.scalars = scalars
        self.groups = groups
        self.at = at

    def __sub__(self, other: 'Snapshot') -> 'Snapshot':
        # Not Counter arithmetic: that drops the keys whose difference is zero.
        return Snapshot({k: v - other.scalars.get(k, 0) for k, v in self.scalars.items()},
                        {k: v - other.groups.get(k, 0) for k, v in self.groups.items()},
                        self.at - other.at)


async def snapshot(manager: ScyllaClusterManager, servers) -> Snapshot:
    scalars = dict.fromkeys(SCALAR_COUNTERS, 0.0)
    groups = Counter()
    names = {name: key for key, name in SCALAR_COUNTERS.items()}
    for srv in servers:
        m = await manager.metrics.query(srv.ip_addr)
        # Exact names: ScyllaMetrics.get() sums every metric sharing the prefix,
        # e.g. scylla_commitlog_flush_limit_exceeded into scylla_commitlog_flush.
        # The SC counters are skipped while empty, so they are missing during
        # the EC phase and stay 0.
        for line in m.lines_by_prefix('scylla_'):
            parsed = ScyllaMetricsLine.from_string(line)
            if not parsed:
                continue
            if parsed.name in names:
                scalars[names[parsed.name]] += parsed.value
            elif parsed.name == 'scylla_scheduler_runtime_ms':
                groups[parsed.labels['group']] += parsed.value
    return Snapshot(scalars, groups, time.monotonic())


class LeaderRouter:
    """Sends each SC write to the node leading its tablet, as a leader-aware driver
    does, so that no write is bounced to the leader by a replica."""

    def __init__(self, last_tokens: list[int], leaders: list):
        self.last_tokens = last_tokens
        self.leaders = leaders

    @staticmethod
    async def build(manager: ScyllaClusterManager, servers, hosts, ks: str, table: str) -> 'LeaderRouter':
        host_by_id = {await manager.get_host_id(s.server_id): h for s, h in zip(servers, hosts)}
        table_id = await manager.get_table_id(ks, table)
        rows = await manager.get_cql().run_async(
                f"SELECT last_token, raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        rows = sorted(rows, key=lambda r: r.last_token)
        leaders = [host_by_id[await manager.api.get_raft_leader(servers[0].ip_addr, str(r.raft_group_id))]
                   for r in rows]
        return LeaderRouter([r.last_token for r in rows], leaders)

    def host_for(self, key: int):
        token = Murmur3Token.from_key(struct.pack('>i', key)).value
        return self.leaders[bisect.bisect_left(self.last_tokens, token)]


async def run_writes(cql, stmt, first_key: int, count: int, value: bytes, router: LeaderRouter | None = None) -> float:
    """Inserts `count` rows at RATE writes/s with at most CONCURRENCY in flight;
    returns the seconds it took."""
    sem = asyncio.Semaphore(CONCURRENCY)
    start = time.monotonic()

    async def one(i, key):
        # Wait for the write's turn before taking an in-flight slot, so that a
        # slot is never held by a write that is only waiting for its time.
        if RATE:
            delay = start + i / RATE - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
        async with sem:
            if router:
                await cql.run_async(stmt, [key, value], host=router.host_for(key))
            else:
                await cql.run_async(stmt, [key, value])

    # A TaskGroup cancels the other writes when one fails, so none outlives the test.
    async with asyncio.TaskGroup() as tg:
        for i, key in enumerate(range(first_key, first_key + count)):
            tg.create_task(one(i, key))
    return time.monotonic() - start


def report(mode: str, load: Snapshot, idle: Snapshot, writes: int, seconds: float):
    """Logs CPU per write with the idle rate of each counter subtracted."""
    idle_rate = {k: v / idle.at for k, v in idle.scalars.items()}
    idle_group_rate = {k: v / idle.at for k, v in idle.groups.items()}
    net_scalars = {k: v - idle_rate.get(k, 0) * load.at for k, v in load.scalars.items()}
    busy_ms = net_scalars['cpu_busy_ms']
    out("%s: %d writes in %.1f s (%.0f writes/s), %.1f us CPU per write (raw %.1f)",
        mode, writes, seconds, writes / seconds,
        busy_ms * 1000 / writes, load.scalars['cpu_busy_ms'] * 1000 / writes)
    for group, ms in sorted(load.groups.items(), key=lambda kv: -kv[1]):
        net = ms - idle_group_rate.get(group, 0) * load.at
        if ms * 1000 / writes >= 0.5:
            out("%s:   %-20s %7.1f us per write (raw %.1f)", mode, group, net * 1000 / writes, ms * 1000 / writes)
    per_write = {k: v / writes for k, v in net_scalars.items()}
    out("%s:   commitlog flushes/write %.2f, allocs/write %.2f, bytes/write %.0f",
        mode, per_write['cl_flush'], per_write['cl_alloc'], per_write['cl_bytes'])
    if mode == 'sc':
        polls = net_scalars['raft_polls']
        out("%s:   raft polls/write %.2f, persisted entries/poll %.2f, applied/write %.2f, "
            "node bounces/write %.2f, shard bounces/write %.2f",
            mode, per_write['raft_polls'],
            net_scalars['raft_persisted'] / polls if polls else 0,
            per_write['raft_applied'], per_write['sc_node_bounces'], per_write['sc_shard_bounces'])
    return busy_ms * 1000 / writes, writes / seconds


@pytest.mark.perf
@pytest.mark.asyncio
async def test_cpu_per_write_sc_vs_ec(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE) for _ in range(3)]
    cql, hosts = await manager.get_ready_cql(servers)
    value = bytes(VALUE_SIZE)
    results = {}

    for mode in ('ec', 'sc'):
        opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                f" AND tablets = {{'initial': {TABLETS}}}")
        if mode == 'sc':
            opts += " AND consistency = 'global'"
        async with new_test_keyspace(manager, opts) as ks:
            await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v blob)")
            stmt = cql.prepare(f"INSERT INTO {ks}.t (pk, v) VALUES (?, ?)")

            await run_writes(cql, stmt, 0, WARMUP_WRITES, value)
            # The warm-up elected the leaders; route the measured writes to them.
            router = await LeaderRouter.build(manager, servers, hosts, ks, 't') if mode == 'sc' else None
            # Let the warm-up's memtable flushes and the driver's metadata
            # refresh finish before the idle baseline is taken.
            await asyncio.sleep(3)

            idle_start = await snapshot(manager, servers)
            await asyncio.sleep(IDLE_SECONDS)
            idle = (await snapshot(manager, servers)) - idle_start

            perf = None
            data = f'{PERF_DIR}/perf-{mode}.data'
            if PERF_DIR:
                callgraph = ['--call-graph', PERF_CALLGRAPH] if PERF_CALLGRAPH else []
                perf = subprocess.Popen(['perf', 'record', '-F', '999', *callgraph, '-o', data,
                                         '-p', ','.join(str(s.pid) for s in servers)])
                await asyncio.sleep(1)

            try:
                before = await snapshot(manager, servers)
                seconds = await run_writes(cql, stmt, WARMUP_WRITES, WRITES, value, router)
                load = (await snapshot(manager, servers)) - before
            finally:
                status = stop_perf(perf)

            # perf record exits with -SIGINT when stopped by SIGINT.
            if perf and (status not in (0, -signal.SIGINT) or not os.path.exists(data) or os.path.getsize(data) == 0):
                raise RuntimeError(f"perf record failed for {mode}: status {status}, profile {data}")

            results[mode] = report(mode, load, idle, WRITES, seconds)

    (sc_cpu, sc_rate), (ec_cpu, ec_rate) = results['sc'], results['ec']
    out("SC/EC CPU per write: %.2fx", sc_cpu / ec_cpu)
    # Batching follows the rate, so a comparison at different rates is not one
    # of the write paths. Pacing keeps the rates equal unless the cluster
    # cannot keep up with RATE.
    if abs(sc_rate - ec_rate) > 0.1 * max(sc_rate, ec_rate):
        out("WARNING: the rates differ (%.0f vs %.0f writes/s), lower SC_CPU_RATE", sc_rate, ec_rate)
    for mode, rate in (('ec', ec_rate), ('sc', sc_rate)):
        if RATE and rate < 0.9 * RATE:
            out("WARNING: %s ran at %.0f writes/s, below SC_CPU_RATE=%d; lower it or raise SC_CPU_CONCURRENCY",
                mode, rate, RATE)
