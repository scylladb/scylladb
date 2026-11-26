#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import os
import random
import time

import pytest
from cassandra.cluster import Session
from cassandra.query import PreparedStatement

from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import ScyllaMetrics, ScyllaMetricsLine
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# One small shard, so a modest data set overflows the cache within seconds.
CMDLINE = ['--smp', '1', '-m', '256M', '--cache-ghost-list-size-mb', '1']
VALUE_SIZE = 128 * 1024
# 192 MB of rows: the cache of a 256 MB node holds about 110 MB.
NUM_KEYS = 1500
HOT_KEYS = 200
SKIP_DEBUG = 'debug builds ignore --memory, so the cache never fills'


class Histogram:
    """One cache histogram of a node; empty until its first sample."""

    def __init__(self, metrics: ScyllaMetrics, name: str) -> None:
        lines = [ScyllaMetricsLine.from_string(l) for l in metrics.lines_by_prefix(f'{name}_bucket')]
        # Cumulative (upper bound, samples) pairs.
        self.buckets = sorted((float(l.labels['le']), l.value) for l in lines if l is not None)
        self.labels = [l.labels for l in lines if l is not None]
        self.count = self.buckets[-1][1] if self.buckets else 0

    def samples_up_to(self, bound: float) -> float:
        return max((v for le, v in self.buckets if le <= bound), default=0)

    def __str__(self) -> str:
        # Buckets are cumulative; show how many samples each one adds.
        prev = [0] + [v for _, v in self.buckets]
        per_bucket = {f'<={le:g}': int(v - p) for (le, v), p in zip(self.buckets, prev) if v > p}
        return f'samples={int(self.count)} {per_bucket}'


class AgeStats:
    """The eviction age and ghost hit histograms, and the LRU front age, of one node."""

    def __init__(self, metrics: ScyllaMetrics) -> None:
        self.eviction_age = Histogram(metrics, 'scylla_cache_eviction_age_seconds')
        self.ghost_hit_age = Histogram(metrics, 'scylla_cache_ghost_hit_age_seconds')
        self.ghost_hit_bytes = Histogram(metrics, 'scylla_cache_ghost_hit_evicted_bytes')
        self.front_age = metrics.get('scylla_cache_lru_front_age_seconds') or 0

    def __str__(self) -> str:
        return (f'front_age={self.front_age:.0f}s eviction age (s): {self.eviction_age}; '
                f'ghost hit age (s): {self.ghost_hit_age}; evicted before ghost hit (bytes): {self.ghost_hit_bytes}')


async def age_stats(manager: ScyllaClusterManager, server: ServerInfo) -> AgeStats:
    return AgeStats(await manager.metrics.query(server.ip_addr))


async def read_keys(cql: Session, select: PreparedStatement, keys: list[int]) -> None:
    """Reads the keys, 50 at a time."""
    for i in range(0, len(keys), 50):
        await asyncio.gather(*[cql.run_async(select, [pk]) for pk in keys[i:i + 50]])


async def populate(manager: ScyllaClusterManager, server: ServerInfo, cql: Session, ks: str) -> PreparedStatement:
    """Writes and flushes the data set; returns the statement that reads one key."""
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v blob)")
    insert = cql.prepare(f"INSERT INTO {ks}.t (pk, v) VALUES (?, ?)")
    value = os.urandom(VALUE_SIZE)
    for i in range(0, NUM_KEYS, 50):
        await asyncio.gather(*[cql.run_async(insert, [pk, value]) for pk in range(i, i + 50)])
    # Reads must come from sstables, so that they populate the cache.
    await manager.api.keyspace_flush(server.ip_addr, ks, 't')
    return cql.prepare(f"SELECT v FROM {ks}.t WHERE pk = ?")


@pytest.mark.skip_mode(mode='debug', reason=SKIP_DEBUG)
async def test_eviction_age_cache_unfriendly_workload(manager: ScyllaClusterManager):
    """Reads spread evenly over a data set larger than the cache keep evicting entries
    that were read moments ago: many samples, all of them young."""
    boot = time.time()
    server = await manager.server_add(cmdline=CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        select = await populate(manager, server, cql, ks)
        before = await age_stats(manager, server)
        start = time.time()
        for _ in range(2):
            keys = list(range(NUM_KEYS))
            random.shuffle(keys)
            await read_keys(cql, select, keys)
        elapsed = time.time() - start
        after = await age_stats(manager, server)
        logger.info(f'unfriendly workload, {elapsed:.0f}s: before: {before}, after: {after}')

        # The second pass alone must evict at least the part of the data that does not fit.
        assert after.eviction_age.count - before.eviction_age.count >= NUM_KEYS // 4
        # Nothing in the cache can be older than the node; the bucket bound adds slack.
        bound = (time.time() - boot) * 2 + 2
        assert after.eviction_age.samples_up_to(bound) == after.eviction_age.count
        assert after.front_age <= bound
        # The second pass re-reads what the first one evicted.
        assert after.ghost_hit_age.count - before.ghost_hit_age.count >= NUM_KEYS // 4
        assert after.ghost_hit_age.samples_up_to(bound) == after.ghost_hit_age.count
        # Evicted early in pass one, re-read late in pass two: under two data sets in between (2 GB bucket bound).
        assert after.ghost_hit_bytes.samples_up_to(2 ** 31) == after.ghost_hit_bytes.count
        # Histograms are reported per node, the front age per shard.
        assert all('shard' not in labels for labels in after.eviction_age.labels + after.ghost_hit_age.labels)


@pytest.mark.skip_mode(mode='debug', reason=SKIP_DEBUG)
async def test_eviction_age_cache_friendly_workload(manager: ScyllaClusterManager):
    """A working set that fits in the cache evicts nothing, and the oldest cached
    entry keeps ageing: the signal that the cache is larger than the workload needs."""
    server = await manager.server_add(cmdline=CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        select = await populate(manager, server, cql, ks)
        # Fill the cache with cold entries, then bring the working set in.
        await read_keys(cql, select, list(range(NUM_KEYS)))
        hot = list(range(HOT_KEYS))
        await read_keys(cql, select, hot)
        before = await age_stats(manager, server)
        duration = 4
        start = time.time()
        while time.time() - start < duration:
            await read_keys(cql, select, hot)
        after = await age_stats(manager, server)
        logger.info(f'friendly workload, {duration}s: before: {before}, after: {after}')

        # Filling the cache evicted the overflow, so the histogram is populated.
        assert before.eviction_age.count >= NUM_KEYS // 4
        # Background work, such as compaction, may still evict some entries.
        assert after.eviction_age.count - before.eviction_age.count <= NUM_KEYS // 10
        # The hot keys stay cached, so they are never read back from ghosts.
        assert after.ghost_hit_age.count - before.ghost_hit_age.count <= NUM_KEYS // 10
        # The cold entries at the LRU front were last read before the hot loop started.
        assert after.front_age >= duration - 2
