#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the sizes the tablet balancer sees for logstor tables. A logstor tablet reports the live
# record bytes of the segments its group owns, and not the space of those segments: compaction keeps
# every shard's pool nearly full, so the space follows the churn of the shard, not the data of the
# tablet.

import asyncio
import logging
import struct
import time
from collections import defaultdict

from cassandra.murmur3 import murmur3

from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_count
from test.pylib.util import wait_for
from test.cluster.util import get_topology_coordinator, new_test_keyspace

logger = logging.getLogger(__name__)

segment_size = 128 * 1024

cmdline = ['--logger-log-level', 'load_balancer=debug', '--logger-log-level', 'logstor=debug',
           '--logger-log-level', 'raft_topology=debug', '--smp', '1']
cfg = {
    'experimental_features': ['logstor'],
    'tablet_load_stats_refresh_interval_in_seconds': 1,
    # The default clamps every tablet of a test to 51 MiB, so they would all look the same size.
    'minimal_tablet_size_for_balancing': 1024,
    'enable_keyspace_column_family_metrics': True,
}


async def get_metric(manager: ScyllaClusterManager, server, name: str, labels: dict | None = None) -> int:
    metrics = await manager.metrics.query(server.ip_addr)
    return int(metrics.get(name, labels or {}) or 0)


async def owned_live_bytes(manager: ScyllaClusterManager, servers) -> list[int]:
    """The live record bytes of the segments the groups of each node own, as the engine counts them.
    Compaction frees the old records of a segment before the group takes the new segment, so read
    again until all the live records of each node are in segments its groups own."""
    async def settled() -> list[int] | None:
        owned = [await get_metric(manager, s, 'scylla_logstor_sm_owned_segments_live_record_bytes') for s in servers]
        live = [await get_metric(manager, s, 'scylla_logstor_sm_live_record_bytes') for s in servers]
        logger.info(f"owned live record bytes: {owned}, live record bytes: {live}")
        if owned == live:
            return owned
    return await wait_for(settled, time.time() + 60)


async def segments_in_use(manager: ScyllaClusterManager, server) -> int:
    return await get_metric(manager, server, 'scylla_logstor_sm_segments_in_use')


def token_of(pk: int) -> int:
    return murmur3(struct.pack('>i', pk))


async def keys_by_tablet(manager: ScyllaClusterManager, server, ks: str, table: str, keys_per_tablet: int):
    """Returns the tablets of the table as (last_token, host) pairs, each with keys_per_tablet integer
    keys whose token the tablet owns"""
    tablets = await get_all_tablet_replicas(manager, server, ks, table)
    last_tokens = sorted(t.last_token for t in tablets)
    keys = defaultdict(list)
    pk = 0
    while any(len(keys[t]) < keys_per_tablet for t in last_tokens):
        token = token_of(pk)
        tablet_last_token = next(t for t in last_tokens if t >= token)
        if len(keys[tablet_last_token]) < keys_per_tablet:
            keys[tablet_last_token].append(pk)
        pk += 1
    return [(t.last_token, t.replicas[0][0], keys[t.last_token]) for t in tablets]


async def write(cql, ks: str, keys: list[int], value: str):
    insert = cql.prepare(f"INSERT INTO {ks}.test (pk, v) VALUES (?, ?)")
    await asyncio.gather(*[cql.run_async(insert, [pk, value]) for pk in keys])


async def flush_and_refresh(manager: ScyllaClusterManager, servers):
    """Moves the records of all the nodes into the segments of their groups, which is where a tablet
    size counts them, and waits for the coordinator to read the load stats after that"""
    for s in servers:
        await manager.api.logstor_flush(s.ip_addr)
    coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    log = await manager.server_open_log(coordinator.server_id)
    mark = await log.mark()
    # The refreshes run one at a time. The first one logged after the mark may have read the sizes
    # during a flush; the second one started after the first ended, so after all the flushes.
    for _ in range(2):
        mark, _ = await log.wait_for('Refreshed table load stats for all DC', from_mark=mark, timeout=60)


async def placement(manager: ScyllaClusterManager, server, ks: str):
    return sorted((t.last_token, t.replicas) for t in await get_all_tablet_replicas(manager, server, ks, 'test'))


async def test_tablet_split_trigger_by_size(manager: ScyllaClusterManager):
    """
    Test that a logstor table automatically splits and merges tablets as the size of its data
    crosses the thresholds set by --target-tablet-size-in-bytes.

    The size of a logstor tablet is the live record bytes of its segments. A tablet splits above
    twice the target, 600KB: writing 7 keys with ~100KB values gives ~700KB of live records, which
    triggers an automatic split. After the split, all data must remain readable.

    A tablet merges below half the target, 150KB: deleting 6 of the keys replaces their values with
    small tombstones, which leaves ~100KB of live records, so the tablets merge back. The segments
    of the deleted values stay in place until compaction reclaims them, so the merge shows that the
    size follows the live records and not the space of the segments.
    """
    cmdline = [
        '--logger-log-level', 'logstor=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--target-tablet-size-in-bytes', '300000',
        '--smp=1',
    ]
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'experimental_features': ['logstor'],
    }
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]
    cql = manager.get_cql()

    s0_log = await manager.server_open_log(servers[0].server_id)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        assert await get_tablet_count(manager, servers[0], ks, 'test') == 1

        s0_mark = await s0_log.mark()

        # Write 7 keys with ~100KB values, ~700KB of live records.
        # This exceeds twice the 300KB target and should trigger a split.
        key_count = 7
        value = 'x' * (100 * 1024)
        for i in range(key_count):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{value}')")

        # Flush to assign segments to compaction groups so their size is
        # visible to tablet load stats.
        await manager.api.logstor_flush(servers[0].ip_addr)

        await s0_log.wait_for('Detected tablet split for table', from_mark=s0_mark, timeout=60)

        # Verify all data is accessible after the split
        for i in range(key_count):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1, f"Key {i} not found after tablet split"
            assert rows[0].v == value, f"Wrong value for key {i} after tablet split"

        # Delete all keys but one, which leaves ~100KB of live records, below half the target, so the
        # two tablets merge back into one.
        s0_mark = await s0_log.mark()
        for i in range(1, key_count):
            await cql.run_async(f"DELETE FROM {ks}.test WHERE pk = {i}")
        await manager.api.logstor_flush(servers[0].ip_addr)
        await s0_log.wait_for('Detected tablet merge for table', from_mark=s0_mark, timeout=60)
        assert await get_tablet_count(manager, servers[0], ks, 'test') == 1

        # The merge came from the live records alone: no segment was compacted.
        metrics = await manager.metrics.query(servers[0].ip_addr)
        assert (metrics.get("scylla_logstor_sm_compaction_segments_in") or 0) == 0

        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test")
        assert [(r.pk, r.v) for r in rows] == [(0, value)]


async def test_balance_by_live_bytes(manager: ScyllaClusterManager):
    """
    Two nodes hold the same number of tablets, and churn fills most of the segments of both pools, but
    one node holds three times the live data of the other. A third node joins, and the balancer must
    spread the live data over the three nodes. If the balancer saw the occupied segments instead, the
    two first nodes would look almost the same and it would move tablets mostly by count, which leaves
    the most loaded node with at least twice the live data of the least loaded one.
    """
    value_size = 60 * 1024
    value = 'x' * value_size
    heavy_keys_per_tablet = 12
    light_keys_per_tablet = 4
    # The capacity of a node is its free space plus its pool, not the raw disk.
    balancing_cfg = cfg | {'force_effective_capacity_to_raw_disk_capacity': False}

    servers = await manager.servers_add(2, cmdline=cmdline, config=balancing_cfg, property_file={'dc': 'dc1', 'rack': 'r1'})
    cql = manager.get_cql()
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        heavy_host = await manager.get_host_id(servers[0].server_id)
        tablets = await keys_by_tablet(manager, servers[0], ks, 'test', heavy_keys_per_tablet)
        assert sum(1 for _, host, _ in tablets if host == heavy_host) == 4, f"expected 4 tablets on each node, got {tablets}"
        keys_on = defaultdict(list)
        for _, host, keys in tablets:
            keys_on[host] += keys if host == heavy_host else keys[:light_keys_per_tablet]
        all_keys = [pk for keys in keys_on.values() for pk in keys]

        await write(cql, ks, all_keys, value)

        # Overwrite the data until most of the segments of both pools are in use, while their live data
        # is not the same: the heavy node holds about 24 segments of live records and the light node
        # about 8, out of 64.
        async def pools_full() -> bool:
            in_use = [await segments_in_use(manager, s) for s in servers]
            logger.info(f"segments in use: {in_use}")
            if all(n >= 40 for n in in_use):
                return True
            await write(cql, ks, all_keys, value)
        await wait_for(pools_full, time.time() + 120)

        await flush_and_refresh(manager, servers)
        live_before = await owned_live_bytes(manager, servers)
        logger.info(f"live bytes before balancing: {live_before}")

        servers.append(await manager.server_add(cmdline=cmdline, config=balancing_cfg, property_file={'dc': 'dc1', 'rack': 'r1'}))
        await flush_and_refresh(manager, servers)
        await manager.enable_tablet_balancing()
        await manager.api.quiesce_topology(servers[0].ip_addr)
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

        live_after = await owned_live_bytes(manager, servers)
        logger.info(f"live bytes after balancing: {live_after}")
        assert sum(live_after) == sum(live_before), f"expected the live data to be unchanged by the moves, {live_before} -> {live_after}"
        assert min(live_after) > 0 and max(live_after) / min(live_after) < 2, f"expected the live data to be spread over the nodes, got {live_after}"

        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test")
        assert sorted(r.pk for r in rows) == sorted(all_keys)
        assert all(r.v == value for r in rows)


async def test_churn_does_not_move_tablets(manager: ScyllaClusterManager):
    """
    The tablets of two nodes hold the same data and are balanced. The keys of one tablet are then
    overwritten many times, which adds dead records to the segments its group owns, but leaves its
    live data the same. The balancer must not move any tablet: the churn says nothing about the data
    of the tablet.
    """
    value = 'x' * (30 * 1024)
    keys_per_tablet = 4

    servers = await manager.servers_add(2, cmdline=cmdline, config=cfg, property_file={'dc': 'dc1', 'rack': 'r1'})
    cql = manager.get_cql()
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        tablets = await keys_by_tablet(manager, servers[0], ks, 'test', keys_per_tablet)
        await write(cql, ks, [pk for _, _, keys in tablets for pk in keys], value)
        await flush_and_refresh(manager, servers)

        await manager.enable_tablet_balancing()
        await manager.api.quiesce_topology(servers[0].ip_addr)
        before = await placement(manager, servers[0], ks)

        # Balancing is off during the churn: until the separator moves an overwrite into the segments
        # of its group, the tablet does not count it, so the sizes the balancer sees lag behind.
        await manager.disable_tablet_balancing()
        churned_keys = tablets[0][2]
        for _ in range(40):
            await write(cql, ks, churned_keys, value)
        await flush_and_refresh(manager, servers)

        await manager.enable_tablet_balancing()
        await manager.api.quiesce_topology(servers[0].ip_addr)
        after = await placement(manager, servers[0], ks)
        assert after == before, f"expected no tablet to move after the churn, placement {before} -> {after}"


async def test_tablet_sizes_reach_coordinator(manager: ScyllaClusterManager):
    """
    The tablet sizes the coordinator reads are the live record bytes of the table: overwrites and
    compaction change the space the table takes, but not its size. The capacity of the node covers
    the logstor files, of which the tablet sizes are only the live part. On a development disk the
    free space alone is much larger than the files, so the capacity check cannot tell the files
    were added.
    """
    key_count = 20
    value = 'x' * 2000

    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg | {'force_effective_capacity_to_raw_disk_capacity': False})
    server = servers[0]
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table_id = await manager.get_table_id(ks, 'test')
        labels = {'ks': ks, 'cf': 'test'}

        async def tablet_sizes() -> int:
            rows = await cql.run_async(f"SELECT * FROM system.tablet_sizes WHERE table_id = {table_id}")
            assert all(not r.missing_replicas for r in rows), f"expected every tablet to report its size, got {rows}"
            return sum(size for r in rows for size in r.replicas.values())

        await write(cql, ks, list(range(key_count)), value)
        await flush_and_refresh(manager, servers)

        size = await tablet_sizes()
        live_record_bytes = await get_metric(manager, server, 'scylla_column_family_logstor_live_record_bytes', labels)
        live_disk_space = await get_metric(manager, server, 'scylla_column_family_live_disk_space', labels)
        logger.info(f"after writes: tablet sizes={size} live_record_bytes={live_record_bytes} live_disk_space={live_disk_space}")
        assert size == live_record_bytes, f"expected the tablet sizes to be the live record bytes {live_record_bytes}, got {size}"
        assert size < live_disk_space

        for _ in range(3):
            await write(cql, ks, list(range(key_count)), value)
        await flush_and_refresh(manager, servers)
        assert await tablet_sizes() == size
        live_disk_space_after_overwrites = await get_metric(manager, server, 'scylla_column_family_live_disk_space', labels)
        assert live_disk_space_after_overwrites > live_disk_space

        async def compacted() -> bool:
            if await get_metric(manager, server, 'scylla_column_family_live_disk_space', labels) < live_disk_space_after_overwrites:
                return True
            await manager.api.logstor_compaction(server.ip_addr)
        await wait_for(compacted, time.time() + 60)

        # Compaction can still run. It frees the old records of a segment before the group takes the
        # new segment, and a refresh between the two reads less than the size, so refresh again.
        async def size_unchanged() -> bool | None:
            await flush_and_refresh(manager, servers)
            if await tablet_sizes() == size:
                return True
        await wait_for(size_unchanged, time.time() + 60)

        host_id = await manager.get_host_id(server.server_id)
        rows = await cql.run_async(f"SELECT * FROM system.load_per_node WHERE node = {host_id}")
        assert len(rows) == 1
        load = rows[0]
        disk_usage = await get_metric(manager, server, 'scylla_logstor_sm_disk_usage')
        logger.info(f"load_per_node: effective_capacity={load.effective_capacity} storage_load={load.storage_load} "
                    f"storage_utilization={load.storage_utilization} logstor disk_usage={disk_usage}")
        assert load.effective_capacity >= disk_usage
        assert load.storage_load == size
        assert 0 <= load.storage_utilization < 1
