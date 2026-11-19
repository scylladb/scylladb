#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
import logging
import time

import pytest
from cassandra.cluster import Session
from cassandra.pool import Host
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.pylib.repair import create_table_insert_data_for_repair, load_tablet_repair_time
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_count
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


async def partition_tombstone_sources(cql: Session, ks: str, key: int, host: Host) -> list[str]:
    """The mutation sources (memtables, sstables) of `host` alone that hold a partition tombstone for `key`."""
    rows = await cql.run_async(SimpleStatement(
        f"SELECT mutation_source, mutation_fragment_kind, metadata FROM MUTATION_FRAGMENTS({ks}.test) WHERE pk = {key}",
        consistency_level=ConsistencyLevel.ONE), host=host)
    return [r.mutation_source for r in rows
            if r.mutation_fragment_kind == "partition start" and json.loads(r.metadata)["tombstone"]]


async def sstable_partition_tombstone(cql: Session, ks: str, key: int, host: Host) -> bool:
    return any(src.startswith("sstable:") for src in await partition_tombstone_sources(cql, ks, key, host))


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_merge_does_not_advance_tombstone_gc_of_unrepaired_sibling(manager: ScyllaClusterManager):
    """
    Two sibling tablets L and R. R holds a key whose deletion reached only two of
    the three replicas. L is repaired after the deletion, R is not. Merging L and
    R must not let the two replicas holding the tombstone purge it: no repair has
    copied it to the third replica yet, which still holds the deleted row.
    Before the fix, the merged tablet took L's repair time over the whole merged
    range, the tombstone was purged, and the row came back.
    Reproduces SCYLLADB-6091.
    """
    # One shard, so both siblings and the merged tablet live on shard 0 of every node.
    # No hints, so nothing but repair can deliver the tombstone to the replica that missed it.
    # No commitlog, so its replay-safety bound on gc_before does not depend on segment timing.
    cmdline = ['--smp', '1', '--hinted-handoff-enabled', '0', '--enable-commitlog', '0',
               '--logger-log-level', 'table=debug']
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(
        manager, rf=3, tablets=2, nr_keys=0, cmdline=cmdline, disable_flush_cache_time=True)
    # gc_before == repair_time, so a tombstone written before a repair is purgeable right after it.
    await cql.run_async(f"ALTER TABLE {ks}.test WITH tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '0'}}")

    # x misses the deletion; y and z get it.
    x, y, z = servers
    host_x, host_y, host_z = hosts

    tablets = sorted(await get_all_tablet_replicas(manager, servers[0], ks, 'test'), key=lambda t: t.last_token)
    assert len(tablets) == 2
    left_last_token = tablets[0].last_token

    # The row is on all three replicas.
    await asyncio.gather(*[cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})",
                                                         consistency_level=ConsistencyLevel.ALL)) for k in range(32)])
    rows = await cql.run_async(f"SELECT pk, token(pk) AS t FROM {ks}.test")
    key = next(r.pk for r in rows if r.t > left_last_token)
    logger.info(f"Key {key} is in the right sibling (left last_token={left_last_token})")
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # The tombstone is on y and z only.
    await manager.api.enable_injection(x.ip_addr, "database_apply", one_shot=False,
                                       parameters={"ks_name": ks, "cf_name": "test", "what": "throw"})
    await cql.run_async(SimpleStatement(f"DELETE FROM {ks}.test WHERE pk = {key}",
                                        consistency_level=ConsistencyLevel.QUORUM), host=host_y)
    await manager.api.disable_injection(x.ip_addr, "database_apply")
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)
    assert not await partition_tombstone_sources(cql, ks, key, host_x), f"{x.ip_addr} got the deletion"

    # deletion_time and repair_time have a one-second granularity. Make the repair
    # time later than the deletion time, as it would be for any real deletion that
    # happened before a repair (same idiom as guarantee_repair_time_next_second()).
    await asyncio.sleep(1)

    # Repair the left sibling only, with all replicas up, keeping every sstable unrepaired.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", left_last_token, incremental_mode='disabled')
    await read_barrier(manager.api, servers[0].ip_addr)
    before = await load_tablet_repair_time(cql, [hosts[0]], table_id)
    logger.info(f"repair_time before merge: {before}")
    assert before[str(tablets[0].last_token)] is not None
    assert before[str(tablets[1].last_token)] is None

    # Control: the right sibling was never repaired, so compaction keeps the tombstone.
    for s, h in ((y, host_y), (z, host_z)):
        await manager.api.keyspace_compaction(s.ip_addr, ks, "test")
        assert await sstable_partition_tombstone(cql, ks, key, h), \
            f"tombstone purged on {s.ip_addr} before the merge"

    # Merge the siblings. Keep the forced decrease on afterwards, so the balancer
    # does not split the table back to its initial tablet count while the test runs.
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, "replica_merge_completion_wait", one_shot=False)
        await manager.api.enable_injection(s.ip_addr, "tablet_force_tablet_count_decrease", one_shot=False)

    async def merged():
        return await get_tablet_count(manager, servers[0], ks, 'test') == 1 or None
    await wait_for(merged, time.time() + 120)
    for s, log, mark in zip(servers, logs, marks):
        # Every replica has loaded the merged tablet map, and with it the merged
        # repair_time into its tombstone-GC state (group0 apply does both).
        await read_barrier(manager.api, s.ip_addr)
        # The sibling compaction groups are merged, and the fiber that merged
        # them has enabled compaction again (a major compaction of a group with
        # compaction disabled does nothing).
        pos, _ = await log.wait_for(rf"Merge compaction groups for table={ks}\.test group_id=\S+ range=.* finished",
                                    from_mark=mark)
        await log.wait_for("Merge completion fiber finished", from_mark=pos)

    after = await load_tablet_repair_time(cql, [hosts[0]], table_id)
    logger.info(f"repair_time after merge: {after}")

    for s in (y, z):
        await manager.api.keyspace_compaction(s.ip_addr, ks, "test")
    tombstone_kept = {s.ip_addr: await sstable_partition_tombstone(cql, ks, key, h)
                      for s, h in ((y, host_y), (z, host_z))}
    logger.info(f"Tombstone in sstables after merge and compaction: {tombstone_kept}")

    # x still holds the row; if y and z dropped the tombstone, the row is back.
    rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test WHERE pk = {key}",
                                               consistency_level=ConsistencyLevel.ALL))
    assert not rows, (f"Deleted key {key} resurrected after tablet merge: tombstone kept={tombstone_kept}, "
                      f"repair_time before merge={before}, after merge={after}")
    assert all(tombstone_kept.values()), f"Tombstone of unrepaired key {key} purged after tablet merge: {tombstone_kept}"
    # The right sibling was never repaired, so neither is the merged tablet.
    assert all(t is None for t in after.values()), f"Merged tablet got a repair_time: {after}"
