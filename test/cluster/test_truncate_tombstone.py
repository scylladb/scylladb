#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Truncate deletes a table by writing a tombstone covering the whole ring,
rather than by flushing and discarding sstables. These cover the two paths
which only a real truncate exercises: getting the tombstone back out of the
commitlog after a restart, and reconciling it between replicas by repair.
"""

import logging
import random
import time

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.tablets import get_tablet_count
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


async def _insert_rows(cql, table, count):
    for pk in range(count):
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})")


def _count_local(cql, table, host):
    """Rows this node holds, read from the node itself."""
    stmt = SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.ONE)
    return len(list(cql.execute(stmt, host=host)))


@pytest.mark.asyncio
async def test_truncate_survives_restart(manager: ScyllaClusterManager):
    """
    Truncate no longer flushes, so until the memtable is flushed the tombstone
    lives only in the commitlog. Restarting without flushing therefore replays
    it, which exercises the replay path: the tombstone carries a placeholder
    partition key, so replay must not filter or route it by that key.
    """
    server = await manager.server_add()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await _insert_rows(cql, table, 100)

        await cql.run_async(f"TRUNCATE {table}")
        assert len(list(await cql.run_async(f"SELECT * FROM {table}"))) == 0

        # A point read selects sstables by bloom filter, which an sstable
        # holding only the tombstone fails, so it has to be read regardless.
        assert len(list(await cql.run_async(f"SELECT * FROM {table} WHERE pk = 7 BYPASS CACHE"))) == 0

        # Deliberately no flush: the tombstone has to come back from the
        # commitlog, not from an sstable.
        await manager.server_restart(server.server_id)
        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        rows = list(await cql.run_async(f"SELECT * FROM {table}"))
        assert len(rows) == 0, f"truncate was lost across a restart, {len(rows)} rows came back"
        # The shutdown flushed the tombstone into an sstable of its own, which
        # stores no key and so fails every bloom filter check; a point read
        # selects sstables by that filter, and has to read this one anyway.
        rows = list(await cql.run_async(f"SELECT * FROM {table} WHERE pk = 7 BYPASS CACHE"))
        assert len(rows) == 0, f"a point read missed the truncate after a restart, {len(rows)} rows came back"


@pytest.mark.asyncio
async def test_truncate_reconciled_by_repair(manager: ScyllaClusterManager):
    """
    A token range tombstone is not a row, so it takes no part in repair's row
    diff; repair exchanges these separately in its per range handshake. Leave
    one node without the tombstone and check that repair gives it one.

    Truncate cannot be made to miss a node by stopping it - it refuses to run
    unless every node is up - so an injection is used instead.
    """
    servers = await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await _insert_rows(cql, table, 100)

        # Only the third node skips applying the tombstone.
        await inject_error_one_shot(manager.api, servers[2].ip_addr, "skip_truncate_token_range_tombstone")

        await cql.run_async(f"TRUNCATE {table}")

        assert _count_local(cql, table, hosts[0]) == 0
        assert _count_local(cql, table, hosts[1]) == 0
        assert _count_local(cql, table, hosts[2]) > 0, \
            "injection did not take effect, the third node has no data to reconcile"

        await manager.api.repair(servers[2].ip_addr, ks, "t")

        assert _count_local(cql, table, hosts[2]) == 0, \
            "repair did not carry the token range tombstone to the node which missed the truncate"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_truncate_survives_tablet_split_and_merge(manager: ScyllaClusterManager):
    """
    Splitting a tablet flushes the memtables of the group being split, so the
    tombstone reaches an sstable, and the split compaction then has to carry it
    into both halves. Merging brings the halves back together and has to keep
    it. Either losing it resurrects everything the truncate deleted.
    """
    cmdline = [
        '--logger-log-level', 'table=debug',
        '--target-tablet-size-in-bytes', '30000',
    ]
    servers = [await manager.server_add(config={
        'tablet_load_stats_refresh_interval_in_seconds': 1
    }, cmdline=cmdline)]
    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                          "AND tablets = {'initial': 1}") as ks:
        table = f"{ks}.t"
        # gc_grace_seconds=0 so that compaction is free to drop what the
        # tombstone covers, which is the case where losing it would show.
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c blob) "
                            f"WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1")

        insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
        for pk in range(200):
            cql.execute(insert, [pk, random.randbytes(2000)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        assert await get_tablet_count(manager, servers[0], ks, 't') == 1

        await cql.run_async(f"TRUNCATE {table}")
        assert len(list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))) == 0

        # A memtable holding only the tombstone has no partitions; flushing it
        # must still produce an sstable carrying the tombstone.
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        rows = list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))
        assert len(rows) == 0, f"flush lost the truncate, {len(rows)} rows came back"
        rows = list(await cql.run_async(f"SELECT * FROM {table} WHERE pk = 7 BYPASS CACHE"))
        assert len(rows) == 0, f"a point read missed the flushed truncate, {len(rows)} rows came back"

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        # The data written above is well over the target tablet size, so the
        # balancer splits, which flushes and then splits the sstables.
        await manager.enable_tablet_balancing()
        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)
        assert await get_tablet_count(manager, servers[0], ks, 't') > 1

        rows = list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))
        assert len(rows) == 0, f"tablet split lost the truncate, {len(rows)} rows came back"

        # Now shrink it back. Compacting away the data the tombstone covers is
        # what makes the tablets small enough to merge.
        await manager.disable_tablet_balancing()
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        await manager.api.keyspace_compaction(servers[0].ip_addr, ks)
        s1_mark = await s1_log.mark()
        await manager.enable_tablet_balancing()
        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)

        rows = list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))
        assert len(rows) == 0, f"tablet merge lost the truncate, {len(rows)} rows came back"
