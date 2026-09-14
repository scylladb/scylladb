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


async def test_truncate_survives_tablet_split_and_merge(manager: ScyllaClusterManager):
    """
    A tablet split compacts each sstable of the group into the two halves, and
    a merge brings the halves back together; both have to carry the truncate's
    tombstone along. Losing it would let anything written with a timestamp
    older than the truncate show up again.

    The data the truncate deleted cannot serve as the witness: the tombstone
    sets off a compaction of everything it deletes as soon as it is flushed,
    and the first compaction of any kind drops those sstables unread. So the
    tablet is made large enough to split with data written after the truncate,
    and the tombstone's presence in each half is checked by writing a row with
    a timestamp older than the truncate and seeing that it stays deleted.
    """
    # A tablet splits above twice the target and merges below half of it. 200
    # rows of 2000 bytes are ~420KB: above 300KB, so the tablet splits once,
    # and the halves, ~210KB, stay put.
    cmdline = [
        '--logger-log-level', 'table=debug',
        '--target-tablet-size-in-bytes', '150000',
    ]
    servers = [await manager.server_add(config={
        'tablet_load_stats_refresh_interval_in_seconds': 1
    }, cmdline=cmdline)]
    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                          "AND tablets = {'initial': 1}") as ks:
        table = f"{ks}.t"
        # The default gc_grace_seconds keeps the tombstone from being purged
        # once the data it deleted is gone; the test relies on it being there.
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c blob) WITH bloom_filter_fp_chance=1")

        insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
        for pk in range(200):
            cql.execute(insert, [pk, random.randbytes(2000)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        assert await get_tablet_count(manager, servers[0], ks, 't') == 1

        async def deletes_writes_older_than(truncated_at_us, keys):
            """The tombstone from a truncate at `truncated_at_us` is still in
            force for `keys`: a write from before it stays deleted."""
            for pk in keys:
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, 0x00) USING TIMESTAMP {truncated_at_us - 1}")
                rows = list(await cql.run_async(f"SELECT pk FROM {table} WHERE pk = {pk} BYPASS CACHE"))
                assert len(rows) == 0, f"a write older than the truncate came back for pk={pk}: the tombstone was lost"

        before_truncate = int(time.time() * 1_000_000)
        await cql.run_async(f"TRUNCATE {table}")
        assert len(list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))) == 0

        # A memtable holding only the tombstone has no partitions; flushing it
        # must still produce an sstable carrying the tombstone.
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        rows = list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))
        assert len(rows) == 0, f"flush lost the truncate, {len(rows)} rows came back"
        rows = list(await cql.run_async(f"SELECT * FROM {table} WHERE pk = 7 BYPASS CACHE"))
        assert len(rows) == 0, f"a point read missed the flushed truncate, {len(rows)} rows came back"
        await deletes_writes_older_than(before_truncate, range(0, 20))

        # Data written after the truncate survives it, and makes the tablet
        # worth splitting again.
        for pk in range(1000, 1200):
            cql.execute(insert, [pk, random.randbytes(2000)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()
        await manager.enable_tablet_balancing()
        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)
        assert await get_tablet_count(manager, servers[0], ks, 't') == 2
        # Let the split finish moving sstables into the new groups before
        # looking at what they hold.
        await manager.disable_tablet_balancing()
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        # Both halves still hold the tombstone, and the data written after it.
        await deletes_writes_older_than(before_truncate, range(0, 40))
        rows = list(await cql.run_async(f"SELECT pk FROM {table} BYPASS CACHE"))
        assert len(rows) == 200, f"the split lost data written after the truncate, {len(rows)} of 200 rows left"

        # Now shrink it back: truncate again, which compacts away what the new
        # tombstone deletes and leaves the tablets small enough to merge.
        before_second_truncate = int(time.time() * 1_000_000)
        await cql.run_async(f"TRUNCATE {table}")
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        await manager.api.keyspace_compaction(servers[0].ip_addr, ks)
        s1_mark = await s1_log.mark()
        await manager.enable_tablet_balancing()
        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)

        rows = list(await cql.run_async(f"SELECT * FROM {table} BYPASS CACHE"))
        assert len(rows) == 0, f"tablet merge lost the truncate, {len(rows)} rows came back"
        await deletes_writes_older_than(before_second_truncate, range(1000, 1040))
