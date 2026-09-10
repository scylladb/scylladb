#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Tombstone GC on a view must wait until the view is built on every node.

The view builder writes view rows with the timestamps of the base cells they
come from, and a build task may land them any time before the build is done.
When the base row changes in the meantime, the write path deletes the same view
row with a shadowable tombstone that carries that same timestamp. If tombstone
GC purges the tombstone before the builder's row arrives, the row comes back
to life. Reproduces SCYLLADB-4337 on one node: a tablets keyspace with RF=1
collects tombstones as soon as the gc_clock second changes.
"""

import asyncio
import json
import struct
import time

import pytest
from cassandra.murmur3 import murmur3

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_view
from test.cluster.util import new_test_keyspace


PAUSE_COORDINATOR = "view_building_coordinator_pause_main_loop"
PAUSE_BUILD_TASK = "view_building_worker_pause_build_range_task"
PAUSE_LEGACY_BUILDER = "view_builder_consume_end_of_partition_delay"
TOKEN_OF_PK_1 = murmur3(struct.pack('>i', 1))
DEAD_ENTRY = (1, 1, 1, 1)
LIVE_ENTRIES = [(1, 1, 1, 3), (1, 1, 3, 3), (1, 3, 3, 3)]


def rows_for_rounds(first, last):
    # Round i moves (1,1,1,1) to v=i and adds three rows with v=i.
    return [row for i in range(first, last + 1)
            for row in ((1, 1, 1, 1, i), (1, 1, 1, i, i), (1, 1, i, i, i), (1, i, i, i, i))]


async def index_entries(cql, view):
    rows = await cql.run_async(f"SELECT pk, c1, c2, c3 FROM {view} WHERE v = 3")
    return sorted(tuple(r) for r in rows)


async def dead_entry_in_sstables(cql, view):
    """The sstable fragments of the dead entry under v=3, with their parsed metadata."""
    rows = await cql.run_async(f"SELECT mutation_source, mutation_fragment_kind, pk, c1, c2, c3, metadata"
                               f" FROM MUTATION_FRAGMENTS({view}) WHERE v = 3")
    return [json.loads(r.metadata) for r in rows
            if r.mutation_source.startswith("sstable:") and r.mutation_fragment_kind == "clustering row"
            and (r.pk, r.c1, r.c2, r.c3) == DEAD_ENTRY]


@pytest.mark.xfail(reason="SCYLLADB-4337", strict=True)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_view_build_suspends_tombstone_gc(manager: ScyllaClusterManager):
    server = (await manager.servers_add(1, cmdline=['--tablets-initial-scale-factor', '1']))[0]
    ip = server.ip_addr
    cql, _ = await manager.get_ready_cql([server])
    log = await manager.server_open_log(server.server_id)

    ks_opts = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND TABLETS = {'enabled': true}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        table = f"{ks}.t"
        view = f"{ks}.v_idx_index"
        await cql.run_async(f"CREATE TABLE {table} (pk int, c1 int, c2 int, c3 int, v int, PRIMARY KEY (pk, c1, c2, c3))")
        insert = cql.prepare(f"INSERT INTO {table} (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)")

        # Hold the coordinator so the worker flushes the base table only after
        # round 3, and hold the build task right after it created its reader
        # over that flushed state, in which (1,1,1,1) has v=3.
        await manager.api.enable_injection(ip, PAUSE_COORDINATOR, one_shot=False)
        await manager.api.enable_injection(ip, PAUSE_BUILD_TASK, one_shot=False, parameters={"token": TOKEN_OF_PK_1})
        mark = await log.mark()
        await cql.run_async(f"CREATE INDEX v_idx ON {table} (v)")
        for row in rows_for_rounds(1, 3):
            await cql.run_async(insert, list(row))
        await manager.api.disable_injection(ip, PAUSE_COORDINATOR)
        await manager.api.message_injection(ip, PAUSE_COORDINATOR)
        await log.wait_for("do_build_range: paused, waiting for message", from_mark=mark, timeout=60)

        # Rounds 4 and 5 move (1,1,1,1) to v=4 and v=5. The write path deletes
        # its v=3 entry with a tombstone stamped with the timestamp of the v=3
        # cell, the timestamp the builder will use for the same entry.
        for row in rows_for_rounds(4, 5):
            await cql.run_async(insert, list(row))
        assert await index_entries(cql, view) == LIVE_ENTRIES

        # Cross the gc_clock second boundary, then move the tombstone to an
        # sstable and read it back into the row cache.
        await asyncio.sleep(1.05 - (time.time() % 1))
        await manager.api.keyspace_flush(ip, ks, "t")
        await manager.api.keyspace_flush(ip, ks, "v_idx_index")
        assert await index_entries(cql, view) == LIVE_ENTRIES

        # Keep the coordinator from finishing the build, then let the build
        # task write its rows.
        await manager.api.enable_injection(ip, PAUSE_COORDINATOR, one_shot=False)
        await manager.api.message_injection(ip, PAUSE_BUILD_TASK)
        await log.wait_for(r"Built range \(minimum token", from_mark=mark, timeout=60)
        assert await index_entries(cql, view) == LIVE_ENTRIES

        # The build is not finished, so compaction keeps the tombstone.
        await manager.api.keyspace_flush(ip, ks, "v_idx_index")
        await manager.api.keyspace_compaction(ip, ks, "v_idx_index")
        fragments = await dead_entry_in_sstables(cql, view)
        assert fragments and all("shadowable_tombstone" in f for f in fragments), fragments
        assert await index_entries(cql, view) == LIVE_ENTRIES

        # Once the view is built, the tombstone is collectible again.
        await manager.api.disable_injection(ip, PAUSE_COORDINATOR)
        await manager.api.message_injection(ip, PAUSE_COORDINATOR)
        await wait_for_view(cql, "v_idx_index", 1)

        async def dead_entry_purged():
            await manager.api.keyspace_compaction(ip, ks, "v_idx_index")
            return not await dead_entry_in_sstables(cql, view) or None
        await wait_for(dead_entry_purged, time.time() + 60)
        assert await index_entries(cql, view) == LIVE_ENTRIES


@pytest.mark.xfail(reason="SCYLLADB-4337", strict=True)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_view_build_suspends_tombstone_gc_vnodes(manager: ScyllaClusterManager):
    """The same race with the vnodes view builder, which reports its progress per node."""
    server = (await manager.servers_add(1))[0]
    ip = server.ip_addr
    cql, _ = await manager.get_ready_cql([server])

    ks_opts = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND TABLETS = {'enabled': false}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        table = f"{ks}.t"
        view = f"{ks}.v_idx_index"
        await cql.run_async(f"CREATE TABLE {table} (pk int, c1 int, c2 int, c3 int, v int, PRIMARY KEY (pk, c1, c2, c3))")
        insert = cql.prepare(f"INSERT INTO {table} (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)")

        # The builder reads sstables only. Give it a base sstable in which
        # (1,1,1,1) has v=3, and hold it right before it writes the rows it
        # read from that sstable.
        for row in rows_for_rounds(1, 3):
            await cql.run_async(insert, list(row))
        await manager.api.keyspace_flush(ip, ks, "t")
        await manager.api.enable_injection(ip, PAUSE_LEGACY_BUILDER, one_shot=True)
        await cql.run_async(f"CREATE INDEX v_idx ON {table} (v)")
        await manager.api.wait_for_injection_enter(ip, PAUSE_LEGACY_BUILDER)

        # Only the builder can add the v=3 entries of rounds 1 to 3. The write
        # path only deletes the one of (1,1,1,1), which it sees move to v=4.
        for row in rows_for_rounds(4, 5):
            await cql.run_async(insert, list(row))
        assert await index_entries(cql, view) == []

        await asyncio.sleep(1.05 - (time.time() % 1))
        await manager.api.keyspace_flush(ip, ks, "v_idx_index")
        assert await index_entries(cql, view) == []

        # The build is not finished, so compaction keeps the tombstone.
        await manager.api.keyspace_compaction(ip, ks, "v_idx_index")
        fragments = await dead_entry_in_sstables(cql, view)
        assert fragments and all("shadowable_tombstone" in f for f in fragments), fragments

        await manager.api.message_injection(ip, PAUSE_LEGACY_BUILDER)
        await wait_for_view(cql, "v_idx_index", 1)
        assert await index_entries(cql, view) == LIVE_ENTRIES

        async def dead_entry_purged():
            await manager.api.keyspace_compaction(ip, ks, "v_idx_index")
            return not await dead_entry_in_sstables(cql, view) or None
        await wait_for(dead_entry_purged, time.time() + 60)
        assert await index_entries(cql, view) == LIVE_ENTRIES
