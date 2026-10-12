#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Snapshots taken, listed, cleared and restored through "scylla nodetool", on
a node of their own, and checked against the snapshot directories on disk.

Ported from scylla-dtest. Like the dtests, the nodes create keyspaces with
vnodes by default.
"""

import asyncio
import contextlib
import uuid

import pytest

from test import DEBUG_MODES
from test.cluster.util import run_nodetool
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node, and dropped or
# truncated tables are snapshotted (test.py turns auto_snapshot off).
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'tablets_initial_scale_factor': 1, 'auto_snapshot': True}


def ks_opts(rf: int = 1) -> str:
    return f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"


############################################################################
# Ported from snapshot_test.py: snapshots of keyspaces and tables, restored
# by copying their sstables into upload/ and "nodetool refresh", their
# schema.cql files, concurrent snapshot operations, and "nodetool snapshot"
# options.


def create_cf_statement(ks: str, name: str, key_type: str = "varchar", columns: dict | None = None) -> str:
    """The CREATE statement of the dtests' create_cf() helper."""
    if columns:
        cols = "".join(f", {k} {v}" for k, v in columns.items())
        query = f"CREATE COLUMNFAMILY {ks}.{name} (key {key_type} PRIMARY KEY{cols}) WITH comment='test cf'"
    else:
        query = f"CREATE COLUMNFAMILY {ks}.{name} (key {key_type}, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf'"
    return query + " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}"


async def create_cf(cql, ks: str, name: str, key_type: str = "varchar", columns: dict | None = None) -> None:
    """The dtests' create_cf(): create the table, then read it with a
    "SELECT * ... LIMIT 1" to check it exists."""
    await cql.run_async(create_cf_statement(ks, name, key_type, columns))
    await cql.run_async(f"SELECT * FROM {ks}.{name} LIMIT 1")


async def prepare_schemas_and_data(cql, num_ks=1, num_cf=1, num_rows=1, column_length=10):
    """The keyspaces ks<i> of tables table_cf<j> of the parallel snapshot
    dtests, with rows written concurrently."""
    for j in range(num_ks):
        await cql.run_async(f"CREATE KEYSPACE ks{j} {ks_opts()}")
        for k in range(num_cf):
            await create_cf(cql, f"ks{j}", f"table_cf{k}")
            st = cql.prepare(f"INSERT INTO ks{j}.table_cf{k} (key, c, v) VALUES (?, ?, ?)")
            await asyncio.gather(*[cql.run_async(st, [str(i), str(i), f"{i}" * column_length]) for i in range(num_rows)])


class SnapshotOperations:
    """The concurrent snapshot operations of the parallel dtests, each a
    sequence of nodetool commands; run() returns their stderr."""
    def __init__(self, manager, server):
        self.manager, self.server = manager, server

    async def nodetool(self, *args) -> str:
        _, err, _ = await run_nodetool(self.manager, self.server, *args, check=False)
        return err

    async def snapshot_all(self) -> list[str]:
        return [await self.nodetool("snapshot", "-t", str(uuid.uuid4()))]

    async def snapshot_per_table(self, num_ks: int, num_cf: int) -> list[str]:
        return [await self.nodetool("snapshot", f"ks{i}.table_cf{j}", "-t", str(uuid.uuid4())) for i in range(num_ks) for j in range(num_cf)]

    async def clear_all(self) -> list[str]:
        return [await self.nodetool("clearsnapshot")]

    async def clear_per_keyspace(self, num_ks: int) -> list[str]:
        return [await self.nodetool("clearsnapshot", f"ks{i}") for i in range(num_ks)]

    async def list(self) -> list[str]:
        return [await self.nodetool("listsnapshots")]


async def run_in_parallel(*ops) -> None:
    """Run the operations concurrently; none may print to stderr."""
    for errs in await asyncio.gather(*ops):
        assert not any(errs), errs


# The operations mixed concurrently on 15 keyspaces of 15 tables of 1000
# rows with 1000-byte values, as in the dtest.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_operations_with_data(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=15, num_cf=15, num_rows=1000, column_length=1000)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_all()
    await run_in_parallel(ops.snapshot_per_table(15, 15), ops.clear_per_keyspace(15), ops.snapshot_all(), ops.list(), ops.clear_all())


# The operations mixed concurrently while listings and clears of all
# snapshots repeat, on the same data as above.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_operations_with_periodic_list_and_clear(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=15, num_cf=15, num_rows=1000, column_length=1000)
    ops = SnapshotOperations(manager, server)
    done = asyncio.Event()
    async def periodically(op, interval):
        errs = []
        while not done.is_set():
            errs += await op()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(done.wait(), interval)
        return errs
    await ops.snapshot_all()
    monitor = asyncio.create_task(periodically(ops.list, 1))
    clearer = asyncio.create_task(periodically(ops.clear_all, 2))
    await run_in_parallel(ops.snapshot_all(), ops.snapshot_per_table(15, 15), ops.clear_all(), ops.snapshot_all())
    done.set()
    for t in [monitor, clearer]:
        assert not any(await t)

