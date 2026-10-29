#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
A schema change is committed shard by shard on every node (see schema_applier::commit()), so a
request built on a node which has already committed it may reach a node - and a shard of that
node - which has not. It has to wait for the commit there instead of failing with
no_such_column_family.

test/boost/schema_change_cross_shard_test.cc covers the single-node case. Here the request
crosses nodes as well. A verb handler on a shard of the paused node which has not committed the
change does not have the schema synced, so it takes a group 0 read barrier, which waits for the
change to be applied; one on shard 0, which has committed it, forwards the request to the
owning shard, which waits for shard 0's schema commit generation (see global_schema_ptr::get()).
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

import pytest
from cassandra.cluster import ConsistencyLevel              # type: ignore # pylint: disable=no-name-in-module
from cassandra.protocol import InvalidRequest

from test.pylib.rest_client import inject_error, read_barrier
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

PAUSE_INJECTION = "schema_applier_pause_before_commit_on_other_shards"

# Both nodes run two shards, so with this many keys some of them are owned by a shard of the
# paused node which has not committed the change yet.
KEYS = list(range(20))

# How long to let the requests reach the paused node. They are expected to be waiting there when
# it elapses, so the test pays it in full on the happy path - keep it short.
REACH_TIMEOUT = 10

# Which racks ks replicates to. The nodes sit one per rack, so a rack list names the replicas
# directly: both nodes, or the paused one alone where a single replica is wanted. A plain
# replication_factor of 1 would leave the choice to the tablet allocator, which puts all of the
# tablets in one rack - about half the time the coordinator's, and then no request ever reaches
# the paused node and the test proves nothing.
BOTH_RACKS = ['rack1', 'rack2']
PAUSED_RACK = ['rack2']


@asynccontextmanager
async def paused_schema_change(manager, cql, node, statement):
    """Runs `statement` and yields once `node` has committed it on shard 0, with its applier
       paused before committing on the other shards. The read barrier first makes `node` apply
       the earlier schema changes, so that the pause catches `statement` and not one of those.
       Leaving the block releases the pause and waits for the statement, so a failing assertion
       in between does not leave the applier waiting out the injection's one-minute timeout."""
    await read_barrier(manager.api, node.ip_addr)
    async with inject_error(manager.api, node.ip_addr, PAUSE_INJECTION) as pause:
        change = cql.run_async(statement)
        try:
            await manager.api.wait_for_injection_enter(node.ip_addr, PAUSE_INJECTION,
                                                       deadline=time.time() + 60)
            yield
        finally:
            await pause.message()
            await change


async def prepare_on_coordinator(cql, statement):
    """Prepares `statement`, waiting for the coordinator to have the table. The two nodes apply
       the group0 command independently, so the coordinator may not have committed the CREATE yet
       when the other node is already paused mid-commit. Preparing needs nothing from the paused
       node, so retrying here waits for the coordinator alone - a failure originating on the
       paused node is what these tests are about, and is never retried away."""
    async def try_prepare():
        try:
            return cql.prepare(statement)
        except InvalidRequest as e:
            logger.info(f"coordinator not ready to prepare {statement!r} yet: {e}")
            return None
    return await wait_for(try_prepare, time.time() + 60)


async def setup(manager, racks=BOTH_RACKS):
    """Two nodes of two shards each, with the driver pinned to the first one so that it
       coordinates every request and the second one only ever sees them as a replica.
       `racks` are the racks ks replicates to, one replica in each."""
    servers = await manager.servers_add(2, cmdline=['--smp=2'],
                                        property_file=[{"dc": "dc1", "rack": "rack1"},
                                                       {"dc": "dc1", "rack": "rack2"}])
    coordinator, paused = servers
    await manager.driver_connect(server=coordinator)
    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async("CREATE KEYSPACE ks WITH replication = "
                        f"{{'class': 'NetworkTopologyStrategy', 'dc1': {racks}}} "
                        "AND tablets = {'initial': 8}")
    return cql, coordinator, paused


def settle(requests):
    """Splits `requests` into the ones which already failed and whether any is still running."""
    failed = [r.exception() for r in requests if r.done() and r.exception() is not None]
    return failed, any(not r.done() for r in requests)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_remote_write_during_shard_commit(manager):
    """A write coordinated by a node which has committed a CREATE TABLE reaches a shard of
       another node which has not. Without waiting for the commit it fails with
       no_such_column_family, in storage_proxy's write verb handler."""
    cql, _, paused = await setup(manager)

    async with paused_schema_change(manager, cql, paused,
                                    "CREATE TABLE ks.t (pk int PRIMARY KEY, v int)"):
        insert = await prepare_on_coordinator(cql, "INSERT INTO ks.t (pk, v) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        writes = [cql.run_async(insert, [pk, pk], execution_profile='whitelist') for pk in KEYS]
        await asyncio.wait(writes, timeout=REACH_TIMEOUT)
        failed, waited = settle(writes)

    await asyncio.gather(*writes, return_exceptions=True)
    assert not failed, f"write failed on the node still committing the schema change: {failed[0]}"
    assert waited, "no write reached the paused node while its commit was pending"
    for write in writes:
        write.result()

    rows = await cql.run_async("SELECT pk, v FROM ks.t")
    assert sorted((r.pk, r.v) for r in rows) == [(pk, pk) for pk in KEYS]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_remote_write_with_added_column_during_shard_commit(manager):
    """A write carrying a column added by ALTER TABLE reaches a shard of another node which
       has not committed the ALTER. Without waiting for the commit that shard applies the mutation
       against the old schema and drops the new column's value, and nothing reports it. The
       paused node is the only replica, so the value is read back from the shard which took the
       write."""
    cql, _, paused = await setup(manager, racks=PAUSED_RACK)

    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    async with paused_schema_change(manager, cql, paused, "ALTER TABLE ks.t ADD w int"):
        insert = await prepare_on_coordinator(cql, "INSERT INTO ks.t (pk, v, w) VALUES (?, ?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        writes = [cql.run_async(insert, [pk, pk, pk], execution_profile='whitelist') for pk in KEYS]
        await asyncio.wait(writes, timeout=REACH_TIMEOUT)
        failed, waited = settle(writes)

    await asyncio.gather(*writes, return_exceptions=True)
    assert not failed, f"write failed on the node still committing the schema change: {failed[0]}"
    assert waited, "no write reached the paused node while its commit was pending"
    for write in writes:
        write.result()

    rows = await cql.run_async("SELECT pk, w FROM ks.t")
    assert sorted((r.pk, r.w) for r in rows) == [(pk, pk) for pk in KEYS]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_remote_read_during_shard_commit(manager):
    """The same for a read: it is forwarded to a shard of a node which has not committed the
       CREATE TABLE yet, and must wait for the commit rather than fail in storage_proxy's read
       verb handler. The table is empty, so every read returns no rows."""
    cql, _, paused = await setup(manager)

    async with paused_schema_change(manager, cql, paused,
                                    "CREATE TABLE ks.t (pk int PRIMARY KEY, v int)"):
        select = await prepare_on_coordinator(cql, "SELECT pk, v FROM ks.t WHERE pk = ?")
        select.consistency_level = ConsistencyLevel.ALL
        reads = [cql.run_async(select, [pk], execution_profile='whitelist') for pk in KEYS]
        await asyncio.wait(reads, timeout=REACH_TIMEOUT)
        failed, waited = settle(reads)

    await asyncio.gather(*reads, return_exceptions=True)
    assert not failed, f"read failed on the node still committing the schema change: {failed[0]}"
    assert waited, "no read reached the paused node while its commit was pending"
    for read in reads:
        assert list(read.result()) == []
