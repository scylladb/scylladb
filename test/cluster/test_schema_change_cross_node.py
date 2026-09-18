#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
A schema change is committed shard by shard on every node (see schema_applier::commit()), so a
request built on a node which has already committed it may reach a node - and a shard of that
node - which hasn't. It has to wait for the commit there instead of failing with
no_such_column_family.

test/boost/schema_change_cross_shard_test.cc covers the single-node case, where the request
reaches the other shard through database::apply() and database::query(). Here it crosses nodes
as well, so it arrives through the write and read RPC verbs, which resolve the table on the
receiving shard before the request gets to those paths.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

import pytest
from cassandra.cluster import ConsistencyLevel              # type: ignore # pylint: disable=no-name-in-module

from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

PAUSE_INJECTION = "schema_applier_pause_before_commit_on_other_shards"

# Both nodes run two shards, so with this many keys some of them are owned by a shard of the
# paused node which hasn't committed the change yet.
KEYS = list(range(20))

# How long to let the requests reach the paused node. They are expected to be waiting there when
# it elapses, so the test pays it in full on the happy path - keep it short.
REACH_TIMEOUT = 10


@asynccontextmanager
async def paused_schema_change(manager, cql, node, statement):
    """Runs `statement` and yields once `node` has committed it on shard 0, with its applier
       paused before committing on the other shards. Leaving the block releases the pause and
       waits for the statement, so a failing assertion in between doesn't leave the applier
       waiting out the injection's one-minute timeout."""
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
        except Exception as e:
            logger.info(f"coordinator not ready to prepare {statement!r} yet: {e}")
            return None
    return await wait_for(try_prepare, time.time() + 60)


async def setup(manager):
    """Two nodes of two shards each, with the driver pinned to the first one so that it
       coordinates every request and the second one only ever sees them as a replica."""
    servers = await manager.servers_add(2, cmdline=['--smp=2'],
                                        property_file=[{"dc": "dc1", "rack": "rack1"},
                                                       {"dc": "dc1", "rack": "rack2"}])
    coordinator, paused = servers
    await manager.driver_connect(server=coordinator)
    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async("CREATE KEYSPACE ks WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                        "AND tablets = {'initial': 8}")
    return cql, coordinator, paused


def settle(requests):
    """Splits `requests` into the ones which already failed and whether any is still running."""
    failed = [r.exception() for r in requests if r.done() and r.exception() is not None]
    return failed, any(not r.done() for r in requests)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_remote_write_during_shard_commit(manager):
    """A write coordinated by a node which has committed a CREATE TABLE reaches a shard of
       another node which hasn't. Without waiting for the commit it fails with
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
async def test_remote_read_during_shard_commit(manager):
    """The same for a read: it is forwarded to a shard of a node which hasn't committed the
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
