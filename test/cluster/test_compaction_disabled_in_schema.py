#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Autocompaction disabled with the "enabled" compaction option of a table's
schema stays disabled after the node restarts.
"""

import asyncio
import datetime
import time

import pytest

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

FLUSHES = 5
ROWS_PER_FLUSH = 200
EPOCH = datetime.datetime(1970, 1, 1)


async def write_and_flush(manager, server, cql, table, start):
    """Write rows into the table in FLUSHES flushes of all tables, as the
    dtest did, so it gets FLUSHES sstables of the same size, which every
    compaction strategy compacts."""
    insert = cql.prepare(f"INSERT INTO {table} (key, val) VALUES (?, 'value')")
    for i in range(FLUSHES):
        await asyncio.gather(*(cql.run_async(insert, [start + i * ROWS_PER_FLUSH + k]) for k in range(ROWS_PER_FLUSH)))
        await manager.api.flush_all_keyspaces(server.ip_addr)


async def last_compaction(cql, table, since=EPOCH):
    """When the table was last compacted after since, from
    system.compaction_history, or None."""
    ks, cf = table.split('.')
    query = cql.prepare("SELECT MAX(compacted_at) FROM system.compaction_history WHERE keyspace_name = ? "
                        "AND columnfamily_name = ? AND compacted_at > ? ALLOW FILTERING")
    [row] = await cql.run_async(query, [ks, cf, since])
    return row[0]


async def wait_compacted(cql, table, since=EPOCH, timeout=120):
    """Wait until the table is compacted after since, and return when."""
    deadline = time.time() + timeout
    while (compacted_at := await last_compaction(cql, table, since)) is None:
        assert time.time() < deadline, f"{table} was not compacted"
        await asyncio.sleep(0.5)
    return compacted_at


# Test that a table created with the "enabled": "false" compaction option
# isn't compacted, while another table with the same sstables is, also after
# the node restarts and loads the table's schema again; and that it is
# compacted once ALTER TABLE enables autocompaction.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_disable_autocompaction_schema,
# which ran for each of the four compaction strategies, on one node with
# vnodes, and was translated, without the restart, as test/cqlpy's
# test_compaction.py::test_disable_autocompaction_with_option.
@pytest.mark.parametrize('strategy', ['SizeTieredCompactionStrategy', 'LeveledCompactionStrategy',
                                      'TimeWindowCompactionStrategy', 'IncrementalCompactionStrategy'])
async def test_compaction_disabled_in_schema_survives_restart(manager: ScyllaClusterManager, strategy):
    server = await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'disabled'})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        disabled, enabled = f'{ks}.disabled', f'{ks}.enabled'
        await cql.run_async(f"CREATE TABLE {disabled} (key int PRIMARY KEY, val text) "
                            f"WITH compaction = {{'class': '{strategy}', 'enabled': 'false'}}")
        await cql.run_async(f"CREATE TABLE {enabled} (key int PRIMARY KEY, val text) "
                            f"WITH compaction = {{'class': '{strategy}'}}")
        for restart in [False, True]:
            if restart:
                await manager.server_restart(server.server_id)
                cql = await reconnect_driver(manager)
                await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
            since = await last_compaction(cql, enabled) or EPOCH
            for table in [disabled, enabled]:
                await write_and_flush(manager, server, cql, table, start=10000 * restart)
            await wait_compacted(cql, enabled, since)
            assert await last_compaction(cql, disabled) is None
        await cql.run_async(f"ALTER TABLE {disabled} WITH compaction = {{'class': '{strategy}', 'enabled': 'true'}}")
        await write_and_flush(manager, server, cql, disabled, start=20000)
        await wait_compacted(cql, disabled)
