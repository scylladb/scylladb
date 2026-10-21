#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Basic operations on nodes of their own: schema and data surviving a
restart, inserts and selects by composite and compound keys, consistency
levels with nodes down, and data written by cassandra-stress read back after
a restart.

Ported from scylla-dtest. Like the dtests' nodes, the nodes use vnodes (256
per node), and are stopped gracefully at the end of each test, as the
dtests' teardown did.
"""

import logging

import pytest

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def ks_opts(rf: int) -> str:
    return f"WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}"


async def check_k1_and_not_k2(cql) -> None:
    res = list(await cql.run_async("SELECT * FROM ks.test1 WHERE k=1"))
    assert len(res) == 1, f"expected length=1 got {res}"
    res = list(await cql.run_async("SELECT * FROM ks.test1 WHERE k=2"))
    assert len(res) == 0, f"expected length=0 got {res}"


############################################################################
# Ported from simple_boot_shutdown_test.py.

async def boot_with_table(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test1 (k int PRIMARY KEY, c int)")
    return server, cql


async def restart(manager: ScyllaClusterManager, server, gracefully: bool = True) -> None:
    if gracefully:
        await manager.server_stop_gracefully(server.server_id)
    else:
        await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)


# A table created (and flushed) before a restart takes a row after it.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_boot_create_keyspace_table_shutdown_boot_insert_select(manager: ScyllaClusterManager):
    server, cql = await boot_with_table(manager)
    await run_nodetool(manager, server, "flush")
    await restart(manager, server)
    cql = manager.get_cql()
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await check_k1_and_not_k2(cql)


# A row written and flushed before a graceful restart is read after it.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_boot_create_keyspace_table_insert_shutdown_select(manager: ScyllaClusterManager):
    server, cql = await boot_with_table(manager)
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await run_nodetool(manager, server, "flush")
    await restart(manager, server)
    await check_k1_and_not_k2(manager.get_cql())


# The same with the node killed after the flush (the dtest's "commitlog
# replay" variant).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_boot_create_keyspace_table_insert_shutdown_commitlog_replay_select(manager: ScyllaClusterManager):
    server, cql = await boot_with_table(manager)
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await run_nodetool(manager, server, "flush")
    await restart(manager, server, gracefully=False)
    await check_k1_and_not_k2(manager.get_cql())
