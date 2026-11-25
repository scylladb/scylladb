#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
A wide partition of a COMPACT STORAGE table with deleted rows: written by an
old Scylla and by Cassandra in the ka format and loaded at node start, and
written here and read back across a flush and a restart.

Ported from scylla-dtest compact_storage_tests.py, one test per dtest. Like
the dtests' node (ccm populate(1)), a single node runs with SimpleSnitch in
datacenter1/rack1, vnodes, and enable_create_table_with_compact_storage, and
is stopped gracefully at the end of each test, as the dtests' teardown did.
"""

import os
import shutil

import pytest

from test import DEBUG_MODES
from test.cluster.util import get_table_dir, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CONFIG = {'enable_create_table_with_compact_storage': True, 'endpoint_snitch': 'SimpleSnitch',
          'tablets_mode_for_new_keyspaces': 'disabled'}
SSTABLES_DIR = os.path.join(os.path.dirname(__file__), '..', 'resource', 'sstables')
ROW_SIZE = 1000


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the node gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def start_node(manager: ScyllaClusterManager):
    """ccm's populate(1).start() with the class's cluster_options."""
    return await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack1"})


async def create_ks_and_tb(cql) -> None:
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'};")
    await cql.run_async("CREATE TABLE ks.tb (key1 int, key2 int, val blob, PRIMARY KEY (key1,key2)) WITH COMPACT STORAGE;")


async def restart(manager: ScyllaClusterManager, server):
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    return manager.get_cql()


async def load_and_read_from_sstables(manager: ScyllaClusterManager, data_dir: str, lines: int) -> None:
    """TestCompactStorage.load_and_read_from_sstables(): with the node
    stopped, copy the sstables into the table's directory, start the node
    (which loads them) and count the rows."""
    server = await start_node(manager)
    await create_ks_and_tb(manager.get_cql())
    await manager.server_stop_gracefully(server.server_id)
    shutil.copytree(os.path.join(SSTABLES_DIR, data_dir), await get_table_dir(manager, server, "ks", "tb"), dirs_exist_ok=True)
    cql = await restart(manager, server)
    result = list(await cql.run_async("SELECT key2 FROM ks.tb"))
    assert len(result) == lines


# Ported from scylla-dtest compact_storage_tests.py::TestCompactStorage::test_read_old_format_wide_row_data.
# The ka sstable of an old Scylla, a 1000-row partition with 200 rows deleted.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_read_old_format_wide_row_data(manager: ScyllaClusterManager):
    await load_and_read_from_sstables(manager, "compact_wide_partition_scylla_ka", ROW_SIZE - 200)


# Ported from scylla-dtest compact_storage_tests.py::TestCompactStorage::test_read_cassandra_wide_row_data.
# Cassandra's ka sstables, a 1000-row partition with 100 rows deleted.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_read_cassandra_wide_row_data(manager: ScyllaClusterManager):
    await load_and_read_from_sstables(manager, "compact_wide_partition_cassandra", ROW_SIZE - 100)


# Ported from scylla-dtest compact_storage_tests.py::TestCompactStorage::test_wide_row.
# 1000 rows of 10 KB in one partition, flushed, then 100 of them deleted:
# 900 rows are read with the deletions in the memtable, and again after a
# flush and a restart.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_wide_row(manager: ScyllaClusterManager):
    server = await start_node(manager)
    cql = manager.get_cql()
    await create_ks_and_tb(cql)
    blob = "a" * 10000
    insert = cql.prepare("INSERT INTO ks.tb (key1,key2,val) values (1,?,textAsBlob(?));")
    for key2 in range(ROW_SIZE):
        await cql.run_async(insert, (key2, blob))

    await run_nodetool(manager, server, "flush")
    for key2 in range(100):
        await cql.run_async("delete from ks.tb where key1=1 and key2 = %d" % (key2 * 10))
    result = list(await cql.run_async("SELECT * FROM ks.tb"))
    assert len(result) == ROW_SIZE - 100

    await run_nodetool(manager, server, "flush")
    await manager.server_stop_gracefully(server.server_id)
    cql = await restart(manager, server)
    result = list(await cql.run_async("SELECT * FROM ks.tb"))
    assert len(result) == ROW_SIZE - 100
