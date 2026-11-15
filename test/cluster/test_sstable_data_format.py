#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests of the `sstable_data_format` cluster config option, which makes a
table write sstables whose Data file is a Parquet file (format `pq`, version
`aa`). The option takes effect on the next sstable, sstables of both formats
move between nodes by repair and by file-based tablet streaming, and
`upgradesstables` converts a table back.
"""

import asyncio
import glob
import logging
import os

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import TABLE_CONFIGS_QUERY, wait_for_config_map_value_on_hosts
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas

logger = logging.getLogger(__name__)

KS = "ks"
CF = "t"

# The node option that sets the default format is pinned, so that the test
# also passes when the suite runs with Parquet as the default.
CONFIG = {
    "test_sstable_data_format_default": "native",
    # Hints would deliver the writes that the test keeps from a stopped node.
    "hinted_handoff_enabled": False,
}
# The cluster config manager logs each refresh of its caches at debug level.
CMDLINE = ["--logger-log-level", "cluster_config_manager=debug"]


async def data_file_formats(manager: ScyllaClusterManager, server: ServerInfo) -> set[str]:
    """The formats of the Data files of the table on a node: "big", "pq", or both."""
    workdir = await manager.server_get_workdir(server.server_id)
    pattern = os.path.join(workdir, "data", KS, f"{CF}-????????????????????????????????", "*-Data.db")
    # A Data file is named `<version>-<generation>-<format>-Data.db`.
    return {os.path.basename(f).rsplit("-", 2)[1] for f in glob.glob(pattern)}


async def set_table_format(manager: ScyllaClusterManager, cql, servers: list[ServerInfo], hosts, value: str) -> None:
    """Sets the option for the table, and waits until every node has refreshed its cluster config."""
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    await cql.run_async(f"ALTER TABLE {KS}.{CF} WITH sstable_data_format = {value}")
    expected = None if value == "NULL" else value.strip("'")
    await wait_for_config_map_value_on_hosts(cql, hosts, TABLE_CONFIGS_QUERY, [KS, CF], "sstable_data_format", expected)
    for log, mark in zip(logs, marks):
        await log.wait_for("Refreshed cluster config cache", from_mark=mark, timeout=60)


async def insert(cql, keys: range, cl: ConsistencyLevel, host=None) -> None:
    stmt = SimpleStatement(f"INSERT INTO {KS}.{CF} (pk, v) VALUES (%s, %s)", consistency_level=cl)
    await asyncio.gather(*[cql.run_async(stmt, [k, k * 10], host=host) for k in keys])


async def check_rows(cql, count: int) -> None:
    rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {KS}.{CF}", consistency_level=ConsistencyLevel.ALL))
    assert sorted((r.pk, r.v) for r in rows) == [(k, k * 10) for k in range(count)]


async def flush(manager: ScyllaClusterManager, servers: list[ServerInfo]) -> None:
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, KS, CF) for s in servers])


async def test_sstable_data_format(manager: ScyllaClusterManager) -> None:
    servers = [
        await manager.server_add(config=CONFIG, cmdline=CMDLINE, property_file={"dc": "dc1", "rack": "r1"}),
        await manager.server_add(config=CONFIG, cmdline=CMDLINE, property_file={"dc": "dc1", "rack": "r2"}),
    ]
    await manager.disable_tablet_balancing()
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} "
                        "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE {KS}.{CF} (pk int PRIMARY KEY, v int)")

    # Before the option is set, the table writes the native format.
    await insert(cql, range(0, 50), ConsistencyLevel.ALL)
    await flush(manager, servers)
    for s in servers:
        assert await data_file_formats(manager, s) == {"big"}

    # The option takes effect on the next sstable, and the old ones stay.
    await set_table_format(manager, cql, servers, hosts, "'parquet'")
    await insert(cql, range(50, 100), ConsistencyLevel.ALL)
    await flush(manager, servers)
    for s in servers:
        assert await data_file_formats(manager, s) == {"big", "pq"}
    await check_rows(cql, 100)

    # Repair moves rows between nodes that hold them in different formats.
    # The second node misses rows that the first one has in Parquet sstables,
    # and writes what it receives in the format of the table's choice.
    await manager.server_stop_gracefully(servers[1].server_id)
    await insert(cql, range(100, 150), ConsistencyLevel.ONE, host=hosts[0])
    await flush(manager, servers[:1])
    await manager.server_start(servers[1].server_id)
    cql, hosts = await manager.get_ready_cql(servers)
    before_repair = await data_file_formats(manager, servers[1])
    await manager.api.tablet_repair(servers[0].ip_addr, KS, CF, "all", await_completion=True)
    await flush(manager, servers)
    assert before_repair == {"big", "pq"}
    assert await data_file_formats(manager, servers[1]) == {"big", "pq"}
    await check_rows(cql, 150)

    # File-based tablet streaming sends the sstables as they are, in both
    # formats, to a new node, which loads them, also after a restart.
    servers.append(await manager.server_add(config=CONFIG, cmdline=CMDLINE, property_file={"dc": "dc1", "rack": "r1"}))
    cql, hosts = await manager.get_ready_cql(servers)
    src_host = await manager.get_host_id(servers[0].server_id)
    dst_host = await manager.get_host_id(servers[2].server_id)
    replicas = await get_tablet_replicas(manager, servers[0], KS, CF, 0)
    src_shard = next(shard for host, shard in replicas if host == src_host)
    await manager.api.move_tablet(servers[0].ip_addr, KS, CF, src_host, src_shard, dst_host, 0, 0)
    assert "pq" in await data_file_formats(manager, servers[2])
    await check_rows(cql, 150)
    await manager.server_restart(servers[2].server_id)
    cql, hosts = await manager.get_ready_cql(servers)
    assert "pq" in await data_file_formats(manager, servers[2])
    await check_rows(cql, 150)

    # Once the option is reset, upgradesstables converts the table back. The
    # request is what `nodetool upgradesstables` sends without
    # `--include-all-sstables`, which rewrites only the sstables that need it.
    await set_table_format(manager, cql, servers, hosts, "NULL")
    await asyncio.gather(*[manager.api.client.get(f"/storage_service/keyspace_upgrade_sstables/{KS}", host=s.ip_addr,
                                                  params={"exclude_current_version": "true"}) for s in servers])
    for s in servers:
        assert await data_file_formats(manager, s) <= {"big"}
    assert await data_file_formats(manager, servers[2]) == {"big"}
    await check_rows(cql, 150)


async def test_commitlog_replay_writes_the_table_format(manager: ScyllaClusterManager) -> None:
    """At startup, the flush after the commitlog replay writes the format of the
    table's choice. The node reads the cluster config before it loads the
    sstables of the user tables and replays the commitlog. Until that read,
    every table would choose the native format."""
    server = await manager.server_add(config=CONFIG | {"commitlog_sync": "batch"}, cmdline=CMDLINE)
    cql, hosts = await manager.get_ready_cql([server])
    await cql.run_async(f"CREATE KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE {KS}.{CF} (pk int PRIMARY KEY, v int)")
    await set_table_format(manager, cql, [server], hosts, "'parquet'")

    # The rows are only in the memtable and in the commitlog when the node is killed.
    await insert(cql, range(0, 50), ConsistencyLevel.ONE)
    assert await data_file_formats(manager, server) == set()
    await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    cql, hosts = await manager.get_ready_cql([server])
    assert await data_file_formats(manager, server) == {"pq"}
    await check_rows(cql, 50)
