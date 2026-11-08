#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import psutil
from typing import Callable

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.storage.conftest import space_limited_servers
from test.cluster.storage.test_out_of_space_prevention import DISK_FILL_TARGET_RATIO, global_cmdline, random_content_file

logger = logging.getLogger(__name__)


async def populate_vnodes_table(manager: ScyllaClusterManager, cql, server) -> tuple[str, str]:
    """Create a vnodes table, write data into it and flush it. Return the
    names of its keyspace and table."""
    # Create the keyspace and table without the helper context managers:
    # they drop the table on exit, but by then the node is down.
    ks = "test_ks"
    table = "test"
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}")
    await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, v int)")
    for pk in range(100):
        await cql.run_async(f"INSERT INTO {ks}.{table} (pk, v) VALUES ({pk}, {pk})")
    await manager.api.flush_keyspace(server.ip_addr, ks)
    return ks, table


async def test_boot_fails_on_critical_disk_before_resharding(manager: ScyllaClusterManager, volumes_factory: Callable) -> None:
    """
    Test that a node fails to start, with an error, if it must reshard while
    the disk utilization is above the critical level.

    Out-of-space prevention disables compaction before sstable loading
    starts, so resharding cannot run. Startup must then fail ("Startup
    failed", non-zero exit) instead of the node booting without part of its
    data.

    Scenario:
      - Start a node with 2 shards and write data into a vnodes table.
      - Stop the node and create a big file to push the disk over the
        critical utilization level.
      - Start the node with 3 shards and expect startup to fail with an
        error.
    """
    async with space_limited_servers(manager, volumes_factory, ["20M"], cmdline=[*global_cmdline, "--smp", "2"]) as servers:
        server = servers[0]
        cql, _ = await manager.get_ready_cql(servers)
        workdir = await manager.server_get_workdir(server.server_id)
        await populate_vnodes_table(manager, cql, server)

        await manager.server_stop_gracefully(server.server_id)
        await manager.server_update_cmdline(server.server_id, ["--smp", "3"])

        logger.info("Create a big file on the node to reach critical disk utilization level")
        disk_info = psutil.disk_usage(workdir)
        with random_content_file(workdir, int(disk_info.total*DISK_FILL_TARGET_RATIO) - disk_info.used):
            logger.info("Start the node with 3 shards and expect the boot to fail")
            await manager.server_start(server.server_id,
                                       expected_error="Startup failed: .*populating keyspace.*critical disk utilization")
