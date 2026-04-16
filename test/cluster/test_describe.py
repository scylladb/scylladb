#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import pytest
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy
from test.cluster.conftest import cluster_con
from time import time
import os

# The following test verifies that Scylla avoids making an oversized allocation
# when generating a large create statement when performing a DESCRIBE statement.
# The threshold for generating a warning about an oversized allocation is set
# to 128 * 2^10 bytes.
#
# Reproducer for issue scylladb/scylladb#24018.
@pytest.mark.asyncio
async def test_large_create_statement(manager: ManagerClient):
    cmdline = ["--logger-log-level", "describe=trace"]
    srv = await manager.server_add(cmdline=cmdline)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY") as table:
            # CQL will not accept identifiers longers than ~2^16.
            col_name_len = 60_000
            # An oversized allocation warning is issued for allocations bigger than 128 * 2^10.
            target_size_threshold = 128 * (2 ** 10)

            async def add_and_drop(col_name: str) -> None:
                await cql.run_async(f"ALTER TABLE {table} ADD {col_name} int")
                await cql.run_async(f"ALTER TABLE {table} DROP {col_name}")

            # Let's get ourselves a little bit more room with the size just
            # to make sure an oversized allocation will be triggered.
            col_count = 2 * (target_size_threshold // col_name_len) + 1
            col_name_prefix = "a" * col_name_len

            await asyncio.gather(*[add_and_drop(f"{col_name_prefix}{idx}") for idx in range(col_count)])

            log = await manager.server_open_log(srv.server_id)
            marker = await log.mark()

            await cql.run_async("DESCRIBE SCHEMA WITH INTERNALS")

            matches = await log.grep("oversized allocation", from_mark=marker)
            assert len(matches) == 0

@pytest.mark.parametrize("mode", ["normal", "maintenance"])
@pytest.mark.asyncio
async def test_describe_cluster_sanity(manager: ManagerClient, mode: str):
    """
    Parametrized test that DESCRIBE CLUSTER returns correct cluster information
    in both normal and maintenance modes.

    This test verifies that cluster metadata from gossiper is properly initialized
    and the cluster name is consistent with system.local in both:
    - normal mode: standard cluster operation
    - maintenance mode: node isolated from the cluster
    """

    if mode == "normal":
        await manager.server_add()
        cql = manager.get_cql()
    else:  # maintenance mode
        srv = await manager.server_add(config={"maintenance_mode": True}, connect_driver=False)
        maintenance_socket_path = await manager.server_get_maintenance_socket_path(srv.server_id)
        async def socket_exists():
            return True if os.path.exists(maintenance_socket_path) else None
        await wait_for(socket_exists, time() + 30)
        socket_endpoint = UnixSocketEndPoint(maintenance_socket_path)
        cluster = cluster_con([socket_endpoint], load_balancing_policy=WhiteListRoundRobinPolicy([socket_endpoint]))
        cql = cluster.connect()

    try:
        system_local_results = await cql.run_async("SELECT cluster_name FROM system.local")
        assert system_local_results[0].cluster_name != ""  # sanity check

        describe_results = await cql.run_async("DESCRIBE CLUSTER")
        assert describe_results[0].partitioner == 'org.apache.cassandra.dht.Murmur3Partitioner'
        assert describe_results[0].snitch == 'org.apache.cassandra.locator.SimpleSnitch'
        assert describe_results[0].cluster == system_local_results[0].cluster_name
    finally:
        if mode == "maintenance":
            cluster.shutdown()
