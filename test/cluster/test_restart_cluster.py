#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test clusters can restart fine after all nodes are stopped gracefully
"""

import logging
import os
import time

import pytest
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace, reconnect_driver, run_nodetool
from test.cqlpy.util import run_cqlsh_at
import asyncio

logger = logging.getLogger(__name__)


async def test_restart_cluster(manager: ScyllaClusterManager) -> None:
    """Test that cluster can restart fine after all nodes are stopped gracefully"""
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    logger.info(f"Servers {servers}, gracefully stopping servers {[s.server_id for s in servers]} to check if all will go up")
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)

    logger.info(f"Starting servers {[s.server_id for s in servers]}")
    for s in servers:
        await manager.server_start(s.server_id)

    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_uptime_resets_after_restart(manager: ScyllaClusterManager) -> None:
    """Test that /system/uptime_ms counts from the last start of the node, not the first one"""
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})

    async def get_uptime_ms() -> int:
        return int(await manager.api.client.get_text("/system/uptime_ms", host=server.ip_addr))

    uptime_before_restart = await get_uptime_ms()
    restart_time = time.monotonic()
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    uptime = await get_uptime_ms()
    # Without a reset the uptime would exceed the time since the restart by the node's boot time.
    assert uptime <= (time.monotonic() - restart_time) * 1000, \
        f"uptime {uptime} ms (was {uptime_before_restart} ms before restart) is longer than the time since the restart"


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_bug_5732.
# A secondary index still answers queries after the node restarts
# (#5732). As in the dtest, the table caches all keys and rows, the index
# build is awaited in system."IndexInfo", and the whole (one-node) cluster
# is stopped gracefully and started again.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_secondary_index_after_restart(manager: ScyllaClusterManager) -> None:
    # The dtest's prepare(use_cache=True) sets row_cache_size_in_mb.
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled", "row_cache_size_in_mb": 100},
                                      property_file={"dc": "dc1", "rack": "rack1"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (k int PRIMARY KEY, v int, )")
        await cql.run_async(f"ALTER TABLE {ks}.test WITH CACHING={{'keys':'ALL','rows_per_partition':'ALL'}}")
        await cql.run_async(f"INSERT INTO {ks}.test (k,v) VALUES (0,0)")
        await cql.run_async(f"INSERT INTO {ks}.test (k,v) VALUES (1,1)")
        await cql.run_async(f"CREATE INDEX testindex on {ks}.test(v)")

        # Wait for the index to be fully built.
        start = time.time()
        while True:
            results = await cql.run_async(f"""SELECT index_name FROM system."IndexInfo" WHERE table_name = '{ks}'""")
            if results:
                assert results[0][0] == "testindex", results
                break
            if time.time() - start > 10.0:
                results = await cql.run_async('SELECT * FROM system."IndexInfo"')
                raise Exception(f"Failed to build secondary index within ten seconds: {results}")
            await asyncio.sleep(0.1)

        assert [r.k for r in await cql.run_async(f"SELECT k FROM {ks}.test WHERE v = 0")] == [0]

        await manager.server_stop_gracefully(server.server_id)
        await asyncio.sleep(0.5)
        await manager.server_start(server.server_id)
        await asyncio.sleep(0.5)

        cql = await reconnect_driver(manager)
        assert [r.k for r in await cql.run_async(f"SELECT k FROM {ks}.test WHERE v = 0")] == [0]


async def describe_tables(server, keyspace: str) -> list[str]:
    """The table names "DESCRIBE TABLES" prints in cqlsh, as the dtest
    reads them."""
    res = await asyncio.to_thread(run_cqlsh_at, server.ip_addr, 9042, f"USE {keyspace}; DESCRIBE TABLES")
    logger.debug("cqlsh DESCRIBE TABLES: %s %s", res.stdout, res.stderr)
    return res.stdout.split()


# Ported from scylla-dtest cql_additional_tests.py::TestsCQLAdditional::test_create_100tables.
# On three nodes, the dtest's schema file of 112 tables and 4 types is
# loaded with cqlsh's SOURCE; then node1 and node2 in turn are drained,
# killed and started again (#2923, where loading the schema at boot timed
# out), and cqlsh still lists all 112 tables.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_restart_with_many_tables(manager: ScyllaClusterManager) -> None:
    # The dtest's populate(3): SimpleSnitch, every node in datacenter1/rack1.
    # Its options: one of its TWCS tables has 90 windows.
    servers = await manager.servers_add(3, cmdline=["--twcs-max-window-count", "100", "--request-timeout-in-ms", "60000",
                                                    "--write-request-timeout-in-ms", "60000"],
                                        config={"tablets_mode_for_new_keyspaces": "disabled", "endpoint_snitch": "SimpleSnitch"},
                                        property_file=[{"dc": "datacenter1", "rack": "rack1"}] * 3)
    schema_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "resource", "cql", "create_100tables.cql")
    assert os.path.exists(schema_file), "schema file doesn't exist"

    res = await asyncio.to_thread(run_cqlsh_at, servers[0].ip_addr, 9042, f"SOURCE '{schema_file}'",
                                  ["--request-timeout=300", "--connect-timeout=60"])
    logger.debug("cqlsh SOURCE: %s %s", res.stdout, res.stderr)

    tables = await describe_tables(servers[0], "veraminetest")
    assert len(tables) == 112, tables

    for server in servers[:2]:
        await run_nodetool(manager, server, "drain")
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id, wait_others=2)

    await reconnect_driver(manager)
    tables = await describe_tables(servers[0], "veraminetest")
    assert len(tables) == 112, "created 100+ tables"
