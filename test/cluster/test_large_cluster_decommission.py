#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#


import asyncio
import time
import pytest
import logging

from test.cluster.util import get_coordinator_host, check_token_ring_and_group0_consistency, wait_for_token_ring_and_group0_consistency
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.random_tables import RandomTables

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_large_cluster_decommission(manager: ManagerClient):
    """
    Test decommissioning a node in a large cluster.
    This test verifies that:
    - A large cluster can be started successfully
    - One node can be decommissioned without issues
    - The decommission operation completes successfully
    - The cluster remains consistent after decommission
    """

    # Create a large cluster
    node_count = 100
    logger.info(
        f"Starting {node_count}-node cluster - this may take some time...")
    start_time = time.time()

    # Use reduced resource settings for large cluster test
    cmdline = [
        '--logger-log-level=gossip=info',  # Reduce log verbosity
        '--logger-log-level=raft_topology=info'
    ]

    servers = []
    for _ in range(node_count):
        servers.append(await manager.server_add(cmdline=cmdline))

    cluster_start_time = time.time() - start_time
    logger.info(f"{node_count}-node cluster started in {cluster_start_time:.2f} seconds")

    # Wait for cluster to stabilize
    logger.info("Waiting for cluster to stabilize...")
    # 5 minute timeout
    await wait_for_cql_and_get_hosts(manager.cql, servers, time.time() + 300)

    coordinator = await get_coordinator_host(manager=manager)
    coordinator_log = await manager.server_open_log(server_id=coordinator.server_id)

    # Create keyspace with replication factor 3 and 100 random tables (without tablets)
    logger.info("Creating keyspace with replication factor 3 and 100 random tables...")
    keyspace_name = "test_ks"
    random_tables = RandomTables("test_large_cluster_decommission", manager, keyspace_name,
                                 replication_factor=3, enable_tablets=False)
    
    # Create 100 random tables
    table_count = 100
    await random_tables.add_tables(ntables=table_count, ncolumns=5)
    
    logger.info(f"Successfully created keyspace '{keyspace_name}' and {table_count} tables")

    # Verify initial cluster consistency
    logger.info("Checking initial cluster consistency...")
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 1200)

    # Select a node to decommission (not the first node which is likely the coordinator)
    decom_node = servers[node_count // 2]  # Choose a node in the middle
    logger.info(f"Selected node {decom_node.server_id} for decommission")

    # Record the number of running servers before decommission
    initial_running_servers = await manager.running_servers()
    initial_count = len(initial_running_servers)
    logger.info(f"Initial running servers count: {initial_count}")

    # Start decommission operation
    logger.info(f"Starting decommission of node {decom_node.server_id}...")
    decom_start_time = time.time()

    coordinator_log_mark = await coordinator_log.mark()

    # The decommission_node function waits for completion automatically
    await manager.decommission_node(decom_node.server_id, timeout=3600)

    # wait for the node to finish the removal
    await coordinator_log.wait_for(
        "Finished to force remove node",
        from_mark=coordinator_log_mark,
    )

    decom_duration = time.time() - decom_start_time
    logger.info(f"Decommission completed in {decom_duration:.2f} seconds")

    # Verify the node is no longer running
    final_running_servers = await manager.running_servers()
    final_count = len(final_running_servers)
    logger.info(f"Final running servers count: {final_count}")

    # Assert that we have one less server running
    assert final_count == initial_count - \
        1, f"Expected {initial_count - 1} servers, but got {final_count}"

    # Verify the decommissioned node is not in the running list
    decommissioned_server_ids = [s.server_id for s in final_running_servers]
    assert decom_node.server_id not in decommissioned_server_ids, "Decommissioned node still appears to be running"

    # sleep for 5 minutes to stabilize (and to make sure there is no abort error)
    await asyncio.sleep(300)

    # Check final cluster consistency
    logger.info("Checking final cluster consistency...")
    await check_token_ring_and_group0_consistency(manager)

    # check that all nodes except the decommissioned one are still running
    final_running_servers = await manager.running_servers()
    for s in initial_running_servers:
        if s.server_id != decom_node.server_id:
            assert s.server_id in [ns.server_id for ns in final_running_servers], \
                f"Node {s.server_id} is not running after decommission"

    logger.info("Large cluster decommission test completed successfully")
