#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import check_token_ring_and_group0_consistency, reconnect_driver, \
        check_node_log_for_failed_mutations, start_writes, wait_for_token_ring_and_group0_consistency

from cassandra.cluster import ConsistencyLevel

import time
import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.parametrize("tablets_enabled", [True, False])
async def test_topology_ops(request, manager: ManagerClient, tablets_enabled: bool):
    """Test basic topology operations using the topology coordinator."""
    rf_rack_cfg = {'rf_rack_valid_keyspaces': False}
    cfg = {'tablets_mode_for_new_keyspaces': 'enabled' if tablets_enabled else 'disabled'} | rf_rack_cfg
    rf = 3
    num_nodes = rf
    if tablets_enabled:
        num_nodes += 1

    logger.info("Bootstrapping first node")
    servers = [await manager.server_add(config=cfg)]

    logger.info(f"Restarting node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    logger.info("Bootstrapping other nodes")
    servers += await manager.servers_add(num_nodes, config=cfg)

    await wait_for_cql_and_get_hosts(manager.cql, servers, time.time() + 60)
    cql = await reconnect_driver(manager)
    finish_writes = await start_writes(cql, rf, ConsistencyLevel.ONE)

    logger.info(f"Decommissioning node {servers[0]}")
    await manager.decommission_node(servers[0].server_id)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    servers = servers[1:]

    logger.info(f"Restarting node {servers[0]} when other nodes have bootstrapped")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await check_node_log_for_failed_mutations(manager, servers[0])

    logger.info(f"Replacing node {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    servers = servers[1:] + [await manager.server_add(replace_cfg, config=rf_rack_cfg)]
    await check_token_ring_and_group0_consistency(manager)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await check_node_log_for_failed_mutations(manager, servers[0])

    logger.info(f"Removing node {servers[0]} using {servers[1]}")
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
    servers = servers[1:]

    logger.info("Checking results of the background writes")
    await finish_writes()

    for server in servers:
        await check_node_log_for_failed_mutations(manager, server)
