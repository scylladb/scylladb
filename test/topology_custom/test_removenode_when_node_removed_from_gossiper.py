#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import check_token_ring_and_group0_consistency, reconnect_driver, \
        check_node_log_for_failed_mutations, start_writes, wait_for_token_ring_and_group0_consistency

from cassandra.cluster import ConsistencyLevel

import time
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
#@pytest.mark.parametrize("tablets_enabled", [True, False])
async def test_removenode_when_node_removed_from_gossiper(request, manager: ManagerClient):
    """Test basic topology operations using the topology coordinator."""
    #rf_rack_cfg = {'rf_rack_valid_keyspaces': False, 'enable_repair_based_node_ops': False}
    rf_rack_cfg = {'rf_rack_valid_keyspaces': False}
    cmdline = [
        '--logger-log-level', 'gossip=trace',
        #'--logger-log-level', 'raft_topology=trace',
    ]

    cfg = rf_rack_cfg
    rf = 3
    num_nodes = rf
    num_nodes += 1

    logger.info("Bootstrapping first node")
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]

    logger.info("Bootstrapping other nodes")
    servers += await manager.servers_add(num_nodes, config=cfg)

    await wait_for_cql_and_get_hosts(manager.cql, servers, time.time() + 60)
    cql = await reconnect_driver(manager)

    await manager.api.enable_injection(servers[1].ip_addr, "gossiper_immediately_remove_dead_node", one_shot=False)
    await manager.api.enable_injection(servers[2].ip_addr, "gossiper_immediately_remove_dead_node", one_shot=False)
    await manager.api.enable_injection(servers[3].ip_addr, "gossiper_immediately_remove_dead_node", one_shot=False)
    await manager.api.enable_injection(servers[4].ip_addr, "gossiper_immediately_remove_dead_node", one_shot=False)

    logger.info(f"Stopping node {servers[0]}")
    coordinator_log = await manager.server_open_log(servers[1].server_id)

    await manager.server_stop(servers[0].server_id)

    logger.info("Waiting for the stopped node to be evicted from gossiper")
    await coordinator_log.wait_for(f"InetAddress .+/{servers[0].ip_addr} is removed from gossiper")


    logger.info(f"Removing node {servers[0]} using {servers[1]}")
    await manager.remove_node(initiator_id=servers[1].server_id, server_id=servers[0].server_id, ignore_dead=[servers[0].ip_addr])
    await check_token_ring_and_group0_consistency(manager)
    servers = servers[1:]
