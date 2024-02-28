#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import time

import pytest
from cassandra.cluster import Cluster, Session
from cassandra.policies import WhiteListRoundRobinPolicy
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo, IPAddress
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import cluster_con

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_no_removed_node_event_on_ip_change(manager: ManagerClient, caplog: pytest.LogCaptureFixture):
    logger.info("starting the first node (leader)")
    servers = [await manager.server_add()]

    logger.info("starting the second node (follower)")
    servers += [await manager.server_add()]

    logger.info("close the default manager driver")
    manager.driver_close()

    logger.info(f"stopping the follower node {servers[1]}")
    await manager.server_stop_gracefully(servers[1].server_id)

    s1_old_ip: IPAddress = servers[1].ip_addr
    s1_new_ip: IPAddress = await manager.server_change_ip(servers[1].server_id)
    logger.info(f"changed {servers[1]} ip to {s1_new_ip}")

    logger.info("creating the driver")
    # We use load_balancing_policy to ensure that the control connection
    # is goes to the first node, so that we get the TOPOLOGY_CHANGE notifications
    # about the second.
    test_cluster: Cluster
    with cluster_con([servers[0].ip_addr, s1_old_ip, s1_new_ip], manager.port, manager.use_ssl,
                     load_balancing_policy=WhiteListRoundRobinPolicy([servers[0].ip_addr])) as test_cluster:
        logger.info("connecting driver")
        test_cql: Session
        with test_cluster.connect() as test_cql:
            logger.info(f"starting the follower node {servers[1]}")
            await manager.server_start(servers[1].server_id)
            servers[1] = ServerInfo(servers[1].server_id, s1_new_ip, s1_new_ip)

            logger.info("waiting for cql and hosts")
            await wait_for_cql_and_get_hosts(test_cql, servers, time.time() + 30)

            log_output: str = caplog.text

    assert f"'change_type': 'NEW_NODE', 'address': ('{s1_new_ip}'" in log_output
    assert f"'change_type': 'REMOVED_NODE', 'address': ('{s1_old_ip}'" not in log_output
