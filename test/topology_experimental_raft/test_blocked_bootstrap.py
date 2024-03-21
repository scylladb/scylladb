# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode

import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.mark.skip(reason = "can't make it work with the new join procedure, without error recovery")
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_blocked_bootstrap(manager: ManagerClient):
    """
    Scenario:
    1. Start a cluster with nodes node1, node2, node3
    2. Start node4 replacing node node2
    3. Stop node node4 after it joined group0 but before it advertised itself in gossip
    4. Start node5 replacing node node2

    Test simulates the behavior described in #13543.

    Test passes only if `wait_for_peers_to_enter_synchronize_state` doesn't need to
    resolve all IPs to return early. If not, node5 will hang trying to resolve the
    IP of node4:
    ```
    raft_group0_upgrade - : failed to resolve IP addresses of some of the cluster members ([node4's host ID])
    ```
    """
    servers = await manager.servers_add(3)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)

    logger.info(f"Replacing node {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)

    try:
        await manager.server_add(replace_cfg, config={
            'error_injections_at_startup': ['stop_after_joining_group0']
        })
    except:
        # Node stops before it advertised itself in gossip, so manager.server_add throws an exception
        pass
    else:
        assert False, "Node should stop before it advertised itself in gossip"

    logger.info(f"Replacing node {servers[0]}")
    await manager.server_add(replace_cfg)
