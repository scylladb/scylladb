#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
import pytest
import logging

from test.pylib.internal_types import IPAddress, HostID
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_boot_after_ip_change(manager: ManagerClient) -> None:
    """Bootstrap a new node after existing one changed its IP.
       Regression test for #14468. Does not apply to Raft-topology mode.
    """
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    logger.info(f"Booting initial cluster")
    servers = [await manager.server_add(config=cfg) for _ in range(2)]
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info(f"Stopping server {servers[1]}")
    await manager.server_stop_gracefully(servers[1].server_id)

    logger.info(f"Changing IP of server {servers[1]}")
    new_ip = await manager.server_change_ip(servers[1].server_id)
    servers[1] = servers[1]._replace(ip_addr = new_ip)
    logger.info(f"New IP: {new_ip}")

    logger.info(f"Restarting server {servers[1]}")
    await manager.server_start(servers[1].server_id)

    # We need to do this wait before we boot a new node.
    # Otherwise the newly booting node may contact servers[0] even before servers[0]
    # saw the new IP of servers[1], and then the booting node will try to wait
    # for servers[1] to be alive using its old IP (and eventually time out).
    #
    # Note that this still acts as a regression test for #14468.
    # In #14468, the problem was that a booting node would try to wait for the old IP
    # of servers[0] even after all existing servers saw the IP change.
    logger.info(f"Wait until {servers[0]} sees the new IP of {servers[1]}")
    await manager.server_sees_other_server(servers[0].ip_addr, servers[1].ip_addr)

    logger.info(f"Booting new node")
    await manager.server_add(config=cfg)
