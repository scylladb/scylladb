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
async def test_raft_replace_ignore_nodes(manager: ManagerClient) -> None:
    """Replace 3 dead nodes.

       This is a slow test with a 7 node cluster and 3 replace operations,
       we want to run it only in dev mode.
    """
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(7, config={'failure_detector_timeout_in_ms': 2000})
    s1_id = await manager.get_host_id(servers[1].server_id)
    s2_id = await manager.get_host_id(servers[2].server_id)
    logger.info(f"Stopping servers {servers[:3]}")
    await manager.server_stop(servers[0].server_id)
    await manager.server_stop(servers[1].server_id)
    await manager.server_stop_gracefully(servers[2].server_id)

    ignore_dead: list[IPAddress | HostID] = [s1_id, servers[2].ip_addr]
    logger.info(f"Replacing {servers[0]}, ignore_dead_nodes = {ignore_dead}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True,
                                ignore_dead_nodes = ignore_dead)
    await manager.server_add(replace_cfg=replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    ignore_dead = [s2_id]
    logger.info(f"Replacing {servers[1]}, ignore_dead_nodes = {ignore_dead}")
    replace_cfg = ReplaceConfig(replaced_id = servers[1].server_id, reuse_ip_addr = False, use_host_id = True,
                                ignore_dead_nodes = ignore_dead)
    await manager.server_add(replace_cfg=replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info(f"Replacing {servers[2]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg=replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)


@pytest.mark.asyncio
async def test_raft_remove_ignore_nodes(manager: ManagerClient) -> None:
    """Remove 3 dead nodes.

       This is a slow test with a 7 node cluster and 3 removenode operations,
       we want to run it only in dev mode.
    """
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(7)
    s1_id = await manager.get_host_id(servers[1].server_id)
    s2_id = await manager.get_host_id(servers[2].server_id)
    logger.info(f"Stopping servers {servers[:3]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_stop_gracefully(servers[1].server_id)
    await manager.server_stop_gracefully(servers[2].server_id)

    ignore_dead: list[IPAddress] | list[HostID] = [s1_id, s2_id]
    logger.info(f"Removing {servers[0]} initiated by {servers[3]}, ignore_dead_nodes = {ignore_dead}")
    await manager.remove_node(servers[3].server_id, servers[0].server_id, ignore_dead)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    ignore_dead = [servers[2].ip_addr]
    logger.info(f"Removing {servers[1]} initiated by {servers[4]}, ignore_dead_nodes = {ignore_dead}")
    await manager.remove_node(servers[4].server_id, servers[1].server_id, ignore_dead)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info(f"Removing {servers[2]} initiated by {servers[5]}")
    await manager.remove_node(servers[5].server_id, servers[2].server_id, ignore_dead)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
