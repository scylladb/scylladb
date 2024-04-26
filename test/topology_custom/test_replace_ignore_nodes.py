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
async def test_replace_ignore_nodes(manager: ManagerClient) -> None:
    """Replace a node in presence of multiple dead nodes.
       Regression test for #14487. Does not apply to Raft-topology mode.

       This is a slow test with a 7 node cluster any 3 replace operations,
       we don't want to run it in debug mode.
       Preferably run it only in one mode e.g. dev.
    """
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    logger.info(f"Booting initial cluster")
    servers = [await manager.server_add(config=cfg) for _ in range(7)]
    s2_id = await manager.get_host_id(servers[2].server_id)
    logger.info(f"Stopping servers {servers[:3]}")
    await manager.server_stop(servers[0].server_id)
    await manager.server_stop(servers[1].server_id)
    await manager.server_stop_gracefully(servers[2].server_id)

    # The parameter accepts both IP addrs with host IDs.
    # We must be able to resolve them in both ways.
    ignore_dead: list[IPAddress | HostID] = [servers[1].ip_addr, s2_id]
    logger.info(f"Replacing {servers[0]}, ignore_dead_nodes = {ignore_dead}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False,
                                ignore_dead_nodes = ignore_dead)
    await manager.server_add(replace_cfg=replace_cfg, config=cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    ignore_dead = [servers[2].ip_addr] 
    logger.info(f"Replacing {servers[1]}, ignore_dead_nodes = {ignore_dead}")
    replace_cfg = ReplaceConfig(replaced_id = servers[1].server_id, reuse_ip_addr = False, use_host_id = False,
                                ignore_dead_nodes = ignore_dead)
    await manager.server_add(replace_cfg=replace_cfg, config=cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info(f"Replacing {servers[2]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = False)
    await manager.server_add(replace_cfg=replace_cfg, config=cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
