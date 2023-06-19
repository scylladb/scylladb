#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.util import check_token_ring_and_group0_consistency

from cassandra.cluster import ConsistencyLevel # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement # type: ignore # pylint: disable=no-name-in-module

import pytest
import logging
import time
from datetime import datetime
from typing import Optional


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_topology_ops(request, manager: ManagerClient):
    """Test basic topology operations using the topology coordinator."""
    logger.info("Bootstrapping first node")
    servers = [await manager.server_add()]

    logger.info(f"Restarting node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    logger.info("Bootstrapping other nodes")
    servers += [await manager.server_add(), await manager.server_add()]

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)

    logger.info(f"Replacing node {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    servers = servers[1:] + [await manager.server_add(replace_cfg)]
    await check_token_ring_and_group0_consistency(manager)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)

    logger.info(f"Removing node {servers[0]} using {servers[1]}")
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
    servers = servers[1:]

    cql = manager.get_cql()
    query = SimpleStatement(
        "select time from system_distributed.cdc_generation_timestamps where key = 'timestamps'",
        consistency_level = ConsistencyLevel.QUORUM)

    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    gen_timestamps = {r.time for r in await manager.get_cql().run_async(query)}
    logger.info(f"Timestamps before check_and_repair: {gen_timestamps}")
    await manager.api.client.post("/storage_service/cdc_streams_check_and_repair", servers[1].ip_addr)
    async def new_gen_appeared() -> Optional[set[datetime]]:
        new_gen_timestamps = {r.time for r in await manager.get_cql().run_async(query)}
        assert(gen_timestamps <= new_gen_timestamps)
        if gen_timestamps < new_gen_timestamps:
            return new_gen_timestamps
        return None
    new_gen_timestamps = await wait_for(new_gen_appeared, time.time() + 60)
    logger.info(f"Timestamps after check_and_repair: {new_gen_timestamps}")

    logger.info(f"Decommissioning node {servers[0]}")
    await manager.decommission_node(servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
