#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency
from test.pylib.internal_types import ServerInfo
import pytest
import logging


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_replace_with_same_ip_twice(manager: ManagerClient) -> None:
    logger.info("starting a cluster with two nodes")
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    logger.info(f"cluster started {servers}")

    async def replace_with_same_ip(s: ServerInfo) -> ServerInfo:
        logger.info(f"stopping server {s.server_id}")
        await manager.server_stop_gracefully(s.server_id)

        logger.info(f"replacing server {s.server_id} with same ip")
        replace_cfg = ReplaceConfig(replaced_id = s.server_id, reuse_ip_addr = True, use_host_id = False)
        return await manager.server_add(replace_cfg)

    test_server = servers[1]
    test_server = await replace_with_same_ip(test_server)
    await replace_with_same_ip(test_server)
