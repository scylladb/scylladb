#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import time
from contextlib import suppress

import pytest

from test.cluster.conftest import skip_mode
from test.cluster.util import (
    get_topology_coordinator,
    find_server_by_host_id,
    wait_for_token_ring_and_group0_consistency,
)
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for_first_completed

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_decommission_kill_then_replace(manager: ManagerClient) -> None:
    """
    Boots a 3-node cluster, pauses the topology coordinator before processing backlog,
    starts decommissioning a non-coordinator node, kills it before decommission completes,
    and replaces it. Finally, unpauses the coordinator and waits for topology convergence.
    """
    # Start a 3-node cluster
    servers = await manager.servers_add(3)

    # Identify the topology coordinator and enable the pause injection there
    coord_host_id = await get_topology_coordinator(manager)
    coord_srv = await find_server_by_host_id(manager, servers, coord_host_id)
    inj = 'topology_coordinator_pause_before_processing_backlog'
    await manager.api.enable_injection(coord_srv.ip_addr, inj, one_shot=True)

    # Pick a decommission target that's not the coordinator
    decomm_target = next(s for s in servers if s.server_id != coord_srv.server_id)
    logger.info(f"Starting decommission on {decomm_target}, coordinator is {coord_srv}")

    # Start decommission in the background
    decomm_task = asyncio.create_task(
        manager.decommission_node(
            decomm_target.server_id,
        )
    )

    log = await manager.server_open_log(decomm_target.server_id)
    await log.wait_for("api - decommission")

    # Kill the node before decommission completes to simulate failure mid-operation
    await manager.server_stop_gracefully(decomm_target.server_id)
    logger.info(f"Killed decommissioning node {decomm_target}")

    # Replace the killed node with a new one
    replace_cfg = ReplaceConfig(
        replaced_id=decomm_target.server_id,
        reuse_ip_addr=False,
        use_host_id=True,
    )
    logger.info(f"Replacing killed node {decomm_target} with a new node (async)")

    # Start replacement asynchronously
    replace_task = asyncio.create_task(manager.server_add(replace_cfg=replace_cfg))

    # Wait until ANY node receives the join request from the replacing node
    running = await manager.running_servers()
    logs = [await manager.server_open_log(s.server_id) for s in running]
    marks = [await log.mark() for log in logs]
    await wait_for_first_completed([
        l.wait_for("received request to join from host_id", from_mark=m)
        for l, m in zip(logs, marks)
    ])

    # Unpause the coordinator so backlog processing can proceed
    await manager.api.message_injection(coord_srv.ip_addr, inj)

    # Wait for the replacement to finish once the coordinator is unpaused
    new_server_info = await replace_task
    logger.info(f"Replacement finished: {new_server_info}")

    # Wait for topology to converge: group0 and token ring match
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 60)

    # Make sure the background decommission task completes (expected to fail)
    with suppress(Exception):
        await decomm_task
