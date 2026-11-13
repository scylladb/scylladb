#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging

import pytest

from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


logger = logging.getLogger(__name__)


async def check_group0_endpoints(manager: ScyllaClusterManager, ip_addr: str, available: bool) -> None:
    requests = {
        "GET /raft/leader_host": lambda: manager.api.client.get_json("/raft/leader_host", host=ip_addr),
        "POST /raft/read_barrier": lambda: manager.api.client.post("/raft/read_barrier", host=ip_addr),
        "POST /raft/trigger_stepdown": lambda: manager.api.client.post("/raft/trigger_stepdown", host=ip_addr),
    }
    for name, request in requests.items():
        logger.info(f"Sending {name} to {ip_addr}")
        if available is False:
            with pytest.raises(HTTPError) as exc_info:
                await request()
            assert exc_info.value.code == 503, f"{name}: {exc_info.value}"
        else:
            try:
                await request()
            except HTTPError as e:
                # Stepdown can only be called on the leader, so this is expected.
                assert name == "POST /raft/trigger_stepdown", f"{name}: Unexpected error: {e.code} / {e.message}"
                assert e.code == 500, f"{name}: Unexpected error code: {e.code}"
                assert "Not a leader" in e.message, f"{name}: Unexpected error message: {e.message}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_raft_api_during_startup(manager: ScyllaClusterManager):
    """
    A bootstrapping node sets up group 0 only in join_cluster(), long after
    the Raft API is registered.

    Reproducer of https://scylladb.atlassian.net/browse/SCYLLADB-6055.
    """
    injection = "pause_after_registering_raft_api"

    await manager.server_add()
    server = await manager.server_add(start=False, config={"error_injections_at_startup": [injection]})

    start_task = asyncio.create_task(manager.server_start(server.server_id))
    await manager.api.wait_for_injection_enter(server.ip_addr, injection)

    await check_group0_endpoints(manager, server.ip_addr, available=False)

    # The node must still be able to finish bootstrapping.
    await manager.api.message_injection(server.ip_addr, injection)
    await manager.api.disable_injection(server.ip_addr, injection)
    await start_task

    await check_group0_endpoints(manager, server.ip_addr, available=True)


async def test_raft_api_in_maintenance_mode(manager: ScyllaClusterManager):
    """
    A node in maintenance mode never sets up group 0.

    Reproducer of https://scylladb.atlassian.net/browse/SCYLLADB-6055.
    """
    server = await manager.server_add()

    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, "maintenance_mode", True)
    await manager.server_start(server.server_id)

    await check_group0_endpoints(manager, server.ip_addr, available=False)
