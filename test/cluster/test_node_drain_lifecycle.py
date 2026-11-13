# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio

import pytest

from test.pylib.rest_client import HTTPError, inject_error_one_shot
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("shutdown_first,shutdown_second", [(False, False), (False, True), (True, False)])
async def test_drain_and_shutdown_share_completion(
        manager: ScyllaClusterManager, shutdown_first: bool, shutdown_second: bool) -> None:
    """Regression for #15899: REST drain and SIGTERM must run one drain operation."""
    server = await manager.server_add(cmdline=["--logger-log-level=storage_service=debug"])
    log = await manager.server_open_log(server.server_id)
    injection = await inject_error_one_shot(manager.api, server.ip_addr, "storage_service_drain_wait")
    first = asyncio.create_task(manager.server_stop_gracefully(server.server_id) if shutdown_first
                                else manager.api.drain(server.ip_addr))
    second = None
    try:
        await manager.api.wait_for_injection_enter(server.ip_addr, "storage_service_drain_wait")
        mode = await manager.api.client.get_json("/storage_service/operation_mode", host=server.ip_addr)
        assert mode == "DRAINING", "process shutdown must enter the shared drain operation"
        second = asyncio.create_task(manager.server_stop_gracefully(server.server_id) if shutdown_second
                                     else manager.api.drain(server.ip_addr))
        await log.wait_for("Joining existing node drain", timeout=10)
        assert not first.done() and not second.done()
    finally:
        await injection.message()
        await asyncio.wait_for(asyncio.gather(first, *([second] if second else [])), timeout=60)
    if not shutdown_first and not shutdown_second:
        assert await manager.api.client.get_json("/storage_service/operation_mode", host=server.ip_addr) == "DRAINED"


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_drain_shares_failure(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: all callers must observe a failed drain instead of waiting for an unset promise."""
    server = await manager.server_add()
    await manager.api.enable_injection(server.ip_addr, "storage_service_drain_fail", one_shot=True)
    try:
        for _ in range(2):
            with pytest.raises(HTTPError, match="storage_service_drain_fail"):
                await asyncio.wait_for(manager.api.drain(server.ip_addr), timeout=10)
    finally:
        # The deliberately failed drain cannot perform a graceful process shutdown.
        await manager.server_stop(server.server_id, convict=False)
