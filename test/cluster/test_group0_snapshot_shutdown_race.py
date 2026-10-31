#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_shutdown_during_group0_snapshot_apply(manager: ScyllaClusterManager) -> None:
    """Verify that stopping a joining node while it is applying the group 0 snapshot
    neither crashes nor hangs.

    Reproduces a use-after-free where raft_rpc::apply_snapshot() awaited
    transfer_snapshot() outside raft_rpc's shutdown gate, so raft_rpc::abort()
    could resolve and group 0 be destroyed while the continuation was still queued.
    """
    srv1 = await manager.server_add()
    logger.info("Leader node %s started", srv1.server_id)

    # Pause the joining node in raft_rpc::apply_snapshot() after transfer_snapshot() completed.
    injection = "raft_rpc_apply_snapshot_after_transfer"
    srv2 = await manager.server_add(start=False,
                                    config={'error_injections_at_startup': [injection]})
    logger.info("Created node %s, starting with injection", srv2.server_id)

    start_task = asyncio.create_task(manager.server_start(srv2.server_id))

    log = await manager.server_open_log(srv2.server_id)
    await log.wait_for("transfer snapshot from .* completed", timeout=60)
    await log.wait_for(f"{injection}: waiting for message", timeout=60)
    logger.info("Node %s is paused after transfer_snapshot, stopping it", srv2.server_id)

    stop_task = asyncio.create_task(manager.server_stop_gracefully(srv2.server_id))
    await log.wait_for("Shutting down group 0 service", timeout=60)

    # Group 0 shutdown must wait for the paused apply_snapshot() to leave the gate.
    # Always release the injection, even if an assertion fails, so that teardown
    # does not have to wait for the injection timeout.
    try:
        await asyncio.sleep(1)
        assert not stop_task.done(), "shutdown should be blocked by the paused apply_snapshot"
        assert not await log.grep("Shutting down group 0 service was successful")
    finally:
        await manager.api.message_injection(srv2.ip_addr, injection)

    # An interrupted join exits with 0 or 1, depending on whether it observes
    # abort_requested_exception or raft::request_aborted. A crash is a death by
    # signal, i.e. a negative return code.
    try:
        await stop_task
    except RuntimeError as e:
        assert "exit code: 1" in str(e), f"node {srv2.server_id} did not stop cleanly: {e}"
    logger.info("Node %s stopped without crashing", srv2.server_id)

    # The node was stopped mid-join, so the start must fail. If it succeeds, the stop
    # did not interrupt the join and the test did not exercise the race.
    with pytest.raises(RuntimeError, match="requested to be stopped in the meantime") as start_error:
        await start_task
    logger.info("Start of node %s failed as expected: %r", srv2.server_id, start_error.value)

    assert not await log.grep(r"gate_closed_exception.*raft_rpc::shutdown")
