#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Test for on_internal_error behavior when triggered during shutdown
# after a startup failure leaves a raft server not properly destroyed.
#
# Related: CUSTOMER-340

import logging

import pytest
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(
    mode="release", reason="error injections are not supported in release mode"
)
async def test_on_internal_error_during_shutdown_after_startup_failure(
    manager: ManagerClient,
) -> None:
    """Test that on_internal_error during shutdown reports the original failure.

    This test reproduces the CUSTOMER-340 scenario using error injections:
    1. Node starts, group0 is established normally
    2. Node is stopped and restarted with the group0_fail_before_emplace
       injection enabled at startup. This causes the server to be registered
       in raft_group_registry but _group0 is never set, so destroy() is
       never called. During shutdown, raft_group_registry::stop() finds the
       undestroyed server and calls on_internal_error.

    The test verifies:
    - The node fails to start (crashes due to on_internal_error abort)
    - The "server for group ... is not destroyed" message is logged
    - The original injection error message is visible in the log
    """
    # Tell the manager to ignore the intentional abort from our error injection
    manager.ignore_cores_log_patterns.append("server for group.*is not destroyed")

    # Start a node normally so group0 is established
    srv = await manager.server_add()
    logger.info("Server %s started successfully", srv.server_id)

    # Stop the node gracefully
    await manager.server_stop_gracefully(srv.server_id)

    # Restart with the injection that throws after server registration
    # but before _group0.emplace, leaving the server undestroyed
    logger.info("Restarting with group0_fail_before_emplace injection")
    await manager.server_update_config(
        srv.server_id,
        key="error_injections_at_startup",
        value=["group0_fail_before_emplace"],
    )

    log = await manager.server_open_log(srv.server_id)
    # The injection throws after start_server_for_group registers the server
    # but before _group0.emplace. During shutdown, raft_group_registry::stop()
    # finds the undestroyed server and calls utils::on_internal_error which aborts.
    await manager.server_start(
        srv.server_id, expected_error="server for group.*is not destroyed"
    )

    # Verify the on_internal_error message is logged
    matches = await log.grep("server for group.*is not destroyed")
    assert len(matches) > 0, (
        "Expected 'server for group ... is not destroyed' error message in the log"
    )

    # Verify our fix detected the stack unwinding condition
    matches = await log.grep("on_internal_error called during stack unwinding")
    assert len(matches) > 0, (
        "Expected 'on_internal_error called during stack unwinding' in the log"
    )

    logger.info("Verified: on_internal_error properly reported during shutdown")
