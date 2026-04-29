#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_failure_after_group0_server_registration(manager: ManagerClient) -> None:
    """Test that a node shuts down cleanly when group0 startup fails after server registration.

    Reproducer for: CUSTOMER-340, CUSTOMER-335, SCYLLADB-1217

    On restart, setup_group0_if_exist() calls start_server_for_group0() which
    registers the raft server in raft_group_registry._servers, then calls
    enable_in_memory_state_machine(). If enable_in_memory_state_machine() throws
    (e.g., because reload_state() -> auth_cache().load_all() fails due to topology
    being in a transitional state), the exception propagates and stack unwinding
    calls raft_group_registry::stop().

    Previously, _group0 was set AFTER the with_scheduling_group lambda returned,
    so a throw inside the lambda left _group0 as monostate. abort_and_drain() and
    destroy() would be no-ops, leaving the server orphaned in _servers.
    raft_group_registry::stop() would then hit on_internal_error
    ("server for group ... is not destroyed") and abort.

    The fix moves _group0.emplace() inside the lambda, immediately after
    start_server_for_group(), so destroy() can always find and clean up the server.

    This test:
    1. Starts a node normally (group0 established)
    2. Stops the node
    3. Restarts with an injection that fails enable_in_memory_state_machine()
    4. Verifies the node fails startup cleanly (no abort)
    """
    # Start a node normally so group0 is established
    srv = await manager.server_add()
    logger.info("Server %s started successfully with group0", srv.server_id)

    logger.info("Stopping server %s", srv.server_id)
    await manager.server_stop_gracefully(srv.server_id)

    logger.info("Restarting server %s with injection to fail enable_in_memory_state_machine", srv.server_id)
    await manager.server_update_config(srv.server_id,
                                       key='error_injections_at_startup',
                                       value=['group0_state_machine_enable_in_memory_fail'])
    await manager.server_start(srv.server_id,
                               expected_error="injected failure in enable_in_memory_state_machine")

    # If we get here without the test framework detecting a crash/abort,
    # the node shut down cleanly. The fix ensures abort_and_drain()/destroy()
    # can find and clean up the raft server even when startup fails.
    logger.info("Server failed startup and shut down cleanly (no abort)")
