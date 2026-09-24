#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for when a topology request is allowed to report completion.
"""

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.util import wait_for
import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

APPLY_PAUSE = 'group0_pause_before_topology_transition'
REQUEST_PAUSE = 'topology_request_pause_before_wait'


async def wait_for_injection(manager: ScyllaClusterManager, server: ServerInfo, injection: str, count: int = 1):
    """Wait until `injection` has been entered at least `count` times."""
    async def entered():
        enters = await manager.api.get_injection_enter_count(server.ip_addr, injection)
        return True if enters >= count else None
    await wait_for(entered, time.time() + 60, label=f"injection {injection} entered {count} time(s)")


@pytest.mark.xfail(reason="SCYLLADB-4011: a topology request reports completion before the local apply finishes")
async def test_request_completes_after_local_apply(manager: ScyllaClusterManager):
    """A topology request must not report completion while the group0 command
    carrying its result is still being applied on the local node.

    Applying a command writes its mutations to the tables before it rebuilds the
    in-memory state from them, so the request's done flag becomes true while
    the node still holds the old state. The waiter reads that flag before it ever
    sleeps on a topology event, so its very first read can land in that window.

    Disabling tablet balancing submits a no-op topology request, which exists
    only to be waited on, so the test does not depend on what any particular
    request does with its result.
    """
    server = await manager.server_add()

    # Hold every command in the window between writing its mutations and
    # reloading the in-memory state.
    await manager.api.enable_injection(server.ip_addr, APPLY_PAUSE, one_shot=False)
    # Hold the caller between submitting the request and its first read.
    await manager.api.enable_injection(server.ip_addr, REQUEST_PAUSE, one_shot=True)

    logger.info("Submitting a no-op topology request")
    request = asyncio.create_task(manager.api.disable_tablet_balancing(server.ip_addr))

    # The request's own command lands first and carries done=false. Let it
    # through, so the caller reaches the wait and parks before reading.
    await wait_for_injection(manager, server, APPLY_PAUSE, count=1)
    await manager.api.message_injection(server.ip_addr, APPLY_PAUSE)
    await wait_for_injection(manager, server, REQUEST_PAUSE)

    # The coordinator answers the request with a second command, which sets
    # done=true. Its apply parks in the same window, turning the flag true
    # while the in-memory topology state is still the old one.
    await wait_for_injection(manager, server, APPLY_PAUSE, count=2)

    logger.info("Releasing the caller so that its first read lands in the window")
    await manager.api.message_injection(server.ip_addr, REQUEST_PAUSE)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(request), timeout=10)

    logger.info("Releasing the apply; the request must complete now")
    await manager.api.message_injection(server.ip_addr, APPLY_PAUSE)
    await asyncio.wait_for(request, timeout=60)
