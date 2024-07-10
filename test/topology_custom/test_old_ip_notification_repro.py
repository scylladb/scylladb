#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import time
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error, read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_old_ip_notification_repro(manager: ManagerClient) -> None:
    """
    Regression test for #14257.
    It starts two nodes. It introduces a sleep in gossiper::real_mark_alive
    when receiving a gossip notification about
    HOST_ID update from the second node. Then it restarts the second node with
    a different IP. Due to the sleep, the old notification from the old IP arrives
    after the second node has restarted. If the bug is present, this notification
    overrides the address map entry and the second read barrier times out, since
    the first node cannot reach the second node with the old IP.
    """
    s1 = await manager.server_add()
    s2 = await manager.server_add(start=False)
    async with inject_error(manager.api, s1.ip_addr, 'gossiper::real_mark_alive',
                            parameters={ "second_node_ip": s2.ip_addr }) as handler:
        # This injection delays the gossip notification from the initial IP of s2.
        logger.info(f"Starting {s2}")
        await manager.server_start(s2.server_id)
        logger.info(f"Stopping {s2}")
        await manager.server_stop_gracefully(s2.server_id)
        await manager.server_change_ip(s2.server_id)
        logger.info(f"Starting {s2}")
        await manager.server_start(s2.server_id)
        logger.info(f"Wait for cql")
        await manager.get_ready_cql([s1])
        logger.info(f"Read barrier")
        await read_barrier(manager.api, s1.ip_addr)  # Wait for s1 to be aware of s2 with the new IP.
        await handler.message()  # s1 receives the gossip notification from the initial IP of s2.
        logger.info(f"Read barrier")
        # If IP of s2 is overridden by its initial IP, the read barrier should time out.
        await read_barrier(manager.api, s1.ip_addr)
        logger.info(f"Done")
