#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import asyncio
import time

import pytest

from test.cluster.util import get_current_group0_config
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_bootstrap_with_quick_group0_join(manager: ManagerClient):
    """Regression test for https://scylladb.atlassian.net/browse/SCYLLADB-959.

    The bug was that when the bootstrapping node joined group0 before reaching
    post_server_start, it skipped post_server_start and thus hung forever.

    The test simulates the scenario by starting the second node with the
    join_group0_pause_before_config_check injection. Without the fix, the
    startup times out.
    """
    logger.info("Adding first server")
    s1 = await manager.server_add()

    logger.info("Adding second server with join_group0_pause_before_config_check enabled")
    s2 = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['join_group0_pause_before_config_check']
    })

    logger.info(f"Starting {s2}")
    start_task = asyncio.create_task(manager.server_start(s2.server_id))

    s2_log = await manager.server_open_log(s2.server_id)

    await s2_log.wait_for("join_group0_pause_before_config_check: waiting for message", timeout=60)

    s1_host_id = await manager.get_host_id(s1.server_id)
    s2_host_id = await manager.get_host_id(s2.server_id)

    async def s2_in_group0_config_on_s1():
        config = await get_current_group0_config(manager, s1)
        ids = {m[0] for m in config}
        assert s1_host_id in ids  # sanity check
        return True if s2_host_id in ids else None

    # Note: we would like to wait for s2 to see itself in the group0 config, but we can't execute
    # get_current_group0_config for s2, as s2 doesn't handle CQL requests at this point. As a workaround, we wait for s1
    # to see s2 and then perform a read barrier on s2.
    logger.info(f"Waiting for {s1} to see {s2} in the group0 config")
    await wait_for(s2_in_group0_config_on_s1, deadline=time.time() + 60, period=0.1)

    logger.info(f"Performing read barrier on {s2} to make sure it sees itself in the group0 config")
    await read_barrier(manager.api, s2.ip_addr)

    logger.info(f"Unblocking {s2}")
    await manager.api.message_injection(s2.ip_addr, 'join_group0_pause_before_config_check')

    logger.info(f"Waiting for {s2} to complete bootstrap")
    await asyncio.wait_for(start_task, timeout=60)
