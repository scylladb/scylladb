#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging

import pytest

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_joining_node_as_seed(manager: ManagerClient) -> None:
    """
    1. Add node A.
    2. Add node B with seeds={A}, but block it via an error injection after it joins group0, and before it sets its
    local group0 upgrade state to synchronize and later to use_post_raft_procedures.
    3. Add node C with seeds={B}, so that B starts handling C's join request while still using use_pre_raft_procedures.
    4. Unblock B after it starts handling C's join request (and calls raft_group0_client::start_operation()).
    5. Make sure B and C successfully start.

    Reproducer for https://scylladb.atlassian.net/browse/SCYLLADB-2843.
    """
    logger.info("Starting node A")
    node_a = await manager.server_add()

    injection = 'join_group0_pause_before_config_check'
    logger.info(f"Adding stopped node B with {injection} enabled")
    node_b = await manager.server_add(start=False, config={
        'error_injections_at_startup': [injection],
    })

    logger.info("Starting node B")
    node_b_start = asyncio.create_task(manager.server_start(node_b.server_id, seeds=[node_a.ip_addr]))

    node_b_log = await manager.server_open_log(node_b.server_id)
    await node_b_log.wait_for(f"{injection}: waiting for message", timeout=60)

    logger.info("Adding node C")
    node_c_add = asyncio.create_task(manager.server_add(seeds=[node_b.ip_addr]))

    # Lower timeout to fail the test faster without the fix.
    await node_b_log.wait_for("waiting until local node in state use_pre_raft_procedures completes upgrade to group 0",
                              timeout=15)

    logger.info("Unblocking node B")
    await manager.api.message_injection(node_b.ip_addr, injection)

    logger.info("Waiting for nodes B and C to start")
    await node_b_start
    await node_c_add
