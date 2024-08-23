#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging

from test.pylib.internal_types import IPAddress
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_start_bootstrapped_with_invalid_seed(manager: ManagerClient):
    """
    Issue https://github.com/scylladb/scylladb/issues/14945.

    Check non-regression: try starting a non-bootstrapped node with an invalid seed.
     1. Start a node with an invalid seed hostname in the seeds list config.
     2. Make sure starting the node fails with an error message.

    Check that when bootstrapped, having a non-resolving DNS name in
    the config, does not prevent node restart.
     1. Start a node and wait till it starts and joins the cluster.
     2. Stop the node and restart it with an invalid seed hostname in the seeds list config.
     3. Make sure the node started successfully since it's already bootstrapped (i.e. is a cluster member).
    """

    s1 = await manager.server_add(start=False)

    # Start the node with an invalid seed and make sure it fails with an error message.
    await manager.server_start(s1.server_id, seeds=[IPAddress("no_address")],
                               expected_error="Bad configuration: invalid value in 'seeds'", wait_interval=20)

    # Start the node with default seeds, to make it join the cluster.
    await manager.server_start(s1.server_id, wait_interval=20)

    # Stop the node.
    await manager.server_stop_gracefully(s1.server_id)

    # Start the node with an invalid seed. and make sure it starts successfully.
    await manager.server_start(s1.server_id, seeds=[IPAddress("no_address")], wait_interval=20)
