# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_bad_initial_token(manager: ManagerClient):
    # The validity of "initial_token" option is checked in the topology
    # coordinator, even if this is the first node being bootstrap, and triggers
    # rollback. Rollback currently gets stuck in case of rolling back the first
    # node, so use two nodes in the test.
    await manager.server_add()
    manager.ignore_log_patterns.append(r"raft_topology - raft_topology_cmd barrier failed with: service::raft_group_not_found")
    await manager.server_add(config={"initial_token": "etaoin shrdlu"}, expected_error="Failed to assign tokens")
