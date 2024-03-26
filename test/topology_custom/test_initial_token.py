#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import logging
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver
from cassandra.policies import WhiteListRoundRobinPolicy
from test.topology.conftest import cluster_con

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_initial_token(manager: ManagerClient) -> None:
    tokens = ["-9223372036854775808", "-4611686018427387904", "0", "4611686018427387904"]
    cfg1 = {'initial_token': f"{tokens[0]}, {tokens[1]}"}
    cfg2 = {'initial_token': f"{tokens[2]}, {tokens[3]}"}
    s1 = await manager.server_add(config=cfg1)
    s2 = await manager.server_add(config=cfg2)
    cql1 = cluster_con([s1.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([s1.ip_addr])).connect()
    cql2 = cluster_con([s2.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([s2.ip_addr])).connect()
    res1 = cql1.execute("SELECT tokens From system.local").one()
    res2 = cql2.execute("SELECT tokens From system.local").one()
    assert all([i in res1.tokens for i in tokens[:2]]) and all([i in res2.tokens for i in tokens[-2:]])
    # Try to boot a node with conflicting tokens. It should fail.
    s2 = await manager.server_add(config=cfg2, expected_error="Bootstrap failed. See earlier errors")
