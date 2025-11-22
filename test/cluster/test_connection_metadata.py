# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
from cassandra.policies import WhiteListRoundRobinPolicy
from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, IntType
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.util import wait_for_first_completed
from test.cluster.util import wait_for_token_ring_and_group0_consistency

import asyncio
from datetime import datetime, timedelta
import pytest
import logging
import time
from test.pylib.util import wait_for
from cassandra.query import SimpleStatement, ConsistencyLevel

logger = logging.getLogger(__name__)
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.asyncio

async def test_connection_metadata(request, manager: ManagerClient):
    num_servers = 3

    async def expected_size(inner_cql, expected_size):
        res = list(inner_cql.execute(SimpleStatement("SELECT * FROM system.connection_metadata"), consistency_level=ConsistencyLevel.QUORUM))
        if len(res) == expected_size:
            return res
        return None

    # Run three nodes one by one
    for i in range(num_servers):
        servers = await manager.servers_add(1)
        cql, hosts = await manager.get_ready_cql(await manager.running_servers())

        await manager.api.client.post("/v2/connection-metadata", host=servers[0].ip_addr, json=[
            {
                "connection_id": "123e4567-e89b-12d3-a456-42661417400%d" % i,
                "host_id": "123e4567-e89b-12d3-a456-426614174000",
                "address": "addr1.test",
                "port": 8001,
                "tls_port": 8002,
                "alternator_port": 8003,
                "alternator_https_port": 8004,
                "rack": "rack1",
                "datacenter": "dc1",
            },
        ])
        wait_for(lambda: expected_size(cql, i), time.time() + 10)

    # Remove one node
    running_servers = await manager.running_servers()
    server_to_stop = running_servers[0]
    running_server = running_servers[1]
    await manager.server_stop(server_to_stop.server_id)
    await manager.remove_node(running_server.server_id, server_to_stop.server_id)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    wait_for(lambda: expected_size(cql, num_servers), time.time() + 10)

    # Verify everything works
    await manager.api.client.post("/v2/connection-metadata", timeout=10, host=running_server.ip_addr, json=[
        {
            "connection_id": "99999999-e89b-12d3-a456-426614174000",
            "host_id": "123e4567-e89b-12d3-a456-426614174002",
            "address": "addr1.test",
            "port": 8001,
            "tls_port": 8002,
            "alternator_port": 8003,
            "alternator_https_port": 8004,
            "rack": "rack1",
            "datacenter": "dc1",
        },
    ])
    wait_for(lambda: expected_size(cql, num_servers + 1), time.time() + 10)

