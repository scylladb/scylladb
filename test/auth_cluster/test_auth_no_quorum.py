#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import time
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
import pytest
from cassandra.auth import PlainTextAuthProvider
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from test.topology.util import trigger_snapshot


"""
Tests how cluster behaves when lost quorum. Ideally for operations with CL=1 live part of the
cluster should still work but that's guaranteed only if auth data is replicated everywhere.
"""
@pytest.mark.asyncio
async def test_auth_no_quorum(manager: ManagerClient) -> None:
    config = {
        # disable auth cache
        'permissions_validity_in_ms': 0,
        'permissions_update_interval_in_ms': 0,
    }
    servers = await manager.servers_add(3, config=config)

    cql, _ = await manager.get_ready_cql(servers)

    # create users, this is done so that previous auth implementation (with RF=1) fails this test
    # otherwise it could happen that all users are luckily placed on a single node
    roles = ["r" + unique_name() for _ in range(10)]
    for role in roles:
        await cql.run_async(f"CREATE ROLE {role} WITH PASSWORD = '{role}' AND LOGIN = true")

    # auth reads are eventually consistent so we need to sync all nodes
    await asyncio.gather(*(read_barrier(manager.api, s.ip_addr) for s in servers))

    # check if users are replicated everywhere
    for role in roles:
        for server in servers:
            await manager.driver_connect(server=server,
                auth_provider=PlainTextAuthProvider(username=role, password=role))
    # lost quorum
    await asyncio.gather(*(
        manager.server_stop_gracefully(srv.server_id) for srv in servers[0:2]))
    alive_server = servers[2]
    # can still login on remaining node - whole auth data is local
    roles.append('cassandra') # include default admin role
    for role in roles:
        await manager.driver_connect(server=alive_server,
            auth_provider=PlainTextAuthProvider(username=role, password=role))
    names = [row.role for row in await manager.get_cql().run_async(f"LIST ROLES", execution_profile='whitelist')]
    assert set(names) == set(roles)

"""
Tests raft snapshot transfer of auth data.
"""
@pytest.mark.asyncio
async def test_auth_raft_snapshot_transfer(manager: ManagerClient) -> None:
    servers = await manager.servers_add(1)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)

    roles = ["ro" + unique_name() for _ in range(10)]
    for role in roles:
        await cql.run_async(f"CREATE ROLE {role}")

    await trigger_snapshot(manager, servers[0])

    # on startup node should receive snapshot
    snapshot_receiving_server = await manager.server_add()
    servers.append(snapshot_receiving_server)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)

    # simulate lost quorum but snapshot with data should have been transferred already
    await manager.server_stop_gracefully(servers[0].server_id)

    await manager.driver_connect(server=snapshot_receiving_server)
    cql = manager.get_cql()
    names = [row.role for row in await cql.run_async(f"LIST ROLES", execution_profile='whitelist')]
    assert(set(names) == set(['cassandra'] + roles))
