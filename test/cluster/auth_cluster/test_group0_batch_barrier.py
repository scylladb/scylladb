#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import time

import pytest
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.scylla_cluster import ScyllaVersionDescription
from test.pylib.util import unique_name, wait_for
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
async def test_create_role_visible_on_all_nodes(manager: ManagerClient) -> None:
    """After CREATE ROLE returns, the role should be immediately visible
    on every node thanks to the group0 read barrier broadcast.

    Without the barrier, normal Raft replication would eventually deliver
    the committed entry to followers, but there is no guarantee it is
    applied before CREATE ROLE returns.  To exercise the barrier we inject
    a 1-second delay into Raft apply on follower nodes so that ordinary
    replication alone cannot make the role visible in time.  The global
    barrier inside CREATE ROLE must wait for the delayed apply to finish
    before returning, ensuring immediate visibility on every node.
    """
    servers = await manager.servers_add(3, config=auth_config)
    cql, hosts = await manager.get_ready_cql(servers)

    role = "r" + unique_name()

    # Delay Raft apply on one follower node.  Without the broadcast
    # barrier, LIST ROLES right after CREATE ROLE would not see the new
    # role on these nodes because their apply is still sleeping.
    async with inject_error(manager.api, servers[-1].ip_addr,
                            'group0_state_machine::delay_apply'):
        await cql.run_async(f"CREATE ROLE {role}", host=hosts[0])

    # The role must now be visible on every node — no explicit read
    # barrier needed because CREATE ROLE already performed one.
    for host in hosts:
        rows = await cql.run_async("LIST ROLES", host=host)
        roles = {row.role for row in rows}
        assert role in roles, (
            f"Role {role} not visible on {host.address} immediately after creation. "
            f"Visible roles: {roles}"
        )


@pytest.mark.asyncio
async def test_create_role_mixed_cluster(manager: ManagerClient,
                                                            scylla_2025_1: ScyllaVersionDescription) -> None:
    """Variant of test_create_role_visible_on_all_nodes that runs a mixed
    cluster: one new node (master) and two nodes with scylla_2025_1.
    CREATE ROLE is executed on the new node, then we poll LIST ROLES
    on every node until the new role appears. This tests backward
    compatibility of global barrier feature.
    """
    old_servers = await manager.servers_add(2, config=auth_config,
                                            version=scylla_2025_1)
    new_server = await manager.servers_add(1, config=auth_config)
    servers = old_servers + new_server
    cql, hosts = await manager.get_ready_cql(servers)

    new_host = next(h for h in hosts if h.address == new_server[0].ip_addr)

    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role}", host=new_host)

    deadline = time.time() + 180
    for host in hosts:
        async def _role_visible(h=host):
            rows = await cql.run_async("LIST ROLES", host=h)
            roles = {row.role for row in rows}
            if role in roles:
                return True
            return None

        await wait_for(_role_visible, deadline, period=0.1)
