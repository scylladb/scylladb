#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time

import pytest
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error
from test.pylib.scylla_server import ScyllaVersionDescription
from test.pylib.util import unique_name, wait_for
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
async def test_create_role_visible_on_all_nodes(manager: ScyllaClusterManager) -> None:
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



@pytest.mark.parametrize("create", ["ROLE", "SERVICE_LEVEL"])
@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
async def test_noop_create_does_not_block_apply(manager: ScyllaClusterManager, create: str) -> None:
    """Reproduces SCYLLADB-6047: a no-op CREATE ... IF NOT EXISTS kept its
    group0 guard while it sent the read barrier to all live nodes. The
    guard holds the read-apply mutex, so group0 could not apply on its
    node until all nodes answered.

    Node B cannot apply group0 entries, so the broadcast of a no-op
    statement on node A waits for B. Check that a change made through
    node C is applied on node A in the meantime.
    """
    servers = await manager.servers_add(3, config=auth_config)
    cql, hosts = await manager.get_ready_cql(servers)
    server_a, server_b, _ = servers
    host_a, _, host_c = hosts
    existing = "e" + unique_name()
    behind_b = "b" + unique_name()
    new = "n" + unique_name()
    table, column = ("roles", "role") if create == "ROLE" else ("service_levels_v2", "service_level")

    async def applied_on_a(name: str) -> bool | None:
        rows = await cql.run_async(f"SELECT {column} FROM system.{table} WHERE {column} = '{name}'", host=host_a)
        return True if rows else None

    await cql.run_async(f"CREATE {create} {existing}", host=host_a)

    block_apply = "group0_state_machine::block_apply"
    await manager.api.enable_injection(server_b.ip_addr, block_apply, one_shot=False)
    statements = []
    try:
        statements.append(cql.run_async(f"CREATE {create} {behind_b}", host=host_c))
        await wait_for(lambda: applied_on_a(behind_b), time.time() + 60, period=0.1)

        await manager.api.set_logger_level(server_a.ip_addr, "group0_client", "debug")
        log_a = await manager.server_open_log(server_a.server_id)
        mark = await log_a.mark()
        statements.append(cql.run_async(f"CREATE {create} IF NOT EXISTS {existing}", host=host_a))
        await log_a.wait_for("broadcast_group0_read_barrier: sending read barrier", from_mark=mark, timeout=60)

        statements.append(cql.run_async(f"CREATE {create} {new}", host=host_c))
        await wait_for(lambda: applied_on_a(new), time.time() + 30, period=0.1)
    finally:
        await manager.api.disable_injection(server_b.ip_addr, block_apply)
        await asyncio.gather(*statements)


async def test_create_role_mixed_cluster(manager: ScyllaClusterManager,
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
    # Use IF NOT EXISTS to work around the Python driver bug where connection
    # pool renewal after concurrent node bootstraps can cause the statement
    # to be executed twice (scylladb/python-driver#317).
    await cql.run_async(f"CREATE ROLE IF NOT EXISTS {role}", host=new_host)

    deadline = time.time() + 180
    for host in hosts:
        async def _role_visible(h=host):
            rows = await cql.run_async("LIST ROLES", host=h)
            roles = {row.role for row in rows}
            if role in roles:
                return True
            return None

        await wait_for(_role_visible, deadline, period=0.1)
