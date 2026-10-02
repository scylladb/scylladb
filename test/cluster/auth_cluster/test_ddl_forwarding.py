#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest
from cassandra.auth import PlainTextAuthProvider
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.test_ddl_forwarding import forward_attempts, leader_and_follower


@pytest.mark.asyncio
async def test_role_statements_run_locally(manager: ScyllaClusterManager):
    """Role statements carry plaintext passwords, so they are not forwarded."""
    servers = await manager.servers_add(3, config=auth_config, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    leader, follower, host_of = await leader_and_follower(manager, servers, hosts)
    follower_host = host_of[follower.ip_addr]

    attempts = await forward_attempts(manager, follower.ip_addr)
    await cql.run_async("CREATE ROLE bob WITH PASSWORD = 'pw' AND LOGIN = true", host=follower_host)
    await cql.run_async("ALTER ROLE bob WITH PASSWORD = 'pw2'", host=follower_host)
    assert await forward_attempts(manager, follower.ip_addr) == attempts
    bob = manager.con_gen([follower.ip_addr], auth_provider=PlainTextAuthProvider(username='bob', password='pw2'))
    try:
        bob.connect().execute("SELECT * FROM system.local")
    finally:
        bob.shutdown()
