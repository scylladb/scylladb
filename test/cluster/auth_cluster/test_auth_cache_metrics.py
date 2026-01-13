#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import logging

from test.pylib.manager_client import ManagerClient
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from cassandra.auth import PlainTextAuthProvider

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_auth_cache_metrics(manager: ManagerClient):
    """
    Verify that auth cache metrics correctly track roles and permissions
    """
    smp = 4
    servers = await manager.servers_add(1, cmdline=[f'--smp={smp}'], config=auth_config)
    cql, _ = await manager.get_ready_cql(servers)
    node = servers[0]

    async def get_metric(node_ip, metric_name, sum_all_shards=False):
        metrics = await manager.metrics.query(node_ip)
        shard_zero_val = metrics.get(metric_name, {'shard': '0'})
        total = 0.0
        all_vals = [total]
        for shard in range(smp):
            shard_val = metrics.get(metric_name, {'shard': str(shard)})
            if not sum_all_shards:
                assert shard_val == shard_zero_val, f"metric {metric_name} differs for shard {shard}"
            else:
                total += float(shard_val)
        if sum_all_shards:
            return total
        else:
            return shard_zero_val

    initial_roles = await get_metric(node.ip_addr, "scylla_auth_cache_roles")
    initial_perms = await get_metric(node.ip_addr, "scylla_auth_cache_permissions", sum_all_shards=True)
    logger.info(f"Initial metrics - Roles: {initial_roles}, Permissions: {initial_perms}")

    assert initial_roles == 1
    assert initial_perms == 0

    new_roles_count = 10
    roles = [f"metric_test_role_{i}" for i in range(new_roles_count)]

    logger.info(f"Creating new roles")
    for role_name in roles:
        await cql.run_async(f"CREATE ROLE {role_name} WITH PASSWORD = 'password' AND LOGIN = true")
        # This should results in adding 2 permissions (1 for each of 2 resources)
        await cql.run_async(f"GRANT SELECT ON KEYSPACE system TO {role_name}")
        await cql.run_async(f"GRANT MODIFY ON ALL KEYSPACES TO {role_name}")


    logger.info(f"Log in to each role to lazy cache permissions")
    for role_name in roles:
        await manager.driver_connect(auth_provider=PlainTextAuthProvider(username=role_name, password="password"))
        cql = manager.get_cql()
        # This loads permissions for system keyspace resource, on a single shard
        await cql.run_async(f"SELECT * FROM system.roles")

    logger.info(f"Log in back to cassandra")
    await manager.driver_connect(auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    cql = manager.get_cql()

    current_roles = await get_metric(node.ip_addr, "scylla_auth_cache_roles")
    current_perms = await get_metric(node.ip_addr, "scylla_auth_cache_permissions", sum_all_shards=True)
    
    logger.info(f"After addition - Roles: {current_roles}, Permissions: {current_perms}")
    
    cassandra_perms_count = 2 # Creating roles causes data and data/system resources to be added

    assert current_roles == initial_roles + new_roles_count
    assert current_perms == initial_perms + 2*new_roles_count + cassandra_perms_count

    logger.info(f"Dropping created roles")
    for role_name in roles:
        await cql.run_async(f"DROP ROLE {role_name}")

    final_roles = await get_metric(node.ip_addr, "scylla_auth_cache_roles")
    final_perms = await get_metric(node.ip_addr, "scylla_auth_cache_permissions", sum_all_shards=True)

    logger.info(f"After cleanup - Roles: {final_roles}, Permissions: {final_perms}")

    assert final_roles == initial_roles
    assert final_perms == initial_perms
