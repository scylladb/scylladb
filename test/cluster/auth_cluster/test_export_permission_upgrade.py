#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

import pytest
from cassandra.protocol import InvalidRequest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_feature
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import new_test_keyspace, new_test_table


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_export_permission_rolling_upgrade(manager: ScyllaClusterManager) -> None:
    """While a node does not support the EXPORT_PERMISSION feature, no grant stores EXPORT:
    GRANT EXPORT is refused, and the creator of a table gets every other permission on it.
    After that node is upgraded, the feature is enabled, and GRANT EXPORT works on the node
    that was not restarted.
    """
    old_config = auth_config | {'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'EXPORT_PERMISSION'}]}
    servers = [await manager.server_add(config=old_config), await manager.server_add(config=auth_config)]
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async("CREATE ROLE exporter")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY") as table:
            with pytest.raises(InvalidRequest, match='EXPORT permission is not yet supported by this cluster'):
                await cql.run_async(f"GRANT EXPORT ON {table} TO exporter", host=hosts[1])
            resource = 'data/' + table.replace('.', '/')
            rows = await cql.run_async(f"SELECT permissions FROM system.role_permissions WHERE role = 'cassandra' AND resource = '{resource}'")
            assert 'SELECT' in rows[0].permissions
            assert 'EXPORT' not in rows[0].permissions

            await manager.server_update_config(servers[0].server_id, 'error_injections_at_startup', [])
            await manager.server_restart(servers[0].server_id)
            cql, hosts = await manager.get_ready_cql(servers)
            await wait_for_feature('EXPORT_PERMISSION', cql, hosts[1], time.time() + 60)
            await cql.run_async(f"GRANT EXPORT ON {table} TO exporter", host=hosts[1])
