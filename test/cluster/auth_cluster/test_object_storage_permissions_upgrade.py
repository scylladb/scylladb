#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

import pytest
from cassandra.protocol import InvalidRequest

from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_feature
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


# Test that GRANT on ALL OBJECT STORAGE waits for the OBJECT_STORAGE_PERMISSIONS feature in a rolling upgrade:
# 1. Start two nodes. The first node acts as an old version, because it does not announce the feature.
# 2. Check that GRANT on ALL OBJECT STORAGE fails on the second node.
# 3. Restart the first node with the feature, as if it is upgraded.
# 4. Wait until the cluster enables the feature, then check that GRANT works on the second node.
# 5. Check that the first node lists the grant.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_object_storage_permissions_rolling_upgrade(manager: ScyllaClusterManager) -> None:
    old_config = auth_config | {'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'OBJECT_STORAGE_PERMISSIONS'}]}
    servers = [await manager.server_add(config=old_config), await manager.server_add(config=auth_config)]
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async("CREATE ROLE exporter")
    with pytest.raises(InvalidRequest, match='ALL OBJECT STORAGE are not yet supported by this cluster'):
        await cql.run_async("GRANT MODIFY ON ALL OBJECT STORAGE TO exporter", host=hosts[1])

    await manager.server_update_config(servers[0].server_id, 'error_injections_at_startup', [])
    await manager.server_restart(servers[0].server_id)
    cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature('OBJECT_STORAGE_PERMISSIONS', cql, hosts[1], time.time() + 60)
    await cql.run_async("GRANT MODIFY ON ALL OBJECT STORAGE TO exporter", host=hosts[1])
    await read_barrier(manager.api, servers[0].ip_addr)
    rows = await cql.run_async("LIST ALL PERMISSIONS OF exporter", host=hosts[0])
    assert [(row.resource, row.permission) for row in rows] == [('<all object storage>', 'MODIFY')]
