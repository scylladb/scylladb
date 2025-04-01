#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""
Test consistency of schema changes with server hard stop.
"""
import time
from test.topology.util import wait_for_token_ring_and_group0_consistency
import pytest

<<<<<<< HEAD:test/topology/test_topology_schema.py
=======
pytestmark = pytest.mark.prepare_3_racks_cluster

>>>>>>> 0fdf2a2090 (Merge 'test/pylib: servers_add: support list of property_files' from Benny Halevy):test/cluster/test_topology_schema.py

@pytest.mark.asyncio
async def test_topology_schema_changes(manager, random_tables):
    """Test schema consistency with restart, add, and sudden stop of servers"""
    table = await random_tables.add_table(ncolumns=5)
    servers = await manager.running_servers()

    # Test add column after server restart
    await manager.server_restart(servers[1].server_id)
    await manager.servers_see_each_other(servers)
    await table.add_column()
    await random_tables.verify_schema()

    # Test add column after adding a server
    await manager.server_add(property_file=servers[1].property_file())
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    await table.add_column()
    await random_tables.verify_schema()

    # Test add column after hard stop of a server (1/3)
    await manager.server_stop(servers[1].server_id)
    await table.add_column()
    await random_tables.verify_schema()
