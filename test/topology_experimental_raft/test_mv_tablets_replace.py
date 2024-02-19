#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from typing import List

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.internal_types import ServerInfo, HostID

import pytest
import asyncio
import logging

from test.topology.conftest import skip_mode
from test.topology.util import get_topology_coordinator
from test.topology_experimental_raft.test_mv_tablets import get_tablet_replicas

logger = logging.getLogger(__name__)


async def find_server_by_host_id(manager: ManagerClient, servers: List[ServerInfo], host_id: HostID) -> ServerInfo:
    for s in servers:
        if await manager.get_host_id(s.server_id) == host_id:
            return s
    raise Exception(f"Host ID {host_id} not found in {servers}")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_mv_replica_pairing_during_replace(manager: ManagerClient):
    """
    Verifies that view replica pairing is stable in the case of node replace.
    After replace, the node is in left state, but still present in the replica set.
    If view pairing code would use get_natural_endpoints(), which excludes left nodes,
    the pairing would be shifted during replace.
    """

    servers = await manager.servers_add(4)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"
                        " AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")

    # Disable migrations concurrent with replace since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    base_replicas = await get_tablet_replicas(manager, servers[0], "test", "test", 0)
    logger.info(f'test.test replicas: {base_replicas}')
    view_replicas = await get_tablet_replicas(manager, servers[0], "test", "tv", 0)
    logger.info(f'test.tv replicas: {view_replicas}')
    server_to_replace = await find_server_by_host_id(manager, servers, HostID(str(view_replicas[0][0])))
    server_to_down = await find_server_by_host_id(manager, servers, HostID(str(base_replicas[0][0])))

    logger.info('Downing a node to be replaced')
    await manager.server_stop(server_to_replace.server_id)

    logger.info('Blocking tablet rebuild')
    coord = await get_topology_coordinator(manager)
    coord_serv = await find_server_by_host_id(manager, servers, coord)
    await manager.api.enable_injection(coord_serv.ip_addr, "tablet_transition_updates", one_shot=True)
    coord_log = await manager.server_open_log(coord_serv.server_id)
    coord_mark = await coord_log.mark()

    logger.info('Replacing the node')
    replace_cfg = ReplaceConfig(replaced_id = server_to_replace.server_id, reuse_ip_addr = False, use_host_id = True)
    replace_task = asyncio.create_task(manager.server_add(replace_cfg))

    await coord_log.wait_for('tablet_transition_updates: start', from_mark=coord_mark)

    if server_to_down.server_id != server_to_replace.server_id:
        await manager.server_stop(server_to_down.server_id)

    # The update is supposed to go to the second replica only, since the other one is downed.
    # If pairing would shift, the update to the view would be lost because the first replica
    # is the one which is in the left state.
    logger.info('Updating base table')
    await cql.run_async(SimpleStatement("INSERT INTO test.test (pk, c) VALUES (3, 4)", consistency_level=ConsistencyLevel.ONE))
    logger.info('Querying the view')
    assert [(4,3)] == list(await cql.run_async(SimpleStatement("SELECT * FROM test.tv WHERE c=4", consistency_level=ConsistencyLevel.ONE)))

    if server_to_down.server_id != server_to_replace.server_id:
        await manager.server_start(server_to_down.server_id)

    logger.info('Unblocking tablet rebuild')
    if coord_serv.server_id != server_to_down.server_id:
        await manager.api.message_injection(coord_serv.ip_addr, "tablet_transition_updates")

    logger.info('Waiting for replace')
    await replace_task

    logger.info('Querying')
    assert [(4,3)] == list(await cql.run_async("SELECT * FROM test.tv WHERE c=4"))
