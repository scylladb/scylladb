#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.internal_types import HostID

import pytest
import asyncio
import logging
import time
from typing import Callable

from test.cluster.util import get_topology_coordinator
from test.cluster.mv.tablets.test_mv_tablets import get_tablet_replicas
from test.cluster.util import new_test_keyspace, wait_for

logger = logging.getLogger(__name__)


async def get_disjoint_paired_replicas(manager, servers, ks):
    """
    finds disjoint sets for paired view and base replicas validates RF=2.
    MV tablet pairing logic is rack-based so base and view rack will be validated.
    returns placment that can be used later in the tests body.
    """
    base_replicas = await get_tablet_replicas(manager, servers[0], ks, "test", 0)
    view_replicas = await get_tablet_replicas(manager, servers[0], ks, "tv", 0)
    if len(base_replicas) != 2 or len(view_replicas) != 2:
        logger.info("Waiting for RF=2 placement: base=%s view=%s", base_replicas, view_replicas)
        return None
    base_ids = {host for host, _ in base_replicas}
    view_ids = {host for host, _ in view_replicas}
    if base_ids & view_ids:
        logger.info("Waiting for disjoint base/view replicas: base=%s view=%s", base_replicas, view_replicas)
        return None
    base_servers = [ await manager.find_server_by_host_id(servers, host) for host, _ in base_replicas ]
    view_servers = [ await manager.find_server_by_host_id(servers, host) for host, _ in view_replicas ]
    base_by_rack = {(s.datacenter, s.rack): s for s in base_servers}
    view_by_rack = {(s.datacenter, s.rack): s for s in view_servers}
    if len(base_by_rack) != len(base_servers) or len(view_by_rack) != len(view_servers):
        logger.info("Waiting for one replica per rack: base=%s view=%s", base_servers, view_servers)
        return None
    if base_by_rack.keys() != view_by_rack.keys():
        logger.info("Waiting for matching base/view racks: base=%s view=%s", base_by_rack, view_by_rack)
        return None
    return base_replicas, view_replicas, base_by_rack, view_by_rack


def select_non_coordinator_replica_pair(base_by_rack, view_by_rack, coord_serv):
    for location in view_by_rack.keys():
        view_server = view_by_rack[location]
        base_server = base_by_rack[location]
        if (view_server.server_id != coord_serv.server_id and base_server.server_id != coord_serv.server_id):
            return view_server, base_server
    raise AssertionError(f"Could not find paired base/view replicas outside topology coordinator {coord_serv}. "
        f"base_by_rack={base_by_rack}, view_by_rack={view_by_rack}")


@pytest.mark.nightly
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_mv_replica_pairing_during_replace(manager: ManagerClient, scale_timeout: Callable[[int | float], int | float]):
    """
    Verifies that view replica pairing is stable in the case of node replace.
    After replace, the node is in left state, but still present in the replica set.
    If view pairing code would use get_natural_endpoints(), which excludes left nodes,
    the pairing would be shifted during replace.
    """

    cmdline = ["--smp=1"]
    config = {"failure_detector_timeout_in_ms": 5000}
    servers = await manager.servers_add(4, cmdline=cmdline, 
                                        config=config,
                                        property_file=[{"dc": "dc1", "rack": "r1"},
                                                       {"dc": "dc1", "rack": "r1"},
                                                       {"dc": "dc1", "rack": "r2"},
                                                       {"dc": "dc1", "rack": "r2"}])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")

        # wait for replicas to be in RF=2 and rack paired
        async def read_disjoint_paired_replicas():
            return await get_disjoint_paired_replicas(manager, servers, ks)

        await wait_for(read_disjoint_paired_replicas, time.time() + scale_timeout(60))

        # Disable migrations concurrent with replace since we don't handle nodes going down during migration yet.
        # See https://github.com/scylladb/scylladb/issues/16527
        await manager.disable_tablet_balancing()

        # get topology coordinator so we wont stop it
        coord = await get_topology_coordinator(manager)
        coord_serv = await manager.find_server_by_host_id(servers, coord)

        # make sure that our replicas are still in a good shape and paired correctly
        base_replicas, view_replicas, base_by_rack, view_by_rack = await wait_for(read_disjoint_paired_replicas, time.time() + scale_timeout(60))
        logger.info(f"{ks}.test replicas: {base_replicas}")
        logger.info(f"{ks}.tv replicas: {view_replicas}")
        server_to_replace, server_to_down = select_non_coordinator_replica_pair(base_by_rack, view_by_rack, coord_serv)

        logger.info('Downing a node to be replaced')
        # convict=False to avoid triggering SCYLLADB-1996
        await manager.server_stop(server_to_replace.server_id, convict=False)
        await manager.others_not_see_server(server_to_replace.ip_addr)

        logger.info('Blocking tablet rebuild')
        await manager.api.enable_injection(coord_serv.ip_addr, "tablet_transition_updates", one_shot=True)

        logger.info('Replacing the node')
        replace_cfg = ReplaceConfig(replaced_id = server_to_replace.server_id, reuse_ip_addr = False, use_host_id = True)
        replace_task = asyncio.create_task(manager.server_add(replace_cfg, property_file={
            "dc": server_to_replace.datacenter,
            "rack": server_to_replace.rack
        }))

        await manager.api.wait_for_injection_enter(coord_serv.ip_addr, "tablet_transition_updates")

        await manager.server_stop(server_to_down.server_id, convict=True)
        await manager.others_not_see_server(server_to_down.ip_addr)

        # The update is supposed to go to the second replica only, since the other one is downed.
        # If pairing would shift, the update to the view would be lost because the first replica
        # is the one which is in the left state.
        logger.info('Updating base table')
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, c) VALUES (3, 4)", consistency_level=ConsistencyLevel.ONE))
        logger.info('Querying the view')
        assert [(4,3)] == list(await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.tv WHERE c=4", consistency_level=ConsistencyLevel.ONE)))
        
        assert server_to_down.server_id != server_to_replace.server_id, f"server to down and server to replace cant be the same id {server_to_down.server_id}"
        await manager.server_start(server_to_down.server_id)
        await manager.servers_see_each_other(await manager.running_servers())

        logger.info('Unblocking tablet rebuild')
        await manager.api.message_injection(coord_serv.ip_addr, "tablet_transition_updates")

        logger.info('Waiting for replace')
        await replace_task
        await manager.servers_see_each_other(await manager.running_servers())

        logger.info('Querying')
        assert [(4,3)] == list(await cql.run_async(f"SELECT * FROM {ks}.tv WHERE c=4"))
