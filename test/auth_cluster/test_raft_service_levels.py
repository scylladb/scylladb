#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import time
import asyncio
import logging
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, read_barrier
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.topology.util import trigger_snapshot, wait_until_topology_upgrade_finishes
from test.topology.conftest import skip_mode
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_service_levels_snapshot(manager: ManagerClient):
    """
        Cluster with 3 nodes.
        Add 10 service levels. Start new server and it should get a snapshot on bootstrap.
        Stop 3 `old` servers and query the new server to validete if it has the same service levels.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)

    sls = ["sl" + unique_name() for _ in range(10)]
    for sl in sls:
        await cql.run_async(f"CREATE SERVICE LEVEL {sl}")

    # we don't know who the leader is, so trigger the snapshot on all nodes
    for server in servers:
        await trigger_snapshot(manager, server)

    host0 = cql.cluster.metadata.get_host(servers[0].ip_addr)
    result = await cql.run_async("SELECT service_level FROM system.service_levels_v2", host=host0)

    new_server = await manager.server_add()
    all_servers = servers + [new_server]
    await wait_for_cql_and_get_hosts(cql, all_servers, time.time() + 60)
    await manager.servers_see_each_other(all_servers)

    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id)
                           for s in servers])

    await manager.driver_connect(server=new_server)
    cql = manager.get_cql()
    new_result = await cql.run_async("SELECT service_level FROM system.service_levels_v2")

    assert set([sl.service_level for sl in result]) == set([sl.service_level for sl in new_result])

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_service_levels_upgrade(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode due to the error injection
    cfg = {'error_injections_at_startup': ['force_gossip_based_join']}

    servers = [await manager.server_add(config=cfg)]
    # Disable injections for the subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['error_injections_at_startup']

    servers += [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.get_cql()
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    sls = ["sl" + unique_name() for _ in range(10)]
    for sl in sls:
        await cql.run_async(f"CREATE SERVICE LEVEL {sl}")

    result = await cql.run_async("SELECT service_level FROM system_distributed.service_levels")
    assert set([sl.service_level for sl in result]) == set(sls)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    result_v2 = await cql.run_async("SELECT service_level FROM system.service_levels_v2")
    assert set([sl.service_level for sl in result_v2]) == set(sls)

    sl_v2 = "sl" + unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl_v2}")

    await asyncio.gather(*(read_barrier(cql, host) for host in hosts))
    result_with_sl_v2 = await cql.run_async(f"SELECT service_level FROM system.service_levels_v2")
    assert set([sl.service_level for sl in result_with_sl_v2]) == set(sls + [sl_v2])

    

