#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.test_tablets2 import safe_rolling_restart
from test.cluster.util import new_test_keyspace, wait_for_cql_and_get_hosts
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import read_barrier
from test.pylib.tablets import get_all_tablet_replicas

logger = logging.getLogger(__name__)


async def local_partition_count(manager: ScyllaClusterManager, cql, server, ks: str) -> int:
    host = (await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30))[0]
    await read_barrier(manager.api, host.address)
    rows = await cql.run_async(f"SELECT pk FROM MUTATION_FRAGMENTS({ks}.test)", host=host)
    return len({r.pk for r in rows})


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("queued_filtered_repair", [False, True])
async def test_queued_filtered_repair_does_not_narrow_rebuild_repair(manager: ScyllaClusterManager, queued_filtered_repair: bool):
    """
    A queued user repair request carrying a DC filter must not restrict the
    replica set of the repair phase of a tablet rebuild (rebuild_v2).

    dc1 holds one replica, dc2 the other. Rows written while the dc1 replica is
    down exist only in dc2. A dc1-only repair request is queued but kept from
    running. Adding a second dc1 replica triggers rebuild_v2: repair among the
    survivors, then stream to the new replica. Every replica must end up with
    every row. The variant without the queued request is the control.
    """
    cfg = {'enable_tablets': True, 'hinted_handoff_enabled': False}
    # One rack per DC: the suite enforces rack-valid keyspaces, so RF 1 needs a single rack.
    dc1 = [await manager.server_add(config=cfg, property_file={'dc': 'dc1', 'rack': 'r1'}) for _ in range(2)]
    dc2 = [await manager.server_add(config=cfg, property_file={'dc': 'dc2', 'rack': 'r1'})]
    servers = dc1 + dc2
    host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")

        async def insert(keys, cl):
            await asyncio.gather(*[cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})", consistency_level=cl)) for k in keys])

        # CL=ALL so both replicas hold these rows before the dc1 replica is stopped.
        await insert(range(100), ConsistencyLevel.ALL)

        replica_hosts = [r[0] for r in (await get_all_tablet_replicas(manager, servers[0], ks, 'test'))[0].replicas]
        dc1_replica = next(s for s in dc1 if host_ids[s.server_id] in replica_hosts)
        new_replica = next(s for s in dc1 if s is not dc1_replica)
        logger.info(f"dc1 replica: {dc1_replica}, new dc1 replica: {new_replica}, dc2 replica: {dc2[0]}")

        # CL=ONE: the driver's LOCAL_ONE would need the (down) dc1 replica.
        async def insert_while_down(_):
            await insert(range(100, 200), ConsistencyLevel.ONE)

        cql = await safe_rolling_restart(manager, [dc1_replica], with_down=insert_while_down)
        assert await local_partition_count(manager, cql, dc1_replica, ks) == 100
        assert await local_partition_count(manager, cql, dc2[0], ks) == 200

        if queued_filtered_repair:
            # Queue a dc1-only user repair and keep the scheduler from running it.
            await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, 'tablet_repair_skip_sched', False, {'value': '0'}) for s in servers])
            await manager.api.tablet_repair(servers[0].ip_addr, ks, 'test', 'all', dcs_filter='dc1', await_completion=False)

        # Extend RF into dc1: rebuild_v2 repairs among the survivors, then streams to the new replica.
        # A second dc1 replica breaks the keyspace's RF, hence force=True.
        await manager.api.add_tablet_replica(servers[0].ip_addr, ks, 'test', host_ids[new_replica.server_id], 0, 0, force=True)

        for s in servers:
            count = await local_partition_count(manager, cql, s, ks)
            logger.info(f"{s} holds {count} partitions after rebuild")
            assert count == 200, f"{s} is missing partitions after rebuild"
