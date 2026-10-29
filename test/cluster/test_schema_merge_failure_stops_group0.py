#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time

import pytest
from cassandra import ConsistencyLevel, WriteFailure
from cassandra.cluster import Session
from cassandra.pool import Host
from cassandra.query import SimpleStatement

from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

GROUP0_STATUS_ABORTED = 2


async def wait_for_group0_aborted(manager: ScyllaClusterManager, server: ServerInfo) -> None:
    async def aborted():
        try:
            metrics = await manager.metrics.query(server.ip_addr)
        except Exception:
            return None
        return True if metrics.get("scylla_raft_group0_status") == GROUP0_STATUS_ABORTED else None
    await wait_for(aborted, time.time() + 60)


async def get_system_distributed_state(cql: Session, host: Host) -> tuple:
    replication = (await cql.run_async(
        "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'system_distributed'",
        host=host))[0].replication
    versions = {r.table_name: r.version for r in await cql.run_async(
        "SELECT table_name, version FROM system_schema.scylla_tables WHERE keyspace_name = 'system_distributed'",
        host=host)}
    return replication, versions


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_joining_node_stops_group0_on_schema_merge_failure(manager: ScyllaClusterManager) -> None:
    """Verify that a failure in schema_applier::update() after the schema
    mutations are persisted stops group 0 on a joining node.

    Reproducer for SCYLLADB-5770:
    1. A joining node applies the group0 snapshot; the schema is persisted,
       but building the in-memory schema fails.
    2. Raft resends the snapshot. The second merge diffs persisted state against
       persisted state, finds nothing to do and succeeds, so the node continues
       with an in-memory schema that lacks system_distributed.
    3. The node announces creation of system_distributed with default
       replication, which overwrites the replication of the existing keyspace
       on the whole cluster.

    With the fix the joining node stops its group 0 raft server on the first
    failure and fails to start, and the replication and table schema versions
    of system_distributed on the existing nodes are left intact.
    """
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    dc = (await cql.run_async("SELECT data_center FROM system.local"))[0].data_center
    await cql.run_async("ALTER KEYSPACE system_distributed WITH replication = "
                        f"{{'class': 'NetworkTopologyStrategy', '{dc}': 2}}")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    for s in servers:
        await read_barrier(manager.api, s.ip_addr)
    states = [await get_system_distributed_state(cql, host) for host in hosts]
    logger.info("system_distributed state before join: %s", states)

    await manager.server_add(config={'error_injections_at_startup': [{'name': 'schema_applier_update_fail', 'one_shot': True}]},
                             expected_error="schema mutations were persisted, but the in-memory schema was not updated")

    for host, state in zip(hosts, states):
        assert await get_system_distributed_state(cql, host) == state


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_node_with_stopped_group0_fails_new_schema_requests_fast(manager: ScyllaClusterManager) -> None:
    """Verify that a node that failed to update its in-memory schema while
    applying a group 0 command stops group 0 and fails requests that use the
    newer schema version immediately.

    Such a request makes the replica sync its schema through a group 0 read
    barrier. With group 0 stopped the barrier fails right away instead of
    waiting for the request to time out, while gossip still reports the node
    as UP.

    The test also verifies that restarting the node brings it back.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        failed = servers[2]
        await manager.api.enable_injection(failed.ip_addr, "schema_applier_update_fail", one_shot=True)
        await cql.run_async(f"ALTER TABLE {ks}.t ADD w int", host=hosts[0])
        await wait_for_group0_aborted(manager, failed)
        for s in servers[:2]:
            await read_barrier(manager.api, s.ip_addr)

        def insert(pk: int, cl: ConsistencyLevel) -> SimpleStatement:
            return SimpleStatement(f"INSERT INTO {ks}.t (pk, v, w) VALUES ({pk}, {pk}, {pk})", consistency_level=cl)

        await cql.run_async(insert(1, ConsistencyLevel.QUORUM), host=hosts[0])

        start = time.time()
        with pytest.raises(WriteFailure):
            await cql.run_async(insert(2, ConsistencyLevel.ALL), host=hosts[0])
        elapsed = time.time() - start
        logger.info("CL=ALL write failed after %.2fs", elapsed)
        assert elapsed < 10

        await manager.server_stop_gracefully(failed.server_id)
        await manager.server_start(failed.server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await cql.run_async(insert(3, ConsistencyLevel.ALL), host=hosts[0])
