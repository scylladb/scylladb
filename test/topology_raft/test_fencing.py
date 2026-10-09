#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, IntType, CounterType
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, wait_for
from cassandra import WriteFailure, ConsistencyLevel
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import ScyllaMetrics
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement
from test.topology.conftest import skip_mode
import pytest
import logging
import time


logger = logging.getLogger(__name__)


def host_by_server(hosts: list[Host], srv: ServerInfo):
    for h in hosts:
        if h.address == srv.ip_addr:
            return h
    raise ValueError(f"can't find host for server {srv}")


async def set_version(manager: ManagerClient, host: Host, new_version: int):
    await manager.cql.run_async("update system.topology set version=%s where key = 'topology'",
                                parameters=[new_version],
                                host=host)


async def set_fence_version(manager: ManagerClient, host: Host, new_version: int):
    await manager.cql.run_async("update system.topology set fence_version=%s where key = 'topology'",
                                parameters=[new_version],
                                host=host)


async def get_version(manager: ManagerClient, host: Host):
    rows = await manager.cql.run_async(
        "select version from system.topology where key = 'topology'",
        host=host)
    return rows[0].version


def send_errors_metric(metrics: ScyllaMetrics):
    return metrics.get('scylla_hints_manager_send_errors')


def sent_total_metric(metrics: ScyllaMetrics):
    return metrics.get('scylla_hints_manager_sent_total')


def all_hints_metrics(metrics: ScyllaMetrics) -> list[str]:
    return metrics.lines_by_prefix('scylla_hints_manager_')


@pytest.mark.asyncio
@pytest.mark.parametrize("tablets_enabled", [True, False])
async def test_fence_writes(request, manager: ManagerClient, tablets_enabled: bool):
    cfg = {'enable_tablets' : tablets_enabled}

    logger.info("Bootstrapping first two nodes")
    servers = await manager.servers_add(2, config=cfg)

    # The third node is started as the last one, so we can be sure that is has
    # the latest topology version
    logger.info("Bootstrapping the last node")
    servers += [await manager.server_add(config=cfg)]

    # Disable load balancer as it might bump topology version, undoing the decrement below.
    # This should be done before adding the last two servers,
    # otherwise it can break the version == fence_version condition
    # which the test relies on.
    await manager.api.disable_tablet_balancing(servers[2].ip_addr)

    logger.info(f'Creating new tables')
    random_tables = RandomTables(request.node.name, manager, unique_name(), 3)
    table1 = await random_tables.add_table(name='t1', pks=1, columns=[
        Column("pk", IntType),
        Column('int_c', IntType)
    ])
    if not tablets_enabled:  # issue #18180
        table2 = await random_tables.add_table(name='t2', pks=1, columns=[
            Column("pk", IntType),
            Column('counter_c', CounterType)
        ])
    cql = manager.get_cql()
    await cql.run_async(f"USE {random_tables.keyspace}")

    logger.info(f'Waiting for cql and hosts')
    host2 = (await wait_for_cql_and_get_hosts(cql, [servers[2]], time.time() + 60))[0]

    version = await get_version(manager, host2)
    logger.info(f"version on host2 {version}")

    await set_version(manager, host2, version - 1)
    logger.info(f"decremented version on host2")
    await set_fence_version(manager, host2, version - 1)
    logger.info(f"decremented fence version on host2")

    await manager.server_restart(servers[2].server_id, wait_others=2)
    logger.info(f"host2 restarted")

    host2 = (await wait_for_cql_and_get_hosts(cql, [servers[2]], time.time() + 60))[0]

    logger.info(f"trying to write through host2 to regular column [{host2}]")
    with pytest.raises(WriteFailure, match="stale topology exception"):
        await cql.run_async("insert into t1(pk, int_c) values (1, 1)", host=host2)

    if not tablets_enabled:  # issue #18180
        logger.info(f"trying to write through host2 to counter column [{host2}]")
        with pytest.raises(WriteFailure, match="stale topology exception"):
            await cql.run_async("update t2 set counter_c=counter_c+1 where pk=1", host=host2)

    random_tables.drop_all()


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_fence_hints(request, manager: ManagerClient):
    logger.info("Bootstrapping cluster with three nodes")
    s0 = await manager.server_add(config={
        'error_injections_at_startup': ['decrease_hints_flush_period']
    }, cmdline=['--logger-log-level', 'hints_manager=trace'])

    # Disable load balancer as it might bump topology version, potentially creating a race condition
    # with read modify write below.
    # This should be done before adding the last two servers,
    # otherwise it can break the version == fence_version condition
    # which the test relies on.
    await manager.api.disable_tablet_balancing(s0.ip_addr)

    [s1, s2] = await manager.servers_add(2)

    logger.info(f'Creating test table')
    random_tables = RandomTables(request.node.name, manager, unique_name(), 3)
    table1 = await random_tables.add_table(name='t1', pks=1, columns=[
        Column("pk", IntType),
        Column('int_c', IntType)
    ])
    cql = manager.get_cql()
    await cql.run_async(f"USE {random_tables.keyspace}")

    logger.info(f'Waiting for cql and hosts')
    hosts = await wait_for_cql_and_get_hosts(cql, [s0, s2], time.time() + 60)

    host2 = host_by_server(hosts, s2)
    new_version = (await get_version(manager, host2)) + 1
    logger.info(f"Set version and fence_version to {new_version} on node {host2}")
    await set_version(manager, host2, new_version)
    await set_fence_version(manager, host2, new_version)

    select_all_stmt = SimpleStatement("select * from t1", consistency_level=ConsistencyLevel.ONE)
    rows = await cql.run_async(select_all_stmt, host=host2)
    assert len(list(rows)) == 0

    logger.info(f"Stopping node {host2}")
    await manager.server_stop_gracefully(s2.server_id)

    host0 = host_by_server(hosts, s0)
    logger.info(f"Writing through {host0} to regular column")
    await cql.run_async("insert into t1(pk, int_c) values (1, 1)", host=host0)

    logger.info(f"Starting last node {host2}")
    await manager.server_start(s2.server_id)

    logger.info(f"Waiting for failed hints on {host0}")
    async def at_least_one_hint_failed():
        metrics_data = await manager.metrics.query(s0.ip_addr)
        if sent_total_metric(metrics_data) > 0:
            pytest.fail(f"Unexpected successful hints; metrics on {s0}: {all_hints_metrics(metrics_data)}")
        if send_errors_metric(metrics_data) >= 1:
            return True
        logger.info(f"Metrics on {s0}: {all_hints_metrics(metrics_data)}")
    await wait_for(at_least_one_hint_failed, time.time() + 60)

    host2 = (await wait_for_cql_and_get_hosts(cql, [s2], time.time() + 60))[0]

    # Check there is no new data on host2.
    rows = await cql.run_async(select_all_stmt, host=host2)
    assert len(list(rows)) == 0

    logger.info("Updating version on first node")
    await set_version(manager, host0, new_version)
    await set_fence_version(manager, host0, new_version)
    await manager.api.client.post("/storage_service/raft_topology/reload", s0.ip_addr)

    logger.info(f"Waiting for sent hints on {host0}")
    async def exactly_one_hint_sent():
        metrics_data = await manager.metrics.query(s0.ip_addr)
        if sent_total_metric(metrics_data) > 1:
            pytest.fail(f"Unexpected more than 1 successful hints; metrics on {s0}: {all_hints_metrics(metrics_data)}")
        if sent_total_metric(metrics_data) == 1:
            return True
        logger.info(f"Metrics on {s0}: {all_hints_metrics(metrics_data)}")
    await wait_for(exactly_one_hint_sent, time.time() + 60)

    # Check the hint is delivered, and we see the new data on host2
    rows = await cql.run_async(select_all_stmt, host=host2)
    assert len(list(rows)) == 1

    random_tables.drop_all()
