#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, IntType, CounterType
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, wait_for
from cassandra import WriteFailure, ConsistencyLevel
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import ScyllaMetrics
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace, reconnect_driver
import pytest
import logging
import time
import asyncio


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
    cfg = {'tablets_mode_for_new_keyspaces' : 'enabled' if tablets_enabled else 'disabled'}

    logger.info("Bootstrapping first two nodes")
    servers = await manager.servers_add(2, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"}
    ])

    # The third node is started as the last one, so we can be sure that is has
    # the latest topology version
    logger.info("Bootstrapping the last node")
    servers += [await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": "r3"})]

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
    s0 = await manager.server_add(
        config={'error_injections_at_startup': ['decrease_hints_flush_period']},
        cmdline=['--logger-log-level', 'hints_manager=trace'],
        property_file={"dc": "dc1", "rack": "r1"})

    # Disable load balancer as it might bump topology version, potentially creating a race condition
    # with read modify write below.
    # This should be done before adding the last two servers,
    # otherwise it can break the version == fence_version condition
    # which the test relies on.
    await manager.api.disable_tablet_balancing(s0.ip_addr)

    [s1, s2] = await manager.servers_add(2, property_file=[
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"}
    ])

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


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_fence_lwt_during_bootstap(manager: ManagerClient):
    """
    Scenario:
    1. Three nodes s0, s1 and s2 in a cluster, s0 is a topology coordinator, test table with rf=3
    2. Set injection topology_coordinator/write_both_read_old/before_version_increment on s0
    3. Start bootstrapping a new node s3
    4. When topology_coordinator/write_both_read_old/before_version_increment is reached,
       inject topology_state_load_error into s1. This means that from now on
       s1 won't be able to apply any topology updates, including version increments.
       The group0 on s1 will be aborted, all barriers will throw.
    5. Wait s3 is started successfully.
    6. Run LWT with the coordinator on s1.
    7. Check that s1 is fenced out.
    """
    config = {
        'tablets_mode_for_new_keyspaces': 'disabled',
        'ring_delay_ms': 10  # To avoid waiting in topology_coordinator/write_both_read_new
    }
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    property_file = {"dc": "dc1", "rack": "r1"}

    # The first node is a topology_coordinator
    logger.info("Bootstrapping the first node")
    servers = [await manager.server_add(property_file=property_file, config=config, cmdline=cmdline)]

    logger.info("Bootstrapping the second and third nodes")
    servers += await manager.servers_add(2, property_file=property_file, config=config, cmdline=cmdline)

    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Create a test keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        logger.info("Create test table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Add the fourth server")
        servers += [await manager.server_add(property_file=property_file,
                                             config=config,
                                             cmdline=cmdline,
                                             start=False)]

        logger.info("Open s0 log")
        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        logger.info("Enable topology_coordinator/write_both_read_old/before_version_increment injection on s0")
        await manager.api.enable_injection(servers[0].ip_addr,
                                           'topology_coordinator/write_both_read_old/before_version_increment', 
                                           one_shot=True)

        logger.info("Start bootstrapping a new server")
        s3_start_task = asyncio.create_task(manager.server_start(servers[3].server_id))

        logger.info(f"Waiting for 'topology_coordinator/write_both_read_old/before_version_increment' injection on {servers[0]}")
        await s0_log.wait_for('topology_coordinator/write_both_read_old/before_version_increment: waiting for message', from_mark=s0_mark)

        logger.info(f"Injecting 'topology_state_load_error' into {servers[1]}")
        await manager.api.enable_injection(servers[1].ip_addr, 'topology_state_load_error', one_shot=False)

        logger.info(f"Release 'topology_coordinator/write_both_read_old/before_version_increment' on {servers[0]}")
        await manager.api.message_injection(servers[0].ip_addr, "topology_coordinator/write_both_read_old/before_version_increment")

        logger.info(f"Waiting for {servers[3]} to finish bootstrapping")
        await s3_start_task

        logger.info("Waiting for get_ready_cql")
        (cql, hosts) = await manager.get_ready_cql(servers)

        logger.info(f"Running an LWT INSERT on a stale {hosts[1]} node")

        async def fenced_out_requests():
            metrics = await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
            metric_name = 'scylla_storage_proxy_replica_fenced_out_requests'
            result = 0
            for m in metrics:
                result += m.get(metric_name) or 0
            return result

        assert await fenced_out_requests() == 0
        with pytest.raises(WriteFailure, match="stale topology exception"):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS", host=hosts[1])
        # Node2 still sees node4 in a bootstrapping state because its group0 is broken.
        # As a result, it uses an 'extended quorum' with 4 replicas, requiring 3 responses to reach quorum.
        # An LWT fails as soon as it is certain that the quorum is unreachable. In this case, at least
        # 2 failed responses are required to determine failure. Since we send prepare RPCs to all 4 replicas,
        # the fourth replica may also return a failure, making the possible outcomes 2 or 3 failures.
        assert await fenced_out_requests() in (2, 3)

        logger.info(f"Restart {servers[1].ip_addr}")
        await manager.server_restart(servers[1].server_id)

        # We reconnect to the second node to force the driver's control connection to use it.
        # This is required to verify that we do not get stuck in the following scenario:
        #
        # 1. Before restart, the second node observed the fourth node in a bootstrapping state.
        #    As a result, after restart, it has a record in the system.peers table for the fourth node
        #    that contains only its IP and host_id. The driver skips such incomplete records,
        #    but they are necessary to handle the case where a bootstrapping node restarts
        #    while the bootstrap process is still in progress (see issue #18927 for details).
        #
        # 2. The gossiper.add_saved_endpoint method is called for the fourth node,
        #    while the second node is starting up, but without DC and rack information.
        #
        # 3. While the second node is catching up with the latest Raft state, topology_state_load
        #    is invoked, which in turn calls raft_topology_update_ip. At this point, the fourth node
        #    is already in the 'normal' state, but update_peer_info is not called because gossiper
        #    does not yet have any information for the fourth node. Consequently,
        #    get_gossiper_peer_info_for_update returns empty.
        #
        # 4. Later, the gossiper component on the second node synchronizes with the rest of the cluster,
        #    retrieves complete information about the fourth node, and one of the ip_address_updater
        #    methods is invoked. However, the IP address of the fourth node in gossiper matches
        #    the address already stored in the peers table, so raft_topology_update_ip call is skipped.
        #
        # 5. As a result, nobody updates the system.peers entry for the fourth node, leaving it with only
        #    the IP and host_id columns populated. Consequently, the test fails in
        #    get_ready_cql/wait_for_cql_and_get_hosts because it waits for all nodes to appear in
        #    cluster.metadata.all_hosts(). Node4 is skipped because its system.peers record on node2
        #    is considered invalid by the driver.
        manager.driver_close()
        await manager.driver_connect(servers[1])
        (cql, hosts) = await manager.get_ready_cql(servers)

        logger.info(f"Running an LWT INSERT on an up-to-date {hosts[1]} node")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 2) IF NOT EXISTS", host=hosts[1])

        logger.info(f"Run paxos SELECT on {hosts[0]} node")
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = 1",
                                                   consistency_level=ConsistencyLevel.SERIAL),
                                   host=hosts[0])
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 1
        assert row.c == 2
