#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from pathlib import Path

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.random_tables import RandomTables, Column, IntType, CounterType
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, wait_for
from cassandra import WriteFailure, ConsistencyLevel
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import ScyllaMetrics
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement
from test.cluster.util import new_test_keyspace, get_topology_version, get_coordinator_host, get_non_coordinator_host
from test.pylib.scylla_server import ScyllaVersionDescription
from test.pylib.tablets import get_all_tablet_replicas
import pytest
import logging
import time
import asyncio
import os
import random


logger = logging.getLogger(__name__)


def host_by_server(hosts: list[Host], srv: ServerInfo):
    for h in hosts:
        if h.address == srv.ip_addr:
            return h
    raise ValueError(f"can't find host for server {srv}")


async def set_version(manager: ScyllaClusterManager, host: Host, new_version: int):
    await manager.cql.run_async("update system.topology set version=%s where key = 'topology'",
                                parameters=[new_version],
                                host=host)


async def set_fence_version(manager: ScyllaClusterManager, host: Host, new_version: int):
    await manager.cql.run_async("update system.topology set fence_version=%s where key = 'topology'",
                                parameters=[new_version],
                                host=host)


def send_errors_metric(metrics: ScyllaMetrics):
    return metrics.get('scylla_hints_manager_send_errors')


def sent_total_metric(metrics: ScyllaMetrics):
    return metrics.get('scylla_hints_manager_sent_total')


def all_hints_metrics(metrics: ScyllaMetrics) -> list[str]:
    return metrics.lines_by_prefix('scylla_hints_manager_')


# The fence version advanced by global_token_metadata_barrier, and the plain barrier which
# follows it. 'barrier' is a prefix of 'barrier_and_drain', hence the trailing comma.
FENCE_ADVANCED = r"updating topology state: advance fence version to (\d+)"
BARRIER_SENT = "executing global topology command barrier,"
COORDINATOR_BARRIER_EVENTS = f"{FENCE_ADVANCED}|{BARRIER_SENT}"
# exec_global_command_helper doesn't say which plain barrier failed, the events
# preceding it in the log do.
BARRIER_FAILED = r"raft topology: exec_global_command\(barrier\) failed"

# The replica side: the fence being applied on a shard (logged per shard) and the point
# at which the barrier handler is done waiting for the group0 read barrier.
FENCE_APPLIED = r"update_fence_version: new fence_version (\d+) is set"
BARRIER_READ_BARRIER_COMPLETED = r"topology cmd rpc barrier index=\d+: read_barrier completed"
REPLICA_BARRIER_EVENTS = f"{FENCE_APPLIED}|{BARRIER_READ_BARRIER_COMPLETED}"

SINGLE_TABLET_KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                         "AND tablets = {'initial': 1}")


def barrier_follows_fence(events, version: int) -> bool:
    """events are the grep results of one of the *_BARRIER_EVENTS patterns: a fence version
    message captures the version, a barrier message captures nothing. Check that a barrier
    message follows the last message of version, before the first one of a later version."""
    at = [i for i, (_, m) in enumerate(events) if m.group(1) == str(version)]
    later = [i for i, (_, m) in enumerate(events) if m.group(1) is not None and int(m.group(1)) > version]
    return bool(at) and any(m.group(1) is None for _, m in events[max(at) + 1:min(later, default=len(events))])


async def start_tablets_cluster(manager: ScyllaClusterManager) -> list[ServerInfo]:
    """Three nodes with tablets enabled and the tablet load balancer disabled,
    so the only topology operations are the ones the test drives itself."""
    servers = await manager.servers_add(3, config={'tablets_mode_for_new_keyspaces': 'enabled'})
    await manager.disable_tablet_balancing()
    return servers


async def pick_tablet_move(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str):
    """Return (src_host, src_shard, dst_host, token) which move the single tablet
    of ks.test to a node which doesn't hold it yet."""
    tablets = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
    assert len(tablets) == 1 and len(tablets[0].replicas) == 1, f"expected a single tablet replica, got {tablets}"
    src_host, src_shard = tablets[0].replicas[0]
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    dst_host = next(h for h in host_ids if h != src_host)
    return src_host, src_shard, dst_host, tablets[0].last_token


async def test_fence_version_applied_before_barrier_returns(manager: ScyllaClusterManager):
    """
    Reproducer for SCYLLADB-3375: global_token_metadata_barrier returned right after
    committing fence_version := version, without waiting for the other nodes to apply
    it, so they could still admit requests with the previous version. It now always
    runs a plain barrier after the commit, so by the time it returns every non-excluded
    node has applied the new fence version on all of its shards.

    Over the window of a single tablet migration (which goes through
    global_tablet_token_metadata_barrier, i.e. drain_all_nodes=true) check that
    * the coordinator sends a barrier after every fence version it advances,
    * every node replies to a barrier after applying each of those fence versions.
    Both checks key on the fence version, not on the position of a message in the log,
    so neither a plain barrier outside of a fence round nor a retried fence commit
    can shift them.
    """
    servers = await start_tablets_cluster(manager)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, SINGLE_TABLET_KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        src_host, src_shard, dst_host, token = await pick_tablet_move(manager, servers, ks)
        coordinator = await get_coordinator_host(manager)

        logs = {s.server_id: await manager.server_open_log(s.server_id) for s in servers}
        marks = {s.server_id: await logs[s.server_id].mark() for s in servers}

        logger.info(f"Moving the tablet of {ks}.test from {src_host} to {dst_host}")
        await manager.api.move_tablet(coordinator.ip_addr, ks, "test", src_host, src_shard, dst_host, 0, token)

        coordinator_log = logs[coordinator.server_id]
        coordinator_mark = marks[coordinator.server_id]

        # The predicates raise rather than return None, so that wait_for reports
        # the unsatisfied fence version when it times out.
        async def barrier_sent_after_each_fence():
            events = await coordinator_log.grep(COORDINATOR_BARRIER_EVENTS, from_mark=coordinator_mark)
            versions = sorted({int(m.group(1)) for _, m in events if m.group(1) is not None})
            assert versions, "the tablet migration advanced no fence version"
            for version in versions:
                assert barrier_follows_fence(events, version), \
                    f"the coordinator sent no barrier after advancing fence version {version}"
            return versions

        fence_versions = await wait_for(barrier_sent_after_each_fence, time.time() + 60)
        logger.info(f"Fence versions advanced during the migration: {fence_versions}")

        for s in servers:
            log, mark = logs[s.server_id], marks[s.server_id]

            async def barrier_replied_after_each_fence():
                events = await log.grep(REPLICA_BARRIER_EVENTS, from_mark=mark)
                for version in fence_versions:
                    assert barrier_follows_fence(events, version), \
                        f"{s.server_id} replied to no barrier after applying fence version {version}"
                return True

            await wait_for(barrier_replied_after_each_fence, time.time() + 60)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_barrier_after_fence_failure_is_not_fatal(manager: ScyllaClusterManager):
    """
    SCYLLADB-3375: the barrier which follows the fence commit is a new failure point
    on the success path of a tablet operation: before it was made unconditional no plain
    barrier ran there at all. Fail it once on a non-coordinator replica and check that
    it is that barrier which fails, and that the migration still completes.
    """
    servers = await start_tablets_cluster(manager)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, SINGLE_TABLET_KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        src_host, src_shard, dst_host, token = await pick_tablet_move(manager, servers, ks)
        coordinator = await get_coordinator_host(manager)
        replica = await get_non_coordinator_host(manager)
        assert replica is not None

        # The one shot injection is consumed by any plain barrier, including the one the
        # coordinator runs before it enables features. Wait until there is nothing left to enable.
        host_ids = {await manager.get_host_id(s.server_id) for s in servers}
        [coordinator_cql_host] = await wait_for_cql_and_get_hosts(cql, [coordinator], time.time() + 60)

        async def features_enabled():
            rows = await cql.run_async("select host_id, supported_features, enabled_features from system.topology",
                                       host=coordinator_cql_host)
            live = [r for r in rows if str(r.host_id) in host_ids]
            supported = frozenset.intersection(*(frozenset(r.supported_features) for r in live))
            return True if frozenset(live[0].enabled_features or []) == supported else None

        await wait_for(features_enabled, time.time() + 60)

        coordinator_log = await manager.server_open_log(coordinator.server_id)
        coordinator_mark = await coordinator_log.mark()

        logger.info(f"Enabling 'raft_topology_barrier_fail' injection on {replica.ip_addr}")
        await manager.api.enable_injection(replica.ip_addr, 'raft_topology_barrier_fail', True)

        logger.info(f"Moving the tablet of {ks}.test from {src_host} to {dst_host}")
        await manager.api.move_tablet(coordinator.ip_addr, ks, "test", src_host, src_shard, dst_host, 0, token)

        await coordinator_log.wait_for(BARRIER_FAILED, from_mark=coordinator_mark, timeout=60)
        events = [m.group(0) for _, m in await coordinator_log.grep(f"{COORDINATOR_BARRIER_EVENTS}|{BARRIER_FAILED}",
                                                                    from_mark=coordinator_mark)]
        failed = next(i for i, e in enumerate(events) if e.startswith("raft topology: exec_global_command(barrier) failed"))
        assert failed >= 2 and events[failed - 1] == BARRIER_SENT \
            and events[failed - 2].startswith("updating topology state: advance fence version to "), \
            f"the failed barrier is not the one which follows a fence commit: {events}"

        tablets = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert tablets[0].replicas == [(dst_host, 0)], f"the tablet was not migrated: {tablets}"


@pytest.mark.parametrize("tablets_enabled", [True, False])
async def test_fence_writes(request, manager: ScyllaClusterManager, tablets_enabled: bool):
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
    await manager.disable_tablet_balancing()

    logger.info('Creating new tables')
    random_tables = RandomTables(request.node.name, manager, unique_name(), 3)
    await random_tables.add_table(name='t1', pks=1, columns=[
        Column("pk", IntType),
        Column('int_c', IntType)
    ])
    await random_tables.add_table(name='t2', pks=1, columns=[
        Column("pk", IntType),
        Column('counter_c', CounterType)
    ])
    cql = manager.get_cql()
    await cql.run_async(f"USE {random_tables.keyspace}")

    logger.info('Waiting for cql and hosts')
    host2 = (await wait_for_cql_and_get_hosts(cql, [servers[2]], time.time() + 60))[0]

    # Run cleanup_all so the global barrier distributes fence_version := version to all nodes.
    # It must be invoked on the same host where version and fence_version are decremented.
    # Otherwise, cleanup_all might finish before all group0 state updates are applied on host2,
    # causing topology_state_load on it to see the decremented version and report broken invariants.
    await manager.api.cleanup_all(servers[2].ip_addr)

    version = await get_topology_version(cql, host2)
    logger.info(f"version on host2 {version}")

    await set_version(manager, host2, version - 1)
    logger.info(f"set version on host2 to {version - 1}")
    await set_fence_version(manager, host2, version - 1)
    logger.info(f"set fence version on host2 to {version - 1}")

    await manager.server_restart(servers[2].server_id, wait_others=2)
    logger.info("host2 restarted")

    host2 = (await wait_for_cql_and_get_hosts(cql, [servers[2]], time.time() + 60))[0]

    logger.info(f"trying to write through host2 to regular column [{host2}]")
    with pytest.raises(WriteFailure, match="stale topology exception"):
        await cql.run_async("insert into t1(pk, int_c) values (1, 1)", host=host2)

    logger.info(f"trying to write through host2 to counter column [{host2}]")
    with pytest.raises(WriteFailure, match="stale topology exception"):
        await cql.run_async("update t2 set counter_c=counter_c+1 where pk=1", host=host2)

    random_tables.drop_all()


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_fence_hints(request, manager: ScyllaClusterManager):
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
    await manager.disable_tablet_balancing()

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
    new_version = (await get_topology_version(cql, host2)) + 1
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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_fence_lwt_during_bootstap(manager: ScyllaClusterManager):
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

        logger.info("Enable topology_coordinator/write_both_read_old/before_version_increment injection on s0")
        await manager.api.enable_injection(servers[0].ip_addr,
                                           'topology_coordinator/write_both_read_old/before_version_increment', 
                                           one_shot=True)

        logger.info("Start bootstrapping a new server")
        s3_start_task = asyncio.create_task(manager.server_start(servers[3].server_id))

        logger.info(f"Waiting for 'topology_coordinator/write_both_read_old/before_version_increment' injection on {servers[0]}")
        await manager.api.wait_for_injection_enter(servers[0].ip_addr, "topology_coordinator/write_both_read_old/before_version_increment")

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
                # Internal requests of the stale node, like TTL scans, are fenced out too, but in other scheduling groups.
                # We don't want them counted here.
                result += m.get(metric_name, {'scheduling_group_name': 'sl:default'}) or 0
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


@pytest.mark.skip_mode(mode='release', reason='dev mode is enough for this test')
@pytest.mark.skip_mode(mode='debug', reason='dev mode is enough for this test')
async def test_lwt_fencing_upgrade(manager: ScyllaClusterManager, scylla_2025_1: ScyllaVersionDescription, scylla_binary: Path):
    """
    The test runs some LWT workload on a vnodes-based table, rolling-restarts nodes
    with a new Scylla version and checks that LWTs complete as expected. Downgrading
    a single node back to original version is also covered.
    """

    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3,
                                        cmdline=[
                                            '--logger-log-level', 'paxos=trace'
                                        ],
                                        config={
                                            'tablets_mode_for_new_keyspaces': 'disabled'
                                        },
                                        auto_rack_dc='dc1',
                                        version=scylla_2025_1)
    (cql, _) = await manager.get_ready_cql(servers)

    logger.info("Create a test keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        logger.info("Create test table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1)")

        update_stmt = cql.prepare(f"UPDATE {ks}.test SET c = ? WHERE pk = 1 IF c = ?")
        stop = False
        cond = asyncio.Condition()
        # Pause the LWT workload while a node is restarted and until gossip converges.
        # Otherwise, a read can choose a replica just before it is marked down and fail
        # instead of using another live replica. See SCYLLADB-2824.
        lwt_lock = asyncio.Lock()
        lwt_counter = 1
        async def lwt_workload():
            nonlocal lwt_counter
            while not stop:
                async with lwt_lock:
                    result = await cql.run_async(update_stmt, [lwt_counter + 1, lwt_counter])

                # The driver may retry the statement, so 'applied' can be false here.
                # applied == true  -> 'c' holds the previous value  -> lwt_counter
                # applied == false -> 'c' holds the new value       -> lwt_counter + 1
                assert result[0] in ((True, lwt_counter), (False, lwt_counter + 1))

                async with cond:
                    lwt_counter += 1
                    cond.notify_all()
                await asyncio.sleep(random.random() / 100)
        async def wait_for_some_lwts():
            nonlocal lwt_counter
            if lwt_workload_task.done():
                e = lwt_workload_task.exception()
                raise e if e is not None else RuntimeError(
                    'unexpected lwt_workload_task state')
            async with cond:
                start = lwt_counter
                await cond.wait_for(lambda: lwt_counter - start >= 10)

        async def change_version_and_wait(s: ServerInfo, scylla_path: str):
            async with lwt_lock:
                await manager.server_change_version(s.server_id, scylla_path)
                await manager.server_sees_others(s.server_id, 2, interval=60.0)

        logger.info("LWT workoad started")
        lwt_workload_task = asyncio.create_task(lwt_workload())
        await wait_for_some_lwts()

        logger.info(f"Upgrading {servers[0].server_id}")
        await change_version_and_wait(servers[0], str(scylla_binary))
        await wait_for_some_lwts()

        logger.info(f"Downgrading {servers[0].server_id}")
        await change_version_and_wait(servers[0], scylla_2025_1.path)
        await wait_for_some_lwts()

        for s in servers:
            # Ensure all hosts are alive before restarting the last server,
            # so the LWT workload doesn’t fail if the driver suddenly sees all nodes as “down”.
            if s == servers[-1]:
                logger.info("Wait all nodes are up")
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            logger.info(f"Upgrading {s.server_id}")
            await change_version_and_wait(s, str(scylla_binary))

        logger.info("Done upgrading servers")

        await wait_for_some_lwts()

        stop = True
        await lwt_workload_task
        assert lwt_counter >= 40, f"unexpected counter value: {lwt_counter}"

        logger.info(f"Done, number of successfull LWTs: {lwt_counter}")
