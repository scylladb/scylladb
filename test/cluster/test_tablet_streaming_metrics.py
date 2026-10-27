#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.tablets import get_tablet_replica


logger = logging.getLogger(__name__)


async def _get_tablet_streaming_metrics(manager: ScyllaClusterManager, server, kind: str) -> tuple[int, int]:
    metrics = await manager.metrics.query(server.ip_addr)
    tx = int(metrics.get('scylla_tablet_streaming_total_outgoing_bytes', {'kind': kind}) or 0)
    rx = int(metrics.get('scylla_tablet_streaming_total_incoming_bytes', {'kind': kind}) or 0)
    return tx, rx


async def _run_tablet_migration_metrics_test(manager: ScyllaClusterManager, *, use_logstor: bool) -> None:
    cmdline = ['--smp=1']
    cfg = {}
    create_table_suffix = ''

    if use_logstor:
        cfg = {'experimental_features': ['logstor']}
        create_table_suffix = " WITH storage_engine = 'logstor'"

    s1 = await manager.server_add(cmdline=cmdline, config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
    servers = [s1]
    cql, _ = await manager.get_ready_cql(servers)

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets={'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text){create_table_suffix}")

        nrows = 32
        value = 'x' * (32 * 1024)
        for i in range(nrows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{i}_{value}')")

        if use_logstor:
            await manager.api.logstor_flush(s1.ip_addr)
        else:
            await manager.api.keyspace_flush(s1.ip_addr, ks, 'test')

        s2 = await manager.server_add(cmdline=cmdline, config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
        servers.append(s2)
        cql, _ = await manager.get_ready_cql(servers)

        h1 = await manager.get_host_id(s1.server_id)
        h2 = await manager.get_host_id(s2.server_id)
        tablet_token = 0
        source_host_id, source_shard = await get_tablet_replica(manager, s1, ks, 'test', tablet_token)
        assert source_host_id == h1

        source_before = await _get_tablet_streaming_metrics(manager, s1, 'tablet_migration')
        destination_before = await _get_tablet_streaming_metrics(manager, s2, 'tablet_migration')

        await manager.api.move_tablet(s1.ip_addr, ks, 'test', h1, source_shard, h2, 0, tablet_token)

        source_after = await _get_tablet_streaming_metrics(manager, s1, 'tablet_migration')
        destination_after = await _get_tablet_streaming_metrics(manager, s2, 'tablet_migration')
        source_delta = (source_after[0] - source_before[0], source_after[1] - source_before[1])
        destination_delta = (destination_after[0] - destination_before[0], destination_after[1] - destination_before[1])

        assert source_delta[0] > 0
        assert destination_delta[1] > 0
        assert source_delta[0] == destination_delta[1]
        assert source_delta[1] == 0
        assert destination_delta[0] == 0

        new_replica = await get_tablet_replica(manager, s1, ks, 'test', tablet_token)
        assert new_replica[0] == h2

        for i in range(nrows):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1, f"Expected 1 row for key {i} after tablet migration, but got {len(rows)}"
            assert rows[0].v == f"{i}_{value}", f"Expected value '{i}_{value}' for key {i} after tablet migration, but got {rows[0].v}"


@pytest.mark.asyncio
async def test_tablet_migration_metrics_sstables(manager: ScyllaClusterManager) -> None:
    """
    Verify that regular SSTable-based tablet migration updates the tx/rx byte counters.
    """
    await _run_tablet_migration_metrics_test(manager, use_logstor=False)


@pytest.mark.asyncio
async def test_tablet_migration_metrics_logstor(manager: ScyllaClusterManager) -> None:
    """
    Verify that logstor-based tablet migration updates the tx/rx byte counters.
    """
    await _run_tablet_migration_metrics_test(manager, use_logstor=True)


@pytest.mark.asyncio
async def test_tablet_rebuild_metrics(manager: ScyllaClusterManager) -> None:
    """
    Verify that tablet rebuild updates the rebuild tx/rx byte counters.
    """
    logger.info("Bootstrapping cluster")
    host_ids = []
    servers = []

    async def make_server(rack: str) -> None:
        server = await manager.server_add(property_file={"dc": "dc1", "rack": rack})
        servers.append(server)
        host_ids.append(await manager.get_host_id(server.server_id))

    await make_server("r1")
    await make_server("r1")
    await make_server("r2")

    await manager.disable_tablet_balancing()

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c text)")
        value = 'x' * (32 * 1024)
        for key in range(64):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, '{key}_{value}')")

        await manager.api.keyspace_flush(servers[0].ip_addr, ks, 'test')

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(replicas) == 1 and len(replicas[0].replicas) == 2
        source_replicas = replicas[0].replicas

        target_host_id = None
        for host_id in host_ids:
            if host_id not in [replica[0] for replica in source_replicas]:
                target_host_id = host_id
                break
        assert target_host_id is not None

        metrics_before = {
            server.server_id: await _get_tablet_streaming_metrics(manager, server, 'rebuild')
            for server in servers
        }
        migration_before = {
            server.server_id: await _get_tablet_streaming_metrics(manager, server, 'tablet_migration')
            for server in servers
        }

        await manager.api.add_tablet_replica(servers[0].ip_addr, ks, 'test', target_host_id, 0, 0, force=True)

        metrics_after = {
            server.server_id: await _get_tablet_streaming_metrics(manager, server, 'rebuild')
            for server in servers
        }
        migration_after = {
            server.server_id: await _get_tablet_streaming_metrics(manager, server, 'tablet_migration')
            for server in servers
        }
        assert migration_after == migration_before

        source_tx = 0
        target_rx = 0
        for server in servers:
            before_tx, before_rx = metrics_before[server.server_id]
            after_tx, after_rx = metrics_after[server.server_id]
            delta_tx = after_tx - before_tx
            delta_rx = after_rx - before_rx
            server_host_id = host_ids[servers.index(server)]

            if server_host_id == target_host_id:
                target_rx = delta_rx
                assert delta_tx == 0
            else:
                source_tx += delta_tx

        assert source_tx > 0
        assert target_rx > 0
        assert source_tx == target_rx

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(replicas) == 1
        assert len(replicas[0].replicas) == 3
        assert target_host_id in [replica[0] for replica in replicas[0].replicas]

        for key in range(64):
            rows = await cql.run_async(f"SELECT c FROM {ks}.test WHERE pk = {key}")
            assert len(rows) == 1, f"Expected 1 row for key {key} after tablet rebuild, but got {len(rows)}"
            assert rows[0].c == f"{key}_{value}", f"Expected value '{key}_{value}' for key {key} after tablet rebuild, but got {rows[0].c}"
