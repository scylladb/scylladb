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

from test.cluster.util import FeatureConfig, FeatureConfigurations, get_topology_coordinator, new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


async def do_test_restart_after_intranode_streaming_acked(manager: ScyllaClusterManager, feature_config: FeatureConfig, commit_while_down: bool):
    """
    Reproduces https://scylladb.atlassian.net/browse/SCYLLADB-6226.

    Intra-node migration "streams" by hard-linking the tablet's sstables for the
    pending shard. If the node restarts after the coordinator got the streaming
    ack, but while the node's own persisted tablet stage is still `streaming`,
    it loads the tablet's sstables, the clones included, on the leaving shard:
    the sharder still reads from there. The coordinator doesn't stream again, so
    the pending shard stays empty, and the cleanup stage then deletes the only
    copy of the tablet on that node.

    commit_while_down: the coordinator commits the next stage while the node is
    down, as in the ticket, where the node shut down before applying it.
    Otherwise it commits only after the node has restarted and loaded its
    sstables, which a group0 read barrier before the load would not cover.
    """
    logstor = 'logstor' in feature_config.table_opts
    # The replica-side pause before streaming and its completion message differ by path.
    streaming_wait = 'migration_streaming_wait' if logstor else 'intranode_migration_streaming_wait'
    streaming_finished = 'Streaming for tablet migration of .* finished' if logstor else 'Finished intra-node streaming of tablet'
    cmdline = ['--smp=2']
    # Disabling stream_tablet_wait raises no topology event; the coordinator
    # resumes on the load stats refresh tick, which is 60s by default.
    config = feature_config.get_cluster_cfg({'tablet_load_stats_refresh_interval_in_seconds': 1})
    # Three nodes, so that the coordinator keeps the group0 quorum, and with it
    # the in-memory streaming ack, while the replica is down.
    servers = await manager.servers_add(3, cmdline=cmdline, config=config, property_file=[
        {'dc': 'dc1', 'rack': 'r1'},
        {'dc': 'dc1', 'rack': 'r2'},
        {'dc': 'dc1', 'rack': 'r3'},
    ])
    await manager.disable_tablet_balancing()

    coord_host_id = await get_topology_coordinator(manager)
    coord = await manager.find_server_by_host_id(servers, coord_host_id)
    replica = next(s for s in servers if s.server_id != coord.server_id)
    logger.info(f"Topology coordinator: {coord}, replica: {replica}")

    cql = await manager.get_cql_exclusive(coord)
    num_keys = 100

    keyspace_opts = feature_config.get_keyspace_opts(
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['{replica.rack}']}} AND tablets = {{'initial': 1}}")
    async with new_test_keyspace(manager, keyspace_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int){feature_config.table_opts}")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({k}, {k})") for k in range(num_keys)])
        # No write may follow the clone: it would go to both shards and to the
        # commitlog, and its replay could hide the loss.
        await manager.api.keyspace_flush(replica.ip_addr, ks)

        tablet_token = 0
        replica_host, src_shard = await get_tablet_replica(manager, coord, ks, 'test', tablet_token)
        assert str(replica_host) == await manager.get_host_id(replica.server_id)
        dst_shard = 1 - src_shard

        replica_log = await manager.server_open_log(replica.server_id)
        log_mark = await replica_log.mark()

        await manager.api.enable_injection(replica.ip_addr, streaming_wait, one_shot=True)
        # Through the coordinator's API, so that the request survives the replica's restart.
        migration_task = asyncio.create_task(
            manager.api.move_tablet(coord.ip_addr, ks, 'test', replica_host, src_shard, replica_host, dst_shard, tablet_token))
        await manager.api.wait_for_injection_enter(replica.ip_addr, streaming_wait)

        # Let the clone finish and its ack reach the coordinator, but keep the
        # coordinator from committing the next stage. So the replica persists
        # nothing past `streaming`.
        await manager.api.enable_injection(coord.ip_addr, 'stream_tablet_wait', one_shot=False)
        await manager.api.message_injection(replica.ip_addr, streaming_wait)
        await replica_log.wait_for(streaming_finished, from_mark=log_mark)

        await manager.server_stop_gracefully(replica.server_id)

        if not commit_while_down:
            await manager.server_start(replica.server_id)
            # Nothing is lost yet: the leaving shard still serves the reads.
            rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test", consistency_level=ConsistencyLevel.ONE))
            assert len(rows) == num_keys

        # The stage change needs only the group0 quorum, so the coordinator
        # commits it with the replica down too. Its barrier then waits for the replica.
        await manager.api.disable_injection(coord.ip_addr, 'stream_tablet_wait')

        async def left_streaming():
            rows = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name = '{ks}' ALLOW FILTERING")
            stages = {row.stage for row in rows}
            logger.info(f"Current stages: {stages}")
            return 'streaming' not in stages or None
        await wait_for(left_streaming, time.time() + 60)

        if commit_while_down:
            await manager.server_start(replica.server_id)
        await migration_task

        # Guard the scenario: streamed once only, and the leaving shard was cleaned up.
        assert len(await replica_log.grep(streaming_finished, from_mark=log_mark)) == 1
        assert len(await replica_log.grep('Cleaned up tablet', from_mark=log_mark)) >= 1

        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test", consistency_level=ConsistencyLevel.ONE))
        assert sorted((r.pk, r.v) for r in rows) == [(k, k) for k in range(num_keys)]


@pytest.mark.parametrize("commit_while_down", [True, False])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_restart_after_intranode_streaming_acked(manager: ScyllaClusterManager, storage_config: FeatureConfig, commit_while_down: bool):
    """
    Local and object storage clone the sstables in different ways: by hard links,
    or by sharing the objects between two entries of the sstables registry.
    """
    await do_test_restart_after_intranode_streaming_acked(manager, storage_config, commit_while_down)


@pytest.mark.parametrize("commit_while_down", [True, False])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_restart_after_intranode_streaming_acked_logstor(manager: ScyllaClusterManager, commit_while_down: bool):
    """
    A logstor table migrates by file streaming, into files the pending shard
    owns, so that shard recovers them by itself after the restart.
    """
    await do_test_restart_after_intranode_streaming_acked(manager, FeatureConfigurations.LOGSTOR_EVENTUAL_CONSISTENCY.value, commit_while_down)
