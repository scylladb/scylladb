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

from test.cluster.util import get_topology_coordinator, new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts
from test.pylib.manager_client import ManagerClient
from test.pylib.object_storage import format_tuples
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


async def do_test_restart_after_intranode_streaming_acked(manager: ManagerClient, commit_while_down: bool, storage=None, logstor: bool = False):
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
    # The replica-side pause before streaming and its completion message differ by path.
    streaming_wait = 'migration_streaming_wait' if logstor else 'intranode_migration_streaming_wait'
    streaming_finished = 'Streaming for tablet migration of .* finished' if logstor else 'Finished intra-node streaming of tablet'
    # The coordinator logs the resolution of the streaming action at debug level.
    cmdline = ['--smp=2', '--logger-log-level', 'raft_topology=debug']
    # Disabling stream_tablet_wait raises no topology event; the coordinator
    # resumes on the load stats refresh tick, which is 60s by default.
    base_config = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    if storage:
        base_config['object_storage_endpoints'] = storage.create_endpoint_conf()
    if logstor:
        base_config |= {'experimental_features': ['logstor'], 'logstor_disk_size_in_mb': 20, 'logstor_file_size_in_mb': 1}
    config = base_config
    # Three nodes, so that the coordinator keeps the group0 quorum while the
    # replica is down. It must keep the leadership too: the streaming ack is
    # held in memory, and a new coordinator would stream again. So the other
    # nodes never stand for election, the restarted replica included.
    coord = await manager.server_add(cmdline=cmdline, config=config, property_file={'dc': 'dc1', 'rack': 'r1'})
    follower_config = base_config | {'error_injections_at_startup': ['avoid_being_raft_leader']}
    followers = await manager.servers_add(2, cmdline=cmdline, config=follower_config, property_file=[
        {'dc': 'dc1', 'rack': 'r2'},
        {'dc': 'dc1', 'rack': 'r3'},
    ])
    servers = [coord, *followers]
    replica = followers[0]
    await manager.disable_tablet_balancing()

    coord_host_id = await manager.get_host_id(coord.server_id)
    assert await get_topology_coordinator(manager) == coord_host_id

    cql = await manager.get_cql_exclusive(coord)
    num_keys = 100

    keyspace_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['{replica.rack}']}} AND tablets = {{'initial': 1}}"
    if storage:
        keyspace_opts += f" AND STORAGE = {format_tuples(type=storage.type, endpoint=storage.address, bucket=storage.bucket_name)}"
    table_opts = " WITH storage_engine = 'logstor'" if logstor else ""
    async with new_test_keyspace(manager, keyspace_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int){table_opts}")
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
        coord_log = await manager.server_open_log(coord.server_id)
        coord_log_mark = await coord_log.mark()

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
        # The replica logs that before it replies. Stopping it before the reply
        # arrives would fail the streaming action, and the coordinator would
        # stream again.
        await coord_log.wait_for('streaming for tablet .* resolved', from_mark=coord_log_mark)
        assert not await coord_log.grep('streaming for tablet .* failed', from_mark=coord_log_mark)

        # Otherwise the coordinator may still hold the replica's alive state from
        # before the stop when the replica is back, and mark it down only later.
        await manager.server_stop_gracefully(replica.server_id, seen_by=[coord])

        if not commit_while_down:
            # The read goes through the coordinator, which must see the only replica alive.
            await manager.server_start(replica.server_id, seen_by=[coord])
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
        assert await get_topology_coordinator(manager) == coord_host_id

        # Guard the scenario: streamed once only, and the leaving shard was cleaned up.
        assert len(await replica_log.grep(streaming_finished, from_mark=log_mark)) == 1
        assert len(await replica_log.grep('Cleaned up tablet', from_mark=log_mark)) >= 1

        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test", consistency_level=ConsistencyLevel.ONE))
        assert sorted((r.pk, r.v) for r in rows) == [(k, k) for k in range(num_keys)]


@pytest.mark.parametrize("commit_while_down", [True, False])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_restart_after_intranode_streaming_acked(manager: ManagerClient, storage, commit_while_down: bool):
    """
    Local and object storage clone the sstables in different ways: by hard links,
    or by sharing the objects between two entries of the sstables registry.
    """
    await do_test_restart_after_intranode_streaming_acked(manager, commit_while_down, storage=storage)


@pytest.mark.parametrize("commit_while_down", [True, False])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_restart_after_intranode_streaming_acked_logstor(manager: ManagerClient, commit_while_down: bool):
    """
    A logstor table migrates by file streaming, into files the pending shard
    owns, so that shard recovers them by itself after the restart.
    """
    await do_test_restart_after_intranode_streaming_acked(manager, commit_while_down, logstor=True)
