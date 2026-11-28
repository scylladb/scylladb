#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-4722",
                      reason="a request forwarded to a leader without a CQL server fails with SERVER_ERROR 'unknown verb'")
async def test_requests_to_leader_restarted_after_kill(manager: ScyllaClusterManager):
    """
    Idea: a node killed with SIGKILL and started again runs its strongly consistent
    raft groups, and listens for RPC, well before it starts its CQL server. While it
    leads a tablet in that window, the other replicas forward the tablet's requests to
    it. They must not fail with SERVER_ERROR "unknown verb": that error tells the
    client nothing about whether a write was applied.

    Setup: three nodes, an RF=3 table with one tablet, so every node is a replica and
    a request coordinated by a follower is forwarded to the leader. The node that leads
    the tablet is killed while a coordinator runs a steady stream of writes and reads,
    and restarted with delay_bootstrap_120s, which holds it in join_cluster, after its
    raft groups and RPC are up and before its CQL server is. The restarted node comes
    back before the other replicas elect a new leader and usually wins the election
    itself, during its startup; if another replica wins, the test hands the leadership
    back to it. The stream then goes on for a few more requests.

    Requests may time out or fail while the leader is down or still starting; the test
    checks only that none of them fails with "unknown verb".
    """
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    victim, coordinator = servers[0], hosts[1]

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks, \
            new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
        group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
        await ensure_raft_group_leader_on(manager, victim, group_id)

        # One attempt per request, so that every error is the server's answer, and a short
        # timeout, so that a request waiting for a leader does not stall the stream.
        write = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?) USING TIMEOUT 2s")
        write.retry_policy = FallthroughRetryPolicy()
        read = cql.prepare(f"SELECT v FROM {table} WHERE pk = ? USING TIMEOUT 2s")
        read.retry_policy = FallthroughRetryPolicy()
        read.consistency_level = ConsistencyLevel.QUORUM

        done = 0
        errors: list[str] = []
        stop = asyncio.Event()

        async def stream():
            nonlocal done
            while not stop.is_set():
                for stmt, args in ((write, [done % 10, done]), (read, [done % 10])):
                    try:
                        await cql.run_async(stmt.bind(args), host=coordinator)
                    except Exception as e:
                        errors.append(f"{type(e).__name__}: {e}")
                        await asyncio.sleep(0.1)
                done += 1

        async def wait_for_requests(n: int):
            target = done + n
            while done < target:
                await asyncio.sleep(0.1)

        load = asyncio.create_task(stream())
        log = await manager.server_open_log(victim.server_id)
        try:
            await wait_for_requests(10)
            await manager.server_stop(victim.server_id, convict=False)
            await wait_for_requests(5)

            await manager.server_update_config(victim.server_id, 'error_injections_at_startup', ['delay_bootstrap_120s'])
            mark = await log.mark()
            await manager.server_start(victim.server_id, connect_driver=False,
                                       expected_server_up_state=ServerUpState.HOST_ID_QUERIED)
            await log.wait_for("init - join cluster", from_mark=mark, timeout=120)
            await ensure_raft_group_leader_on(manager, victim, group_id)
            await wait_for_requests(5)
        finally:
            stop.set()
            await load
            # Restart the victim without the delay, rather than wait two minutes for it.
            await manager.server_stop(victim.server_id, convict=False)
            await manager.server_update_config(victim.server_id, 'error_injections_at_startup', [])
            await manager.server_start(victim.server_id)
            cql, _ = await manager.get_ready_cql(servers)

        logger.info("%d iterations, %d errors: %s", done, len(errors), errors)
        unknown_verb = [e for e in errors if "unknown verb" in e]
        assert not unknown_verb, f"{len(unknown_verb)} requests failed with 'unknown verb', e.g. {unknown_verb[0]}"
