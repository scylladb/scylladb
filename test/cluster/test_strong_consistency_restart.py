#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time

import pytest
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for

logger = logging.getLogger(__name__)

KEYSPACE_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                 " AND tablets = {'initial': 32} AND consistency = 'global'")
# main.cc waits up to 30 s for the tablet raft groups to start before CQL opens.
GROUPS_START_TIMEOUT = 30
RESTARTED_TOGETHER_BUG = pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-6019",
    reason="nodes restarted together wait 30 s for their raft groups: peer IPs are unknown until join_cluster")


@pytest.mark.asyncio
@pytest.mark.parametrize("killed", [1, pytest.param(2, marks=RESTARTED_TOGETHER_BUG),
                                    pytest.param(3, marks=RESTARTED_TOGETHER_BUG)])
async def test_nodes_restarted_together_serve_quickly(manager: ScyllaClusterManager, killed: int):
    """
    Idea: a node restarted after a crash must serve CQL and strongly consistent
    writes as soon as its tablet raft groups can elect a leader, whether or not
    other replicas restart at the same time. On startup main.cc starts the tablet
    raft groups and then waits, up to 30 s, until each of them knows a leader.
    The address map that resolves a peer's host id to its IP is loaded from
    system.peers only later, in join_cluster(), so before that a restarting node
    can reach a peer only after the peer has opened a connection to it. Nodes
    restarted together cannot reach each other: with all of them restarted no
    group can elect, and with two of three restarted a group whose new leader is
    one of them never reaches the other. Either way the wait runs out.

    Setup: 3 nodes, an RF=3 strongly consistent table with 32 tablets, one
    tablet's raft leader placed on the first node, a few rows written. Which node
    wins each election after the restart is not fixed, so with two nodes
    restarted the 32 groups make it near-certain that one leader lands on a
    restarted node.

    The test kills `killed` nodes, the first node among them, with SIGKILL, starts
    them together, and measures the time from the start until each serves CQL
    and until a write through the first node succeeds. Both must stay well under
    the 30 s wait. killed=1 is the control: the two serving nodes reach the
    restarted one, and it serves within seconds.
    """
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks, \
            new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
        group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
        await ensure_raft_group_leader_on(manager, servers[0], group_id)
        for pk in range(10):
            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

        # Crash and start together.
        down = servers[:killed]
        for s in down:
            await manager.server_stop(s.server_id, convict=killed < len(servers))
        logs = [await manager.server_open_log(s.server_id) for s in down]
        marks = [await log.mark() for log in logs]

        start = time.monotonic()

        async def serving(server_id) -> float:
            await manager.server_start(server_id)
            return time.monotonic() - start

        cql_ready = await gather_safely(*(serving(s.server_id) for s in down))

        # First strongly consistent write through the first node.
        cql, hosts = await manager.get_ready_cql(servers)
        write = SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES (100, 100)")

        async def written():
            try:
                await cql.run_async(write, host=hosts[0])
                return True
            except Exception as e:
                logger.info("write after restart failed: %r", e)
                return None
        await wait_for(written, time.time() + 120, period=0.5)
        sc_ready = time.monotonic() - start

        timed_out = [line for log, mark in zip(logs, marks)
                     for line, _ in await log.grep("Not all Raft groups started", from_mark=mark)]
        logger.info("killed %d: CQL serving after %s s, SC write after %.1f s, wait timeouts: %s",
                    killed, [f"{t:.1f}" for t in cql_ready], sc_ready, timed_out)
        assert max(cql_ready) < GROUPS_START_TIMEOUT / 2 and sc_ready < GROUPS_START_TIMEOUT / 2, \
            f"{killed} node(s) restarted together: CQL serving after {[f'{t:.1f}' for t in cql_ready]} s, " \
            f"SC write after {sc_ready:.1f} s; {timed_out}"
