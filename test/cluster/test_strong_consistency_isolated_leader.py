#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"
WRITE_NODE_BOUNCES = "scylla_strong_consistency_coordinator_write_node_bounces"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_isolated_leader_reconnects_without_divergence(manager: ScyllaClusterManager):
    """Check that a leader cut off with an uncommitted write in its log rejoins the group
    without diverging, and that the write is neither lost nor applied twice.

    Both followers drop the leader's AppendEntries, so a write reaches the leader's log but
    cannot commit. The leader is then frozen (SIGSTOP). The followers elect a new leader and
    accept a write of their own. When the initial leader is resumed, raft replaces its uncommitted
    entry with the new leader's log. The client's write must not fail because of that: the
    coordinator on the initial leader gets dropped_entry (or not_a_leader), retries, and is told
    to forward to the new leader. The write is applied exactly once, after the new leader's
    own write, because the client was still waiting for it. Every replica then serves the
    same value, both through a quorum read and locally.

    The write gets a long server-side timeout so that raft decides its fate, not the timer.
    """
    config = DEFAULT_CONFIG | {'write_request_timeout_in_ms': 60000}
    cmdline = DEFAULT_CMDLINE + ['--logger-log-level', 'raft_group_registry=debug']
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            initial_leader_id = await wait_for_leader(manager, servers[0], group_id)
            initial_leader, initial_leader_host = by_host_id[initial_leader_id]
            follower_ids = [hid for hid in host_ids if hid != initial_leader_id]
            followers = [by_host_id[hid] for hid in follower_ids]
            logger.info(f"group {group_id}: leader {initial_leader_id}, followers {follower_ids}")

            quorum_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM)
            local_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)

            async def read(stmt: SimpleStatement, host) -> list[int]:
                return [r.c for r in await cql.run_async(stmt, host=host)]

            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=initial_leader_host)
            for s in servers:
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)
            bounces_before = (await manager.metrics.query(initial_leader.ip_addr)).get(WRITE_NODE_BOUNCES) or 0

            follower_logs = [await manager.server_open_log(s.server_id) for s, _ in followers]
            follower_marks = [await log.mark() for log in follower_logs]
            for s, _ in followers:
                await manager.api.enable_injection(s.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})

            # Reaches the initial leader's log and stays uncommitted: heartbeats have size 0.
            hung_write = cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 2)", host=initial_leader_host)
            for log, mark in zip(follower_logs, follower_marks):
                await log.wait_for(rf"Dropping append request \(size: [1-9]\d*\) .* for group {group_id}", from_mark=mark, timeout=60)
            assert not hung_write.done()

            await manager.server_pause(initial_leader.server_id)

            # The followers keep dropping AppendEntries until a new leader exists: votes are not
            # AppendEntries, so the election goes through, and once a higher term exists the
            # initial leader can no longer commit its entry, even if SIGSTOP landed late. Only
            # the winner can learn who the leader is while the drop is on, so ask both.
            async def elected_leader() -> str | None:
                for s, _ in followers:
                    leader = str(await manager.api.get_raft_leader(s.ip_addr, group_id))
                    if leader != initial_leader_id and uuid.UUID(leader).int != 0:
                        return leader
                return None
            new_leader_id = await wait_for(elected_leader, time.time() + 60, label=f"the followers of group {group_id} to elect a new leader")
            _, new_leader_host = by_host_id[new_leader_id]
            logger.info(f"group {group_id}: new leader {new_leader_id}")
            for s, _ in followers:
                await manager.api.disable_injection(s.ip_addr, DROP_APPEND_ENTRIES)

            # The majority makes progress without the frozen leader.
            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 3)", host=new_leader_host)
            assert await read(quorum_read, new_leader_host) == [3]
            for s, host in followers:
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)
                assert await read(local_read, host) == [3], f"local read on {s.ip_addr}"

            await manager.server_unpause(initial_leader.server_id)
            # Truncated on the initial leader, re-submitted through the new one.
            await asyncio.wait_for(hung_write, 60)
            hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            for s in servers:
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)

            # One history everywhere: the re-submitted write is the last one.
            for s, host in zip(servers, hosts):
                assert await read(quorum_read, host) == [2], f"quorum read via {s.ip_addr}"
                assert await read(local_read, host) == [2], f"local read on {s.ip_addr}"
            bounces_after = (await manager.metrics.query(initial_leader.ip_addr)).get(WRITE_NODE_BOUNCES) or 0
            assert bounces_after - bounces_before == 1, "the hung write was not re-submitted through the new leader exactly once"
