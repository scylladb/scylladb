#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.internal_types import HostID
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_paused_leader_uncommitted_write_resubmitted(manager: ScyllaClusterManager):
    """Check that a write left uncommitted in a paused leader's log is neither lost nor applied
    twice once the leader is resumed, and that every replica ends up with the same history.

    Both followers drop the leader's AppendEntries, so a write reaches the leader's log but
    cannot commit. The leader is then frozen (SIGSTOP). The followers elect a new leader and
    accept a write of their own. When the initial leader is resumed, raft replaces its uncommitted
    entry with the new leader's log. The client's write must not fail because of that: the
    coordinator on the initial leader gets dropped_entry (or not_a_leader), retries, and is told
    to forward to the new leader, where the write is applied once, after the new leader's own write.

    The row holds a list and every write is an append, so the table records every write that was
    applied, in timestamp order, and a later write cannot hide an earlier one. The timestamp comes
    from begin_mutate() on the leader that appends the entry, and a new leader starts from the
    tablet's max timestamp, so the resubmitted write sorts after the new leader's write. Given
    [1, 3] on the majority before the leader is resumed, [1, 3, 2] everywhere is the only
    linearizable outcome; the entry applied twice, the majority's write lost, or the truncated
    entry applied locally on the initial leader each leave a different list.

    A live partitioned leader is not covered: the injection drops AppendEntries only, and
    failure-detector pings still arrive, so a running leader would never be replaced. The window
    in which an old leader keeps serving reads and accepting writes next to the new one, until it
    steps down on its own, is out of scope here.
    """
    cmdline = DEFAULT_CMDLINE + ['--logger-log-level', 'raft_group_registry=debug']
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=cmdline, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    initial_leader, *followers = servers
    initial_leader_host, *follower_hosts = hosts
    initial_leader_id = await manager.get_host_id(initial_leader.server_id)
    follower_ids = await gather_safely(*(manager.get_host_id(s.server_id) for s in followers))

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, l list<int>") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await ensure_raft_group_leader_on(manager, initial_leader, group_id)
            logger.info(f"group {group_id}: leader {initial_leader_id}, followers {follower_ids}")

            quorum_read = SimpleStatement(f"SELECT l FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM)
            local_read = SimpleStatement(f"SELECT l FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)

            async def read(stmt: SimpleStatement, host) -> list[int]:
                rows = await cql.run_async(stmt, host=host)
                return rows[0].l if rows else []

            def append(n: int, host) -> asyncio.Future:
                return cql.run_async(f"UPDATE {table} SET l = l + [{n}] WHERE pk = 0", host=host)

            async def local_history(s, host) -> list[int]:
                """The row as the replica itself holds it, once it has applied everything committed so far."""
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)
                return await read(local_read, host)

            await append(1, initial_leader_host)
            await gather_safely(*(read_barrier(manager.api, s.ip_addr, group_id, timeout=60) for s in followers))

            follower_logs = [await manager.server_open_log(s.server_id) for s in followers]
            follower_marks = [await log.mark() for log in follower_logs]
            await gather_safely(*(manager.api.enable_injection(s.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})
                                  for s in followers))

            # Reaches the initial leader's log and stays uncommitted: heartbeats have size 0.
            hung_write = append(2, initial_leader_host)
            await gather_safely(*(log.wait_for(rf"Dropping append request \(size: [1-9]\d*\) .* for group {group_id}", from_mark=mark, timeout=60)
                                  for log, mark in zip(follower_logs, follower_marks)))
            assert not hung_write.done()

            await manager.server_pause(initial_leader.server_id)

            # The followers keep dropping AppendEntries until a new leader exists: votes are not
            # AppendEntries, so the election goes through, and once a higher term exists the
            # initial leader can no longer commit its entry, even if SIGSTOP landed late. Only
            # the winner can learn who the leader is while the drop is on, so ask both.
            async def elected_leader() -> HostID | None:
                leaders = await gather_safely(*(manager.api.get_raft_leader(s.ip_addr, group_id) for s in followers))
                return next((hid for hid in leaders if hid is not None and hid != initial_leader_id), None)
            new_leader_id = await wait_for(elected_leader, time.time() + 60, label=f"the followers of group {group_id} to elect a new leader")
            new_leader_host = follower_hosts[follower_ids.index(new_leader_id)]
            logger.info(f"group {group_id}: new leader {new_leader_id}")
            await gather_safely(*(manager.api.disable_injection(s.ip_addr, DROP_APPEND_ENTRIES) for s in followers))

            # The majority makes progress without the frozen leader. Write 2 has not taken
            # effect on it, so 2 can only be ordered after 3.
            await append(3, new_leader_host)
            assert await read(quorum_read, new_leader_host) == [1, 3]
            histories = await gather_safely(*(local_history(s, host) for s, host in zip(followers, follower_hosts)))
            for s, history in zip(followers, histories):
                assert history == [1, 3], f"local read on {s.ip_addr}"

            await manager.server_unpause(initial_leader.server_id)
            # Truncated on the initial leader, re-submitted through the new one.
            await asyncio.wait_for(hung_write, 60)
            hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

            # One history everywhere: write 2 applied once, after write 3.
            histories = await gather_safely(*(local_history(s, host) for s, host in zip(servers, hosts)))
            quorum_histories = await gather_safely(*(read(quorum_read, host) for host in hosts))
            for s, history, quorum_history in zip(servers, histories, quorum_histories):
                assert quorum_history == [1, 3, 2], f"quorum read via {s.ip_addr}"
                assert history == [1, 3, 2], f"local read on {s.ip_addr}"
