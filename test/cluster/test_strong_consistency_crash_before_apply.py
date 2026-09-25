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

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

WAIT_BEFORE_APPLY = "strong_consistency_state_machine_wait_before_apply"
DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"


async def wait_for_read_barrier(manager: ScyllaClusterManager, ip: str, group_id: str) -> None:
    """Wait until a read barrier for `group_id` succeeds on `ip`. It raises while the
    group's Raft server is not registered on the node yet (e.g. right after a restart)."""
    async def barrier() -> bool | None:
        try:
            await read_barrier(manager.api, ip, group_id, timeout=60)
            return True
        except Exception as e:
            logger.info(f"Read barrier for group {group_id} on {ip} not ready yet: {e}")
            return None
    await wait_for(barrier, time.time() + 120, label=f"read barrier for group {group_id} on {ip}")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("rf", [1, 3])
async def test_crash_between_commit_and_apply(manager: ScyllaClusterManager, rf: int):
    """Verify that a strongly consistent write survives a crash of a replica that has
    committed the entry to its Raft log but has not applied it to the table yet.

    The coordinator acknowledges an SC write once the entry is committed; applying it to
    the local table happens later in the state machine's apply fiber, on every replica.
    The test pauses apply() on one replica right at that point (the entry is committed
    and durable in the Raft log, but not stored locally), SIGKILLs it, restarts it and
    checks that the row is visible - both through a local (CL=ONE) read on the restarted
    node and through a quorum read.

    The restarted replica's own Raft log must be the only place the entry can come back
    from; otherwise a peer would re-send it and hide a replica that lost it:
    rf=1: the single node is coordinator, leader and the crashed replica.
    rf=3: the leader coordinates. The second follower drops the group's AppendEntries, so
    the entry commits with the leader and the target alone. Then the target and the leader
    are both killed and only the target is restarted: the entry survives only if the
    target kept it, and it can only be served if the target wins the election with it.
    """
    leader = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, property_file={'dc': 'dc1', 'rack': 'r1'})
    followers = []
    if rf == 3:
        followers = await manager.servers_add(2, config=DEFAULT_CONFIG | {'error_injections_at_startup': ['avoid_being_raft_leader']},
                                              cmdline=DEFAULT_CMDLINE + ['--logger-log-level', 'raft_group_registry=debug'],
                                              property_file=[{'dc': 'dc1', 'rack': 'r2'}, {'dc': 'dc1', 'rack': 'r3'}])
    servers = [leader, *followers]
    target = followers[0] if rf == 3 else leader
    other = followers[1] if rf == 3 else None
    cql, hosts = await manager.get_ready_cql(servers)
    leader_host = hosts[0]

    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'initial': 1}} AND consistency = 'global'"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await wait_for_leader(manager, leader, group_id, expected_host_id=await manager.get_host_id(leader.server_id))

            # Warm-up: a committed and applied write, so that the next apply() on the target is ours.
            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (-1, -1)", host=leader_host)
            assert len(await cql.run_async(f"SELECT c FROM {table} WHERE pk = -1", host=leader_host)) == 1
            for s in servers:
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)

            if other:
                other_log = await manager.server_open_log(other.server_id)
                other_mark = await other_log.mark()
                await manager.api.enable_injection(other.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})
            await manager.api.enable_injection(target.ip_addr, WAIT_BEFORE_APPLY, one_shot=True)
            write = cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=leader_host)
            # Entered apply() means the entry is committed on the target and its apply is paused.
            await manager.api.wait_for_injection_enter(target.ip_addr, WAIT_BEFORE_APPLY)
            local_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)
            assert await cql.run_async(local_read, host=hosts[servers.index(target)]) == [], "apply() was paused after the store"

            # The coordinator waits for commit only, so the client is acknowledged while apply is still paused.
            await asyncio.wait_for(write, 5)

            if other:
                # The premise: the second follower never got the entry (heartbeats have size 0).
                await other_log.wait_for(rf"Dropping append request \(size: [1-9]\d*\) .* for group {group_id}", from_mark=other_mark, timeout=60)
            await manager.server_stop(target.server_id, convict=False)
            if other:
                # With the leader gone too, the target's own log holds the only copy of the entry.
                await manager.server_stop(leader.server_id, convict=False)
                await manager.api.disable_injection(other.ip_addr, DROP_APPEND_ENTRIES)
                # The target is the only node that can bring the entry back, so it must be allowed to lead.
                await manager.server_update_config(target.server_id, 'error_injections_at_startup', [])

            await manager.server_start(target.server_id)
            alive = [target, other] if other else [target]
            hosts = await wait_for_cql_and_get_hosts(cql, alive, time.time() + 120)
            await wait_for_leader(manager, target, group_id, expected_host_id=await manager.get_host_id(target.server_id))
            await wait_for_read_barrier(manager, target.ip_addr, group_id)

            assert [r.c for r in await cql.run_async(local_read, host=hosts[0])] == [1], \
                "the committed write is gone: the restarted node's own Raft log was its only copy"
            assert [r.c for r in await cql.run_async(f"SELECT c FROM {table} WHERE pk = 0", host=hosts[-1])] == [1]

            if other:
                # Bring the old leader back, for the keyspace teardown and to check it rejoins the same history.
                await manager.server_start(leader.server_id)
                hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)
                await wait_for_read_barrier(manager, leader.ip_addr, group_id)
                assert [r.c for r in await cql.run_async(local_read, host=hosts[0])] == [1]
