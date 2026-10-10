#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import time
import uuid

import pytest
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"
# raft::log::maybe_append (raft/log.cc), trace level: a rejoining node found an entry of its own
# at an index where the leader's log holds a different term, and dropped it and everything after.
# The line does not name the group; nothing writes to group0 between the kill and the restart,
# so a match on the restarted leader can only be the tablet group's entry.
TRUNCATED = r"append_entries: entries with index \d+ has non matching terms"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("via", ["leader", "follower"])
@pytest.mark.parametrize("settle", ["heal", "heal_then_kill", "kill_then_heal"])
async def test_write_timed_out_before_commit(manager: ScyllaClusterManager, via: str, settle: str):
    """
    Verify what happens to a strongly consistent write whose client got a timeout while the entry
    was still uncommitted in the leader's log: it may commit later or it may be lost, but it is
    never applied twice, never reordered, and every replica ends up with the same history.

    Both followers drop the leader's AppendEntries, so a write reaches the leader's log but cannot
    gather a quorum. The leader keeps its role: raft liveness comes from the failure detector,
    not from AppendEntries replies. A write with `USING TIMEOUT 1s` therefore times out inside
    raft's wait_for_entry with the entry still in the log, and the client is told WriteTimeout.
    A QUORUM read must not see the write at this point; the read barrier goes through read_quorum,
    which is not dropped. The write is sent to the leader or to a follower (`via`), so the
    forwarding path reports the same timeout and does not re-execute the write.

    The entry is then settled (`settle`):
      heal            - followers accept AppendEntries again:        the write is applied.
      heal_then_kill  - heal, wait until it is visible, kill leader:  applied, and survives the leader.
      kill_then_heal  - kill the leader, then heal:                   lost, it was in the leader's log only.

    A fence write through a live leader then pins the history. Each append adds a list cell keyed
    by the write's timestamp, so a duplicate (e.g. a retry of the timed-out write) or a reorder
    would show up in the list. With a killed leader, the test restarts it and checks that it
    serves the same history locally, and that raft truncated its uncommitted entry if and only
    if the write was lost.

    test_strong_consistency_isolated_leader.py starts from the same dropped AppendEntries but keeps
    the client waiting and checks that the hung write is resubmitted through the new leader. Here
    the client has already been told the write failed, so the question is only whether the server
    stays consistent with itself.
    """
    cmdline = DEFAULT_CMDLINE + ['--logger-log-level', 'raft_group_registry=debug', '--logger-log-level', 'raft=trace']
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=cmdline, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, l list<int>") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            leader_id = await wait_for_leader(manager, servers[0], group_id)
            leader, leader_host = by_host_id[leader_id]
            follower_ids = [hid for hid in host_ids if hid != leader_id]
            followers = [by_host_id[hid] for hid in follower_ids]
            logger.info(f"group {group_id}: leader {leader_id}, followers {follower_ids}")

            async def read(cl: ConsistencyLevel, host) -> list[int]:
                rows = await cql.run_async(SimpleStatement(f"SELECT l FROM {table} WHERE pk = 0", consistency_level=cl), host=host)
                return rows[0].l

            async def append(value: int, host, timeout: str | None = None) -> None:
                using = f" USING TIMEOUT {timeout}" if timeout else ""
                # A driver-side retry of a timed-out write is exactly the duplicate this test looks for.
                stmt = SimpleStatement(f"UPDATE {table}{using} SET l = l + [{value}] WHERE pk = 0", retry_policy=FallthroughRetryPolicy())
                await cql.run_async(stmt, host=host)

            await append(0, leader_host)

            follower_logs = [await manager.server_open_log(s.server_id) for s, _ in followers]
            follower_marks = [await log.mark() for log in follower_logs]
            for s, _ in followers:
                await manager.api.enable_injection(s.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})

            logger.info(f"Append 1 via the {via} with a 1s timeout, expecting it to time out uncommitted")
            with pytest.raises(WriteTimeout):
                await append(1, leader_host if via == "leader" else followers[0][1], timeout="1s")
            # The entry reached the leader's log and was sent: heartbeats have size 0.
            for log, mark in zip(follower_logs, follower_marks):
                await log.wait_for(rf"Dropping append request \(size: [1-9]\d*\) .* for group {group_id}", from_mark=mark, timeout=60)
            assert await read(ConsistencyLevel.QUORUM, followers[1][1]) == [0], "an uncommitted write is visible"

            async def heal() -> None:
                for s, _ in followers:
                    await manager.api.disable_injection(s.ip_addr, DROP_APPEND_ENTRIES)

            logger.info(f"Settle the write: {settle}")
            if settle == "heal":
                await heal()
            elif settle == "heal_then_kill":
                await heal()

                async def committed() -> bool | None:
                    return await read(ConsistencyLevel.QUORUM, followers[0][1]) == [0, 1] or None
                await wait_for(committed, time.time() + 60, label="the healed write to commit")
                await manager.server_stop(leader.server_id, convict=True)
            else:
                await manager.server_stop(leader.server_id, convict=True)
                await heal()

            if settle != "heal":
                # A follower names the killed leader until it hears from the new one, so ask both and skip the old answer.
                async def elected_leader() -> str | None:
                    for s, _ in followers:
                        candidate = str(await manager.api.get_raft_leader(s.ip_addr, group_id))
                        if candidate != leader_id and uuid.UUID(candidate).int != 0:
                            return candidate
                    return None
                new_leader_id = await wait_for(elected_leader, time.time() + 60, label=f"the followers of group {group_id} to elect a new leader")
                logger.info(f"group {group_id}: new leader {new_leader_id}")

            await append(2, by_host_id[new_leader_id][1] if settle != "heal" else leader_host)
            expected = [0, 2] if settle == "kill_then_heal" else [0, 1, 2]

            for s, host in followers:
                assert await read(ConsistencyLevel.QUORUM, host) == expected, f"quorum read via {s.ip_addr}"

            if settle != "heal":
                leader_log = await manager.server_open_log(leader.server_id)
                leader_mark = await leader_log.mark()
                await manager.server_start(leader.server_id)
                await manager.servers_see_each_other(servers)
                hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

            # Every replica serves the same history locally once caught up with the leader.
            for s, host in zip(servers, hosts):
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)
                assert await read(ConsistencyLevel.ONE, host) == expected, f"local read on {s.ip_addr}"

            if settle != "heal":
                # Catching up with the new leader (the read barrier above) implies the conflict, if any, is resolved.
                truncated = await leader_log.grep(TRUNCATED, from_mark=leader_mark)
                assert bool(truncated) == (settle == "kill_then_heal"), f"old leader truncation: {truncated}"
