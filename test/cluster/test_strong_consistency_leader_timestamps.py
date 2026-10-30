#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
#############################################################################
# A strongly consistent write takes its timestamp from the clock of the raft
# group leader, and a newer write wins only by its higher timestamp. Clocks of
# different nodes are not in sync, so a leader whose clock is behind the clock
# of the leader before it must still not issue a timestamp at or below the
# timestamps of the writes in the group's log. Otherwise its writes would be
# acked and then silently lose to older ones.
#
# On test.py all the nodes share one clock, so the tests below move the clock
# the old leader takes its write timestamps from an hour ahead. Each test puts
# the old leader's write in another place the new leader has to find it in.
#############################################################################

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
from cassandra.cluster import ConsistencyLevel, Session
from cassandra.policies import FallthroughRetryPolicy
from cassandra.pool import Host
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table, trigger_stepdown
from test.pylib.internal_types import HostID, ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')

CLOCK_OFFSET_US = 3600 * 1_000_000
CLOCK_OFFSET = 'sc_leader_timestamp_offset_us'
AVOID_LEADERSHIP = 'avoid_being_raft_leader'
PARK_APPLY = 'strong_consistency_state_machine_wait_before_apply'
DROP_APPEND_ENTRIES = 'raft_drop_incoming_append_entries_for_specified_group'
DROP_APPEND_REPLIES = 'raft_drop_incoming_append_entries_reply_for_specified_group'
PERSISTED_LOG_ENTRIES = 'scylla_strong_consistency_raft_persisted_log_entries'
WAIT_TIMEOUT_S = 60


@dataclass
class Cluster:
    manager: ScyllaClusterManager
    cql: Session
    old_leader: ServerInfo
    new_leader: ServerInfo
    third: ServerInfo
    old_leader_host: Host
    new_leader_host: Host
    new_leader_id: HostID
    ks: str
    table: str
    table_name: str
    group_id: str

    @property
    def servers(self) -> list[ServerInfo]:
        return [self.old_leader, self.new_leader, self.third]


@asynccontextmanager
async def cluster_with_skewed_leader(manager: ScyllaClusterManager):
    """Set up a strongly consistent table whose raft group is led by a node with its clock an
    hour ahead. Of the two other replicas, only the new leader can ever take over: the third
    one never campaigns."""
    old_leader, new_leader = await manager.servers_add(2, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE,
                                                       auto_rack_dc='dc1')
    third = await manager.server_add(config=DEFAULT_CONFIG | {'error_injections_at_startup': [AVOID_LEADERSHIP]},
                                     cmdline=DEFAULT_CMDLINE, property_file={'dc': 'dc1', 'rack': 'rack3'})
    cql, [old_leader_host, new_leader_host, _] = await manager.get_ready_cql([old_leader, new_leader, third])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                                          " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            await ensure_raft_group_leader_on(manager, old_leader, group_id, timeout_seconds=WAIT_TIMEOUT_S)
            await manager.api.enable_injection(old_leader.ip_addr, CLOCK_OFFSET, one_shot=False,
                                               parameters={'value': str(CLOCK_OFFSET_US)})
            yield Cluster(manager=manager, cql=cql, old_leader=old_leader, new_leader=new_leader, third=third,
                          old_leader_host=old_leader_host, new_leader_host=new_leader_host,
                          new_leader_id=await manager.get_host_id(new_leader.server_id),
                          ks=ks, table=table, table_name=table_name, group_id=group_id)


@asynccontextmanager
async def new_leader_apply_parked(cluster: Cluster):
    """Park the applier of the new leader on the next entry it applies."""
    # The new leader must have applied everything before, or the injection parks
    # an earlier entry rather than the next write.
    await read_barrier(cluster.manager.api, cluster.new_leader.ip_addr, cluster.group_id, timeout=WAIT_TIMEOUT_S)
    await cluster.manager.api.enable_injection(cluster.new_leader.ip_addr, PARK_APPLY, one_shot=True)
    # Release the applier even if the test fails: the parked apply doesn't see
    # the node's shutdown and would hold up its stop for 20 minutes.
    try:
        yield
    finally:
        await cluster.manager.api.message_injection(cluster.new_leader.ip_addr, PARK_APPLY)


def read_stmt(cluster: Cluster) -> SimpleStatement:
    return SimpleStatement(f"SELECT v, writetime(v) AS wt FROM {cluster.table} WHERE pk = 0",
                           consistency_level=ConsistencyLevel.QUORUM)


def check_old_leader_clock_ahead(write_ts: int) -> None:
    # Without the shifted clock the old leader's write would not be ahead of the new
    # leader's clock, and the tests would pass without anything to protect against.
    assert write_ts > time.time() * 1_000_000 + CLOCK_OFFSET_US / 2, \
        f"the old leader's write timestamp {write_ts} is not ahead of the clock"


async def write_on_old_leader(cluster: Cluster) -> int:
    """Write through the old leader and return the timestamp the write got."""
    await cluster.cql.run_async(f"INSERT INTO {cluster.table} (pk, v) VALUES (0, 1)", host=cluster.old_leader_host)
    [row] = await cluster.cql.run_async(read_stmt(cluster), host=cluster.old_leader_host)
    assert row.v == 1
    check_old_leader_clock_ahead(row.wt)
    return row.wt


async def hand_over_to_new_leader(cluster: Cluster) -> None:
    logger.info(f"Handing the leadership of group {cluster.group_id} over to {cluster.new_leader_id}")
    await trigger_stepdown(cluster.manager, cluster.old_leader, cluster.group_id, target_host_id=cluster.new_leader_id)
    await wait_for_leader(cluster.manager, cluster.new_leader, cluster.group_id, expected_host_id=cluster.new_leader_id)


async def check_new_leader_write_wins(cluster: Cluster, old_write_ts: int) -> None:
    """Write through the new leader, whose own clock is an hour behind `old_write_ts`, and
    check that the write still wins: its timestamp is above the old leader's one."""
    await cluster.cql.run_async(f"INSERT INTO {cluster.table} (pk, v) VALUES (0, 2)", host=cluster.new_leader_host)
    [row] = await cluster.cql.run_async(read_stmt(cluster), host=cluster.new_leader_host)
    assert row.v == 2, f"the new leader's write lost to the old leader's one, writetime {row.wt} <= {old_write_ts}"
    assert row.wt > old_write_ts


@pytest.mark.asyncio
async def test_new_leader_timestamps_above_write_in_raft_log(manager: ScyllaClusterManager):
    """
    Verify that a new leader whose clock is behind issues write timestamps above the
    timestamp of an acked write of the old leader which, when it takes over, it holds
    in its raft log only: committed, but not applied - its applier is parked, so the
    write is in none of its memtables.
    """
    async with cluster_with_skewed_leader(manager) as cluster:
        async with new_leader_apply_parked(cluster):
            old_write_ts = await write_on_old_leader(cluster)
            await manager.api.wait_for_injection_enter(cluster.new_leader.ip_addr, PARK_APPLY)
            await hand_over_to_new_leader(cluster)
        await check_new_leader_write_wins(cluster, old_write_ts)


@pytest.mark.asyncio
async def test_new_leader_timestamps_above_write_in_sstable(manager: ScyllaClusterManager):
    """
    Verify that a new leader whose clock is behind issues write timestamps above the
    timestamp of an acked write of the old leader which, when it takes over, is in
    sstables only: applied and flushed on every replica.
    """
    async with cluster_with_skewed_leader(manager) as cluster:
        old_write_ts = await write_on_old_leader(cluster)
        for s in cluster.servers:
            # The write is acked once it is committed: a follower may not have applied it yet,
            # and its flush would then leave the write in the memtable.
            await read_barrier(manager.api, s.ip_addr, cluster.group_id, timeout=WAIT_TIMEOUT_S)
            await manager.api.keyspace_flush(s.ip_addr, cluster.ks, cluster.table_name)
        await hand_over_to_new_leader(cluster)
        await check_new_leader_write_wins(cluster, old_write_ts)


@pytest.mark.asyncio
async def test_new_leader_timestamps_above_uncommitted_write(manager: ScyllaClusterManager):
    """
    Verify that a new leader whose clock is behind issues write timestamps above the
    timestamp of a write the old leader appended but did not commit.

    The write reaches the log of the new leader only: the third replica drops it, and the
    old leader never hears that the new leader appended it, so it is never committed and
    never acked. Once the old leader is gone, the new leader - the replica with the
    longest log - commits the write along with the first entry of its own term. The write
    is then a part of the group's history, and the new leader's writes have to be above it.
    """
    async with cluster_with_skewed_leader(manager) as cluster:
        # The write below is then the only entry the new leader appends to its log
        # from here on, so its count of persisted entries shows when it got there.
        await read_barrier(manager.api, cluster.new_leader.ip_addr, cluster.group_id, timeout=WAIT_TIMEOUT_S)

        async def persisted_on_new_leader() -> int:
            metrics = await manager.metrics.query(cluster.new_leader.ip_addr)
            return metrics.get(PERSISTED_LOG_ENTRIES, {'ks': cluster.ks, 'cf': cluster.table_name}) or 0
        persisted_before = await persisted_on_new_leader()

        async def appended_on_new_leader():
            return True if await persisted_on_new_leader() > persisted_before else None

        group = {'value': cluster.group_id}
        await manager.api.enable_injection(cluster.third.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters=group)
        await manager.api.enable_injection(cluster.old_leader.ip_addr, DROP_APPEND_REPLIES, one_shot=False, parameters=group)

        # Not retrying the write keeps it from coming back through another node with a new timestamp.
        insert = SimpleStatement(f"INSERT INTO {cluster.table} (pk, v) VALUES (0, 1)", retry_policy=FallthroughRetryPolicy())
        old_write = cluster.cql.run_async(insert, host=cluster.old_leader_host)
        try:
            await wait_for(appended_on_new_leader, time.time() + WAIT_TIMEOUT_S)
            logger.info(f"The write is in the log of {cluster.new_leader_id} only, stopping the old leader")
        finally:
            # Stopping the old leader also ends the write it cannot commit and its injection.
            await manager.server_stop(cluster.old_leader.server_id, convict=True)
            [old_write_outcome] = await asyncio.gather(old_write, return_exceptions=True)
            await manager.api.disable_injection(cluster.third.ip_addr, DROP_APPEND_ENTRIES)
        # An ack would mean the write got committed after all, and the test would not
        # check what it is meant to.
        assert isinstance(old_write_outcome, Exception), "the old leader acked the write it was not to commit"

        await wait_for_leader(manager, cluster.new_leader, cluster.group_id, expected_host_id=cluster.new_leader_id)
        [row] = await cluster.cql.run_async(read_stmt(cluster), host=cluster.new_leader_host)
        assert row.v == 1, "the new leader did not commit the write it got from the old leader"
        check_old_leader_clock_ahead(row.wt)
        await check_new_leader_write_wins(cluster, row.wt)
