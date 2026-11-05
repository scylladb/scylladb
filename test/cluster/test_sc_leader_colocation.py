#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace, trigger_stepdown
from test.pylib.internal_types import ServerInfo
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, wait_for_leader

import pytest
import logging
import time


logger = logging.getLogger(__name__)


# Co-location of the leaders of a strongly consistent tablet's child groups with the leader of its
# group, while a split of the tablet is recorded.
#
# Nothing records such a split on its own yet, so the tests have the topology coordinator do it
# through the sc_record_raft_split injection: it writes a split decision and the two child group
# ids, as the resize finalization will, and nothing carries the split any further. Every replica
# then starts the child groups next to the tablet's own group, and keeps them until the table is
# dropped.


async def get_tablet_raft_row(manager: ScyllaClusterManager, ks: str, table: str):
    """The raft group id and the recorded child group ids of the table's only tablet."""
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.get_cql().run_async(
        "SELECT raft_group_id, transition_raft_group_ids"
        f" FROM system.tablets WHERE table_id = {table_id}")
    rows = [r for r in rows if r.raft_group_id is not None]
    assert len(rows) == 1
    return rows[0]


async def record_split(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str, table: str) -> list[str]:
    """Record a split of the table's only tablet and return the ids of its two child groups."""
    table_id = await manager.get_table_id(ks, table)
    # Enabled everywhere, since any node may be the topology coordinator.
    await gather_safely(*[manager.api.enable_injection(s.ip_addr, 'sc_record_raft_split', one_shot=False,
                                                       parameters={'value': str(table_id)})
                          for s in servers])
    # The coordinator looks at the injection when it has nothing else to do, which it finds out
    # on the next topology change. Disabling the balancing, which the tests want anyway, is one.
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    async def children_recorded():
        row = await get_tablet_raft_row(manager, ks, table)
        return [str(gid) for gid in row.transition_raft_group_ids] if row.transition_raft_group_ids else None
    new_gids = await wait_for(children_recorded, time.time() + 60, label="the split to be recorded")
    await gather_safely(*[manager.api.disable_injection(s.ip_addr, 'sc_record_raft_split') for s in servers])
    assert len(new_gids) == 2
    logger.info(f"Recorded the split into {new_gids}")
    return new_gids


async def wait_for_children_led_by(manager: ScyllaClusterManager, servers: list[ServerInfo],
                                   new_gids: list[str], leader_host_id):
    """Wait until every server reports `leader_host_id` as the leader of both child groups."""
    await gather_safely(*[wait_for_leader(manager, s, gid, expected_host_id=leader_host_id)
                          for s in servers for gid in new_gids])


def server_by_host_id(servers: list[ServerInfo], host_ids, host_id) -> ServerInfo:
    return [s for s, hid in zip(servers, host_ids) if str(hid) == str(host_id)][0]


async def setup_table(manager: ScyllaClusterManager, cql, ks: str, servers: list[ServerInfo]):
    """Create a single-tablet table and wait until every server knows the leader of its group."""
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
    parent_gid = str((await get_tablet_raft_row(manager, ks, 'test')).raft_group_id)
    parent_leader = await wait_for_leader(manager, servers[0], parent_gid)
    # A replica which does not know the parent's leader when it starts a child cannot seed its
    # election, so make sure they all do.
    await gather_safely(*[wait_for_leader(manager, s, parent_gid, expected_host_id=parent_leader)
                          for s in servers])
    logger.info(f"Group {parent_gid} is led by {parent_leader}")
    return parent_gid, parent_leader


KEYSPACE_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                 " AND tablets = {'initial': 1} AND consistency = 'global'")


# The child groups elect the parent's leader on their own: each replica derives the seed of their
# fast bootstrap from the rank of the parent's leader, so no leadership has to be transferred.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_children_start_colocated(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        parent_gid, parent_leader = await setup_table(manager, cql, ks, servers)

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]

        new_gids = await record_split(manager, servers, ks, 'test')
        await wait_for_children_led_by(manager, servers, new_gids, parent_leader)

        for log, mark in zip(logs, marks):
            for gid in new_gids:
                assert await log.grep(f"co-locating child {gid} initial leader with parent {parent_gid} "
                                      f"leader {parent_leader}", from_mark=mark), \
                    f"{gid} did not derive its fast bootstrap seed from the parent's leader"
            assert not await log.grep("colocate_leaders: group .* transferring the leadership", from_mark=mark), \
                "a child's leadership had to be transferred, so its first election was not seeded"


# Once the leaders have drifted apart, the colocator brings them back together: the children
# follow the parent's leader when it moves, and a child whose leadership moves on its own is
# brought back to it.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_children_follow_parent_leader(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        parent_gid, parent_leader = await setup_table(manager, cql, ks, servers)
        new_gids = await record_split(manager, servers, ks, 'test')
        await wait_for_children_led_by(manager, servers, new_gids, parent_leader)

        logs = [await manager.server_open_log(s.server_id) for s in servers]

        # The parent moves: both children have to follow it.
        old_leader_server = server_by_host_id(servers, host_ids, parent_leader)
        new_leader = [hid for hid in host_ids if str(hid) != str(parent_leader)][0]
        logger.info(f"Moving the leadership of {parent_gid} from {parent_leader} to {new_leader}")
        mark = await logs[servers.index(old_leader_server)].mark()
        await trigger_stepdown(manager, old_leader_server, parent_gid, target_host_id=new_leader)
        await wait_for_children_led_by(manager, servers, new_gids, new_leader)
        # The old leader still led the children, so it is the one which handed them over.
        assert await logs[servers.index(old_leader_server)].grep(
                f"colocate_leaders: group .* is led by this node while its parent {parent_gid} is led by {new_leader}",
                from_mark=mark)

        # A child moves away on its own: it has to be brought back.
        leader_server = server_by_host_id(servers, host_ids, new_leader)
        drifted_child = new_gids[0]
        elsewhere = [hid for hid in host_ids if str(hid) != str(new_leader)][0]
        elsewhere_server = server_by_host_id(servers, host_ids, elsewhere)
        logger.info(f"Moving the leadership of {drifted_child} from {new_leader} to {elsewhere}")
        mark = await logs[servers.index(elsewhere_server)].mark()
        await trigger_stepdown(manager, leader_server, drifted_child, target_host_id=elsewhere)
        await wait_for_children_led_by(manager, servers, new_gids, new_leader)
        # Only the node the child moved to can hand it back.
        assert await logs[servers.index(elsewhere_server)].grep(
                f"colocate_leaders: group {drifted_child} is led by this node while its parent {parent_gid} is led by {new_leader}",
                from_mark=mark)


# The colocator runs for as long as the parent group exists, so it must not stand in the way of the
# group's deletion, nor of the shutdown of a node it runs on.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_colocator_stops_with_parent(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        parent_gid, parent_leader = await setup_table(manager, cql, ks, servers)
        new_gids = await record_split(manager, servers, ks, 'test')
        await wait_for_children_led_by(manager, servers, new_gids, parent_leader)

        # Shut down a node whose colocator is running. The colocator of a follower is parked
        # waiting for a leadership change, which only its abort ends.
        follower = [s for s, hid in zip(servers, host_ids) if str(hid) != str(parent_leader)][0]
        logger.info(f"Shutting down {follower} with its colocator running")
        await manager.server_stop_gracefully(follower.server_id, timeout=60)
        live = [s for s in servers if s.server_id != follower.server_id]
        cql, _ = await manager.get_ready_cql(live)

        # Drop the table: deleting the parent group stops its colocator on every live node.
        logs = [await manager.server_open_log(s.server_id) for s in live]
        marks = [await log.mark() for log in logs]
        await cql.run_async(f"DROP TABLE {ks}.test", timeout=60)
        for log, mark in zip(logs, marks):
            await log.wait_for(rf"leader_colocator\(.*-{parent_gid}\): stopping", from_mark=mark, timeout=60)
            assert not await log.grep(rf"leader_colocator\(.*-{parent_gid}\): stopped with an error", from_mark=mark)
