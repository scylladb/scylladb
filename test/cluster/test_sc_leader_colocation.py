#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import trigger_stepdown
from test.pylib.internal_types import ServerInfo
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, collect_all_raft_state, \
        wait_for_leader

import pytest
import logging
import time


logger = logging.getLogger(__name__)


# Co-location of the leaders of a strongly consistent tablet's child groups with the leader of its
# group, while a split of the tablet is recorded.
#
# Nothing records such a split on its own yet, so the tests have the topology coordinator do it
# through the sc_record_raft_split injection. The injection writes a split decision and the two
# child group ids, as the resize finalization will, and nothing carries the split any further.
# Every replica then starts the child groups next to the tablet's own group, and keeps them until
# the table is dropped.


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
    # A replica which does not know the parent's leader when it starts a child gives the child no
    # priority members, so make sure they all do.
    await gather_safely(*[wait_for_leader(manager, s, parent_gid, expected_host_id=parent_leader)
                          for s in servers])
    logger.info(f"Group {parent_gid} is led by {parent_leader}")
    return parent_gid, parent_leader


# The tests do not drop the keyspace, because every test has a cluster of its own, which goes
# away with the test.
KEYSPACE = 'ks'
KEYSPACE_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                 " AND tablets = {'initial': 1} AND consistency = 'global'")

# The rack of the last server. We don't prefer the rack of the first server, because that server
# leads group0 and the driver is slow to notice it going down, and a test which stops the node in
# the preferred rack would wait for both.
PREFERRED_RACK = 'rack3'
PREFERRED_RACK_KEYSPACE_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2', 'rack3']}"
                                " AND tablets = {'initial': 1} AND consistency = 'global'"
                                f" AND preferred_rack = {{'dc1': '{PREFERRED_RACK}'}}")


# The leaders of the children stay with the leader of their parent. The keyspace has no preferred
# rack, so the parent's leader is the only priority member of a child. The steps below share one
# cluster, because starting it takes about half of the time each of them would need on its own.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_children_stay_with_parent_leader(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE {KEYSPACE} {KEYSPACE_OPTS}")
    parent_gid, parent_leader = await setup_table(manager, cql, KEYSPACE, servers)
    new_gids = await record_split(manager, servers, KEYSPACE, 'test')
    await wait_for_children_led_by(manager, servers, new_gids, parent_leader)

    # The children elect the parent's leader on their own. Each replica sets the priority
    # members of a child before it starts the child, so the children elect their first leaders
    # there and no leadership has to be handed over.
    #
    # The parent belongs to a table which has just been created, so it has held a single
    # election. A child in the same term has held a single one too, which means that its first
    # election chose the parent's leader and that its leadership was never handed over.
    for host in hosts:
        state = await collect_all_raft_state(cql, host)
        _, parent_term, _ = state[parent_gid]
        for gid in new_gids:
            _, child_term, _ = state[gid]
            assert child_term == parent_term, \
                f"{gid} is in term {child_term} on {host}, its parent {parent_gid} in term {parent_term}"

    # Once the leaders have drifted apart, they are brought back together. The parent moves,
    # so both children have to follow it.
    old_leader_server = server_by_host_id(servers, host_ids, parent_leader)
    new_leader = [hid for hid in host_ids if str(hid) != str(parent_leader)][0]
    logger.info(f"Moving the leadership of {parent_gid} from {parent_leader} to {new_leader}")
    await trigger_stepdown(manager, old_leader_server, parent_gid, target_host_id=new_leader)
    await wait_for_children_led_by(manager, servers, new_gids, new_leader)

    # A child moves away on its own, so it has to be brought back. The node which stepped down
    # stops reporting itself as the child's leader, so the wait below cannot pass before the
    # child's leadership has returned to it.
    leader_server = server_by_host_id(servers, host_ids, new_leader)
    drifted_child = new_gids[0]
    elsewhere = [hid for hid in host_ids if str(hid) != str(new_leader)][0]
    logger.info(f"Moving the leadership of {drifted_child} from {new_leader} to {elsewhere}")
    await trigger_stepdown(manager, leader_server, drifted_child, target_host_id=elsewhere)
    await wait_for_children_led_by(manager, servers, new_gids, new_leader)

    # The colocator runs for as long as the node does, so it must not stand in the way of the
    # node's graceful shutdown, nor of the deletion of the groups it looks after.
    #
    # We shut down a follower with the resize in progress. Its colocator is waiting for a
    # leadership change, which only the shutdown ends. We don't pick the first server, because
    # the driver is slow to notice that one going down, and DROP TABLE below would then wait
    # for its schema version.
    follower = [s for s, hid in zip(servers[1:], host_ids[1:]) if str(hid) != str(new_leader)][0]
    follower_log = await manager.server_open_log(follower.server_id)
    mark = await follower_log.mark()
    logger.info(f"Shutting down {follower} with a resize in progress")
    await manager.server_stop_gracefully(follower.server_id, timeout=60)
    assert await follower_log.grep(r"leader_colocator\(\): stopping", from_mark=mark)
    live = [s for s in servers if s.server_id != follower.server_id]
    cql, _ = await manager.get_ready_cql(live)

    # Drop the table, which deletes the parent and its children on every live node.
    logs = [await manager.server_open_log(s.server_id) for s in live]
    marks = [await log.mark() for log in logs]
    await cql.run_async(f"DROP TABLE {KEYSPACE}.test", timeout=60)
    for log, mark in zip(logs, marks):
        assert not await log.grep(r"leader_colocator\(\): unexpected exception", from_mark=mark)


# With a preferred rack, the parent prefers the replica in that rack, and the children follow
# their parent. While the node in the preferred rack is down, the children are therefore led by
# the node which leads the parent, and they return to the preferred rack with their parent once
# the node is back.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_children_follow_parent_to_preferred_rack(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    preferred = [s for s in servers if s.rack == PREFERRED_RACK][0]
    live = [s for s in servers if s.server_id != preferred.server_id]
    preferred_host_id = host_ids[servers.index(preferred)]
    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE {KEYSPACE} {PREFERRED_RACK_KEYSPACE_OPTS}")
    parent_gid, parent_leader = await setup_table(manager, cql, KEYSPACE, servers)
    assert str(parent_leader) == str(preferred_host_id)

    logger.info(f"Stopping {preferred}, the node in the preferred rack")
    await manager.server_stop_gracefully(preferred.server_id)
    cql, _ = await manager.get_ready_cql(live)

    async def parent_led_elsewhere():
        leader = await manager.api.get_raft_leader(live[0].ip_addr, parent_gid)
        return leader if leader and str(leader) != str(preferred_host_id) else None
    new_leader = await wait_for(parent_led_elsewhere, time.time() + 60,
                                label=f"group {parent_gid} to be led outside the preferred rack")
    await gather_safely(*[wait_for_leader(manager, s, parent_gid, expected_host_id=new_leader) for s in live])

    # The parent is led outside the preferred rack, and the children follow its leader there.
    new_gids = await record_split(manager, live, KEYSPACE, 'test')
    await wait_for_children_led_by(manager, live, new_gids, new_leader)

    # Once the replica in the preferred rack is back, the parent returns to it, and so do the
    # children.
    await manager.server_start(preferred.server_id)
    await manager.servers_see_each_other(servers)
    await gather_safely(*[wait_for_leader(manager, s, gid, expected_host_id=preferred_host_id)
                          for s in servers for gid in [parent_gid] + new_gids])
