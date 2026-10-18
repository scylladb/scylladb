#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Global strong consistency across two data centers.

Three nodes in dc1 and two in dc2, plus a third dc2 node outside the RF where a coordinator without
a replica is needed, and a keyspace replicated 3 + 2 with a single tablet. Every replica of a strongly
consistent tablet is a voter of its raft group, so the quorum is 3 of 5: dc1 on its own can elect a
leader and commit, dc2 on its own cannot. Nothing in the write and read paths looks at the DC. The
consistency level of a statement only picks the kind of read: QUORUM and LOCAL_QUORUM are both a
read served by the leader (the linearizable read type of
cql3/statements/strong_consistency/select_statement.cc), ONE and LOCAL_ONE both a local read served
by a replica without waiting for it to catch up. Writes here go at the session's default
LOCAL_QUORUM and are forwarded to the leader like QUORUM ones.

Linearizability itself is not checked here: the tests check that an acknowledged write is returned
by every leader read right away and by local reads once the replica caught up, with one client and
one key at a time.
"""
import logging
import time
from typing import NamedTuple

import pytest
from cassandra import ConsistencyLevel, ReadFailure, ReadTimeout, Unavailable, WriteFailure, WriteTimeout
from cassandra.cluster import Session
from cassandra.policies import FallthroughRetryPolicy
from cassandra.pool import Host
from cassandra.query import PreparedStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import gather_safely, wait_for

logger = logging.getLogger(__name__)

RF = {'dc1': 3, 'dc2': 2}
KS_OPTS = (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {RF['dc1']}, 'dc2': {RF['dc2']}}}"
           " AND tablets = {'initial': 1} AND consistency = 'global'")
SCHEMA = "pk int PRIMARY KEY, v int"
LEADER_READS = (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM)
LOCAL_READS = (ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE)
KEYS = 10

WRITE_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_write_node_bounces'
# Forwards of a CQL request by the transport layer: those served by their target, and those the
# target answered with a redirect to another node, which the forwarding node then follows.
FORWARDED = 'scylla_transport_requests_forwarded_successfully'
REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
COUNTERS = (WRITE_NODE_BOUNCES, FORWARDED, REDIRECTED)


class Node(NamedTuple):
    server: ServerInfo
    host: Host  # the driver's view of the node, to route a statement to it
    host_id: str


async def two_dc_cluster(manager: ScyllaClusterManager, extra_dc2: int = 0, config: dict = DEFAULT_CONFIG) -> tuple[Session, list[Node], list[Node]]:
    """Start three nodes in dc1 and 2 + extra_dc2 in dc2 and return the session with the nodes of each DC.
    Every node is a rack of its own, except the extra dc2 nodes, which share the last dc2 rack:
    rf_rack_valid_keyspaces wants the RF of a DC to equal its number of racks, so with 2 replicas in
    dc2 an extra node there holds no replica of a single tablet."""
    layout = ([('dc1', f'rack{i + 1}') for i in range(RF['dc1'])]
              + [('dc2', f'rack{min(i + 1, RF["dc2"])}') for i in range(RF['dc2'] + extra_dc2)])
    servers = await manager.servers_add(len(layout), config=config, cmdline=DEFAULT_CMDLINE,
                                        property_file=[{'dc': dc, 'rack': rack} for dc, rack in layout])
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*(manager.get_host_id(s.server_id) for s in servers))
    nodes = [Node(s, h, str(hid)) for s, h, hid in zip(servers, hosts, host_ids)]
    return cql, [n for n in nodes if n.server.datacenter == 'dc1'], [n for n in nodes if n.server.datacenter == 'dc2']


async def group_led_by(manager: ScyllaClusterManager, leader: Node, nodes: list[Node], ks: str, table: str) -> tuple[str, list[Node]]:
    """The raft group of the single tablet of `table`, with its leader moved to `leader`, and the nodes
    holding its replicas. Waits until every replica names the new leader, so that a request through
    any of them goes to it in one hop."""
    table_name = table.split('.')[-1]
    group_id = await get_table_raft_group_id(manager, ks, table_name)
    replica_ids = {str(hid) for hid, _ in await get_tablet_replicas(manager, leader.server, ks, table_name, 0)}
    replicas = [n for n in nodes if n.host_id in replica_ids]
    assert leader in replicas, f"{leader.server} holds no replica: {replica_ids}"
    await ensure_raft_group_leader_on(manager, leader.server, group_id)
    await gather_safely(*(wait_for_leader(manager, n.server, group_id, expected_host_id=leader.host_id) for n in replicas))
    logger.info(f"group {group_id} led by {leader.server}, replicas {[str(n.server) for n in replicas]}")
    return group_id, replicas


def statements(cql: Session, table: str) -> tuple[PreparedStatement, PreparedStatement]:
    """A prepared insert and select of one row. A bound statement carries a routing key, so the driver
    sends every request for a key to the same shard of the coordinator, where the leader cache
    lives. The driver is kept from retrying a failed request on its own: with the default policy a
    timed-out QUORUM read is re-sent at ONE."""
    insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
    select = cql.prepare(f"SELECT v FROM {table} WHERE pk = ?")
    for stmt in (insert, select):
        stmt.retry_policy = FallthroughRetryPolicy()
    return insert, select


async def write(cql: Session, insert: PreparedStatement, node: Node, pk: int, v: int) -> None:
    await cql.run_async(insert.bind([pk, v]), host=node.host)


async def read(cql: Session, select: PreparedStatement, node: Node, pk: int, cl: ConsistencyLevel) -> int | None:
    stmt = select.bind([pk])
    stmt.consistency_level = cl
    rows = await cql.run_async(stmt, host=node.host)
    return rows[0].v if rows else None


async def wait_until_leaderless(manager: ScyllaClusterManager, node: Node, group_id: str) -> None:
    """Wait until `node` names no leader of `group_id`: its election timeout passed and no election was won."""
    async def leaderless() -> bool | None:
        return None if await manager.api.get_raft_leader(node.server.ip_addr, group_id) else True
    await wait_for(leaderless, time.time() + 60)


def cl_name(cl: ConsistencyLevel) -> str:
    return ConsistencyLevel.value_to_name[cl]


async def counters(manager: ScyllaClusterManager, nodes: list[Node]) -> dict[Node, dict[str, int]]:
    metrics = await gather_safely(*(manager.metrics.query(n.server.ip_addr) for n in nodes))
    return {n: {name: m.get(name) or 0 for name in COUNTERS} for n, m in zip(nodes, metrics)}


async def test_cross_dc_read_after_write(manager: ScyllaClusterManager):
    """Idea: a write acknowledged through dc2 is seen right away by reads served by the leader,
    which is in dc1, on every node of both DCs. Reads served locally by a replica may miss it until
    that replica caught up, which a read barrier forces.

    Setup: three nodes in dc1 and three in dc2, one of them outside the RF, so that dc2 has both a
    replica and a plain coordinator to write through; the leader in dc1.

    For each of the two dc2 coordinators, the test writes a new value for pk 0 through it and then
    reads pk 0 on every node of both DCs: QUORUM and LOCAL_QUORUM must return the new value, ONE
    and LOCAL_ONE the old or the new one and never an error. Then each replica passes a read
    barrier, after which its ONE and LOCAL_ONE reads must return the new value, as must the ones
    through the node without a replica, which hands them to the nearest replica, in dc2.
    """
    cql, dc1, dc2 = await two_dc_cluster(manager, extra_dc2=1)
    async with new_test_keyspace(manager, KS_OPTS) as ks, new_test_table(manager, ks, SCHEMA) as table:
        group_id, replicas = await group_led_by(manager, dc1[0], dc1 + dc2, ks, table)
        insert, select = statements(cql, table)
        dc2_replica = next(n for n in dc2 if n in replicas)
        dc2_other = next(n for n in dc2 if n not in replicas)
        prev = None
        for v, coordinator in enumerate((dc2_replica, dc2_other), 1):
            await write(cql, insert, coordinator, 0, v)
            # Leader reads see the write on every node at once, local reads may still lag.
            for node in dc1 + dc2:
                where = f"on {node.server} after a write via {coordinator.server}"
                for cl in LEADER_READS:
                    assert await read(cql, select, node, 0, cl) == v, f"{cl_name(cl)} {where}"
                for cl in LOCAL_READS:
                    assert await read(cql, select, node, 0, cl) in (prev, v), f"{cl_name(cl)} {where}"
            # Local reads see it once the replica caught up.
            for node in replicas:
                await read_barrier(manager.api, node.server.ip_addr, group_id, timeout=60)
                for cl in LOCAL_READS:
                    assert await read(cql, select, node, 0, cl) == v, f"{cl_name(cl)} on {node.server} after its read barrier"
            # The node without a replica hands them to the nearest replica, in dc2, which caught up above.
            for cl in LOCAL_READS:
                assert await read(cql, select, dc2_other, 0, cl) == v, f"{cl_name(cl)} on {dc2_other.server} after the barriers"
            prev = v


async def test_cross_dc_forwarding_prefers_local_replica(manager: ScyllaClusterManager):
    """Idea: a write sent to a dc2 node holding no replica goes to the nearest live replica first,
    the one in its own rack (service/strong_consistency/coordinator.cc closest_replica sorts by
    rack, then DC), which as a follower answers with a redirect to the leader in dc1; the
    coordinator forwards the write there and caches the leader, so the next write to the key goes
    to dc1 straight away.

    Setup: as in test_cross_dc_read_after_write, with the leader in dc1. The coordinator is the dc2
    node without a replica, its neighbour the replica in the same rack.

    The test writes pk 0 twice through the coordinator and checks how three counters grew on every
    node: the strongly consistent node bounces, and the transport forwards that succeeded and that
    came back as a redirect. The first write costs the coordinator one bounce, one redirect and one
    forward, and the neighbour one bounce; the second costs the coordinator one bounce and one
    forward. Nothing else moves, the leader included. The counts hold only while the leader stays
    put, which it has just been made; the two writes take milliseconds.
    """
    cql, dc1, dc2 = await two_dc_cluster(manager, extra_dc2=1)
    async with new_test_keyspace(manager, KS_OPTS) as ks, new_test_table(manager, ks, SCHEMA) as table:
        _, replicas = await group_led_by(manager, dc1[0], dc1 + dc2, ks, table)
        insert, _ = statements(cql, table)
        coordinator = next(n for n in dc2 if n not in replicas)
        neighbour = next(n for n in dc2 if n in replicas and n.server.rack == coordinator.server.rack)
        nodes = dc1 + dc2
        # The expected growth of the counters per node for each write; a node or a counter not
        # listed must not move.
        first = {coordinator: {WRITE_NODE_BOUNCES: 1, FORWARDED: 1, REDIRECTED: 1},
                 neighbour: {WRITE_NODE_BOUNCES: 1}}
        second = {coordinator: {WRITE_NODE_BOUNCES: 1, FORWARDED: 1}}
        for v, expected in ((1, first), (2, second)):
            before = await counters(manager, nodes)
            await write(cql, insert, coordinator, 0, v)
            after = await counters(manager, nodes)
            for node in nodes:
                want = {name: expected.get(node, {}).get(name, 0) for name in COUNTERS}
                got = {name: after[node][name] - before[node][name] for name in COUNTERS}
                assert got == want, f"write {v}: counters of {node.server} grew by {got}, expected {want}"


async def test_minority_dc_loss(manager: ScyllaClusterManager):
    """Idea: with both dc2 nodes down the three dc1 replicas are still a majority of the five
    voters, so the group keeps its leader and commits; dc2 catches up from the leader's log once
    it is back.

    Setup: three nodes in dc1 and two in dc2; the leader in dc1.

    The test stops both dc2 nodes, writes ten keys through the dc1 nodes in turn and reads each
    back at QUORUM through the node that wrote it. Then it starts dc2 again, and after a read
    barrier on each dc2 node, LOCAL_ONE reads on it must return every key.
    """
    cql, dc1, dc2 = await two_dc_cluster(manager)
    async with new_test_keyspace(manager, KS_OPTS) as ks, new_test_table(manager, ks, SCHEMA) as table:
        group_id, _ = await group_led_by(manager, dc1[0], dc1 + dc2, ks, table)
        insert, select = statements(cql, table)
        await gather_safely(*(manager.server_stop(n.server.server_id, convict=True) for n in dc2))
        # dc1 alone keeps writing and reading.
        for pk in range(KEYS):
            node = dc1[pk % len(dc1)]
            await write(cql, insert, node, pk, pk)
            assert await read(cql, select, node, pk, ConsistencyLevel.QUORUM) == pk
        # dc2 back and caught up.
        await gather_safely(*(manager.server_start(n.server.server_id) for n in dc2))
        await manager.get_ready_cql([n.server for n in dc1 + dc2])
        for node in dc2:
            await read_barrier(manager.api, node.server.ip_addr, group_id, timeout=60)
            for pk in range(KEYS):
                got = await read(cql, select, node, pk, ConsistencyLevel.LOCAL_ONE)
                assert got == pk, f"LOCAL_ONE on {node.server} after its read barrier"


async def test_majority_dc_loss(manager: ScyllaClusterManager):
    """Idea: with all three dc1 nodes down the two dc2 replicas can neither elect a leader nor
    commit, so writes and leader reads sent to dc2 fail, while local reads still answer with what
    the replica has applied. Once dc1 is back every acknowledged write is there and no failed one,
    as nothing could be appended to the log without a leader.

    Setup: three nodes in dc1 and two in dc2; the leader in dc1; request timeouts of 5 s instead
    of the harness's 30 s, to bound the requests that have to time out.

    The test writes ten keys through the dc2 nodes and has each dc2 node pass a read barrier, as a
    local read does not wait for the replica to catch up. Then it stops dc1 and asks each dc2 node
    in turn for a write and a leader read (QUORUM through the first node, LOCAL_QUORUM through the
    second), which must fail, and for a LOCAL_ONE read of every key, which must succeed. The first
    node is asked at once, while it still names the dead leader: its requests fail fast, forwarding
    to it. The second is asked once it reports no leader: its requests wait for one that dc2 cannot
    elect and time out. Then dc1 is started again, and after a read barrier on a dc2 node QUORUM
    reads there must return the ten keys and none of the two failed writes.
    """
    config = DEFAULT_CONFIG | {'write_request_timeout_in_ms': 5000, 'read_request_timeout_in_ms': 5000}
    cql, dc1, dc2 = await two_dc_cluster(manager, config=config)
    async with new_test_keyspace(manager, KS_OPTS) as ks, new_test_table(manager, ks, SCHEMA) as table:
        group_id, _ = await group_led_by(manager, dc1[0], dc1 + dc2, ks, table)
        insert, select = statements(cql, table)
        for pk in range(KEYS):
            await write(cql, insert, dc2[pk % len(dc2)], pk, pk)
        for node in dc2:
            await read_barrier(manager.api, node.server.ip_addr, group_id, timeout=60)

        await gather_safely(*(manager.server_stop(n.server.server_id, convict=True) for n in dc1))
        try:
            for i, (node, cl) in enumerate(zip(dc2, LEADER_READS)):
                if i:
                    await wait_until_leaderless(manager, node, group_id)
                with pytest.raises((WriteTimeout, WriteFailure, Unavailable)) as e:
                    await write(cql, insert, node, KEYS + i, KEYS + i)
                logger.info(f"write via {node.server} failed with {e.value!r}")
                with pytest.raises((ReadTimeout, ReadFailure, Unavailable)) as e:
                    await read(cql, select, node, 0, cl)
                logger.info(f"{cl_name(cl)} read via {node.server} failed with {e.value!r}")
                for pk in range(KEYS):
                    got = await read(cql, select, node, pk, ConsistencyLevel.LOCAL_ONE)
                    assert got == pk, f"LOCAL_ONE on {node.server} without a leader"
        finally:
            # Also on a failed assertion: without dc1 group0 has no quorum either, and dropping the
            # table would hang instead of reporting the failure.
            await gather_safely(*(manager.server_start(n.server.server_id) for n in dc1))
        # The barrier passes once the group has a leader again.
        await manager.get_ready_cql([n.server for n in dc1 + dc2])
        await read_barrier(manager.api, dc2[0].server.ip_addr, group_id, timeout=60)
        for pk in range(KEYS + len(dc2)):
            want = pk if pk < KEYS else None  # the failed writes left nothing behind
            assert await read(cql, select, dc2[0], pk, ConsistencyLevel.QUORUM) == want, f"pk {pk} after dc1 came back"
