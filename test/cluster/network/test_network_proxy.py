#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The internode network of live nodes, cut and degraded through a TCP proxy.

Each node advertises the address of a proxy in front of its internode and CQL
ports, so its peers and the driver reach it through the proxy, where the bytes
between any two nodes can be withheld, delayed, throttled or their connections
refused, with no privileges and no change to Scylla. The tests here act on the
internode ports only, so the node keeps answering the test over CQL, and over the
REST API at its real address. See test/pylib/network_proxy.py for what a proxy
can and cannot do compared to a packet filter.

Which messages a node's proxy sees follows from how Scylla's verbs answer. A
request/response verb (gossip echo, read_data) answers on the connection the
request came in on, so a request to the proxied node and its answer both go
through the proxy. A write is two one-way verbs: the coordinator's mutation
goes to the replica on the coordinator's connection, and the replica's
mutation_done comes back on the replica's own connection to the coordinator,
through the coordinator's proxy. The rules name byte flows between two nodes and
apply on whichever proxy carries them, so a cut between two nodes holds both
connections' traffic, and a delay of a node's flows both ways is paid twice by
a write to it (mutation, mutation_done) as by a read from it (read_data, its
answer).
"""

import asyncio
import logging
import statistics
import time

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement

from test.cluster.network.util import (down_endpoints, metric, new_table, wait_for_all_up, wait_for_down)
from test.cluster.util import new_test_keyspace
from test.pylib.network_proxy import NetworkProxy
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


async def timed(cql, host, statements: list[str], cl: ConsistencyLevel = ConsistencyLevel.ALL) -> float:
    """The median duration of the statements, run one after the other through ``host``."""
    durations = []
    for statement in statements:
        start = time.time()
        await cql.run_async(SimpleStatement(statement, consistency_level=cl), host=host)
        durations.append(time.time() - start)
    return statistics.median(durations)


def inserts(table: str, count: int, value: str = "v") -> list[str]:
    return [f"INSERT INTO {table} (pk, v) VALUES ({i}, '{value}')" for i in range(count)]


def selects(table: str, count: int) -> list[str]:
    return [f"SELECT v FROM {table} WHERE pk = {i}" for i in range(count)]


async def test_proxied_cluster_forms(manager: ScyllaClusterManager, network_proxy: NetworkProxy) -> None:
    """A cluster of proxied nodes forms: the peers know each other by their proxy addresses in
    gossip and system.peers, the driver reaches them at their proxy addresses too, while the
    REST API answers at the real ones, and a write at ALL goes through every proxy.
    """
    servers = await manager.servers_add(3, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    proxied = {s.ip_addr: network_proxy.address_of(s.ip_addr) for s in servers}
    logger.info(f"Proxy addresses: {proxied}")
    assert len(set(proxied.values())) == 3 and not set(proxied.values()) & set(proxied)

    for server in servers:
        live = set(await manager.api.get_alive_endpoints(server.ip_addr))
        assert live == set(proxied.values()), f"{server} sees {live}"
        assert await manager.api.get_down_endpoints(server.ip_addr) == []

    peers = await cql.run_async("SELECT peer, rpc_address FROM system.peers", host=hosts[0])
    assert {str(r.peer) for r in peers} == {proxied[s.ip_addr] for s in servers[1:]}
    assert {str(r.rpc_address) for r in peers} == {proxied[s.ip_addr] for s in servers[1:]}
    assert {h.address for h in hosts} == set(proxied.values())
    assert {s.rpc_address for s in servers} == set(proxied.values())

    async with new_test_keyspace(manager, RF3) as ks:
        async with new_table(manager, ks, "pk int PRIMARY KEY, v text") as table:
            for host in hosts:
                await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 'x')", consistency_level=ConsistencyLevel.ALL),
                                    host=host)
            rows = await cql.run_async(SimpleStatement(f"SELECT v FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ALL),
                                       host=hosts[0])
            assert [r.v for r in rows] == ['x']


async def test_isolated_node_is_down_for_its_peers(manager: ScyllaClusterManager, network_proxy: NetworkProxy,
                                                   failure_detector_timeout: int) -> None:
    """A node cut off from both peers is DOWN for them and they for it, while the test still talks
    to all three. Writes at QUORUM through a peer go on, writes at ALL are unavailable, a read at
    ALL through the cut-off node is unavailable too, and everything recovers once the proxy is
    healed.

    The cut is symmetric, as a packet filter's would be, because the rules follow the pair of
    nodes, not the proxy: the cut-off node's own connections to its peers go through their
    proxies, where its bytes are withheld too.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    node_a, node_b, node_c = servers
    host_a, _, host_c = hosts
    proxy_c = network_proxy.address_of(node_c.ip_addr)

    async with new_test_keyspace(manager, RF3) as ks:
        async with new_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            def insert(v: int, cl: ConsistencyLevel):
                return cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, {v})", consistency_level=cl),
                                     host=host_a)

            def select(host):
                return cql.run_async(SimpleStatement(f"SELECT v FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ALL),
                                     host=host)

            await insert(1, ConsistencyLevel.ALL)

            logger.info(f"Stalling every connection to {node_c} (proxy {proxy_c})")
            await network_proxy.isolate(node_c.ip_addr, [node_a.ip_addr, node_b.ip_addr])

            took_a, took_b = await asyncio.gather(wait_for_down(manager, network_proxy, node_a, {node_c.ip_addr}),
                                                  wait_for_down(manager, network_proxy, node_b, {node_c.ip_addr}))
            logger.info(f"Peers marked {node_c} DOWN after {took_a:.1f}s / {took_b:.1f}s")
            # The cut-off node still answers the test over REST, and has lost both peers.
            took_c = await wait_for_down(manager, network_proxy, node_c, {node_a.ip_addr, node_b.ip_addr})
            logger.info(f"{node_c} marked both peers DOWN after {took_c:.1f}s")

            await insert(2, ConsistencyLevel.QUORUM)
            with pytest.raises((Unavailable, NoHostAvailable)):
                await insert(3, ConsistencyLevel.ALL)
            # Through the cut-off node a read at ALL has no replicas to go to.
            with pytest.raises((Unavailable, NoHostAvailable)):
                await select(host_c)

            logger.info("Healing")
            await network_proxy.heal()

            took = await asyncio.gather(*(wait_for_down(manager, network_proxy, s, set()) for s in servers))
            logger.info(f"All nodes UP again after {max(took):.1f}s")

            await insert(4, ConsistencyLevel.ALL)
            assert [r.v for r in await select(host_a)] == [4]


async def test_delayed_node_slows_requests_but_stays_up(manager: ScyllaClusterManager, network_proxy: NetworkProxy,
                                                        failure_detector_timeout: int) -> None:
    """Latency added to a node's flows shows up in requests at ALL through a peer, once each way
    for a write as for a read, without the failure detector marking the node DOWN, and goes
    away with the heal. A bandwidth limit then throttles a large write the same way.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    node_a, _, node_c = servers
    host_a = hosts[0]

    async with new_test_keyspace(manager, RF3) as ks:
        async with new_table(manager, ks, "pk int PRIMARY KEY, v text") as table:
            write_base = await timed(cql, host_a, inserts(table, 5))
            read_base = await timed(cql, host_a, selects(table, 5))
            logger.info(f"Baseline at ALL: write {write_base * 1000:.1f}ms, read {read_base * 1000:.1f}ms")

            # 300ms each way between C and its peers. A write through A pays it twice, for the
            # mutation to C and C's mutation_done; a read too, for read_data and its answer.
            await network_proxy.delay(node_c.ip_addr, 300, jitter_ms=50)
            write_slow = await timed(cql, host_a, inserts(table, 5))
            read_slow = await timed(cql, host_a, selects(table, 5))
            logger.info(f"With C delayed: write {write_slow * 1000:.1f}ms, read {read_slow * 1000:.1f}ms")
            assert write_slow >= write_base + 0.4, f"expected two delays of 300ms +/- 50ms, got {write_slow - write_base:.3f}s over the baseline"
            assert read_slow >= read_base + 0.4, f"expected two delays of 300ms +/- 50ms, got {read_slow - read_base:.3f}s over the baseline"

            # The failure detector tolerates the delay: nothing goes DOWN over a few of its timeouts.
            deadline = time.time() + 1.5 * failure_detector_timeout / 1000
            while time.time() < deadline:
                assert await manager.api.get_down_endpoints(node_a.ip_addr) == []
                assert await manager.api.get_down_endpoints(node_c.ip_addr) == []
                await asyncio.sleep(0.5)

            await network_proxy.heal()
            write_healed = await timed(cql, host_a, inserts(table, 5))
            read_healed = await timed(cql, host_a, selects(table, 5))
            logger.info(f"After heal: write {write_healed * 1000:.1f}ms, read {read_healed * 1000:.1f}ms")
            assert write_healed < write_base + 0.2 and read_healed < read_base + 0.2, "latency did not go away"

            # 16 kB/s towards C: a 64 kB mutation through A needs about four seconds to reach C.
            await network_proxy.bandwidth(node_c.ip_addr, 16, direction="upstream")
            throttled = await timed(cql, host_a, inserts(table, 1, value="x" * 65536))
            logger.info(f"64 kB write at ALL with C throttled to 16 kB/s: {throttled:.2f}s")
            assert throttled >= 2, f"expected about four seconds, got {throttled:.2f}s"

            await network_proxy.heal()
            unthrottled = await timed(cql, host_a, inserts(table, 1, value="x" * 65536))
            logger.info(f"64 kB write at ALL after heal: {unthrottled:.2f}s")
            assert unthrottled < 1


async def test_isolated_coordinator_stores_hints_and_delivers_them_after_heal(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int) -> None:
    """A node cut off from both peers keeps taking CL=ONE writes for a regular table, as a
    coordinator and the only reachable replica. It stores a hint per unreachable replica
    (storage_proxy::hint_to_dead_endpoints), and once the cut is removed hinted handoff
    delivers them: the two other replicas then hold the rows without a read repair having
    to copy them over.

    The proof is in the metrics: `written` on the coordinator grows by two hints per write,
    the majority reads none of the rows while the cut is up, `sent_total` catches up with
    `written` after the heal, and a read at ALL afterwards returns every row without the
    coordinator's read-repair diff counter moving.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    a, b, c = servers
    host_a, _, host_c = hosts
    writes = 20

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        async with new_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            written_before = await metric(manager, c, "scylla_hints_manager_written")
            sent_before = await metric(manager, c, "scylla_hints_manager_sent_total")

            logger.info(f"Cutting {c.ip_addr} off from {a.ip_addr} and {b.ip_addr}")
            await network_proxy.isolate(c.ip_addr, [a.ip_addr, b.ip_addr])
            await wait_for_down(manager, network_proxy, c, {a.ip_addr, b.ip_addr})
            await wait_for_down(manager, network_proxy, a, {c.ip_addr})
            await wait_for_down(manager, network_proxy, b, {c.ip_addr})

            for pk in range(writes):
                await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})", consistency_level=ConsistencyLevel.ONE),
                                    host=host_c)

            async def hints_written():
                written = await metric(manager, c, "scylla_hints_manager_written") - written_before
                return written if written >= 2 * writes else None
            written = await wait_for(hints_written, time.time() + 60, label=f"{2 * writes} hints written on {c.ip_addr}")
            logger.info(f"{c.ip_addr} wrote {written} hints")

            # The majority has none of it: a QUORUM read through A is answered by A and B.
            pks = ", ".join(str(pk) for pk in range(writes))
            rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {table} WHERE pk IN ({pks})", consistency_level=ConsistencyLevel.QUORUM),
                                       host=host_a)
            assert rows == [], f"the majority already has {[r.pk for r in rows]}"

            logger.info("Removing the cut")
            await network_proxy.heal()
            await wait_for_all_up(manager, network_proxy, servers)

            async def hints_sent():
                sent = await metric(manager, c, "scylla_hints_manager_sent_total") - sent_before
                return sent if sent >= written else None
            sent = await wait_for(hints_sent, time.time() + 120, label=f"{written} hints sent by {c.ip_addr}")
            logger.info(f"{c.ip_addr} sent {sent} hints")

            diff_before = await metric(manager, a, "scylla_storage_proxy_coordinator_read_repairs_diff_found_total")
            rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table} WHERE pk IN ({pks})", consistency_level=ConsistencyLevel.ALL),
                                       host=host_a)
            assert sorted((r.pk, r.v) for r in rows) == [(pk, pk) for pk in range(writes)]
            diff_after = await metric(manager, a, "scylla_storage_proxy_coordinator_read_repairs_diff_found_total")
            assert diff_after == diff_before, f"the replicas disagreed at ALL: hints did not deliver the rows ({diff_before} -> {diff_after})"


async def test_one_way_cut_converges_to_down_both_ways_and_graceful_stop_completes(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int) -> None:
    """A link that works in one direction only: A's bytes to B are withheld, B's reach A.
    Neither node's echo gets a reply, so each marks the other DOWN
    (gossiper::failure_detector_loop_for_node), and that view holds steady: no flapping while
    the cut is up, and the third node sees nobody down. A then shuts down gracefully with the
    half-open peer still there: the shutdown must not wait on a connection to B that can never
    complete (SCYLLADB-4693), and the peers must see A leave. After the heal A comes back and
    everyone is UP.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, _ = await manager.get_ready_cql(servers)
    a, b, c = servers
    a_id, b_id, _ = await gather_safely(*(manager.get_host_id(s.server_id) for s in servers))
    await wait_for_all_up(manager, network_proxy, servers)
    await asyncio.sleep(1)
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    logger.info(f"Cutting packets from {a.ip_addr} to {b.ip_addr}; the other way stays open")
    await network_proxy.cut(a.ip_addr, b.ip_addr)
    await wait_for_down(manager, network_proxy, a, {b.ip_addr})
    await wait_for_down(manager, network_proxy, b, {a.ip_addr})
    converged_at = time.monotonic()
    assert await down_endpoints(manager, network_proxy, c) == set()

    # Steady: every sample over the next while shows the same view.
    while time.monotonic() - converged_at < 8:
        views = await gather_safely(*(down_endpoints(manager, network_proxy, s) for s in servers))
        assert views == [{b.ip_addr}, {a.ip_addr}, set()], f"views changed: {views}"
        await asyncio.sleep(0.5)

    transitions = []
    for s, log, mark in zip(servers, logs, marks):
        lines = await log.grep(r"InetAddress .* is now (UP|DOWN)", from_mark=mark)
        transitions.append([line.strip() for line, _ in lines])
        logger.info(f"{s.ip_addr} gossip transitions: {transitions[-1]}")
    # One DOWN on each side of the cut, nothing on the third node, and no UP anywhere.
    assert all("is now UP" not in line for t in transitions for line in t), transitions
    assert len(transitions[0]) == 1 and b_id in transitions[0][0], transitions[0]
    assert len(transitions[1]) == 1 and a_id in transitions[1][0], transitions[1]
    assert transitions[2] == [], transitions[2]

    logger.info(f"Gracefully stopping {a.ip_addr} with the cut in place")
    t0 = time.monotonic()
    await manager.server_stop_gracefully(a.server_id, timeout=120)
    stop_s = time.monotonic() - t0
    logger.info(f"{a.ip_addr} stopped in {stop_s:.1f}s")
    assert stop_s < 60, f"graceful stop took {stop_s:.1f}s"
    await wait_for_down(manager, network_proxy, b, {a.ip_addr})
    await wait_for_down(manager, network_proxy, c, {a.ip_addr})

    logger.info("Removing the cut and starting the node again")
    await network_proxy.heal()
    await manager.server_start(a.server_id)
    await wait_for_all_up(manager, network_proxy, servers)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
