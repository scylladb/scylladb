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
connections' traffic, and a delay on a node's proxy is paid once by a write to
it and twice by a read from it.
"""

import asyncio
import logging
import statistics
import time

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import IPAddress, ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


async def wait_for_down(manager: ScyllaClusterManager, server: ServerInfo, expected: set[IPAddress], timeout: float = 60) -> float:
    """Wait until ``server`` reports exactly ``expected`` as DOWN; return how long it took."""
    start = time.time()

    async def done():
        return True if set(await manager.api.get_down_endpoints(server.ip_addr)) == expected else None
    await wait_for(done, start + timeout, period=0.2)
    return time.time() - start


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
    proxied = {s.ip_addr: await network_proxy.address_of(s.ip_addr) for s in servers}
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
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v text") as table:
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
    proxy_a, proxy_b, proxy_c = [await network_proxy.address_of(s.ip_addr) for s in servers]

    async with new_test_keyspace(manager, RF3) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            def insert(v: int, cl: ConsistencyLevel):
                return cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, {v})", consistency_level=cl),
                                     host=host_a)

            def select(host):
                return cql.run_async(SimpleStatement(f"SELECT v FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ALL),
                                     host=host)

            await insert(1, ConsistencyLevel.ALL)

            logger.info(f"Stalling every connection to {node_c} (proxy {proxy_c})")
            await network_proxy.isolate(node_c.ip_addr, [node_a.ip_addr, node_b.ip_addr])

            took_a, took_b = await asyncio.gather(wait_for_down(manager, node_a, {proxy_c}), wait_for_down(manager, node_b, {proxy_c}))
            logger.info(f"Peers marked {node_c} DOWN after {took_a:.1f}s / {took_b:.1f}s")
            # The cut-off node still answers the test over REST, and has lost both peers.
            took_c = await wait_for_down(manager, node_c, {proxy_a, proxy_b})
            logger.info(f"{node_c} marked both peers DOWN after {took_c:.1f}s")

            await insert(2, ConsistencyLevel.QUORUM)
            with pytest.raises((Unavailable, NoHostAvailable)):
                await insert(3, ConsistencyLevel.ALL)
            # Through the cut-off node a read at ALL has no replicas to go to.
            with pytest.raises((Unavailable, NoHostAvailable)):
                await select(host_c)

            logger.info("Healing")
            await network_proxy.heal()

            took = await asyncio.gather(*(wait_for_down(manager, s, set()) for s in servers))
            logger.info(f"All nodes UP again after {max(took):.1f}s")

            await insert(4, ConsistencyLevel.ALL)
            assert [r.v for r in await select(host_a)] == [4]


async def test_delayed_node_slows_requests_but_stays_up(manager: ScyllaClusterManager, network_proxy: NetworkProxy,
                                                        failure_detector_timeout: int) -> None:
    """Latency added on a node's proxy shows up in requests at ALL through a peer, once for a
    write and twice for a read, without the failure detector marking the node DOWN, and goes
    away with the heal. A bandwidth limit then throttles a large write the same way.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    node_a, _, node_c = servers
    host_a = hosts[0]

    async with new_test_keyspace(manager, RF3) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v text") as table:
            write_base = await timed(cql, host_a, inserts(table, 5))
            read_base = await timed(cql, host_a, selects(table, 5))
            logger.info(f"Baseline at ALL: write {write_base * 1000:.1f}ms, read {read_base * 1000:.1f}ms")

            # 300ms each way on the connections to C. A write through A pays it once, for the
            # mutation; a read pays it twice, for read_data and its answer.
            await network_proxy.delay(node_c.ip_addr, 300, jitter_ms=50)
            write_slow = await timed(cql, host_a, inserts(table, 5))
            read_slow = await timed(cql, host_a, selects(table, 5))
            logger.info(f"With C delayed: write {write_slow * 1000:.1f}ms, read {read_slow * 1000:.1f}ms")
            assert write_slow >= write_base + 0.2, f"expected one delay of 300ms +/- 50ms, got {write_slow - write_base:.3f}s over the baseline"
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
