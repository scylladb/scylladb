#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for nodes whose own addresses differ. The harness gives every node a single
address (listen = rpc = broadcast), so these tests set the addresses apart explicitly.
SCYLLADB-6265 lists the code paths they cover.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

import psutil
import pytest
from cassandra.cluster import ConsistencyLevel, NoHostAvailable
from cassandra.connection import DefaultEndPoint
from cassandra.policies import AddressTranslator, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.pylib.host_registry import HostRegistry
from test.pylib.internal_types import IPAddress
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, scale_timeout_by_mode, wait_for

logger = logging.getLogger(__name__)

INTERNODE_PORT = 7000


class WrongAddress(Exception):
    """A node used the wrong address. The xfail markers expect only this exception, so a harness
    timeout, which raises AssertionError, does not count as the known bug."""


@asynccontextmanager
async def leased_ips(manager: ScyllaClusterManager, n: int):
    """Lease n extra IPs from the host registry for addresses the harness does not set.

    The nodes are stopped before the IPs are released, so that another test cannot lease
    an IP that a node still binds. If a stop fails, the IPs stay leased.
    """
    hosts = HostRegistry()
    ips = [IPAddress(await hosts.lease_host()) for _ in range(n)]
    try:
        yield ips
    finally:
        await gather_safely(*(manager.server_stop(s.server_id, convict=False)
                              for s in await manager.running_servers()))
        for ip in ips:
            await hosts.release_host(ip)


def internode_connections(ips: set[str]) -> set[tuple[str, str]]:
    """(local IP, remote IP) of every established internode connection that a node with one of `ips` dialed."""
    return {(c.laddr.ip, c.raddr.ip) for c in psutil.net_connections(kind='tcp4')
            if c.status == psutil.CONN_ESTABLISHED and c.raddr and c.raddr.port == INTERNODE_PORT
            and c.laddr.ip in ips}


class Translator(AddressTranslator):
    """Maps an advertised broadcast_rpc_address to the bound rpc_address, like a driver behind NAT."""
    def __init__(self, nat: dict[str, str]):
        self.nat = nat

    def translate(self, addr):
        return self.nat.get(addr, addr)


async def exclusive_session(manager: ScyllaClusterManager, rpc_address: IPAddress, nat: dict[str, str],
                            timeout: float):
    """A CQL session that talks only to the node bound at rpc_address.

    get_cql_exclusive() connects to the listen address, which is not the CQL address here.
    The node may not serve CQL yet: server_add(connect_driver=False) does not wait for the join.
    """
    async def connect():
        cluster = manager.con_gen([rpc_address], manager.port, manager.use_ssl, manager.auth_provider,
                                  WhiteListRoundRobinPolicy([rpc_address]))
        cluster.address_translator = Translator(nat)
        try:
            session = cluster.connect()
        except NoHostAvailable as e:
            logger.info("CQL connect to %s failed: %s", rpc_address, e)
            return None
        manager.exclusive_clusters.append(cluster)
        return session
    return await wait_for(connect, time.time() + timeout, period=1)


async def test_address_columns_and_replace(manager: ScyllaClusterManager, build_mode: str) -> None:
    """listen, broadcast, rpc and broadcast_rpc addresses all differ.

    listen_on_broadcast_address makes the node bind its broadcast address too, so peers can reach
    it without a NAT. The broadcast_rpc_address stands for a NAT address and is not bound.
    Checks what the nodes store and advertise, that internode connections run between broadcast
    addresses, and that a replace keeps system.peers right.
    """
    async with leased_ips(manager, 9) as ips:
        broadcast = ips[0:3]
        rpc = ips[3:6]
        broadcast_rpc = ips[6:9]
        nat = dict(zip(broadcast_rpc, rpc))
        # exclusive_session() waits for a whole bootstrap or replace.
        join_timeout = scale_timeout_by_mode(build_mode, 60)

        def config(i: int) -> dict:
            return {'broadcast_address': broadcast[i], 'listen_on_broadcast_address': True,
                    'rpc_address': rpc[i], 'broadcast_rpc_address': broadcast_rpc[i]}

        # connect_driver=False: the driver would try to reach the unbound broadcast_rpc addresses.
        servers = [await manager.server_add(config=config(i), seeds=[broadcast[0]], connect_driver=False,
                                            property_file={'dc': 'dc1', 'rack': f'rack{i + 1}'})
                   for i in range(2)]

        async def check_addresses(nodes: list[int], listen: list[IPAddress]):
            # A node serves CQL only after it joined, so connect to every node before checking any.
            sessions = [await exclusive_session(manager, rpc[i], nat, join_timeout) for i in nodes]
            for n, i in enumerate(nodes):
                cql = sessions[n]
                local = cql.execute("SELECT listen_address, broadcast_address, rpc_address FROM system.local").one()
                assert (str(local.listen_address), str(local.broadcast_address), str(local.rpc_address)) == \
                       (listen[n], broadcast[i], broadcast_rpc[i])

                # rpc_address comes from gossip and may arrive after the join. GossipingPropertyFileSnitch
                # gossips the listen address as INTERNAL_IP even without prefer_local.
                expected = {(broadcast[j], broadcast_rpc[j], listen[m]) for m, j in enumerate(nodes) if j != i}
                peers = set()
                async def peers_as_expected():
                    nonlocal peers
                    peers = {(str(r.peer), str(r.rpc_address) if r.rpc_address else None,
                              str(r.preferred_ip) if r.preferred_ip else None)
                             for r in cql.execute("SELECT peer, rpc_address, preferred_ip FROM system.peers")}
                    return True if peers == expected else None
                try:
                    await wait_for(peers_as_expected, time.time() + 30, period=.5)
                except Exception:
                    pytest.fail(f"node {i + 1}: system.peers {sorted(peers, key=str)}, expected {sorted(expected)}")

            # Outgoing connections bind the broadcast address and dial the peer's broadcast address.
            # A node also dials its own broadcast address; skip those.
            own = {str(a) for a in listen} | {broadcast[i] for i in nodes}
            conns = {(l, r) for l, r in internode_connections(own) if l != r}
            assert conns and conns <= {(broadcast[i], broadcast[j]) for i in nodes for j in nodes if i != j}

        await check_addresses([0, 1], [s.ip_addr for s in servers])

        cql = await exclusive_session(manager, rpc[0], nat, join_timeout)
        cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}")
        cql.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
        cql.execute(SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ALL))

        # Replace node 2 with a node that has new addresses of every kind.
        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.others_not_see_server(broadcast[1])
        replace_cfg = ReplaceConfig(replaced_id=servers[1].server_id, reuse_ip_addr=False, use_host_id=True,
                                    wait_dead=False)
        new = await manager.server_add(replace_cfg=replace_cfg, config=config(2), seeds=[broadcast[0]],
                                       connect_driver=False, property_file={'dc': 'dc1', 'rack': 'rack2'})

        await check_addresses([0, 2], [servers[0].ip_addr, new.ip_addr])

        cql = await exclusive_session(manager, rpc[2], nat, join_timeout)
        row = cql.execute(SimpleStatement("SELECT v FROM ks.t WHERE pk = 1", consistency_level=ConsistencyLevel.ALL)).one()
        assert row.v == 1


@pytest.mark.xfail(reason="SCYLLADB-6286", raises=WrongAddress, strict=True)
async def test_prefer_local_dials_listen_address(manager: ScyllaClusterManager) -> None:
    """With prefer_local, a node dials a same-DC peer at its INTERNAL_IP, which is the listen address.

    listen_on_broadcast_address keeps the broadcast addresses reachable, so the test can tell
    which address a node dials. Checked on a fresh cluster and after a restart, which reloads the
    preferred IPs from system.peers.
    """
    async with leased_ips(manager, 3) as broadcast:
        dcs = ['dc1', 'dc1', 'dc2']
        racks = ['rack1', 'rack2', 'rack1']
        servers = [await manager.server_add(config={'broadcast_address': broadcast[i], 'listen_on_broadcast_address': True},
                                            seeds=[broadcast[0]],
                                            property_file={'dc': dcs[i], 'rack': racks[i], 'prefer_local': 'true'})
                   for i in range(3)]
        listen = [s.ip_addr for s in servers]

        # preferred_ip comes from gossip and may arrive after the join.
        for i in range(3):
            cql = await manager.get_cql_exclusive(servers[i])
            expected = {broadcast[j]: listen[j] for j in range(3) if j != i}
            peers = {}
            async def peers_as_expected():
                nonlocal peers
                peers = {str(r.peer): str(r.preferred_ip) if r.preferred_ip else None
                         for r in cql.execute("SELECT peer, preferred_ip FROM system.peers")}
                return True if peers == expected else None
            try:
                await wait_for(peers_as_expected, time.time() + 30, period=.5)
            except Exception:
                pytest.fail(f"node {i + 1}: system.peers {peers}, expected {expected}")

        # The source is the broadcast address because of listen_on_broadcast_address.
        # Each node must reach a same-DC peer at its preferred IP, and a peer in another DC at its
        # broadcast address only. A node may also keep the gossip connection it opened to the seed's
        # broadcast address before it knew the topology.
        required = {(broadcast[i], listen[j] if dcs[i] == dcs[j] else broadcast[j])
                    for i in range(3) for j in range(3) if i != j}
        forbidden = {(broadcast[i], listen[j]) for i in range(3) for j in range(3) if dcs[i] != dcs[j]}
        same_dc_listen = required - {(broadcast[i], broadcast[j]) for i in range(3) for j in range(3)}

        node_of = {str(a): i for i in range(3) for a in (listen[i], broadcast[i])}

        async def wait_for_preferred_ips(when: str) -> tuple[bool, str] | None:
            """Returns None if the connections become as expected, else whether only same-DC dials
            to the listen address are missing, which is SCYLLADB-6286, and what the connections were."""
            last: set[tuple[str, str]] | None = None
            async def connected_as_expected():
                nonlocal last
                last = None
                # Gossip dials every peer from every node. The CL=ALL write also checks that the
                # cluster serves writes.
                manager.get_cql().execute(SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)",
                                                          consistency_level=ConsistencyLevel.ALL))
                # A node also connects to its own broadcast address; that is not a peer connection.
                last = {c for c in internode_connections(set(node_of)) if node_of[c[0]] != node_of[c[1]]}
                return True if required <= last and not forbidden & last else None
            try:
                await wait_for(connected_as_expected, time.time() + 30, period=1)
                return None
            except AssertionError as e:
                if last is None:
                    raise  # The last write failed, so the connections were not checked.
                missing = required - last
                return (missing <= same_dc_listen and not forbidden & last,
                        f"{when}: internode connections {sorted(last)}, missing {sorted(missing)}, "
                        f"forbidden {sorted(forbidden & last)}; {e}")

        cql = manager.get_cql()
        cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}")
        cql.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
        fresh = await wait_for_preferred_ips("fresh cluster")

        # Seed by broadcast address. By default the harness restarts a node with the listen addresses
        # of the others as seeds, and gossip to a seed's listen address would break the checks above.
        for s in servers:
            await manager.server_stop_gracefully(s.server_id)
            await manager.server_start(s.server_id, seeds=[broadcast[0]])
        await manager.driver_connect()
        await manager.get_ready_cql(servers)
        # SCYLLADB-6286 breaks only the fresh cluster, so a failure here is a separate bug.
        after_restart = await wait_for_preferred_ips("after restart")
        if after_restart:
            pytest.fail("\n".join(message for _, message in filter(None, [fresh, after_restart])))
        if fresh:
            known, message = fresh
            assert known, message
            raise WrongAddress(message)


@pytest.mark.xfail(reason="SCYLLADB-6285", raises=WrongAddress, strict=True)
async def test_cql_events_carry_cql_address(manager: ScyllaClusterManager) -> None:
    """Drivers match TOPOLOGY_CHANGE and STATUS_CHANGE events to hosts by the CQL address,
    so the events must carry it, not the internode address."""
    async with leased_ips(manager, 3) as rpc:
        servers = [await manager.server_add(config={'rpc_address': rpc[i]}) for i in range(2)]

        events = []
        def on_event(event):
            logger.info("CQL event %s", event)
            events.append((event['change_type'], event['address'][0]))

        cluster = manager.get_cql().cluster
        conn = await asyncio.get_running_loop().run_in_executor(
            None, cluster.connection_factory, DefaultEndPoint(rpc[0], manager.port))
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: conn.register_watchers({'TOPOLOGY_CHANGE': on_event, 'STATUS_CHANGE': on_event}))

            # Waits for an event of the new node under either of its addresses, so that the
            # check below reports which address the event carried.
            async def wait_for_event(change_type: str, since: int):
                async def seen():
                    return True if any(t == change_type and a in (s3.ip_addr, rpc[2])
                                       for t, a in events[since:]) else None
                try:
                    await wait_for(seen, time.time() + 60, period=.5, label=change_type)
                except AssertionError:
                    pytest.fail(f"no {change_type} event for node 3, events {events[since:]}")

            s3 = await manager.server_add(config={'rpc_address': rpc[2]}, start=False)
            n = len(events)
            await manager.server_start(s3.server_id)
            await wait_for_event('NEW_NODE', n)
            n = len(events)
            await manager.server_stop_gracefully(s3.server_id)
            await wait_for_event('DOWN', n)
            n = len(events)
            await manager.server_start(s3.server_id)
            await wait_for_event('UP', n)
            n = len(events)
            await manager.decommission_node(s3.server_id)
            await wait_for_event('REMOVED_NODE', n)
        finally:
            conn.close()

        listen = [s.ip_addr for s in servers] + [s3.ip_addr]
        logger.info("listen addresses %s, CQL addresses %s", listen, rpc)
        # Events about nodes 1 and 2 may arrive too: node 1 may still owe the NEW_NODE of node 2.
        wrong = {a for _, a in events} - set(rpc)
        if wrong:
            # SCYLLADB-6285 sends the gossip address, which is the listen address here.
            assert wrong <= set(listen), f"events {events} carry unknown addresses {sorted(wrong - set(listen))}"
            raise WrongAddress(f"events {events} carry non-CQL addresses {sorted(wrong)}")
        assert {t for t, a in events if a == rpc[2]} >= {'NEW_NODE', 'DOWN', 'UP', 'REMOVED_NODE'}
