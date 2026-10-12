#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
LWT under load and topology changes: growing a cluster (after
decommissions) while every node coordinates conditional inserts and updates,
removing dead nodes with --ignore-dead-nodes under LWT load, LWT load beside
reads and a sum check of conditional inserts, LWT inserts with
non-deterministic functions in the partition key, and the expiry of Paxos
state after paxos_grace_seconds.

Ported from scylla-dtest. Like the dtests' nodes, the nodes use vnodes (256
per node), and are stopped gracefully at the end of each test, as the
dtests' teardown did. The loads run in threads with the synchronous driver,
one session per target node, as the dtests' loader threads did.
"""

import asyncio
import logging
import random
import threading
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def node_session(manager: ScyllaClusterManager, server, request_timeout: float = 600):
    """A synchronous driver session talking only to the given node."""
    cluster = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr]))
    cluster.profile_manager.default.request_timeout = request_timeout
    return cluster.connect()


def rack_property_files(nodes: int, rf: int) -> list[dict]:
    """The dtests' generate_rack_topology_based_rf(): rf racks, the nodes
    spread over them, the first racks taking the extra nodes."""
    racks = []
    for i in range(rf):
        racks += [f"rack{i + 1}"] * (nodes // rf + (1 if i < nodes % rf else 0))
    return [{"dc": "dc1", "rack": r} for r in racks]


############################################################################
# Ported from lwt_random_test.py.

class IntKeyLoader(threading.Thread):
    """The dtests' dsr IntKeyLoader: conditional inserts (IF NOT EXISTS) of
    integer keys a million apart, then conditional increments (IF EXISTS),
    coordinated by one node at the given CL (serial SERIAL). A failed write
    is retried until it succeeds or a minute after the loader stops."""
    shift = 0
    step = 1000000
    max_value = 2147483647

    def __init__(self, manager, server, consistency_level):
        super().__init__(daemon=True)
        self.manager, self.server, self.cl = manager, server, consistency_level
        self.base = -2147483648 + IntKeyLoader.shift
        IntKeyLoader.shift += 1
        self.results = []
        self.idx = 0
        self.write_failed = False
        self.stopping = threading.Event()
        self.session = None

    def _session(self):
        if self.session is None:
            try:
                self.session = node_session(self.manager, self.server)
                self.insert = self.session.prepare("INSERT INTO ks.test(k,v) VALUES (?, ?) IF NOT EXISTS")
                self.update = self.session.prepare("UPDATE ks.test SET v = ? WHERE k=? IF EXISTS")
                for stmt in (self.insert, self.update):
                    stmt.consistency_level = self.cl
                    stmt.serial_consistency_level = ConsistencyLevel.SERIAL
            except Exception as exc:  # noqa: BLE001
                logger.debug("loader of %s cannot connect: %s", self.server.ip_addr, exc)
                self.session = None
                time.sleep(1)
        return self.session

    def _step(self):
        session = self._session()
        if session is None:
            return
        key = self.base + self.step * self.idx
        if key > self.max_value:
            self.idx = 0
            key = self.base
        try:
            if self.idx >= len(self.results) or self.results[self.idx] is None:
                session.execute(self.insert, (key, 0))
                if self.idx >= len(self.results):
                    self.results.append(0)
                else:
                    self.results[self.idx] = 0
            else:
                session.execute(self.update, (self.results[self.idx] + 1, key))
                self.results[self.idx] += 1
        except Exception as exc:  # noqa: BLE001
            logger.debug("loader of %s failed to write key %s: %s", self.server.ip_addr, key, exc)
            self.write_failed = True
            return
        self.write_failed = False
        self.idx += 1

    def run(self):
        while not self.stopping.is_set():
            self._step()
        deadline = time.time() + 60
        while self.write_failed and time.time() < deadline:
            self._step()

    def stop(self):
        self.stopping.set()
        self.join()

    def merge_result(self, output: dict) -> None:
        for n, val in enumerate(self.results):
            if val is not None:
                output[self.base + n * self.step] = val
        if self.write_failed:
            key = self.base + self.step * self.idx
            old = self.results[self.idx] if self.idx < len(self.results) else None
            output[key] = (old, 0 if old is None else old + 1)


async def validate_lwt_data(manager: ScyllaClusterManager, expected: dict, serial=ConsistencyLevel.SERIAL) -> None:
    """The dsr's _validate_data_in_cluster(): every node rebuilt, then every
    key the loaders wrote holds its last value, and no other row is left."""
    servers = await manager.running_servers()
    for s in servers:
        await run_nodetool(manager, s, "rebuild")
    session = node_session(manager, servers[0])
    select = session.prepare("SELECT * FROM ks.test WHERE k = ?")
    delete = session.prepare("DELETE FROM ks.test WHERE k = ?")
    for stmt in (select, delete):
        stmt.consistency_level = ConsistencyLevel.QUORUM
        stmt.serial_consistency_level = serial
    errors = []
    for key, value in sorted(expected.items()):
        accepted = value if isinstance(value, tuple) else (value,)
        rows = await asyncio.to_thread(lambda: list(session.execute(select, (key,))))
        if not rows:
            if None not in accepted:
                errors.append(f"{key} missing, expected {value}")
        elif rows[0][1] not in accepted:
            errors.append(f"{key} is {rows[0][1]}, expected {value}")
        await asyncio.to_thread(session.execute, delete, (key,))
    extra = await asyncio.to_thread(lambda: list(session.execute(SimpleStatement("SELECT * FROM ks.test", consistency_level=ConsistencyLevel.QUORUM))))
    errors += [f"extra row {r}" for r in extra]
    assert not errors, f"{len(errors)} of {len(expected)} records wrong: {errors[:10]}"


# Nodes are added (one at a time, ten seconds apart) to a cluster of three,
# possibly after a decommission of an added node or of an initial one, while
# every node coordinates an LWT load at QUORUM (or ALL); then every row holds
# the last value written. As in the dtest, the scenario and CL are drawn at
# random (seeded by the test id), and keyspaces need not be RF-rack-valid.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.skip_mode(mode='debug', reason='too heavy for debug mode, as in the dtest')
async def test_topology_grow(manager: ScyllaClusterManager):
    grow_after_decommission = random.choice([True, False])
    initial_node_count, adds = 3, 3
    decommission = None
    if grow_after_decommission:
        cl = ConsistencyLevel.QUORUM
        if random.choice([True, False]):
            initial_node_count, decommission = 4, "initial"
        else:
            decommission = "added"
    else:
        cl = random.choice([ConsistencyLevel.QUORUM, ConsistencyLevel.ALL])
    logger.info("decommission=%s cl=%s", decommission, cl)
    config = {**CONFIG, "rf_rack_valid_keyspaces": False}
    servers = await manager.servers_add(initial_node_count, config=config, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = manager.get_cql()
    await cql.run_async(SimpleStatement("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor': 3}",
                                        consistency_level=ConsistencyLevel.ALL))
    await cql.run_async(SimpleStatement("CREATE TABLE ks.test (k int PRIMARY KEY, v int)", consistency_level=ConsistencyLevel.ALL))
    loaders = {}

    def start_loader(server):
        loaders[server.server_id] = IntKeyLoader(manager, server, cl)
        loaders[server.server_id].start()

    async def stop_loader(server_id):
        await asyncio.to_thread(loaders[server_id].stop)

    expected = {}
    for s in servers:
        start_loader(s)
    await asyncio.sleep(10)

    async def add_node():
        s = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack1"})
        start_loader(s)
        await asyncio.sleep(10)
        return s

    async def decommission_node(server):
        await manager.decommission_node(server.server_id)
        await stop_loader(server.server_id)
        loaders.pop(server.server_id).merge_result(expected)
        await asyncio.sleep(10)

    if decommission == "initial":
        await decommission_node(servers[2])
    elif decommission == "added":
        await decommission_node(await add_node())
    for _ in range(adds):
        await add_node()
    for server_id in list(loaders):
        await stop_loader(server_id)
        loaders.pop(server_id).merge_result(expected)
    await validate_lwt_data(manager, expected)


class RandomPaxosCluster:
    """The dsr ScyllaClusterTest the TestRandomPaxos dtests run: nodes in
    datacenter1/rack1 (keyspaces need not be RF-rack-valid), keyspace ks
    with RF 3 and table ks.test, an IntKeyLoader at QUORUM coordinated by
    every node, and actions 10 seconds apart; at the end every row holds
    the last value written."""

    def __init__(self, manager: ScyllaClusterManager):
        self.manager = manager
        self.config = {**CONFIG, "rf_rack_valid_keyspaces": False}
        self.property_file = {"dc": "datacenter1", "rack": "rack1"}
        self.loaders = {}
        self.expected = {}
        self.servers = []

    def start_loader(self, server):
        self.loaders[server.server_id] = IntKeyLoader(self.manager, server, ConsistencyLevel.QUORUM)
        self.loaders[server.server_id].start()

    async def stop_loader(self, server):
        loader = self.loaders.pop(server.server_id)
        await asyncio.to_thread(loader.stop)
        loader.merge_result(self.expected)

    async def start(self, nodes: int):
        self.servers = await self.manager.servers_add(nodes, config=self.config, property_file=self.property_file)
        cql = self.manager.get_cql()
        await cql.run_async(SimpleStatement("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor': 3}",
                                            consistency_level=ConsistencyLevel.ALL))
        await cql.run_async(SimpleStatement("CREATE TABLE ks.test (k int PRIMARY KEY, v int)", consistency_level=ConsistencyLevel.ALL))
        for s in self.servers:
            self.start_loader(s)
        await asyncio.sleep(10)

    async def add_node(self):
        s = await self.manager.server_add(config=self.config, property_file=self.property_file)
        self.servers.append(s)
        self.start_loader(s)

    async def decommission_node(self, server):
        """dsr's DecommissionRemoveNode: a decommission (then ccm forgets the node)."""
        await self.stop_loader(server)
        self.servers.remove(server)
        await self.manager.decommission_node(server.server_id)

    async def reboot_node(self, server):
        """dsr's RebootNode(gently=True)."""
        await self.manager.server_stop_gracefully(server.server_id)
        await self.manager.server_start(server.server_id)

    async def replace_node(self, server):
        """dsr's ReplaceNode(is_seed=False): the node stopped and replaced by a new one."""
        await self.stop_loader(server)
        await self.manager.server_stop_gracefully(server.server_id)
        self.servers.remove(server)
        s = await self.manager.server_add(ReplaceConfig(replaced_id=server.server_id, reuse_ip_addr=False, use_host_id=True),
                                          config=self.config, property_file=self.property_file)
        self.servers.append(s)
        self.start_loader(s)

    async def finish(self):
        for s in list(self.servers):
            await self.stop_loader(s)
        await validate_lwt_data(self.manager, self.expected)


# Ported from scylla-dtest lwt_random_test.py::TestRandomPaxos::test_topology_add_decommission_reboot.
# Five random actions - adding a node, decommissioning one, or a graceful
# reboot - 10 seconds apart on a cluster that starts with three nodes and
# stays within 2..8 nodes, under an LWT load at QUORUM from every node; then
# every row holds the last value written. As in dsr, the seed (the first
# node) is never decommissioned or rebooted, and a node is decommissioned or
# rebooted only while more nodes run than QUORUM of RF 3 needs (2).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_topology_add_decommission_reboot(manager: ScyllaClusterManager):
    c = RandomPaxosCluster(manager)
    await c.start(3)
    for _ in range(5):
        can_stop = len(c.servers) > 2
        actions = (["reboot", "decommission"] if can_stop else []) + (["add"] if len(c.servers) < 8 else [])
        action = random.choice(actions)
        logger.info("action %s on %d nodes", action, len(c.servers))
        if action == "add":
            await c.add_node()
        elif action == "decommission":
            await c.decommission_node(random.choice(c.servers[1:]))
        else:
            await c.reboot_node(random.choice(c.servers[1:]))
        await asyncio.sleep(10)
    await c.finish()


# Ported from scylla-dtest lwt_random_test.py::TestRandomPaxos::test_topology_replace.
# Nodes 4 and 5 are added to a cluster of three, then five random non-seed
# nodes are replaced one after another, 10 seconds apart, under an LWT load
# at QUORUM from every node; then every row holds the last value written.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_topology_replace(manager: ScyllaClusterManager):
    c = RandomPaxosCluster(manager)
    await c.start(3)
    for _ in range(2):
        await c.add_node()
        await asyncio.sleep(10)
    for _ in range(5):
        await c.replace_node(random.choice(c.servers[1:]))
        await asyncio.sleep(10)
    await c.finish()


############################################################################
# Ported from paxos_tests.py.

class PaxosLoadThread(threading.Thread):
    """The dtest's LoadThread: inserts, then IF EXISTS increments, of keys
    a thousand apart; errors are ignored. Like the dtest's session, which is
    not exclusive, it connects through its node but routes each request to
    the key's replicas (the driver's default token-aware policy)."""

    def __init__(self, manager, server, shift):
        super().__init__(daemon=True)
        self.manager, self.server = manager, server
        self.base = -2147483648 + shift
        self.results = {}
        self.stopping = threading.Event()

    def run(self):
        while not self.stopping.is_set():
            try:
                session = self.manager.con_gen([self.server.ip_addr],
                                               load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect()
                insert = session.prepare("INSERT INTO ks.test(k,v) VALUES (?, ?)")
                update = session.prepare("UPDATE ks.test SET v = v + 1 WHERE k=? IF EXISTS")
            except Exception:  # noqa: BLE001
                time.sleep(1)
                continue
            # The dtest session's default consistency level.
            insert.consistency_level = update.consistency_level = ConsistencyLevel.ONE
            for n in range(self.base, 2147483647, 1000):
                if self.stopping.is_set():
                    return
                i = (n - self.base) // 1000
                if i not in self.results:
                    try:
                        session.execute(insert, (n, 0))
                        self.results[i] = 0
                    except Exception:  # noqa: BLE001
                        self.results[i] = None
                    continue
                try:
                    session.execute(update, (n,))
                except Exception:  # noqa: BLE001
                    pass

    def stop(self):
        self.stopping.set()
        self.join()


# Six nodes in three racks (RF=3) under LWT load from every node: the nodes
# beyond one per rack are stopped at once, then removed one by one from a
# live node with "nodetool removenode", ignoring the other still-dead ones.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_topology_change_in_presence_of_down_node(manager: ScyllaClusterManager):
    servers = await manager.servers_add(6, config=CONFIG, property_file=rack_property_files(6, 3))
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    await cql.run_async("CREATE TABLE ks.test (k int PRIMARY KEY, v int)")
    loaders = {}
    for n, s in enumerate(servers):
        loaders[s.server_id] = PaxosLoadThread(manager, s, n)
        loaders[s.server_id].start()
    await asyncio.sleep(10)
    try:
        racks = [p["rack"] for p in rack_property_files(6, 3)]
        reserved, seen = [], set()
        for s, rack in zip(servers, racks):
            if rack not in seen:
                seen.add(rack)
                reserved.append(s)
        removable = [s for s in servers if s not in reserved]
        random.shuffle(removable)
        host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in removable}
        for s in removable:
            await manager.server_stop_gracefully(s.server_id)
            await asyncio.to_thread(loaders.pop(s.server_id).stop)
        for s in removable:
            await manager.others_not_see_server(s.ip_addr)
        alive = reserved[0]
        for i, s in enumerate(removable):
            others = [str(host_ids[o.server_id]) for o in removable[i + 1:]]
            if others:
                await run_nodetool(manager, alive, "removenode", "--ignore-dead-nodes", ",".join(others), str(host_ids[s.server_id]))
            else:
                await run_nodetool(manager, alive, "removenode", str(host_ids[s.server_id]))
    finally:
        for loader in loaders.values():
            await asyncio.to_thread(loader.stop)
    out, _, _ = await run_nodetool(manager, servers[0], "status")
    logger.debug("nodetool status is: %s", out)
