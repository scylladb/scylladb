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
import os
import random
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.lwt.test_lwt_basic import (assert_row_count, create_cf, create_ks, cql_timeout, lwt_insert_with_paxos_quorum,
                                             patient_cql_connection, paxos_hosts, wait_for_paxos_expiry)
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
async def test_topology_change_in_presence_of_down_node(manager: ScyllaClusterManager, build_mode: str):
    # The dtest's request timeouts: 10 s in release mode, 20 s in the other
    # non-debug modes.
    timeout = 10000 * (1 if build_mode == "release" else 2)
    config = {**CONFIG,
              "read_request_timeout_in_ms": timeout,
              "range_request_timeout_in_ms": 3 * timeout,
              "write_request_timeout_in_ms": timeout,
              "truncate_request_timeout_in_ms": 3 * timeout,
              "counter_write_request_timeout_in_ms": 2 * timeout,
              "cas_contention_timeout_in_ms": timeout,
              "request_timeout_in_ms": timeout}
    servers = await manager.servers_add(6, config=config, property_file=rack_property_files(6, 3))
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
        # As the dtest, wait for the others to see each node down before
        # stopping its loader and the next node: meanwhile, coordinators keep
        # sending writes to the stopped node.
        for s in removable:
            await manager.server_stop_gracefully(s.server_id)
            await manager.others_not_see_server(s.ip_addr)
            await asyncio.to_thread(loaders.pop(s.server_id).stop)
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


############################################################################
# Ported from lwt_schema_modification_test.py: actions in threads, each
# with its own session to the first node, for a number of rounds of
# run_s seconds, as the dtest's _test_combine().

KEYSPACE = "lwt_load_ks"


def read_rows(session, stop, row_start, row_end, **_):
    """The dtest's ReadRows: rows of a range read at ALL never change."""
    select = session.prepare(f"SELECT pk, v FROM table1 WHERE pk > {row_start} AND pk < {row_end} ALLOW FILTERING")
    select.consistency_level = ConsistencyLevel.ALL
    expected = sorted(session.execute(select).current_rows)
    while not stop.is_set():
        assert sorted(session.execute(select).current_rows) == expected


def lwt_load(session, stop, row_start, row_end, **_):
    """The dtest's LWTLoad: random rows inserted IF NOT EXISTS and updated
    with a condition, at serial LOCAL_SERIAL."""
    insert = session.prepare("INSERT INTO table1 (pk, ck, v) VALUES (:pk, :ck, :v) IF NOT EXISTS")
    update = session.prepare("UPDATE table1 SET v = :v WHERE pk = :pk AND ck = :ck IF v > 50 AND v < 1000")
    insert.serial_consistency_level = update.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
    val = 0
    while not stop.is_set():
        pk = ck = random.randint(row_start, row_end)
        session.execute(insert, (pk, ck, val))
        session.execute(update, (val, pk, ck))
        val += 1


def lwt_load_check(session, stop, row_start, row_end, **_):
    """The dtest's LWTLoadCheck: consecutive rows inserted IF NOT EXISTS at
    LOCAL_QUORUM until stopped, then their sum(v) is the expected one."""
    insert = session.prepare("INSERT INTO table1 (pk, ck, v) VALUES (:pk, :ck, :v)  IF NOT EXISTS")
    insert.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
    insert.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    i = row_start
    while i <= row_end and not stop.is_set():
        row = session.execute(insert, (i, i, i)).current_rows[0]
        # An insert that timed out but was committed gets applied=False with
        # our own row back when the driver retries it: a success too.
        if row.applied or row.v == i:
            i += 1
    select = session.prepare(f"SELECT sum(v) FROM table1 WHERE pk >= {row_start} AND pk <= {row_end} ALLOW FILTERING")
    select.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    result = session.execute(select).current_rows[0][0]
    n = i - row_start
    assert result == (n / 2) * (row_start + i - 1)


def index_drop_add(session, stop, inter_delay, _unused=None, **_):
    """The dtest's IndexDropAdd: the index on v dropped, and after
    inter_delay seconds created again."""
    session.execute("DROP INDEX table1_v_idx")
    time.sleep(inter_delay)
    session.execute("CREATE INDEX table1_v_idx ON table1 (v)")


def materialized_view(session, stop, row_max, _unused=None, node=None, **_):
    """The dtest's MaterializedView: a view with synchronous updates created,
    "nodetool viewbuildstatus" on the node retried until it has finished
    building, its first row_max rows read at ALL until they are all there,
    and then, until stopped, they never change."""
    session.execute("""
        CREATE MATERIALIZED VIEW table1_top_v_view AS
            SELECT pk, ck, v FROM table1
            WHERE ck IS NOT NULL
            PRIMARY KEY (pk, ck)
            WITH CLUSTERING ORDER BY (ck ASC) AND synchronous_updates = true
        """)
    exe, ip = node

    def retry_till_success(fun, timeout=60):
        deadline = time.time() + timeout
        while True:
            try:
                return fun()
            except AssertionError:
                if time.time() > deadline:
                    raise
                time.sleep(0.25)

    def wait_for_mv_created():
        res = subprocess.run([exe, "nodetool", "-h", ip, "viewbuildstatus", KEYSPACE, "table1_top_v_view"], capture_output=True, text=True)
        assert "has finished building" in res.stdout, res
    retry_till_success(wait_for_mv_created)

    select = session.prepare(f"SELECT pk, ck, v FROM table1_top_v_view LIMIT {row_max}")
    select.consistency_level = ConsistencyLevel.ALL

    def wait_for_row_max():
        assert len(session.execute(select).current_rows) == row_max
    retry_till_success(wait_for_row_max)
    expected = sorted(session.execute(select).current_rows)
    while not stop.is_set():
        assert sorted(session.execute(select).current_rows) == expected


async def lwt_combine(manager: ScyllaClusterManager, actions, nodes: int, rf: int, nrows: int, loops: int, run_s: int,
                      smp: int | None = None) -> None:
    """The dtest's _test_combine() with its _setup() (all LWT phases given 30 s,
    rf racks) and _case_prologue() (table1 with an index on v, nrows rows).
    The dtest ran the nodes with 8 shards (4 in debug); test.py's 2 are kept,
    as eight nodes of eight shards would not fit test.py's machines."""
    manager.ignore_log_patterns += ["exception during mutation write .*schema_mismatch_error",
                                    "Error applying view update to .*: data_dictionary::no_such_column_family"]
    config = {**CONFIG, "write_request_timeout_in_ms": 30000, "cas_contention_timeout_in_ms": 30000,
              # Scylla's default, as on the dtest nodes; test.py turns it off.
              "auto_snapshot": True}
    servers = await manager.servers_add(nodes, config=config, property_file=rack_property_files(nodes, rf),
                                        cmdline=["--smp", str(smp)] if smp else None)
    node1 = (await manager.server_get_exe(servers[0].server_id), servers[0].ip_addr)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE {KEYSPACE} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    await cql.run_async(f"CREATE TABLE {KEYSPACE}.table1 (pk int, ck int, v int, int_col int, PRIMARY KEY (pk, ck))")
    await cql.run_async(f"CREATE INDEX table1_v_idx ON {KEYSPACE}.table1 (v)")
    insert = cql.prepare(f"INSERT INTO {KEYSPACE}.table1 (pk, ck, v, int_col) VALUES (?, ?, ?, ?)")
    for i in range(nrows):
        await cql.run_async(insert, (i, i, i, i))
    for loop in range(loops):
        logger.debug("loop %d/%d", loop, loops)
        stop = threading.Event()

        def run(action):
            fn, row_start, row_end = action
            session = node_session(manager, servers[0], request_timeout=1000)
            session.execute(f"USE {KEYSPACE}")
            fn(session, stop, row_start, row_end, node=node1)

        with ThreadPoolExecutor(max_workers=len(actions)) as pool:
            futures = [pool.submit(run, a) for a in actions]
            await asyncio.sleep(run_s)
            stop.set()
            for f in futures:
                await asyncio.wrap_future(f)
        running = {s.server_id for s in await manager.running_servers()}
        assert all(s.server_id in running for s in servers)


# LWT load on rows 1001..9999 beside reads at ALL of the untouched rows
# 0..1000, on eight nodes (RF=3), four rounds of 30 seconds.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.skip_mode(mode='debug', reason='too heavy for debug mode, as in the dtest')
async def test_lwt_load(manager: ScyllaClusterManager):
    await lwt_combine(manager, [(read_rows, 0, 1000), (lwt_load, 1001, 9999)], nodes=8, rf=3, nrows=10000, loops=4, run_s=30)


# Conditional inserts of consecutive new keys beside other LWT load for 30
# seconds, then the sum of the inserted values matches.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.skip_mode(mode='debug', reason='too heavy for debug mode, as in the dtest')
async def test_lwt_load_check(manager: ScyllaClusterManager):
    await lwt_combine(manager, [(lwt_load, 1, 99), (lwt_load_check, 100, 99999999)], nodes=8, rf=3, nrows=0, loops=1, run_s=30)


# Ported from scylla-dtest lwt_schema_modification_test.py::TestLWTSchemaModification::test_index_drop_add.
# On four nodes of four shards (RF=3, racks by RF), an LWT load on rows
# 1001..9999 and reads at ALL of rows 0..1000 run while the index on v is
# dropped and, 0.5 s later, created again; four rounds of 10 seconds.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_index_drop_add(manager: ScyllaClusterManager):
    await lwt_combine(manager, [(lwt_load, 1001, 9999), (read_rows, 0, 1000), (index_drop_add, 0.5, None)],
                      nodes=4, rf=3, nrows=10000, loops=4, run_s=10, smp=min(4, os.cpu_count() or 1))


# Ported from scylla-dtest lwt_schema_modification_test.py::TestLWTSchemaModification::test_materialized_view.
# On four nodes of four shards (RF=3), a materialized view with synchronous
# updates is created during an LWT load on rows 1001..9999; once built, its
# first 1000 rows (untouched by the load) read at ALL never change; one
# round of 10 seconds.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_materialized_view(manager: ScyllaClusterManager):
    await lwt_combine(manager, [(lwt_load, 1001, 9999), (materialized_view, 1000, None)],
                      nodes=4, rf=3, nrows=10000, loops=1, run_s=10, smp=min(4, os.cpu_count() or 1))


############################################################################
# Ported from lwt_test.py.

# LWT INSERTs with uuid(), currentTimestamp() and currentDate() in the key,
# unprepared and prepared, insert a new row every time.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_not_deterministic_function_in_pk(manager: ScyllaClusterManager):
    await manager.server_add(config={**CONFIG, "hinted_handoff_enabled": False})
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE lwt WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    await cql.run_async("CREATE TABLE IF NOT EXISTS lwt.test (pk1 uuid, pk2 timestamp, ck date, PRIMARY KEY((pk1, pk2), ck))")
    insert_cql = "INSERT INTO lwt.test(pk1, pk2, ck) VALUES (uuid(), currentTimestamp(), currentDate()) IF NOT EXISTS"

    async def count():
        return list(await cql.run_async("SELECT COUNT(*) FROM lwt.test"))[0][0]
    for _ in range(1000):
        await cql.run_async(insert_cql)
    assert await count() == 1000
    prepared = cql.prepare(insert_cql)
    for _ in range(1000):
        await cql.run_async(prepared, [])
    assert await count() == 2000


# On three nodes (one per rack, RF=3), a table created with the default
# paxos_grace_seconds is ALTERed to 3 s: the Paxos state of an LWT insert at
# ALL is then in a quorum of the nodes' system.paxos, and is polled until it
# is in none of them (it must be by the end of the 3 s), while the row
# remains. As the dtest ran: vnodes (256 per node), so the state is read
# from system.paxos by cf_id, at ONE through one session pinned to each
# node's Host. That read needs filtering; the dtests' nodes run with
# Scylla's default strict_allow_filtering (warn), test.py's with true, so
# the port sets warn.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_paxos_grace_seconds_alter(manager: ScyllaClusterManager, build_mode: str):
    config = {**CONFIG, "hinted_handoff_enabled": False, "strict_allow_filtering": "warn"}
    servers = await manager.servers_add(3, config=config,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    session = patient_cql_connection(manager, servers[0])
    create_ks(session, "lwt", 3)
    hosts = paxos_hosts(session, servers)
    grace = cql_timeout(build_mode, 3)
    cf_id_cache = {}
    create_cf(session, "paxos_grace_seconds")
    session.execute(f"ALTER TABLE lwt.paxos_grace_seconds WITH paxos_grace_seconds = {grace}")
    assert session.cluster.control_connection.wait_for_schema_agreement(wait_time=120), "No schema agreement within 120s"

    keys, expiry_deadline = await asyncio.to_thread(lwt_insert_with_paxos_quorum, session, hosts, "paxos_grace_seconds", grace, cf_id_cache)
    await asyncio.to_thread(wait_for_paxos_expiry, session, hosts, "paxos_grace_seconds", keys, cf_id_cache, expiry_deadline)
    assert_row_count(session, "paxos_grace_seconds", len(keys), cl=ConsistencyLevel.QUORUM)
    session.cluster.shutdown()
