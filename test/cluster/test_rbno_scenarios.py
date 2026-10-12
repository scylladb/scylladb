#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Scenarios of repair-based node operations (RBNO), ported from scylla-dtest
# repair_based_node_operations_test.py: rebuild, removenode, bootstrap, a
# rejected removenode of a node still seen alive, replace and decommission,
# in single- and multi-rack and multi-DC clusters, and replaces that ignore
# other dead nodes, also after a restart of the whole cluster. After each
# operation the logs tell which nodes synced data by repair
# ("repair_reason=<operation>,"). Keyspaces use vnodes, like the dtests; the
# RBNO options are given on the command line of every node, like in the
# dtests.

import asyncio
import dataclasses
import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.util import stop_servers_gracefully
from test.cqlpy.util import run_cqlsh_at
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
ALL_OPS = "replace,removenode,rebuild,bootstrap,decommission"
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


@dataclass
class Scenario:
    name: str
    operation: str
    repair_on_tested_node: bool
    repair_on_all_nodes: bool


REBUILD = Scenario("rebuild", "rebuild", True, False)
REMOVENODE = Scenario("removenode", "removenode", False, True)
BOOTSTRAP = Scenario("bootstrap", "bootstrap", True, False)
REMOVENODE_REJECTING = Scenario("removenode_rejecting", "removenode", False, False)
REPLACE = Scenario("replace", "replace", True, False)
DECOMMISSION = Scenario("decommission", "decommission", True, False)
REPLACE_WITH_DEAD_NODES = Scenario("replace_with_dead_nodes", "replace", True, False)
REPLACE_WITH_DEAD_NODES_AFTER_RESTART = Scenario("replace_with_dead_nodes_after_restart", "replace", True, False)
REPLACE_DEAD_NODE_AFTER_CLUSTER_RESTART = Scenario("replace_dead_node_after_cluster_restart", "replace", True, False)
SAFE_UNSAFE_REBUILD = Scenario("safe_unsafe_rebuild", "rebuild", True, False)
OPERATIONS_FLOW = [REBUILD, REMOVENODE, BOOTSTRAP, REMOVENODE_REJECTING, REPLACE, DECOMMISSION]


class RBNOCluster:
    """The nodes of the cluster in the order of the dtest's ccm node list:
    nodes that were replaced or decommissioned leave the list, nodes that
    were removed stay in it."""

    def __init__(self, manager: ScyllaClusterManager, cmdline, config=None):
        self.manager = manager
        self.cmdline = cmdline
        self.config = {**VNODES, **(config or {})}
        self.nodes = []
        # ccm's seed list: the populated nodes, until cluster.remove() drops
        # one (a replaced node); a removed or stopped node stays a seed. The
        # nodes added later are not seeds and get the whole list - test.py
        # would give them the running nodes only.
        self.seeds = []

    async def start(self, topology):
        """topology: (dc, rack) of each node, in order, or a node count: ccm's
        populate(n), the SimpleSnitch with every node in datacenter1/rack1."""
        if isinstance(topology, int):
            self.config["endpoint_snitch"] = "SimpleSnitch"
            topology = [("datacenter1", "rack1")] * topology
        for dc, rack in topology:
            self.nodes.append(await self.manager.server_add(cmdline=self.cmdline, config=self.config,
                                                            property_file={"dc": dc, "rack": rack}))
            self.seeds.append(self.nodes[-1].ip_addr)

    async def nodetool(self, server, *args):
        exe = await self.manager.server_get_exe(server.server_id)
        res = await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", server.ip_addr, *args],
                                      capture_output=True, text=True)
        assert res.returncode == 0, f"nodetool {' '.join(args)} failed: {res.stderr}"
        return res.stdout

    async def prepare_schema(self, rf, rows=1000, compaction_strategy="SizeTieredCompactionStrategy"):
        if isinstance(rf, int):
            replication = f"'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}"
        else:
            replication = "'class': 'NetworkTopologyStrategy', " + ", ".join(f"'{dc}': {n}" for dc, n in rf.items())
        node = self.nodes[0]
        cql = self.manager.con_gen([node.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([node.ip_addr])).connect()
        cql.execute(f"CREATE KEYSPACE ks WITH replication = {{{replication}}}")
        cql.execute("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                    f"AND compaction = {{'class': '{compaction_strategy}', 'enabled': 'true'}} AND compression = {{}} "
                    "AND read_repair_chance=0.000000")
        # The dtests' create_cf() checks that the table exists by reading it.
        cql.execute("SELECT * FROM ks.cf LIMIT 1")
        stmt = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        stmt.consistency_level = ConsistencyLevel.ONE
        execute_concurrent_with_args(cql, stmt, [[f"k{k}"] for k in range(rows)], concurrency=20)
        for s in self.nodes:
            await self.nodetool(s, "flush")

    async def add_node(self, dc, rack, replace_cfg=None):
        new = await self.manager.server_add(replace_cfg, cmdline=self.cmdline, config=self.config,
                                            property_file={"dc": dc, "rack": rack}, seeds=list(self.seeds))
        self.nodes.append(new)
        return new

    def forget(self, server):
        """ccm's cluster.remove() of a replaced node: it leaves the node and
        seed lists."""
        self.nodes.remove(server)
        if server.ip_addr in self.seeds:
            self.seeds.remove(server.ip_addr)

    async def stop(self, server, gently=True, wait=True):
        if gently:
            await self.manager.server_stop_gracefully(server.server_id)
        else:
            await self.manager.server_stop(server.server_id, convict=False)
        if wait:
            await self.manager.others_not_see_server(server.ip_addr)

    async def wait_force_removed(self, server, address, host_id):
        log = await self.manager.server_open_log(server.server_id)
        await log.wait_for(f"gossip - Finished to force remove node ({address}|{host_id})", timeout=120)

    # The operations; each returns the node it tested.

    async def rebuild(self, dc, rack):
        node = self.nodes[0]
        await self.nodetool(node, "rebuild")
        return node

    async def removenode(self, dc, rack):
        node = self.nodes[-1]
        await self.stop(node, gently=False)
        await self.manager.remove_node(self.nodes[0].server_id, node.server_id)
        return node

    def new_node_location(self, dc, rack):
        """Where ccm puts a new node: without a DC, in the DC and rack of the
        first node; with a DC but no rack, in RAC1."""
        if dc is None:
            return self.nodes[0].datacenter, self.nodes[0].rack
        return dc, rack or "RAC1"

    async def bootstrap(self, dc, rack):
        return await self.add_node(*self.new_node_location(dc, rack))

    async def removenode_rejecting(self, dc, rack):
        """A removenode right after the node is killed, while the others still
        see it alive, is rejected; the node is started again."""
        node = self.nodes[-1]
        host_id = await self.manager.get_host_id(node.server_id)
        await self.manager.server_stop(node.server_id, convict=False)
        exe = await self.manager.server_get_exe(self.nodes[0].server_id)
        res = await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", self.nodes[0].ip_addr, "removenode", host_id],
                                      capture_output=True, text=True)
        assert res.returncode != 0 and "Rejected removenode operation" in res.stdout + res.stderr, res
        await self.manager.others_not_see_server(node.ip_addr)
        await self.manager.server_start(node.server_id)
        return node

    async def replace(self, dc, rack):
        replaced = self.nodes[-1]
        host_id = await self.manager.get_host_id(replaced.server_id)
        await self.stop(replaced)
        cfg = ReplaceConfig(replaced_id=replaced.server_id, reuse_ip_addr=False, use_host_id=True)
        new = await self.add_node(dc or replaced.datacenter, rack or replaced.rack, cfg)
        self.forget(replaced)
        await self.wait_force_removed(new, replaced.ip_addr, host_id)
        return new

    async def decommission(self, dc, rack):
        node = self.nodes[-1]
        await self.manager.decommission_node(node.server_id)
        return node

    async def replace_with_dead_nodes(self, dc, rack):
        """Three nodes are dead; one is replaced, ignoring the other two."""
        replaced, dead1, dead2 = self.nodes[-3:]
        dead_ids = [await self.manager.get_host_id(s.server_id) for s in (dead1, dead2)]
        for s in (replaced, dead1, dead2):
            await self.stop(s)
        cfg = ReplaceConfig(replaced_id=replaced.server_id, reuse_ip_addr=False, use_host_id=True, ignore_dead_nodes=dead_ids)
        new = await self.add_node(replaced.datacenter, replaced.rack, cfg)
        self.forget(replaced)
        # The ignored nodes are banned and cannot come back.
        return new

    async def nodes_to_stop(self, count):
        """The last nodes that can be stopped without losing the majority of
        group 0 voters, skipping the topology coordinator."""
        # The group 0 voters, read with cqlsh like the dtests' get_group0_members().
        node = self.nodes[0]
        group0_id = await cqlsh_rows(node, "select value from system.scylla_local where key = 'raft_group0_id'")
        members = await cqlsh_rows(node, f"select server_id, can_vote from system.raft_state "
                                         f"where group_id = {group0_id[0][0]} and disposition = 'CURRENT'")
        voters = {server_id for server_id, can_vote in members if can_vote == "True"}
        # The latest elected topology coordinator, from the group 0 history,
        # like the dtests' TopologyCoordinatorFinder.
        cql = self.manager.get_cql()
        host_ids = {s.server_id: await self.manager.get_host_id(s.server_id) for s in self.nodes}
        voters_allowed = (len([s for s in self.nodes if host_ids[s.server_id] in voters]) - 1) // 2
        history = cql.execute("select description from system.group0_history where key = 'history' "
                              "and description LIKE 'Starting new topology coordinator%' ALLOW FILTERING;")
        coordinators = [m.group(0) for r in history if (m := UUID_RE.search(r.description))]
        assert coordinators, "No host ids were found in raft history"
        coordinator = coordinators[0]
        chosen = []
        for s in reversed(self.nodes):
            if len(chosen) == count:
                break
            if host_ids[s.server_id] == coordinator:
                continue
            if host_ids[s.server_id] in voters:
                if not voters_allowed:
                    continue
                voters_allowed -= 1
            chosen.append(s)
        assert len(chosen) == count
        return chosen, host_ids

    async def replace_with_dead_nodes_after_restart(self, dc, rack):
        """The whole cluster is killed and started again without three nodes;
        one of them is replaced in place, ignoring the other two."""
        (replaced, dead1, dead2), host_ids = await self.nodes_to_stop(3)
        for s in self.nodes:
            await self.manager.server_stop(s.server_id, convict=False)
        for s in self.nodes:
            if s not in (replaced, dead1, dead2):
                await self.manager.server_start(s.server_id)
        for s in (replaced, dead1, dead2):
            await self.manager.others_not_see_server(s.ip_addr)
        # As the dtest, ccm's seeds become the first node only before the
        # node is replaced in place: the replacing node must not be a seed.
        self.seeds = [self.nodes[0].ip_addr]
        cfg = ReplaceConfig(replaced_id=replaced.server_id, reuse_ip_addr=True, use_host_id=True,
                            ignore_dead_nodes=[host_ids[dead1.server_id], host_ids[dead2.server_id]])
        new = await self.add_node(replaced.datacenter, replaced.rack, cfg)
        self.forget(replaced)
        running = [x.server_id for x in await self.manager.running_servers()]
        for s in self.nodes:
            if s.server_id in running:
                logger.info(f"{s} nodetool status:\n{await self.nodetool(s, 'status')}")
                logger.info(f"{s} nodetool gossipinfo:\n{await self.nodetool(s, 'gossipinfo')}")
        return new

    async def replace_dead_node_after_cluster_restart(self, dc, rack):
        """The whole cluster is killed and started again without its last
        node, which is then replaced in place."""
        replaced = self.nodes[-1]
        for s in self.nodes:
            await self.manager.server_stop(s.server_id, convict=False)
        for s in self.nodes:
            if s is not replaced:
                await self.manager.server_start(s.server_id)
        await self.manager.others_not_see_server(replaced.ip_addr)
        # As the dtest, ccm's seeds become the first node only before the
        # node is replaced in place: the replacing node must not be a seed.
        self.seeds = [self.nodes[0].ip_addr]
        cfg = ReplaceConfig(replaced_id=replaced.server_id, reuse_ip_addr=True, use_host_id=True)
        new = await self.add_node(replaced.datacenter, replaced.rack, cfg)
        self.forget(replaced)
        running = [x.server_id for x in await self.manager.running_servers()]
        for s in self.nodes:
            if s.server_id in running:
                logger.info(f"{s} nodetool status:\n{await self.nodetool(s, 'status')}")
                logger.info(f"{s} nodetool gossipinfo:\n{await self.nodetool(s, 'gossipinfo')}")
        return new

    async def wipe_node_keyspace_directory(self, server, ks):
        """The dtests' wipe_node_keyspace_directory(): stop the node and remove
        the keyspace's data directory."""
        await self.stop(server)
        shutil.rmtree(os.path.join(await self.manager.server_get_workdir(server.server_id), "data", ks), ignore_errors=True)

    async def verify_data_integrity(self, server, rows=1000):
        cql = self.manager.con_gen([server.ip_addr]).connect()
        try:
            for key in range(rows):
                res = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM ks.cf WHERE key='k{key}'",
                                                       consistency_level=ConsistencyLevel.QUORUM)))
                assert len(res) == 1 and tuple(res[0]) == ("value1", "value2"), res
        finally:
            cql.cluster.shutdown()

    async def safe_unsafe_rebuild(self, dc, rack, rf=1):
        """The first DC2 node loses its keyspace and is rebuilt from DC1, from
        DC2 (unsafe with RF=1), from all nodes, and forced from DC2."""
        node = [s for s in self.nodes if s.datacenter == "DC2"][0]
        dc1_nodes = [s for s in self.nodes if s.datacenter == "DC1"]
        for src, expect_success in [("DC1", True), ("DC2", rf > 1), (None, True), ("force", True)]:
            await self.wipe_node_keyspace_directory(node, "ks")
            await self.manager.server_start(node.server_id)
            await self.manager.servers_see_each_other(await self.manager.running_servers())
            args = ["rebuild"] + (["--force", "DC2"] if src == "force" else [src] if src else [])
            exe = await self.manager.server_get_exe(node.server_id)
            res = await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", node.ip_addr, *args], capture_output=True, text=True)
            if expect_success:
                assert res.returncode == 0, f"nodetool {' '.join(args)} failed: {res.stderr}"
            else:
                assert res.returncode != 0, f"nodetool {' '.join(args)} succeeded unexpectedly"
            if rf == 1 and src == "force":
                with pytest.raises(Unavailable, match="Cannot achieve consistency level for cl QUORUM"):
                    for s in dc1_nodes:
                        await self.stop(s)
                    await self.verify_data_integrity(node)
                    for s in dc1_nodes:
                        await self.manager.server_start(s.server_id)
            else:
                await self.verify_data_integrity(node)
        return node

    async def logged_repair(self, server, operation, mark=None):
        log = await self.manager.server_open_log(server.server_id)
        return bool(await log.grep(f"repair_reason={operation},", from_mark=mark))

    async def run_scenarios(self, rbno_enabled, scenarios, dc=None, rack=None, lcs=False, rf=None):
        for scenario in scenarios:
            marks = {}
            for s in self.nodes:
                marks[s.server_id] = await (await self.manager.server_open_log(s.server_id)).mark()
            logger.info(f"Start {scenario.name} scenario")
            if rf is None:
                tested = await getattr(self, scenario.name)(dc, rack)
            else:
                tested = await getattr(self, scenario.name)(dc, rack, rf=rf)
            known = self.nodes if tested in self.nodes else self.nodes + [tested]
            for s in known:
                if s.server_id == tested.server_id:
                    expected = rbno_enabled and scenario.repair_on_tested_node
                    assert await self.logged_repair(s, scenario.operation) == expected, f"{scenario.name}: tested node {s}"
                elif s.server_id in marks:
                    expected = rbno_enabled and scenario.repair_on_all_nodes
                    assert await self.logged_repair(s, scenario.operation, marks[s.server_id]) == expected, f"{scenario.name}: node {s}"
            if lcs and scenario.operation in ["bootstrap", "replace"]:
                log = await self.manager.server_open_log(tested.server_id)
                assert await log.grep(r"LeveledManifest - Reshaping \d+ disjoint sstables in level 0 into level \d+"), \
                    "Reshaping was ran in inefficient way"
            if scenario.name == "decommission":
                self.nodes.remove(tested)


async def cqlsh_rows(server, query):
    """The rows cqlsh prints for the query, as lists of strings."""
    res = await asyncio.to_thread(run_cqlsh_at, server.ip_addr, 9042, query)
    # cqlsh may print warnings, like a missing pytz module, on stderr.
    assert res.returncode == 0, res
    lines = []
    for line in res.stdout.strip().split("\n"):
        if not line.strip():
            break
        lines.append(line.strip())
    return [[c.strip() for c in line.split("|")] for line in lines[2:]]


def cmdline(enable=None, allowed=None):
    args = []
    if enable is not None:
        args += ["--enable-repair-based-node-ops", str(enable).lower()]
    if allowed:
        args += ["--allowed-repair-based-node-ops", allowed]
    return args


def cluster_topology(num_nodes, rf=3):
    """The dtests' get_cluster_topology(): DC1 with racks RAC1..RAC<rf>, the
    nodes beyond rf in the last rack."""
    return [("DC1", f"RAC{i + 1}") for i in range(rf)] + [("DC1", f"RAC{rf}")] * (num_nodes - rf)


def racks(layout):
    """{dc: {rack: count}} as a list of (dc, rack), in ccm's order."""
    return [(dc, rack) for dc, rs in layout.items() for rack, n in rs.items() for _ in range(n)]


async def run_rbno(manager, topology, rf, enable, allowed, scenarios, dc=None, rack=None, **kwargs):
    cluster = RBNOCluster(manager, cmdline(enable, allowed))
    await cluster.start(topology)
    await cluster.prepare_schema(rf)
    await cluster.run_scenarios(enable, scenarios, dc=dc, rack=rack, **kwargs)
    return cluster


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_disable_rbno.
# With --enable-repair-based-node-ops false, no operation syncs data by repair.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("num_nodes", [3, 4])
async def test_disable_rbno(manager: ScyllaClusterManager, num_nodes):
    await run_rbno(manager, cluster_topology(num_nodes), 3, False, None, OPERATIONS_FLOW, dc="DC1", rack="RAC1")


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_enable_rbno_for_default_operation.
# populate(3), RBNO enabled for all operations: rebuild, bootstrap, replace
# and decommission repair on the tested node, removenode on all others.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_enable_rbno_for_default_operation(manager: ScyllaClusterManager):
    await run_rbno(manager, 3, 3, True, ALL_OPS, OPERATIONS_FLOW)


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_enable_rbno_extra_nodes.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_enable_rbno_extra_nodes(manager: ScyllaClusterManager):
    await run_rbno(manager, racks({"DC1": {"RAC1": 1, "RAC2": 2}}), 2, True, ALL_OPS, OPERATIONS_FLOW, dc="DC1", rack="RAC1")


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_enable_rbno_extra_nodes_multi_rack.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_enable_rbno_extra_nodes_multi_rack(manager: ScyllaClusterManager):
    await run_rbno(manager, racks({"DC1": {"RAC1": 2, "RAC2": 2}}), {"DC1": 2}, True, ALL_OPS, OPERATIONS_FLOW, dc="DC1", rack="RAC2")


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_enable_rbno_extra_nodes_multi_dc.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_enable_rbno_extra_nodes_multi_dc(manager: ScyllaClusterManager):
    await run_rbno(manager, racks({"DC1": {"RAC1": 1, "RAC2": 2}, "DC2": {"RAC1": 1, "RAC2": 2}}), {"DC1": 2, "DC2": 2}, True,
                   ALL_OPS, OPERATIONS_FLOW, dc="DC2")


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_enable_rbno_extra_nodes_multi_dc_rack.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_enable_rbno_extra_nodes_multi_dc_rack(manager: ScyllaClusterManager):
    await run_rbno(manager, racks({"DC1": {"RAC1": 2, "RAC2": 2}, "DC2": {"RAC1": 2, "RAC2": 2}}), {"DC1": 2, "DC2": 2}, True,
                   ALL_OPS, OPERATIONS_FLOW, dc="DC2", rack="RAC2")


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_enable_rbno_for_bootstrap.
# With RBNO allowed for bootstrap only, only bootstrap syncs data by repair.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("num_nodes", [3, 4])
async def test_enable_rbno_for_bootstrap(manager: ScyllaClusterManager, num_nodes):
    no_repair = {REBUILD.name, REMOVENODE.name, REPLACE.name, DECOMMISSION.name}
    scenarios = [dataclasses.replace(sc, repair_on_tested_node=False, repair_on_all_nodes=False) if sc.name in no_repair else sc
                 for sc in OPERATIONS_FLOW]
    await run_rbno(manager, cluster_topology(num_nodes), 3, True, "bootstrap", scenarios, dc="DC1", rack="RAC1")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("num_nodes", [3, 4])
async def test_disable_rbno_for_all_operations(manager: ScyllaClusterManager, num_nodes):
    """With RBNO disabled, no operation syncs data by repair, even though all
    of them are in --allowed-repair-based-node-ops."""
    topology = [("DC1", "RAC1"), ("DC1", "RAC2"), ("DC1", "RAC3")] + [("DC1", "RAC3")] * (num_nodes - 3)
    cluster = RBNOCluster(manager, cmdline(False, "bootstrap,replace,removenode,decommission,rebuild"))
    await cluster.start(topology)
    await cluster.prepare_schema(3)
    await cluster.run_scenarios(False, OPERATIONS_FLOW, dc="DC1", rack="RAC1")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ignore_dead_nodes_for_replace_option(manager: ScyllaClusterManager):
    """A node is replaced while two other nodes are dead and ignored
    (--ignore-dead-nodes-for-replace); the replace syncs data by repair."""
    topology = [("DC1", "RAC1")] * 2 + [("DC1", "RAC2")] * 2 + [("DC1", "RAC3")] * 3
    cluster = RBNOCluster(manager, cmdline(True))
    await cluster.start(topology)
    await cluster.prepare_schema(3)
    await cluster.run_scenarios(True, [REPLACE_WITH_DEAD_NODES])


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ignore_dead_nodes_for_replace_option_multi_dc(manager: ScyllaClusterManager):
    """In three DCs, the whole cluster is restarted without three nodes, and
    one of them is replaced in place, ignoring the other two."""
    topology = [("dc1", "RAC1")] * 3 + [("dc2", "RAC1")] * 2 + [("dc3", "RAC1")] * 2
    cluster = RBNOCluster(manager, cmdline(True))
    await cluster.start(topology)
    await cluster.prepare_schema({"dc1": 1, "dc2": 1, "dc3": 1})
    await cluster.run_scenarios(True, [REPLACE_WITH_DEAD_NODES_AFTER_RESTART])


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_removenode_rejected_with_rnbo(manager: ScyllaClusterManager):
    """A removenode of a node that the others still see alive is rejected,
    and no node syncs data by repair for it."""
    topology = [("datacenter1", f"rack{i}") for i in (1, 2, 3)]
    cluster = RBNOCluster(manager, cmdline(True))
    await cluster.start(topology)
    await cluster.prepare_schema(3)
    await cluster.run_scenarios(True, [REMOVENODE_REJECTING])


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_lcs_reshape_efficiency.
# populate(3) with an LCS table, RBNO for bootstrap and replace, the small
# table optimization off: the joining node logs the efficient reshape of
# the disjoint level-0 run.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_lcs_reshape_efficiency(manager: ScyllaClusterManager):
    cluster = RBNOCluster(manager, cmdline(True, "bootstrap,replace"), config={"small_table_optimization_for_rbno_max_table_size": 0})
    await cluster.start(3)
    await cluster.prepare_schema(3, compaction_strategy="LeveledCompactionStrategy")
    await cluster.run_scenarios(True, [BOOTSTRAP, REPLACE], lcs=True)


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_replace_node_after_cluster_restart.
# The whole cluster is killed and restarted without its last node, which is
# replaced in place by repair.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("num_nodes", [3, 4])
async def test_replace_node_after_cluster_restart(manager: ScyllaClusterManager, num_nodes):
    await run_rbno(manager, cluster_topology(num_nodes), 3, True, None, [REPLACE_DEAD_NODE_AFTER_CLUSTER_RESTART])


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_ignore_dead_nodes_for_replace_option_multi_rack.
# In three racks, the whole cluster is restarted without three nodes, and
# one of them is replaced in place, ignoring the other two.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ignore_dead_nodes_for_replace_option_multi_rack(manager: ScyllaClusterManager):
    await run_rbno(manager, racks({"DC1": {"RACK1": 3, "RACK2": 2, "RACK3": 2}}), {"DC1": 3}, True, None,
                   [REPLACE_WITH_DEAD_NODES_AFTER_RESTART])


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_replace_error_with_different_dc_or_rack.
# Replacing a node by one in another DC or rack fails at startup with
# "Cannot replace node ... with a node on a different data center or rack";
# the replaced node is started again each time. scylladb#16858
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_replace_error_with_different_dc_or_rack(manager: ScyllaClusterManager):
    cluster = RBNOCluster(manager, cmdline(True))
    await cluster.start(racks({"DC1": {"RACK1": 1, "RACK2": 1}, "DC2": {"RACK1": 1, "RACK2": 1}}))
    await cluster.prepare_schema({"DC1": 2, "DC2": 2})
    node1, node2, node3, node4 = cluster.nodes
    startup_error_pat = r"Cannot replace node .* with a node on a different data center or rack"
    manager.ignore_log_patterns.append(startup_error_pat)
    for replaced, new_dc, new_rack in [(node1, "DC2", "RACK1"), (node2, "DC1", "RACK1"), (node3, "DC1", "RACK3"), (node4, "DC3", "RACK3")]:
        await cluster.stop(replaced)
        cfg = ReplaceConfig(replaced_id=replaced.server_id, reuse_ip_addr=False, use_host_id=True)
        new = await manager.server_add(cfg, cmdline=cluster.cmdline, config=cluster.config,
                                       property_file={"dc": new_dc, "rack": new_rack}, expected_error="Cannot replace node")
        log = await manager.server_open_log(new.server_id)
        pattern = (rf"Startup failed:.*{startup_error_pat}.*Current location={replaced.datacenter}/{replaced.rack}"
                   rf".*new location={new_dc}/{new_rack}")
        assert await log.grep(pattern), f"Expected following pattern in {new} log: {pattern}"
        await manager.server_start(replaced.server_id)
        await manager.servers_see_each_other(await manager.running_servers())


# Ported from scylla-dtest repair_based_node_operations_test.py::TestRepairBasedNodeOperations::test_safe_unsafe_rebuild.
# Two DCs of three nodes (rack RAC1): a DC2 node that lost its data is
# rebuilt from DC1, from DC2 (refused with RF=1), from all nodes, and forced
# from DC2; its data is then read at QUORUM, which with RF=1 and DC1 down
# fails.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("replication_factor", [1, 2, 3])
async def test_safe_unsafe_rebuild(manager: ScyllaClusterManager, replication_factor):
    manager.ignore_log_patterns.append("raft_topology -.*stream_ranges.*failed")
    cluster = RBNOCluster(manager, cmdline(True, "bootstrap,replace,removenode,decommission,rebuild"))
    await cluster.start([("DC1", "RAC1")] * 3 + [("DC2", "RAC1")] * 3)
    cql = manager.get_cql()
    if list(cql.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'audit'")):
        cql.execute(f"ALTER KEYSPACE audit WITH replication = {{'class': 'NetworkTopologyStrategy', 'DC1': '{replication_factor}', 'DC2': '{replication_factor}'}}")
    await cluster.prepare_schema({"DC1": replication_factor, "DC2": replication_factor})
    await cluster.run_scenarios(True, [SAFE_UNSAFE_REBUILD], rf=replication_factor)
