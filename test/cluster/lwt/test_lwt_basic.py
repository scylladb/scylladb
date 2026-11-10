#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Basic LWT (Paxos) behaviour, ported one test per dtest from scylla-dtest
lwt_test.py: cross-shard operations and metrics of LWTs on one node, the
read round optimization, node outages on one and two DCs, paxos_grace_seconds,
SERIAL reads after the Paxos state expired (CASSANDRA-12043) and read
linearizability with failed Paxos rounds.

Like the dtests: keyspaces use vnodes, hinted handoff is off where the dtest
turns it off, nodes are stopped gracefully, and the driver sessions are the
dtests' ones - token-aware (or pinned to one node for the exclusive ones),
CL=ONE and the server's default serial consistency (SERIAL), retrying
timeouts 5 times and Unavailable 3 times on the same node.
"""

import logging
import re
import struct
import time

import pytest
from cassandra import ConsistencyLevel, Unavailable, WriteFailure
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.dtest_helpers import nodetool
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

NO_HINTS = {"hinted_handoff_enabled": False}
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
# Scylla's default, as on the dtest nodes; test.py turns it off.
AUTO_SNAPSHOT = {"auto_snapshot": True}
# The dtests' populate(1): SimpleSnitch, the node in datacenter1/rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}
DATACENTER1_RACK1 = {"dc": "datacenter1", "rack": "rack1"}


def cluster_topology(dc_num: int = 1, rack_num: int = 1, dc_name_prefix: str = "datacenter") -> list[dict[str, str]]:
    """Property files of the dtests' generate_cluster_topology(dc_num,
    rack_num, nodes_per_rack=1), in ccm's node order."""
    return [{"dc": f"{dc_name_prefix}{d + 1}", "rack": f"rack{r + 1}"} for d in range(dc_num) for r in range(rack_num)]


class FlakyRetryPolicy(RetryPolicy):
    """The dtests' retry policy: timeouts are retried 5 times and
    Unavailable 3 times, on the same node."""
    max_retries = 5
    max_unavail_retries = 3

    def on_read_timeout(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < self.max_retries else (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < self.max_retries else (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < self.max_unavail_retries else (self.RETHROW, None)


def dtest_session(cluster, keyspace: str | None = None):
    session = cluster.connect()
    profile = session.get_execution_profile(EXEC_PROFILE_DEFAULT)
    profile.consistency_level = ConsistencyLevel.ONE
    profile.serial_consistency_level = None
    profile.retry_policy = FlakyRetryPolicy()
    if keyspace:
        session.set_keyspace(keyspace)
    return session


def patient_cql_connection(manager: ScyllaClusterManager, server: ServerInfo, keyspace: str | None = None):
    """The dtests' patient_cql_connection(): the driver's default token- and
    DC-aware policy, the node as contact point."""
    return dtest_session(manager.con_gen([server.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())), keyspace)


def exclusive_cql_connection(manager: ScyllaClusterManager, server: ServerInfo, keyspace: str | None = None):
    """The dtests' (patient_)exclusive_cql_connection(): a session to the node alone."""
    return dtest_session(manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])), keyspace)


def create_ks(session, name: str, rf: int | dict[str, int]) -> None:
    """The dtests' create_ks(): NetworkTopologyStrategy, then USE it."""
    if isinstance(rf, int):
        replication = f"'class':'NetworkTopologyStrategy', 'replication_factor':{rf}"
    else:
        replication = "'class':'NetworkTopologyStrategy', " + ", ".join(f"'{dc}':{n}" for dc, n in rf.items())
    session.execute(f"CREATE KEYSPACE {name} WITH replication={{{replication}}}")
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    session.execute(f"USE {name}")


def create_cf(session, name: str, paxos_grace_seconds: int | None = None) -> None:
    """The dtests' create_cf(session, name, key_type="int", columns={"v1": "int"}, ...)."""
    query = (f"CREATE COLUMNFAMILY {name} (key int PRIMARY KEY, v1 int) WITH comment='test cf' "
             "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    if paxos_grace_seconds is not None:
        query += f" AND paxos_grace_seconds={paxos_grace_seconds}"
    session.execute(query)
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    session.execute(f"SELECT * FROM {name} LIMIT 1")


async def start_node(manager: ScyllaClusterManager, server: ServerInfo, seeds: list | None = None) -> None:
    """ccm's node.start(): returns once the other running nodes see it up."""
    await manager.server_start(server.server_id, seeds=seeds)
    for other in await manager.running_servers():
        if other.server_id != server.server_id:
            await manager.server_sees_other_server(other.ip_addr, server.ip_addr)


async def stop_node(manager: ScyllaClusterManager, server: ServerInfo, wait_other_notice: bool = False) -> None:
    """ccm's node.stop(): graceful."""
    await manager.server_stop_gracefully(server.server_id)
    if wait_other_notice:
        await manager.others_not_see_server(server.ip_addr)


async def get_node_metrics(manager: ScyllaClusterManager, server: ServerInfo, metrics: list[str]) -> dict:
    """The dtests' get_node_metrics(): the sum of the values of the metric
    lines matching each regular expression."""
    res = {k: 0 for k in metrics}
    lines = [line for line in (await manager.metrics.query(server.ip_addr)).lines if line and not line.startswith("#")]
    for line in lines:
        for name in metrics:
            if re.search(name, line):
                val = line.split()[-1]
                try:
                    val = int(val)
                except ValueError:
                    val = float(val)
                res[name] += val
    return res


def assert_one(session, query: str, expected: list, cl=None) -> None:
    res = [list(r) for r in session.execute(SimpleStatement(query, consistency_level=cl))]
    assert res == [expected], f"Expected {[expected]} from {query}, but got {res}"


def assert_none(session, query: str, cl=None) -> None:
    res = [list(r) for r in session.execute(SimpleStatement(query, consistency_level=cl))]
    assert res == [], f"Expected nothing from {query}, but got {res}"


def assert_row_count(session, table: str, expected: int, cl=ConsistencyLevel.ONE) -> None:
    count = session.execute(SimpleStatement(f"SELECT count(*) FROM {table}", consistency_level=cl)).one()[0]
    assert count == expected, f"Expected a row count of {expected} in table '{table}', but got {count}"


async def case_prologue(manager: ScyllaClusterManager, smp: str, table_cql="CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"):
    """The dtests' TestLwt.case_prologue(): one node (populate(1)) with
    hinted handoff off, keyspace lwt with RF=1, and the table."""
    server = await manager.server_add(config={**NO_HINTS, **VNODES, **SIMPLE_SNITCH, **AUTO_SNAPSHOT}, property_file=DATACENTER1_RACK1,
                                      cmdline=["--smp", smp])
    session = patient_cql_connection(manager, server)
    create_ks(session, "lwt", 1)
    session.execute(table_cql)
    return server, session


# Ported from scylla-dtest lwt_test.py::TestLwt::test_no_cross_shard_ops.
# LWTs on a node with 4 shards, prepared and unprepared (whose condition
# then fails), add at most one replica cross-shard operation each in the
# user statements' scheduling group: the Paxos state is sharded like the
# table, and bounce-to-shard moves the request before Paxos starts.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_cross_shard_ops(manager: ScyllaClusterManager):
    server, session = await case_prologue(manager, "4")
    name = r'scylla_storage_proxy_replica_cross_shard_ops\{scheduling_group_name="(sl:default|statement)"'

    before = await get_node_metrics(manager, server, [name])
    stmt = session.prepare("insert into t (a, b) values (?, ?) if not exists")
    for i in range(16):
        session.execute(stmt, (i, i))
    after = await get_node_metrics(manager, server, [name])
    assert after.get(name, 0) - before.get(name, 0) <= 16, f"{before}:{after}"
    before = after
    for i in range(16):
        session.execute(f"INSERT INTO t (a, b) VALUES ({i}, {i}) IF NOT EXISTS")
    after = await get_node_metrics(manager, server, [name])
    assert after.get(name, 0) - before.get(name, 0) <= 16, f"{before}:{after}"
    session.execute("DROP TABLE IF EXISTS t")


# Ported from scylla-dtest lwt_test.py::TestLwt::test_metrics.
# The deltas of the LWT-related metrics: failed conditions, batches,
# statements in batches and the conditional="yes" insert/update/delete
# counters. One shard, as a bounced statement is counted twice (#5860).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_metrics(manager: ScyllaClusterManager):
    server, session = await case_prologue(manager, "1")

    async def check1(name, cql, expect):
        before = await get_node_metrics(manager, server, [name])
        session.execute(cql)
        after = await get_node_metrics(manager, server, [name])
        assert after.get(name, 0) - before.get(name, 0) == expect, f"{before} {after}"

    name = "scylla_storage_proxy_coordinator_cas_write_condition_not_met"
    cql = "INSERT INTO t (a, b) VALUES (1, 1) IF NOT EXISTS"
    await check1(name, cql, 0)
    await check1(name, cql, 1)
    name = "scylla_cql_batches"
    await check1(name, cql, 0)
    cql = "BEGIN BATCH " + cql + " APPLY BATCH"
    await check1(name, cql, 1)
    name = "scylla_cql_statements_in_batches"
    await check1(name, cql, 1)
    cql = "BEGIN BATCH APPLY BATCH"
    await check1(name, cql, 0)
    # Not scylla_cql_deletes_per_ks.
    name = 'scylla_cql_deletes{.*conditional="yes"'
    await check1(name, cql, 0)
    cql = "DELETE FROM t WHERE a = 1 IF EXISTS"
    await check1(name, cql, 1)
    name = 'scylla_cql_inserts{.*conditional="yes"'
    cql = "INSERT INTO t (a, b) VALUES (1, 1) IF NOT EXISTS"
    await check1(name, cql, 1)
    name = 'scylla_cql_updates{.*conditional="yes"'
    cql = "UPDATE t SET b = 1 WHERE a = 1 if b = 1"
    await check1(name, cql, 1)
    session.execute("DROP TABLE IF EXISTS t")


# Ported from scylla-dtest lwt_test.py::TestLwt::test_read_round_optimization.
# 3.5: repeated successful conditional updates of a row rarely need the
# separate read round. 3.6: for each key whose replicas disagree (a plain
# QUORUM update missed node2, which then forms the LWT quorum with node1),
# the read round optimization fails exactly once.
# The dtest is not next_gating: it runs in release and debug mode.
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it in release and debug mode only")
async def test_read_round_optimization(manager: ScyllaClusterManager, build_mode: str):
    servers = await manager.servers_add(3, config={**NO_HINTS, **VNODES, **AUTO_SNAPSHOT}, property_file=cluster_topology(rack_num=3))
    node = servers[0]
    session = exclusive_cql_connection(manager, node)
    create_ks(session, "lwt", 3)
    session.execute("DROP TABLE IF EXISTS t")
    session.execute("CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)")
    session.execute(SimpleStatement("INSERT INTO t (a,b) VALUES (1,0) IF NOT EXISTS", consistency_level=ConsistencyLevel.QUORUM))
    stmt = session.prepare("UPDATE t SET b = ? WHERE a = 1 IF b = ?")
    stmt.consistency_level = ConsistencyLevel.QUORUM
    name = "scylla_storage_proxy_coordinator_cas_failed_read_round_optimization"
    before = await get_node_metrics(manager, node, [name])

    run_count = 10000 if build_mode not in DEBUG_MODES else 1000
    fails_probability = 0.5

    for i in range(run_count):
        session.execute(stmt, (i + 1, i))
    after = await get_node_metrics(manager, node, [name])
    assert after.get(name, 0) - before.get(name, 0) < run_count * fails_probability
    session.execute("DROP TABLE t")

    # 3.6, with a range of keys to avoid flakiness.
    key_count = 100
    session.execute("CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)")
    stmt = session.prepare("INSERT INTO t (a, b) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ALL
    for i in range(key_count):
        session.execute(stmt, (i, i))

    non_paxos_stmt = session.prepare("UPDATE t SET b = 2 WHERE a = ?")
    non_paxos_stmt.consistency_level = ConsistencyLevel.QUORUM
    paxos_stmt = session.prepare("UPDATE t SET b = 3 WHERE a = ? IF b = 2")
    node1, node2 = servers[1], servers[2]

    before = await get_node_metrics(manager, node, [name])
    await stop_node(manager, node1)
    for i in range(key_count):
        session.execute(non_paxos_stmt, (i,))
    await stop_node(manager, node2)
    await start_node(manager, node1)
    for i in range(key_count):
        session.execute(paxos_stmt, (i,))
    after = await get_node_metrics(manager, node, [name])
    assert after.get(name, 0) - before.get(name, 0) == key_count


# Ported from scylla-dtest lwt_test.py::TestLwt::test_basic_distributed.
# 3.1: a SERIAL read through another node sees an LWT. 3.2: node2 missed an
# LWT; alone it does not have the row, and a SERIAL read through it repairs
# it, so a CL=ONE read then has it. 3.3: without a quorum the LWT fails
# with Unavailable; once node2 is back (with node3 as seed), it succeeds,
# and node1 with node2 sees it. 3.4: after the Paxos state of node1 was
# deleted and compacted, node3, which missed an LWT, sees it with a SERIAL read.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_basic_distributed(manager: ScyllaClusterManager):  # noqa: PLR0915
    servers = await manager.servers_add(3, config={**NO_HINTS, **VNODES, **AUTO_SNAPSHOT}, property_file=cluster_topology(rack_num=3))
    node1, node2, node3 = servers
    session1 = patient_cql_connection(manager, node1)
    create_ks(session1, "lwt", 3)
    session1.execute("CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)")
    session1.execute("INSERT INTO t (a,b) VALUES (1,1) IF NOT EXISTS")

    # 3.1
    session2 = exclusive_cql_connection(manager, node2, keyspace="lwt")
    assert_one(session2, "SELECT * FROM t WHERE a = 1", [1, 1], cl=ConsistencyLevel.SERIAL)

    # 3.2
    await stop_node(manager, node2)
    session1.execute("INSERT INTO t (a,b) VALUES (2,2) IF NOT EXISTS")
    await start_node(manager, node2)
    await stop_node(manager, node1)
    await stop_node(manager, node3)
    session2 = exclusive_cql_connection(manager, node2, keyspace="lwt")
    cql = "SELECT * FROM t WHERE a = 2"
    assert_none(session2, cql, cl=ConsistencyLevel.ONE)
    await start_node(manager, node1)
    await start_node(manager, node3)
    assert_one(session2, cql, [2, 2], cl=ConsistencyLevel.SERIAL)
    assert_one(session2, cql, [2, 2], cl=ConsistencyLevel.ONE)

    # 3.3
    await stop_node(manager, node1)
    await stop_node(manager, node2)
    session3 = exclusive_cql_connection(manager, node3, keyspace="lwt")
    with pytest.raises(Unavailable):
        session3.execute("INSERT INTO t (a,b) VALUES (3,3) IF NOT EXISTS")
    assert_none(session3, "SELECT * FROM t WHERE a=3", cl=ConsistencyLevel.ONE)
    # node1, the seed, is down: node2 finds the cluster through node3.
    await start_node(manager, node2, seeds=[node3.ip_addr])
    assert_none(session3, "SELECT * FROM t WHERE a=3", cl=ConsistencyLevel.SERIAL)
    assert_one(session3, "INSERT INTO t (a,b) VALUES (3,3) IF NOT EXISTS", [True, None, None])
    await stop_node(manager, node3)
    await start_node(manager, node1)
    session1 = exclusive_cql_connection(manager, node1, keyspace="lwt")
    assert_one(session1, "SELECT * FROM t WHERE a=3", [3, 3], cl=ConsistencyLevel.SERIAL)

    # 3.4
    assert_one(session1, "INSERT INTO t (a,b) VALUES (4,4) IF NOT EXISTS", [True, None, None])
    rows = session1.execute("SELECT row_key, cf_id FROM system.paxos")
    stmt = session1.prepare("DELETE FROM system.paxos WHERE row_key = ? AND cf_id = ?")
    for row in rows:
        session1.execute(stmt, (row[0], row[1]))
    # ccm's cluster.compact(): `nodetool compact` on every running node.
    for s in await manager.running_servers():
        await nodetool(manager, s, "compact")
    await start_node(manager, node3)
    session3 = exclusive_cql_connection(manager, node3, keyspace="lwt")
    assert_one(session3, "SELECT * FROM t WHERE a=4", [4, 4], cl=ConsistencyLevel.SERIAL)
    session3.execute("DROP TABLE IF EXISTS t")


# Ported from scylla-dtest lwt_test.py::TestLwt::test_multi_dc.
# RF=3 in each of two DCs. 4.1: LOCAL_SERIAL works in dc1 with dc2 down,
# SERIAL fails with Unavailable. 4.2: LWTs at LOCAL_SERIAL in dc2 work with
# dc1 down, SERIAL does not; a SERIAL read once 4 of 6 nodes are up. 4.3:
# with two dc1 nodes down, LOCAL_SERIAL fails there and SERIAL works.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_multi_dc(manager: ScyllaClusterManager):
    nodes = await manager.servers_add(6, config={**NO_HINTS, **VNODES}, property_file=cluster_topology(dc_num=2, rack_num=3, dc_name_prefix="dc"))
    node1 = nodes[0]
    session1 = patient_cql_connection(manager, node1)
    create_ks(session1, "lwt", {"dc1": 3, "dc2": 3})
    session1.execute("CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY)")
    node2 = nodes[5]
    session2 = exclusive_cql_connection(manager, node2, keyspace="lwt")
    cql = "INSERT INTO t (a) VALUES (?) IF NOT EXISTS"
    stmt1 = session1.prepare(cql)
    stmt1.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
    session1.execute(stmt1, (1,))
    stmt2 = session2.prepare(cql)
    stmt2.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
    session2.execute(stmt2, (2,))
    for i in range(2, 6):
        await stop_node(manager, nodes[i])
    session1.execute(stmt1, (3,))
    with pytest.raises(Unavailable):
        stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
        session1.execute(stmt1, (4,))
    # 4.2
    for i in range(4, 6):
        await start_node(manager, nodes[i])
    for i in range(2):
        await stop_node(manager, nodes[i])
    session2 = exclusive_cql_connection(manager, node2, keyspace="lwt")
    session2.execute(stmt2, (4,))
    stmt2.serial_consistency_level = ConsistencyLevel.SERIAL
    with pytest.raises(Unavailable):
        session2.execute(stmt2, (5,))
    await start_node(manager, nodes[0])
    await start_node(manager, nodes[3])
    session2.execute(SimpleStatement("SELECT * FROM t WHERE a = 4", consistency_level=ConsistencyLevel.SERIAL))
    # 4.3
    session1 = exclusive_cql_connection(manager, node1, keyspace="lwt")
    stmt1.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
    with pytest.raises(Unavailable):
        session1.execute(stmt1, (6,))
    stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
    session1.execute(stmt1, (7,))


# The dtests' TestLWTPaxos.setup_cluster(): three nodes, one per rack of
# datacenter1, hinted handoff off. As for the existing port of
# test_paxos_grace_seconds_alter, the vnodes path of the dtest is taken: the
# Paxos state is read from system.paxos by cf_id, which needs filtering; the
# dtests' nodes run with Scylla's default strict_allow_filtering (warn),
# test.py's with true.
LWT_PAXOS_CONFIG = {**NO_HINTS, **VNODES, "strict_allow_filtering": "warn"}


def cql_timeout(build_mode: str, seconds: int) -> int:
    """The dtests' cql_timeout(): 3x in debug mode, 2x in other non-release modes."""
    return seconds * (1 if build_mode == "release" else 3 if build_mode in DEBUG_MODES else 2)


PAXOS_KEYSPACE = "lwt"
PAXOS_QUORUM = 2  # RF=3


def retry_till_success(fun, *args, timeout: float = 60, bypassed_exception=Exception, **kwargs):
    """The dtests' retry_till_success(): calls fun until it does not raise
    bypassed_exception, every 0.25 s; past the timeout the error is raised."""
    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            time.sleep(0.25)


def paxos_hosts(session, servers) -> list:
    """The dtests' prepare_lwt_keyspace() hosts: the driver Host of every
    node, in node order. Paxos state is node-local, so it is read from each
    node by pinning the query to its Host."""
    hosts_by_ip = {host.address: host for host in session.cluster.metadata.all_hosts()}
    return [hosts_by_ip[s.ip_addr] for s in servers]


def paxos_row_keys_on_host(session, host, table: str, cf_id_cache: dict) -> set[bytes]:
    """All the system.paxos row keys of the table on the node of `host`."""
    if table not in cf_id_cache:
        row = session.execute(SimpleStatement("SELECT id FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
                                              consistency_level=ConsistencyLevel.ONE), (PAXOS_KEYSPACE, table)).one()
        assert row and row[0], f"Failed to resolve table id for {PAXOS_KEYSPACE}.{table}"
        cf_id_cache[table] = row[0]
    stmt = SimpleStatement("SELECT row_key FROM system.paxos WHERE cf_id=%s", consistency_level=ConsistencyLevel.ONE)
    return {r[0] for r in session.execute(stmt, (cf_id_cache[table],), host=host)}


def paxos_holders(session, hosts, table: str, keys, cf_id_cache: dict) -> dict[int, list[str]]:
    """key -> the nodes that hold Paxos state for it."""
    row_keys = {str(host.endpoint): paxos_row_keys_on_host(session, host, table, cf_id_cache) for host in hosts}
    return {key: [ep for ep, node_row_keys in row_keys.items() if struct.pack(">i", key) in node_row_keys] for key in keys}


def assert_paxos_quorum_presence(session, hosts, table: str, keys, cf_id_cache: dict) -> None:
    holders = paxos_holders(session, hosts, table, keys, cf_id_cache)
    violations = {key: eps for key, eps in holders.items() if len(eps) < PAXOS_QUORUM}
    assert not violations, f"{table} paxos state below quorum ({PAXOS_QUORUM}/{len(hosts)}): " + \
        "; ".join(f"key {key} only on {eps}" for key, eps in sorted(violations.items()))


def assert_paxos_absent_everywhere(session, hosts, table: str, keys, cf_id_cache: dict) -> None:
    holders = paxos_holders(session, hosts, table, keys, cf_id_cache)
    remaining = {key: eps for key, eps in holders.items() if eps}
    assert not remaining, f"{table} paxos state still present: " + "; ".join(f"key {key} on {eps}" for key, eps in sorted(remaining.items()))


def lwt_insert(session, table: str, key: int) -> None:
    """The dtests' lwt_insert(): INSERT IF NOT EXISTS at CL=ALL, serial SERIAL."""
    stmt = SimpleStatement(f"INSERT INTO {PAXOS_KEYSPACE}.{table} (key, v1) VALUES (%s, %s) IF NOT EXISTS",
                           consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
    assert session.execute(stmt, (key, key)).one()[0], f"LWT insert of key {key} into {table} was not applied"


def lwt_insert_with_paxos_quorum(session, hosts, table: str, paxos_grace_seconds: int, cf_id_cache: dict,
                                 attempts: int = 3) -> tuple[list[int], float]:
    """The dtests' lwt_insert_with_paxos_quorum(): LWT-insert a fresh key
    (at CL=ALL, so that the grace countdown starts
    no later than the ack) and check its Paxos state is on a quorum. The
    check only proves something inside the grace window (an expired row
    looks like a missing one), so a miss outside it is retried with a new
    key, and a miss inside it fails. Returns the keys written and the
    time.monotonic() deadline by which all their Paxos state must have
    expired (grace + 1 s for gc_clock's one-second granularity)."""
    # TTL expiry has one-second (gc_clock) granularity, so the window may close up to 1s early.
    window = paxos_grace_seconds - 1
    keys = []
    for key in range(attempts):
        started = time.monotonic()
        lwt_insert(session, table, key)
        expiry_deadline = time.monotonic() + paxos_grace_seconds + 1
        keys.append(key)
        holders = paxos_holders(session, hosts, table, [key], cf_id_cache)[key]
        elapsed = time.monotonic() - started
        if len(holders) >= PAXOS_QUORUM:
            return keys, expiry_deadline
        assert elapsed >= window, f"{table} paxos state for key {key} only on {holders} {elapsed:.1f}s after the insert, " \
                                  f"inside paxos_grace_seconds={paxos_grace_seconds}s"
        logger.warning("Paxos presence check for key %d finished %.1fs after the insert, past the %ds grace window; retrying with a new key",
                       key, elapsed, paxos_grace_seconds)
    raise AssertionError(f"{table}: no paxos presence check finished within paxos_grace_seconds={paxos_grace_seconds}s "
                         f"in {attempts} attempts, the host is too slow")


def wait_for_paxos_expiry(session, hosts, table: str, keys, cf_id_cache: dict, deadline: float) -> None:
    """Retries until the Paxos state of the keys is gone from the nodes,
    instead of sleeping out the whole grace period; it must be gone by the
    time.monotonic() deadline."""
    remaining = max(0.0, deadline - time.monotonic())
    logger.info("Wait up to %.1fs for %s paxos state to expire", remaining, table)
    retry_till_success(assert_paxos_absent_everywhere, session, hosts, table, keys, cf_id_cache, timeout=remaining,
                       bypassed_exception=AssertionError)


# Ported from scylla-dtest lwt_test.py::TestLWTPaxos::test_paxos_grace_seconds_basic.
# paxos_grace_seconds is the TTL of the Paxos state: an LWT insert into a
# table with the default paxos_grace_seconds and one into a table with a
# short one (3 s, more in slower modes) leave Paxos state on a quorum of the
# nodes; the short table's state is then polled until it is gone everywhere
# (it must be by the end of its grace period), the default table's is still
# on a quorum, and the rows of both tables stay.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_paxos_grace_seconds_basic(manager: ScyllaClusterManager, build_mode: str):
    servers = await manager.servers_add(3, config=LWT_PAXOS_CONFIG, property_file=cluster_topology(rack_num=3))
    session = patient_cql_connection(manager, servers[0])
    create_ks(session, PAXOS_KEYSPACE, 3)
    hosts = paxos_hosts(session, servers)
    paxos_grace_seconds = cql_timeout(build_mode, 3)
    short_tbl = "short_grace"
    default_tbl = "default_grace"
    cf_id_cache = {}

    create_cf(session, short_tbl, paxos_grace_seconds=paxos_grace_seconds)
    create_cf(session, default_tbl)

    # The default table first, so its insert doesn't eat into the short grace window.
    lwt_insert(session, default_tbl, 0)
    assert_paxos_quorum_presence(session, hosts, default_tbl, [0], cf_id_cache)
    short_keys, expiry_deadline = lwt_insert_with_paxos_quorum(session, hosts, short_tbl, paxos_grace_seconds, cf_id_cache)

    wait_for_paxos_expiry(session, hosts, short_tbl, short_keys, cf_id_cache, expiry_deadline)
    # The default table's state was written before the short table's, so it has already outlived the short grace period.
    assert_paxos_quorum_presence(session, hosts, default_tbl, [0], cf_id_cache)
    assert_row_count(session, default_tbl, 1, cl=ConsistencyLevel.QUORUM)
    assert_row_count(session, short_tbl, len(short_keys), cl=ConsistencyLevel.QUORUM)
    session.cluster.shutdown()


# Ported from scylla-dtest lwt_test.py::TestLWTPaxos::test_paxos_grace_seconds_negative.
# A table with a negative paxos_grace_seconds is rejected. Schema validation
# only: like the dtest, one node (populate(1)) and RF 1.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_paxos_grace_seconds_negative(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**VNODES, **SIMPLE_SNITCH}, property_file=DATACENTER1_RACK1)
    session1 = patient_cql_connection(manager, server)
    create_ks(session1, "lwt", 1)
    with pytest.raises(ConfigurationException, match="paxos_grace_seconds cannot be smaller than 0, \\(default 864000\\)"):
        create_cf(session1, "zero_ttl", paxos_grace_seconds=-1)


# Ported from scylla-dtest lwt_test.py::TestPaxosBug::test_synced_most_recent_commit_in_cas_should_not_cause_timeouts.
# An LWT at QUORUM that node C missed (A and B flushed it), followed by
# SERIAL reads through A with B down and C up (paxos_grace_seconds 40, 80
# in debug mode): one halfway through the grace period, while the most
# recent commit is still inside it at A (and is replayed to C with a fresh
# TTL), and one after A's copy expired while C's is still alive. Neither
# may time out (CASSANDRA-12043).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_synced_most_recent_commit_in_cas_should_not_cause_timeouts(manager: ScyllaClusterManager, build_mode: str):
    servers = await manager.servers_add(3, config=VNODES, property_file=cluster_topology(rack_num=3))
    node_a, node_b, node_c = servers
    # Debug-mode node stop/start can take a large share of the grace period
    grace = 80 if build_mode in DEBUG_MODES else 40
    session_a = exclusive_cql_connection(manager, node_a)
    create_ks(session_a, "paxos_bug", 3)
    create_cf(session_a, "cassandra_12043", paxos_grace_seconds=grace)

    await stop_node(manager, node_c)
    query = SimpleStatement("INSERT INTO cassandra_12043 (key, v1) VALUES (%s, %s) IF NOT EXISTS", consistency_level=ConsistencyLevel.QUORUM)
    session_a.execute(query, (1, 1))
    await nodetool(manager, node_a, "flush")
    await nodetool(manager, node_b, "flush")
    t0 = time.time()

    def wait_seconds(seconds):
        logger.info("Waiting %.1f seconds", seconds)
        time.sleep(max(seconds, 0))

    def query_data_from_nodes_a_and_c_with_consistency_serial():
        session_a.execute(SimpleStatement("select * from cassandra_12043 where key = 1;", consistency_level=ConsistencyLevel.SERIAL))

    # Read 1 must run while the MRC is still inside the grace at A (replayed to C, with a fresh TTL)
    running = {s.server_id for s in await manager.running_servers()}
    if node_b.server_id in running:
        await stop_node(manager, node_b)
    if node_c.server_id not in running:
        await start_node(manager, node_c)
    elapsed = time.time() - t0
    logger.info("Stopping B and starting C took %.1f seconds", elapsed)
    # Read 1 may run anywhere in (t0 + 10s, t0 + grace - 10s): the MRC must still be inside the grace to be replayed
    # The t0+10s lower bound comes from the read 2 assert below (t0+grace+5 < t_read1+grace-5); change both margins together
    assert elapsed < grace - 10, f"stopping B / starting C took {elapsed:.1f}s, too close to the {grace}s paxos grace"
    wait_seconds(t0 + grace / 2 - time.time())
    t_read1 = time.time()
    query_data_from_nodes_a_and_c_with_consistency_serial()

    # Read 2 must run after A's copy expired (+5s: expiry has second granularity), while C's copy is alive
    wait_seconds(t0 + grace + 5 - time.time())
    # Only catches read 1 overrunning the wait above; C's MRC expiry itself is not checked
    assert time.time() < t_read1 + grace - 5, \
        f"read 1 took too long (started at t0+{t_read1 - t0:.1f}s): read 2 is now too close to C's replayed MRC expiry (TTL {grace}s)"
    query_data_from_nodes_a_and_c_with_consistency_serial()  # should not time out


# The dtest's error injections, all enabled on a node at once.
ERROR_INJECTIONS = [
    "paxos_prepare_timeout",
    "paxos_error_before_save_promise",
    "paxos_error_after_save_promise",
    "paxos_accept_proposal_timeout",
    "paxos_error_before_save_proposal",
    "paxos_error_after_save_proposal",
    "paxos_error_before_learn",
    "paxos_state_learn_timeout",
    "paxos_timeout_after_save_decision",
]


# Ported from scylla-dtest lwt_test.py::TestLwtReadLinearizability::test_read_linearizability.
# A write whose proposal is accepted on A only (B and C fail to save it)
# fails. Reads of B and C alone find no value, and a SERIAL read with all
# the injections on A finds none either; then, with the injections moved to
# C, a SERIAL read through C must not return the value: two reads with no
# write in between agree.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_read_linearizability(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=VNODES, property_file=cluster_topology(rack_num=3))
    node_a, node_b, node_c = servers
    session_a = patient_cql_connection(manager, node_a)
    create_ks(session_a, "ks", 3)
    session_a.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

    for node in [node_b, node_c]:
        await manager.api.enable_injection(node.ip_addr, "paxos_error_before_save_proposal", one_shot=False)
    with pytest.raises(WriteFailure):
        session_a.execute("INSERT INTO ks.t (id, v) VALUES (1, 1) IF NOT EXISTS")
    for error in ERROR_INJECTIONS:
        await manager.api.enable_injection(node_a.ip_addr, error, one_shot=False)
    for node in [node_b, node_c]:
        await manager.api.disable_injection(node.ip_addr, "paxos_error_before_save_proposal")

    for node in [node_b, node_c]:
        session = exclusive_cql_connection(manager, node)
        ret = session.execute(SimpleStatement("SELECT v FROM ks.t WHERE id = 1", consistency_level=ConsistencyLevel.ONE)).current_rows
        assert ret == []

    ret = session_a.execute(SimpleStatement("SELECT v FROM ks.t WHERE id = 1", consistency_level=ConsistencyLevel.SERIAL)).current_rows
    assert ret == []

    for error in ERROR_INJECTIONS:
        await manager.api.disable_injection(node_a.ip_addr, error)
        await manager.api.enable_injection(node_c.ip_addr, error, one_shot=False)

    # The session to C, the last of the loop above.
    ret = session.execute(SimpleStatement("SELECT v FROM ks.t WHERE id = 1", consistency_level=ConsistencyLevel.SERIAL)).current_rows
    assert ret == []
