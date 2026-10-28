#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time
from random import randint

import pytest
from cassandra import ConsistencyLevel, ReadFailure, ReadTimeout, Unavailable, WriteFailure, WriteTimeout  # type: ignore
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable  # type: ignore
from cassandra.policies import DCAwareRoundRobinPolicy, FallthroughRetryPolicy, RetryPolicy, TokenAwarePolicy  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.cluster.dtest_helpers import DTEST_CONFIG, create_ks, dtest_session, exclusive_one, nodetool, topology
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CL = ConsistencyLevel


# The TestSimpleCluster dtests: generate_cluster_topology(rack_num=3)
# (datacenter1, rack1-rack3), vnodes, keyspace ks and table ks.test1.

async def simple_prepare_cluster(manager: ScyllaClusterManager, ks_rf: int):
    """TestSimpleCluster.prepare_cluster()."""
    servers = await manager.servers_add(3, config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256},
                                        property_file=topology({"rack1": 1, "rack2": 1, "rack3": 1}))
    cql = dtest_session(manager.get_cql())
    create_ks(cql, "ks", ks_rf)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("CREATE TABLE ks.test1 (k int PRIMARY KEY, c int)")
    return servers


def simple_consistency_level_validate(session, read_keys, read_cls_pass, read_cls_fail, write_keys, write_cls_pass, write_cls_fail):
    """TestSimpleCluster.simple_consistency_level_validate() with
    read/write_cls_pass_all: every read of a key and write of a new key at
    the passing levels succeeds; at the failing ones (no retries, 1 s
    timeout) they fail with Unavailable."""
    for cl in read_cls_pass:
        failed = 0
        for key in read_keys:
            try:
                assert len(list(session.execute(SimpleStatement(f"SELECT * FROM ks.test1 WHERE k={key}", consistency_level=cl)))) == 1
            except Exception:  # noqa: BLE001
                failed += 1
        assert failed == 0, f"Expected all reads at {cl} to pass, {failed} failed"
    for cl in read_cls_fail:
        with pytest.raises(Unavailable):
            list(session.execute(SimpleStatement("SELECT * FROM ks.test1 WHERE k=1", consistency_level=cl, retry_policy=FallthroughRetryPolicy()), timeout=1))
    for cl in write_cls_pass:
        failed = 0
        for key in write_keys:
            try:
                list(session.execute(SimpleStatement(f"insert into ks.test1  (k,c) values ({key},{key})", consistency_level=cl)))
            except Exception:  # noqa: BLE001
                failed += 1
        assert failed == 0, f"Expected all writes at {cl} to pass, {failed} failed"
    for cl in write_cls_fail:
        with pytest.raises(Unavailable):
            list(session.execute(SimpleStatement("insert into ks.test1 (k,c) values (101,101)", consistency_level=cl, retry_policy=FallthroughRetryPolicy()), timeout=1))


def simple_query_validate(session, read_keys: int, query: str, read_cls_pass, read_cls_fail):
    """TestSimpleCluster.simple_query_validate() with read_cls_pass_all."""
    for cl in read_cls_pass:
        res = list(session.execute(SimpleStatement(query, consistency_level=cl)))
        assert len(res) == read_keys, f"got {len(res)} expected {read_keys} : {res}"
    for cl in read_cls_fail:
        with pytest.raises(Unavailable):
            list(session.execute(SimpleStatement(query, consistency_level=cl)))


def insert_keys(session, keys):
    for val in keys:
        session.execute(SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=CL.ALL))


# Ported from scylla-dtest simple_cluster_driver_test.py::TestSimpleCluster::test_simple_rf_3_consistency_level.
# RF 3 on three nodes: through each node's exclusive session, reads of keys
# 1..99 and writes of new keys at every level; then with node1 stopped, and
# then node2, the levels needing more replicas fail with Unavailable.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_simple_rf_3_consistency_level(manager: ScyllaClusterManager):
    node1, node2, node3 = await simple_prepare_cluster(manager, 3)
    session1, session2, session3 = [await exclusive_one(manager, n) for n in (node1, node2, node3)]
    keys = range(1, 100)
    insert_keys(session1, keys)
    read_pass = [CL.ONE, CL.TWO, CL.THREE, CL.QUORUM, CL.ALL]
    write_pass = [CL.ANY, CL.ONE, CL.TWO, CL.THREE, CL.QUORUM, CL.ALL]
    for session, write_keys in [(session1, range(101, 200)), (session2, range(201, 300)), (session3, range(301, 400))]:
        simple_consistency_level_validate(session, keys, read_pass, [], write_keys, write_pass, [])

    await manager.server_stop_gracefully(node1.server_id)
    await manager.others_not_see_server(node1.ip_addr)
    read_pass, read_fail = [CL.ONE, CL.TWO, CL.QUORUM], [CL.THREE, CL.ALL]
    write_pass, write_fail = [CL.ANY, CL.ONE, CL.TWO, CL.QUORUM], [CL.THREE, CL.ALL]
    simple_consistency_level_validate(session2, keys, read_pass, read_fail, range(401, 500), write_pass, write_fail)
    simple_consistency_level_validate(session3, keys, read_pass, read_fail, range(501, 600), write_pass, write_fail)

    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)
    read_pass, read_fail = [CL.ONE], [CL.TWO, CL.THREE, CL.QUORUM, CL.ALL]
    write_pass, write_fail = [CL.ANY, CL.ONE], [CL.TWO, CL.THREE, CL.QUORUM, CL.ALL]
    simple_consistency_level_validate(session3, keys, read_pass, read_fail, range(601, 700), write_pass, write_fail)
async def simple_query(manager: ScyllaClusterManager, ks_rf: int, read_cls_pass, read_cls_fail) -> None:
    """test_simple_rf_<n>_query(): 99 keys at ALL through node1, then a full
    scan and an IN query of 10 keys at each level through sessions to each
    node (non-exclusive, as patient_cql_connection)."""
    servers = await simple_prepare_cluster(manager, ks_rf)
    clusters = [manager.con_gen([s.ip_addr]) for s in servers]
    try:
        sessions = [c.connect() for c in clusters]
        keys = range(1, 100)
        insert_keys(sessions[0], keys)
        for session in sessions:
            simple_query_validate(session, len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail)
            simple_query_validate(session, 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail)
    finally:
        for c in clusters:
            c.shutdown()
# Ported from scylla-dtest simple_cluster_driver_test.py::TestSimpleCluster::test_simple_rf_3_query.
# RF 3 on three nodes: full scans and IN queries succeed at every level
# through every node.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_simple_rf_3_query(manager: ScyllaClusterManager):
    await simple_query(manager, 3, [CL.ONE, CL.TWO, CL.THREE, CL.QUORUM, CL.ALL], [])


# Ported from scylla-dtest simple_cluster_driver_test.py::TestSimpleCluster::test_simple_rf_1_query.
# RF 1 on three nodes: full scans and IN queries succeed at ONE, QUORUM and
# ALL through every node, and fail with Unavailable at TWO and THREE.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_simple_rf_1_query(manager: ScyllaClusterManager):
    await simple_query(manager, 1, [CL.ONE, CL.QUORUM, CL.ALL], [CL.TWO, CL.THREE])


# Availability by consistency level as the nodes go down one by one, ported
# from scylla-dtest consistency_test.py (TestAvailability): through each node
# in turn, writes and reads of 100 users at each pair of consistency levels
# succeed when enough replicas are alive and fail with Unavailable (or a
# timeout or failure) otherwise; then that node is stopped. Writes with a
# serial consistency level are lightweight transactions (IF NOT EXISTS).

AVAILABILITY_CONFIG = {**DTEST_CONFIG, "cache_hit_rate_read_balancing": False, "hinted_handoff_enabled": False}
UNAVAILABLE = (Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure)
AVAILABILITY_COMBINATIONS = [
    (CL.ALL, CL.ALL),
    (CL.QUORUM, CL.QUORUM),
    (CL.LOCAL_QUORUM, CL.LOCAL_QUORUM),
    (CL.EACH_QUORUM, CL.LOCAL_QUORUM),
    (CL.ONE, CL.ONE, None, False),
    (CL.ONE, CL.ALL),
    (CL.ALL, CL.ONE),
    (CL.QUORUM, CL.TWO),
    (CL.QUORUM, CL.THREE),
    (CL.TWO, CL.TWO),
    (CL.THREE, CL.ONE),
    (CL.ANY, CL.ONE, None, False),
    (CL.LOCAL_ONE, CL.LOCAL_ONE, None, False),
    (CL.QUORUM, CL.SERIAL, CL.SERIAL),
    (CL.LOCAL_QUORUM, CL.LOCAL_SERIAL, CL.LOCAL_SERIAL),
    (CL.QUORUM, CL.LOCAL_SERIAL, CL.SERIAL),
    (CL.LOCAL_QUORUM, CL.SERIAL, CL.LOCAL_SERIAL),
]


class FlakyRetryPolicy(RetryPolicy):
    """The dtests' retry policy: read and write timeouts are retried up to 5
    times, Unavailable up to 3 times."""

    def on_read_timeout(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < 5 else (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < 5 else (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < 3 else (self.RETHROW, None)


FLAKY_RETRY_POLICY = FlakyRetryPolicy()


def required_nodes(cl, rf_factors, dc):
    """The number of nodes the consistency level needs in DC number dc,
    given the replication factor of each DC."""
    return {
        CL.ANY: 1,
        CL.ONE: 1,
        CL.TWO: 2,
        CL.THREE: 3,
        CL.QUORUM: sum(rf_factors) // 2 + 1,
        CL.ALL: sum(rf_factors),
        CL.LOCAL_QUORUM: rf_factors[dc] // 2 + 1,
        CL.EACH_QUORUM: rf_factors[dc] // 2 + 1,
        CL.SERIAL: sum(rf_factors) // 2 + 1,
        CL.LOCAL_SERIAL: rf_factors[dc] // 2 + 1,
        CL.LOCAL_ONE: 1,
    }[cl]


def should_succeed(cl, rf_factors, num_nodes_alive, current):
    """Whether an operation at the consistency level through a node of DC
    number current succeeds with num_nodes_alive nodes alive in each DC."""
    if cl in (CL.LOCAL_QUORUM, CL.LOCAL_ONE, CL.LOCAL_SERIAL):
        return num_nodes_alive[current] >= required_nodes(cl, rf_factors, current)
    if cl == CL.EACH_QUORUM:
        return all(num_nodes_alive[i] >= required_nodes(cl, rf_factors, i) for i in range(len(rf_factors)))
    return sum(num_nodes_alive) >= required_nodes(cl, rf_factors, current)


def availability_statement(query, cl, serial_cl=None):
    return SimpleStatement(query, consistency_level=cl, serial_consistency_level=serial_cl, retry_policy=FLAKY_RETRY_POLICY)


def insert_user(cql, userid, age, cl, serial_cl=None):
    if_not_exists = "IF NOT EXISTS" if serial_cl else ""
    cql.execute(availability_statement(f"INSERT INTO mytestks.users (userid, firstname, lastname, age) "
                                       f"VALUES ({userid}, 'first{userid}', 'last{userid}', {age}) {if_not_exists}", cl, serial_cl))


def query_user(cql, userid, age, cl, check_ret=True):
    res = [list(r) for r in cql.execute(availability_statement(f"SELECT userid, age FROM mytestks.users where userid = {userid}", cl))]
    expected = [[userid, age]] if age else []
    if check_ret:
        assert res == expected, f"Got {res}, expected {expected} at {cl}"


def insert_query_from_node(cql, dc_idx, rf_factors, num_nodes_alive, write_cl, read_cl, serial_cl=None, check_ret=True):
    """The dtests' _test_insert_query_from_node(): write users 0..99 at
    write_cl (with serial_cl, as IF NOT EXISTS), then read them at read_cl,
    or check that one write or read is unavailable."""
    start, end, age = 0, 100, 30
    if should_succeed(write_cl, rf_factors, num_nodes_alive, dc_idx):
        for userid in range(start, end):
            insert_user(cql, userid, age, write_cl, serial_cl)
    else:
        with pytest.raises(UNAVAILABLE):
            insert_user(cql, end, age, write_cl, serial_cl)
    if should_succeed(read_cl, rf_factors, num_nodes_alive, dc_idx):
        for userid in range(start, end):
            query_user(cql, userid, age, read_cl, check_ret)
    else:
        with pytest.raises(UNAVAILABLE):
            query_user(cql, end, age, read_cl, check_ret)


async def start_availability_cluster(manager: ScyllaClusterManager, dcs, rf):
    """The dtests' _start_cluster(): 3 nodes in rack1..rack3 of each DC, a
    keyspace mytestks with the given RF, and the users and counters tables."""
    property_files = [p for dc in dcs for p in topology({"rack1": 1, "rack2": 1, "rack3": 1}, dc=dc)]
    servers = await manager.servers_add(len(property_files), config=AVAILABILITY_CONFIG, property_file=property_files)
    cql = await exclusive_one(manager, servers[0])
    create_ks(cql, "mytestks", rf)
    cql.execute("CREATE TABLE mytestks.users (userid int PRIMARY KEY, firstname text, lastname text, age int)")
    cql.execute("CREATE TABLE mytestks.counters (id int PRIMARY KEY, c counter)")
    return servers


async def stop_and_wait_other_notice(manager: ScyllaClusterManager, server):
    await manager.server_stop_gracefully(server.server_id)
    for other in await manager.running_servers():
        await manager.server_not_sees_other_server(other.ip_addr, server.ip_addr)






# Ported from scylla-dtest cql_tests.py::TestMiscellaneousCQL::test_query_failed_when_node_is_down.
# With RF=2 on 2 nodes (rack1 and rack2 of datacenter1), 1000 rows flushed
# and node2 stopped (gracefully or killed), a CL=ONE read through node1
# works, and a CL=QUORUM read, single-row or a count, fails with Unavailable
# (the count also with NoHostAvailable: the dtests' retry policy retries its
# server error on the next host, and node1 is the only one) soon enough:
# within 1 s after a graceful stop, and within the request timeout plus the
# retries, at most 20 s, after a kill.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("stop_gently, query_type", ((gently, query) for gently in [True, False] for query in ["point", "count"]))
async def test_query_failed_when_node_is_down(manager: ScyllaClusterManager, stop_gently: bool, query_type: str):
    # A short read/range request timeout makes the first try time out
    # sooner when node2 is killed.
    request_timeout_in_ms = randint(1, 10) * 1000
    config_option = "read_request_timeout_in_ms" if query_type == "point" else "range_request_timeout_in_ms"
    node1, node2 = await manager.servers_add(2, config={**DTEST_CONFIG, config_option: request_timeout_in_ms},
                                             property_file=topology({"rack1": 1, "rack2": 1}))
    cql = dtest_session(manager.con_gen([node1.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect())
    create_ks(cql, "ks", 2)
    ks, cf = "ks", "cf"
    cql.execute(f"CREATE TABLE {ks}.{cf} (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
    for pk in range(10):
        for ck in range(100):
            cql.execute(SimpleStatement(f"INSERT INTO {ks}.{cf} (pk, ck, v) VALUES ({pk}, {ck}, 'foo')", consistency_level=CL.ALL))
    for s in [node1, node2]:
        await nodetool(manager, s, "flush")

    if stop_gently:
        await manager.server_stop_gracefully(node2.server_id)
    else:
        await manager.server_stop(node2.server_id, convict=False)
    await manager.server_not_sees_other_server(node1.ip_addr, node2.ip_addr)

    # The dtests' patient_cql_cluster_session(node1, ks, exclusive=True).
    session = await exclusive_one(manager, node1)
    session.get_execution_profile(EXEC_PROFILE_DEFAULT).retry_policy = FLAKY_RETRY_POLICY
    if query_type == "point":
        query = f"SELECT v from {ks}.{cf} WHERE pk = 1 AND ck = 1 BYPASS CACHE"
        assert [list(r) for r in session.execute(SimpleStatement(query, consistency_level=CL.ONE))] == [["foo"]]
    else:
        query = f"SELECT count(*) from {ks}.{cf} BYPASS CACHE"
        assert [list(r) for r in session.execute(SimpleStatement(query, consistency_level=CL.ONE))] == [[1000]]

    t0 = time.time()
    expected = UNAVAILABLE if query_type == "point" else UNAVAILABLE + (NoHostAvailable,)
    with pytest.raises(expected):
        # The dtests' cql_timeout(60) in release mode.
        session.execute(SimpleStatement(query, consistency_level=CL.QUORUM), timeout=60)
    dt = time.time() - t0

    allowed_timeout = 1
    if not stop_gently:
        request_timeout = request_timeout_in_ms / 1000
        # The request should fail up to request_timeout + 1 seconds after
        # max_retries or after the node is marked as DN by gossip, the
        # earlier of the two.
        allowed_timeout += 1 + request_timeout + min(request_timeout * 5, 20)
    assert dt <= allowed_timeout, f"Query took too long to timeout: {dt} > {allowed_timeout}"
