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
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, TokenAwarePolicy
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.cluster.dtest_helpers import DTEST_CONFIG, create_ks, dtest_session, exclusive_one, nodetool, topology
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CL = ConsistencyLevel
KEYS = range(10)


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


def simple_query_validate(session, read_keys: int, query: str, read_cls_pass, read_cls_fail):
    """TestSimpleCluster.simple_query_validate() with read_cls_pass_all."""
    for cl in read_cls_pass:
        res = list(session.execute(SimpleStatement(query, consistency_level=cl)))
        assert len(res) == read_keys, f"got {len(res)} expected {read_keys} : {res}"
    for cl in read_cls_fail:
        with pytest.raises(Unavailable):
            list(session.execute(SimpleStatement(query, consistency_level=cl)))


UNAVAILABLE = (Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure)
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
