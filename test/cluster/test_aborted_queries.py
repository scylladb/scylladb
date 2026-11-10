#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Reads time out when the coordinator's read and range request timeouts are
# zero: a query run on the coordinator itself, queries the coordinator sends
# to another node, and a materialized view query. Ported from scylla-dtest
# cql_tests.py (TestAbortedQueries, CASSANDRA-7392). Keyspaces use vnodes.
# The multi-node dtests start node1 with join_ring=False, which ccm ignores
# for Scylla: both nodes own tokens, as in the ports.

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, ReadFailure, ReadTimeout, Unavailable, WriteFailure, WriteTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionShutdown
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.dtest_helpers import DTEST_CONFIG, create_ks, dtest_session
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

UNAVAILABLE = (Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure)
NO_TIMEOUTS = ["--read-request-timeout-in-ms=0", "--range-request-timeout-in-ms=0"]
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
RF1 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"


def exclusive_session(manager: ScyllaClusterManager, server):
    """A session whose requests all go to the given node."""
    return manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()


def wait_reconnection(cql, timeout=120):
    """Waits until the session gets an answer from the node again. With no
    time for reads, a timeout is an answer too."""
    deadline = time.time() + timeout
    while True:
        try:
            cql.execute("SELECT key FROM system.local")
            return
        except UNAVAILABLE:
            return
        except (NoHostAvailable, ConnectionShutdown, OperationTimedOut):
            assert time.time() < deadline, "the session did not reconnect"
            time.sleep(1)


def dtest_wait_reconnection(session, num_attempts=5, sleep_time=5, allowed_exceptions=None):
    """The dtests' tools/session.py wait_reconnection(): up to num_attempts
    "SELECT * FROM system.local", sleep_time apart, until one succeeds or
    fails with an allowed exception; other failures are retried."""
    for i in range(num_attempts):
        try:
            logger.debug(f"wait_reconnection: query results: {list(session.execute('SELECT * FROM system.local'))}")
            return
        except NoHostAvailable:
            pass
        except Exception as e:  # noqa: BLE001
            if allowed_exceptions and isinstance(e, allowed_exceptions):
                logger.debug(f"wait_reconnection: allowed exception raised: {e}")
                return
        logger.debug(f"wait_reconnection: attempt {i + 1} of {num_attempts} failed, sleep for {sleep_time} secs and retry")
        time.sleep(sleep_time)
    logger.warning("wait_reconnection: no host available")


async def restart(manager: ScyllaClusterManager, server, cmdline):
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_cmdline(server.server_id, cmdline)
    await manager.server_start(server.server_id, connect_driver=False)


def fallthrough(statement):
    statement.consistency_level = ConsistencyLevel.ONE
    statement.retry_policy = FallthroughRetryPolicy()
    return statement


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_local_query(manager: ScyllaClusterManager):
    """A query running on the coordinator node times out."""
    server = await manager.server_add(config=VNODES)
    cql = exclusive_session(manager, server)
    cql.execute(f"CREATE KEYSPACE ks {RF1}")
    cql.execute("""
        CREATE TABLE ks.test1 (
            id int PRIMARY KEY,
            val text
        );
    """)
    for i in range(500):
        cql.execute(f"INSERT INTO ks.test1 (id, val) VALUES ({i}, 'foo')")

    await restart(manager, server, NO_TIMEOUTS)
    wait_reconnection(cql)
    with pytest.raises(UNAVAILABLE + (NoHostAvailable, ConnectionShutdown)):
        cql.execute(fallthrough(SimpleStatement("SELECT * from ks.test1")))


async def two_nodes(manager: ScyllaClusterManager):
    """The dtests' populate(2): SimpleSnitch, both nodes in datacenter1/rack1,
    node1 started first. Returns the nodes and a session to node1 alone,
    with statements at CL=ONE by default, as the dtests'
    patient_exclusive_cql_connection(node1)."""
    servers = []
    for _ in range(2):
        servers.append(await manager.server_add(config={**DTEST_CONFIG, "endpoint_snitch": "SimpleSnitch"},
                                                property_file={"dc": "datacenter1", "rack": "rack1"}))
    return servers, dtest_session(exclusive_session(manager, servers[0]))


# Ported from scylla-dtest cql_tests.py::TestAbortedQueries::test_remote_query.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_remote_query(manager: ScyllaClusterManager):
    """Queries a coordinator sends to the node owning the data time out."""
    (node1, node2), cql = await two_nodes(manager)
    create_ks(cql, "ks", 1)
    # The dtests' create_ks() also switches the session to the keyspace, and
    # the statements below name the table without it: after the restart, the
    # driver's new connections re-prepare them before their USE.
    cql.execute("USE ks")
    cql.execute("""
        CREATE TABLE test2 (
            id int,
            col int,
            val text,
            PRIMARY KEY(id, col)
        );
    """)
    num_keys, num_rows = 500, 10
    for i in range(num_keys):
        for j in range(num_rows):
            cql.execute(f"INSERT INTO test2 (id, col, val) VALUES ({i}, {j}, 'foo')")

    await manager.server_stop_gracefully(node2.server_id)
    statement = fallthrough(cql.prepare("SELECT * from test2 where id = ? BYPASS CACHE"))
    # Keys that only node2, which is down, has: look for them through node1.
    ids = []
    for i in range(num_keys):
        try:
            cql.execute(statement, (i,))
        except Unavailable:
            ids.append(i)
            if len(ids) >= 3:
                break

    # Restart the cluster with no time for reads.
    await manager.server_stop_gracefully(node1.server_id)
    for server in [node1, node2]:
        await manager.server_update_config(server.server_id, config_options={
            "read_request_timeout_in_ms": 0, "range_request_timeout_in_ms": 0}, reload=False)
    await asyncio.gather(*[manager.server_start(s.server_id, connect_driver=False) for s in [node1, node2]])
    # The local query may time out.
    dtest_wait_reconnection(cql, allowed_exceptions=(ReadTimeout,))

    with pytest.raises(UNAVAILABLE):
        cql.execute(statement, ids[0:1])
    statement = fallthrough(cql.prepare("SELECT * from test2 where id IN (?, ?, ?) AND col < 10 BYPASS CACHE"))
    with pytest.raises(UNAVAILABLE):
        cql.execute(statement, ids)
    with pytest.raises(UNAVAILABLE):
        cql.execute(fallthrough(SimpleStatement("SELECT * from test2 where col > 5 ALLOW FILTERING BYPASS CACHE")))
    with pytest.raises(UNAVAILABLE):
        cql.execute(fallthrough(SimpleStatement("SELECT * from test2 BYPASS CACHE")))


# Ported from scylla-dtest cql_tests.py::TestAbortedQueries::test_materialized_view.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_materialized_view(manager: ScyllaClusterManager):
    """A materialized view query times out."""
    (node1, _), cql = await two_nodes(manager)
    create_ks(cql, "ks", 1)
    cql.execute("""
        CREATE TABLE ks.test4 (
            id int PRIMARY KEY,
            col int,
            val text
        );
    """)
    cql.execute("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.test4 WHERE col IS NOT NULL AND id IS NOT NULL PRIMARY KEY (col, id)")
    for i in range(50):
        cql.execute(f"INSERT INTO ks.test4 (id, col, val) VALUES ({i}, {i // 10}, 'foo')")

    await restart(manager, node1, NO_TIMEOUTS)
    wait_reconnection(cql)
    with pytest.raises(UNAVAILABLE + (NoHostAvailable, ConnectionShutdown)):
        cql.execute(fallthrough(SimpleStatement("SELECT * FROM ks.mv WHERE col = 50")))
