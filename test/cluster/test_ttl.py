#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for classic CQL TTL (USING TTL / default_time_to_live) on several
# nodes, ported from scylla-dtest ttl_test.py: rows of a four-node cluster
# expire by their default or explicit TTL, and the TTL of a write is the same
# on every replica, whether the replica got the data at once, from hints
# after it was down, or from repair after the replication factor was raised.
# Like the dtests' nodes, the keyspaces use vnodes, sessions default to
# CL=ONE, and nodes are stopped gracefully.

import logging
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.dtest_helpers import cql_one, exclusive_one, nodetool
from test.cluster.util import dump_sstables
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest

logger = logging.getLogger(__name__)

# The dtests' populate(n): SimpleSnitch, every node in datacenter1/rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch", "tablets_mode_for_new_keyspaces": "disabled"}
DATACENTER1_RACK1 = {"dc": "datacenter1", "rack": "rack1"}

TTL_TABLE = "CREATE TABLE ttl_table (key int primary key, col1 int, col2 int, col3 int, )"


def rows(cql, query: str, cl=ConsistencyLevel.ONE) -> list[list]:
    return [list(r) for r in cql.execute(SimpleStatement(query, consistency_level=cl))]


def assert_all(cql, query: str, expected: list, cl=ConsistencyLevel.ONE) -> None:
    res = rows(cql, query, cl)
    assert res == expected, f"Expected {expected} from {query}, but got {res}"


def assert_row_count(cql, table: str, expected: int, cl=ConsistencyLevel.ONE) -> None:
    count = rows(cql, f"SELECT count(*) FROM {table}", cl)[0][0]
    assert count == expected, f"Expected a row count of {expected} in table '{table}', but got {count}"


def create_ttl_table(cql, default_time_to_live=None, create_table_statement=TTL_TABLE) -> None:
    """The table part of the dtests' prepare(): drop ttl_table if it exists,
    then create it, with the default TTL if given."""
    cql.execute("DROP TABLE IF EXISTS ttl_table")
    query = create_table_statement
    if default_time_to_live:
        query += f" WITH default_time_to_live = {default_time_to_live};"
    cql.execute(query)


async def prepare_distributed(manager: ScyllaClusterManager, default_time_to_live=None, cmdline=None,
                              options=None, replication_factor=2, rack_num=None):
    """The dtests' TestDistributedTTL.prepare(): two nodes in datacenter1,
    one per rack (rack1, rack2), keyspace ks with the given RF, and
    ttl_table. Returns the servers and a session (CL=ONE) using ks."""
    rack_num = rack_num or replication_factor
    config = {"tablets_mode_for_new_keyspaces": "disabled", **(options or {})}
    servers = await manager.servers_add(rack_num, config=config, cmdline=cmdline,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(rack_num)])
    cql = cql_one(manager)
    cql.execute(f"CREATE KEYSPACE ks WITH replication = {{'class':'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    create_ttl_table(cql, default_time_to_live)
    return servers, cql


async def exclusive_ks(manager: ScyllaClusterManager, server):
    """The dtests' patient_exclusive_cql_connection() followed by USE ks."""
    cql = await exclusive_one(manager, server)
    cql.execute("USE ks;")
    return cql


async def print_sstable(manager: ScyllaClusterManager, server, keyspace: str, table: str) -> None:
    """The dtests' print_sstable(): flush the node and dump the table's sstables."""
    await nodetool(manager, server, "flush")
    sstable_dump = await dump_sstables(manager, server, keyspace, table)
    logger.debug("[%s] SStable dump of %s.%s: %s", server.ip_addr, keyspace, table, sstable_dump)


async def node_repair(manager: ScyllaClusterManager, server, keyspace: str) -> None:
    """ccm's node.repair(keyspace=...): of a vnodes keyspace, `nodetool repair <ks>`."""
    vnode_keyspaces = await manager.api.client.get_json("/storage_service/keyspaces", host=server.ip_addr,
                                                        params={"replication": "vnodes"})
    assert keyspace in vnode_keyspaces
    await nodetool(manager, server, "repair", keyspace)


# Ported from scylla-dtest ttl_test.py::TestTTL::test_row_marker_for_ttl.
# Rows of a table with a clustering key expire by the default TTL in force
# when they were written (altered after the table's creation) or by their
# explicit TTL, each row with its row marker. Four nodes, RF=1.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_row_marker_for_ttl(manager: ScyllaClusterManager, build_mode: str):
    await manager.servers_add(4, config=SIMPLE_SNITCH, property_file=DATACENTER1_RACK1)
    cql = cql_one(manager)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    create_ttl_table(cql, default_time_to_live=1,
                     create_table_statement="CREATE TABLE ttl_table (key int, col1 int, col2 int, col3 int, primary key(key, col1))")

    # debug SELECTs can take >1s and the first read must beat default_ttl
    default_ttl, explicit_ttl = (10, 15) if build_mode == "debug" else (4, 9)
    cql.execute(f"ALTER TABLE ttl_table WITH default_time_to_live = {default_ttl};")
    cql.execute("INSERT INTO ttl_table (key, col1, col2, col3) VALUES (1, 1, 1, 1);")
    start_default = time.time()
    cql.execute(f"INSERT INTO ttl_table (key, col1, col2, col3) VALUES (1, 2, 2, 2) USING TTL {explicit_ttl};")
    start_explicit = time.time()

    def get_rows():
        return [list(row) for row in cql.execute("SELECT * FROM ttl_table;")]

    expected = [[1, 1, 1, 1], [1, 2, 2, 2]]
    assert get_rows() == expected

    def wait_for_rows_to_change(expected_cur, expected_next, start, ttl):
        delta = time.time() - start
        current = get_rows()
        while current == expected_cur and delta < ttl + 2:
            time.sleep(1)
            delta = time.time() - start
            current = get_rows()
        logger.debug("Got %s after %s seconds", current, delta)
        assert current == expected_next, f"Expected the following rows: {expected_next}, but got: {current}"
        assert ttl - 1 <= delta, f"Expected delta time to be greater than {ttl - 1} seconds, but got {delta}"

    wait_for_rows_to_change(expected, [[1, 2, 2, 2]], start_default, default_ttl)
    wait_for_rows_to_change([[1, 2, 2, 2]], [], start_explicit, explicit_ttl)
    assert_row_count(cql, "ttl_table", 0)


# Ported from scylla-dtest ttl_test.py::TestDistributedTTL::test_ttl_is_replicated.
# A write getting the table's default TTL has the same TTL on both replicas,
# read with ttl() from each node, and expires on both.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ttl_is_replicated(manager: ScyllaClusterManager):
    servers, _ = await prepare_distributed(manager, default_time_to_live=5)
    session1 = await exclusive_ks(manager, servers[0])
    session2 = await exclusive_ks(manager, servers[1])
    session1.execute(SimpleStatement("INSERT INTO ttl_table (key, col1) VALUES (1, 1);", consistency_level=ConsistencyLevel.ALL))
    assert_all(session1, "SELECT * FROM ttl_table;", [[1, 1, None, None]], cl=ConsistencyLevel.ALL)
    ttl_session1 = list(session1.execute("SELECT ttl(col1) FROM ttl_table;"))
    ttl_session2 = list(session2.execute("SELECT ttl(col1) FROM ttl_table;"))
    # The two queries are not executed simultaneously, so the remaining TTLs
    # can differ by one second.
    assert abs(ttl_session1[0][0] - ttl_session2[0][0]) <= 1

    time.sleep(7)

    assert rows(session1, "SELECT * FROM ttl_table;", cl=ConsistencyLevel.ALL) == []


# Ported from scylla-dtest ttl_test.py::TestDistributedTTL::test_ttl_is_respected_on_delayed_replication.
# Writes made while node2 is down reach it later: once node1 is back (and
# cleaned up), the row that expired meanwhile is gone at CL=ALL, and the
# live row has the same TTL on both nodes.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ttl_is_respected_on_delayed_replication(manager: ScyllaClusterManager):
    servers, session1 = await prepare_distributed(manager, options={"shadow_round_ms": 1000})
    node1, node2 = servers
    logger.debug("Stopping node2")
    await manager.server_stop_gracefully(node2.server_id)
    short_ttl = 5
    long_ttl = 1000
    session1.execute(f"INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL {short_ttl};")
    session1.execute(f"INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL {long_ttl};")
    assert_all(session1, "SELECT * FROM ttl_table;", [[1, 1, None, None], [2, 2, None, None]])
    time.sleep(short_ttl + 2)
    logger.debug("Stopping node1")
    await manager.server_stop_gracefully(node1.server_id)
    logger.debug("Restarting node2")
    await manager.server_start(node2.server_id)
    session2 = await exclusive_ks(manager, node2)
    # node1 is down, so node2 has no replica of the data yet.
    assert_row_count(session2, "ttl_table", 0)
    logger.debug("Restarting node1")
    await manager.server_start(node1.server_id)
    session1 = await exclusive_ks(manager, node1)
    await nodetool(manager, node1, "cleanup")

    assert_all(session2, "SELECT count(*) FROM ttl_table", [[1]], cl=ConsistencyLevel.ALL)
    assert_all(session2, "SELECT * FROM ttl_table;", [[2, 2, None, None]], cl=ConsistencyLevel.ALL)

    # The TTL is the same on both nodes.
    ttl_session1 = list(session1.execute("SELECT ttl(col1) FROM ttl_table;"))
    ttl_session2 = list(session2.execute("SELECT ttl(col1) FROM ttl_table;"))
    logger.debug("ttl_session1=%s ttl_session2=%s", ttl_session1, ttl_session2)
    assert abs(ttl_session1[0][0] - ttl_session2[0][0]) <= 1


# Ported from scylla-dtest ttl_test.py::TestDistributedTTL::test_ttl_is_respected_on_repair.
# Data written with RF=1, one row of which expires; then RF is raised to 2
# and every node repaired. node2 alone then returns only the live row, with
# the TTL node1 had. With and without parallelized aggregation (count(*)),
# whose forwarding is traced as in the dtest.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("enable_parallized_aggregation", ["true", "false"],
                         ids=["parallel_aggregates_enabled", "parallel_aggregates_disabled"])
async def test_ttl_is_respected_on_repair(manager: ScyllaClusterManager, enable_parallized_aggregation):
    servers, session1 = await prepare_distributed(
        manager, cmdline=["--logger-log-level", "forward_service=trace", "--enable-parallelized-aggregation", enable_parallized_aggregation],
        replication_factor=1, rack_num=2)
    node1, node2 = servers
    short_ttl = 5
    long_ttl = 1000

    session1.execute(f"INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL {short_ttl};")
    session1.execute(f"INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL {long_ttl};")

    assert_all(session1, "SELECT * FROM ttl_table;", [[1, 1, None, None], [2, 2, None, None]])
    time.sleep(short_ttl + 2)
    logger.debug("SStable dump after writes with RF = 1")
    await print_sstable(manager, node1, "ks", "ttl_table")
    await print_sstable(manager, node2, "ks", "ttl_table")
    session1 = await exclusive_ks(manager, node1)
    dc = session1.execute("SELECT data_center FROM system.local").one()[0]
    # The dtests' get_replication_options(): RF of ks is a number, so the new
    # RF is 2 (not a rack list).
    replication = session1.execute("SELECT * FROM system_schema.keyspaces  WHERE keyspace_name = 'ks'").one().replication
    logger.debug("ks replication: %s", replication)
    # The dtests' change_schema_safely(): wait for every node to log the
    # schema change, then for schema agreement.
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    session1.execute(f"""
            ALTER KEYSPACE ks WITH REPLICATION =
            {{'class' : 'NetworkTopologyStrategy', '{dc}' : 2}};
        """)
    for log, mark in zip(logs, marks):
        await log.wait_for("Schema version changed", from_mark=mark, timeout=30)
    assert session1.cluster.control_connection.wait_for_schema_agreement(wait_time=30)
    for s in servers:
        await node_repair(manager, s, "ks")
    logger.debug("SStable dump after alter RF to 2 and repair")
    await print_sstable(manager, node1, "ks", "ttl_table")
    await print_sstable(manager, node2, "ks", "ttl_table")

    # node2.stop(wait_other_notice=True)
    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_not_sees_other_server(node1.ip_addr, node2.ip_addr)
    ttl_start = time.time()
    ttl_session1 = session1.execute("SELECT ttl(col1) FROM ttl_table;").one()[0]
    assert_all(session1, "SELECT * FROM ttl_table;", [[2, 2, None, None]])

    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_start(node2.server_id)
    session2 = await exclusive_ks(manager, node2)
    assert_row_count(session2, "ttl_table", 1)
    assert_all(session2, "SELECT * FROM ttl_table;", [[2, 2, None, None]])

    # The TTL is the same on both nodes.
    ttl_session2 = session2.execute("SELECT ttl(col1) FROM ttl_table;").one()[0]
    ttl_session1 -= time.time() - ttl_start
    # The dtests' assert_almost_equal(..., error=0.005).
    vmax, vmin = max(ttl_session1, ttl_session2), min(ttl_session1, ttl_session2)
    assert vmin > vmax * (1.0 - 0.005) or vmin == vmax, f"values not within 0.50% of the max: {(ttl_session1, ttl_session2)}"
