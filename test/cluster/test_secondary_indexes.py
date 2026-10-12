#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Global and local secondary indexes, ported from scylla-dtest
# secondary_indexes_test.py, one test per dtest. Like the dtests' prepare(),
# the clusters spread their nodes over three racks (one rack for a single
# node), the keyspace "ks" (or the dtest's own name) uses vnodes with the
# dtest's RF, and the session's statements default to QUORUM (ONE on a
# single node). The base tables and the index views use one compaction
# strategy, drawn at random once per process like the dtests'
# random_compaction_strategy().

import logging
import random
import re
import string
import time
from collections import defaultdict

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut, WriteFailure
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The tables and index views use one compaction strategy, drawn at random once
# per process like the dtests' random_compaction_strategy().
COMPACTION = random.choice(["LeveledCompactionStrategy", "SizeTieredCompactionStrategy",
                            "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"])
RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
USE_FILTERING = ("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. "
                 "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING")
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
# The dtests' cluster_options(enable_create_table_with_compact_storage=True).
COMPACT_STORAGE = {"enable_create_table_with_compact_storage": True}
LONG_TEXT_LENGTH = 8193
OVERSIZE_LENGTH = 66536
OVERSIZE_MESSAGE = "Key size too large|is longer than maximum"


async def prepare(manager: ScyllaClusterManager, session_rack=1, nodes=4, config=None):
    """The dtests' prepare(): the nodes spread over three racks, rack1 first
    (four nodes: two of them in rack1), and a session to a node of the given
    rack whose statements default to QUORUM."""
    racks = [f"rack{r + 1}" for r in range(3) for _ in range(nodes // 3 + (1 if r < nodes % 3 else 0))]
    # auto_snapshot: Scylla's default, as on the dtest nodes; test.py turns it off.
    servers = await manager.servers_add(nodes, config={**VNODES, "auto_snapshot": True, **(config or {})},
                                        property_file=[{"dc": "dc1", "rack": r} for r in racks])
    node = servers[racks.index(f"rack{session_rack}")]
    cql = manager.con_gen([node.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.QUORUM
    return servers, cql


def create_ks(cql, name, rf):
    """The dtests' create_ks(): an RF keyspace the session then uses."""
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"USE {name}")


async def prepare_ks(manager: ScyllaClusterManager, nodes, rf, keyspace_name="ks", user_table=False, session_rack=1, config=None):
    """The dtests' prepare(nodes=..., rf=...): the cluster, a session (CL
    QUORUM, ONE on a single node), the keyspace, and optionally the users
    table."""
    servers, cql = await prepare(manager, session_rack=session_rack, nodes=nodes, config=config)
    if nodes == 1:
        cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    create_ks(cql, keyspace_name, rf)
    if user_table:
        create_users(cql, keyspace_name)
    return servers, cql


def create_cf(cql, table, key_type, columns=None, compaction=f"{{'class': '{COMPACTION}'}}", primary_key=None, compact_storage=False):
    """The dtests' create_cf(): with no columns, a (key, c, v) table;
    compaction None is create_cf()'s default (STCS). It waits for schema
    agreement and checks that the table was created by reading from it."""
    if columns is None:
        schema = f"key {key_type}, c varchar, v varchar, PRIMARY KEY(key, c)"
    elif primary_key:
        schema = f"key {key_type}" + "".join(f", {k} {v}" for k, v in columns.items()) + f", PRIMARY KEY({primary_key})"
    else:
        schema = f"key {key_type} PRIMARY KEY" + "".join(f", {k} {v}" for k, v in columns.items())
    if compaction is None:
        compaction = "{'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}"
    query = f"CREATE COLUMNFAMILY {table} ({schema}) WITH comment='test cf' AND compaction={compaction} AND compression = {{}}"
    if compact_storage:
        query += " AND COMPACT STORAGE"
    cql.execute(query)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"SELECT * FROM {table} LIMIT 1")


def create_users(cql, ks):
    create_cf(cql, f"{ks}.users", "varchar", {"password": "varchar", "gender": "varchar", "session_token": "varchar",
                                              "state": "varchar", "birth_year": "bigint"})


def wait_for_view(cql, servers, ks, view, timeout=600):
    """Waits until every node reports the view built in view_build_status."""
    query = SimpleStatement(f"SELECT host_id, status FROM system_distributed.view_build_status "
                            f"WHERE keyspace_name = '{ks}' AND view_name = '{view}'",
                            consistency_level=ConsistencyLevel.ONE if len(servers) == 1 else ConsistencyLevel.QUORUM)
    deadline = time.time() + timeout
    while True:
        done = {r.host_id for r in cql.execute(query) if r.status == "SUCCESS"}
        if len(done) == len(servers):
            return
        assert time.time() < deadline, f"view {ks}.{view} is not built"
        time.sleep(0.5)


def create_index(cql, table, column, name, pk_name=None, compaction=None):
    """The dtests' create_index() / create_local_index(): column may be a
    list of columns; with a compaction, the index view is altered to it."""
    columns = ", ".join([column] if isinstance(column, str) else list(column))
    target = f"(({pk_name}), {columns})" if pk_name else f"({columns})"
    cql.execute(f"CREATE INDEX {name} ON {table} {target}")
    if compaction:
        cql.execute(f"ALTER MATERIALIZED VIEW {name}_index WITH compaction={{'class': '{compaction}'}}")


def create_and_build_index(cql, servers, ks, table, column, name, pk_name=None, compaction=COMPACTION):
    target = f"(({pk_name}), {column})" if pk_name else f"({column})"
    cql.execute(f"CREATE INDEX {name} ON {ks}.{table} {target}")
    if compaction:
        cql.execute(f"ALTER MATERIALIZED VIEW {ks}.{name}_index WITH compaction = {{'class': '{compaction}'}}")
    # As in the dtests: updates of a local index (pk_name) are synchronous
    # already, a global index view is altered to synchronous_updates.
    if not pk_name:
        cql.execute(f"ALTER MATERIALIZED VIEW {ks}.{name}_index WITH synchronous_updates = true")
    wait_for_view(cql, servers, ks, f"{name}_index")
    rows = cql.execute(f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{ks}' and table_name = '{table}' AND index_name = '{name}'")
    assert len(list(rows)) == 1


def rows(cql, query, cl=None):
    if cl is not None:
        query = SimpleStatement(query, consistency_level=cl)
    return [list(r) for r in cql.execute(query)]


def assert_all(cql, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False, num_attempts=1, sleep_time=10):
    """The dtests' assert_all(): CL ONE unless given, retried sleep_time
    seconds apart."""
    for attempt in range(num_attempts):
        res = rows(cql, query, cl)
        if ignore_order:
            res, exp = sorted(res, key=repr), sorted(expected, key=repr)
        else:
            exp = expected
        if res == exp or attempt == num_attempts - 1:
            assert res == exp, f"{query}: expected {exp}, got {res}"
            return
        time.sleep(sleep_time)


def assert_none(cql, query, cl=None):
    """The dtests' assert_none(): the session's CL unless given."""
    res = rows(cql, query, cl)
    assert res == [], f"{query}: expected no rows, got {res}"


def assert_row_count(cql, table, expected, cl=ConsistencyLevel.ONE, num_attempts=1, sleep_time=10):
    assert_all(cql, f"SELECT count(*) FROM {table}", [[expected]], cl=cl, num_attempts=num_attempts, sleep_time=sleep_time)


def assert_row_count_in_select(cql, query, expected, cl=ConsistencyLevel.ONE, num_attempts=1):
    for attempt in range(num_attempts):
        count = len(rows(cql, query, cl))
        if count == expected or attempt == num_attempts - 1:
            assert count == expected, f"{query}: expected {expected} rows, got {count}"
            return
        time.sleep(10)


def assert_invalid(cql, query, matching, expected=InvalidRequest):
    with pytest.raises(expected, match=matching):
        cql.execute(query)


def wait_for_schema_agreement(cql):
    """The dtests' tools.data.wait_for_schema_agreement(): the node's
    schema version is that of all its peers."""
    while True:
        local = list(cql.execute("SELECT schema_version FROM system.local WHERE key='local'"))[0]
        if all(peer == local for peer in cql.execute("SELECT schema_version FROM system.peers")):
            return
        time.sleep(1)


def generate_random_text(length=10):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def exclusive_session(manager: ScyllaClusterManager, server):
    cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    return cql


Q = ConsistencyLevel.QUORUM
ALL = ConsistencyLevel.ALL
GATE = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")


# Ported from scylla-dtest secondary_indexes_test.py::TestStaticSecondaryIndexes::test_query_data_with_index.
# An index on a static column, built on existing rows: queries by it alone,
# with the partition key, and with the clustering key (ALLOW FILTERING),
# deletions, TTL expiry, truncation, and counting rows of a partition, last
# by the partition key, clustering key and static column index together.
# The dtest is @unmark.next_gating in a dtest_full class.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_query_data_with_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    create_cf(cql, "ks.statics", "text", {"ck": "text", "cs1": "text static"}, primary_key="key, ck")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc1', 'val1', 'stat1');")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc2', 'val2', 'stat2');")
    create_and_build_index(cql, servers, "ks", "statics", "cs1", "cs1_key")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc3', 'val3', 'stat3');")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc4', 'val4', 'stat3');")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc5', 'val5', 'stat2');")

    assert_all(cql, "select count(*) from statics", [[5]], cl=Q)
    assert_all(cql, "select count(*) from statics where cs1='stat3'", [[2]], cl=Q, num_attempts=10)
    assert_all(cql, "select count(*) from statics where cs1='stat2'", [[2]], cl=Q)
    assert_all(cql, "select count(*) from statics where cs1='stat1'", [[1]], cl=Q)
    assert_all(cql, "select count(*) from statics where key='abc1' and cs1='stat1'", [[1]], cl=Q)
    assert_invalid(cql, "select count(*) from statics where ck='val1' and cs1='stat1'", "use ALLOW FILTERING")
    assert_all(cql, "select count(*) from statics where ck='val1' and cs1='stat1' ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "select count(*) from statics where cs1='stat2' and cs1='stat1' and key='abc2'", [[0]], cl=Q)

    cql.execute("DELETE FROM statics WHERE KEY='abc3';")
    assert_all(cql, "select count(*) from statics where cs1='stat3'", [[1]], cl=Q, num_attempts=10)

    cql.execute("UPDATE statics USING TTL 15 SET cs1='I will expire' WHERE key='abc5';")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc6', 'val6', 'stat4') USING TTL 15;")
    assert_all(cql, "select count(*) from statics where cs1='I will expire'", [[1]], cl=Q, num_attempts=10, sleep_time=1)
    assert_all(cql, "select count(*) from statics where cs1='stat4'", [[1]], cl=Q)
    # As in the dtest: index rows share the base cell's expiry, so poll
    # rather than sleep a fixed TTL.
    assert_all(cql, "select count(*) from statics where cs1='stat4'", [[0]], cl=Q, num_attempts=20, sleep_time=1)
    assert_all(cql, "select count(*) from statics where cs1='I will expire'", [[0]], cl=Q)
    assert_all(cql, "select count(*) from statics where cs1='stat4'", [[0]], cl=Q)

    for s in servers:
        await manager.api.flush_all_keyspaces(s.ip_addr)
    cql.execute("TRUNCATE table statics")
    assert_all(cql, "select count(*) from statics", [[0]], cl=Q)
    assert_all(cql, "select count(*) from statics where cs1='stat3'", [[0]], cl=Q, num_attempts=10)
    assert_all(cql, "select count(*) from statics where cs1='stat2'", [[0]], cl=Q)

    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc1', 'val1', 'stat1');")
    cql.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc2', 'val1', 'stat1');")
    cql.execute("INSERT INTO statics (KEY, ck) VALUES ('abc1', 'val2');")
    assert_all(cql, "select count(*) from statics where cs1='stat1'", [[3]], cl=Q, num_attempts=10)

    assert_all(cql, "select count(*) from statics where key='abc1' and ck='val1' and cs1='stat1'", [[1]], cl=Q, num_attempts=10)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_agg_query_by_pk.
# Aggregates filtered by the first partition key column, with ALLOW
# FILTERING and then through an index on it, also for rows added later.
@GATE
async def test_agg_query_by_pk(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    cql.execute("CREATE TABLE ks.t3(pk1 int, pk2 int, ck int, PRIMARY KEY((pk1, pk2), ck))")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 1)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT SUM(pk2) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT MIN(ck) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", [[1]], cl=Q)
    create_and_build_index(cql, servers, "ks", "t3", "pk1", "ks_t3")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1", [[1]], cl=Q)
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 2)")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 3)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1", [[3]], cl=Q)
    assert_all(cql, "SELECT SUM(pk2) FROM ks.t3 WHERE pk1 = 1", [[3]], cl=Q)
    assert_all(cql, "SELECT MAX(ck) FROM ks.t3 WHERE pk1 = 1", [[3]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_agg_query_by_second_pk.
# The same, filtered by the second partition key column.
@GATE
async def test_agg_query_by_second_pk(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    cql.execute("CREATE TABLE ks.t3(pk1 int, pk2 int, ck int, PRIMARY KEY((pk1, pk2), ck))")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 1)")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 4)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", [[2]], cl=Q)
    assert_all(cql, "SELECT SUM(pk1) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", [[2]], cl=Q)
    assert_all(cql, "SELECT MIN(ck) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", [[1]], cl=Q)
    create_and_build_index(cql, servers, "ks", "t3", "pk2", "ks_t3")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1", [[2]], cl=Q)
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 2)")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 3)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1", [[4]], cl=Q)
    assert_all(cql, "SELECT SUM(pk1) FROM ks.t3 WHERE pk2 = 1", [[4]], cl=Q)
    assert_all(cql, "SELECT MAX(ck) FROM ks.t3 WHERE pk2 = 1", [[4]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_agg_query_by_ck.
# The same, filtered by the clustering key column.
@GATE
async def test_agg_query_by_ck(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    cql.execute("CREATE TABLE ks.t3(pk1 int, ck int, v int, PRIMARY KEY(pk1, ck))")
    cql.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 1, 1)")
    cql.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 4, 1)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT SUM(pk1) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT MIN(v) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", [[1]], cl=Q)
    create_and_build_index(cql, servers, "ks", "t3", "ck", "ks_t3")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1", [[1]], cl=Q)
    cql.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 2, 2)")
    cql.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 3, 3)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1", [[1]], cl=Q)
    assert_all(cql, "SELECT SUM(pk1) FROM ks.t3 WHERE ck = 1", [[1]], cl=Q)
    assert_all(cql, "SELECT MAX(v) FROM ks.t3 WHERE ck = 3", [[3]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_agg_query_by_second_ck.
# The same, filtered by the second clustering key column.
@GATE
async def test_agg_query_by_second_ck(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    cql.execute("CREATE TABLE ks.t3(pk1 int, ck1 int, ck2 int, PRIMARY KEY(pk1, ck1, ck2))")
    cql.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 1)")
    cql.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 3)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT SUM(pk1) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT MIN(ck1) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", [[1]], cl=Q)
    create_and_build_index(cql, servers, "ks", "t3", "ck2", "ks_t3")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1", [[1]], cl=Q)
    cql.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 2)")
    cql.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 2, 1)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1", [[2]], cl=Q)
    assert_all(cql, "SELECT SUM(pk1) FROM ks.t3 WHERE ck2 = 1", [[2]], cl=Q)
    assert_all(cql, "SELECT MAX(ck1) FROM ks.t3 WHERE ck2 = 1", [[2]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_agg_query_by_two_ck.
# The same, filtered by two clustering key columns, both indexed, one page
# row at a time.
@GATE
async def test_agg_query_by_two_ck(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    cql.default_fetch_size = 1
    cql.execute("CREATE TABLE ks.t3(pk1 int, pk2 int, ck1 int, ck2 int, PRIMARY KEY((pk1, pk2), ck1, ck2))")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 1, 1, 1)")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 1, 1, 2)")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 2, 1, 3)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", [[2]], cl=Q)
    assert_all(cql, "SELECT AVG(pk2) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT MAX(ck2) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", [[3]], cl=Q)
    create_and_build_index(cql, servers, "ks", "t3", "ck1", "ck1_index")
    create_and_build_index(cql, servers, "ks", "t3", "ck2", "ks_t3")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", [[2]], cl=Q)
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 >= 1 ALLOW FILTERING", [[3]], cl=Q)
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (2, 1, 2, 1)")
    cql.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (2, 2, 1, 4)")
    assert_all(cql, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 2 and ck2 in (1, 3) ALLOW FILTERING", [[1]], cl=Q)
    assert_all(cql, "SELECT SUM(pk2) FROM ks.t3 WHERE ck1 = 1 and ck2 > 1 ALLOW FILTERING", [[5]], cl=Q)
    assert_all(cql, "SELECT AVG(pk1) FROM ks.t3 WHERE ck1 = 2 and ck2 >= 1 ALLOW FILTERING", [[2]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_group_by_pk_filter_by_index.
# GROUP BY the partition key of rows filtered by an indexed column, with
# ALLOW FILTERING before the index is built and through the index after.
@GATE
async def test_group_by_pk_filter_by_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    cql.execute("CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck))")
    cql.execute("INSERT INTO ks.t(pk, ck, v) VALUES (1, 2, 3)")
    cql.execute("INSERT INTO ks.t(pk, ck, v) VALUES (1, 4, 3)")
    cql.execute("INSERT INTO ks.t(pk, ck, v) VALUES (2, 4, 3)")
    assert_all(cql, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk ALLOW FILTERING", [[1], [2]], cl=Q)
    create_and_build_index(cql, servers, "ks", "t", "v", "v_key")
    assert_all(cql, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk", [[1], [2]], cl=Q)
    assert_all(cql, "SELECT pk, count(pk) FROM ks.t WHERE v=3 GROUP BY pk", [[1, 2], [2, 1]], cl=Q)


USERS = [("user1", "ch@ngem3a", "f", "TX", 1968), ("user2", "ch@ngem3b", "m", "CA", 1971),
         ("user3", "ch@ngem3c", "f", "FL", 1978), ("user4", "ch@ngem3d", "m", "TX", 1974)]


def insert_users(cql, ks, users):
    for u in users:
        cql.execute(f"INSERT INTO {ks}.users (KEY, password, gender, state, birth_year) VALUES ('{u[0]}', '{u[1]}', '{u[2]}', '{u[3]}', {u[4]});")


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_query_data_created_before_index.
# Indexes built on existing rows find them and the rows inserted later.
@GATE
async def test_query_data_created_before_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    insert_users(cql, "ks", USERS[:2])
    for column in ["gender", "state", "birth_year"]:
        create_and_build_index(cql, servers, "ks", "users", column, f"{column}_key")
    insert_users(cql, "ks", USERS[2:])
    assert_all(cql, "select count(*) from users", [[4]], cl=Q)
    assert_all(cql, "select count(*) from users where state='TX'", [[2]], cl=Q)
    assert_all(cql, "select count(*) from users where state='CA'", [[1]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_query_data_by_pk_and_index.
# Queries by the partition key and an indexed column.
@GATE
async def test_query_data_by_pk_and_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    insert_users(cql, "ks", USERS)
    create_and_build_index(cql, servers, "ks", "users", "gender", "gender_key")
    assert_all(cql, "select count(*) from users", [[4]], cl=Q)
    assert_all(cql, "select count(*) from users where gender='f'", [[2]], cl=Q)
    assert_all(cql, "select KEY, password, gender, state, birth_year from users where KEY='user2' and gender='m'",
               [["user2", "ch@ngem3b", "m", "CA", 1971]], cl=ALL)
    assert_all(cql, "select count(*) from users where KEY='user1' and gender='m'", [[0]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_query_data_by_ck_and_index.
# Queries by the partition and clustering keys and an indexed column.
@GATE
async def test_query_data_by_ck_and_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    create_cf(cql, "ks.cf", "text")
    for u in USERS:
        cql.execute(f"INSERT INTO cf (key, c, v) VALUES ('{u[0]}', '{u[1]}', '{u[2]}')")
    create_and_build_index(cql, servers, "ks", "cf", "v", "v_inx")
    assert_all(cql, "select count(*) from cf", [[4]], cl=Q)
    assert_all(cql, "select count(*) from cf where v='f'", [[2]], cl=Q)
    assert_all(cql, "select count(*) from cf where key='user2' and c='ch@ngem3b' and v='m'", [[1]], cl=ALL)
    assert_all(cql, "select count(*) from cf where KEY='user1' and c='ch@ngem3a' and v='m'", [[0]], cl=Q)


def config_keyspace(cql, servers, ks, table, index_column, index_name, ks_create=True):
    """TestSecondaryIndexes.config_keyspace(): (optionally an RF=1 keyspace,)
    a (key text, col1 text) table and a global index on it."""
    if ks_create:
        create_ks(cql, ks, 1)
    create_cf(cql, f"{ks}.{table}", "text", {"col1": "text"})
    create_and_build_index(cql, servers, ks, table, index_column, index_name)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_low_cardinality_indexes.
# An index whose value matches three rows, read with and without LIMIT.
@GATE
async def test_low_cardinality_indexes(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    config_keyspace(cql, servers, "ks", "cf", "col1", "col1_index", ks_create=False)
    num_rows = 100
    for i in range(num_rows):
        cql.execute(f"INSERT INTO ks.cf (key, col1) VALUES ('{i}', '{i % (num_rows // 3)}');")
    for limit in ["", " LIMIT 100", " LIMIT 3"]:
        assert_all(cql, f"SELECT count(*) FROM ks.cf WHERE col1='1'{limit}", [[3]], cl=Q, num_attempts=20)
    for limit in (1, 2):
        assert_row_count_in_select(cql, f"select * from ks.cf WHERE col1='1' LIMIT {limit}", limit, cl=Q, num_attempts=20)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_insert_data_after_recreating_ks.
# Ten times, the keyspace with an indexed table is dropped and created again
# (RF=1) and rows written right after it are all found through the index.
@GATE
async def test_insert_data_after_recreating_ks(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    config_keyspace(cql, servers, "ks", "cf", "col1", "col1_key", ks_create=False)
    for i in range(10):
        logger.debug(f"round {i}")
        cql.execute("DROP KEYSPACE ks")
        config_keyspace(cql, servers, "ks", "cf", "col1", "col1_key")
        for r in range(10):
            cql.execute(f"INSERT INTO ks.cf (key, col1) VALUES ('{r}','asdf');")
        wait_for_schema_agreement(cql)
        assert_all(cql, "select count(*) from ks.cf WHERE col1='asdf'", [[10]], cl=Q, num_attempts=60, sleep_time=1)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_insert_data_after_recreating_cf.
# Ten times, the indexed table is dropped and created again and rows written
# right after it are all found through the index.
@GATE
async def test_insert_data_after_recreating_cf(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    config_keyspace(cql, servers, "ks", "cf", "col1", "col1_key", ks_create=False)
    for r in range(10):
        cql.execute(f"INSERT INTO ks.cf (key, col1) VALUES ('{r}','asdf');")
    for i in range(10):
        logger.debug(f"round {i}")
        deadline = time.time() + 120
        while True:
            try:
                cql.execute("DROP COLUMNFAMILY ks.cf")
                break
            except OperationTimedOut:
                assert time.time() < deadline
            except InvalidRequest:
                break
        wait_for_schema_agreement(cql)
        config_keyspace(cql, servers, "ks", "cf", "col1", "col1_key", ks_create=False)
        for r in range(10):
            cql.execute(f"INSERT INTO ks.cf (key, col1) VALUES ('{r}','asdf');")
        wait_for_schema_agreement(cql)
        assert_all(cql, "select count(*) from ks.cf WHERE col1='asdf'", [[10]], cl=Q, num_attempts=60, sleep_time=1)


def execute_and_assert(manager, servers, operation, cql_string, table, column, cql, value_length, expect_message, local):
    """The dtests' execute_and_assert(): the statement succeeds and the value
    is read back whole (through the local index when local), or it fails
    with expect_message (in a node's log, for a WriteFailure)."""
    try:
        operation()
        if local:
            stmt = f"select {column} from {table} where a=0 and {column}='{'X' * value_length}'"
        else:
            stmt = f"select {column} from {table}"
        result = list(cql.execute(stmt))
        res_length = len(next(iter(result[0]))) if result else 0
        if value_length == OVERSIZE_LENGTH:
            assert False, f"Expecting query {cql_string} to be invalid"
        assert value_length == res_length, f"Expecting value length is {value_length}, received {res_length}. Result: {result}"
    except AssertionError:
        raise
    except WriteFailure:
        if not expect_message:
            raise
        return "write_failure"
    except Exception as e:  # noqa: BLE001
        if not expect_message or not re.search(expect_message, str(e)):
            raise
    return None


async def insert_row_with_long_value(manager, servers, cql, create_table_cql, create_index_cql, insert_cql, column, value_length,
                                     expect_message, local):
    """The dtests' insert_row_with_long_value(): a new table and its index;
    the insert of the value alone, then in a batch."""
    table = "table_" + str(round(time.time() * 1000, None))
    cql.execute(create_table_cql % (table, {"class": COMPACTION}))
    cql.execute(create_index_cql % table)
    value = "X" * value_length
    insert = insert_cql % table
    prepared = cql.prepare(insert)
    batch = BatchStatement()
    batch.add(prepared, [value])
    for op in [lambda: cql.execute(prepared, [value]), lambda: cql.execute(batch)]:
        if execute_and_assert(manager, servers, op, insert, table, column, cql, value_length, expect_message, local) == "write_failure":
            found = False
            for s in servers:
                log = await manager.server_open_log(s.server_id)
                if await log.grep(expect_message):
                    found = True
                    break
            assert found, f'Expected that failure reason is "{expect_message}", but the message wasn\'t found in the log'


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_long_indexed_values.
# 8193-byte values of globally indexed regular, clustering, partition key,
# compact storage and static columns are written, alone and in a batch, and
# read back.
@GATE
async def test_long_indexed_values(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, config=COMPACT_STORAGE)
    for create_table, create_index_cql, insert, column in [
            ("CREATE TABLE %s(a int, b int, c varchar, PRIMARY KEY (a)) WITH compaction = %s", "CREATE INDEX ON %s(c)",
             "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", "c"),
            ("CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b)) WITH compaction = %s", "CREATE INDEX ON %s(b)",
             "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)", "b"),
            ("CREATE TABLE %s(a text, b int, c int, PRIMARY KEY ((a, b))) WITH compaction = %s", "CREATE INDEX ON %s(a)",
             "INSERT INTO %s (a, b, c) VALUES (?, 0, 0)", "a"),
            ("CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE and compaction = %s", "CREATE INDEX ON %s(b)",
             "INSERT INTO %s (a, b) VALUES (0, ?)", "b"),
            ("CREATE TABLE %s(a int, b int, c text static, PRIMARY KEY (a, b)) WITH compaction = %s", "CREATE INDEX ON %s(c)",
             "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", "c")]:
        await insert_row_with_long_value(manager, servers, cql, create_table, create_index_cql, insert, column,
                                         LONG_TEXT_LENGTH, None, local=False)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_multi_index_filtering_query.
# Restrictions on two indexed columns still need ALLOW FILTERING.
@GATE
async def test_multi_index_filtering_query(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    create_cf(cql, "tbl", "uuid", {"c0": "text", "c1": "text", "c2": "text"})
    for name, column in {"ix_tbl_c0": "c0", "ix_tbl_c1": "c1"}.items():
        create_and_build_index(cql, servers, "ks", "tbl", column, name)
    for c0, c1, c2 in [("a", "b", "c"), ("a", "b", "c"), ("q", "b", "c"), ("a", "e", "f"), ("a", "e", "f")]:
        cql.execute(f"INSERT INTO tbl (key, c0, c1, c2) values (uuid(), '{c0}', '{c1}', '{c2}')")
    assert_all(cql, "SELECT count(*) FROM tbl WHERE c0 = 'a';", [[4]], cl=Q, num_attempts=5, sleep_time=1)
    smt = "SELECT count(*) FROM tbl WHERE c0 = 'a' AND c1 = 'b'"
    assert_invalid(cql, smt, "use ALLOW FILTERING")
    assert_all(cql, f"{smt} ALLOW FILTERING", [[2]], cl=Q, num_attempts=5, sleep_time=1)


def validate_truncated_entries(manager, servers, ks, names):
    """The truncation of the table and of the index views is recorded in
    system.truncated, not in system.local, on every node."""
    for s in servers:
        node_cql = exclusive_session(manager, s)
        try:
            for name, kind in names:
                _id = list(node_cql.execute(f"SELECT id FROM system_schema.{kind}s WHERE keyspace_name='{ks}' and {kind}_name='{name}'"))[0][0]
                truncated = [list(r) for r in node_cql.execute(f"SELECT truncated_at FROM system.truncated WHERE table_uuid={_id}")]
                assert truncated[0], "Expected truncated entry in the system.truncated table, but it's not found"
                assert [list(r) for r in node_cql.execute("SELECT truncated_at FROM system.local")] == [[None]], \
                    "Not expected truncated entry in the system.local table, but it's found"
        finally:
            node_cql.cluster.shutdown()


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_truncate_base.
# Truncating a base table with two global indexes, its data in sstables and
# in the cache, empties the indexes too; the truncations are recorded in
# system.truncated on every node, also after new writes.
@GATE
async def test_truncate_base(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    create_cf(cql, "tbl", "uuid", {"c0": "text", "c1": "text", "c2": "text"})
    index_names = {"ix_tbl_c0": "c0", "ix_tbl_c1": "c1"}
    for name, column in index_names.items():
        create_and_build_index(cql, servers, "ks", "tbl", column, name)

    def create_data():
        for c0, c1 in [("a", "b"), ("a", "b"), ("q", "b"), ("a", "e"), ("a", "e")]:
            cql.execute(f"INSERT INTO tbl (key, c0, c1) values (uuid(), '{c0}', '{c1}')")

    create_data()
    for s in servers:
        await manager.api.flush_all_keyspaces(s.ip_addr)
    assert_all(cql, "SELECT count(*) FROM tbl WHERE c0 = 'a'", [[4]], cl=Q)
    assert_row_count(cql, "tbl", 5)
    cql.execute("TRUNCATE table tbl")
    assert_row_count(cql, "tbl", 0)
    assert_all(cql, "SELECT count(*) FROM tbl WHERE c0 = 'a'", [[0]], cl=Q)
    assert_all(cql, "SELECT count(*) FROM tbl WHERE c1 = 'b'", [[0]], cl=Q)
    names = [("tbl", "table")] + [(f"{name}_index", "view") for name in index_names]
    validate_truncated_entries(manager, servers, "ks", names)
    create_data()
    validate_truncated_entries(manager, servers, "ks", names)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_query_indexes_with_vnodes.
# A single node: an index on a regular and on a compact storage table finds
# the 50 of 100 rows with its value. CASSANDRA-11104.
@GATE
async def test_query_indexes_with_vnodes(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=1, rf=1, config=COMPACT_STORAGE)
    tables = {"compact_table": True, "regular_table": False}
    for table, compact_storage in tables.items():
        create_cf(cql, table, "int", {"b": "int"}, compact_storage=compact_storage)
        create_and_build_index(cql, servers, "ks", table, "b", f"{table}_index")
    insert_args = [(i, i % 2) for i in range(100)]
    for table in tables:
        execute_concurrent_with_args(cql, cql.prepare(f"INSERT INTO ks.{table} (key, b) VALUES (?, ?)"), insert_args)
        res = list(cql.execute(f"SELECT * FROM ks.{table} WHERE b = 0"))
        assert len(res) == 50, f"Expected: 50, got {len(res)}"


def assert_expected_error(fun, expected_error):
    with pytest.raises(Exception) as e:
        fun()
    assert expected_error in str(e.value)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_multi_column_index.
# A single node: a (non-custom) index on two or six columns is refused.
@GATE
async def test_multi_column_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=1, rf=1)
    for table, columns, name in [("cf", ["b", "c"], "two_columns_index"),
                                 ("cf_6columns", ["b", "c", "d", "e", "f", "g"], "six_columns_index")]:
        create_cf(cql, table, "int", {c: "int" for c in columns})
        assert_expected_error(lambda: create_index(cql, table, columns, name, compaction=COMPACTION),
                              "Only CUSTOM indexes support multiple columns")


def prepare_for_ttl(manager, servers, cql, local):
    """The dtests' _prepare_for_ttl(): a row (0, 1, 2) of (key, b, c) and an
    index on b (local, by key, or global) built on it."""
    create_cf(cql, "cf", "int", {"b": "int", "c": "int"})
    cql.execute("INSERT INTO cf (key, b, c) VALUES (0, 1, 2)")
    if local:
        assert_all(cql, "select * from cf where  key = 0", [[0, 1, 2]], cl=ALL)
        create_and_build_index(cql, servers, "ks", "cf", "b", "b_inx", pk_name="key")
        assert_all(cql, "select * from cf where b = 1 and key = 0", [[0, 1, 2]], cl=ALL)
    else:
        assert_all(cql, "select * from cf where key = 0", [[0, 1, 2]], cl=ALL)
        create_and_build_index(cql, servers, "ks", "cf", "b", "b_inx")
        assert_all(cql, "select * from cf where b = 1", [[0, 1, 2]], cl=ALL)


MV_QUERY = "select key, b from b_inx_index"


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_ttl_index_column.
# A single node: when the TTL of an indexed column expires, the row is no
# longer found by the old or the new value, and the index view is empty.
@GATE
async def test_ttl_index_column(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=1, rf=1)
    prepare_for_ttl(manager, servers, cql, local=False)
    ttl = 15
    cql.execute(f"UPDATE cf USING TTL {ttl} SET b=3 WHERE key=0")
    assert_all(cql, "select * from cf where key = 0", [[0, 3, 2]], cl=ALL)
    assert_all(cql, "select * from cf where b = 3", [[0, 3, 2]], cl=ALL)
    assert_none(cql, "select * from cf where b = 1", cl=ALL)
    assert_all(cql, MV_QUERY, [[0, 3]], cl=ALL)
    time.sleep(ttl + 5)
    assert_all(cql, "select * from cf where key = 0", [[0, None, 2]], cl=ALL)
    assert_none(cql, "select * from cf where b = 3", cl=ALL)
    assert_none(cql, "select * from cf where b = 1", cl=ALL)
    assert_none(cql, MV_QUERY, cl=ALL)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_ttl_non_index_column.
# A single node: the TTL of a non-indexed column does not affect the index.
@GATE
async def test_ttl_non_index_column(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=1, rf=1)
    prepare_for_ttl(manager, servers, cql, local=False)
    ttl = 15
    cql.execute(f"UPDATE cf USING TTL {ttl} SET c=3 WHERE key=0")
    assert_all(cql, "select * from cf where key = 0", [[0, 1, 3]], cl=ALL)
    assert_all(cql, "select * from cf where b = 1", [[0, 1, 3]], cl=ALL)
    assert_all(cql, MV_QUERY, [[0, 1]], cl=ALL)
    time.sleep(ttl + 5)
    assert_all(cql, "select * from cf where key = 0", [[0, 1, None]], cl=ALL)
    assert_all(cql, "select * from cf where b = 1", [[0, 1, None]], cl=ALL)
    assert_all(cql, MV_QUERY, [[0, 1]], cl=ALL)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_delete_indexed_rows.
# Deleted rows are gone from the base table, from the queries by a global
# index and from the index view.
@GATE
async def test_delete_indexed_rows(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, session_rack=2)
    create_cf(cql, "cf", "int", {"b": "int"})
    create_and_build_index(cql, servers, "ks", "cf", "b", "b_index")
    num_rows = 100
    for i in range(num_rows):
        cql.execute(f"INSERT INTO ks.cf (key, b) VALUES ({i}, {i + 100})")
    deleted = range(30, 40)
    for i in deleted:
        cql.execute(f"DELETE FROM cf WHERE key = {i}")
    assert_row_count(cql, "cf", num_rows - len(deleted), cl=ALL, num_attempts=60, sleep_time=1)
    for i in range(num_rows):
        if i in deleted:
            assert_none(cql, f"select key, b from cf where key={i}", cl=ALL)
            assert_none(cql, f"select key, b from cf where b={i + 100}", cl=ALL)
        else:
            assert_all(cql, f"select key, b from cf where key={i}", [[i, i + 100]], cl=ALL)
            assert_all(cql, f"select key, b from cf where b={i + 100}", [[i, i + 100]], cl=ALL)
    assert_row_count(cql, "b_index_index", num_rows - len(deleted), cl=ALL)


TUPLE_INDEX_COLUMNS = {"single_tuple": "({0})", "double_tuple": "({0},{0})", "triple_tuple": "({0},{0},{0})",
                       "nested_one": "({0},({0},{0}))"}


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexesOnCollections::test_tuple_indexes.
# A single node: indexes on tuple and nested tuple columns find rows written
# before and after they were built, and follow updates.
@GATE
async def test_tuple_indexes(manager: ScyllaClusterManager):
    ks, table = "tuple_index_test", "simple_with_tuple"
    servers, cql = await prepare_ks(manager, nodes=1, rf=1, keyspace_name=ks)
    create_cf(cql, table, "uuid", {"normal_col": "int", "single_tuple": "tuple<int>", "double_tuple": "tuple<int, int>",
                                   "triple_tuple": "tuple<int, int, int>", "nested_one": "tuple<int, tuple<int, int>>"})
    cmds = [(f"""insert into {table}
                        (key, normal_col, single_tuple, double_tuple, triple_tuple, nested_one)
                    values
                        (uuid(), {n}, ({n}), ({n},{n}), ({n},{n},{n}), ({n},({n},{n})))""", ()) for n in range(50)]
    for success, result in execute_concurrent(cql, cmds * 5, raise_on_first_error=True, concurrency=200):
        assert success, f"didn't get success on insert: {result}"
    assert_invalid(cql, f"SELECT * from {table} where single_tuple = (1)", "use ALLOW FILTERING", expected=Exception)
    for column in TUPLE_INDEX_COLUMNS:
        create_and_build_index(cql, servers, ks, table, column, f"idx_{column}")

    select = "select * from {} where {} = {}"
    for n in range(50):
        for column, template in TUPLE_INDEX_COLUMNS.items():
            assert 5 == len(list(cql.execute(select.format(table, column, template.format(n)))))
            assert 0 == len(list(cql.execute(select.format(table, column, template.format(-1)))))

    for success, result in execute_concurrent(cql, cmds * 3, raise_on_first_error=True, concurrency=200):
        assert success, f"didn't get success on insert: {result}"
    time.sleep(5)

    def validate(expected_rows, value):
        for column, template in TUPLE_INDEX_COLUMNS.items():
            assert expected_rows == len(list(cql.execute(select.format(table, column, template.format(value)))))

    for n in range(50):
        validate(8, n)
    for n in range(5):
        for column, template in TUPLE_INDEX_COLUMNS.items():
            for row in cql.execute(select.format(table, column, template.format(n))):
                cql.execute(f"update {table} set {column} = {template.format(-999)} where key = {row.key}")
    for n in range(5):
        validate(0, n)
    for n in range(50):
        validate(40, -999)


async def frozen_collection_indexes_run(manager, coltype):
    """The dtests' frozen_collection_indexes_run(): neither a global nor a
    local index can be created on the values of a frozen collection."""
    servers, cql = await prepare_ks(manager, nodes=1, rf=1, keyspace_name="index_search")
    create_cf(cql, "users", "uuid", {"email": "text", "uuids": coltype})
    for pk_name in [None, "key"]:
        with pytest.raises(InvalidRequest) as err:
            create_index(cql, "users", "uuids", "user_uuids", pk_name=pk_name, compaction=COMPACTION)
        assert re.search(r".*Cannot create index on (index_values|value) of frozen.*", str(err)), "Not expected error"


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexesOnCollections::test_frozen_list_indexes.
@GATE
async def test_frozen_list_indexes(manager: ScyllaClusterManager):
    await frozen_collection_indexes_run(manager, "frozen<list<uuid>>")


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexesOnCollections::test_frozen_set_indexes.
@GATE
async def test_frozen_set_indexes(manager: ScyllaClusterManager):
    await frozen_collection_indexes_run(manager, "frozen<set<uuid>>")


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexesOnCollections::test_frozen_map_indexes.
@GATE
async def test_frozen_map_indexes(manager: ScyllaClusterManager):
    await frozen_collection_indexes_run(manager, "frozen<map<uuid, uuid>>")


def local_config_keyspace(cql, servers, ks, table, index_column, index_name, pk_name, ks_create=True, global_index_name=None):
    """TestLocalIndexes.config_keyspace(): (optionally an RF=1 keyspace,) a
    (key, c, v) table with create_cf()'s default compaction, a local index,
    and optionally a global index on the same column."""
    if ks_create:
        create_ks(cql, ks, 1)
    cql.execute(f"USE {ks}")
    create_cf(cql, f"{ks}.{table}", "text", compaction=None)
    create_and_build_index(cql, servers, ks, table, index_column, index_name, pk_name=pk_name, compaction=None)
    if global_index_name:
        create_and_build_index(cql, servers, ks, table, index_column, global_index_name, compaction=None)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_simple_local_index.
# Rows are found by their partition key and a local index.
@GATE
async def test_simple_local_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    local_config_keyspace(cql, servers, "ks", "cf", "v", "v_local_key", "key", ks_create=False)
    data = {
        "Tel Aviv": [{"c": generate_random_text(), "v": "Dizzengof"}, {"c": generate_random_text(), "v": "Arlozorov"}],
        "Washington": [{"c": generate_random_text(), "v": "Southgate"}, {"c": generate_random_text(), "v": "11th"}, {"c": generate_random_text(), "v": "10th"}],
        "London": [{"c": generate_random_text(), "v": "Geneva"}, {"c": generate_random_text(), "v": "Moorland"}],
        "Vancouver": [{"c": generate_random_text(), "v": "12th Ave"}, {"c": generate_random_text(), "v": "16th Ave"}],
    }
    for key, rows_data in data.items():
        for d in rows_data:
            cql.execute(f"INSERT INTO cf (key, c, v) VALUES ('{key}', '{d['c']}', '{d['v']}')")
    for key, rows_data in data.items():
        for d in rows_data:
            assert_all(cql, f"SELECT key, c, v FROM cf WHERE key='{key}' AND v='{d['v']}'", [[key, d["c"], d["v"]]], cl=Q, num_attempts=30)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_global_local_index_on_same_column.
# A local and a global index on the same column: rows are found by the
# partition key and the local index, and by the global index alone.
@GATE
async def test_global_local_index_on_same_column(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    local_config_keyspace(cql, servers, "ks", "cf", "v", "v_local_key", "key", ks_create=False, global_index_name="v_global_key")
    local_data = {
        "Tel Aviv": [{"c": generate_random_text(), "v": "Dizzengof"}, {"c": generate_random_text(), "v": "Geneva"}],
        "Washington": [{"c": generate_random_text(), "v": "Southgate"}, {"c": generate_random_text(), "v": "Dizzengof"}, {"c": generate_random_text(), "v": "10th"}],
        "London": [{"c": generate_random_text(), "v": "Geneva"}, {"c": generate_random_text(), "v": "Moorland"}],
        "Vancouver": [{"c": generate_random_text(), "v": "12th Ave"}, {"c": generate_random_text(), "v": "Geneva"}],
    }
    global_data = defaultdict(list)
    for key, rows_data in local_data.items():
        for d in rows_data:
            global_data[d["v"]].append([key, d["c"]])
    for key, rows_data in local_data.items():
        for d in rows_data:
            cql.execute(f"INSERT INTO cf (key, c, v) VALUES ('{key}', '{d['c']}', '{d['v']}')")
    for key, rows_data in local_data.items():
        for d in rows_data:
            assert_all(cql, f"SELECT key, c, v FROM cf WHERE key='{key}' AND v='{d['v']}'", [[key, d["c"], d["v"]]],
                       cl=Q, ignore_order=True, num_attempts=30)
    for value, expected in global_data.items():
        assert_all(cql, f"SELECT key, c FROM cf WHERE v='{value}'", expected, cl=Q, ignore_order=True, num_attempts=30)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_query_data_created_before_local_index.
# Local indexes built on existing rows find them, by partition key, and the
# rows inserted later.
@pytest.mark.tier3
@GATE
async def test_query_data_created_before_local_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, user_table=True)
    insert_users(cql, "ks", USERS[:2])
    for column in ["gender", "state", "birth_year"]:
        create_and_build_index(cql, servers, "ks", "users", column, f"{column}_key", pk_name="key")
    insert_users(cql, "ks", USERS[2:])
    assert_all(cql, "select count(*) from users", [[4]], cl=Q)
    assert_all(cql, "select count(*) from users where key='user4' and state='TX'", [[1]], cl=Q)
    assert_all(cql, "select count(*) from users where key='user2' and state='CA'", [[1]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_query_data_by_ck_and_local_index.
# Queries by the partition and clustering keys and a local index.
@GATE
async def test_query_data_by_ck_and_local_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    create_cf(cql, "ks.cf", "text")
    for u in USERS:
        cql.execute(f"INSERT INTO cf (key, c, v) VALUES ('{u[0]}', '{u[1]}', '{u[2]}')")
    create_and_build_index(cql, servers, "ks", "cf", "v", "v_inx", pk_name="key")
    assert_all(cql, "select count(*) from cf", [[4]], cl=Q)
    assert_all(cql, "select count(*) from cf where key='user2' and c='ch@ngem3b' and v='m'", [[1]], cl=ALL)
    assert_all(cql, "select count(*) from cf where KEY='user1' and c='ch@ngem3a' and v='m'", [[0]], cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_insert_data_after_recreating_ks_with_local_index.
# Ten times, the keyspace with a locally indexed table is dropped and created
# again (RF=1) and rows written right after it are all found through the
# local index.
@GATE
async def test_insert_data_after_recreating_ks_with_local_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    local_config_keyspace(cql, servers, "ks", "cf", "v", "v_ind", "key", ks_create=False)
    for i in range(10):
        logger.debug(f"round {i}")
        cql.execute("DROP KEYSPACE ks")
        local_config_keyspace(cql, servers, "ks", "cf", "v", "v_ind", "key")
        for r in range(10):
            cql.execute(f"INSERT INTO ks.cf (key, c, v) VALUES ('{r}', '{generate_random_text()}','asdf')")
        wait_for_schema_agreement(cql)
        for r in range(10):
            assert_all(cql, f"select count(*) from ks.cf WHERE key='{r}' and v='asdf'", [[1]], cl=Q, num_attempts=60, sleep_time=1)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_local_indexed_values.
# Values of locally indexed regular, clustering and compact storage columns:
# longer than 64 KiB, they are refused, alone and in a batch; 8193 bytes
# long, they are written and found through the local index.
@pytest.mark.tier3
@GATE
@pytest.mark.parametrize("value_length,expect_message", [(OVERSIZE_LENGTH, OVERSIZE_MESSAGE), (LONG_TEXT_LENGTH, None)],
                         ids=["oversize", "long"])
async def test_local_indexed_values(manager: ScyllaClusterManager, value_length, expect_message):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, config=COMPACT_STORAGE)
    for create_table, create_index_cql, insert, column in [
            ("CREATE TABLE %s(a int, b int, c varchar, PRIMARY KEY (a)) WITH compaction = %s", "CREATE INDEX ON %s ((a), c)",
             "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", "c"),
            ("CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b)) WITH compaction = %s", "CREATE INDEX ON %s ((a), b)",
             "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)", "b"),
            ("CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE and compaction = %s", "CREATE INDEX ON %s ((a), b)",
             "INSERT INTO %s (a, b) VALUES (0, ?)", "b")]:
        await insert_row_with_long_value(manager, servers, cql, create_table, create_index_cql, insert, column,
                                         value_length, expect_message, local=True)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_truncate_base_with_local_index.
# Truncating a base table with a local index, its data in sstables and in
# the cache, empties the index too.
@GATE
async def test_truncate_base_with_local_index(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    create_cf(cql, "tbl", "int", {"c0": "text", "c1": "text", "c2": "text"})
    create_and_build_index(cql, servers, "ks", "tbl", "c0", "ix_tbl_c0", pk_name="key")
    data_set = [[0, "a", "b"], [1, "a", "b"], [2, "q", "b"], [3, "a", "e"], [4, "a", "e"]]
    for pk, c0, c1 in data_set:
        cql.execute(f"INSERT INTO tbl (key, c0, c1) values ({pk}, '{c0}', '{c1}')")

    def select_by_index(expected_count):
        for pk, c0, _ in data_set:
            assert_all(cql, f"SELECT count(*) FROM tbl WHERE c0 = '{c0}' and key = {pk}", [[expected_count]], cl=Q)

    for s in servers:
        await manager.api.flush_all_keyspaces(s.ip_addr)
    select_by_index(1)
    assert_row_count(cql, "tbl", 5)
    cql.execute("TRUNCATE table tbl")
    assert_row_count(cql, "tbl", 0)
    select_by_index(0)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_ttl_local_index_column.
# A single node: when the TTL of a locally indexed column expires, the row
# is no longer found through the local index, and the index view is empty.
@pytest.mark.tier3
@GATE
async def test_ttl_local_index_column(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=1, rf=1)
    prepare_for_ttl(manager, servers, cql, local=True)
    ttl = 15
    cql.execute(f"UPDATE cf USING TTL {ttl} SET b=3 WHERE key=0")
    assert_all(cql, "select * from cf where b = 3 and key = 0", [[0, 3, 2]], cl=ALL)
    assert_none(cql, "select * from cf where b = 1 and key = 0", cl=ALL)
    assert_all(cql, MV_QUERY, [[0, 3]], cl=ALL)
    time.sleep(ttl + 5)
    assert_all(cql, "select * from cf where  key = 0", [[0, None, 2]], cl=ALL)
    assert_none(cql, "select * from cf where b = 3 and key = 0", cl=ALL)
    assert_none(cql, "select * from cf where b = 1 and key = 0", cl=ALL)
    assert_none(cql, MV_QUERY, cl=ALL)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_delete_local_indexed_rows.
# Deleted rows are gone from the base table, from the queries by partition
# key and a local index, and from the index view.
@pytest.mark.tier3
@GATE
async def test_delete_local_indexed_rows(manager: ScyllaClusterManager):
    servers, cql = await prepare_ks(manager, nodes=4, rf=3, session_rack=2)
    create_cf(cql, "cf", "int", {"b": "int"})
    create_and_build_index(cql, servers, "ks", "cf", "b", "b_index", pk_name="key")
    num_rows = 100
    for i in range(num_rows):
        cql.execute(f"INSERT INTO ks.cf (key, b) VALUES ({i}, {i + 100})")
    deleted = range(30, 40)
    for i in deleted:
        cql.execute(f"DELETE FROM cf WHERE key = {i}")
    assert_row_count(cql, "cf", num_rows - len(deleted), cl=ALL, num_attempts=60, sleep_time=1)
    for i in range(num_rows):
        query = f"select key, b from cf where key={i} and b={i + 100}"
        if i in deleted:
            assert_none(cql, query, cl=ALL)
        else:
            assert_all(cql, query, [[i, i + 100]], cl=ALL)
    assert_row_count(cql, "b_index_index", num_rows - len(deleted), cl=ALL)


MULTI_INDEX_RECS = [["AAA1", "AAAA", 0], ["AAA2", "AAAA", 100], ["AAA3", "XXXX", 0], ["AAA4", "XXXX", 100],
                    ["AAA5", "AAAA", 0], ["AAA6", "AAAA", 100], ["AAA7", "XXXX", 0], ["AAA8", "XXXX", 100]]


async def prepare_for_multi_index_test(manager):
    """The dtests' _prepare_for_multi_index_test(): a table with two
    indexes, whose views keep the default compaction, and eight rows."""
    servers, cql = await prepare_ks(manager, nodes=4, rf=3)
    cql.execute("CREATE TABLE test_table (row varchar PRIMARY KEY, name varchar, value int);")
    create_and_build_index(cql, servers, "ks", "test_table", "name", "name_idx", compaction=None)
    create_and_build_index(cql, servers, "ks", "test_table", "value", "value_idx", compaction=None)
    insert = cql.prepare("INSERT INTO test_table (row, name, value) VALUES (?, ?, ?)")
    for rec in MULTI_INDEX_RECS:
        cql.execute(insert, rec)
    return cql


def matching(name=None, value=None):
    return [r for r in MULTI_INDEX_RECS if name in (None, r[1]) and value in (None, r[2])]


# Ported from scylla-dtest secondary_indexes_test.py::TestMultipleSecondaryIndexes::test_multy_secondary_query_with_no_pk.
# Queries on one of two indexed columns; on both, they need ALLOW FILTERING.
@GATE
async def test_multy_secondary_query_with_no_pk(manager: ScyllaClusterManager):
    cql = await prepare_for_multi_index_test(manager)
    for name in ["AAAA", "XXXX"]:
        assert_all(cql, f"SELECT * FROM test_table WHERE name='{name}'", matching(name=name), ignore_order=True, cl=Q)
    for value in [0, 100]:
        assert_all(cql, f"SELECT * FROM test_table WHERE value={value}", matching(value=value), ignore_order=True, cl=Q)
    for name in ["AAAA", "XXXX"]:
        for value in [0, 100]:
            where = f"name='{name}' and value={value}"
            assert_invalid(cql, f"SELECT * FROM test_table WHERE {where}", USE_FILTERING)
            assert_all(cql, f"SELECT * FROM test_table WHERE {where} ALLOW FILTERING", matching(name, value), ignore_order=True, cl=Q)


# Ported from scylla-dtest secondary_indexes_test.py::TestMultipleSecondaryIndexes::test_multy_secondary_query_with_pk.
# Queries with the partition key and one or two indexed columns.
@GATE
async def test_multy_secondary_query_with_pk(manager: ScyllaClusterManager):
    cql = await prepare_for_multi_index_test(manager)
    assert_invalid(cql, "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA' and value=0", USE_FILTERING)
    for where in ["row='AAA1' and name='AAAA' and value=0 ALLOW FILTERING", "row='AAA1' and name='AAAA'",
                  "row='AAA1' and value=0", "row='AAA1'"]:
        assert_all(cql, f"SELECT * FROM test_table WHERE {where}", [["AAA1", "AAAA", 0]], ignore_order=True, cl=Q)


async def node_action_after_index_build(manager: ScyllaClusterManager, node_action, nodes, local):
    """The dtests' _node_action_after_index_build(): 1000 rows (key, b = key + 1000)
    written at QUORUM, a (global or local) index on b built, then the last node
    of rack1 stopped, removed, decommissioned, or a new node added to rack1;
    every row is still found by the index at QUORUM, and the index view has
    all the rows."""
    num_rows = 1000
    servers, cql = await prepare(manager, session_rack=2, nodes=nodes)
    # The last node of rack1 (prepare() puts rack1's nodes first).
    node2 = servers[nodes // 3 + (1 if nodes % 3 else 0) - 1]
    async with new_test_keyspace(manager, RF3) as ks:
        create_cf(cql, f"{ks}.cf", "int", {"b": "int"})
        statement = cql.prepare(f"INSERT INTO {ks}.cf (key, b) VALUES (?, ?)")
        statement.consistency_level = Q
        execute_concurrent_with_args(cql, statement, [[k, k + num_rows] for k in range(num_rows)])
        create_and_build_index(cql, servers, ks, "cf", "b", "b_index", pk_name="key" if local else None)

        if node_action == "stop":
            await manager.server_stop_gracefully(node2.server_id)
        elif node_action == "remove":
            await manager.server_stop(node2.server_id, convict=False)
            first = next(s for s in servers if s.server_id != node2.server_id)
            await manager.remove_node(first.server_id, node2.server_id)
        elif node_action == "decommission":
            await manager.decommission_node(node2.server_id)
        else:
            await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"}, property_file={"dc": "dc1", "rack": "rack1"})

        stmt = ("SELECT key FROM {ks}.cf WHERE key = {key} AND b = {value}" if local
                else "SELECT key FROM {ks}.cf WHERE b = {value}")
        for attempt in range(60):
            try:
                for i in range(num_rows):
                    assert_all(cql, stmt.format(ks=ks, key=i, value=i + num_rows), [[i]], cl=Q)
                break
            except Exception:
                if attempt == 59:
                    raise
                time.sleep(1)
        assert len(rows(cql, f"SELECT * FROM {ks}.b_index_index", cl=Q)) == num_rows


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_stop_node_after_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_stop_node_after_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "stop", nodes=4, local=False)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_remove_node_after_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_remove_node_after_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "remove", nodes=4, local=False)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_decommission_node_after_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_decommission_node_after_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "decommission", nodes=4, local=False)


# Ported from scylla-dtest secondary_indexes_test.py::TestSecondaryIndexes::test_add_node_after_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_add_node_after_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "add", nodes=3, local=False)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_stop_node_after_local_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_stop_node_after_local_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "stop", nodes=4, local=True)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_remove_node_after_local_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_remove_node_after_local_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "remove", nodes=4, local=True)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_decommission_node_after_local_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_decommission_node_after_local_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "decommission", nodes=4, local=True)


# Ported from scylla-dtest secondary_indexes_test.py::TestLocalIndexes::test_add_node_after_local_index_build.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_add_node_after_local_index_build(manager: ScyllaClusterManager):
    await node_action_after_index_build(manager, "add", nodes=3, local=True)
