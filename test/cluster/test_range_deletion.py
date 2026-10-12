#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Range deletions by clustering key ranges, ported from scylla-dtest
deletion_test.py (TestRangeDeletion), one test per dtest. As in the dtests,
every test runs for each of the three compaction strategies of its table,
the nodes are in datacenter dc1 with as many racks as the replication factor
of the keyspace "ks", and the keyspaces use vnodes. "cluster.flush()" is
"nodetool flush" on each running node; statements without a consistency
level run at ONE, as in the dtests' sessions.
"""
import asyncio
import logging
from datetime import datetime, timedelta

import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement

from test.cluster.dtest_helpers import cql_one, exclusive_one, create_ks, flush_all, nodetool, topology
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}

# The dtests' autouse fixture_compaction_strategy.
compaction_strategies = pytest.mark.parametrize(
    "compaction_strategy", ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy"])


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def rack_topology_based_rf(nodes: int, rf: int) -> dict[str, int]:
    """The dtests' generate_rack_topology_based_rf(): `rf` racks, the nodes
    spread over them."""
    racks_num = rf if rf > 0 else nodes
    return {f"rack{i + 1}": nodes // racks_num + (1 if i < nodes % racks_num else 0) for i in range(racks_num)}


async def prepare(manager: ScyllaClusterManager, nodes: int | dict[str, int] = 1, rf: int = 1):
    """The dtests' prepare(): the nodes in dc1, and the keyspace "ks" with
    the given replication factor; returns the servers and a session."""
    racks = nodes if isinstance(nodes, dict) else rack_topology_based_rf(nodes, rf)
    servers = await manager.servers_add(sum(racks.values()), config=VNODES, property_file=topology(racks, dc="dc1"))
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    return servers, cql


async def cluster_flush(manager: ScyllaClusterManager):
    """ccm's cluster.flush(): nodetool flush on each running node."""
    await flush_all(manager, await manager.running_servers())


def create_cf_1pk_1ck(cql, compaction_strategy):
    cql.execute(f"CREATE TABLE ks.test1 (pk int, ck date, v1 int, PRIMARY KEY(pk, ck)) WITH compaction = {{'class': '{compaction_strategy}' }}")


def create_cf_2ck_int(cql, compaction_strategy):
    cql.execute(f"CREATE TABLE ks.test1 (pk1 int, ck1 int, ck2 int, v1 int, PRIMARY KEY(pk1, ck1, ck2)) WITH compaction = {{'class': '{compaction_strategy}' }}")


def create_cf_2ck(cql, compaction_strategy):
    cql.execute(f"CREATE TABLE ks.test1 (pk1 int, ck1 int, ck2 varchar, v1 int, PRIMARY KEY(pk1, ck1, ck2)) WITH compaction = {{'class': '{compaction_strategy}' }}")


def insert_data_cf_1pk_1ck(cql, rows_in_pk):
    """Partitions 1 and 2, each with rows_in_pk consecutive days from today
    as clustering keys, written at QUORUM."""
    current_date = datetime.now()
    data = [[1, (current_date + timedelta(days=i)).strftime("%Y-%m-%d"), i] for i in range(rows_in_pk)]
    data.extend([2, (current_date + timedelta(days=i)).strftime("%Y-%m-%d"), i] for i in range(rows_in_pk))
    for pk, ck, v1 in data:
        cql.execute(SimpleStatement(f"INSERT INTO ks.test1 (pk, ck, v1) VALUES ({pk}, '{ck}', {v1})", consistency_level=ConsistencyLevel.QUORUM))
    return data


async def insert_data_cf_2ck_matrix(manager: ScyllaClusterManager, cql, num_of_pks, rows_per_ck):
    """Rows (pk1, ck1, ck2, v1 = ck2) of rows_per_ck x rows_per_ck clustering
    keys per partition; the cluster is flushed after each partition."""
    data = [[pkey, ckey1, ckey2, ckey2] for pkey in range(num_of_pks) for ckey1 in range(rows_per_ck) for ckey2 in range(rows_per_ck)]
    rows_in_pk = rows_per_ck**2
    for start_row in range(0, len(data), rows_in_pk):
        for pk1, ck1, ck2, v1 in data[start_row:start_row + rows_in_pk]:
            cql.execute(f"INSERT INTO ks.test1 (pk1, ck1, ck2, v1) VALUES ({pk1}, {ck1}, {ck2}, {v1})")
        await cluster_flush(manager)
    return data


def insert_data_cf_2ck(cql, rows_in_pk):
    """Partitions 0 and 1, with rows (p, p, 'ck<k>', k), written at QUORUM."""
    data = [[p, p, f"ck{k}", k] for p in [0, 1] for k in range(rows_in_pk)]
    for pk1, ck1, ck2, v1 in data:
        cql.execute(SimpleStatement(f"INSERT INTO ks.test1 (pk1, ck1, ck2, v1) VALUES ({pk1}, {ck1}, '{ck2}', {v1})", consistency_level=ConsistencyLevel.QUORUM))
    return data


def assert_all(cql, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False):
    rows = [list(r) for r in cql.execute(SimpleStatement(query, consistency_level=cl))]
    if ignore_order:
        rows, expected = sorted(rows), sorted(expected)
    assert rows == expected, f"Expected {expected} from {query}, but got {rows}"


def assert_one(cql, query, expected):
    rows = [list(r) for r in cql.execute(query)]
    assert rows == [expected], f"Expected {[expected]} from {query}, but got {rows}"


def assert_invalid(cql, query, matching):
    with pytest.raises(InvalidRequest, match=matching):
        cql.execute(query)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_2ck_range_in.
# A deletion with IN on both clustering columns deletes exactly the listed rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_2ck_range_in(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_2ck(cql, compaction_strategy)
    data = insert_data_cf_2ck(cql, rows_in_pk=10)

    select_query = "SELECT * FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
    await cluster_flush(manager)

    range_indexes = [4, 6, 8]  # these rows are deleted
    pk_index = range_indexes[0]
    ck1 = ", ".join(str(data[i][1]) for i in range_indexes)
    ck2 = ", ".join(f"'{data[i][2]}'" for i in range_indexes)
    cql.execute(f"DELETE FROM ks.test1 WHERE pk1={data[pk_index][0]} and ck1 in ({ck1}) and ck2 in ({ck2})")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [17])
    for i in sorted(range_indexes, reverse=True):
        del data[i]
    assert_all(cql, select_query, data, cl=ConsistencyLevel.ALL, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_multiple_ranges_by_2ck.
# Overlapping range tombstones, written while the second replica is down and
# flushed into several sstables, are repaired to it: then it alone returns
# the remaining rows, and so does the first node alone after a major
# compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_multiple_ranges_by_2ck(manager: ScyllaClusterManager, compaction_strategy):
    (node1, node2), cql = await prepare(manager, nodes=2, rf=2)
    await nodetool(manager, node1, "disableautocompaction")
    create_cf_2ck_int(cql, compaction_strategy)
    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)
    num_of_pks = 2
    rows_per_ck = 4
    total_rows_num = num_of_pks * rows_per_ck**2
    ck_deletion_skip = 2
    data = await insert_data_cf_2ck_matrix(manager, cql, num_of_pks=num_of_pks, rows_per_ck=rows_per_ck)

    assert_all(cql, "SELECT * FROM ks.test1", data, cl=ConsistencyLevel.ONE, ignore_order=True)

    overlap_index = 2  # where the deletion ranges intersect
    deletion_range = 2
    delete_query = "DELETE FROM ks.test1 WHERE pk1={pk1} and ck1 = {ck1} and ck2 >= {ck2_min} and ck2 < {ck2_max}"
    # For each pk and every other ck1: two overlapping ranges of ck2.
    for pkey in range(num_of_pks):
        for ckey1 in range(0, rows_per_ck, ck_deletion_skip):
            cql.execute(delete_query.format(pk1=pkey, ck1=ckey1, ck2_min=overlap_index - deletion_range, ck2_max=overlap_index))
            cql.execute(delete_query.format(pk1=pkey, ck1=ckey1, ck2_min=overlap_index - deletion_range + 1, ck2_max=overlap_index + 1))
        await cluster_flush(manager)

    num_of_deleted_rows = total_rows_num // ck_deletion_skip // (rows_per_ck / (deletion_range + 1))
    total_left_rows = total_rows_num - num_of_deleted_rows
    assert_one(cql, "select count(*) from ks.test1", [total_left_rows])

    # For each pk, half of the rows by ck1, overlapping the ranges above.
    for pkey in range(num_of_pks):
        cql.execute(f"DELETE FROM ks.test1 WHERE pk1={pkey} and ck1 >= {rows_per_ck // 2}")
        await cluster_flush(manager)

    total_left_rows //= 2
    assert_one(cql, "select count(*) from ks.test1", [total_left_rows])

    # All the rows of one partition.
    cql.execute("DELETE FROM ks.test1 WHERE pk1 = 0 and ck1 >= 0")
    await cluster_flush(manager)

    total_left_rows -= total_left_rows / num_of_pks
    assert_one(cql, "select count(*) from ks.test1", [total_left_rows])

    # node2.start() and node2.repair(): "nodetool repair", then "nodetool cluster repair".
    await manager.server_start(node2.server_id)
    await nodetool(manager, node2, "repair")
    await nodetool(manager, node2, "cluster", "repair")
    await manager.server_stop_gracefully(node1.server_id)
    await manager.others_not_see_server(node1.ip_addr)
    session = await exclusive_one(manager, node2)
    assert_one(session, "select count(*) from ks.test1", [total_left_rows])
    await manager.server_start(node1.server_id)
    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)
    await nodetool(manager, node1, "compact")
    session = await exclusive_one(manager, node1)
    assert_one(session, "select count(*) from ks.test1 bypass cache", [total_left_rows])


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_2ck_range_equal_and_not_equal.
# A deletion with "=" on the first clustering column and ">=" on the second.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_2ck_range_equal_and_not_equal(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_2ck(cql, compaction_strategy)
    data = insert_data_cf_2ck(cql, rows_in_pk=10)

    select_query = "SELECT * FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
    await cluster_flush(manager)

    lower_index = 4
    cql.execute(f"DELETE FROM ks.test1 WHERE pk1={data[lower_index][0]} and ck1 = {data[lower_index][1]} and ck2 >= '{data[lower_index][2]}'")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [14])
    assert_all(cql, select_query, data[:lower_index] + data[lower_index + 6:], cl=ConsistencyLevel.ALL, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_2ck_range_one_non_equal.
# A deletion with ">=" on the first clustering column.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_2ck_range_one_non_equal(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_2ck(cql, compaction_strategy)
    data = insert_data_cf_2ck(cql, rows_in_pk=10)

    select_query = "SELECT * FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
    await cluster_flush(manager)

    lower_index = 4
    cql.execute(f"DELETE FROM ks.test1 WHERE pk1={data[lower_index][0]} and ck1 >= {data[lower_index][1]}")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [10])
    assert_all(cql, select_query, data[10:], cl=ConsistencyLevel.ALL, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_update_by_1ck_range.
# An UPDATE of a clustering key range is rejected.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_update_by_1ck_range(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, rows_in_pk=10)

    lower_index = 4
    assert_invalid(cql, f"UPDATE ks.test1 SET v1 = 100 WHERE pk={data[lower_index][0]} and ck < '{data[lower_index][1]}'",
                   matching="Invalid operator in where clause")


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_1ck_range_in.
# A deletion with IN on the partition key and on the clustering key.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_1ck_range_in(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, rows_in_pk=10)
    await cluster_flush(manager)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    first_index = 4
    second_index = 12
    cql.execute(f"DELETE FROM ks.test1 WHERE pk in ({data[first_index][0]}, {data[second_index][0]}) "
                f"and ck in ('{data[first_index][1]}', '{data[second_index][1]}')")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [16])
    for i in [first_index + 10, second_index, first_index, second_index - 10]:
        del data[i]
    assert_all(cql, select_query, data, cl=ConsistencyLevel.ALL, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_1ck_range_less.
# A deletion with "<" on the clustering key.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_1ck_range_less(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, rows_in_pk=10)
    await cluster_flush(manager)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    lower_index = 4
    cql.execute(f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck < '{data[lower_index][1]}'")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [16])
    assert_all(cql, select_query, data[lower_index:], cl=ConsistencyLevel.ALL, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_1ck_range_less_more.
# A deletion with "<" and ">=" on the clustering key.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_1ck_range_less_more(manager: ScyllaClusterManager, compaction_strategy):
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, rows_in_pk=10)
    await cluster_flush(manager)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    lower_index = 4
    upper_index = 6
    cql.execute(f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck < '{data[upper_index][1]}' and ck >= '{data[lower_index][1]}'")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [18])
    assert_all(cql, select_query, data[:lower_index] + data[upper_index:], cl=ConsistencyLevel.ALL, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_when_node_stopped.
# An open range deletion written while a replica is stopped reaches it by
# hinted handoff once it is back: with the other nodes stopped, it alone
# returns the remaining rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_when_node_stopped(manager: ScyllaClusterManager, compaction_strategy):
    servers, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, rows_in_pk=10)
    await cluster_flush(manager)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    node2 = servers[1]
    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)

    lower_index = 16  # the rows after it are deleted
    cql.execute(f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck > '{data[lower_index][1]}'")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [17])
    assert_all(cql, select_query, data[:lower_index + 1], cl=ConsistencyLevel.QUORUM, ignore_order=True)

    await manager.server_start(node2.server_id)
    # Wait for hinted handoff to flush.
    await asyncio.sleep(15)

    for s in servers:
        if s is not node2:
            await manager.server_stop_gracefully(s.server_id)
            await manager.others_not_see_server(s.ip_addr)
    session = await exclusive_one(manager, node2)

    assert_one(session, "select count(*) from ks.test1", [17])
    assert_all(session, select_query, data[:lower_index + 1], cl=ConsistencyLevel.ONE, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_when_decommission_node.
# After a node of three in one rack (RF=3) is decommissioned, an open range
# deletion is written; a new node then bootstraps and, with the old nodes
# stopped, alone returns the remaining rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_when_decommission_node(manager: ScyllaClusterManager, compaction_strategy):
    # The cluster shrinks below the RF, so all the nodes are in one rack.
    servers, cql = await prepare(manager, nodes={"rack1": 3}, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, rows_in_pk=10)
    await cluster_flush(manager)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    # ccm's node2.decommission(): "nodetool decommission"; the process keeps running.
    node2 = servers[1]
    await nodetool(manager, node2, "decommission")

    lower_index = 16  # the rows after it are deleted
    cql.execute(f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck > '{data[lower_index][1]}'")
    await cluster_flush(manager)

    assert_one(cql, "select count(*) from ks.test1", [17])
    assert_all(cql, select_query, data[:lower_index + 1], cl=ConsistencyLevel.QUORUM, ignore_order=True)

    node_new = await manager.server_add(config=VNODES, property_file={"dc": "dc1", "rack": "rack1"})

    for s in servers:
        if s is not node2:
            await manager.server_stop_gracefully(s.server_id)

    session = await exclusive_one(manager, node_new)
    assert_one(session, "select count(*) from ks.test1", [17])
    assert_all(session, select_query, data[:lower_index + 1], cl=ConsistencyLevel.ONE, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_1ck_range_conditional_batch.
# A batch with a clustering range deletion and conditional INSERTs is
# conditional as a whole, and is applied.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_1ck_range_conditional_batch(manager: ScyllaClusterManager, compaction_strategy):
    num_rows = 5
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, num_rows)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    lower_index = 1
    upper_index = 3
    # A range deletion from lower_index (included) to upper_index (excluded),
    query = f""" BEGIN BATCH DELETE FROM ks.test1 where pk={data[0][0]} and ck < '{data[upper_index][1]}' and ck >= '{data[lower_index][1]}';
            """
    # and conditional inserts of new rows in their place.
    current_date = datetime.now()
    for i in range(lower_index, upper_index):
        data[i][1] = (current_date + timedelta(days=i + num_rows)).strftime("%Y-%m-%d")
        data[i][2] = i * num_rows
    for i in range(lower_index, upper_index):
        query += f" INSERT INTO ks.test1 (pk, ck, v1) VALUES ({data[0][0]}, '{data[i][1]}', {data[i][2]}) IF NOT EXISTS;"
    query += "APPLY BATCH;"
    assert_all(cql, query, [[True, None, None, None], [True, None, None, None], [True, None, None, None]], cl=ConsistencyLevel.QUORUM)

    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)


# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_1ck_range_conditional_batch_update.
# A batch with a clustering range deletion and conditional UPDATEs is
# conditional as a whole, and is applied; its result has the old values of
# the updated rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@compaction_strategies
async def test_delete_by_1ck_range_conditional_batch_update(manager: ScyllaClusterManager, compaction_strategy):
    num_rows = 5
    _, cql = await prepare(manager, nodes=3, rf=3)
    create_cf_1pk_1ck(cql, compaction_strategy)
    data = insert_data_cf_1pk_1ck(cql, num_rows)

    select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    lower_index = 1
    upper_index = 3
    # A range deletion from lower_index (included) to upper_index (excluded),
    query = f""" BEGIN BATCH DELETE FROM ks.test1 where pk={data[0][0]} and ck < '{data[upper_index][1]}' and ck >= '{data[lower_index][1]}';
            """
    # and conditional updates of the rows after it.
    for i in range(upper_index, num_rows):
        query += f" UPDATE ks.test1 SET v1 = {data[i][2] * num_rows} WHERE pk = {data[0][0]} and ck = '{data[i][1]}' IF v1 = {data[i][2]};"
    query += "APPLY BATCH;"
    conditional_batch_result = [[True] + [None] * len(data[upper_index])]  # the row of the DELETE
    for row in data[upper_index:num_rows]:
        batch_row = row[:]
        batch_row[1] = datetime.strptime(batch_row[1], "%Y-%m-%d").date()
        batch_row.insert(0, True)
        conditional_batch_result.append(batch_row)
    assert_all(cql, query, conditional_batch_result, cl=ConsistencyLevel.QUORUM)

    for row in data[upper_index:num_rows]:
        row[2] *= num_rows
    del data[lower_index:upper_index]
    assert_all(cql, select_query, data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
