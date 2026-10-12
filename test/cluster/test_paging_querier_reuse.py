#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Paging, ported from scylla-dtest paging_additional_test.py: counts paged
# with a LIMIT or over a clustering slice, large pages, querier reuse between
# pages (checked with the nodes' querier cache metrics), pages of a CL=ONE
# query staying on one replica, and paging at CL=QUORUM. As in the dtests'
# BasePagingTester.prepare(), the cluster has three nodes, in rack1..rack3 of
# datacenter1, on vnodes, and the session to node1 returns rows as dicts at
# CL=QUORUM by default.

import asyncio
import logging
import uuid
from random import randint

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import SimpleStatement, dict_factory

from test import DEBUG_MODES
from test.cluster.dtest_helpers import DTEST_CONFIG, create_ks, topology
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


async def prepare(manager: ScyllaClusterManager, consistency_level=ConsistencyLevel.QUORUM):
    """The dtests' BasePagingTester.prepare()."""
    servers = await manager.servers_add(3, config=DTEST_CONFIG, property_file=topology({"rack1": 1, "rack2": 1, "rack3": 1}))
    session = manager.con_gen([servers[0].ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect()
    profile = session.get_execution_profile(EXEC_PROFILE_DEFAULT)
    profile.row_factory = dict_factory
    profile.consistency_level = consistency_level
    return servers, session


def create_rows(session, table, rows, cl=ConsistencyLevel.ALL):
    """The dtests' create_rows(): the rows (dicts) written concurrently with
    one prepared statement."""
    cols = list(rows[0])
    prepared = session.prepare(f"INSERT INTO {table} ({', '.join(cols)}) values ({', '.join('?' for _ in cols)})")
    prepared.consistency_level = cl
    execute_concurrent_with_args(session, prepared, [list(r.values()) for r in rows])
    return rows


class PageFetcher:
    """The dtests' PageFetcher: all the pages of a query, one by one; empty
    pages are counted as requested, but not kept."""

    def __init__(self, session, statement):
        self.pages = []
        self.requested_pages = 1
        self.result = session.execute(statement)
        self.handle_page()

    def handle_page(self):
        rows = list(self.result.current_rows)
        if rows:
            self.pages.append(rows)

    def request_all(self):
        while self.result.has_more_pages:
            self.result.fetch_next_page()
            self.requested_pages += 1
            self.handle_page()
        return self

    def num_results_all(self):
        return [len(page) for page in self.pages]

    def all_data(self):
        return [row for page in self.pages for row in page]


QUERIER_CACHE_METRICS = ["lookups", "misses", "drops", "time_based_evictions", "resource_based_evictions"]


async def node_metrics(manager: ScyllaClusterManager, server):
    """The dtests' get_node_metrics() of the querier cache metrics: each
    summed over the node's shards."""
    metrics = await manager.metrics.query(server.ip_addr)
    return {m: metrics.get(f"scylla_database_querier_cache_{m}") or 0 for m in QUERIER_CACHE_METRICS}


def metrics_equal(metrics, expected):
    """Whether the node's metrics are as expected: 0 unless given, -1 for any."""
    for m in QUERIER_CACHE_METRICS:
        expected_value = expected.get(m, 0)
        if expected_value != -1 and metrics[m] != expected_value:
            return False
    return True


async def assert_nodes_metrics(manager: ScyllaClusterManager, servers, expected_metrics, verifier=None):
    """The dtests' assert_nodes_metrics(): every node's metrics match one of
    the expected variants, and every variant matches some node."""
    matched = set()
    for server in servers:
        metrics = await node_metrics(manager, server)
        logger.debug(f"{server.ip_addr} metrics: {metrics}")
        matching = {i for i, variant in enumerate(expected_metrics) if metrics_equal(metrics, variant)}
        assert matching, f"Node metrics doesn't match any of the expected metrics: \nnode_metrics: {metrics}\nexpected_metrics: {expected_metrics}"
        matched |= matching
        if verifier is not None:
            verifier(metrics)
    assert len(matched) == len(expected_metrics), f"Expected {len(expected_metrics)}, but got {len(matched)}"


async def paged_count_with_clustering_key(manager: ScyllaClusterManager, order):
    """A count over 1234 rows of one clustering slice, in the given order,
    paged by 100 rows, is one row with the whole count."""
    _, session = await prepare(manager)
    create_ks(session, "test_aggregate_paging", 3)
    table = "test_aggregate_paging.paging_test"
    session.execute(f"CREATE TABLE {table} (pk int, ck1 int, ck2 text, v int, PRIMARY KEY(pk, ck1, ck2))")
    create_rows(session, table, [{"pk": 0, "ck1": 1, "ck2": str(uuid.uuid4()), "v": 0} for _ in range(1234)])

    pf = PageFetcher(session, SimpleStatement(f"select count(*) from {table} where pk = 0 and ck1 = 1 order by ck1 {order}, ck2 {order}",
                                              fetch_size=100, consistency_level=ConsistencyLevel.ALL)).request_all()
    rows_count = pf.num_results_all()
    all_data = pf.all_data()
    assert rows_count == [1], f"Expected 1 row, but got {rows_count}"
    assert all_data == [{"count": 1234}], f'Expected "{{"count": 1234}}", but got {all_data}'


# Ported from scylla-dtest paging_additional_test.py::TestAggregatePaging::test_paged_count_with_limit.
# A count with LIMIT 1 over 5001 rows, with pages smaller and larger than
# the limit, is one row with the whole count (#650).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_paged_count_with_limit(manager: ScyllaClusterManager):
    _, session = await prepare(manager)
    create_ks(session, "test_aggregate_paging", 3)
    table = "test_aggregate_paging.paging_test"
    session.execute(f"CREATE TABLE {table} ( id uuid PRIMARY KEY, value text )")
    create_rows(session, table, [{"id": uuid.uuid4(), "value": "testing"} for _ in range(5001)])

    for page_size in [10, 100, 1000, 3000, 5000]:
        pf = PageFetcher(session, SimpleStatement(f"select count(*) from {table} limit 1", fetch_size=page_size,
                                                  consistency_level=ConsistencyLevel.ALL)).request_all()
        rows_count = pf.num_results_all()
        all_data = pf.all_data()
        assert rows_count == [1], f"Expected 1 row, but got {rows_count}"
        assert all_data == [{"count": 5001}], f'Expected "{{"count": 5001}}", but got {all_data}'


# Ported from scylla-dtest paging_additional_test.py::TestAggregatePaging::test_paged_count_with_clustering_key.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_paged_count_with_clustering_key(manager: ScyllaClusterManager):
    await paged_count_with_clustering_key(manager, "asc")


# Ported from scylla-dtest paging_additional_test.py::TestAggregatePaging::test_paged_count_with_clustering_key_reversed.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_paged_count_with_clustering_key_reversed(manager: ScyllaClusterManager):
    await paged_count_with_clustering_key(manager, "desc")


LARGE_PAGING_TABLE = "test_large_paging.paging_test"


async def prepare_large_paging(manager: ScyllaClusterManager, schema):
    """The dtests' TestLargePaging.prepare_schema() and its table."""
    servers, session = await prepare(manager)
    create_ks(session, "test_large_paging", 3)
    session.execute(f"CREATE TABLE {LARGE_PAGING_TABLE} {schema}")
    return servers, session


async def validate_data(manager: ScyllaClusterManager, servers, session, query, fetch_size, row_cnt, validate_metrics=False):
    """The dtests' TestLargePaging.validate_data(): the query at ALL returns
    row_cnt rows in pages of at most fetch_size rows; optionally every node
    looked up a saved querier for every page but the first, and missed only
    queriers evicted for lack of resources."""
    pf = PageFetcher(session, SimpleStatement(query, fetch_size=fetch_size, consistency_level=ConsistencyLevel.ALL)).request_all()
    all_pages = pf.num_results_all()
    assert sum(all_pages) == row_cnt, f"Expected {row_cnt}, got {sum(all_pages)}"
    for page in all_pages:
        assert page <= fetch_size, f"Fetch size {page} is more then {fetch_size} unexpectedly"

    def verify_misses(metrics):
        assert metrics["misses"] == metrics["resource_based_evictions"], metrics

    if validate_metrics:
        await assert_nodes_metrics(manager, servers, [{"lookups": pf.requested_pages - 1, "misses": -1, "resource_based_evictions": -1}] * len(servers),
                                   verifier=verify_misses)


# Ported from scylla-dtest paging_additional_test.py::TestLargePaging::test_large_page_range_queries.
# 1000 partitions with a 64 KiB value each, scanned in pages of 1000 rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_large_page_range_queries(manager: ScyllaClusterManager):
    servers, session = await prepare_large_paging(manager, "(pk text, ck text, v text, PRIMARY KEY(pk, ck))")
    create_rows(session, LARGE_PAGING_TABLE, [{"pk": str(uuid.uuid4()), "ck": str(uuid.uuid4()), "v": " " * (64 * 1024)} for _ in range(1000)])
    await validate_data(manager, servers, session, f"select * from {LARGE_PAGING_TABLE}", fetch_size=1000, row_cnt=1000)


# Ported from scylla-dtest paging_additional_test.py::TestLargePaging::test_large_page_range_queries_static_columns.
# The same with partitions of only a 64 KiB static cell.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_large_page_range_queries_static_columns(manager: ScyllaClusterManager):
    servers, session = await prepare_large_paging(manager, "(pk text, ck text, s text static, v text, PRIMARY KEY(pk, ck))")
    create_rows(session, LARGE_PAGING_TABLE, [{"pk": str(uuid.uuid4()), "s": " " * (64 * 1024)} for _ in range(1000)])
    await validate_data(manager, servers, session, f"select * from {LARGE_PAGING_TABLE}", fetch_size=1000, row_cnt=1000)


async def single_partition_paging(manager: ScyllaClusterManager, fetch_size):
    servers, session = await prepare_large_paging(manager, "(pk int, ck text, v text, PRIMARY KEY(pk, ck))")
    create_rows(session, LARGE_PAGING_TABLE, [{"pk": 0, "ck": str(uuid.uuid4()), "v": " " * (32 * 1024)} for _ in range(1000)])
    await validate_data(manager, servers, session, f"select * from {LARGE_PAGING_TABLE} where pk = 0", fetch_size=fetch_size, row_cnt=1000,
                        validate_metrics=True)


# Ported from scylla-dtest paging_additional_test.py::TestLargePaging::test_large_page_single_partition.
# One partition of 1000 rows of 32 KiB, in pages of 400 rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_large_page_single_partition(manager: ScyllaClusterManager):
    await single_partition_paging(manager, 400)


# Ported from scylla-dtest paging_additional_test.py::TestLargePaging::test_small_page_single_partition.
# The same in pages of 15 rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_small_page_single_partition(manager: ScyllaClusterManager):
    await single_partition_paging(manager, 15)


SINGULAR_TABLE = "paging_additional_test_querier_reuse.test_singular"


def setup_simple_table(session, **kwargs):
    """The dtests' setup_simple_table(): 4 rows in partition 1 and 6 in
    partition 2, written at ALL."""
    create_ks(session, "paging_additional_test_querier_reuse", 3)
    query = f"CREATE TABLE {SINGULAR_TABLE} (pk int, ck int, val text, PRIMARY KEY (pk, ck))"
    if kwargs:
        options = [f"{key}={val}" if type(val) in (float, int) else f"{key}='{val}'" for key, val in kwargs.items()]
        query += " WITH " + " AND ".join(options)
    session.execute(query)
    data = [{"pk": 1, "ck": ck, "val": f"val1_{ck}"} for ck in range(1, 5)] + [{"pk": 2, "ck": ck, "val": f"val2_{ck}"} for ck in range(1, 7)]
    return create_rows(session, SINGULAR_TABLE, data)


# Ported from scylla-dtest paging_additional_test.py::TestPagingSavedQueryStateSingularRanges::test_single_partition.
# The querier saved at the end of the first page is reused for the second.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_single_partition(manager: ScyllaClusterManager):
    servers, session = await prepare(manager)
    data = setup_simple_table(session)
    pf = PageFetcher(session, SimpleStatement(f"select * from {SINGULAR_TABLE} where pk = 1", fetch_size=3,
                                              consistency_level=ConsistencyLevel.ALL)).request_all()
    all_data = pf.all_data()
    expected = [p for p in data if p["pk"] == 1]
    assert all_data == expected, f"Expected {expected}, got {all_data}"
    requested_pages = pf.requested_pages
    assert requested_pages == 2, f"Expected 2 pages, got {requested_pages}"
    await assert_nodes_metrics(manager, servers, [{"lookups": requested_pages - 1}] * len(servers))


# Ported from scylla-dtest paging_additional_test.py::TestPagingSavedQueryStateSingularRanges::test_two_partitions.
# When the coordinator throws away part of the results, the replica sees the
# position mismatch and drops the saved querier.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_two_partitions(manager: ScyllaClusterManager):
    servers, session = await prepare(manager)
    data = setup_simple_table(session)
    pf = PageFetcher(session, SimpleStatement(f"select * from {SINGULAR_TABLE} where pk in (1, 2)", fetch_size=5,
                                              consistency_level=ConsistencyLevel.ALL)).request_all()
    requested_pages = pf.requested_pages
    all_data = pf.all_data()
    assert requested_pages == 3, f"Expected 3 pages, got {requested_pages}"
    assert all_data == data, f"Expected {data}, got {all_data}"
    await assert_nodes_metrics(manager, servers, [{"lookups": requested_pages - 1, "drops": 1}] * len(servers))


# Ported from scylla-dtest paging_additional_test.py::TestPagingSavedQueryStateSingularRanges::test_replica_usage.
# The coordinators send all the page requests of a CL=ONE query to the same
# replica: every node coordinates some pages (the driver's token-aware
# policy spreads them over the replicas), and one node looks up a saved
# querier for every page but the first, the others none.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_replica_usage(manager: ScyllaClusterManager):
    servers, session = await prepare(manager)
    data = setup_simple_table(session, speculative_retry="NONE", dclocal_read_repair_chance=0.0)

    async def coordinator_reads(server):
        return (await manager.metrics.query(server.ip_addr)).get("scylla_storage_proxy_coordinator_reads") or 0

    baseline = {s.ip_addr: await coordinator_reads(s) for s in servers}
    statement = SimpleStatement(f"select * from {SINGULAR_TABLE} where pk = 2", fetch_size=1, consistency_level=ConsistencyLevel.ONE)
    pf = await asyncio.to_thread(lambda: PageFetcher(session, statement).request_all())

    for s in servers:
        new_reads = await coordinator_reads(s) - baseline[s.ip_addr]
        # Each node was a coordinator at least once, so the test checks something.
        assert new_reads > 0, f"Expected reads more then 0, but got {new_reads}"

    requested_pages = pf.requested_pages
    all_data = pf.all_data()
    expected = [p for p in data if p["pk"] == 2]
    assert requested_pages == 7, f"Expected 7 pages, got {requested_pages}"
    assert all_data == expected, f"Expected {expected}, got {all_data}"
    await assert_nodes_metrics(manager, servers, ({"lookups": pf.requested_pages - 1}, {}))


# Ported from scylla-dtest paging_additional_test.py::TestPagingQueryAlternativeConsistencyLevel::test_consistency_level_quorum.
# A full scan at CL=QUORUM returns the same rows unpaged (the driver's
# default page size) and in pages of 100 rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_consistency_level_quorum(manager: ScyllaClusterManager):
    test_ks_name = "keyspace_complex"
    test_table_name = "paged_query_test"
    statement = f"select * from {test_ks_name}.{test_table_name};"
    _, session = await prepare(manager, consistency_level=ConsistencyLevel.QUORUM)
    create_ks(session, test_ks_name, 3)
    session.execute(f"CREATE TABLE IF NOT EXISTS {test_ks_name}.{test_table_name} (k int PRIMARY KEY, v1 int, v2 int)")
    for i in range(1000):
        random_int = randint(1, 1000000)
        session.execute(f"INSERT INTO {test_ks_name}.{test_table_name} (k, v1, v2) VALUES ({i}, {random_int}, {random_int + i})")

    query_result = [list(row.values()) for row in session.execute(statement)]
    assert query_result, f"Query '{statement}' returned no entries"

    session.default_fetch_size = 100
    fetcher = PageFetcher(session, statement).request_all()
    paging_query_result = [list(row.values()) for row in fetcher.all_data()]
    assert paging_query_result, f"Paged query '{statement}' returned no entries"
    assert sorted(query_result) == sorted(paging_query_result), "Results should be identical!"
