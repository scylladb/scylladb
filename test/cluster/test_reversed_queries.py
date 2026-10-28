#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Ported from scylla-dtest reversed_queries_test.py: reversed queries on a
# TWCS table and on a partition larger than a shard's memory.

import asyncio

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

# The dtests' nodes: vnodes keyspaces with 256 tokens per node.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}


def create_ks(cql, name: str, rf: int) -> None:
    """The dtests' create_ks(): the keyspace, schema agreement and USE."""
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"USE {name}")


def page_count(cql, query: str) -> tuple[int, int]:
    """The dtests' PageFetcher(...).request_all(): the number of pages and
    of rows."""
    result = cql.execute(query)
    pages, rows = 1, len(result.current_rows)
    while result.has_more_pages:
        result.fetch_next_page()
        pages += 1
        rows += len(result.current_rows)
    return pages, rows


# Ported from scylla-dtest reversed_queries_test.py::TestReversedQueriesMemoryUsage::test_memory.
# Two nodes (two racks), RF=2: a 1 GB partition is
# written by 10 workers at CL=ALL; reading its last 200 rows in reverse is
# paged (by the page size cutoff), with as many pages as the same read in
# forward order (scylladb#9815).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_memory(manager: ScyllaClusterManager):
    # The dtest sets "smp": 1 and "memory": "512M" as scylla.yaml options,
    # which Scylla does not read: its nodes ran with ccm's 2 shards and 1 GB,
    # as test.py's nodes do.
    await manager.servers_add(2, config=CONFIG, property_file=[
        {"dc": "datacenter1", "rack": "rack1"}, {"dc": "datacenter1", "rack": "rack2"}])
    cql = manager.get_cql()
    create_ks(cql, "test_reversed_queries", 2)
    cql.execute("CREATE TABLE memtest (bucket int, id int, value text, PRIMARY KEY (bucket, id))")

    worker_count = 10
    longstring = "x" * 10000
    row_count = 100 * 1000
    # Using CL=ALL avoids hints and so "Too many in-flight hints" when the disk is slow.
    stmt = cql.prepare("INSERT INTO memtest (bucket, id, value) VALUES(?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.ALL

    def run_writes(worker_id):
        for i in range(worker_id, row_count, worker_count):
            cql.execute(stmt, (0, i, longstring))
    await asyncio.gather(*[asyncio.to_thread(run_writes, w) for w in range(worker_count)])

    reverse_pages, reverse_rows = page_count(cql, SimpleStatement("SELECT * FROM memtest WHERE bucket = 0 ORDER BY id DESC LIMIT 200"))
    assert reverse_pages > 1, "Response was not paged but should"
    assert reverse_rows == 200
    normal_pages, _ = page_count(cql, SimpleStatement("SELECT * FROM memtest WHERE bucket = 0 ORDER BY id LIMIT 200"))
    assert reverse_pages == normal_pages


async def reversed_paging(manager: ScyllaClusterManager, rows: int, fetch_size: int, expected_page_count: int) -> None:
    """TestReversedQueriesPaging.reversed_query_template(): BasePagingTester's
    three nodes (datacenter1, rack1-rack3) with a QUORUM session, RF=3, rows
    written at ALL, and a reversed CL=ALL read of the partition in pages."""
    await manager.servers_add(3, config=CONFIG, property_file=[
        {"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(3)])
    cql = manager.get_cql()
    create_ks(cql, "test_reversed_queries", 3)
    cql.execute("CREATE TABLE paging_test (bucket int, id int, value text, PRIMARY KEY (bucket, id))")
    insert = cql.prepare("INSERT INTO paging_test (bucket, id, value) values (?, ?, ?)")
    insert.consistency_level = ConsistencyLevel.ALL
    for i in range(1, rows + 1):
        cql.execute(insert, (1, i, "testing" if i % 5 == 1 else "and more testing"))
    pages, ids = 0, []
    result = cql.execute(SimpleStatement("select id from paging_test where bucket = 1 order by id desc",
                                         fetch_size=fetch_size, consistency_level=ConsistencyLevel.ALL))
    while True:
        pages += 1
        ids += [r.id for r in result.current_rows]
        if not result.has_more_pages:
            break
        result.fetch_next_page()
    assert pages == expected_page_count
    assert ids == list(range(rows, 0, -1))


# Ported from scylla-dtest reversed_queries_test.py::TestReversedQueriesPaging::test_with_less_results_than_page_size.
# A reversed read of 5 rows with a page size of 100 comes in one page.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_with_less_results_than_page_size(manager: ScyllaClusterManager):
    await reversed_paging(manager, rows=5, fetch_size=100, expected_page_count=1)


# Ported from scylla-dtest reversed_queries_test.py::TestReversedQueriesPaging::test_with_more_results_than_page_size.
# A reversed read of 13 rows with a page size of 5 comes in 3 pages.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_with_more_results_than_page_size(manager: ScyllaClusterManager):
    await reversed_paging(manager, rows=13, fetch_size=5, expected_page_count=3)

