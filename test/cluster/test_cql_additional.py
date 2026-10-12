#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# CQL tests from scylla-dtest cql_additional_tests.py that need a cluster of
# their own: a node loaded by cassandra-stress, and queries of time functions
# on three nodes with RF=3. As in the dtests, keyspaces use vnodes and
# statements use consistency level ONE; populate(1) puts the node in
# datacenter1/rack1 with the SimpleSnitch, while prepare(nodes=3, rf=3) puts
# its three nodes in dc1, one per rack, with ccm's GossipingPropertyFileSnitch.
# The indexed-query paging test runs, like its dtest, on three nodes in dc1,
# one per rack, with RF=3.

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, ReadTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The dtests' populate(1): SimpleSnitch, the node in datacenter1/rack1, vnodes.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "endpoint_snitch": "SimpleSnitch"}
PROPERTY_FILE = {"dc": "datacenter1", "rack": "rack1"}


def one(query: str) -> SimpleStatement:
    """A statement at consistency level ONE, the dtest sessions' default."""
    return SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)


async def prepare_three_nodes_rf3(manager: ScyllaClusterManager):
    """The dtests' prepare(nodes=3, rf=3): ccm's populate({"dc1":
    generate_rack_topology_based_rf(3, 3)}), three nodes in dc1, one per rack,
    with ccm's GossipingPropertyFileSnitch and vnodes. Returns the driver
    session; the keyspace (RF=3) is created by the caller with
    new_test_keyspace."""
    await manager.servers_add(3, config={**CONFIG, "endpoint_snitch": "GossipingPropertyFileSnitch"},
                              property_file=[{"dc": "dc1", "rack": f"rack{i}"} for i in range(1, 4)])
    return manager.get_cql()


KS_RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_cql_timeout_non_zero_value.
# A positive, non-zero USING TIMEOUT is enforced: an unpaged full scan
# (BYPASS CACHE) of a flushed table of 50000 rows of 200 bytes takes longer
# than 2 ms, and the same scan with USING TIMEOUT 2ms fails with a read
# timeout at the coordinator. Both checks are retried, as in the dtest.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_per_query_timeout_nonzero(manager: ScyllaClusterManager):
    # As in the dtest: the unpaged scans read about 10 MB; keep them under the
    # soft limit, so that they don't log a warning.
    server = await manager.server_add(config={**CONFIG, "max_memory_for_unlimited_query_soft_limit": 64 * 1024 * 1024},
                                      property_file=PROPERTY_FILE)
    timeout_duration_ms = 2
    cql_timeout_duration_param = f"{timeout_duration_ms}ms"
    timeout_msg = "Coordinator node timed out waiting for replica nodes"
    cql = manager.get_cql()
    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    cql.execute("CREATE TABLE ks.t (k int PRIMARY KEY, v blob)")
    insert = cql.prepare("INSERT INTO ks.t (k, v) VALUES (?, ?)")
    execute_concurrent_with_args(cql, insert, [(k, bytes(200)) for k in range(50000)], concurrency=100)
    await manager.api.flush_all_keyspaces(server.ip_addr)

    # The scans are unpaged, so a single request reads the whole table, which
    # takes far longer than the 2 ms timeout even on an idle node.
    for attempt in range(8):
        try:
            start = time.time()
            await cql.run_async(SimpleStatement("SELECT * FROM ks.t BYPASS CACHE;", fetch_size=None))
            full_scan_duration = time.time() - start
            logger.debug(f"Full-scan duration is: {full_scan_duration}")
            assert full_scan_duration > timeout_duration_ms / 1000, \
                f"The full-scan 'select' command took unexpectedly shorter time than {cql_timeout_duration_param}"
            break
        except AssertionError:
            if attempt == 7:
                raise
            await asyncio.sleep(2)

    for attempt in range(8):
        try:
            with pytest.raises(ReadTimeout, match=timeout_msg):
                execution_start = time.time()
                await cql.run_async(SimpleStatement(f"SELECT * FROM ks.t BYPASS CACHE USING TIMEOUT {cql_timeout_duration_param};",
                                                    fetch_size=None))
                logger.debug(f"Full-scan duration is: {time.time() - execution_start}")
            break
        except pytest.fail.Exception:
            if attempt == 7:
                raise
            await asyncio.sleep(2)


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_writetime_functions_query.
# toDate(max(mintimeuuid(writetime(x)))) over the dtest's clicks table (100
# users x 3 urls) on three nodes with RF=3 is refused, as writetime() is a
# bigint, or its value is out of range for mintimeuuid() (reported as a
# server error, so NoHostAvailable); it used to crash the node (#5552). Both
# queries are repeated three times, to check the nodes are still up after
# serving them.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_writetime_functions_query(manager: ScyllaClusterManager):
    cql = await prepare_three_nodes_rf3(manager)
    async with new_test_keyspace(manager, KS_RF3) as ks:
        await cql.run_async(f"""
            CREATE TABLE {ks}.clicks (
                userid int,
                url text,
                day int,
                month text,
                year int,
                PRIMARY KEY (userid, url)
            );
        """)
        for _id in range(100):
            for tld in ["com", "org", "net"]:
                await cql.run_async(one(f"INSERT INTO {ks}.clicks (userid, url, day, month, year) VALUES ({_id}, 'http://foo.{tld}', 1, 'jan', 2012)"))

        matching = "timestamp is out of range|A WRITETIME.*doesn't match the type"
        for tries in range(3):
            with pytest.raises((NoHostAvailable, InvalidRequest), match=matching):
                await cql.run_async(one(f"select toDate(max(mintimeuuid(writetime(day)))) from {ks}.clicks ;"))
            with pytest.raises((NoHostAvailable, InvalidRequest), match=matching):
                await cql.run_async(one(f"select toDate(max(mintimeuuid(writetime(month)))) from {ks}.clicks ;"))


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_query_coloumn_timeuuid_with_invalid_values.
# On three nodes with RF=3, mintimeuuid() of a millisecond timestamp beyond
# what a timeuuid can hold, in a WHERE restriction on a timeuuid clustering
# column, is rejected as out of range (a server error, so NoHostAvailable)
# instead of wrapping around (#5552).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_query_column_timeuuid_with_invalid_values(manager: ScyllaClusterManager):
    cql = await prepare_three_nodes_rf3(manager)
    async with new_test_keyspace(manager, KS_RF3) as ks:
        await cql.run_async(f"""
              CREATE TABLE {ks}.test (
                  k int,
                  t timeuuid,
                  PRIMARY KEY (k, t)
              )
          """)
        for i in range(4):
            await cql.run_async(one(f"INSERT INTO {ks}.test (k, t) VALUES (0, now())"))

        for value in (160616626311127, 16061662631112228):
            with pytest.raises(NoHostAvailable, match="timestamp is out of range"):
                await cql.run_async(one(f"SELECT t FROM {ks}.test WHERE t > mintimeuuid({value}) ALLOW FILTERING;"))


# Ported from scylla-dtest cql_additional_tests.py::TestsCQLAdditional::test_indexed_statement_concurrency_limit.
# On three nodes in dc1, one per rack, with RF=3: the first page of a paged
# query through a secondary index (page size 1024, CL=LOCAL_ONE) over 3072
# rows of 4KB each holds fewer rows than requested, as the indexed read is
# cut short by its internal limit of about 1MB per page.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_indexed_statement_concurrency_limit(manager: ScyllaClusterManager):
    # generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
    await manager.servers_add(3, config={"tablets_mode_for_new_keyspaces": "disabled"}, auto_rack_dc="dc1")
    cql = manager.get_cql()
    # The dtest's own keyspace statement.
    await cql.run_async("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    ks = "ks"
    await cql.run_async(SimpleStatement(f"CREATE TABLE IF NOT EXISTS {ks}.tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))",
                                        consistency_level=ConsistencyLevel.ALL))
    await cql.run_async(f"CREATE INDEX ON {ks}.tab (v)")

    blob = "d" * 4096
    total_rows = 3 * 1024
    # This page size should trigger the internal limit, so the query
    # returns fewer rows than requested.
    page_size = 1024
    for i in range(total_rows):
        await cql.run_async(one(f"INSERT INTO {ks}.tab (pk, ck, v, v2, v3) VALUES ({i % 3}, 'hello{i}', 1, {i}, '{blob}')"))

    indexed_select_paged = SimpleStatement(f"SELECT * FROM {ks}.tab WHERE v = 1", fetch_size=page_size,
                                           consistency_level=ConsistencyLevel.LOCAL_ONE)
    res = await cql.run_async(indexed_select_paged, all_pages=False)
    rows_received = len(res)
    logger.debug(f"Indexed select fetched {rows_received} rows out of {page_size}")
    assert rows_received < page_size, \
        f"Expected to get less rows than requested, got {rows_received} with page size of {page_size}."
