#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests that reads do, or do not, go through the row cache, judged by the
# node-wide cache read counter. These tests need a node of their own: other
# tests running against a shared node would move the counters.

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace, new_test_table

ROWS = 100
# Internal reads (e.g. of system tables) also go through the cache, so reads
# that bypass the cache may still see the counter move a little, while
# reading each of the ROWS partitions through the cache moves it by ROWS.
INTERNAL_READS_THRESHOLD = ROWS // 4


async def cache_reads(manager, server):
    return (await manager.metrics.query(server.ip_addr)).get("scylla_cache_reads") or 0


# Read every partition of the table with a single-partition query, and
# return how much the cache read counter moved meanwhile.
async def read_all_and_count_cache_reads(manager, server, cql, query):
    stmt = cql.prepare(query)
    before = await cache_reads(manager, server)
    for k in range(ROWS):
        assert list(await cql.run_async(stmt, [k])) == [(k, f"v{k}")]
    reads = await cache_reads(manager, server) - before
    return reads


# A table created WITH caching = {'enabled': false} must be read from disk,
# not through the cache; ALTER TABLE switching caching on and off again must
# switch reads to the cache and back to disk.
# Note that the sstable index page cache can still be used when caching is
# disabled for the table (issue #9968), so it is not checked here.
async def test_table_caching_option(manager: ScyllaClusterManager):
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "k int PRIMARY KEY, v text", "WITH caching = {'enabled': false}") as table:
            stmt = cql.prepare(f"INSERT INTO {table} (k, v) VALUES (?, ?)")
            for k in range(ROWS):
                await cql.run_async(stmt, [k, f"v{k}"])
            await manager.api.keyspace_flush(server.ip_addr, ks)
            query = f"SELECT k, v FROM {table} WHERE k = ?"

            assert await read_all_and_count_cache_reads(manager, server, cql, query) < INTERNAL_READS_THRESHOLD

            await cql.run_async(f"ALTER TABLE {table} WITH caching = {{'enabled': true}}")
            assert await read_all_and_count_cache_reads(manager, server, cql, query) >= ROWS

            await cql.run_async(f"ALTER TABLE {table} WITH caching = {{'enabled': false}}")
            assert await read_all_and_count_cache_reads(manager, server, cql, query) < INTERNAL_READS_THRESHOLD


QUERIES = 10


async def metric(manager, server, name):
    return (await manager.metrics.query(server.ip_addr)).get(name) or 0


# A parallelized aggregation (SELECT count(*) of the whole table, which the
# coordinator splits among the nodes and shards) is a range scan like any
# other: without BYPASS CACHE it must be counted in
# select_partition_range_scan_no_bypass_cache and read through the cache,
# and with BYPASS CACHE it must not be counted there and must not read
# through the cache. Parallelized SELECT statements used to set neither
# range-scan flag, so neither counter moved and BYPASS CACHE was ignored.
async def test_parallelized_aggregation_bypass_cache(manager: ScyllaClusterManager):
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "k int PRIMARY KEY, v text") as table:
            stmt = cql.prepare(f"INSERT INTO {table} (k, v) VALUES (?, ?)")
            for k in range(ROWS):
                await cql.run_async(stmt, [k, f"v{k}"])
            await manager.api.keyspace_flush(server.ip_addr, ks)

            async def run_count(bypass_cache):
                names = ["scylla_cql_select_partition_range_scan_no_bypass_cache", "scylla_cql_select_parallelized", "scylla_cache_reads"]
                before = [await metric(manager, server, name) for name in names]
                for _ in range(QUERIES):
                    rows = await cql.run_async(f"SELECT count(*) FROM {table}{' BYPASS CACHE' if bypass_cache else ''}")
                    assert rows[0].count == ROWS
                after = [await metric(manager, server, name) for name in names]
                return [a - b for a, b in zip(after, before)]

            no_bypass_cache_scans, parallelized, cached_reads = await run_count(bypass_cache=False)
            assert parallelized == QUERIES
            assert no_bypass_cache_scans == QUERIES
            assert cached_reads >= QUERIES
            no_bypass_cache_scans, parallelized, reads = await run_count(bypass_cache=True)
            assert parallelized == QUERIES
            assert no_bypass_cache_scans == 0
            # Allow for a few internal reads through the cache meanwhile.
            assert reads < cached_reads / 2
