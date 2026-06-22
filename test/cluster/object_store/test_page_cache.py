#!/usr/bin/env python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import pytest

from cassandra import InvalidRequest
from test.pylib.manager_client import ManagerClient
from test.pylib.object_storage import format_tuples
from test.cluster.util import new_test_keyspace


async def test_s3_page_cache(manager: ManagerClient, s3_storage):
    '''Verify that a point read served from S3 populates the S3 page cache, so
    that after dropping the row cache the same point read requires zero new S3 GET
    requests.

    We use a point read (WHERE pk = ?) rather than a full-table scan because
    the caching policy for scans may in theory differ - e.g. a full-table scan
    might avoid caching the entire table. So scans have their separate tests
    below.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")

        # Insert a few rows so the flush produces an SSTable with a known key we can point-read.
        n_rows = 10
        row_value = 'x' * 100
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{row_value}');") for i in range(n_rows)])

        # Flush to S3 so the data lives entirely in an SSTable object (not in the memtable).
        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache before the first read so that the read is guaranteed to go to
        # S3 and warm the S3 page cache, rather than possibly being served from an
        # already-populated row cache.
        await manager.api.drop_sstable_caches(ip)

        metrics_before_first = await manager.metrics.query(ip)
        gets_before_first = metrics_before_first.get('scylla_s3_total_read_requests') or 0.0

        # First read: goes to S3, warming the S3 page cache (and the row cache).
        row = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row is not None and row.v == row_value

        # Sanity check: the first read must have issued at least one S3 GET
        # (otherwise the metric is broken or the data was somehow already cached).
        metrics_after_first = await manager.metrics.query(ip)
        gets_after_first = metrics_after_first.get('scylla_s3_total_read_requests') or 0.0
        assert gets_after_first > gets_before_first

        # Drop the row cache so the next read cannot be served from decoded-row cache.
        await manager.api.drop_sstable_caches(ip)

        # Second read (same point read): with a page cache the data page is
        # still cached, so zero new S3 GETs are needed. Without a page cache,
        # new GETs are issued and the metrics would increase.
        row2 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row2 is not None and row2.v == row_value

        metrics_after = await manager.metrics.query(ip)
        gets_after = metrics_after.get('scylla_s3_total_read_requests') or 0.0

        assert 0 == gets_after - gets_after_first


async def test_tiering_requires_compression(manager: ManagerClient, s3_storage):
    '''Verify that creating a table without compression in a keyspace with
    tiering=data_page_cache is rejected. The page cache uses compressed-chunk
    offsets as cache keys, which are only well-defined for compressed SSTables.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        with pytest.raises(InvalidRequest, match="tiering=data_page_cache"):
            await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH compression = {{}};")
        # An empty sstable_compression value also disables compression (maps to algorithm::none)
        # and must be rejected with the same error.
        with pytest.raises(InvalidRequest, match="tiering=data_page_cache"):
            await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH compression = {{'sstable_compression': ''}};")


async def test_s3_page_cache_scan_populates_cache_for_point_reads(manager: ManagerClient, s3_storage):
    '''Verify that a full-table scan populates the S3 page cache at compressed
    chunk offsets, so that subsequent point reads require zero new S3 GET requests.

    If we don't pay special attention to this case, the scan can cache data at
    file_input_stream buffer-aligned positions (0, 128 KB, 256 KB, ...) while
    point reads looked up the cache at compressed chunk-boundary offsets (e.g.
    ~2 KB for the second chunk).  Because these offsets never coincide for
    chunks past the first, point reads after a scan could result in spurious
    cache misses and extra S3 GET requests.

    The correct implementation should store compressed chunks at their exact
    compressed-file offsets, so scan-populated entries are reusable for all
    subsequent point reads.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")

        # Insert enough rows to guarantee the SSTable spans at least two
        # compressed chunks (the default chunk size is 4 KB of uncompressed data).
        # 20 rows x 5000 bytes ~= 100 KB uncompressed -> ~25 compressed chunks.
        n_rows = 20
        row_value = 'x' * 5000
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{row_value}');") for i in range(n_rows)])

        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache so the scan definitely reads from S3.
        await manager.api.drop_sstable_caches(ip)

        metrics_before_scan = await manager.metrics.query(ip)
        gets_before_scan = metrics_before_scan.get('scylla_s3_total_read_requests') or 0.0

        # Full-table scan: reads all compressed chunks from S3 and caches each
        # at its exact compressed-file offset via chunk_page_cache.
        rows = list(cql.execute(f"SELECT * FROM {ks}.test;"))
        assert len(rows) == n_rows

        # Sanity check: the scan must have issued at least one S3 GET.
        metrics_after_scan = await manager.metrics.query(ip)
        gets_after_scan = metrics_after_scan.get('scylla_s3_total_read_requests') or 0.0
        assert gets_after_scan > gets_before_scan

        # Drop the row cache so the next reads go to the SSTable layer.
        await manager.api.drop_sstable_caches(ip)

        # Point reads for every partition: all compressed chunks are already in
        # the S3 page cache, so zero new S3 GETs should be issued.
        for pk in range(n_rows):
            row = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = {pk};").one()
            assert row is not None and row.v == row_value

        metrics_after_reads = await manager.metrics.query(ip)
        gets_after_reads = metrics_after_reads.get('scylla_s3_total_read_requests') or 0.0

        assert gets_after_reads == gets_after_scan


async def test_s3_page_cache_single_partition_multi_chunk(manager: ManagerClient, s3_storage):
    '''Verify page cache behaviour for a single wide partition spread across
    multiple compressed chunks.

    A single partition is written with enough large clustering rows to fill
    more than one compressed chunk (4 KB each by default).  Then:

    1. A partition scan warms the S3 page cache.  Because the whole partition
       is fetched via a single streaming S3 GET, the download issues exactly
       one new S3 GET.
    2. After the scan, system.pagecache contains multiple rows for the SSTable
       (one per cached compressed chunk).
    3. Point reads for each individual clustering row afterwards require zero
       new S3 GETs - all chunks are already cached.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, v text, PRIMARY KEY (pk, ck));")

        # Write enough data to a single partition to span more than one
        # compressed chunks.  The default chunk size is 4 KB of uncompressed
        # data, so 20 rows x 5000 bytes ~= 100 KB -> ~25 chunks.
        n_rows = 20
        row_value = 'x' * 5000
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck, v) VALUES (0, {i}, '{row_value}');") for i in range(n_rows)])

        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache so the partition scan goes to S3.
        await manager.api.drop_sstable_caches(ip)

        metrics_before_scan = await manager.metrics.query(ip)
        gets_before_scan = metrics_before_scan.get('scylla_s3_total_read_requests') or 0.0

        # Scan the whole partition.
        rows = list(cql.execute(f"SELECT ck, v FROM {ks}.test WHERE pk = 0;"))
        assert len(rows) == n_rows

        metrics_after_scan = await manager.metrics.query(ip)
        gets_after_scan = metrics_after_scan.get('scylla_s3_total_read_requests') or 0.0

        # The whole partition lives in one Data.db range; one streaming GET
        # suffices to read it end-to-end.
        assert gets_after_scan - gets_before_scan == 1

        # The system.pagecache table must contain more than one entry for this
        # SSTable - at least one row per compressed chunk.
        cached_rows = list(cql.execute("SELECT sstable, offset FROM system.pagecache;"))
        assert len(cached_rows) > 1

        # Drop the row cache so individual reads go to the SSTable layer.
        await manager.api.drop_sstable_caches(ip)

        # Point reads for each clustering row must not trigger new S3 GETs.
        for ck in range(n_rows):
            row = cql.execute(f"SELECT v FROM {ks}.test WHERE pk = 0 AND ck = {ck};").one()
            assert row is not None and row.v == row_value

        metrics_after_reads = await manager.metrics.query(ip)
        gets_after_reads = metrics_after_reads.get('scylla_s3_total_read_requests') or 0.0

        assert gets_after_reads == gets_after_scan

        # If we drop the row cache again and re-scan the entire partition,
        # we still won't issue any new S3 GETs since all chunks are cached.
        await manager.api.drop_sstable_caches(ip)

        rows2 = list(cql.execute(f"SELECT ck, v FROM {ks}.test WHERE pk = 0;"))
        assert len(rows2) == n_rows

        metrics_after_rescan = await manager.metrics.query(ip)
        gets_after_rescan = metrics_after_rescan.get('scylla_s3_total_read_requests') or 0.0
        assert gets_after_rescan == gets_after_reads


async def test_s3_page_cache_point_reads_populate_cache_for_scan(manager: ManagerClient, s3_storage):
    '''Verify that point reads populate the S3 page cache so that a subsequent
    partition scan requires zero new S3 GET requests.

    This is the reverse of test_s3_page_cache_single_partition_multi_chunk: instead
    of a scan warming the cache for later point reads, here individual point reads
    warm the cache chunk by chunk, and the subsequent scan finds all chunks already
    cached.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, v text, PRIMARY KEY (pk, ck));")

        # Write enough data to span more than one compressed chunk (4 KB each).
        # 20 rows x 5000 bytes ~= 100 KB -> ~25 chunks.
        n_rows = 20
        row_value = 'x' * 5000
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck, v) VALUES (0, {i}, '{row_value}');") for i in range(n_rows)])

        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache so the point reads go to S3.
        await manager.api.drop_sstable_caches(ip)

        # Point reads for every clustering row: each read fetches the chunks it
        # needs from S3 and caches them.
        for ck in range(n_rows):
            row = cql.execute(f"SELECT v FROM {ks}.test WHERE pk = 0 AND ck = {ck};").one()
            assert row is not None and row.v == row_value

        metrics_after_reads = await manager.metrics.query(ip)
        gets_after_reads = metrics_after_reads.get('scylla_s3_total_read_requests') or 0.0

        # Sanity check: the point reads must have issued at least one S3 GET.
        assert gets_after_reads > 0

        # Drop the row cache so the scan goes to the SSTable layer.
        await manager.api.drop_sstable_caches(ip)

        # Full partition scan: all chunks are already in the S3 page cache from
        # the point reads above, so zero new S3 GETs should be issued.
        rows = list(cql.execute(f"SELECT ck, v FROM {ks}.test WHERE pk = 0;"))
        assert len(rows) == n_rows

        metrics_after_scan = await manager.metrics.query(ip)
        gets_after_scan = metrics_after_scan.get('scylla_s3_total_read_requests') or 0.0
        assert gets_after_scan == gets_after_reads


async def test_s3_page_cache_partial_reads_then_scan(manager: ManagerClient, s3_storage):
    '''Verify correctness and cache behaviour when only a few rows of a wide
    partition are point-read before a full partition scan.

    Point reads for ck=0 and one middle clustering row warm a subset of the
    compressed chunks.  The subsequent full scan must still return all 20 rows
    correctly (the un-cached chunks are fetched from S3 on demand).  After the
    scan all chunks are in the page cache, so a second scan (after dropping the
    row cache) issues zero new S3 GETs.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, v text, PRIMARY KEY (pk, ck));")

        # Write enough data to span many compressed chunks (4 KB each by default).
        # 20 rows x 5000 bytes ~= 100 KB -> ~25 chunks.
        n_rows = 20
        row_value = 'x' * 5000
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck, v) VALUES (0, {i}, '{row_value}');") for i in range(n_rows)])

        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache so the point reads definitely go to S3.
        await manager.api.drop_sstable_caches(ip)

        # Read only two rows: the first clustering row and one from the middle.
        # This warms only the subset of chunks that contain those rows.
        for ck in [0, n_rows // 2]:
            row = cql.execute(f"SELECT v FROM {ks}.test WHERE pk = 0 AND ck = {ck};").one()
            assert row is not None and row.v == row_value

        # Drop the row cache so the scan goes to the SSTable layer.
        await manager.api.drop_sstable_caches(ip)

        # Full partition scan: must return all rows correctly even though only
        # some chunks are cached.  Un-cached chunks are fetched from S3.
        rows = list(cql.execute(f"SELECT ck, v FROM {ks}.test WHERE pk = 0;"))
        assert len(rows) == n_rows
        assert all(r.v == row_value for r in rows)

        metrics_after_scan = await manager.metrics.query(ip)
        gets_after_scan = metrics_after_scan.get('scylla_s3_total_read_requests') or 0.0

        # Drop the row cache again: all chunks are now cached (the scan just
        # fetched any that were missing), so a second scan must issue zero new
        # S3 GETs.
        await manager.api.drop_sstable_caches(ip)

        rows2 = list(cql.execute(f"SELECT ck, v FROM {ks}.test WHERE pk = 0;"))
        assert len(rows2) == n_rows

        metrics_after_rescan = await manager.metrics.query(ip)
        gets_after_rescan = metrics_after_rescan.get('scylla_s3_total_read_requests') or 0.0
        assert gets_after_rescan == gets_after_scan


async def test_tiered_compaction(manager: ManagerClient, s3_storage):
    '''Verify that compaction works correctly for tiered (data_page_cache) tables.

    Flush two separate SSTables, compact them together, then read back and
    verify that the merged result is correct: overwrites from the second flush
    are visible and deleted keys are gone.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")

        # First flush: rows 0..9 with value 'first'
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, 'first');") for i in range(10)])
        await manager.api.flush_keyspace(ip, ks)

        # Second flush: overwrite rows 0..4 with 'second', delete rows 5..9
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, 'second');") for i in range(5)])
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.test WHERE pk = {i};") for i in range(5, 10)])
        await manager.api.flush_keyspace(ip, ks)

        # Compact both SSTables into one.
        await manager.api.keyspace_compaction(ip, ks)

        # Read back and verify merged result.
        rows = {row.pk: row.v for row in cql.execute(f"SELECT pk, v FROM {ks}.test;")}

        # Rows 0..4 should have the value from the second flush.
        for pk in range(5):
            assert rows.get(pk) == 'second'

        # Rows 5..9 should have been deleted.
        for pk in range(5, 10):
            assert pk not in rows


async def test_s3_page_cache_eviction_on_compaction(manager: ManagerClient, s3_storage):
    '''Verify that compaction evicts the page cache entries for the input SSTables
    and that the compacted SSTable gets its own entries when subsequently read.

    Two SSTables are flushed.  A full scan populates system.pagecache with entries
    under two distinct SSTable UUIDs.  After compaction the two original UUIDs must
    be gone from system.pagecache (the cache entries for deleted SSTables are
    useless and should be cleaned up).  A second scan then repopulates the cache
    under the single new (compacted) SSTable UUID.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, v text, PRIMARY KEY (pk, ck));")

        # Write enough data per flush to produce multiple compressed chunks.
        # 20 rows x 5000 bytes ~= 100 KB -> ~25 chunks per SSTable.
        n_rows = 20
        row_value = 'x' * 5000

        # Disable auto-compaction so we can check things before compaction
        # happens and not worry about auto-compaction happening before that.
        await manager.api.disable_autocompaction(ip, ks)

        # First sstable: ck=0..n_rows-1 in partition pk=0.
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck, v) VALUES (0, {i}, '{row_value}');") for i in range(n_rows)])
        await manager.api.flush_keyspace(ip, ks)

        # Second sstable: ck=n_rows..2*n_rows-1 in the same partition pk=0.
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck, v) VALUES (0, {n_rows + i}, '{row_value}');") for i in range(n_rows)])
        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache and scan the partition to warm system.pagecache for both SSTables.
        await manager.api.drop_sstable_caches(ip)
        rows = list(cql.execute(f"SELECT ck FROM {ks}.test WHERE pk = 0;"))
        assert len(rows) == 2 * n_rows

        # system.pagecache must now contain entries under at least 2 distinct UUIDs
        # (one per SSTable).  We look up only the UUIDs that belong to our table
        # via system.sstables so we don't pick up entries from other tests.
        table_id_row = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name='{ks}' AND table_name='test';").one()
        table_id = table_id_row.id
        node_owner = cql.execute("SELECT host_id FROM system.local;").one().host_id
        uuids_before = {row.generation for row in cql.execute(f"SELECT generation FROM system.sstables WHERE table_id={table_id} AND node_owner={node_owner};")}
        assert len(uuids_before) >= 2
        # Sanity check: all of those UUIDs must already be present in pagecache.
        uuids_in_pagecache = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache;")}
        assert uuids_before <= uuids_in_pagecache
        # pagecache_hits is written only on a cache hit, not on the initial put.
        # Do a second scan (after dropping the row cache) to generate hits and
        # populate pagecache_hits before checking it.
        await manager.api.drop_sstable_caches(ip)
        rows_rehit = list(cql.execute(f"SELECT ck FROM {ks}.test WHERE pk = 0;"))
        assert len(rows_rehit) == 2 * n_rows
        uuids_in_pagecache_hits = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache_hits;")}
        assert uuids_before <= uuids_in_pagecache_hits

        # Compact both SSTables into one.
        await manager.api.keyspace_compaction(ip, ks)

        # After compaction the two original SSTables are deleted; their page cache
        # entries must have been evicted from both pagecache and pagecache_hits.
        uuids_in_pagecache_after_compaction = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache;")}
        assert uuids_before.isdisjoint(uuids_in_pagecache_after_compaction)
        uuids_in_pagecache_hits_after_compaction = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache_hits;")}
        assert uuids_before.isdisjoint(uuids_in_pagecache_hits_after_compaction)

        # Scan again to warm the cache for the new compacted SSTable.
        await manager.api.drop_sstable_caches(ip)
        rows2 = list(cql.execute(f"SELECT ck FROM {ks}.test WHERE pk = 0;"))
        assert len(rows2) == 2 * n_rows

        # After compaction there is exactly one SSTable; its UUID must be in pagecache.
        uuids_after_compaction = {row.generation for row in cql.execute(f"SELECT generation FROM system.sstables WHERE table_id={table_id} AND node_owner={node_owner};")}
        assert len(uuids_after_compaction) == 1
        uuids_in_pagecache_after_scan = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache;")}
        assert uuids_after_compaction <= uuids_in_pagecache_after_scan


async def test_s3_page_cache_eviction_on_table_drop(manager: ManagerClient, s3_storage):
    '''Verify that dropping a table evicts all its page cache entries from
    system.pagecache and system.pagecache_hits.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (0, 'hello');")
        await manager.api.flush_keyspace(ip, ks)

        # Warm the page cache with a point read.
        await manager.api.drop_sstable_caches(ip)
        row = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row is not None and row.v == 'hello'

        # Record the SSTable UUID(s) for this table, then verify they are in the cache.
        table_id_row = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name='{ks}' AND table_name='test';").one()
        table_id = table_id_row.id
        node_owner = cql.execute("SELECT host_id FROM system.local;").one().host_id
        uuids = {row.generation for row in cql.execute(f"SELECT generation FROM system.sstables WHERE table_id={table_id} AND node_owner={node_owner};")}
        assert len(uuids) >= 1

        uuids_in_pagecache = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache;")}
        assert uuids <= uuids_in_pagecache
        # pagecache_hits is written only on a cache hit, not on the initial put.
        # Re-read the row (after dropping the row cache) to generate a hit and
        # populate pagecache_hits before checking it.
        await manager.api.drop_sstable_caches(ip)
        row2 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row2 is not None and row2.v == 'hello'
        uuids_in_pagecache_hits = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache_hits;")}
        assert uuids <= uuids_in_pagecache_hits

        # Drop the table; this must evict all its entries from the page cache.
        await cql.run_async(f"DROP TABLE {ks}.test;")

        uuids_in_pagecache_after_drop = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache;")}
        assert uuids.isdisjoint(uuids_in_pagecache_after_drop)
        uuids_in_pagecache_hits_after_drop = {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache_hits;")}
        assert uuids.isdisjoint(uuids_in_pagecache_hits_after_drop)


async def test_s3_page_cache_non_bti_index_write(manager: ManagerClient, s3_storage):
    '''Verify that writing an SSTable with tiering=data_page_cache using the
    non-BTI "me" sstable format works.

    Non-BTI sstables use component_type::Index (Index.db) rather than BTI's
    Rows.db + Partitions.db pair.  Some code paths are different in this case,
    and during the development we saw crashes and errors in this code path
    (both read and write paths), so it's worth continuing to check this code
    path. All other tests in this file use the default sstable format with
    BTI, so don't exercise the non-BTI code paths.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'sstable_format': 'me'}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (0, 'hello');")

        # Flush to S3, exercising the write to S3 path
        await manager.api.flush_keyspace(ip, ks)

        # Read the row back to confirm the write completed successfully and
        # exercising the read path..
        await manager.api.drop_sstable_caches(ip)
        row = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row is not None and row.v == 'hello'


async def test_s3_pagecache_hits_tracking(manager: ManagerClient, s3_storage):
    '''Verify the write pattern of system.pagecache_hits.

    system.pagecache_hits records the last time a cached page was *accessed*
    (i.e. served from the cache on a hit), not the time it was first stored.
    This is intentional: the table is used by the eviction daemon to find
    least-recently-used pages.  A page that was put into the cache but never
    re-read should look *old* to the eviction daemon - writing a fresh
    timestamp on the initial put would make it appear recently used and
    suppress eviction, which is wrong.

    The expected sequence is:
      1st read  (cache miss)  -> page stored in system.pagecache,
                                 system.pagecache_hits has NO entry yet.
      2nd read  (cache hit)   -> system.pagecache_hits gets an entry with
                                 last_hit = now.
      3rd read  (cache hit)   -> system.pagecache_hits entry updated to a
                                 newer last_hit timestamp.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (0, 'hello');")
        await manager.api.flush_keyspace(ip, ks)

        table_id_row = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name='{ks}' AND table_name='test';").one()
        table_id = table_id_row.id
        node_owner = cql.execute("SELECT host_id FROM system.local;").one().host_id

        def sstable_uuids():
            return {row.generation for row in cql.execute(f"SELECT generation FROM system.sstables WHERE table_id={table_id} AND node_owner={node_owner};")}

        def pagecache_uuids():
            return {row.sstable for row in cql.execute("SELECT sstable FROM system.pagecache;")}

        def pagecache_hits_rows():
            return {(row.sstable, row.offset): row.last_hit for row in cql.execute("SELECT sstable, offset, last_hit FROM system.pagecache_hits;")}

        # 1st read: cache miss - the page is fetched from S3 and stored in
        # system.pagecache.  system.pagecache_hits must NOT be written yet.
        await manager.api.drop_sstable_caches(ip)
        row = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row is not None and row.v == 'hello'

        uuids = sstable_uuids()
        assert len(uuids) >= 1
        assert uuids <= pagecache_uuids()
        assert not any(k[0] in uuids for k in pagecache_hits_rows())

        # 2nd read: cache hit - the page is served from system.pagecache.
        # system.pagecache_hits must now contain an entry for our SSTable.
        await manager.api.drop_sstable_caches(ip)
        row2 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row2 is not None and row2.v == 'hello'

        hits_after_second = pagecache_hits_rows()
        our_hits_after_second = {k: v for k, v in hits_after_second.items() if k[0] in uuids}
        assert len(our_hits_after_second) >= 1

        # 3rd read: another cache hit - last_hit must be newer than after the
        # 2nd read.  Sleep 2 ms first to ensure db_clock (millisecond precision)
        # advances, so we can assert > rather than >=.
        await asyncio.sleep(0.002)
        await manager.api.drop_sstable_caches(ip)
        row3 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row3 is not None and row3.v == 'hello'

        hits_after_third = pagecache_hits_rows()
        our_hits_after_third = {k: v for k, v in hits_after_third.items() if k[0] in uuids}
        assert len(our_hits_after_third) >= 1
        for key in our_hits_after_second:
            if key in our_hits_after_third:
                assert our_hits_after_third[key] > our_hits_after_second[key]


async def test_s3_bypass_cache_does_not_populate_page_cache(manager: ManagerClient, s3_storage):
    '''Verify that reads with BYPASS CACHE do not populate the S3 page cache.

    A normal read warms the page cache (entries appear in system.pagecache).
    A subsequent read with BYPASS CACHE must still return correct data by
    fetching directly from S3, and must not add any new entries to
    system.pagecache.
    '''
    objconf = s3_storage.create_endpoint_conf()
    cfg = {'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)
    ip = server.ip_addr

    cql = manager.get_cql()
    storage_opts = format_tuples(type=s3_storage.type, endpoint=s3_storage.address, bucket=s3_storage.bucket_name, tiering='data_page_cache')
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND STORAGE = {storage_opts}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text);")

        n_rows = 10
        row_value = 'x' * 100
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{row_value}');") for i in range(n_rows)])

        await manager.api.flush_keyspace(ip, ks)

        # Drop the row cache so the first read goes to the SSTable layer.
        await manager.api.drop_sstable_caches(ip)

        # BYPASS CACHE read: must return correct data but must NOT populate the
        # page cache.  Check that system.pagecache has no entries for our table
        # after the read.
        table_id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name='{ks}' AND table_name='test';").one().id
        node_owner = cql.execute("SELECT host_id FROM system.local;").one().host_id

        def our_sstable_uuids():
            return {row.generation for row in cql.execute(f"SELECT generation FROM system.sstables WHERE table_id={table_id} AND node_owner={node_owner};")}

        def our_pagecache_entries():
            return [row for row in cql.execute("SELECT sstable FROM system.pagecache;") if row.sstable in our_sstable_uuids()]

        def our_pagecache_hits_timestamps():
            uuids = our_sstable_uuids()
            return {(row.sstable, row.offset): row.last_hit for row in cql.execute("SELECT sstable, offset, last_hit FROM system.pagecache_hits;") if row.sstable in uuids}

        row = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0 BYPASS CACHE;").one()
        assert row is not None and row.v == row_value

        # The page cache must remain empty for our table after a BYPASS CACHE read.
        assert our_pagecache_entries() == []

        # First normal read: warms the page cache (cache miss, so pagecache_hits is not written yet).
        row2 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row2 is not None and row2.v == row_value

        entries_after_normal_read = our_pagecache_entries()
        assert len(entries_after_normal_read) >= 1

        # Second normal read (after dropping the row cache): cache hit, so pagecache_hits is now written.
        await manager.api.drop_sstable_caches(ip)
        row3 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0;").one()
        assert row3 is not None and row3.v == row_value

        hits_before_bypass = our_pagecache_hits_timestamps()
        assert len(hits_before_bypass) >= 1

        # BYPASS CACHE read must not add new pagecache entries, and must not
        # update the last_hit timestamps in pagecache_hits.  Sleep 2 ms first
        # so that any spurious timestamp update would be detectable.
        await asyncio.sleep(0.002)
        row4 = cql.execute(f"SELECT * FROM {ks}.test WHERE pk = 0 BYPASS CACHE;").one()
        assert row4 is not None and row4.v == row_value

        entries_after_bypass = our_pagecache_entries()
        assert len(entries_after_bypass) == len(entries_after_normal_read)

        hits_after_bypass = our_pagecache_hits_timestamps()
        assert hits_after_bypass == hits_before_bypass
