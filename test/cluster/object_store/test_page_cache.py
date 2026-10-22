#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the disk page cache of object-storage keyspaces: a keyspace
# created with STORAGE = {..., 'disk_cache': 'pages'} keeps all of its
# sstables on the object store, but caches byte ranges read from it on the
# node's disk.

import asyncio
import random

import pytest
from cassandra.protocol import ConfigurationException, InvalidRequest

from test.pylib.object_storage import format_tuples
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace


def page_cache_config(object_storage):
    return {'object_storage_endpoints': object_storage.create_endpoint_conf(),
            'experimental_features': ['object-storage-page-cache']}


def storage_options(object_storage, **extra):
    return format_tuples(type=object_storage.type, endpoint=object_storage.address,
                         bucket=object_storage.bucket_name, **extra)


def keyspace_options(object_storage, tablets=True, **extra):
    return (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
            f" AND tablets = {{'enabled': {'true' if tablets else 'false'}}}"
            f' AND STORAGE = {storage_options(object_storage, **extra)}')


# ALTER KEYSPACE options that change only the storage options.
def alter_options(object_storage, **extra):
    return f'WITH STORAGE = {storage_options(object_storage, **extra)}'


async def test_disk_cache_pages(manager: ScyllaClusterManager, object_storage):
    '''A keyspace can be created with 'disk_cache': 'pages'.'''
    server = await manager.server_add(config=page_cache_config(object_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(object_storage, disk_cache='pages')) as ks:
        # Check that DESCRIBE shows the option.
        expected_storage = (f"{{'type': '{object_storage.type}', 'bucket': '{object_storage.bucket_name}',"
                            f" 'disk_cache': 'pages', 'endpoint': '{object_storage.address}'}}")
        desc = cql.execute(f'DESCRIBE KEYSPACE {ks}').one().create_statement
        assert expected_storage in desc

        # Write data to an sstable on the object store.
        await cql.run_async(f'CREATE TABLE {ks}.test (p int PRIMARY KEY, v int)')
        await cql.run_async(f'INSERT INTO {ks}.test (p, v) VALUES (1, 2)')
        await manager.api.flush_keyspace(server.ip_addr, ks)

        # Check that the option survives a restart, which reads it back from
        # the schema tables, and that the data can be read back.
        await manager.server_restart(server.server_id)
        cql, _ = await manager.get_ready_cql([server])
        desc = cql.execute(f'DESCRIBE KEYSPACE {ks}').one().create_statement
        assert expected_storage in desc
        assert list(cql.execute(f'SELECT p, v FROM {ks}.test')) == [(1, 2)]


async def test_disk_cache_none(manager: ScyllaClusterManager, s3_storage):
    ''''disk_cache': 'none' is the same as not giving the option.'''
    # Unlike 'pages', 'none' needs no experimental feature.
    await manager.server_add(config={'object_storage_endpoints': s3_storage.create_endpoint_conf()})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='none')) as ks:
        # Check that DESCRIBE doesn't show the option.
        desc = cql.execute(f'DESCRIBE KEYSPACE {ks}').one().create_statement
        assert 'disk_cache' not in desc
        # Check that ALTER KEYSPACE may switch between the two forms, as
        # they are the same storage options.
        await cql.run_async(f'ALTER KEYSPACE {ks} {alter_options(s3_storage)}')
        none_options = alter_options(s3_storage, disk_cache='none')
        await cql.run_async(f'ALTER KEYSPACE {ks} {none_options}')


async def test_disk_cache_rejected(manager: ScyllaClusterManager, s3_storage):
    '''Invalid uses of the 'disk_cache' option are refused.'''
    await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()

    # Check that an unknown value 'nonsense' is refused.
    bad_value_options = keyspace_options(s3_storage, disk_cache='nonsense')
    with pytest.raises(InvalidRequest, match="Unknown disk_cache value 'nonsense'"):
        await cql.run_async(f'CREATE KEYSPACE ks_bad_value {bad_value_options}')

    # Check that 'pages' is refused in a keyspace without tablets.
    vnodes_options = keyspace_options(s3_storage, tablets=False, disk_cache='pages')
    with pytest.raises(ConfigurationException, match='only supported in keyspaces with tablets'):
        await cql.run_async(f'CREATE KEYSPACE ks_vnodes {vnodes_options}')

    # Check that the option is refused for local storage.
    with pytest.raises(InvalidRequest, match='Local storage does not accept any custom options'):
        await cql.run_async("CREATE KEYSPACE ks_local WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                            " AND STORAGE = {'type': 'LOCAL', 'disk_cache': 'pages'}")

    # Check that ALTER KEYSPACE cannot add a disk cache: it is chosen when
    # the keyspace is created.
    async with new_test_keyspace(manager, keyspace_options(s3_storage)) as ks:
        pages_options = alter_options(s3_storage, disk_cache='pages')
        with pytest.raises(InvalidRequest, match='Cannot alter storage options'):
            await cql.run_async(f'ALTER KEYSPACE {ks} {pages_options}')
    # Check that ALTER KEYSPACE cannot remove the disk cache either.
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        with pytest.raises(InvalidRequest, match='Cannot alter storage options'):
            await cql.run_async(f'ALTER KEYSPACE {ks} {alter_options(s3_storage)}')


async def test_disk_cache_needs_experimental_feature(manager: ScyllaClusterManager, s3_storage):
    '''Without the 'object-storage-page-cache' experimental feature, a keyspace
    with 'disk_cache': 'pages' cannot be created.'''
    await manager.server_add(config={'object_storage_endpoints': s3_storage.create_endpoint_conf()})
    cql = manager.get_cql()
    pages_options = keyspace_options(s3_storage, disk_cache='pages')
    with pytest.raises(ConfigurationException, match="requires the experimental feature 'object-storage-page-cache'"):
        await cql.run_async(f'CREATE KEYSPACE ks_no_feature {pages_options}')


async def read_requests(manager: ScyllaClusterManager, server, object_storage):
    '''The number of read requests that the server has sent to the object store.'''
    metrics = await manager.metrics.query(server.ip_addr)
    return metrics.get('scylla_object_storage_total_read_requests', {'type': object_storage.type.lower()}) or 0


def range_read_requests(object_storage):
    '''The number of requests to read a range (of up to 8 MiB) of an object:
    a single request on S3, while on GCS there is also a request for the
    object's metadata (see the FIXME in make_ranged_download_source()).'''
    return 1 if object_storage.type == 'S3' else 2


PLAINTEXT_MARKER = 'plaintext-marker:Xq7Hz2Lw9Kp4Nr8Vt3'


def marker_in(pages):
    '''Whether any of the pages contains PLAINTEXT_MARKER.'''
    return any(PLAINTEXT_MARKER.encode() in page for page in pages)


async def create_table(manager: ScyllaClusterManager, server, ks, table_options=''):
    '''Creates the table ks.test with some data, flushed to sstables, and
    returns the data as a dict. The data does not compress well, so its
    sstables are larger than the sstable reader's 128 KiB read buffer, but
    the whole table fits in one page of query results.'''
    cql = manager.get_cql()
    await cql.run_async(f'CREATE TABLE {ks}.test (p int PRIMARY KEY, v text) {table_options}')
    insert = cql.prepare(f'INSERT INTO {ks}.test (p, v) VALUES (?, ?)')
    rand = random.Random(1)
    data = {p: rand.randbytes(500).hex() for p in range(400)}
    # A value whose bytes can be looked for in the sstables, to check if they
    # hold it as plaintext.
    data[400] = PLAINTEXT_MARKER
    await asyncio.gather(*[cql.run_async(insert, [p, v]) for p, v in data.items()])
    await manager.api.flush_keyspace(server.ip_addr, ks)
    return data


def scan(cql, ks):
    return {row.p: row.v for row in cql.execute(f'SELECT p, v FROM {ks}.test')}


def cached_data_pages(cql):
    '''The (sstable, offset) of each cached page of a Data component.'''
    return [(row.sstable, row.offset) for row in
            cql.execute("SELECT sstable, offset FROM system_cache.pagecache WHERE component = 'Data' ALLOW FILTERING")]


async def test_scan_reads_each_sstable_in_one_request(manager: ScyllaClusterManager, object_storage):
    '''A scan which misses the page cache reads each sstable's Data from the
    object store in a single request (on S3), even though the sstable is
    larger than the sstable reader's read buffer.'''
    server = await manager.server_add(config=page_cache_config(object_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(object_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)

        requests_before = await read_requests(manager, server, object_storage)
        assert scan(cql, ks) == data
        requests = await read_requests(manager, server, object_storage) - requests_before
        # Check that each sstable's Data was read in one request (whose pages
        # are now in the cache). The scan also reads each sstable's index, to
        # find where the tablet's token range starts: these sstables' indexes
        # are a single page, which this first read of it brings to the cache.
        # The index is read through its file, which got the object's size when
        # the sstable was opened, so this is a single request also on GCS.
        sstables = {sstable for sstable, _ in cached_data_pages(cql)}
        assert len(sstables) >= 1
        index_requests = len(sstables)
        assert requests == len(sstables) * range_read_requests(object_storage) + index_requests


async def test_scan_populates_cache(manager: ScyllaClusterManager, object_storage):
    '''A scan caches the pages it reads, so that a second scan, even after the
    in-memory caches are dropped, sends no request to the object store.'''
    server = await manager.server_add(config=page_cache_config(object_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(object_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        assert cached_data_pages(cql)
        # Check that the cached Data holds the values as plaintext (a control
        # for test_encrypted_sstables, which checks the opposite).
        assert marker_in(row.page for row in cql.execute(
            "SELECT page FROM system_cache.pagecache WHERE component = 'Data' ALLOW FILTERING"))

        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, object_storage)
        assert scan(cql, ks) == data
        assert await read_requests(manager, server, object_storage) == requests_before


async def test_scan_populates_cache_uncompressed(manager: ScyllaClusterManager, s3_storage):
    '''Like test_scan_populates_cache, for a table without compression, whose
    Data component is cached in fixed-size pages rather than in compressed
    chunks.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks, "WITH compression = {'sstable_compression': ''}")
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        # Check that the pages are of the fixed size, 4 KiB, apart from the
        # last page of each sstable.
        offsets = [offset for _, offset in cached_data_pages(cql)]
        assert offsets and all(offset % 4096 == 0 for offset in offsets)

        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert scan(cql, ks) == data
        assert await read_requests(manager, server, s3_storage) == requests_before


async def test_point_read_populates_cache(manager: ScyllaClusterManager, s3_storage):
    '''A point read caches the pages it reads - only some of the pages of the
    sstable - so that reading the same row again sends no request to the
    object store.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)

        assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]
        point_read_pages = len(cached_data_pages(cql))
        assert point_read_pages > 0

        # Check that the same point read again, even after the in-memory
        # caches are dropped, reads both the index and the Data from the
        # cache, and sends no request to the object store.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]
        assert await read_requests(manager, server, s3_storage) == requests_before

        # Check that a scan, which reads all the pages, caches more.
        assert scan(cql, ks) == data
        assert len(cached_data_pages(cql)) > point_read_pages


async def test_cache_survives_restart(manager: ScyllaClusterManager, s3_storage):
    '''The cached pages survive a restart of the node: a scan after the restart
    sends no request to the object store.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data


        await manager.server_restart(server.server_id)
        cql, _ = await manager.get_ready_cql([server])
        requests_before = await read_requests(manager, server, s3_storage)
        assert scan(cql, ks) == data
        assert await read_requests(manager, server, s3_storage) == requests_before


async def test_no_cache_without_disk_cache_option(manager: ScyllaClusterManager, s3_storage):
    '''Reads of a keyspace without the 'disk_cache' option do not use the
    page cache, even though the cache is enabled.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage)) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        assert cached_data_pages(cql) == []


async def test_me_format(manager: ScyllaClusterManager, s3_storage):
    '''Repeated point reads and scans send no request to the object store, also
    for sstables of the older "me" format, whose index is the Index component
    rather than Partitions and Rows, and is read in different ways.'''
    server = await manager.server_add(config=page_cache_config(s3_storage) | {'sstable_format': 'me'})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]

        # Check that the same point read again sends no request.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]
        assert await read_requests(manager, server, s3_storage) == requests_before

        # Check that after a scan, a second scan sends no request.
        assert scan(cql, ks) == data
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert scan(cql, ks) == data
        assert await read_requests(manager, server, s3_storage) == requests_before


def encryption_config(tmp_path):
    '''Configuration which encrypts all user tables (encryption at rest).'''
    keys = tmp_path / 'system_keys'
    keys.mkdir()
    return {'system_key_directory': str(keys),
            'user_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}}


def object_bytes(object_storage, sstable, component):
    '''The content of an sstable component's object in the object store.'''
    obj = object_storage.get_resource().Object(object_storage.bucket_name, f'sstables/{sstable}/{component}.db')
    return obj.get()['Body'].read()


@pytest.mark.parametrize('compression', ['compressed', 'uncompressed'])
async def test_encrypted_sstables(manager: ScyllaClusterManager, s3_storage, tmp_path, compression):
    '''Encrypted sstables are read correctly through the page cache, which
    holds their Data and index exactly as stored in the object store, i.e.,
    encrypted - and needs no more requests to the object store than for
    sstables which are not encrypted. The Data of a compressed table is
    compressed before it is encrypted, so its compressed chunks are not
    aligned to the encryption's blocks.'''
    server = await manager.server_add(config=page_cache_config(s3_storage) | encryption_config(tmp_path))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        table_options = "WITH compression = {'sstable_compression': ''}" if compression == 'uncompressed' else ''
        data = await create_table(manager, server, ks, table_options)

        # Check that a scan which misses the cache returns the right data, and
        # reads each sstable's Data in a single request, and its index once,
        # as for sstables which are not encrypted.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert scan(cql, ks) == data
        requests = await read_requests(manager, server, s3_storage) - requests_before
        sstables = {sstable for sstable, _ in cached_data_pages(cql)}
        assert len(sstables) >= 1
        assert requests == 2 * len(sstables)

        # Check that a second scan, and point reads, are served from the cache.
        for read in [lambda: scan(cql, ks) == data,
                     lambda: cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]]:
            await manager.api.drop_sstable_caches(server.ip_addr)
            requests_before = await read_requests(manager, server, s3_storage)
            assert read()
            assert await read_requests(manager, server, s3_storage) == requests_before

        # Check that the cached pages, of both the Data and the index, are
        # exactly the bytes stored in the object store, i.e., encrypted.
        pages = list(cql.execute("SELECT sstable, component, offset, page FROM system_cache.pagecache"))
        assert {page.component for page in pages} >= {'Data', 'Partitions'}
        for page in pages:
            stored = object_bytes(s3_storage, page.sstable, page.component)
            assert page.page == stored[page.offset:page.offset + len(page.page)]
        # Check that the sstables are indeed encrypted: unlike in an sstable
        # which is not encrypted (see test_scan_populates_cache), neither the
        # stored Data nor the cache holds the values as plaintext.
        assert not marker_in(object_bytes(s3_storage, sstable, 'Data') for sstable in sstables)
        assert not marker_in(page.page for page in pages)


@pytest.mark.parametrize('encryption', ['plain', 'encrypted'])
async def test_large_compression_chunks(manager: ScyllaClusterManager, s3_storage, tmp_path, encryption):
    '''With compression chunks larger than the default 4 KiB (here 64 KiB),
    the pages of an sstable's Data - its compressed chunks - are larger, and
    of varying sizes. For an encrypted sstable the pages are still the
    encryption's 4 KiB blocks, which the chunks straddle. Either way, repeated
    reads are served from the cache, which holds exactly the bytes stored in
    the object store.'''
    config = page_cache_config(s3_storage)
    if encryption == 'encrypted':
        config |= encryption_config(tmp_path)
    server = await manager.server_add(config=config)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks,
                                  "WITH compression = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 64}")
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data

        # Check the sizes of the cached Data pages: the compressed 64 KiB
        # chunks, larger than 4 KiB - or, if encrypted, 4 KiB blocks.
        sizes = [len(row.page) for row in cql.execute(
            "SELECT page FROM system_cache.pagecache WHERE component = 'Data' ALLOW FILTERING")]
        if encryption == 'encrypted':
            assert max(sizes) == 4096
        else:
            assert max(sizes) > 4096

        # Check that a second scan, and point reads, are served from the cache.
        for read in [lambda: scan(cql, ks) == data,
                     lambda: cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]]:
            await manager.api.drop_sstable_caches(server.ip_addr)
            requests_before = await read_requests(manager, server, s3_storage)
            assert read()
            assert await read_requests(manager, server, s3_storage) == requests_before

        # Check that the cached pages are exactly the bytes stored in the
        # object store.
        for page in cql.execute("SELECT sstable, component, offset, page FROM system_cache.pagecache"):
            stored = object_bytes(s3_storage, page.sstable, page.component)
            assert page.page == stored[page.offset:page.offset + len(page.page)]


@pytest.mark.parametrize('encryption', ['plain', 'encrypted'])
async def test_point_read_then_scan(manager: ScyllaClusterManager, s3_storage, tmp_path, encryption):
    '''A point read which misses the cache needs one request for the sstable's
    index and one for its Data, and repeating it needs none. A following scan
    needs one request for each sstable's Data - even for the sstable whose
    pages the point read cached in its middle - and one for each sstable's
    index which is not yet cached. An encrypted sstable, whose pages are the
    encryption's 4 KiB blocks rather than its compressed chunks, needs exactly
    the same requests.'''
    config = page_cache_config(s3_storage)
    if encryption == 'encrypted':
        config |= encryption_config(tmp_path)
    server = await manager.server_add(config=config)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)

        # Check the requests of a point read which misses the cache, and of
        # the same point read again.
        for expected_requests in [2, 0]:
            await manager.api.drop_sstable_caches(server.ip_addr)
            requests_before = await read_requests(manager, server, s3_storage)
            assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17').one().v == data[17]
            assert await read_requests(manager, server, s3_storage) - requests_before == expected_requests

        # Check the requests of a scan, after the point read cached some of
        # the pages of one of the sstables, including its index.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert scan(cql, ks) == data
        requests = await read_requests(manager, server, s3_storage) - requests_before
        sstables = {sstable for sstable, _ in cached_data_pages(cql)}
        assert requests == len(sstables) + (len(sstables) - 1)


async def test_many_concurrent_reads(manager: ScyllaClusterManager, s3_storage):
    '''Many more concurrent reads than the reader concurrency semaphore admits
    (100 per shard) all complete. A read looks pages up in the cache while it
    holds its permit, so a lookup must not wait for admission on the same
    semaphore, behind reads which wait for it.'''
    server = await manager.server_add(config=page_cache_config(s3_storage), cmdline=['--smp', '1'])
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        # Without the row cache, every read reads the sstables.
        data = await create_table(manager, server, ks, "WITH caching = {'enabled': 'false'}")
        select = cql.prepare(f'SELECT v FROM {ks}.test WHERE p = ?')
        keys = [random.randrange(len(data)) for _ in range(500)]

        # Check that the reads complete both while the cache is cold - when
        # they miss, and cache pages - and when it is warm.
        for _ in range(2):
            results = await asyncio.gather(*[cql.run_async(select, [p]) for p in keys])
            assert [result[0].v for result in results] == [data[p] for p in keys]
