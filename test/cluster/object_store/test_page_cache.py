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
import time
import uuid

import pytest
from cassandra.protocol import ConfigurationException, InvalidRequest

from test.pylib.object_storage import format_tuples
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts


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


def cached_pages(cql, host=None):
    '''The (sstable, component, offset) of each cached page - of the given
    node, or of the node which the driver chooses.'''
    return sorted((row.sstable, row.component, row.offset) for row in
                  cql.execute('SELECT sstable, component, offset FROM system_cache.pagecache', host=host))


def page_hits(cql):
    '''The time each cached page was last hit - or None, if it wasn't - by
    its (sstable, component, offset).'''
    return {(row.sstable, row.component, row.offset): row.last_hit for row in
            cql.execute('SELECT sstable, component, offset, last_hit FROM system_cache.pagecache_hits')}


def page_hits_with_shard(cql):
    '''The (shard, sstable, component, offset, last_hit) of each hit page.'''
    return sorted((row.shard, row.sstable, row.component, row.offset, row.last_hit) for row in
                  cql.execute('SELECT shard, sstable, component, offset, last_hit FROM system_cache.pagecache_hits'))


def cached_pages_with_shard(cql):
    '''The (shard, sstable, component, offset) of each cached page.'''
    return sorted((row.shard, row.sstable, row.component, row.offset) for row in
                  cql.execute('SELECT shard, sstable, component, offset FROM system_cache.pagecache'))


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


async def test_restart_evicts_orphan_pages(manager: ScyllaClusterManager, s3_storage):
    '''On startup, the pages of sstables which no longer exist - orphans,
    which a crash for example could leave behind - are evicted from the page
    cache, while the pages of the existing sstables stay. The test adds
    orphan pages to the cache table directly: of an sstable which doesn't
    exist, and of a shard which doesn't exist.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        pages = cached_pages(cql)
        assert pages

        orphan = uuid.uuid4()
        insert = cql.prepare('INSERT INTO system_cache.pagecache (shard, sstable, component, offset, page) VALUES (?, ?, ?, ?, ?)')
        cql.execute(insert, [0, orphan, 'Data', 0, b'orphan'])
        cql.execute(insert, [1000, pages[0][0], 'Data', 0, b'orphan'])
        assert len(cached_pages(cql)) == len(pages) + 2
        # Add also an orphan hit, of an sstable which doesn't exist. The pages
        # which the scan cached have rows in the hits table, without hits.
        cql.execute('INSERT INTO system_cache.pagecache_hits (shard, sstable, component, offset, last_hit) '
                    'VALUES (0, %s, %s, 0, toTimestamp(now()))', [orphan, 'Data'])
        assert page_hits(cql)

        await manager.server_restart(server.server_id)
        cql, _ = await manager.get_ready_cql([server])
        assert cached_pages(cql) == pages
        assert page_hits(cql) == {page: None for page in pages}
        assert scan(cql, ks) == data


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


async def test_bypass_cache(manager: ScyllaClusterManager, s3_storage):
    '''A read with BYPASS CACHE doesn't populate the page cache - neither with
    the Data nor with the index - but does read the pages which are already
    cached - both for a scan and for a point read.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)

        # Check that a scan with BYPASS CACHE caches nothing.
        assert {row.p: row.v for row in cql.execute(f'SELECT p, v FROM {ks}.test BYPASS CACHE')} == data
        assert cached_pages(cql) == []

        # A scan without BYPASS CACHE caches all of the Data, and the index.
        # Check that a scan with BYPASS CACHE then reads everything from the
        # cache, and sends no request to the object store.
        assert scan(cql, ks) == data
        pages = cached_pages(cql)
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert {row.p: row.v for row in cql.execute(f'SELECT p, v FROM {ks}.test BYPASS CACHE')} == data
        assert await read_requests(manager, server, s3_storage) == requests_before
        assert cached_pages(cql) == pages

        # Check the same for a point read with BYPASS CACHE.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17 BYPASS CACHE').one().v == data[17]
        assert await read_requests(manager, server, s3_storage) == requests_before

        # Check that a point read with BYPASS CACHE of a partition whose
        # pages are not cached caches nothing either.
        await cql.run_async(f"INSERT INTO {ks}.test (p, v) VALUES (1000, 'new')")
        await manager.api.flush_keyspace(server.ip_addr, ks)
        assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 1000 BYPASS CACHE').one().v == 'new'
        assert cached_pages(cql) == pages


async def test_scan_then_point_reads(manager: ScyllaClusterManager, s3_storage):
    '''After a scan cached the whole table, a point read of each of its
    partitions sends no request to the object store: the pages which the
    scan cached are the same pages which the point reads look for.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data

        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        select = cql.prepare(f'SELECT v FROM {ks}.test WHERE p = ?')
        for p, v in data.items():
            assert cql.execute(select, [p]).one().v == v
        assert await read_requests(manager, server, s3_storage) == requests_before


async def create_wide_partition(manager: ScyllaClusterManager, server, ks, rows=400):
    '''Creates the table ks.test with a single wide partition, p = 0, flushed
    to sstables, and returns its rows as a list of (c, v). Each row has 1000
    bytes of value, which doesn't compress well. With the default number of
    rows, the partition is larger than the sstable reader's 128 KiB read
    buffer, and has a promoted index, so that reading one row looks it up in
    the Rows component.'''
    cql = manager.get_cql()
    await cql.run_async(f'CREATE TABLE {ks}.test (p int, c int, v text, PRIMARY KEY (p, c))')
    insert = cql.prepare(f'INSERT INTO {ks}.test (p, c, v) VALUES (0, ?, ?)')
    rand = random.Random(1)
    rows = [(c, rand.randbytes(500).hex()) for c in range(rows)]
    await asyncio.gather(*[cql.run_async(insert, [c, v]) for c, v in rows])
    await manager.api.flush_keyspace(server.ip_addr, ks)
    return rows


def read_partition(cql, ks):
    return [(row.c, row.v) for row in cql.execute(f'SELECT c, v FROM {ks}.test WHERE p = 0')]


def read_rows(cql, ks, rows):
    '''Reads each of the given rows of the wide partition separately, and
    checks its value.'''
    select = cql.prepare(f'SELECT v FROM {ks}.test WHERE p = 0 AND c = ?')
    for c, v in rows:
        assert cql.execute(select, [c]).one().v == v


async def test_wide_partition_read_then_row_reads(manager: ScyllaClusterManager, s3_storage):
    '''A read of a whole wide partition reads its Data in a single request,
    and caches it in many pages. Reading each of its rows separately then
    finds everything it needs in the cache.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        rows = await create_wide_partition(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert read_partition(cql, ks) == rows
        # Check that the read needed one request for the partition's Data, and
        # one for each of the index components, Partitions and Rows (each of
        # them a single page, in this small sstable).
        assert await read_requests(manager, server, s3_storage) - requests_before == 3
        assert len(cached_data_pages(cql)) > 1

        # Check that reading each row separately sends no request.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        read_rows(cql, ks, rows)
        assert await read_requests(manager, server, s3_storage) == requests_before


async def test_wide_partition_row_reads_then_read(manager: ScyllaClusterManager, s3_storage):
    '''Reading each row of a wide partition separately caches the pages
    which reading the whole partition needs: it then sends no request to
    the object store.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        rows = await create_wide_partition(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        read_rows(cql, ks, rows)

        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert read_partition(cql, ks) == rows
        assert await read_requests(manager, server, s3_storage) == requests_before


async def test_wide_partition_some_row_reads_then_read(manager: ScyllaClusterManager, s3_storage):
    '''Reading a whole wide partition, after only some of its rows were read
    - and cached - returns all its rows correctly, and caches the rest of the
    partition, so reading it again sends no request to the object store.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        rows = await create_wide_partition(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        read_rows(cql, ks, [rows[0], rows[200]])

        await manager.api.drop_sstable_caches(server.ip_addr)
        assert read_partition(cql, ks) == rows

        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert read_partition(cql, ks) == rows
        assert await read_requests(manager, server, s3_storage) == requests_before


async def test_wide_partition_rows_far_apart(manager: ScyllaClusterManager, s3_storage):
    '''A read of two rows far apart in a wide partition which isn't cached
    doesn't download - or cache - the megabytes of the partition between
    them. The reader reads the first row, and then skips to the second, and
    a skip that long closes the request to the object store rather than read
    through the pages it skips.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        # About 3 MB of rows.
        rows = await create_wide_partition(manager, server, ks, rows=3000)
        await manager.api.drop_sstable_caches(server.ip_addr)
        result = cql.execute(f'SELECT c, v FROM {ks}.test WHERE p = 0 AND c IN (0, 2999)')
        assert [(row.c, row.v) for row in result] == [rows[0], rows[-1]]
        # Check that the read cached only a few pages of the partition's Data
        # (each page is a compressed chunk, of at most 4 KiB), not the
        # hundreds of pages between the rows.
        assert 0 < len(cached_data_pages(cql)) <= 32


async def test_reversed_read(manager: ScyllaClusterManager, s3_storage):
    '''A reversed read of a partition, which reads the Data in a different
    way than a forward read, also reads it through the page cache, which it
    populates - unless it is a read with BYPASS CACHE.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        reversed_data = list(reversed(await create_wide_partition(manager, server, ks)))

        def reversed_read(options=''):
            return [(row.c, row.v) for row in cql.execute(f'SELECT c, v FROM {ks}.test WHERE p = 0 ORDER BY c DESC {options}')]

        # Check that a reversed read with BYPASS CACHE caches nothing.
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert reversed_read('BYPASS CACHE') == reversed_data
        assert cached_pages(cql) == []

        # Check that a reversed read caches the Data, so that repeating it
        # sends no request to the object store.
        assert reversed_read() == reversed_data
        assert cached_data_pages(cql)
        pages = cached_pages(cql)
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert reversed_read() == reversed_data
        assert await read_requests(manager, server, s3_storage) == requests_before

        # Check that a reversed read with BYPASS CACHE reads the cached Data
        # too, and changes nothing in the cache.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert reversed_read('BYPASS CACHE') == reversed_data
        assert await read_requests(manager, server, s3_storage) == requests_before
        assert cached_pages(cql) == pages


async def test_compaction_does_not_populate_cache(manager: ScyllaClusterManager, s3_storage):
    '''Compaction, which reads whole sstables once, reads them from the object
    store without populating the page cache. The sstable it writes - with the
    rows which a later sstable overwrote or deleted merged correctly - is
    cached when the user reads it.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        # Write a second sstable, overwriting some of the rows, and deleting
        # others.
        update = cql.prepare(f'UPDATE {ks}.test SET v = ? WHERE p = ?')
        for p in range(0, 400, 10):
            data[p] = f'updated {p}'
            await cql.run_async(update, [data[p], p])
        delete = cql.prepare(f'DELETE FROM {ks}.test WHERE p = ?')
        for p in range(5, 400, 10):
            del data[p]
            await cql.run_async(delete, [p])
        await manager.api.flush_keyspace(server.ip_addr, ks)

        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        await manager.api.keyspace_compaction(server.ip_addr, ks)
        assert await read_requests(manager, server, s3_storage) > requests_before
        assert cached_pages(cql) == []

        assert scan(cql, ks) == data
        assert cached_data_pages(cql)


async def test_compaction_reads_cache(manager: ScyllaClusterManager, s3_storage):
    '''Compaction, although it doesn't populate the page cache, reads the
    pages which are already cached: compacting sstables which a scan cached
    needs fewer requests to the object store than compacting the same
    sstables when they are not cached.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()

    async def compaction_requests(scan_first):
        async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
            data = await create_table(manager, server, ks)
            # Write a second sstable, overwriting some of the rows.
            update = cql.prepare(f'UPDATE {ks}.test SET v = ? WHERE p = ?')
            for p in range(0, 400, 10):
                data[p] = f'updated {p}'
                await cql.run_async(update, [data[p], p])
            await manager.api.flush_keyspace(server.ip_addr, ks)
            await manager.api.drop_sstable_caches(server.ip_addr)
            if scan_first:
                assert scan(cql, ks) == data
            requests_before = await read_requests(manager, server, s3_storage)
            await manager.api.keyspace_compaction(server.ip_addr, ks)
            requests = await read_requests(manager, server, s3_storage) - requests_before
            assert scan(cql, ks) == data
            return requests

    # Compaction also reads things which are never cached, such as the small
    # Digest component, and the sstables which it wrote itself (when it
    # compacts in several rounds), so it does send some requests even when
    # its input is cached. Check that it sends fewer.
    uncached_requests = await compaction_requests(scan_first=False)
    cached_requests = await compaction_requests(scan_first=True)
    assert cached_requests < uncached_requests


async def test_opening_sstables_does_not_populate_cache(manager: ScyllaClusterManager, s3_storage):
    '''Opening an sstable, which reads the footer of its Partitions component,
    doesn't populate the page cache: after a restart, which opens all the
    sstables again, nothing is cached.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        assert cached_pages(cql) == []

        await manager.server_restart(server.server_id)
        cql, _ = await manager.get_ready_cql([server])
        assert cached_pages(cql) == []
        assert scan(cql, ks) == data


def cached_sstables(cql):
    '''The sstables which have cached pages.'''
    return {sstable for sstable, _, _ in cached_pages(cql)}


async def wait_for_eviction(cql, sstables):
    '''Waits until none of the given sstables has cached pages, or hits. An
    sstable is deleted - and its pages evicted - only after its last user is
    done with it, which may happen some time after the operation which
    deleted it.'''
    async def evicted():
        hit_sstables = {sstable for sstable, _, _ in page_hits(cql)}
        return True if not (cached_sstables(cql) | hit_sstables) & sstables else None
    await wait_for(evicted, time.time() + 60)


async def test_compaction_evicts_compacted_sstables(manager: ScyllaClusterManager, s3_storage):
    '''The cached pages of the sstables which compaction replaces, and
    deletes, are evicted from the page cache.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        update = cql.prepare(f'UPDATE {ks}.test SET v = ? WHERE p = ?')
        for p in range(0, 400, 10):
            data[p] = f'updated {p}'
            await cql.run_async(update, [data[p], p])
        await manager.api.flush_keyspace(server.ip_addr, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        compacted = cached_sstables(cql)
        assert compacted
        # Scan again, to record the hits of the cached pages.
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        assert {sstable for sstable, _, _ in page_hits(cql)} == compacted

        await manager.api.keyspace_compaction(server.ip_addr, ks)
        await wait_for_eviction(cql, compacted)
        assert scan(cql, ks) == data


async def test_drop_table_evicts_its_sstables(manager: ScyllaClusterManager, s3_storage):
    '''Dropping a table evicts the cached pages of all its sstables from the
    page cache - but not the cached pages of another table.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks1, \
               new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks2:
        data1 = await create_table(manager, server, ks1)
        data2 = await create_table(manager, server, ks2)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks1) == data1
        sstables1 = cached_sstables(cql)
        assert scan(cql, ks2) == data2
        pages2 = [page for page in cached_pages(cql) if page[0] not in sstables1]
        assert sstables1 and pages2

        await cql.run_async(f'DROP TABLE {ks1}.test')
        await wait_for_eviction(cql, sstables1)
        # Check that the pages of the other table were not evicted.
        assert cached_pages(cql) == pages2


async def test_truncate_evicts_sstables(manager: ScyllaClusterManager, s3_storage):
    '''Truncating a table evicts the cached pages of all its sstables from
    the page cache.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        sstables = cached_sstables(cql)
        assert sstables

        await cql.run_async(f'TRUNCATE TABLE {ks}.test')
        await wait_for_eviction(cql, sstables)
        assert scan(cql, ks) == {}


async def test_intranode_migration_copies_pages(manager: ScyllaClusterManager, s3_storage):
    '''When a tablet moves between shards of the same node, its sstables keep
    their objects, and their sstable_id, so the pages which the old shard
    cached are still useful: they are copied to the new shard's cache - with
    the times they were last hit - and evicted from the old shard's cache.'''
    server = await manager.server_add(config=page_cache_config(s3_storage), cmdline=['--smp', '2'])
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks, "WITH tablets = {'min_tablet_count': 1}")
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        host, src_shard = await get_tablet_replica(manager, server, ks, 'test', 0)
        pages = cached_pages_with_shard(cql)
        assert pages and all(shard == src_shard for shard, _, _, _ in pages)
        # Scan again, to record the hits of the cached pages.
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert scan(cql, ks) == data
        hits = page_hits_with_shard(cql)
        assert hits

        dst_shard = 1 - src_shard
        await manager.api.move_tablet(server.ip_addr, ks, 'test', host, src_shard, host, dst_shard, 0)
        # Check that all the pages moved to the new shard. The old sstables,
        # and their pages on the old shard, are deleted only after their last
        # reader is done, so wait for it.
        moved_pages = sorted((dst_shard, sstable, component, offset) for _, sstable, component, offset in pages)
        async def pages_moved():
            return True if cached_pages_with_shard(cql) == moved_pages else None
        await wait_for(pages_moved, time.time() + 60)
        # Check that the hits moved too.
        assert page_hits_with_shard(cql) == sorted((dst_shard, sstable, component, offset, last_hit)
                                                   for _, sstable, component, offset, last_hit in hits)

        # Check that a scan, on the new shard, reads everything from the cache.
        await manager.api.drop_sstable_caches(server.ip_addr)
        requests_before = await read_requests(manager, server, s3_storage)
        assert scan(cql, ks) == data
        assert await read_requests(manager, server, s3_storage) == requests_before


async def test_migration_to_another_node_evicts_pages(manager: ScyllaClusterManager, s3_storage):
    '''When a tablet moves to another node, the old node evicts the pages
    which it cached for the tablet's sstables - although the new node shares
    their objects. The new node's cache starts empty, and its reads populate
    it.'''
    servers = [await manager.server_add(config=page_cache_config(s3_storage)) for _ in range(2)]
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, servers[0], ks, "WITH tablets = {'min_tablet_count': 1}")
        await manager.api.flush_keyspace(servers[1].ip_addr, ks)
        src_host_id, src_shard = await get_tablet_replica(manager, servers[0], ks, 'test', 0)
        host_ids = [await manager.get_host_id(server.server_id) for server in servers]
        src = host_ids.index(str(src_host_id))
        dst = 1 - src
        await manager.api.drop_sstable_caches(servers[src].ip_addr)
        assert scan(cql, ks) == data
        assert cached_pages(cql, hosts[src])
        assert cached_pages(cql, hosts[dst]) == []

        await manager.api.move_tablet(servers[0].ip_addr, ks, 'test', src_host_id, src_shard, host_ids[dst], 0, 0)
        # The old node deletes its sstables, and evicts their pages, only after
        # their last reader is done, so wait for it.
        async def pages_evicted():
            return True if cached_pages(cql, hosts[src]) == [] else None
        await wait_for(pages_evicted, time.time() + 60)

        # Check that the new node's cache is empty, until it reads the table.
        assert cached_pages(cql, hosts[dst]) == []
        await manager.api.drop_sstable_caches(servers[dst].ip_addr)
        assert scan(cql, ks) == data
        assert cached_pages(cql, hosts[dst])


async def test_hits(manager: ScyllaClusterManager, s3_storage):
    '''The page cache records when each cached page was last hit - read from
    the cache by a read which may populate it. Caching a page isn't a hit, so
    a page which was read only once looks old. A read with BYPASS CACHE reads
    the cached pages, but doesn't record their hits: a scan with BYPASS CACHE
    would otherwise make all of a table's pages look recently used.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)

        def point_read(options=''):
            assert cql.execute(f'SELECT v FROM {ks}.test WHERE p = 17 {options}').one().v == data[17]

        # Check that a point read which caches its pages records no hits:
        # each cached page has a row in the hits table, without a hit.
        await manager.api.drop_sstable_caches(server.ip_addr)
        point_read()
        pages = cached_pages(cql)
        assert pages
        assert page_hits(cql) == {page: None for page in pages}

        # Check that reading them again records a hit of each of them.
        await manager.api.drop_sstable_caches(server.ip_addr)
        point_read()
        first_hits = page_hits(cql)
        assert sorted(first_hits) == pages

        # Check that the next read records later hits. The clock has
        # millisecond resolution, so wait for it to advance.
        await asyncio.sleep(0.002)
        await manager.api.drop_sstable_caches(server.ip_addr)
        point_read()
        second_hits = page_hits(cql)
        assert sorted(second_hits) == pages
        assert all(second_hits[page] > first_hits[page] for page in pages)

        # Check that a read with BYPASS CACHE records no hits.
        await asyncio.sleep(0.002)
        await manager.api.drop_sstable_caches(server.ip_addr)
        point_read('BYPASS CACHE')
        assert page_hits(cql) == second_hits


async def test_lwt_populates_cache(manager: ScyllaClusterManager, s3_storage):
    '''The read of a conditional update (LWT) is a user read, which reads
    through the page cache, and populates it.'''
    server = await manager.server_add(config=page_cache_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_options(s3_storage, disk_cache='pages')) as ks:
        data = await create_table(manager, server, ks)
        await manager.api.drop_sstable_caches(server.ip_addr)
        assert cql.execute(f"UPDATE {ks}.test SET v = 'new' WHERE p = 17 IF v = '{data[17]}'").one().applied
        assert cached_data_pages(cql)


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
