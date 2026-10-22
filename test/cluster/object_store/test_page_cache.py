#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the disk page cache of object-storage keyspaces: a keyspace
# created with STORAGE = {..., 'disk_cache': 'pages'} keeps all of its
# sstables on the object store, but caches byte ranges read from it on the
# node's disk.

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
