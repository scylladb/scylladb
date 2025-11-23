#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
import time
import pytest
import asyncio
import logging

from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature
from test.pylib.manager_client import ManagerClient, ScyllaVersionDescription
from test.cluster.test_alternator import alternator_config, get_alternator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def yaml_to_cmdline(config):
    cmdline = []
    for k, v in config.items():
        if isinstance(v, dict):
            v = ','.join([f'{kk}={vv}' for kk, vv in v.items()])
        cmdline.append(f'--{k.replace("_", "-")}')
        cmdline.append(str(v))
    return cmdline


@pytest.mark.asyncio
@pytest.mark.parametrize('cfg_source', ['yaml', 'cmdline'])
async def test_chunk_size_negative(manager: ManagerClient, cfg_source: str):
    config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'LZ4Compressor',
            'chunk_length_in_kb': -1
        }
    }
    expected_error = 'Invalid sstable_compression_user_table_options: Invalid negative or null for chunk_length_in_kb/chunk_length_kb'
    if cfg_source == 'yaml':
        await manager.server_add(config=config, expected_error=expected_error)
    else:
        await manager.server_add(cmdline=yaml_to_cmdline(config), expected_error=expected_error)


@pytest.mark.asyncio
@pytest.mark.parametrize('cfg_source', ['yaml', 'cmdline'])
async def test_chunk_size_beyond_max(manager: ManagerClient, cfg_source: str):
    config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'LZ4Compressor',
            'chunk_length_in_kb': 256
        }
    }
    expected_error = 'Invalid sstable_compression_user_table_options: chunk_length_in_kb/chunk_length_kb must be 128 or less.'
    if cfg_source == 'yaml':
        await manager.server_add(config=config, expected_error=expected_error)
    else:
        await manager.server_add(cmdline=yaml_to_cmdline(config), expected_error=expected_error)


@pytest.mark.asyncio
@pytest.mark.parametrize('cfg_source', ['yaml', 'cmdline'])
async def test_chunk_size_not_power_of_two(manager: ManagerClient, cfg_source: str):
    config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'LZ4Compressor',
            'chunk_length_in_kb': 3
        }
    }
    expected_error = 'Invalid sstable_compression_user_table_options: chunk_length_in_kb/chunk_length_kb must be a power of 2.'

    if cfg_source == 'yaml':
        await manager.server_add(config=config, expected_error=expected_error)
    else:
        await manager.server_add(cmdline=yaml_to_cmdline(config), expected_error=expected_error)


@pytest.mark.asyncio
@pytest.mark.parametrize('cfg_source', ['yaml', 'cmdline'])
async def test_crc_check_chance_out_of_bounds(manager: ManagerClient, cfg_source: str):
    config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'LZ4Compressor',
            'chunk_length_in_kb': 128,
            'crc_check_chance': 1.1
        }
    }
    expected_error = 'Invalid sstable_compression_user_table_options: crc_check_chance must be between 0.0 and 1.0.'
    if cfg_source == 'yaml':
        await manager.server_add(config=config, expected_error=expected_error)
    else:
        await manager.server_add(cmdline=yaml_to_cmdline(config), expected_error=expected_error)

@pytest.mark.asyncio
async def test_default_compression_on_upgrade(manager: ManagerClient, scylla_2025_1: ScyllaVersionDescription):
    """
    Check that the default SSTable compression algorithm is:
    * LZ4Compressor if SSTABLE_COMPRESSION_DICTS is disabled.
    * LZ4WithDictsCompressor if SSTABLE_COMPRESSION_DICTS is enabled.

    - Start a 2-node cluster running a version where dictionary compression is not supported (2025.1).
    - Create a table. Ensure that it uses the LZ4Compressor.
    - Upgrade one node.
    - Create a second table. Ensure that it still uses the LZ4Compressor.
    - Upgrade the second node.
    - Wait for SSTABLE_COMPRESSION_DICTS to be enabled.
    - Create a third table. Ensure that it uses the new LZ4WithDictsCompressor.
    """
    async def create_table_and_check_compression(cql, keyspace, table_name, expected_compression, context):
        """Helper to create a table and verify its compression algorithm."""
        logger.info(f"Creating table {table_name} ({context})")
        await cql.run_async(f"CREATE TABLE {keyspace}.{table_name} (pk int PRIMARY KEY, v int)")

        logger.info(f"Verifying that the default compression algorithm is {expected_compression}")
        result = await cql.run_async(f"SELECT compression FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{table_name}'")
        actual_compression = result[0].compression.get("sstable_compression")
        logger.info(f"Actual compression for {table_name}: {actual_compression}")

        assert actual_compression == expected_compression, \
            f"Expected {expected_compression} for {table_name} ({context}), got: {actual_compression}"

    new_exe = os.getenv("SCYLLA")
    assert new_exe

    logger.info("Starting servers with version 2025.1")
    servers = await manager.servers_add(2, version=scylla_2025_1)

    logger.info("Creating a test keyspace")
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    await create_table_and_check_compression(cql, "test_ks", "table_before_upgrade", "org.apache.cassandra.io.compress.LZ4Compressor", "before upgrade")

    logger.info("Upgrading server 0")
    await manager.server_change_version(servers[0].server_id, new_exe)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await create_table_and_check_compression(cql, "test_ks", "table_during_upgrade", "org.apache.cassandra.io.compress.LZ4Compressor", "during upgrade")

    logger.info("Upgrading server 1")
    await manager.server_change_version(servers[1].server_id, new_exe)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Waiting for SSTABLE_COMPRESSION_DICTS cluster feature to be enabled on all nodes")
    await asyncio.gather(*(wait_for_feature("SSTABLE_COMPRESSION_DICTS", cql, host, time.time() + 60) for host in hosts))

    await create_table_and_check_compression(cql, "test_ks", "table_after_upgrade", "LZ4WithDictsCompressor", "after upgrade and feature enabled")


@pytest.mark.xfail(reason='issue #26914')
@pytest.mark.asyncio
async def test_alternator_tables_respect_compression_config(manager: ManagerClient):
    """
    Check that the default compression settings for all Alternator tables (base
    tables and auxiliary tables for GSIs, LSIs and Streams) are taken from the
    `sstable_compression_user_table_options` config option.

    A reproducer for #26914 - all Alternator tables have their default compression
    algorithm hardcoded to LZ4Compressor.
    """
    # Start Scylla with custom compression options
    compression_config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'ZstdCompressor',
            'chunk_length_in_kb': 64,
        }
    }
    server = await manager.server_add(config=compression_config | alternator_config)

    # Connect to Alternator and CQL
    alternator = get_alternator(server.ip_addr)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    # Create a table with GSI, LSI and Streams
    table = alternator.create_table(TableName='base',
        BillingMode='PAY_PER_REQUEST',
        Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}],
        KeySchema=[
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi_name',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi_name',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        StreamSpecification={
            'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'
        }
    )

    try:
        def check_compression(result):
            assert len(result) == 1
            compression = result[0].compression
            assert compression.get("sstable_compression") == "org.apache.cassandra.io.compress.ZstdCompressor"
            assert int(compression.get("chunk_length_in_kb", 0)) == 64

        ks = f"alternator_{table.name}"
        base_table = table.name
        gsi_table = f"{table.name}:gsi_name"
        lsi_table = f"{table.name}!:lsi_name"
        cdc_table = f"{table.name}_scylla_cdc_log"

        # Base table
        result = await cql.run_async(f"SELECT compression FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{base_table}'")
        check_compression(result)
        # GSI table
        result = await cql.run_async(f"SELECT compression FROM system_schema.views WHERE keyspace_name = '{ks}' AND view_name = '{gsi_table}'")
        check_compression(result)
        # LSI table
        result = await cql.run_async(f"SELECT compression FROM system_schema.views WHERE keyspace_name = '{ks}' AND view_name = '{lsi_table}'")
        check_compression(result)
        # CDC log table
        result = await cql.run_async(f"SELECT compression FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cdc_table}'")
        check_compression(result)
    finally:
        table.delete()


@pytest.mark.asyncio
async def test_cql_base_tables_respect_compression_config(manager: ManagerClient):
    """
    Check that the default compression settings for CQL base tables are taken
    from the `sstable_compression_user_table_options` config option.
    """
    compression_config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'ZstdCompressor',
            'chunk_length_in_kb': 64,
        },
    }
    server = await manager.server_add(config=compression_config)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    ks = f"cql_aux_test_{int(time.time())}"
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE {ks}.base (pk int PRIMARY KEY, v int)")

    try:
        result = await cql.run_async(f"SELECT compression FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'base'")
        assert len(result) == 1
        compression = result[0].compression
        assert compression.get("sstable_compression") == f"org.apache.cassandra.io.compress.ZstdCompressor"
        assert int(compression.get("chunk_length_in_kb", 0)) == 64
    finally:
        await cql.run_async(f"DROP KEYSPACE {ks}")


@pytest.mark.xfail(reason='issue #26914')
@pytest.mark.asyncio
async def test_cql_aux_tables_respect_compression_config(manager: ManagerClient):
    """
    Check that the default compression settings for CQL auxiliary tables
    (materialized views, secondary indexes and CDC logs) are taken from the
    `sstable_compression_user_table_options` config option.

    To prove that auxiliary tables don't inherit compression from the base table,
    we use a base table compressor that is different from the one set in
    `sstable_compression_user_table_options`.

    A reproducer for #26914 - all auxiliary tables have their default compression
    algorithm hardcoded to LZ4Compressor.

    TODO: Replace this test with a config-agnostic cqlpy test if aux tables are
    modified to inherit compression from the base table (issue #20388).
    """
    compression_config = {
        'sstable_compression_user_table_options': {
            'sstable_compression': 'ZstdCompressor',
            'chunk_length_in_kb': 64,
        },
    }
    server = await manager.server_add(config=compression_config)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    ks = f"cql_aux_test_{int(time.time())}"
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")

    await cql.run_async(f"CREATE TABLE {ks}.base (pk int PRIMARY KEY, v int) WITH compression = {{ 'sstable_compression': 'DeflateCompressor' }}")
    await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.base WHERE pk IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk)")
    await cql.run_async(f"CREATE INDEX ON {ks}.base(v)")
    await cql.run_async(f"ALTER TABLE {ks}.base WITH cdc = {{'enabled': true}}")

    try:
        def check_compression(result):
            assert len(result) == 1
            compression = result[0].compression
            assert compression.get("sstable_compression") == f"org.apache.cassandra.io.compress.ZstdCompressor"
            assert int(compression.get("chunk_length_in_kb", 0)) == 64

        # Materialized view
        result = await cql.run_async(f"SELECT compression FROM system_schema.views WHERE keyspace_name = '{ks}' AND view_name = 'mv'")
        check_compression(result)

        # Secondary index
        result = await cql.run_async(f"SELECT compression FROM system_schema.views WHERE keyspace_name = '{ks}' AND view_name = 'base_v_idx_index'")
        check_compression(result)

        # CDC log table
        result = await cql.run_async(f"SELECT compression FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'base_scylla_cdc_log'")
        check_compression(result)
    finally:
        await cql.run_async(f"DROP KEYSPACE {ks}")