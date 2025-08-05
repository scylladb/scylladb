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
    await cql.run_async("CREATE KEYSPACE test_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

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