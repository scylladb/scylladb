#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging

from test.pylib.manager_client import ManagerClient

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
async def test_dict_compression_not_allowed(manager: ManagerClient, cfg_source: str):
    config = {
        'sstable_compression_dictionaries_allow_in_ddl': False,
        'sstable_compression_user_table_options': {
            'sstable_compression': 'ZstdWithDictsCompressor',
            'chunk_length_in_kb': 4,
            'compression_level': 10
        }
    }
    expected_error = 'Invalid sstable_compression_user_table_options: sstable_compression ZstdWithDictsCompressor has been disabled by `sstable_compression_dictionaries_allow_in_ddl: false`'

    if cfg_source == 'yaml':
        await manager.server_add(config=config, expected_error=expected_error)
    else:
        await manager.server_add(cmdline=yaml_to_cmdline(config), expected_error=expected_error)


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