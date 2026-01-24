#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
from cassandra.protocol import ConfigurationException
import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_property(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t_enabled (pk int PRIMARY KEY, v int) WITH kv_storage = true")
        await cql.run_async(f"CREATE TABLE {ks}.t_disabled (pk int PRIMARY KEY, v int) WITH kv_storage = false")

        desc = await cql.run_async(f"DESCRIBE TABLE {ks}.t_enabled")
        logger.info(f"Table t_enabled description:\n{desc}")
        assert "kv_storage = true" in desc[0].create_statement

        desc = await cql.run_async(f"DESCRIBE TABLE {ks}.t_disabled")
        logger.info(f"Table t_disabled description:\n{desc}")
        assert "kv_storage = false" in desc[0].create_statement

        with pytest.raises(ConfigurationException, match="The property 'kv_storage' cannot be used with tables that have clustering columns"):
            await cql.run_async(f"CREATE TABLE {ks}.t_enabled (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH kv_storage = true")

@pytest.mark.asyncio
async def test_basic_write_and_read(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:

        # test int value

        await cql.run_async(f"CREATE TABLE {ks}.test_int (pk int PRIMARY KEY, v int) WITH kv_storage = true")

        await cql.run_async(f"INSERT INTO {ks}.test_int (pk, v) VALUES (1, 100)")
        await cql.run_async(f"INSERT INTO {ks}.test_int (pk, v) VALUES (2, 150)")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == 100
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 2")
        assert rows[0].pk == 2
        assert rows[0].v == 150

        await cql.run_async(f"INSERT INTO {ks}.test_int (pk, v) VALUES (1, 200)")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == 200
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 2")
        assert rows[0].pk == 2
        assert rows[0].v == 150

        # test frozen map value

        await cql.run_async(f"CREATE TABLE {ks}.test_map (pk int PRIMARY KEY, v frozen<map<text, text>>) WITH kv_storage = true")

        await cql.run_async(f"INSERT INTO {ks}.test_map (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana'}})")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_map WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == {'a': 'apple', 'b': 'banana'}

        await cql.run_async(f"INSERT INTO {ks}.test_map (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana', 'c': 'cherry'}})")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_map WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == {'a': 'apple', 'b': 'banana', 'c': 'cherry'}

@pytest.mark.asyncio
async def test_parallel_writes(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH kv_storage = true")

        # write to different keys in parallel
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i+1})") for i in range(100)])

        # validate
        for i in range(100):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].pk == i
            assert rows[0].v == i + 1

@pytest.mark.asyncio
async def test_overwrites(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH kv_storage = true")

        # write to a single key many times sequentially
        pk = 0
        for i in range(100):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {i})")

        # validate we get the last value
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
        assert rows[0].pk == pk
        assert rows[0].v == 99

@pytest.mark.asyncio
async def test_parallel_big_writes(manager: ManagerClient):
    """
    Perform multiple writes in parallel with large values and validate to test segment switching.
    """
    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        # Create a large value of approximately 100KB, close to segment size
        large_value = 'x' * (100 * 1024)
        num_writes = 8

        # Perform parallel writes with large values
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{i}-{large_value}')") for i in range(num_writes)])

        # Validate that all writes succeeded
        for i in range(num_writes):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].pk == i
            assert rows[0].v == f"{i}-{large_value}"
