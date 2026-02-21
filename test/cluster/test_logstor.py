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

@pytest.mark.asyncio
async def test_recovery_basic(manager: ManagerClient):
    """
    Test that logstor data persists across server restarts.

    This test:
    1. Writes initial data to several keys with large values (~40KB each) to fill multiple segments
    2. Overwrites some keys with new large values
    3. Stops the server
    4. Starts the server again
    5. Verifies all data is correctly recovered
    6. Performs additional writes and reads to verify system continues functioning
    """
    cmdline = ['--logger-log-level', 'logstor=trace']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        # Create large values (~40KB each) to fill multiple segments
        value_size = 40 * 1024

        initial_data = {}
        for pk in [1, 2, 3, 4, 5]:
            value = f"initial_{pk}_" + ('x' * (value_size - 20))
            initial_data[pk] = value
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{value}')")

        # overwrite some keys with new large values
        overwrites = {}
        for pk in [2, 4]:
            value = f"updated_{pk}_" + ('y' * (value_size - 20))
            overwrites[pk] = value
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{value}')")

        # Expected final state combines initial data with overwrites
        expected_data = {**initial_data, **overwrites}

        # Verify data before restart
        for pk, expected_v in expected_data.items():
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
            assert len(rows) == 1
            assert rows[0].pk == pk
            assert rows[0].v == expected_v

        # restart the server
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        cql, _ = await manager.get_ready_cql(servers)

        # verify data after restart
        for pk, expected_v in expected_data.items():
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
            assert len(rows) == 1, f"Key {pk} not found after recovery"
            assert rows[0].pk == pk, f"Key {pk} has wrong pk value"
            assert rows[0].v == expected_v, f"Key {pk} has wrong value: length expected {len(expected_v)}, got {len(rows[0].v)}"

        # perform additional writes after recovery
        new_writes = {}
        for pk in [6, 7, 8]:
            value = f"post_recovery_{pk}_" + ('z' * (value_size - 30))
            new_writes[pk] = value
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{value}')")

        # also overwrite one of the recovered keys
        pk = 1
        value = f"post_recovery_overwrite_{pk}_" + ('w' * (value_size - 40))
        new_writes[pk] = value
        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{value}')")

        # Update expected data with new writes
        expected_data.update(new_writes)

        # Verify all data including new writes
        for pk, expected_v in expected_data.items():
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
            assert len(rows) == 1, f"Key {pk} not found after additional writes"
            assert rows[0].pk == pk, f"Key {pk} has wrong pk value"
            assert rows[0].v == expected_v, f"Key {pk} has wrong value after additional writes"

@pytest.mark.asyncio
async def test_recovery_with_segment_reuse(manager: ManagerClient):
    """
    Test recovery after segments have been compacted and reused.

    This test:
    1. Writes to 10 keys multiple times with overwrites
    2. Fills the disk approximately twice to trigger compaction and segment reuse
    3. Tracks the last written value for each key
    4. Stops and restarts the server
    5. Verifies all last values are correctly recovered
    """
    disk_size_mb = 4
    file_size_mb = 1
    value_size = 50 * 1024
    num_keys = 10

    cmdline = ['--logger-log-level', 'logstor=trace', '--smp=1']
    cfg = {
        'enable_kv_storage': True,
        'kv_storage_disk_size_in_mb': disk_size_mb,
        'kv_storage_file_size_in_mb': file_size_mb,
        'experimental_features': ['kv-storage']
    }
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        # Track the last value written to each key
        last_values = {}

        # Calculate how many writes needed to fill disk twice
        disk_size_bytes = disk_size_mb * 1024 * 1024
        writes_to_fill_disk = disk_size_bytes // (value_size + 100)
        total_writes = 2 * writes_to_fill_disk

        # Write with overwrites to fill disk twice
        for i in range(total_writes):
            pk = i % num_keys  # Rotate through the 10 keys
            value = f"value_{i}_" + ('x' * (value_size - 20))
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{value}')")
            last_values[pk] = value

        # Verify data before restart
        for pk, expected_v in last_values.items():
            rows = await cql.run_async(f"SELECT v FROM {ks}.test WHERE pk = {pk}")
            assert len(rows) == 1
            assert rows[0].v == expected_v

        # Verify compaction ran
        metrics = await manager.metrics.query(servers[0].ip_addr)
        segments_compacted = metrics.get("scylla_logstor_sm_segments_compacted") or 0
        logger.info(f"Segments compacted: {segments_compacted}")
        assert segments_compacted > 0, "Compaction should have run when filling disk twice"

        # restart the server
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        cql, _ = await manager.get_ready_cql(servers)

        # Verify all data after restart
        for pk, expected_v in last_values.items():
            rows = await cql.run_async(f"SELECT v FROM {ks}.test WHERE pk = {pk}")
            assert len(rows) == 1, f"Key {pk} not found after recovery"
            assert rows[0].v == expected_v, f"Key {pk} value mismatch after recovery"

@pytest.mark.asyncio
async def test_compaction(manager: ManagerClient):
    """
    Test log compaction by creating dead data and verifying space reclamation.
    """
    cmdline = ['--logger-log-level', 'logstor=trace', '--smp=1']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH tablets={'initial':1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        # write few segments with unique keys, then few segments with overwrites.
        # write large values so each write fills a single segment.
        value_size = 120 * 1024
        value = 'x' * value_size

        # write few unique keys
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{value}')")

        # few writes to the same key to create dead data except the last one
        for i in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (100, '{value}')")

        # the barrier will flush all segments and put them into a single compaction group since
        # there is a single tablet.
        await manager.api.logstor_barrier(servers[0].ip_addr)

        # trigger compaction. should take the 4 segments with dead data and compact them
        await manager.api.logstor_compaction(servers[0].ip_addr)

        metrics = await manager.metrics.query(servers[0].ip_addr)
        segments_compacted = metrics.get("scylla_logstor_sm_segments_compacted") or 0
        assert segments_compacted == 4, f"Expected 4 segments to be compacted, but got {segments_compacted}"

@pytest.mark.asyncio
async def test_drop_table(manager: ManagerClient):
    """
    Test log compaction by creating dead data and verifying space reclamation.
    """
    cmdline = ['--logger-log-level', 'logstor=trace']
    cfg = {'enable_kv_storage': True, 'experimental_features': ['kv-storage']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH tablets={'initial': 4}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        # create another table that will not be dropped to verify it's not affected
        await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        # write data to fill few segments
        value_size = 30 * 1024
        value = 'x' * value_size
        for i in range(20):
            await cql.run_async(f"INSERT INTO {ks}.test1 (pk, v) VALUES ({i}, '{value}')")
            await cql.run_async(f"INSERT INTO {ks}.test2 (pk, v) VALUES ({i}, '{value}')")

        await cql.run_async(f"DROP TABLE {ks}.test1")

        # verify test2 is not affected
        for i in range(20):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test2 WHERE pk = {i}")
            assert len(rows) == 1, f"Expected 1 row for key {i} in test2, but got {len(rows)}"
            assert rows[0].v == value, f"Expected value of size {value_size} for key {i} in test2, but got {len(rows[0].v)}"

        # recreate the table and verify that old data is not visible
        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, v text) WITH kv_storage = true")

        for i in range(20):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test1 WHERE pk = {i}")
            assert len(rows) == 0, f"Expected no rows for key {i} after table drop, but got {len(rows)}"

        # write new data to the recreated table and verify
        await cql.run_async(f"INSERT INTO {ks}.test1 (pk, v) VALUES (1, 'new_value')")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test1 WHERE pk = 1")
        assert len(rows) == 1, f"Expected 1 row for key 1 after new insert, but got {len(rows)}"
        assert rows[0].v == 'new_value', f"Expected value 'new_value' for key 1 after new insert, but got {rows[0].v}"

        # verify test2 again
        for i in range(20):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test2 WHERE pk = {i}")
            assert len(rows) == 1, f"Expected 1 row for key {i} in test2 after all operations, but got {len(rows)}"
            assert rows[0].v == value, f"Expected value of size {value_size} for key {i} in test2 after all operations, but got {len(rows[0].v)}"
