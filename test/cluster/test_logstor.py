#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import random
import time
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
from cassandra.protocol import ConfigurationException
import pytest
import logging
from test.pylib.tablets import get_tablet_count, get_tablet_replica
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

async def test_property(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'experimental_features': ['logstor']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t_enabled (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")
        await cql.run_async(f"CREATE TABLE {ks}.t_disabled (pk int PRIMARY KEY, v int)")

        desc = await cql.run_async(f"DESCRIBE TABLE {ks}.t_enabled")
        logger.info(f"Table t_enabled description:\n{desc}")
        assert "storage_engine = 'logstor'" in desc[0].create_statement

        desc = await cql.run_async(f"DESCRIBE TABLE {ks}.t_disabled")
        logger.info(f"Table t_disabled description:\n{desc}")
        assert "storage_engine = 'logstor'" not in desc[0].create_statement

        with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with tables that have clustering columns"):
            await cql.run_async(f"CREATE TABLE {ks}.t_enabled (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH storage_engine = 'logstor'")

async def test_config_option_consistency(manager: ManagerClient):
    """
    Test that logstor storage engine requires the experimental 'logstor' feature to be enabled.
    Without the feature flag, users cannot create logstor tables.
    """
    cmdline = ['--logger-log-level', 'logstor=debug']
    # Logstor feature is NOT enabled
    cfg = {'experimental_features': []}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        # Should fail because logstor feature is not enabled
        with pytest.raises(ConfigurationException, match="The experimental feature 'logstor' must be enabled"):
            await cql.run_async(f"CREATE TABLE {ks}.t_logstor (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

async def test_basic_write_and_read(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'experimental_features': ['logstor']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:

        # test int value

        await cql.run_async(f"CREATE TABLE {ks}.test_int (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

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

        await cql.run_async(f"DELETE FROM {ks}.test_int WHERE pk = 1")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 1")
        assert len(rows) == 0
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 2")
        assert rows[0].pk == 2
        assert rows[0].v == 150

        # test conflict resolution by timestamp
        await cql.run_async(f"INSERT INTO {ks}.test_int (pk, v) VALUES (3, 300) USING TIMESTAMP 1000")
        await cql.run_async(f"INSERT INTO {ks}.test_int (pk, v) VALUES (3, 200) USING TIMESTAMP 900")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_int WHERE pk = 3")
        assert rows[0].pk == 3
        assert rows[0].v == 300

        # test frozen map value

        await cql.run_async(f"CREATE TABLE {ks}.test_map (pk int PRIMARY KEY, v frozen<map<text, text>>) WITH storage_engine = 'logstor'")

        await cql.run_async(f"INSERT INTO {ks}.test_map (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana'}})")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_map WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == {'a': 'apple', 'b': 'banana'}

        await cql.run_async(f"INSERT INTO {ks}.test_map (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana', 'c': 'cherry'}})")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_map WHERE pk = 1")
        assert rows[0].pk == 1
        assert rows[0].v == {'a': 'apple', 'b': 'banana', 'c': 'cherry'}

        await cql.run_async(f"DELETE FROM {ks}.test_map WHERE pk = 1")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_map WHERE pk = 1")
        assert len(rows) == 0

async def test_range_read(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'experimental_features': ['logstor']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i*10})")

        # test reading all rows
        rows = await cql.run_async(f"SELECT pk, v, token(pk) AS tok FROM {ks}.test")
        assert len(rows) == 10
        assert sorted([row.pk for row in rows]) == list(range(10))
        for row in rows:
            assert row.v == row.pk * 10

        # assert the rows are sorted by token
        tokens = [row.tok for row in rows]
        assert tokens == sorted(tokens)

        # read rows by a token range
        rows = await cql.run_async(f"SELECT pk, v, token(pk) AS tok FROM {ks}.test WHERE token(pk) >= {tokens[2]} AND token(pk) < {tokens[5]}")
        assert len(rows) == 3
        assert [row.tok for row in rows] == tokens[2:5]

async def test_parallel_writes(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'experimental_features': ['logstor']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        # write to different keys in parallel
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i+1})") for i in range(100)])

        # validate
        for i in range(100):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].pk == i
            assert rows[0].v == i + 1

async def test_overwrites(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=debug']
    cfg = {'experimental_features': ['logstor']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        # write to a single key many times sequentially
        pk = 0
        for i in range(100):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {i})")

        # validate we get the last value
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
        assert rows[0].pk == pk
        assert rows[0].v == 99

async def test_parallel_big_writes(manager: ManagerClient):
    """
    Perform multiple writes in parallel with large values and validate to test segment switching.
    """
    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {'experimental_features': ['logstor']}
    await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

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

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("fail_separator_flush", [False, True], ids=["normal", "fail_separator_flush"])
async def test_recovery_basic(manager: ManagerClient, fail_separator_flush: bool):
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
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    if fail_separator_flush:
        await manager.api.enable_injection(servers[0].ip_addr, "fail_flush_separator_buffer", one_shot=False)

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

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
        'logstor_disk_size_in_mb': disk_size_mb,
        'logstor_file_size_in_mb': file_size_mb,
        'experimental_features': ['logstor']
    }
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

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
        segments_compacted = metrics.get("scylla_logstor_sm_compaction_segments_reclaimed") or 0
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

async def test_compaction(manager: ManagerClient):
    """
    Test log compaction by creating dead data and verifying space reclamation.
    """
    cmdline = ['--logger-log-level', 'logstor=trace', '--smp=1']
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH tablets={'initial':1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        # write few segments with unique keys, then few segments with overwrites.
        # write large values so each write fills a single segment.
        value_size = 120 * 1024
        value = 'x' * value_size

        # write few unique keys
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{value}')")

        # few writes to the same key to create dead data except the last one
        for _ in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (100, '{value}')")

        # flush all segments and put them into a single compaction group since
        # there is a single tablet.
        await manager.api.logstor_flush(servers[0].ip_addr)

        # trigger compaction. should take the 4 segments with dead data and compact them
        await manager.api.logstor_compaction(servers[0].ip_addr)

        async def segments_compacted():
            metrics = await manager.metrics.query(servers[0].ip_addr)
            segments_compacted = metrics.get("scylla_logstor_sm_compaction_segments_reclaimed") or 0
            if segments_compacted == 4:
                return True
            await manager.api.logstor_compaction(servers[0].ip_addr)
        await wait_for(segments_compacted, time.time() + 60)

async def test_drop_table(manager: ManagerClient):
    """
    Test that DROP TABLE works properly with logstor tables.
    """
    cmdline = ['--logger-log-level', 'logstor=trace']
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH tablets={'initial': 4}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        # create another table that will not be dropped to verify it's not affected
        await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

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
        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

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

        # now test recovery after drop table.
        await cql.run_async(f"DROP TABLE {ks}.test1")

        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        cql, _ = await manager.get_ready_cql(servers)

        # verify test2
        for i in range(20):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test2 WHERE pk = {i}")
            assert len(rows) == 1, f"Expected 1 row for key {i} in test2 after all operations, but got {len(rows)}"
            assert rows[0].v == value, f"Expected value of size {value_size} for key {i} in test2 after all operations, but got {len(rows[0].v)}"

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_drop_table_during_logstor_compaction(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'logstor=trace', '--logger-log-level', 'debug_error_injection=debug', '--smp=1']
    cfg = {'experimental_features': ['logstor']}
    server = await manager.server_add(cmdline=cmdline, config=cfg)
    cql = manager.get_cql()
    inj = 'logstor_compaction_wait_before_remove_segments'

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        value_size = 30 * 1024
        base_value = 'a' * value_size
        overwritten_value = 'b' * value_size

        for i in range(20):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{base_value}')")

        for _ in range(4):
            for i in range(10):
                await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{overwritten_value}')")

        await manager.api.logstor_flush(server.ip_addr)

        server_log = await manager.server_open_log(server.server_id)
        await manager.api.enable_injection(server.ip_addr, inj, one_shot=True)
        log_mark = await server_log.mark()

        await manager.api.logstor_compaction(server.ip_addr)
        await server_log.wait_for(f'{inj}: waiting for message', from_mark=log_mark, timeout=60)

        drop_task = cql.run_async(f"DROP TABLE {ks}.test")
        await server_log.wait_for(f"Dropping {ks}.test", from_mark=log_mark, timeout=60)

        await manager.api.message_injection(server.ip_addr, inj)
        await drop_task

async def test_trigger_separator_flush(manager: ManagerClient):
    """
    Write to 2 tablets, one slower than the other.
    The separator buffer of the slow tablet holds writes from many different segments until it becomes full.
    Separator flush should be triggered for the slow tablet before it's full in order to free the segments it holds.
    Otherwise, the faster tablet can get stuck.
    """
    disk_size_mb = 4
    file_size_mb = 1
    value_size = 50 * 1024

    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {
        'logstor_disk_size_in_mb': disk_size_mb,
        'logstor_file_size_in_mb': file_size_mb,
        'experimental_features': ['logstor']
    }
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH tablets={'initial':2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        # Calculate how many writes needed to fill disk twice
        disk_size_bytes = disk_size_mb * 1024 * 1024
        writes_to_fill_disk = disk_size_bytes // (value_size + 100)
        total_writes = 2 * writes_to_fill_disk

        # Write with overwrites to fill disk twice
        for i in range(total_writes):
            # write small values to multiple keys that will go to both tablets, in order to fill the slow tablet.
            for k in range(10):
                await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({k}, 'x')")

            # write large values to a single key in the fast tablet
            pk = 0
            value = f"value_{i}_" + ('x' * (value_size - 20))
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{value}')")

async def test_tablet_split_trigger_by_size(manager: ManagerClient):
    """
    Test that a logstor table automatically splits tablets when the data size
    exceeds --target-tablet-size-in-bytes.

    The split threshold is set to 300KB (~2 segments at 128KB each).
    Writing 5 keys with ~100KB values fills 5 segments (~640KB total), which
    exceeds the threshold and should trigger an automatic split.
    After the split, all data must remain readable.
    """
    cmdline = [
        '--logger-log-level', 'logstor=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--target-tablet-size-in-bytes', '300000',
        '--smp=1',
    ]
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'experimental_features': ['logstor'],
    }
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]
    cql = manager.get_cql()

    s0_log = await manager.server_open_log(servers[0].server_id)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        assert await get_tablet_count(manager, servers[0], ks, 'test') == 1

        s0_mark = await s0_log.mark()

        # Write 5 keys with ~100KB values to fill 5 segments (~640KB total).
        # This exceeds the 300KB target and should trigger a split.
        value = 'x' * (100 * 1024)
        for i in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{value}')")

        # Flush to assign segments to compaction groups so their size is
        # visible to tablet load stats.
        await manager.api.logstor_flush(servers[0].ip_addr)

        await s0_log.wait_for('Detected tablet split for table', from_mark=s0_mark, timeout=60)
        await asyncio.sleep(1)

        # Verify all data is accessible after the split
        for i in range(5):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1, f"Key {i} not found after tablet split"
            assert rows[0].v == value, f"Wrong value for key {i} after tablet split"

async def test_tablet_split_and_merge(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'logstor=trace',
        '--smp', '1'
    ]
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'experimental_features': ['logstor']
    }
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1 AND storage_engine='logstor';")

        total_keys = 10
        keys = range(total_keys)
        insert = cql.prepare(f"INSERT INTO {ks}.test(pk, c) VALUES(?, ?)")
        await asyncio.gather(*[cql.run_async(insert, [pk, random.randbytes(30*1024)]) for pk in keys])

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
            assert len(rows) == len(keys)

        await check()

        await manager.api.logstor_flush(servers[0].ip_addr)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count == 1

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()
        await cql.run_async(f"ALTER TABLE {ks}.test WITH tablets={{'min_tablet_count': 4}}")
        await s0_log.wait_for('Detected tablet split for table', from_mark=s0_mark)

        async def tablet_split_finished():
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            if tablet_count >= 4:
                return True
        await wait_for(tablet_split_finished, time.time() + 60)

        await check()

        s0_mark = await s0_log.mark()
        await cql.run_async(f"ALTER TABLE {ks}.test WITH tablets={{'min_tablet_count': 1}}")
        await s0_log.wait_for('Detected tablet merge for table', from_mark=s0_mark)

        async def tablet_merge_finished():
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            if tablet_count == 1:
                return True
        await wait_for(tablet_merge_finished, time.time() + 60)

        await check()

async def test_tablet_migration(manager: ManagerClient):
    """
    Test tablet migration
    """
    cmdline = ['--logger-log-level', 'logstor=trace', '--logger-log-level', 'stream_blob=trace','--smp=1']
    cfg = {'experimental_features': ['logstor']}
    s1 = await manager.server_add(cmdline=cmdline, config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
    servers = [s1]
    cql, hosts = await manager.get_ready_cql(servers)

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets={'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        nrows = 20
        value = 'x' * (30 * 1024)
        for i in range(nrows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{i}_{value}')")

        s2 = await manager.server_add(cmdline=cmdline, config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
        servers.append(s2)
        cql, hosts = await manager.get_ready_cql(servers)

        # migrate one tablet to the other node
        h1 = await manager.get_host_id(s1.server_id)
        h2 = await manager.get_host_id(s2.server_id)
        tablet_token = 0
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", h1, 0, h2, 0, tablet_token)

        for i in range(nrows):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1, f"Expected 1 row for key {i} after tablet migration, but got {len(rows)}"
            assert rows[0].v == f"{i}_{value}", f"Expected value '{i}_{value}' for key {i} after tablet migration, but got {rows[0].v}"

async def test_tablet_intranode_migration(manager: ManagerClient):
    """
    Test tablet intranode migration
    """
    cmdline = ['--logger-log-level', 'logstor=trace', '--logger-log-level', 'stream_blob=trace','--smp=2']
    cfg = {'experimental_features': ['logstor']}
    s1 = await manager.server_add(cmdline=cmdline, config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
    servers = [s1]
    cql, _ = await manager.get_ready_cql(servers)

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets={'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        nrows = 20
        value = 'x' * (30 * 1024)
        for i in range(nrows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{i}_{value}')")

        tablet_token = 0
        replica = await get_tablet_replica(manager, s1, ks, 'test', tablet_token)
        h1 = replica[0]
        src_shard = replica[1]
        dst_shard = 1 - src_shard

        # migrate one tablet to the other shard
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", h1, src_shard, h1, dst_shard, tablet_token)

        for i in range(nrows):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1, f"Expected 1 row for key {i} after tablet migration, but got {len(rows)}"
            assert rows[0].v == f"{i}_{value}", f"Expected value '{i}_{value}' for key {i} after tablet migration, but got {rows[0].v}"

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_migration_with_compaction(manager: ManagerClient):
    """
    Test that tablet migration is correct when compaction runs concurrently on the source.
    Verifies that data migrated to the destination is intact even when segments are freed
    by compaction while they are being streamed.

    Uses an injection to pause migration after the snapshot of segments is taken
    but before they are streamed, then triggers compaction to free dead
    segments, then unpauses streaming to verify data survives.

    This test:
    1. Writes several segments with overwrites to create dead data suitable for compaction
    2. Flushes to assign all segments to the tablet's compaction group
    3. Enables the injection
    4. Starts intranode tablet migration asynchronously
    5. Waits for the injection to be hit (snapshot taken, streaming paused)
    6. Triggers and waits for compaction to free dead segments
    7. Releases the injection to let streaming proceed
    8. Verifies all data is correct on the destination after migration
    """
    cmdline = ['--logger-log-level', 'logstor=trace', '--logger-log-level', 'stream_blob=trace', '--smp=2']
    cfg = {'experimental_features': ['logstor']}
    s1 = await manager.server_add(cmdline=cmdline, config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
    servers = [s1]
    cql, _ = await manager.get_ready_cql(servers)

    inj = 'wait_before_tablet_stream_files_after_snapshot'

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets={'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        value_size = 30 * 1024
        nrows = 20
        expected = {}

        # Write nrows unique keys to fill several segments
        for i in range(nrows):
            value = f"v{i}_" + ('a' * (value_size - 4))
            expected[i] = value
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{value}')")

        # Overwrite the first half of the keys multiple times to accumulate dead segments
        dead_value = 'b' * value_size
        for _ in range(4):
            for i in range(nrows // 2):
                await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{dead_value}')")

        # Write the final expected values for the overwritten keys
        for i in range(nrows // 2):
            value = f"final{i}_" + ('c' * (value_size - 9))
            expected[i] = value
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, '{value}')")

        # Flush to assign all segments to the tablet's compaction group before migration
        await manager.api.logstor_flush(s1.ip_addr)

        # Find which shard owns the tablet and pick the other shard as destination
        tablet_token = 0
        replica = await get_tablet_replica(manager, s1, ks, 'test', tablet_token)
        h1 = replica[0]
        src_shard = replica[1]
        dst_shard = 1 - src_shard

        # Enable the injection that pauses streaming after the snapshot is taken
        await manager.api.enable_injection(s1.ip_addr, inj, one_shot=True)

        # Start migration in the background; it will pause at the injection point
        migration_task = asyncio.ensure_future(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", h1, src_shard, h1, dst_shard, tablet_token)
        )

        # Wait until streaming has taken the snapshot and is paused at the injection
        await manager.api.wait_for_injection_enter(s1.ip_addr, inj)

        # Trigger compaction while streaming is paused; the snapshot holds segment refs so
        # compaction can compact and free dead segments without affecting the in-flight data
        await manager.api.logstor_compaction(s1.ip_addr)
        await manager.api.logstor_flush(s1.ip_addr)

        # Release the injection to let streaming proceed
        await manager.api.message_injection(s1.ip_addr, inj)
        await migration_task

        # Verify all data is correct on the destination after migration with concurrent compaction
        for pk, expected_v in expected.items():
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {pk}")
            assert len(rows) == 1, f"Key {pk} not found after migration with concurrent compaction"
            assert rows[0].v == expected_v, f"Key {pk} has wrong value after migration with concurrent compaction"

@pytest.mark.asyncio
async def test_cache(manager: ManagerClient):
    """
    Verify the logstor mutation cache works correctly.
    """
    cmdline = ['--logger-log-level', 'logstor=debug', '--smp=1']
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    cql = manager.get_cql()

    async def cache_metrics():
        """Return (hits, misses, insertions, evictions) summed over all shards."""
        m = await manager.metrics.query(servers[0].ip_addr)
        hits       = m.get("scylla_cache_partition_hits")       or 0
        misses     = m.get("scylla_cache_partition_misses")     or 0
        insertions = m.get("scylla_cache_partition_insertions") or 0
        evictions  = m.get("scylla_cache_partition_evictions")  or 0
        return hits, misses, insertions, evictions

    async with new_test_keyspace(manager, "WITH tablets={'initial': 1}") as ks:
        await cql.run_async(
            f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)"
            " WITH storage_engine = 'logstor'"
        )

        # ------------------------------------------------------------------ #
        # Phase 1: cold reads — every point read is a miss + insertion.       #
        # ------------------------------------------------------------------ #
        num_keys = 10
        for i in range(num_keys):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i * 10})")

        hits0, misses0, insertions0, _ = await cache_metrics()

        for i in range(num_keys):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].v == i * 10, f"unexpected value for pk={i}"

        hits1, misses1, insertions1, _ = await cache_metrics()

        # The cache metrics are shared with row_cache, so unrelated background
        # activity can add extra hits/misses/insertions. We therefore only
        # assert the deltas caused by the logstor reads as lower bounds.
        assert misses1 - misses0 >= num_keys, (
            f"expected at least {num_keys} misses for cold reads, "
            f"got {misses1 - misses0}"
        )
        assert insertions1 - insertions0 >= num_keys, (
            f"expected at least {num_keys} insertions, got {insertions1 - insertions0}"
        )

        # ------------------------------------------------------------------ #
        # Phase 2: warm reads — same keys should all be cache hits.           #
        # ------------------------------------------------------------------ #
        for i in range(num_keys):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].v == i * 10, f"unexpected value for pk={i} (warm read)"

        hits2, misses2, insertions2, _ = await cache_metrics()

        assert hits2 - hits1 >= num_keys, (
            f"expected at least {num_keys} cache hits for warm reads, "
            f"got {hits2 - hits1}"
        )

        # ------------------------------------------------------------------ #
        # Phase 3: overwrite invalidates cache.  The overwritten keys should  #
        # be misses on the next read; the untouched keys should still be hits. #
        # ------------------------------------------------------------------ #
        overwrite_pks = [0, 3, 7]
        for pk in overwrite_pks:
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk * 100})")

        hits3_before, misses3_before, insertions3_before, _ = await cache_metrics()

        for i in range(num_keys):
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            if i in overwrite_pks:
                assert rows[0].v == i * 100, f"pk={i}: expected overwritten value"
            else:
                assert rows[0].v == i * 10, f"pk={i}: unexpected value after overwrite"

        hits3, misses3, insertions3, _ = await cache_metrics()

        # Overwritten keys were invalidated → misses; others → hits.
        assert misses3 - misses3_before >= len(overwrite_pks), (
            f"expected at least {len(overwrite_pks)} misses after overwrite, "
            f"got {misses3 - misses3_before}"
        )
        assert hits3 - hits3_before >= num_keys - len(overwrite_pks), (
            f"expected at least {num_keys - len(overwrite_pks)} hits for untouched keys, "
            f"got {hits3 - hits3_before}"
        )
        assert insertions3 - insertions3_before >= len(overwrite_pks), (
            f"expected at least {len(overwrite_pks)} new insertions after overwrite, "
            f"got {insertions3 - insertions3_before}"
        )

        # ------------------------------------------------------------------ #
        # Phase 4: range read — scans all keys through the cache.             #
        # After Phase 3 every key is cached, so a full-table scan should      #
        # produce all hits and no new misses/insertions.                      #
        # ------------------------------------------------------------------ #
        hits4_before, misses4_before, insertions4_before, _ = await cache_metrics()

        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test")
        assert len(rows) == num_keys, f"expected {num_keys} rows in range scan"

        hits4, misses4, insertions4, _ = await cache_metrics()

        assert hits4 - hits4_before >= num_keys, (
            f"expected at least {num_keys} cache hits for range scan, "
            f"got {hits4 - hits4_before}"
        )

        # ------------------------------------------------------------------ #
        # Phase 5: BYPASS CACHE — reads go directly to disk, skipping the    #
        # cache entirely. Shared cache metrics may still move due to         #
        # unrelated background activity, so we only verify that data is      #
        # correct and that the cache remains warm afterward.                 #
        # ------------------------------------------------------------------ #
        for i in range(num_keys):
            expected_v = (i * 100) if i in overwrite_pks else (i * 10)
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i} BYPASS CACHE")
            assert rows[0].v == expected_v, f"BYPASS CACHE point read: unexpected value for pk={i}"

        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test BYPASS CACHE")
        assert len(rows) == num_keys, f"BYPASS CACHE range scan: expected {num_keys} rows"

        # Verify the cache is intact after BYPASS CACHE reads: normal reads
        # should still produce cache hits.
        hits5c_before, misses5c_before, insertions5c_before, _ = await cache_metrics()

        for i in range(num_keys):
            expected_v = (i * 100) if i in overwrite_pks else (i * 10)
            rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = {i}")
            assert rows[0].v == expected_v, f"warm read after BYPASS CACHE: unexpected value for pk={i}"

        hits5c, misses5c, insertions5c, _ = await cache_metrics()

        assert hits5c - hits5c_before >= num_keys, (
            f"expected at least {num_keys} cache hits for warm reads after BYPASS CACHE, "
            f"got {hits5c - hits5c_before}"
        )

        # ------------------------------------------------------------------ #
        # Phase 6: schema change on cached row. The first read after ALTER    #
        # should upgrade the cached mutation in place, and the second read    #
        # should hit the already-upgraded cache entry.                        #
        # ------------------------------------------------------------------ #
        await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES (100, 1000)")
        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test WHERE pk = 100")
        assert rows[0].pk == 100
        assert rows[0].v == 1000

        hits6_before, misses6_before, insertions6_before, _ = await cache_metrics()

        await cql.run_async(f"ALTER TABLE {ks}.test ADD v2 int")

        rows = await cql.run_async(f"SELECT pk, v, v2 FROM {ks}.test WHERE pk = 100")
        assert rows[0].pk == 100
        assert rows[0].v == 1000
        assert rows[0].v2 is None

        hits6_mid, misses6_mid, insertions6_mid, _ = await cache_metrics()
        assert hits6_mid - hits6_before >= 1, (
            f"expected at least 1 cache hit for schema-upgrade read, got {hits6_mid - hits6_before}"
        )

        rows = await cql.run_async(f"SELECT pk, v, v2 FROM {ks}.test WHERE pk = 100")
        assert rows[0].pk == 100
        assert rows[0].v == 1000
        assert rows[0].v2 is None

        hits6, misses6, insertions6, _ = await cache_metrics()
        assert hits6 - hits6_mid >= 1, (
            f"expected at least 1 cache hit for read after cache upgrade, got {hits6 - hits6_mid}"
        )
        hits_final, misses_final, insertions_final, evictions_final = await cache_metrics()
        logger.info(
            "logstor cache test complete: hits=%d misses=%d insertions=%d evictions=%d",
            hits_final, misses_final, insertions_final, evictions_final,
        )

        # ------------------------------------------------------------------ #
        # Phase 7: test table with caching disabled. Shared cache metrics are #
        # global, so we validate correctness only.                           #
        # ------------------------------------------------------------------ #
        await cql.run_async(
            f"CREATE TABLE {ks}.test_no_cache (pk int PRIMARY KEY, v int)"
            " WITH storage_engine = 'logstor' AND caching = {'enabled': false}"
        )

        num_keys = 10
        for i in range(num_keys):
            await cql.run_async(f"INSERT INTO {ks}.test_no_cache (pk, v) VALUES ({i}, {i * 10})")

        for _ in range(2):
            for i in range(num_keys):
                rows = await cql.run_async(f"SELECT pk, v FROM {ks}.test_no_cache WHERE pk = {i}")
                assert rows[0].v == i * 10, f"unexpected value for pk={i}"

async def test_separator_buffer_pressure(manager: ManagerClient):
    """
    Test that the separator buffer pool handles pressure gracefully when many tablets
    compete for a very small pool.

    With logstor_separator_max_memory_in_mb=1 (1 MB / 128 KB = 8 separator buffers)
    and a table split across 32 tablets (4x the pool size), writes to 200 different
    keys target all tablets simultaneously. As the separator fiber replays records
    from the write log into per-tablet compaction groups, it repeatedly exhausts the
    8-buffer pool: when the pool is empty, allocate_separator_buffer() triggers a flush
    of an existing buffer (freeing it back to the pool) before the next allocation can
    proceed. This cycle repeats ~24+ times across the 32 tablets.

    Verification uses three independent signals:
    1. No deadlock: logstor_flush completes, proving pool-exhaustion recovery worked.
    2. Metrics: scylla_logstor_sm_separator_buffer_flushed increased, confirming that
       actual disk writes occurred and each one succeeded (the counter only increments
       after a successful write_full_segment call, never on failure).
    3. Log scan: absence of "Writing to separator failed" warnings, confirming that
       the separator fiber did not swallow any silent exceptions during replay.
    """
    # 1 MB / 128 KB per segment = 8 separator buffers; 32 tablets is 4x the pool size.
    num_tablets = 32
    num_keys = 200

    cmdline = ['--logger-log-level', 'logstor=trace', '--smp=1']
    cfg = {
        'experimental_features': ['logstor'],
        'logstor_separator_max_memory_in_mb': 1,
    }
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    server = servers[0]
    cql = manager.get_cql()
    server_log = await manager.server_open_log(server.server_id)

    async with new_test_keyspace(manager, f"WITH tablets={{'initial': {num_tablets}}}") as ks:
        await cql.run_async(
            f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text)"
            " WITH storage_engine = 'logstor'"
        )

        # 4 KB values: 200 keys × 4 KB ≈ 800 KB, filling ~6 segments (128 KB each).
        # Records from each segment are spread across all 32 tablets by token, so the
        # separator fiber needs up to 32 buffers while only 8 are available.
        value = 'x' * 4096

        # Snapshot log position and the separator flush counter before writes so we
        # can measure the delta and scan only the relevant log window.
        log_mark = await server_log.mark()
        metrics_before = await manager.metrics.query(server.ip_addr)
        sep_flushed_before = metrics_before.get("scylla_logstor_sm_separator_buffer_flushed") or 0

        # Write all 200 keys in parallel; they land in the write log immediately
        # while the separator fiber processes them asynchronously in the background.
        insert = cql.prepare(f"INSERT INTO {ks}.test (pk, v) VALUES (?, ?)")
        await asyncio.gather(*[cql.run_async(insert, [i, value]) for i in range(num_keys)])

        # Flush seals the write buffer and waits for await_pending_writes() to drain
        # the separator task queue, so all records are fully processed by the time
        # this call returns and any log warnings are already written.
        await manager.api.logstor_flush(server.ip_addr)

        # --- Verification 1: data integrity ---
        # All written values must be readable with correct content.
        for i in range(num_keys):
            rows = await cql.run_async(f"SELECT v FROM {ks}.test WHERE pk = {i}")
            assert len(rows) == 1, f"Key {i} missing after separator buffer pressure test"
            assert rows[0].v == value, f"Wrong value for key {i} after separator buffer pressure test"

        # --- Verification 2: separator buffer flushes occurred and succeeded ---
        # The counter only increments inside flush_separator_buffer() after a successful
        # write_full_segment() call; it never increments on failure.  An increase
        # confirms both that pressure was real and that every triggered flush succeeded.
        metrics_after = await manager.metrics.query(server.ip_addr)
        sep_flushed_after = metrics_after.get("scylla_logstor_sm_separator_buffer_flushed") or 0
        logger.info(f"separator_buffer_flushed: {sep_flushed_before} -> {sep_flushed_after}")
        assert sep_flushed_after > sep_flushed_before, (
            f"Expected separator buffer flushes to occur under pressure but "
            f"scylla_logstor_sm_separator_buffer_flushed did not increase: "
            f"{sep_flushed_before} -> {sep_flushed_after}"
        )

        # --- Verification 3: no silent separator write failures ---
        # run_separator_fiber() catches exceptions and logs them as warnings rather
        # than propagating them, so logstor_flush() would still complete even if
        # writes to the separator failed.  Grep the log snapshot (non-blocking, reads
        # only up to the file size at call time) to confirm no such warnings appeared.
        failures = await server_log.grep("Writing to separator failed", from_mark=log_mark)
        assert not failures, (
            "Separator write failures detected in server log:\n"
            + "\n".join(line.rstrip() for line, _ in failures)
        )
