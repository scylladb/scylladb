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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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
            segments_compacted = metrics.get("scylla_logstor_sm_segments_compacted") or 0
            if segments_compacted == 4:
                return True
            await manager.api.logstor_compaction(servers[0].ip_addr)
        await wait_for(segments_compacted, time.time() + 60)

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

@pytest.mark.asyncio
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

    s1_log = await manager.server_open_log(s1.server_id)

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
        log_mark = await s1_log.mark()

        # Start migration in the background; it will pause at the injection point
        migration_task = asyncio.ensure_future(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", h1, src_shard, h1, dst_shard, tablet_token)
        )

        # Wait until streaming has taken the snapshot and is paused at the injection
        await s1_log.wait_for(f'{inj}: waiting for message', from_mark=log_mark)

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
