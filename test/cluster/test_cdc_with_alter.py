#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
from cassandra.protocol import InvalidRequest

import asyncio
import logging
import threading
import pytest


logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_add_and_drop_column_with_cdc(manager: ManagerClient):
    """ Test writing to a table with CDC enabled while adding and dropping a column.
        In particular we are interested at the behavior when the schemas of the base table
        and the CDC log may not be in sync, and we write a value to a column that exists
        in the base table but not in the CDC table.
        Reproduces #24952
    """

    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        # sleep before CDC augmentation, because we want to have a write that starts with some base schema, and then
        # the table is altered while the write is in progress, and the CDC augmentation will use the new schema that
        # is not compatible with the base schema.
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "sleep_before_cdc_augmentation", one_shot=False) for s in servers])

        # The writer thread writes to the column 'a' while it's being added and dropped.
        # We want to write a value to that column while it's in different stages - may exist
        # in one table but not in the other.
        stop_writer = threading.Event()
        writer_error = threading.Event()
        def do_writes():
            i = 0
            try:
                while not stop_writer.is_set():
                    try:
                        cql.execute(f"INSERT INTO {ks}.test(pk, v, a) VALUES({i}, {i+1}, {i+2})")
                    except InvalidRequest as e:
                        if "Unknown identifier" in str(e) or "does not have base column" in str(e):
                            pass
                        else:
                            raise
                    i += 1
            except Exception as e:
                logger.error(f"Unexpected error while writing to {ks}.test: {e}")
                writer_error.set()

        writer_thread = threading.Thread(target=do_writes)
        writer_thread.start()

        await cql.run_async(f"ALTER TABLE {ks}.test ADD a int")
        await asyncio.sleep(1)
        await cql.run_async(f"ALTER TABLE {ks}.test DROP a")

        stop_writer.set()
        writer_thread.join()

        if writer_error.is_set():
            pytest.fail("Unexpected error occurred during writes to the table")

        base_rows = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test")
        cdc_rows = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test_scylla_cdc_log")
        assert base_rows[0].count == cdc_rows[0].count, f"Base table rows: {base_rows[0].count}, CDC log rows: {cdc_rows[0].count}"

@pytest.mark.asyncio
async def test_recreate_column_too_soon(manager: ManagerClient):
    """ Test that recreating a dropped column too soon fails with an appropriate error.

        When dropping a column from a CDC log table, the drop timestamp is set
        several seconds into the future to prevent race conditions with concurrent
        writes. This test verifies that attempting to recreate a column with the
        same name before the drop timestamp has passed results in a proper error
        message, preventing potential data corruption.
    """
    await manager.servers_add(1, auto_rack_dc="dc1")
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int, dropped_col int) WITH cdc={{'enabled': true}}")
        await cql.run_async(f"ALTER TABLE {ks}.test DROP dropped_col")

        # recreating too soon
        with pytest.raises(Exception, match="a column with the same name was dropped too recently"):
            await cql.run_async(f"ALTER TABLE {ks}.test ADD dropped_col int")

@pytest.mark.asyncio
async def test_concurrent_writes_and_drop_column_with_cdc_preimage(manager: ManagerClient):
    """ Test concurrent writes and column drop with CDC preimage enabled.

        This test reproduces an issue where writes concurrent with column drop can cause
        malformed SSTables when CDC preimage is enabled. The problem occurs because:

        1. The table has CDC with preimage='full', which means CDC preimage generation
           accesses all columns in the table, including ones not touched by the write
        2. Writes continuously update existing rows (triggering preimage generation)
        3. Concurrently, a column is dropped from the table
        4. The preimage generation may access the dropped column even though the actual
           write doesn't touch it
        5. This can result in writes having newer timestamps than the column drop
           timestamp, leading to malformed SSTables where dropped columns appear
           with data newer than their drop time

        The test validates that the resulting SSTables are well-formed by running
        compaction, which would fail if the SSTables were corrupted.

        Reproduces #26340.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int, dropped_col int) WITH cdc={{'enabled': true, 'preimage': 'full'}}")

        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, 0)") for pk in range(50)])

        stop_writer = asyncio.Event()

        async def continuous_writer():
            """Task that continuously writes to the table without touching the dynamic column"""
            v = 1
            while not stop_writer.is_set():
                try:
                    # Update existing row to trigger preimage generation
                    await asyncio.gather(*[cql.run_async(f"UPDATE {ks}.test SET v = {v} WHERE pk = {pk}") for pk in range(50)])
                    v += 1
                except Exception as e:
                    # Some writes might fail due to #26405 - ignore
                    if "does not have base column" in str(e):
                        continue
                    else:
                        raise

        async def drop_column():
            await asyncio.sleep(0.5) # Let some writes happen first

            # Drop the column and flush concurrently.
            # we want values that are written at the time the column is dropped to be flushed
            await asyncio.gather(*[
                cql.run_async(f"ALTER TABLE {ks}.test DROP dropped_col"),
                manager.api.flush_keyspace(servers[0].ip_addr, ks)
                ])

        # do writes while dropping the column
        writer_task = asyncio.create_task(continuous_writer())
        schema_task = asyncio.create_task(drop_column())

        await schema_task
        stop_writer.set()
        await writer_task

        # run compaction to trigger validation of the sstables
        await manager.api.keyspace_compaction(servers[0].ip_addr, ks)
