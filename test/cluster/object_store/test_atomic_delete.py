#!/usr/bin/env python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, inject_error
from test.cluster.util import reconnect_driver, new_test_keyspace
from test.cluster.object_store.conftest import keyspace_options

logger = logging.getLogger(__name__)


def make_server_config(object_storage):
    objconf = object_storage.create_endpoint_conf()
    return {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
    }

async def populate_and_flush(cql, manager, server, ks, flushes, rows_per_flush=10):
    """Insert rows and flush multiple times to create separate sstables."""
    for i in range(flushes):
        start = i * rows_per_flush
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({start + k}, {start + k});")
            for k in range(rows_per_flush)
        ])
        await manager.api.flush_keyspace(server.ip_addr, ks)

async def get_registry_entries(cql, table_id):
    """Return list of (status, generation) from sstables registry."""
    res = cql.execute("SELECT * FROM system.sstables;")
    return [(row.status, row.generation) for row in res if row.table_id == table_id]

async def get_table_id(cql, ks, table='test'):
    row = cql.execute(
        f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table}'"
    ).one()
    return row.id

async def assert_registry_clean_after_restart(manager, server, table_id):
    """Restart the server and verify all registry entries are 'sealed' after garbage_collect."""
    await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)
    entries = await get_registry_entries(cql, table_id)
    for status, gen in entries:
        assert status == 'sealed', f"Entry {gen} has status '{status}', expected 'sealed'"
    logger.info("Registry entries after recovery: %d", len(entries))
    return cql, entries

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crash_after_compaction_prepare(manager: ManagerClient, object_storage):
    """Error injection throws after atomic_delete_prepare marks input sstable
    entries as 'removing' but before any S3 objects are actually deleted.

    Verifies the atomicity property: all compaction input sstables are marked
    'removing' together (none left in 'sealed' while others are 'removing').
    On restart, garbage_collect cleans up the 'removing' entries and their
    S3 objects, while the data remains readable from the compaction output.
    """
    cfg = make_server_config(object_storage)
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await populate_and_flush(cql, manager, server, ks, flushes=2)

        table_id = await get_table_id(cql, ks)
        entries_before = await get_registry_entries(cql, table_id)
        sealed_before = {gen for status, gen in entries_before if status == 'sealed'}
        logger.info("Registry entries before compaction: %d sealed", len(sealed_before))
        assert len(sealed_before) >= 2, "Need at least 2 sstables for compaction"

        # The injection throws after marking entries as 'removing' but before unlinking S3 objects.
        # Compaction handles the error internally (the REST API does not propagate it), but the
        # entries are left in 'removing' state with S3 objects still present.
        await inject_error_one_shot(manager.api, server.ip_addr, "delete_atomically_after_prepare")
        await manager.api.keyspace_compaction(server.ip_addr, ks)

        # Verify atomicity of the batch status change: after the failed compaction, the input sstables
        # should ALL be in 'removing' state. If some inputs were 'removing' and others still
        # 'sealed', the batch wasn't atomic. Compaction outputs (new generations) are excluded.
        entries_after_fail = await get_registry_entries(cql, table_id)
        removing_from_original = sealed_before & {gen for status, gen in entries_after_fail if status == 'removing'}
        still_sealed_from_original = sealed_before & {gen for status, gen in entries_after_fail if status == 'sealed'}
        logger.info("After failed compaction: %d original entries now 'removing', %d still 'sealed'",
                    len(removing_from_original), len(still_sealed_from_original))
        assert len(removing_from_original) >= 2, \
            f"Expected at least 2 original entries in 'removing', got {len(removing_from_original)}"
        assert len(still_sealed_from_original) == 0, \
            f"Atomicity violation: {len(still_sealed_from_original)} original entries still 'sealed' " \
            f"while {len(removing_from_original)} are 'removing'"

        cql, _ = await assert_registry_clean_after_restart(manager, server, table_id)

        # Data should be readable from the compaction output sstables
        res = cql.execute(f"SELECT count(*) FROM {ks}.test;")
        count = res.one().count
        logger.info("Row count after recovery: %d", count)
        assert count == 20, f"Expected 20 rows after recovery, got {count}"

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crash_after_compaction_unlink(manager: ManagerClient, object_storage):
    """Error injection throws after S3 objects have been unlinked but before
    atomic_delete_complete removes the registry entries.

    On restart, garbage_collect should clean up the dangling 'removing'
    entries. Data remains readable from the compaction output sstables.
    """
    cfg = make_server_config(object_storage)
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await populate_and_flush(cql, manager, server, ks, flushes=2)

        table_id = await get_table_id(cql, ks)

        # The injection throws after unlinks complete but before atomic_delete_complete.
        # Compaction handles the error internally, but 'removing' entries whose S3 objects
        # are already deleted remain in the registry.
        await inject_error_one_shot(manager.api, server.ip_addr, "delete_atomically_after_unlink")
        await manager.api.keyspace_compaction(server.ip_addr, ks)

        cql, _ = await assert_registry_clean_after_restart(manager, server, table_id)

        res = cql.execute(f"SELECT count(*) FROM {ks}.test;")
        count = res.one().count
        logger.info("Row count after recovery: %d", count)
        assert count == 20, f"Expected 20 rows after recovery, got {count}"

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crash_during_truncate(manager: ManagerClient, object_storage):
    """Error injection throws in delete_atomically after atomic_delete_prepare
    marks entries as 'removing' but before S3 objects are deleted.

    On restart, garbage_collect cleans up the 'removing' registry entries.
    """
    cfg = make_server_config(object_storage)
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        # Create multiple sstables. Disable auto-compaction first so they don't get merged.
        await manager.api.disable_autocompaction(server.ip_addr, ks)
        await populate_and_flush(cql, manager, server, ks, flushes=5, rows_per_flush=1)

        table_id = await get_table_id(cql, ks)
        entries_before = await get_registry_entries(cql, table_id)
        assert len(entries_before) >= 5, f"Expected at least 5 sstables, got {len(entries_before)}"

        # The injection throws after marking sstables 'removing'. The error from
        # delete_atomically is handled internally — truncate completes but leaves
        # entries in 'removing' state.
        async with inject_error(manager.api, server.ip_addr, "delete_atomically_after_prepare"):
            await cql.run_async(f"TRUNCATE TABLE {ks}.test;")

        await assert_registry_clean_after_restart(manager, server, table_id)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crash_before_batch_mutation_commits(manager: ManagerClient, object_storage):
    """Error injection throws inside batch_update_entry_status BEFORE the
    mutation is applied to the commitlog.

    This is the complement to test_crash_after_compaction_prepare: together they
    prove the atomicity guarantee of the batch mutation. If the mutation throws
    before being committed, no entries should be marked 'removing' and all data
    remains intact.
    """
    cfg = make_server_config(object_storage)
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await manager.api.disable_autocompaction(server.ip_addr, ks)
        await populate_and_flush(cql, manager, server, ks, flushes=2)

        table_id = await get_table_id(cql, ks)

        # The injection throws before apply_mutation, so the batch mutation
        # marking entries as 'removing' is never committed. Compaction handles
        # the error internally — the REST API does not propagate it.
        async with inject_error(manager.api, server.ip_addr, "batch_update_entry_status_before_apply"):
            await manager.api.keyspace_compaction(server.ip_addr, ks)
            # Verify: no entries should be in 'removing' state since the mutation
            # was never committed.
            entries = await get_registry_entries(cql, table_id)
            removing = {gen for status, gen in entries if status == 'removing'}
            assert len(removing) == 0, f"Mutation should not have been committed, but found 'removing' entries: {removing}"
            logger.info("After failed compaction: %d entries, none in 'removing' state", len(entries))

        # All data should be readable.
        res = cql.execute(f"SELECT count(*) FROM {ks}.test;")
        count = res.one().count
        logger.info("Row count after failed compaction: %d", count)
        assert count == 20, f"Expected 20 rows, got {count}"
