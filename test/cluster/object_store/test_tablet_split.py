#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
import logging
import time

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.util import new_test_keyspace, wait_for_cql_and_get_hosts, wait_for_no_pending_topology_transition, wait_for_no_running_compactions
from test.cluster.object_store.utils import dump_object_storage_sstable_metadata, get_sealed_sstable_registry_rows
from test.pylib.manager_client import ManagerClient
from test.pylib.object_storage import keyspace_options
from test.pylib.tablets import get_tablet_count
from test.pylib.util import wait_for


logger = logging.getLogger(__name__)


async def dump_object_storage_sstable_run_ids(manager: ManagerClient, object_storage, server, ks: str, table: str, rows) -> list[str]:
    metadata = await dump_object_storage_sstable_metadata(
        manager, object_storage, server, ks, table, rows,
        f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, v blob)")
    run_ids = [entry["run_identifier"] for entry in metadata.values() if "run_identifier" in entry]
    assert len(run_ids) == len(metadata), f"Missing run_identifier in dump output: {json.dumps(metadata, indent=2)}"
    return run_ids


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_split_compaction_preserves_incremental_run_ids_on_object_storage(manager: ManagerClient, object_storage):
    """Verify split compaction updates persisted run IDs when component rewrite uses object-storage clone()."""
    async def dump_run_ids(ks, table, cql, host, table_id):
        rows = await get_sealed_sstable_registry_rows(cql, host, table_id)
        return await dump_object_storage_sstable_run_ids(manager, object_storage, server, ks, table, rows)

    objconf = object_storage.create_endpoint_conf()
    cfg = {
        "enable_user_defined_functions": False,
        "object_storage_endpoints": objconf,
        "experimental_features": ["keyspace-storage-options"],
        "tablet_load_stats_refresh_interval_in_seconds": 1,
    }
    cmdline = [
        "--smp", "1",
        "--logger-log-level", "compaction=debug",
        "--logger-log-level", "sstable=debug",
        "--logger-log-level", "table=debug",
    ]
    logger.info("Starting server for object-storage split-compaction test")
    server = await manager.server_add(config=cfg, cmdline=cmdline)
    cql = manager.get_cql()

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=1) + " AND tablets = {'enabled': true}") as ks:
        await cql.run_async(f"""
            CREATE TABLE {ks}.test (
                pk int PRIMARY KEY,
                v blob
            ) WITH compaction = {{
                'class': 'IncrementalCompactionStrategy',
                'sstable_size_in_mb': '0'
            }} AND tablets = {{
                'min_tablet_count': 1
            }}
        """)
        await manager.api.disable_autocompaction(server.ip_addr, ks, "test")

        payload = bytes([1]) * 1024
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, v) VALUES (?, ?)")
        keys = list(range(96))
        logger.info("Writing rows for object-storage split-compaction test")
        await asyncio.gather(*[
            cql.run_async(insert_stmt, [key, payload])
            for key in keys
        ])

        logger.info("Flushing and compacting rows before split")
        await manager.api.flush_keyspace(server.ip_addr, ks)
        await manager.api.keyspace_compaction(server.ip_addr, ks)
        assert await get_tablet_count(manager, server, ks, "test") == 1

        table_id = await manager.get_table_id(ks, "test")
        host = (await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30))[0]
        logger.info("Dumping object-storage SSTable run ids before split")
        before_run_ids = await dump_run_ids(ks, "test", cql, host, table_id)
        assert len(set(before_run_ids)) == 1
        assert len(before_run_ids) > 1

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        await manager.api.enable_injection(server.ip_addr, "split_sstable_rewrite", one_shot=True)

        logger.info("Triggering tablet split")
        await cql.run_async(f"ALTER TABLE {ks}.test WITH tablets = {{'min_tablet_count': 2}}")
        await manager.enable_tablet_balancing()

        await manager.api.wait_for_injection_enter(server.ip_addr, "split_sstable_rewrite")
        logger.info("Releasing split SSTable rewrite injection")
        await manager.api.message_injection(server.ip_addr, "split_sstable_rewrite")
        await log.wait_for("split_sstable_rewrite: released", from_mark=mark)

        async def split_finished():
            tablet_count = await get_tablet_count(manager, server, ks, "test")
            return tablet_count if tablet_count >= 2 else None

        logger.info("Waiting for split to finish")
        await wait_for(split_finished, time.time() + 120)
        await log.wait_for("Detected tablet split for table", from_mark=mark)
        await wait_for_no_pending_topology_transition(manager, time.time() + 120)
        await wait_for_no_running_compactions(manager, [server], time.time() + 120)

        logger.info("Dumping object-storage SSTable run ids after split")
        after_run_ids = await dump_run_ids(ks, "test", cql, host, table_id)
        assert len(set(after_run_ids)) == 2

        logger.info("Reading rows after split")
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.test", consistency_level=ConsistencyLevel.ONE))
        assert {row.pk for row in rows} == set(keys)
        assert all(bytes(row.v) == payload for row in rows)
