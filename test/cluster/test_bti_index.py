#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import asyncio
import random
import os
import glob
import json
import logging
from typing import Any
from test.cluster.conftest import skip_mode
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import ScyllaMetrics

# main logger
logger = logging.getLogger(__name__)

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
def get_io_read_ops(metrics: list[ScyllaMetrics]) -> int:
    return int(sum([m.get("scylla_io_queue_total_read_ops") for m in metrics]))

async def live_update_config(manager: ManagerClient, servers: list[ServerInfo], key: str, value: Any):
    cql, hosts = await manager.get_ready_cql(servers)
    await asyncio.gather(*[manager.server_update_config(s.server_id, key, value) for s in servers])
    stmt = cql.prepare("UPDATE system.config SET value=? WHERE name=?")
    await asyncio.gather(*[cql.run_async(stmt, [value, key], host=host) for host in hosts])

async def get_sstable_files_for_server(data_dir, ks, cf):
    base_pattern = os.path.join(data_dir, "data", f"{ks}", f"{cf}-????????????????????????????????", "*.*")
    sstables = glob.glob(base_pattern)
    return sstables

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_bti_index_enable(manager: ManagerClient) -> None:
    cassandra_logger = logging.getLogger('cassandra')
    cassandra_logger.setLevel(logging.INFO)

    n_servers = 1
    ks_name = "ks"
    cf_name = "t"

    # We run with `--smp=1` because the test uses CQL tracing.
    #
    # Trace events are written to trace tables asynchronously w.r.t.
    # the traced statements, and AFAIU there's currently no way
    # (other than polling) to wait for them to appear.
    #
    # The Python driver has a polling mechanism for traces,
    # but it is only reliable if the entire statement runs on a single shard.
    # (Because it only waits for the coordinator, and not for replicas, to write their events).
    # So let's just make this a single-shard test, we aren't testing
    # any multi-shard mechanisms here anyway.
    servers = await manager.servers_add(n_servers, cmdline=['--smp=1'], config = {
        'error_injections_at_startup': [
            {
                'name': 'suppress_features',
                'value': 'MS_SSTABLE_FORMAT',
            }
        ],
        'column_index_size_in_kb': 1,
    })
    cql = manager.get_cql()
    workdirs = await asyncio.gather(*[manager.server_get_workdir(s.server_id) for s in servers])

    logger.info("Step 1: Creating keyspace and table")
    await cql.run_async(
        f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {n_servers}}};"
    )
    await cql.run_async(f"CREATE TABLE {ks_name}.{cf_name} (pk int, ck int, v blob, primary key (pk, ck));")

    logger.info("Step 2: Populating the table")
    insert = cql.prepare(f"INSERT INTO {ks_name}.{cf_name} (pk, ck, v) VALUES (?, ?, ?);")
    n_pks = 3;
    n_cks = 32;
    pks = [i for i in range(n_pks)]
    cks = [i for i in range(n_cks)]
    vs = [random.randbytes(1024) for _ in range(n_cks)]
    for pk in pks:
        for ck, v in zip(cks, vs):
            await cql.run_async(insert, (pk, ck, v))
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])

    async def test_files_presence(bti_should_exist: bool, big_should_exist: bool):
        sstable_sets = await asyncio.gather(*[get_sstable_files_for_server(wd, ks_name, cf_name) for wd in workdirs])
        for sstable_set in sstable_sets:
            partitions = [x for x in sstable_set if x.endswith('Partitions.db')]
            rows = [x for x in sstable_set if x.endswith('Rows.db')]
            index = [x for x in sstable_set if x.endswith('Index.db')]
            summary = [x for x in sstable_set if x.endswith('Summary.db')]
            if bti_should_exist:
                assert len(partitions) > 0
                assert len(rows) > 0
            else:
                assert len(partitions) == 0
                assert len(rows) == 0
            if big_should_exist:
                assert len(index) > 0
                assert len(summary) > 0
            else:
                assert len(index) == 0
                assert len(summary) == 0

    select_without_cache = cql.prepare(f"SELECT pk, ck, v FROM {ks_name}.{cf_name} WHERE pk=? and ck=? BYPASS CACHE;")
    select_with_cache = cql.prepare(f"SELECT pk, ck, v FROM {ks_name}.{cf_name} WHERE pk=? and ck=?;")
    chosen_pk = pks[len(pks) // 2]
    chosen_ck_idx = len(cks) // 2
    chosen_ck = cks[chosen_ck_idx]
    chosen_v = vs[chosen_ck_idx]

    async def test_bti_usage_during_reads(should_use_bti: bool, use_cache: bool):
        select = select_with_cache if use_cache else select_without_cache
        metrics_before = await get_metrics(manager, servers)
        select_result = cql.execute(select, (chosen_pk, chosen_ck), trace=True)
        metrics_after = await get_metrics(manager, servers)
        row = select_result.one()
        assert row.pk == chosen_pk
        assert row.ck == chosen_ck
        assert row.v == chosen_v

        trace = select_result.get_query_trace()
        seen_index = False
        seen_partitions = False
        seen_rows = False
        for event in trace.events:
            logger.info(f"Trace event: {event.description}")
            seen_partitions = seen_partitions or "Partitions.db" in event.description
            seen_rows = seen_rows or "Rows.db" in event.description
            seen_index = seen_index or "Index.db" in event.description

        if not use_cache:
            if should_use_bti:
                assert not seen_index, "Index.db was used despite BTI preference"
                assert seen_partitions, "Partitions.db was not used despite BTI preference"
                assert seen_rows, "Rows.db was not used despite BTI preference"
            else:
                assert seen_index, "Index.db was not used despite BIG preference"
                assert not seen_partitions, "Partitions.db was used despite BIG preference"
                assert not seen_rows, "Rows.db was used despite BIG preference"

            # Test that BYPASS CACHE does force disk reads.
            io_read_ops = get_io_read_ops(metrics_after) - get_io_read_ops(metrics_before)
            if should_use_bti:
                # At least one read for Partitions.db, Rows.db, Data.db
                assert io_read_ops >= 3
            else:
                # At least one read in Index.db (main index), Index.db (promoted index), Data.db
                assert io_read_ops >= 3

    logger.info("Step 3: Checking for BTI files (should not exist, because cluster feature is suppressed)")
    await test_files_presence(bti_should_exist=False, big_should_exist=True)
    await test_bti_usage_during_reads(should_use_bti=False, use_cache=False)

    logger.info("Step 4: Updating config to 'me', unsuppressing the cluster feature")
    await live_update_config(manager, servers, 'sstable_format', 'me')
    await asyncio.gather(*[manager.server_update_config(s.server_id, "error_injections_at_startup", []) for s in servers])
    manager.driver_close()
    await manager.rolling_restart(servers)
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)
    logger.info("Step 5: Checking for BTI files (should not exist, because BTI is not enabled in the config)")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks_name) for s in servers])
    await test_files_presence(bti_should_exist=False, big_should_exist=True)
    await test_bti_usage_during_reads(should_use_bti=False, use_cache=False)

    logger.info("Step 6: Updating write config to 'ms'")
    await live_update_config(manager, servers, 'sstable_format', 'ms')
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks_name) for s in servers])
    logger.info("Step 7: Checking for BTI files (should exist)")
    await test_files_presence(bti_should_exist=True, big_should_exist=False)

    # Test that BYPASS CACHE does its thing.
    for _ in range(3):
        await test_bti_usage_during_reads(should_use_bti=True, use_cache=False)
        await test_bti_usage_during_reads(should_use_bti=True, use_cache=True)

    manager.driver_close()
