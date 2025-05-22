# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import random
import logging
import pytest
import itertools
import typing
from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.rest_client import read_barrier
from cassandra.cluster import ConsistencyLevel
from test.pylib.rest_client import ScyllaMetrics

logger = logging.getLogger(__name__)

TIMEOUT = 600

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])

def dict_memory(metrics: list[ScyllaMetrics]) -> int:
    return sum([m.get("scylla_sstable_compression_dicts_total_live_memory_bytes") for m in metrics])

async def get_data_size_for_server(manager: ManagerClient, server: ServerInfo, ks: str, cf: str) -> int:
    sstable_info = await manager.api.get_sstable_info(server.ip_addr, ks, cf)
    sizes = [x['data_size'] for s in sstable_info for x in s['sstables']]
    return sum(sizes)

async def get_total_data_size(manager: ManagerClient, servers: list[ServerInfo], ks: str, cf: str) -> int:
    return sum(await asyncio.gather(*[get_data_size_for_server(manager, s, ks, cf) for s in servers]))

async def with_retries(test_once: typing.Callable[[], typing.Awaitable], timeout: float, period: float):
    async with asyncio.timeout(timeout):
        while True:
            try:
                await test_once()
            except AssertionError as e:
                logger.info(f"test attempt failed with {e}, retrying")
                await asyncio.sleep(period)
            else:
                break

async def test_autoretrain_dict(manager: ManagerClient):
    """
    Tests that sstable compression dictionary autotraining is doing its job.
    It rewrites the dataset several times, and checks that the dictionary
    is eventually optimized for the new dataset.
    """
    rf = 2
    blob_size = 16*1024
    n_blobs = 1024
    uncompressed_size = blob_size * n_blobs * rf

    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(2, cmdline=[
        '--logger-log-level=storage_service=debug',
        '--logger-log-level=database=debug',
        '--logger-log-level=sstable_dict_autotrainer=debug',
        '--sstable-compression-dictionaries-retrain-period-in-seconds=1',
        '--sstable-compression-dictionaries-autotrainer-tick-period-in-seconds=1',
        f'--sstable-compression-dictionaries-min-training-dataset-bytes={int(uncompressed_size/2)}',
    ], auto_rack_dc="dc1")

    logger.info("Creating table")
    cql = manager.get_cql()
    ks_name = "test"
    cf_name = "test"
    await cql.run_async(
        f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
    )
    await cql.run_async(f"CREATE TABLE {ks_name}.{cf_name} (pk int PRIMARY KEY, c blob);")

    logger.info("Disabling autocompaction for the table")
    await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, ks_name, cf_name) for s in servers])

    async def repopulate():
        blob = random.randbytes(blob_size);
        insert = cql.prepare("INSERT INTO test.test (pk, c) VALUES (?, ?);")
        insert.consistency_level = ConsistencyLevel.ALL;
        for pks in itertools.batched(range(n_blobs), n=100):
            await asyncio.gather(*[
                cql.run_async(insert, [k, blob])
                for k in pks
            ])
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])
        await asyncio.gather(*[manager.api.keyspace_compaction(s.ip_addr, ks_name, cf_name) for s in servers])

    logger.info("Populating the table")
    await repopulate()
    assert (await get_total_data_size(manager, servers, ks_name, cf_name)) > 0.9 * uncompressed_size

    logger.info("Altering table to use ZstdWithDictsCompressor with dicts")
    await cql.run_async(
        "ALTER TABLE test.test WITH COMPRESSION = {'sstable_compression': 'ZstdWithDictsCompressor'};"
    )

    async def test_ratio():
        sstable_info = await manager.api.get_sstable_info(servers[0].ip_addr, ks_name, cf_name)
        logger.debug(f"SSTable info {sstable_info}")
        await asyncio.gather(*[manager.api.keyspace_compaction(s.ip_addr, ks_name, cf_name) for s in servers])
        compressed_size = await get_total_data_size(manager, servers, ks_name, cf_name)
        assert compressed_size < 0.1 * uncompressed_size

    logger.info("Change the dataset several times and wait for the automatic training to rewrite the dict accordingly")

    for i in range(3):
        logger.info(f"Round {i}")
        await repopulate()
        await with_retries(test_ratio, timeout=TIMEOUT, period=2)
