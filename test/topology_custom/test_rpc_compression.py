#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""
Test RPC compression
"""
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import ScyllaMetrics
from test.pylib.util import wait_for_cql_and_get_hosts, unique_name
from test.pylib.manager_client import ManagerClient
import pytest
import asyncio
import time
import logging
import random
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
import contextlib
import typing
import functools

logger = logging.getLogger(__name__)

async def live_update_config(manager: ManagerClient, servers: list[ServerInfo], key: str, value: str):
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, deadline = time.time() + 60)
    await asyncio.gather(*[cql.run_async("UPDATE system.config SET value=%s WHERE name=%s", [value, key], host=host) for host in hosts])

def uncompressed_sent(metrics: list[ScyllaMetrics], algo: str) -> float:
    return sum([m.get("scylla_rpc_compression_bytes_sent", {"algorithm": algo}) for m in metrics])
def compressed_sent(metrics: list[ScyllaMetrics], algo: str) -> float:
    return sum([m.get("scylla_rpc_compression_compressed_bytes_sent", {"algorithm": algo}) for m in metrics])
def approximately_equal(a: float, b: float, factor: float) -> bool:
    assert factor < 1.0
    return factor < a / b < (1/factor)
async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])

async def with_retries(test_once: typing.Callable[[], typing.Awaitable], timeout: float):
    async with asyncio.timeout(timeout):
        while True:
            try:
                await test_once()
            except Exception as e:
                logger.info(f"test attempt failed with {e}, retrying")
                await asyncio.sleep(1)
            else:
                break

@pytest.mark.asyncio
async def test_basic(manager: ManagerClient) -> None:
    """Tests basic functionality of internode compression.
    Also, tests that changing internode_compression_zstd_max_cpu_fraction from 0.0 to 1.0 enables zstd as expected.
    """
    cfg = {
        'internode_compression_enable_advanced': True,
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0}
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(servers_num=2, config=cfg)

    cql = manager.get_cql()

    replication_factor = 2
    ks = unique_name()
    await cql.run_async(f"create keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    await cql.run_async(f"create table {ks}.cf (pk int, v blob, primary key (pk))")
    write_stmt = cql.prepare(f"update {ks}.cf set v = ? where pk = ?")
    write_stmt.consistency_level = ConsistencyLevel.ALL

    # 128 kiB message, should give compression ratio of ~0.5 for lz4 and ~0.25 for zstd.
    message = b''.join(bytes(random.choices(range(16), k=1024)) * 2 for _ in range(64))

    async def test_algo(algo: str, expected_ratio: float):
        n_messages = 100
        metrics_before = await get_metrics(manager, servers)
        await asyncio.gather(*[cql.run_async(write_stmt, parameters=[message, pk]) for pk in range(n_messages)])
        metrics_after = await get_metrics(manager, servers)

        volume = n_messages * len(message) * (replication_factor - 1)
        uncompressed = uncompressed_sent(metrics_after, algo) - uncompressed_sent(metrics_before, algo)
        compressed = compressed_sent(metrics_after, algo) - compressed_sent(metrics_before, algo)
        assert approximately_equal(uncompressed, volume, 0.9)
        assert approximately_equal(compressed, expected_ratio * volume, 0.9)

    await with_retries(functools.partial(test_algo, "lz4", 0.5), timeout=600)

    await live_update_config(manager, servers, "internode_compression_zstd_max_cpu_fraction", "1.0")

    await with_retries(functools.partial(test_algo, "zstd", 0.25), timeout=600)

@pytest.mark.asyncio
async def test_dict_training(manager: ManagerClient) -> None:
    """Tests population of system.dicts with dicts trained on RPC traffic."""
    training_min_bytes = 128*1024
    cfg = {
        'internode_compression_enable_advanced': True,
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0,
        'rpc_dict_training_when': 'never',
        'rpc_dict_training_min_bytes': training_min_bytes,
        'rpc_dict_training_min_time_seconds': 0,
    }
    cmdline = [
        '--logger-log-level=dict_training=trace'
    ]
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    replication_factor = 2
    ks = unique_name()
    await cql.run_async(f"create keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    await cql.run_async(f"create table {ks}.cf (pk int, v blob, primary key (pk))")
    write_stmt = cql.prepare(f"update {ks}.cf set v = ? where pk = ?")
    dict_stmt = cql.prepare(f"select data from system.dicts")
    write_stmt.consistency_level = ConsistencyLevel.ALL

    msg_size = 16*1024
    msg_train = random.randbytes(msg_size)
    msg_notrain = random.randbytes(msg_size)

    async def write_messages(m: bytes):
        n_messages = 100
        assert n_messages * len(m) > training_min_bytes
        await asyncio.gather(*[cql.run_async(write_stmt, parameters=[m, pk]) for pk in range(n_messages)])

    async def set_dict_training_when(x: str):
        await live_update_config(manager, servers, "rpc_dict_training_when", x)

    await write_messages(msg_notrain)
    await set_dict_training_when("when_leader")
    await write_messages(msg_train)
    await set_dict_training_when("never")
    await write_messages(msg_notrain)
    await set_dict_training_when("when_leader")
    await write_messages(msg_train)

    ngram_size = 8
    def make_ngrams(x: bytes) -> list[bytes]:
        return [x[i:i+ngram_size] for i in range(len(x) - ngram_size)]
    msg_train_ngrams = set(make_ngrams(msg_train))
    msg_notrain_ngrams = set(make_ngrams(msg_notrain))

    async def test_once() -> None:
        results = await cql.run_async(dict_stmt)
        dicts = [bytes(x[0]) for x in results]
        dict_ngrams = set(make_ngrams(bytes().join(dicts)))
        assert len(msg_train_ngrams & dict_ngrams) > 0.5 * len(msg_train_ngrams)
        assert len(msg_notrain_ngrams & dict_ngrams) < 0.5 * len(msg_notrain_ngrams)

    await with_retries(test_once, timeout=600)

@pytest.mark.asyncio
async def test_external_dicts(manager: ManagerClient) -> None:
    """Tests internode compression with external dictionaries"""
    cfg = {
        'internode_compression_enable_advanced': True,
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0,
        'rpc_dict_training_when': 'when_leader',
        'rpc_dict_training_min_bytes': 10000000,
        'rpc_dict_training_min_time_seconds': 0,
    }
    cmdline = [
        '--logger-log-level=dict_training=trace',
        '--logger-log-level=advanced_rpc_compressor=debug'
    ]
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    replication_factor = 2
    ks = unique_name()
    await cql.run_async(f"create keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    await cql.run_async(f"create table {ks}.cf (pk int, v blob, primary key (pk))")
    write_stmt = cql.prepare(f"update {ks}.cf set v = ? where pk = ?")
    write_stmt.consistency_level = ConsistencyLevel.ALL

    msg_size = 32*1024
    ngram_size = 64
    common_ngrams = [random.randbytes(ngram_size) for _ in range(msg_size//2//ngram_size)]

    # 128 kiB messages, should give compression ratio of ~0.5 for lz4 and ~0.25 for zstd
    # when compressed with a common dictionary.
    def make_message() -> bytes:
        common_part = b''.join(random.sample(common_ngrams, k=msg_size//2//ngram_size))
        assert len(common_part) == msg_size // 2
        unique_part = bytes(random.choices(range(16), k=msg_size//2))
        assert len(unique_part) == msg_size // 2
        return common_part + unique_part

    async def test_once(algo: str, expected_ratio: float):
        n_messages = 1000
        metrics_before = await get_metrics(manager, servers)
        messages = [make_message() for _ in range(n_messages)]
        await asyncio.gather(*[cql.run_async(write_stmt, parameters=[m, pk]) for pk, m in enumerate(messages)])
        metrics_after = await get_metrics(manager, servers)

        volume = sum(len(m) for m in messages) * (replication_factor - 1)
        uncompressed = uncompressed_sent(metrics_after, algo) - uncompressed_sent(metrics_before, algo)
        compressed = compressed_sent(metrics_after, algo) - compressed_sent(metrics_before, algo)
        assert approximately_equal(uncompressed, volume, 0.8)
        assert approximately_equal(compressed, expected_ratio * volume, 0.8)

    await with_retries(functools.partial(test_once, "lz4", 0.5), timeout=600)

    await live_update_config(manager, servers, "internode_compression_zstd_max_cpu_fraction", "1.0"),

    await with_retries(functools.partial(test_once, "zstd", 0.25), timeout=600)

# Similar to test_external_dicts, but simpler.
@pytest.mark.asyncio
async def test_external_dicts_sanity(manager: ManagerClient) -> None:
    """Tests internode compression with external dictionaries, by spamming the same UPDATE statement."""
    cfg = {
        'internode_compression_enable_advanced': True,
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0,
        'rpc_dict_training_when': 'when_leader',
        'rpc_dict_training_min_bytes': 10000000,
        'rpc_dict_training_min_time_seconds': 0,
    }
    cmdline = [
        '--logger-log-level=dict_training=trace',
        '--logger-log-level=advanced_rpc_compressor=debug',
    ]
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    replication_factor = 2
    ks = unique_name()
    await cql.run_async(f"create keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    await cql.run_async(f"create table {ks}.cf (pk int, v blob, primary key (pk))")
    write_stmt = cql.prepare(f"update {ks}.cf set v = ? where pk = ?")
    write_stmt.consistency_level = ConsistencyLevel.ALL

    msg = random.randbytes(8192)

    async def test_algo(algo: str, expected_ratio):
        n_messages = 1000
        metrics_before = await get_metrics(manager, servers)
        await asyncio.gather(*[cql.run_async(write_stmt, parameters=[msg, pk]) for pk in range(n_messages)])
        metrics_after = await get_metrics(manager, servers)

        volume = len(msg) * n_messages * (replication_factor - 1)
        uncompressed = uncompressed_sent(metrics_after, algo) - uncompressed_sent(metrics_before, algo)
        compressed = compressed_sent(metrics_after, algo) - compressed_sent(metrics_before, algo)
        assert approximately_equal(uncompressed, volume, 0.8)
        assert compressed < expected_ratio * uncompressed

    await with_retries(functools.partial(test_algo, "lz4", 0.04), timeout=600)
