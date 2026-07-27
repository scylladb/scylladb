#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test RPC compression
"""
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import ScyllaMetrics
from test.pylib.util import wait_for_cql_and_get_hosts, unique_name
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace

import pytest
import asyncio
import hashlib
import os
import re
import stat
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

async def test_basic(manager: ManagerClient) -> None:
    """Tests basic functionality of internode compression.
    Also, tests that changing internode_compression_zstd_max_cpu_fraction from 0.0 to 1.0 enables zstd as expected.
    """
    cfg = {
        'internode_compression_enable_advanced': True,
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0}
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(servers_num=2, config=cfg, auto_rack_dc="dc1")

    cql = manager.get_cql()

    replication_factor = 2
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}") as ks:
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
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline, auto_rack_dc="dc1")

    cql = manager.get_cql()

    replication_factor = 2
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}") as ks:
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
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline, auto_rack_dc="dc1")

    cql = manager.get_cql()

    replication_factor = 2
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}") as ks:
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

        # Test that the dicts are loaded on startup.
        await asyncio.gather(*[manager.server_stop_gracefully(s.server_id) for s in servers])
        await asyncio.gather(*[manager.server_update_config(s.server_id, 'rpc_dict_training_when', 'never') for s in servers])
        await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])
        await with_retries(functools.partial(test_once, "lz4", 0.5), timeout=600)

# Similar to test_external_dicts, but simpler.
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
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline, auto_rack_dc="dc1")

    cql = manager.get_cql()

    replication_factor = 2
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}") as ks:
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

@pytest.mark.parametrize("use_dict", [False, True], ids=["null_dict", "trained_dict"])
async def test_checksum_error_diagnostics(manager: ManagerClient, use_dict: bool, tmp_path) -> None:
    """Tests that a corruption of a compressed message is diagnosed by the receiver.

    Corrupts a single message on the sender (after it's compressed), and checks that
    the receiver prints the detailed report about the resulting checksum mismatch,
    and dumps the offending frame to a file.
    """
    cfg = {
        'internode_compression_enable_advanced': True,
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0,
        'internode_compression_dump_message_on_checksum_error': True,
        'rpc_dict_training_when': 'when_leader' if use_dict else 'never',
        'rpc_dict_training_min_bytes': 10000000,
        'rpc_dict_training_min_time_seconds': 0,
    }
    cmdline = [
        '--logger-log-level=advanced_rpc_compressor=debug',
        # The checksum mismatch is reported via on_internal_error, which would kill
        # the node if it was configured to abort on those.
        '--abort-on-internal-error', '0',
    ]
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(servers_num=2, config=cfg, cmdline=cmdline, append_env={"TMPDIR": str(tmp_path)}, auto_rack_dc="dc1")

    cql = manager.get_cql()

    replication_factor = 2
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}") as ks:
        await cql.run_async(f"create table {ks}.cf (pk int, v blob, primary key (pk))")
        write_stmt = cql.prepare(f"update {ks}.cf set v = ? where pk = ?")
        write_stmt.consistency_level = ConsistencyLevel.ALL

        msg = random.randbytes(8192)

        async def write_messages(n_messages: int = 1000) -> None:
            await asyncio.gather(*[cql.run_async(write_stmt, parameters=[msg, pk]) for pk in range(n_messages)])

        if use_dict:
            # Wait until a dict is trained and put to use. Since we are spamming the same
            # message over and over, a compression ratio this good is only achievable with
            # a dictionary.
            async def dict_in_use() -> None:
                metrics_before = await get_metrics(manager, servers)
                await write_messages()
                metrics_after = await get_metrics(manager, servers)
                uncompressed = uncompressed_sent(metrics_after, "lz4") - uncompressed_sent(metrics_before, "lz4")
                compressed = compressed_sent(metrics_after, "lz4") - compressed_sent(metrics_before, "lz4")
                assert compressed < 0.04 * uncompressed
            await with_retries(dict_in_use, timeout=600)

        # The sender is servers[0], so the mismatch is detected and reported by servers[1].
        receiver_log = await manager.server_open_log(servers[1].server_id)
        mark = await receiver_log.mark()
        await manager.api.enable_injection(servers[0].ip_addr, "advanced_rpc_compressor_corrupt_last_byte", one_shot=True)

        write_stmt.consistency_level = ConsistencyLevel.ONE
        await write_messages(n_messages=1)

        mark, _ = await receiver_log.wait_for("RPC compression checksum error details", from_mark=mark, timeout=120)

        # The dump is written before this message is logged, so by now the file is complete.
        _, matches = await receiver_log.wait_for(
            r"Dumped the compressed message which failed checksum validation to (\S+)", from_mark=mark, timeout=120)
        dump_path = matches[0][1].group(1)
        tmpdir = str(tmp_path)
        assert re.fullmatch(re.escape(tmpdir) + r"/scylladb_rpc_checksum_error_dump\.shard_\d+\.bin", dump_path)
        try:
            # Only the owner may access the dump. It can contain user data.
            assert stat.S_IMODE(os.stat(dump_path).st_mode) & 0o077 == 0
            with open(dump_path, "rb") as f:
                dump = f.read()
        finally:
            os.unlink(dump_path)

        # Parse the dump. See `dump_message()` for the format.
        pos = 0
        def take(n: int) -> bytes:
            nonlocal pos
            assert pos + n <= len(dump), f"dump truncated at {pos}, wanted {n} more bytes out of {len(dump)}"
            pos += n
            return dump[pos - n:pos]
        def take_be(n: int) -> int:
            return int.from_bytes(take(n), 'big')

        header_byte = take(1)[0]
        # Bit 0x40 is the "frame has a checksum" flag. It must be set, or there would
        # have been no checksum to mismatch.
        assert header_byte & 0x40
        expected_crc = take_be(4)
        actual_crc = take_be(4)
        assert expected_crc != actual_crc
        dict_sha256 = take(32)
        dict_data = take(take_be(8))
        if use_dict:
            assert len(dict_data) > 0
            # The ID stored in the dump must describe the dictionary stored in the dump.
            assert hashlib.sha256(dict_data).digest() == dict_sha256
        else:
            # The null dictionary is empty, and its ID is all zeros.
            assert len(dict_data) == 0
            assert dict_sha256 == bytes(32)
        fragments = []
        while pos < len(dump):
            fragments.append(take(take_be(8)))
        assert sum(len(f) for f in fragments) > 0
