#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the min_compression_saving_percent cluster config option, which stores sstable
# chunks that don't compress well uncompressed (SCYLLADB-5138). Only the `mu` format has raw chunks.

import asyncio
import glob
import json
import logging
import os
import random
import subprocess
import time

import pytest
from cassandra.protocol import InvalidRequest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_feature

logger = logging.getLogger(__name__)

FEATURE = "CLUSTER_CONFIG_MIN_COMPRESSION_SAVING_PERCENT"
SAVING = 10
CHUNK = 4096
SCHEMA = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
OPTION = f"min_compression_saving_percent = {SAVING}"
MU_CONFIG = {"sstable_format": "mu"}


async def add_mu_servers(manager: ScyllaClusterManager, n: int, config: dict | None = None, **kwargs) -> list[ServerInfo]:
    """Starts nodes writing `mu`; the format silently falls back until MU_SSTABLE_FORMAT is enabled."""
    servers = await manager.servers_add(n, config={**MU_CONFIG, **(config or {})}, **kwargs)
    cql, hosts = await manager.get_ready_cql(servers)
    deadline = time.time() + 60
    await asyncio.gather(*(wait_for_feature("MU_SSTABLE_FORMAT", cql, h, deadline) for h in hosts))
    return servers


async def populate(manager: ScyllaClusterManager, table: str) -> dict[tuple[int, int], bytes]:
    """Alternates incompressible and compressible 16 KiB values, so both chunk kinds exist."""
    cql = manager.get_cql()
    insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
    rows = {}
    for pk in range(4):
        for ck in range(8):
            v = random.randbytes(16384) if ck % 2 else bytes([ck]) * 16384
            rows[(pk, ck)] = v
            await cql.run_async(insert, (pk, ck, v))
    return rows


async def check_rows(manager: ScyllaClusterManager, table: str, rows: dict[tuple[int, int], bytes]) -> None:
    res = await manager.get_cql().run_async(f"SELECT pk, ck, v FROM {table} BYPASS CACHE")
    assert {(r.pk, r.ck): r.v for r in res} == rows


def data_files(workdir: str, ks: str, cf: str) -> list[str]:
    return glob.glob(os.path.join(workdir, "data", ks, f"{cf}-*", "*-Data.db"))


def run_sstable_tool(exe: str, workdir: str, op: str, sstables: list[str]) -> dict:
    out = subprocess.check_output([exe, "sstable", op, "--scylla-yaml-file", os.path.join(workdir, "conf", "scylla.yaml"),
                                   "--sstables", *sstables], stderr=subprocess.PIPE)
    return json.loads(out)


def raw_chunk_count(data_file: str, max_len: int) -> int:
    """Walks the framed chunks of a `mu` Data.db and counts those storing at least max_len bytes (raw)."""
    with open(data_file, "rb") as f:
        data = f.read()
    count = pos = 0
    while pos < len(data):
        b0 = data[pos]
        n = 1
        while n < 9 and b0 & (0x80 >> (n - 1)):
            n += 1
        # Header: n-1 leading ones then a zero, then big-endian value bits.
        length = (b0 & ((1 << (8 - n)) - 1)) if n < 9 else 0
        for b in data[pos + 1:pos + n]:
            length = (length << 8) | b
        count += length >= max_len
        pos += length + 2 * n + 4
    assert pos == len(data), "chunks don't tile Data.db"
    return count


async def compression_infos(manager: ScyllaClusterManager, server: ServerInfo, ks: str, cf: str) -> list[tuple[str, dict]]:
    exe = await manager.server_get_exe(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)
    files = data_files(workdir, ks, cf)
    assert files, "no sstables found"
    return [(f, next(iter(run_sstable_tool(exe, workdir, "dump-compression-info", [f])["sstables"].values()))) for f in files]


async def assert_raw_chunks(manager: ScyllaClusterManager, server: ServerInfo, ks: str, cf: str, expect: bool) -> None:
    for f, info in await compression_infos(manager, server, ks, cf):
        assert info["chunk_len"] == CHUNK
        assert os.path.basename(f).startswith("mu-"), f
        if expect:
            max_len = -(-CHUNK * (100 - SAVING) // 100)
            assert raw_chunk_count(f, max_len) > 0, f
        assert info["options"].get("min_compression_saving_percent") == (str(SAVING) if expect else None), info["options"]


async def wait_for_raw_chunks(manager: ScyllaClusterManager, server: ServerInfo, ks: str, cf: str, expect: bool) -> None:
    """A node applies a config change once its cluster config cache refreshes; rewrite until it has."""
    deadline = time.time() + 60
    while True:
        await manager.api.keyspace_upgrade_sstables(server.ip_addr, ks)
        try:
            await assert_raw_chunks(manager, server, ks, cf, expect)
            return
        except AssertionError:
            if time.time() > deadline:
                raise
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_min_compression_saving_percent_feature_gate(manager: ScyllaClusterManager):
    """The option is rejected until every node supports the feature, then accepted and written."""
    suppressed = {'error_injections_at_startup': [{'name': 'suppress_features', 'value': FEATURE}]}
    servers = await add_mu_servers(manager, 2, config=suppressed, auto_rack_dc="dc1")
    # One node without the feature keeps it disabled cluster-wide.
    await manager.server_update_config(servers[1].server_id, 'error_injections_at_startup', [])
    await manager.server_restart(servers[1].server_id, wait_others=1)
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}") as ks:
        with pytest.raises(InvalidRequest, match="min_compression_saving_percent"):
            await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {OPTION}")

        await manager.server_update_config(servers[0].server_id, 'error_injections_at_startup', [])
        await manager.server_restart(servers[0].server_id, wait_others=1)
        cql, hosts = await manager.get_ready_cql(servers)
        deadline = time.time() + 60
        await asyncio.gather(*(wait_for_feature(FEATURE, cql, h, deadline) for h in hosts))

        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {OPTION}")
        rows = await populate(manager, f"{ks}.t")
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, "t") for s in servers])
        for s in servers:
            await wait_for_raw_chunks(manager, s, ks, "t", expect=True)
        await check_rows(manager, f"{ks}.t", rows)


@pytest.mark.asyncio
async def test_min_compression_saving_percent_alter_and_upgradesstables(manager: ScyllaClusterManager):
    """ALTER only affects new sstables; upgradesstables rewrites existing ones with the current setting.
    The table inherits a keyspace-scope value, and a table-scope value overrides it."""
    servers = await add_mu_servers(manager, 1)
    server = servers[0]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH compression = {{'sstable_compression': 'LZ4Compressor'}}")
        rows = await populate(manager, f"{ks}.t")
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")
        await assert_raw_chunks(manager, server, ks, "t", expect=False)

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH {OPTION}")
        # Existing sstables keep their own setting until rewritten.
        await assert_raw_chunks(manager, server, ks, "t", expect=False)
        await wait_for_raw_chunks(manager, server, ks, "t", expect=True)
        await check_rows(manager, f"{ks}.t", rows)

        await cql.run_async(f"ALTER TABLE {ks}.t WITH min_compression_saving_percent = 0")
        await wait_for_raw_chunks(manager, server, ks, "t", expect=False)
        await check_rows(manager, f"{ks}.t", rows)


@pytest.mark.asyncio
async def test_min_compression_saving_percent_sstable_tools(manager: ScyllaClusterManager):
    """scylla-sstable validates and dumps sstables with raw chunks."""
    servers = await add_mu_servers(manager, 1)
    server = servers[0]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {OPTION}")
        rows = await populate(manager, f"{ks}.t")
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")
        await wait_for_raw_chunks(manager, server, ks, "t", expect=True)
        exe = await manager.server_get_exe(server.server_id)
        workdir = await manager.server_get_workdir(server.server_id)
        files = data_files(workdir, ks, "t")

        res = run_sstable_tool(exe, workdir, "validate-checksums", files)
        assert all(r["valid"] for r in res["sstables"].values()), res

        # Decoding every row proves the raw chunks were read back; the values were checked over CQL.
        res = run_sstable_tool(exe, workdir, "dump-data", files)
        n_rows = sum(len(p.get("clustering_elements", [])) for sst in res["sstables"].values() for p in sst)
        assert n_rows == len(rows)
        await check_rows(manager, f"{ks}.t", rows)


@pytest.mark.asyncio
async def test_min_compression_saving_percent_tablet_migration(manager: ScyllaClusterManager):
    """A tablet with raw-chunk sstables streams to another node intact."""
    servers = await add_mu_servers(manager, 2)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {OPTION}")
        rows = await populate(manager, f"{ks}.t")
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, "t") for s in servers])

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, "t")
        assert len(replicas) == 1 and len(replicas[0].replicas) == 1
        src_host, src_shard = replicas[0].replicas[0]
        host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
        dst = next(s for s in servers if host_ids[s.server_id] != src_host)
        src = next(s for s in servers if host_ids[s.server_id] == src_host)
        await wait_for_raw_chunks(manager, src, ks, "t", expect=True)

        await manager.api.move_tablet(servers[0].ip_addr, ks, "t", src_host, src_shard, host_ids[dst.server_id], 0, 0)
        # The streamed sstables themselves, before any rewrite on dst.
        await assert_raw_chunks(manager, dst, ks, "t", expect=True)
        await check_rows(manager, f"{ks}.t", rows)
