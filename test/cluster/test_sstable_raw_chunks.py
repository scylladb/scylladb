#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the min_compression_saving_percent compression option, which stores sstable
# chunks that don't compress well uncompressed (SCYLLADB-5138).

import asyncio
import glob
import json
import logging
import os
import random
import subprocess
import time

import pytest
from cassandra.protocol import ConfigurationException

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_feature

logger = logging.getLogger(__name__)

FEATURE = "SSTABLE_RAW_CHUNKS"
SAVING = 10
CHUNK = 4096
SCHEMA = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
COMPRESSION = f"compression = {{'sstable_compression': 'LZ4Compressor', 'min_compression_saving_percent': {SAVING}}}"


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


def raw_chunk_count(info: dict) -> int:
    """Counts non-last chunks stored raw: exactly chunk_len bytes plus the CRC.

    LZ4 output for incompressible data is a few bytes larger than its input, so
    without the option these chunks are longer than chunk_len.
    """
    offsets = info["offsets"]
    return sum(1 for a, b in zip(offsets, offsets[1:]) if b - a - 4 == CHUNK)


async def compression_infos(manager: ScyllaClusterManager, server: ServerInfo, ks: str, cf: str) -> list[dict]:
    exe = await manager.server_get_exe(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)
    files = data_files(workdir, ks, cf)
    assert files, "no sstables found"
    res = run_sstable_tool(exe, workdir, "dump-compression-info", files)
    return list(res["sstables"].values())


async def assert_raw_chunks(manager: ScyllaClusterManager, server: ServerInfo, ks: str, cf: str, expect: bool) -> None:
    infos = await compression_infos(manager, server, ks, cf)
    for info in infos:
        assert info["chunk_len"] == CHUNK
        assert (raw_chunk_count(info) > 0) == expect, info["offsets"]
        assert info["options"].get("min_compression_saving_percent") == (str(SAVING) if expect else None), info["options"]


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_min_compression_saving_percent_feature_gate(manager: ScyllaClusterManager):
    """The option is rejected until every node supports the feature, then accepted and written."""
    suppressed = {'error_injections_at_startup': [{'name': 'suppress_features', 'value': FEATURE}]}
    servers = await manager.servers_add(2, config=suppressed, auto_rack_dc="dc1")
    # One node without the feature keeps it disabled cluster-wide.
    await manager.server_update_config(servers[1].server_id, 'error_injections_at_startup', [])
    await manager.server_restart(servers[1].server_id, wait_others=1)
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}") as ks:
        with pytest.raises(ConfigurationException, match=FEATURE):
            await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {COMPRESSION}")

        await manager.server_update_config(servers[0].server_id, 'error_injections_at_startup', [])
        await manager.server_restart(servers[0].server_id, wait_others=1)
        cql, hosts = await manager.get_ready_cql(servers)
        deadline = time.time() + 60
        await asyncio.gather(*(wait_for_feature(FEATURE, cql, h, deadline) for h in hosts))

        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {COMPRESSION}")
        rows = await populate(manager, f"{ks}.t")
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, "t") for s in servers])
        for s in servers:
            await assert_raw_chunks(manager, s, ks, "t", expect=True)
        await check_rows(manager, f"{ks}.t", rows)


@pytest.mark.asyncio
async def test_min_compression_saving_percent_alter_and_upgradesstables(manager: ScyllaClusterManager):
    """ALTER only affects new sstables; upgradesstables rewrites existing ones with the current setting."""
    servers = await manager.servers_add(1)
    server = servers[0]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH compression = {{'sstable_compression': 'LZ4Compressor'}}")
        rows = await populate(manager, f"{ks}.t")
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")
        await assert_raw_chunks(manager, server, ks, "t", expect=False)

        await cql.run_async(f"ALTER TABLE {ks}.t WITH {COMPRESSION}")
        # Existing sstables keep their own setting until rewritten.
        await assert_raw_chunks(manager, server, ks, "t", expect=False)
        await manager.api.keyspace_upgrade_sstables(server.ip_addr, ks)
        await assert_raw_chunks(manager, server, ks, "t", expect=True)
        await check_rows(manager, f"{ks}.t", rows)

        await cql.run_async(f"ALTER TABLE {ks}.t WITH compression = {{'sstable_compression': 'LZ4Compressor'}}")
        await manager.api.keyspace_upgrade_sstables(server.ip_addr, ks)
        await assert_raw_chunks(manager, server, ks, "t", expect=False)
        await check_rows(manager, f"{ks}.t", rows)


@pytest.mark.asyncio
async def test_min_compression_saving_percent_sstable_tools(manager: ScyllaClusterManager):
    """scylla-sstable validates and dumps sstables with raw chunks."""
    servers = await manager.servers_add(1)
    server = servers[0]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {COMPRESSION}")
        rows = await populate(manager, f"{ks}.t")
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")
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
    servers = await manager.servers_add(2)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t ({SCHEMA}) WITH {COMPRESSION}")
        rows = await populate(manager, f"{ks}.t")
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, "t") for s in servers])

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, "t")
        assert len(replicas) == 1 and len(replicas[0].replicas) == 1
        src_host, src_shard = replicas[0].replicas[0]
        host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
        dst = next(s for s in servers if host_ids[s.server_id] != src_host)
        src = next(s for s in servers if host_ids[s.server_id] == src_host)
        await assert_raw_chunks(manager, src, ks, "t", expect=True)

        await manager.api.move_tablet(servers[0].ip_addr, ks, "t", src_host, src_shard, host_ids[dst.server_id], 0, 0)
        await manager.api.keyspace_flush(dst.ip_addr, ks, "t")
        await assert_raw_chunks(manager, dst, ks, "t", expect=True)
        await check_rows(manager, f"{ks}.t", rows)
