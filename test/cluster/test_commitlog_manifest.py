#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the commitlog segment manifest: a node refuses to start when a
# segment that a manifest lists, a shard manifest, or the whole commitlog
# directory was removed, and unsafe_ignore_commitlog_manifest overrides that.

import json
import logging
import os

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

ACTIVATED = "Commitlog manifests enabled"
MISSING_SEGMENT_REFUSAL = r"lists segment .* which is missing from .*Refusing to start"
MISSING_SEGMENT_IGNORED = r"lists segment .* which is missing from .*Starting anyway"
WIPED_REFUSAL = r"Commitlog manifest expected in .* but none found\. Refusing to start"
MISSING_SHARD_REFUSAL = r"is sealed for 2 shards but the manifest for shard 1 is missing"


def generations(cl_dir: str, prefix: str) -> list[str]:
    """Generation directories of one commitlog, oldest first."""
    gens = [e for e in os.listdir(cl_dir) if e.startswith(f"{prefix}manifest-")]
    return sorted(gens, key=lambda e: int(e.rsplit("-", 1)[1]))


def listed_segments(cl_dir: str, prefix: str) -> list[str]:
    """Segment names the newest generation lists, over all its shard files."""
    gen_dir = os.path.join(cl_dir, generations(cl_dir, prefix)[-1])
    names = []
    for entry in os.listdir(gen_dir):
        if entry.isdigit():
            with open(os.path.join(gen_dir, entry)) as f:
                names.extend(json.load(f)["segments"])
    return sorted(names)


def read_record(cl_dir: str, gen: str) -> dict:
    with open(os.path.join(cl_dir, gen, "record")) as f:
        return json.load(f)


async def start_activated(manager: ScyllaClusterManager, **kwargs):
    """Adds a node and waits until both commitlogs have a sealed manifest and the marker is set."""
    server = await manager.server_add(**kwargs)
    log = await manager.server_open_log(server.server_id)
    await log.wait_for(ACTIVATED, timeout=60)
    workdir = await manager.server_get_workdir(server.server_id)
    return server, os.path.join(workdir, "commitlog"), os.path.join(workdir, "commitlog", "schema")


async def write_rows(manager: ScyllaClusterManager, ks: str, n: int):
    cql = manager.get_cql()
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
    for pk in range(n):
        await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk}, {pk})")


async def remove_listed_segment_and_check_override(manager: ScyllaClusterManager, server, cl_dir: str, prefix: str):
    """Kills the node, removes a listed segment, checks the refusal, then the override round trip."""
    server_id = server.server_id
    # Flushed first, so the removed segment holds nothing the override start needs.
    await manager.api.flush_all_keyspaces(server.ip_addr)
    await manager.server_stop(server_id, convict=False)
    victim = listed_segments(cl_dir, prefix)[-1]
    logger.info("Removing listed segment %s/%s", cl_dir, victim)
    os.unlink(os.path.join(cl_dir, victim))

    await manager.server_start(server_id, expected_error=MISSING_SEGMENT_REFUSAL)

    await manager.server_update_config(server_id, "unsafe_ignore_commitlog_manifest", True)
    log = await manager.server_open_log(server_id)
    mark = await log.mark()
    await manager.server_start(server_id)
    await log.wait_for(MISSING_SEGMENT_IGNORED, from_mark=mark, timeout=10)

    # The start with the override rebuilt the manifests, so a start without it succeeds.
    await manager.server_stop_gracefully(server_id)
    await manager.server_remove_config_option(server_id, "unsafe_ignore_commitlog_manifest")
    await manager.server_start(server_id)


@pytest.mark.asyncio
async def test_missing_segment_refuses_start(manager: ScyllaClusterManager):
    server, cl_dir, _ = await start_activated(manager)
    await remove_listed_segment_and_check_override(manager, server, cl_dir, "CommitLog-")


@pytest.mark.asyncio
async def test_missing_schema_segment_refuses_start(manager: ScyllaClusterManager):
    # The override goes through init_schema_commitlog(), which builds its config by hand.
    server, _, schema_dir = await start_activated(manager)
    await remove_listed_segment_and_check_override(manager, server, schema_dir, "SchemaLog-")


@pytest.mark.asyncio
@pytest.mark.parametrize("files_only", [False, True])
async def test_wiped_commitlog_refuses_start(manager: ScyllaClusterManager, files_only: bool):
    """files_only mirrors `find commitlog -type f -delete`: the empty generation
    directories stay behind and must not count as a manifest."""
    server, cl_dir, _ = await start_activated(manager)
    # Killed, not drained: the wipe takes the commitlog, so the marker must come from the flushed sstable.
    await manager.server_stop(server.server_id, convict=False)
    # Takes schema/ too.
    for root, dirs, files in os.walk(cl_dir, topdown=False):
        for name in files:
            os.unlink(os.path.join(root, name))
        if not files_only:
            for name in dirs:
                os.rmdir(os.path.join(root, name))
    await manager.server_start(server.server_id, expected_error=WIPED_REFUSAL)


@pytest.mark.asyncio
async def test_missing_shard_manifest_refuses_start(manager: ScyllaClusterManager):
    server, cl_dir, _ = await start_activated(manager)
    await manager.server_stop(server.server_id, convict=False)
    gen = generations(cl_dir, "CommitLog-")[-1]
    assert read_record(cl_dir, gen)["shard_count"] == 2
    os.unlink(os.path.join(cl_dir, gen, "1"))
    await manager.server_start(server.server_id, expected_error=MISSING_SHARD_REFUSAL)


@pytest.mark.asyncio
async def test_feature_enable_creates_manifest(manager: ScyllaClusterManager):
    suppress = {"error_injections_at_startup": [{"name": "suppress_features", "value": "COMMITLOG_MANIFEST"}]}
    server = await manager.server_add(config=suppress)
    workdir = await manager.server_get_workdir(server.server_id)
    cl_dir = os.path.join(workdir, "commitlog")
    schema_dir = os.path.join(cl_dir, "schema")

    assert generations(cl_dir, "CommitLog-") == []
    assert generations(schema_dir, "SchemaLog-") == []
    cql = manager.get_cql()
    rows = await cql.run_async("SELECT value FROM system.scylla_local WHERE key = 'commitlog_manifest'")
    assert rows == []

    # Without the suppression the cluster enables the feature at runtime.
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_remove_config_option(server.server_id, "error_injections_at_startup")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_start(server.server_id)
    await log.wait_for(ACTIVATED, from_mark=mark, timeout=60)
    for d, prefix in ((cl_dir, "CommitLog-"), (schema_dir, "SchemaLog-")):
        gens = generations(d, prefix)
        assert len(gens) == 1, f"{d}: {gens}"
        assert os.path.exists(os.path.join(d, gens[0], "record"))

    await manager.server_stop(server.server_id, convict=False)
    os.unlink(os.path.join(cl_dir, listed_segments(cl_dir, "CommitLog-")[-1]))
    await manager.server_start(server.server_id, expected_error=MISSING_SEGMENT_REFUSAL)


@pytest.mark.asyncio
async def test_shard_count_change(manager: ScyllaClusterManager):
    server, cl_dir, _ = await start_activated(manager, config={"commitlog_sync": "batch"})
    # vnodes: the test is about the commitlog, not about tablets moving off a removed shard.
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await write_rows(manager, ks, 20)

        await manager.server_stop(server.server_id, convict=False)
        await manager.server_update_cmdline(server.server_id, ["--smp", "1"])
        await manager.server_start(server.server_id)
        cql, _ = await manager.get_ready_cql([server])

        rows = await cql.run_async(f"SELECT pk, v FROM {ks}.t")
        assert sorted((r.pk, r.v) for r in rows) == [(pk, pk) for pk in range(20)]

        gens = generations(cl_dir, "CommitLog-")
        assert len(gens) == 1, gens
        assert read_record(cl_dir, gens[0])["shard_count"] == 1
        assert sorted(e for e in os.listdir(os.path.join(cl_dir, gens[0]))) == ["0", "record"]


@pytest.mark.asyncio
async def test_restart_keeps_one_generation(manager: ScyllaClusterManager):
    server, cl_dir, schema_dir = await start_activated(manager)
    for _ in range(2):
        await manager.server_stop_gracefully(server.server_id)
        await manager.server_start(server.server_id)
        for d, prefix in ((cl_dir, "CommitLog-"), (schema_dir, "SchemaLog-")):
            gens = generations(d, prefix)
            assert len(gens) == 1, f"{d}: {gens}"
            assert read_record(d, gens[0])["shard_count"] == (2 if prefix == "CommitLog-" else 1)
