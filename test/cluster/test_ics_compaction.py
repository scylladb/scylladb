#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
IncrementalCompactionStrategy (ICS) on real nodes: altering tables to and
from ICS, restoring snapshots into ICS tables with "nodetool refresh",
sstable sizes after major compaction, and cross-tier compaction triggered
by space_amplification_goal.

Ported from scylla-dtest ics_compaction_test.py. Like the dtests, the
nodes use vnodes, one compaction group per shard and
--compaction-enforce-min-threshold, the table is keyspace1.standard1
(key blob, "C0" blob) and its sstables are written by cassandra-stress, one
flush after each run.
"""

import glob
import logging
import os
import shutil

import pytest

from test.cluster.test_compaction import wait_for_compactions
from test.cluster.util import cassandra_stress, get_table_dir, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes; the tests expect one
# sstable per shard.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'tablets_initial_scale_factor': 1}
CMDLINE = ["--compaction-enforce-min-threshold", "true"]
KS, CF = "keyspace1", "standard1"
MB = 1024 * 1024
STCS, LCS, TWCS, ICS = ("SizeTieredCompactionStrategy", "LeveledCompactionStrategy",
                        "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy")
NON_ICS_STRATEGIES = [STCS, TWCS, LCS]
NUM_OF_GENERATED_SSTABLES = 4


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def create_table(cql, strategy: str, sstable_size_in_mb=None, extra: dict | None = None) -> None:
    params = {"class": strategy}
    if strategy in (ICS, LCS):
        params["sstable_size_in_mb"] = str(sstable_size_in_mb)
    params.update(extra or {})
    await cql.run_async(f'CREATE TABLE {KS}.{CF} (key blob PRIMARY KEY, "C0" blob) WITH compaction = {params}')


async def create_ks_and_table(cql, rf: int, strategy: str, sstable_size_in_mb=10, extra: dict | None = None) -> None:
    await cql.run_async(f"CREATE KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")
    await create_table(cql, strategy, sstable_size_in_mb, extra)


async def prepare(manager: ScyllaClusterManager, strategy: str = STCS, nodes: int = 1, sstable_size_in_mb=10,
                  extra: dict | None = None, cmdline: list[str] = []):
    # As on the dtests' nodes, dropped tables are snapshotted (test.py turns
    # auto_snapshot off).
    servers = await manager.servers_add(nodes, config={**CONFIG, 'auto_snapshot': True}, cmdline=CMDLINE + cmdline,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(nodes)])
    cql = manager.get_cql()
    await create_ks_and_table(cql, nodes, strategy, sstable_size_in_mb, extra)
    return servers[0], cql


async def count_rows(cql) -> int:
    return list(await cql.run_async(f"SELECT COUNT(*) FROM {KS}.{CF}"))[0].count


async def stress_sstables(manager, server, cql, num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES, start_index=1, write_range=1,
                          write_size_unit_in_mb=1, increasing_write_size=False, num_writes_per_sstable=1, read_only=False) -> None:
    """The dtests' _read_or_write_and_flush_sstables(): one cassandra-stress
    run per sstable, of rows of 1 MB (or 1, 2, ... MB), each flushed."""
    for idx in range(1, num_of_generated_sstables + 1):
        size = write_size_unit_in_mb * MB * (idx if increasing_write_size else 1)
        await cassandra_stress(manager, server, "read" if read_only else "write", "no-warmup", f"n={num_writes_per_sstable}",
                               "-pop", f"seq={start_index}..{start_index + write_range}", "-col", "n=fixed(1)",
                               f"size=fixed({size})", "-rate", "threads=1", create_keyspace=False)
        if not read_only:
            await run_nodetool(manager, server, "flush")
            await count_rows(cql)
        start_index += write_range


async def table_strategy(cql) -> str:
    [row] = list(await cql.run_async(f"SELECT keyspace_name, table_name, compaction FROM system_schema.tables "
                                     f"WHERE keyspace_name = '{KS}' AND table_name = '{CF}'"))
    return row.compaction["class"].split(".")[-1]


async def alter_compaction(cql, strategy: str, sstable_size_in_mb=None, extra: list[dict] | None = None) -> None:
    params = {"class": strategy}
    if sstable_size_in_mb and strategy in (ICS, LCS):
        params["sstable_size_in_mb"] = sstable_size_in_mb
    for p in extra or []:
        params.update(p)
    await cql.run_async(f"ALTER TABLE {KS}.{CF} WITH compaction = {params}")
    assert await table_strategy(cql) == strategy


async def sstable_sizes_in_mb(manager, server) -> list[int]:
    table_dir = await get_table_dir(manager, server, KS, CF)
    return [os.path.getsize(f) >> 20 for f in glob.glob(f"{table_dir}/*-Data.db")]


async def create_table_and_alter_compaction(manager, original: str, new: str):
    server, cql = await prepare(manager, original)
    await stress_sstables(manager, server, cql, num_of_generated_sstables=5, increasing_write_size=True, num_writes_per_sstable=8)
    extra = [{"bucket_high": 1.2}, {"bucket_low": 0.8}, {"min_sstable_size": 10}, {"min_threshold": 2}, {"max_threshold": 80}]
    await alter_compaction(cql, new, extra=extra if new in (ICS, STCS) else None)
    await wait_for_compactions(manager, server)
    await run_nodetool(manager, server, "compact")
    await wait_for_compactions(manager, server)
    return server, cql


# A table with data is altered from one strategy to another, then a third,
# each followed by a major compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("first, second, third", [(STCS, LCS, ICS), (STCS, ICS, STCS)], ids=["STCS-LCS-ICS", "STCS-ICS-STCS"])
async def test_alter_table_strategy_twice(manager: ScyllaClusterManager, first, second, third):
    server, cql = await create_table_and_alter_compaction(manager, first, second)
    await alter_compaction(cql, third)
    await run_nodetool(manager, server, "compact")
    await wait_for_compactions(manager, server)


# Ported from scylla-dtest ics_compaction_test.py::TestIcsCompaction::test_alter_table_to_ics.
# A table of another strategy with data is altered to ICS (with the
# dtest's bucket and threshold options), then compacted.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("original_strategy", NON_ICS_STRATEGIES)
async def test_alter_table_to_ics(manager: ScyllaClusterManager, original_strategy):
    await create_table_and_alter_compaction(manager, original_strategy, ICS)


# An ICS table with data is altered to another strategy.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("new_strategy", NON_ICS_STRATEGIES)
async def test_alter_table_from_ics(manager: ScyllaClusterManager, new_strategy):
    await create_table_and_alter_compaction(manager, ICS, new_strategy)


async def make_snapshot(manager, server, copy_dir) -> str:
    """The dtests' make_snapshot() of the table with tag "basic", copied
    to copy_dir/<ks>/<table dir>/basic."""
    await run_nodetool(manager, server, "snapshot", KS, "-cf", CF, "-t", "basic")
    table_dir = await get_table_dir(manager, server, KS, CF)
    shutil.copytree(os.path.join(table_dir, "snapshots", "basic"),
                    os.path.join(copy_dir, KS, os.path.basename(table_dir), "basic"))
    return str(copy_dir)


async def restore_snapshot_with_refresh(manager, server, snapshot_dir) -> None:
    [src] = glob.glob(f"{snapshot_dir}/{KS}/{CF}-*/basic")
    shutil.copytree(src, os.path.join(await get_table_dir(manager, server, KS, CF), "upload"), dirs_exist_ok=True)
    await run_nodetool(manager, server, "refresh", KS, CF)


async def table_data_snapshot(manager, tmp_path, strategy: str, sstable_size_in_mb=2):
    """A table of the strategy with sstables of 1..4 MB, and its snapshot."""
    server, cql = await prepare(manager, strategy, sstable_size_in_mb=sstable_size_in_mb if strategy != TWCS else 10)
    await stress_sstables(manager, server, cql, increasing_write_size=True)
    await wait_for_compactions(manager, server)
    return await make_snapshot(manager, server, tmp_path), server, cql


async def drop_and_recreate_as_ics(manager, server, cql, sstable_size_in_mb: int) -> None:
    await cql.run_async(f"DROP KEYSPACE {KS}")
    shutil.rmtree(os.path.join(await manager.server_get_workdir(server.server_id), "data", KS))
    await create_ks_and_table(cql, 1, ICS, sstable_size_in_mb)


# An ICS table's snapshot, restored with refresh into the recreated table,
# has the rows of the snapshot; after more writes and a major compaction
# the sstables are at least sstable_size_in_mb and total the data.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ics_snapshot_and_restore(manager: ScyllaClusterManager, tmp_path):
    snapshot_dir, server, cql = await table_data_snapshot(manager, tmp_path, ICS)
    await stress_sstables(manager, server, cql, num_of_generated_sstables=2, start_index=NUM_OF_GENERATED_SSTABLES + 1)
    assert await count_rows(cql) == NUM_OF_GENERATED_SSTABLES + 2
    await drop_and_recreate_as_ics(manager, server, cql, 2)
    assert await count_rows(cql) == 0
    await restore_snapshot_with_refresh(manager, server, snapshot_dir)
    assert await count_rows(cql) == NUM_OF_GENERATED_SSTABLES
    await stress_sstables(manager, server, cql, num_of_generated_sstables=1)
    await run_nodetool(manager, server, "compact")
    await manager.server_stop_gracefully(server.server_id)
    sizes = await sstable_sizes_in_mb(manager, server)
    assert all(size >= 2 for size in sizes), sizes
    assert sum(sizes) == 1 + 2 + 3 + 4, sizes


# Snapshot sstables written by another strategy are refreshed into the
# table altered to ICS, and read back.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", NON_ICS_STRATEGIES)
async def test_strategy_to_ics_snapshot_refresh(manager: ScyllaClusterManager, tmp_path, strategy):
    snapshot_dir, server, cql = await table_data_snapshot(manager, tmp_path, strategy)
    await alter_compaction(cql, ICS, sstable_size_in_mb=2)
    await restore_snapshot_with_refresh(manager, server, snapshot_dir)
    await stress_sstables(manager, server, cql, increasing_write_size=True, read_only=True)


# Big STCS sstables refreshed into a new ICS table are split by a major
# compaction into sstables of about sstable_size_in_mb, keeping the rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ics_refresh_with_big_sstable_files(manager: ScyllaClusterManager, tmp_path):
    server, cql = await prepare(manager, STCS)
    rows_per_sstable, sstables = 50, 2
    await stress_sstables(manager, server, cql, num_of_generated_sstables=sstables, increasing_write_size=True,
                          num_writes_per_sstable=rows_per_sstable, write_range=rows_per_sstable)
    await wait_for_compactions(manager, server)
    snapshot_dir = await make_snapshot(manager, server, tmp_path)
    await drop_and_recreate_as_ics(manager, server, cql, 2)
    await restore_snapshot_with_refresh(manager, server, snapshot_dir)
    await run_nodetool(manager, server, "compact")
    sizes = await sstable_sizes_in_mb(manager, server)
    assert all(size <= 2 + sstables - 1 for size in sizes), sizes
    assert await count_rows(cql) == rows_per_sstable * sstables


# A snapshot refreshed into the ICS table that has newer sstables since
# adds its rows to theirs.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ics_sstables_refresh_with_collisions(manager: ScyllaClusterManager, tmp_path):
    snapshot_dir, server, cql = await table_data_snapshot(manager, tmp_path, ICS)
    await stress_sstables(manager, server, cql, num_of_generated_sstables=4, start_index=5)
    await restore_snapshot_with_refresh(manager, server, snapshot_dir)
    assert await count_rows(cql) == 8


# ICS (sstable_size_in_mb 2) with partitions of 1-4 MB, on two nodes of
# one shard (RF=2): a major compaction leaves more than one sstable, none
# larger than the two largest partitions.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_ics_with_partitions_larger_than_sstable_size(manager: ScyllaClusterManager):
    server, cql = await prepare(manager, ICS, nodes=2, sstable_size_in_mb=2, extra={"min_threshold": "2", "min_sstable_size": "1"},
                                cmdline=["--smp", "1"])
    await stress_sstables(manager, server, cql, increasing_write_size=True)
    await run_nodetool(manager, server, "compact")
    await wait_for_compactions(manager, server)
    sizes = await sstable_sizes_in_mb(manager, server)
    assert NUM_OF_GENERATED_SSTABLES > len(sizes) > 1, sizes
    assert max(sizes) <= 2 * NUM_OF_GENERATED_SSTABLES - 1, sizes


# A major compaction of an LCS table, then of the table altered to ICS.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_lcs_major_compaction_then_ics_major_compaction(manager: ScyllaClusterManager):
    server, cql = await prepare(manager, LCS, sstable_size_in_mb=2)
    await stress_sstables(manager, server, cql, num_of_generated_sstables=3, write_range=3, num_writes_per_sstable=3)
    await run_nodetool(manager, server, "compact")
    await alter_compaction(cql, ICS)
    await run_nodetool(manager, server, "compact")


async def sag_prepare(manager, goal: str, min_threshold: str, min_sstable_size: str):
    """A one-shard node with an ICS table of the given space amplification
    goal, holding one 10 MB sstable (10 partitions), major-compacted."""
    server, cql = await prepare(manager, ICS, sstable_size_in_mb=10, cmdline=["--smp", "1"],
                                extra={"space_amplification_goal": goal, "min_threshold": min_threshold, "min_sstable_size": min_sstable_size})
    await stress_sstables(manager, server, cql, num_of_generated_sstables=1, write_range=10, num_writes_per_sstable=10)
    await run_nodetool(manager, server, "compact")
    await wait_for_compactions(manager, server)
    return server, cql


# Four overlapping 1 MB sstables over a 10 MB one exceed the space
# amplification goal of 1.2, and are compacted with the big one.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_space_amplification_goal_trigger(manager: ScyllaClusterManager):
    server, cql = await sag_prepare(manager, "1.20", "6", "1")
    await stress_sstables(manager, server, cql, num_of_generated_sstables=4)
    await wait_for_compactions(manager, server)
    assert sorted(await sstable_sizes_in_mb(manager, server)) == [1, 10], "no cross-tier compaction past space_amplification_goal"


# With a goal of 1.7, four 1 MB sstables are compacted by min_threshold
# among themselves, and a further 5 MB one makes a cross-tier compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_space_amplification_goal_3_buckets(manager: ScyllaClusterManager):
    server, cql = await sag_prepare(manager, "1.70", "4", "4")
    await stress_sstables(manager, server, cql, num_of_generated_sstables=4)
    await wait_for_compactions(manager, server)
    assert sorted(await sstable_sizes_in_mb(manager, server)) == [4, 10]
    await stress_sstables(manager, server, cql, num_of_generated_sstables=1, write_range=5, num_writes_per_sstable=5)
    await wait_for_compactions(manager, server)
    assert await sstable_sizes_in_mb(manager, server) == [10], "no cross-tier compaction past space_amplification_goal"


async def check_sstable_file_size_limit(manager, server, sstable_size_in_mb: int) -> list[int]:
    """The dtests' _check_sstable_file_size_limit() over the node's sstables:
    each at most sstable_size_in_mb + 1 MB. Returns their sizes in MB."""
    sizes = await sstable_sizes_in_mb(manager, server)
    logger.debug(f"Files sizes are: {sizes}")
    assert all(size <= sstable_size_in_mb + 1 for size in sizes), f"File size is bigger than: {sstable_size_in_mb}: {sizes}"
    return sizes


# Ported from scylla-dtest ics_compaction_test.py::TestIcsCompaction::test_sstable_files_validations_with_ics_compaction.
# On two nodes (two racks, RF=2), an ICS table with sstable_size_in_mb=10
# gets three flushed cassandra-stress writes of 10 rows of 1 MB: its
# sstables, and after a major compaction at least three of them, are each
# within the size limit.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_sstable_files_validations_with_ics_compaction(manager: ScyllaClusterManager):
    sstable_size_in_mb = 10
    server, cql = await prepare(manager, ICS, nodes=2, sstable_size_in_mb=sstable_size_in_mb)
    num_of_generated_sstables = 3
    await stress_sstables(manager, server, cql, num_of_generated_sstables=num_of_generated_sstables, start_index=1,
                          write_range=10, num_writes_per_sstable=10)
    await check_sstable_file_size_limit(manager, server, sstable_size_in_mb)
    await run_nodetool(manager, server, "compact")
    sizes = await check_sstable_file_size_limit(manager, server, sstable_size_in_mb)
    assert len(sizes) >= num_of_generated_sstables, \
        f"Less than {num_of_generated_sstables} SSTable files found for {KS}.{CF} after ICS compaction!"
