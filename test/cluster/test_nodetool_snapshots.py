#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Snapshots taken, listed, cleared and restored through "scylla nodetool", on
a node of their own, and checked against the snapshot directories on disk.

Ported from scylla-dtest. Like the dtests, the nodes create keyspaces with
vnodes by default.
"""

import logging
import os
import re

import pytest

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import asyncio
from test.cluster.dtest_helpers import cql_one

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node, and dropped or
# truncated tables are snapshotted (test.py turns auto_snapshot off).
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'tablets_initial_scale_factor': 1, 'auto_snapshot': True}
# Like the dtests' ccm populate(1): the SimpleSnitch, the node in datacenter1/rack1.
SNAPSHOT_CONFIG = {**CONFIG, "endpoint_snitch": "SimpleSnitch"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def ks_opts(rf: int = 1) -> str:
    return f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"


############################################################################
# Ported from listsnapshots_test.py: "nodetool listsnapshots" output against
# the sizes of the snapshot files on disk.

KNOWN_UNITS = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]


def normalize_size(size: str, units: str) -> float:
    """A size printed by nodetool, in bytes. nodetool divides by 1024; older
    versions label the values with base-10 unit names, so accept both."""
    if not units or units.lower() == "bytes":
        units = KNOWN_UNITS[0]
    else:
        units = units[0].upper() + "iB"
    return float(size) * 1024 ** KNOWN_UNITS.index(units)


def snapshot_sizes_on_disk(data_dir: str) -> dict:
    """{(ks, cf, snapshot name): (logical size, allocated size)} of the sstable
    files of every snapshot directory under data_dir."""
    sizes = {}
    for ks in os.listdir(data_dir):
        for path, _, files in os.walk(os.path.join(data_dir, ks)):
            if "/snapshots/" not in path:
                continue
            p = path.split("/")
            cf = p[-3].split("-")[0]
            logical = disk = 0
            for f in files:
                if "manifest.json" in f or "schema.cql" in f:
                    continue
                st = os.lstat(os.path.join(path, f))
                logical += st.st_size
                disk += st.st_blocks * 512
            sizes[(ks, cf, p[-1])] = (float(logical), float(disk))
    return sizes


def parse_listsnapshots(output: str) -> list[tuple]:
    regexp = re.compile(r"^(?P<snsh_name>[\w]+)\s+(?P<ks>[\w]+)\s+(?P<cf>[\w]+)\s+(?P<true_size>[0-9.]+)\s(?P<true_size_units>\w+)"
                        r"\s+(?P<size_on_disk>[0-9.]+)\s+(?P<size_on_disk_units>\w+)\s+$", re.MULTILINE)
    return [(name, ks, cf, normalize_size(ts, tsu), normalize_size(sd, sdu)) for name, ks, cf, ts, tsu, sd, sdu in regexp.findall(output)]


async def check_listsnapshots(manager: ScyllaClusterManager, server) -> list[tuple]:
    """Run "nodetool listsnapshots" and check that every snapshot it lists
    has a size on disk between the logical and the allocated size of its
    files. Returns the parsed listing."""
    out, err, _ = await run_nodetool(manager, server, "listsnapshots")
    assert not err
    listed = parse_listsnapshots(out)
    sizes = snapshot_sizes_on_disk(os.path.join(await manager.server_get_workdir(server.server_id), "data"))
    for name, ks, cf, _, on_disk in listed:
        logical, allocated = sizes[(ks, cf, name)]
        assert logical <= on_disk <= allocated, f"{ks}.{cf} snapshot {name}: listed {on_disk}, files {logical}..{allocated}"
    return listed


async def populate_keyspaces(cql, kses: list[str], cfs: list[str]) -> None:
    for ks in kses:
        await cql.run_async(f"CREATE KEYSPACE {ks} {ks_opts()}")
        for cf in cfs:
            await cql.run_async(f"CREATE TABLE IF NOT EXISTS {ks}.{cf} (key int PRIMARY KEY, val text);")
            await cql.run_async(f"INSERT INTO {ks}.{cf} (key, val) VALUES (1, 'asdf');")


# A node without snapshots lists none.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_listsnapshots_no_snapshots(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    out, err, _ = await run_nodetool(manager, server, "listsnapshots")
    assert not err
    assert "There are no snapshots" in out
    assert await check_listsnapshots(manager, server) == []


# A snapshot of one table is listed with its keyspace, table and size.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_listsnapshots_one_table(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await populate_keyspaces(manager.get_cql(), ["my_ks"], ["my_cf"])
    await run_nodetool(manager, server, "snapshot", "my_ks", "-cf", "my_cf")
    listed = await check_listsnapshots(manager, server)
    assert [(ks, cf) for _, ks, cf, _, _ in listed] == [("my_ks", "my_cf")]


# Snapshots of three keyspaces of three tables each are all listed.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_listsnapshots_several_keyspaces(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    kses, cfs = ["my_ks1", "my_ks2", "my_ks3"], ["my_cf1", "my_cf2", "my_cf3"]
    await populate_keyspaces(manager.get_cql(), kses, cfs)
    for ks in kses:
        await run_nodetool(manager, server, "snapshot", ks)
    listed = await check_listsnapshots(manager, server)
    assert sorted((ks, cf) for _, ks, cf, _, _ in listed) == [(ks, cf) for ks in kses for cf in cfs]
# Ported from scylla-dtest listsnapshots_test.py::TestNodetoolListSnapshots::test_snapshots_of_system_ks.
# A snapshot of the system keyspace is listed with the sizes of its files.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_snapshots_of_system_ks(manager: ScyllaClusterManager):
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    await run_nodetool(manager, server, "snapshot", "system")
    await check_listsnapshots(manager, server)


# Ported from scylla-dtest listsnapshots_test.py::TestNodetoolListSnapshots::test_several_snapshots_for_several_kses.
# Snapshots of three keyspaces of three tables, taken before and after
# 9998 more rows were written to every table, are listed with the sizes of
# their files.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_several_snapshots_for_several_kses(manager: ScyllaClusterManager):
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    cql = cql_one(manager)
    kses, cfs = ["my_ks1", "my_ks2", "my_ks3"], ["my_cf1", "my_cf2", "my_cf3"]
    await populate_keyspaces(cql, kses, cfs)
    for ks in kses:
        await run_nodetool(manager, server, "snapshot", ks)
    for ks in kses:
        for cf in cfs:
            await insert_rows(cql, ks, [cf], 2, 10000)
    for ks in kses:
        await run_nodetool(manager, server, "snapshot", ks)
    await check_listsnapshots(manager, server)


async def insert_rows(cql, ks: str, tables: list[str], start: int, end: int) -> None:
    for t in tables:
        insert = cql.prepare(f"INSERT INTO {ks}.{t} (key, val) VALUES (?, 'asdf')")
        await asyncio.gather(*[cql.run_async(insert, [r]) for r in range(start, end)])
