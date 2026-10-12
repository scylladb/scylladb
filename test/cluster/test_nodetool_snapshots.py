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

import asyncio
import contextlib
import glob
import logging
import os
import pathlib
import random
import re
import shutil
import uuid

import pytest

from test import DEBUG_MODES
from test.cluster.dtest_helpers import cql_one
from test.cluster.util import cassandra_stress, get_table_dir, run_nodetool, stop_servers_gracefully
from test.cqlpy.util import run_cqlsh_at
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node, and dropped or
# truncated tables are snapshotted (test.py turns auto_snapshot off).
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'tablets_initial_scale_factor': 1, 'auto_snapshot': True}
# The nodetool dtests ran with compaction debug logging.
CMDLINE = ["--logger-log-level", "compaction=debug"]
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
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_listsnapshots_no_snapshots(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    out, err, _ = await run_nodetool(manager, server, "listsnapshots")
    assert not err
    assert "There are no snapshots" in out
    assert await check_listsnapshots(manager, server) == []


# A snapshot of one table is listed with its keyspace, table and size.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_listsnapshots_one_table(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await populate_keyspaces(manager.get_cql(), ["my_ks"], ["my_cf"])
    await run_nodetool(manager, server, "snapshot", "my_ks", "-cf", "my_cf")
    listed = await check_listsnapshots(manager, server)
    assert [(ks, cf) for _, ks, cf, _, _ in listed] == [("my_ks", "my_cf")]


# Snapshots of three keyspaces of three tables each are all listed.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
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
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshots_of_system_ks(manager: ScyllaClusterManager):
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    await run_nodetool(manager, server, "snapshot", "system")
    await check_listsnapshots(manager, server)


# Ported from scylla-dtest listsnapshots_test.py::TestNodetoolListSnapshots::test_several_snapshots_for_several_kses.
# Snapshots of three keyspaces of three tables, taken before and after
# 9998 more rows were written to every table, are listed with the sizes of
# their files.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
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


############################################################################
# Ported from snapshot_test.py: snapshots of keyspaces and tables, restored
# by copying their sstables into upload/ and "nodetool refresh", their
# schema.cql files, concurrent snapshot operations, and "nodetool snapshot"
# options.

# SnapshotTester.init_cluster(): auto_snapshot off.
NO_AUTO_SNAPSHOT_CONFIG = {**SNAPSHOT_CONFIG, "auto_snapshot": False}


def create_cf_statement(ks: str, name: str, key_type: str = "varchar", columns: dict | None = None) -> str:
    """The CREATE statement of the dtests' create_cf() helper."""
    if columns:
        cols = "".join(f", {k} {v}" for k, v in columns.items())
        query = f"CREATE COLUMNFAMILY {ks}.{name} (key {key_type} PRIMARY KEY{cols}) WITH comment='test cf'"
    else:
        query = f"CREATE COLUMNFAMILY {ks}.{name} (key {key_type}, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf'"
    return query + " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}"


async def create_cf(cql, ks: str, name: str, key_type: str = "varchar", columns: dict | None = None) -> None:
    """The dtests' create_cf(): create the table, then read it with a
    "SELECT * ... LIMIT 1" to check it exists."""
    await cql.run_async(create_cf_statement(ks, name, key_type, columns))
    await cql.run_async(f"SELECT * FROM {ks}.{name} LIMIT 1")


async def create_tables(cql, ks: str, count: int) -> list[str]:
    tables = [f"cf{i}" for i in range(count)]
    for t in tables:
        await create_cf(cql, ks, t, "int", {"val": "text"})
    return tables


async def insert_rows(cql, ks: str, tables: list[str], start: int, end: int) -> None:
    for t in tables:
        insert = cql.prepare(f"INSERT INTO {ks}.{t} (key, val) VALUES (?, 'asdf')")
        await asyncio.gather(*[cql.run_async(insert, [r]) for r in range(start, end)])


async def count_rows(cql, ks: str, cf: str) -> int:
    return (await cql.run_async(f"SELECT count(*) from {ks}.{cf}"))[0][0]


async def make_snapshot(manager: ScyllaClusterManager, server, copy_dir: str, ks: str | None = None, cf: str = "",
                        cf_param_name: str = "-cf", name: str | None = None, additional_options: list[str] = []) -> str:
    """Like the dtests' make_snapshot(): run "nodetool snapshot" for all
    keyspaces, a keyspace or some tables, then copy the snapshot directories
    into copy_dir as <ks>/<table dir>/<tag>. Returns copy_dir."""
    args = ["snapshot"]
    if ks:
        args.append(ks)
        if cf:
            args += [cf_param_name, cf]
        if name:
            args += ["-t", name]
    args += additional_options
    await run_nodetool(manager, server, *args)
    data_dir = os.path.join(await manager.server_get_workdir(server.server_id), "data")
    if ks:
        dirs = [d for k in ks.split(",") for d in glob.glob(f"{data_dir}/{k}/*/snapshots/{name or '*'}")]
    else:
        dirs = glob.glob(f"{data_dir}/*/*/snapshots/*")
    for d in dirs:
        save_dir = os.path.join(copy_dir, d.replace("/snapshots/", "/").replace(data_dir + "/", ""))
        shutil.copytree(d, save_dir, dirs_exist_ok=True)
    return copy_dir


def get_cf_snapshot_saved_dir(base_dir: str, ks: str, cf: str, name: str | None = None) -> str:
    dirs = glob.glob(f"{base_dir}/{ks}/{cf}-*/{name}" if name else f"{base_dir}/{ks}/{cf}-*/*/")
    return dirs[0] if dirs else ""


def get_schema_file_from_snapshot(base_dir: str, ks: str, cf: str, name: str | None = None) -> str:
    """The schema.cql of the table's snapshot copied by make_snapshot(),
    which must exist."""
    schema_file = os.path.join(get_cf_snapshot_saved_dir(base_dir, ks, cf, name), "schema.cql")
    assert os.path.exists(schema_file)
    return schema_file


async def restore_snapshot_with_refresh(manager: ScyllaClusterManager, server, base_dir: str, ks: str, cf: str, name: str | None = None) -> None:
    """Copy a table's snapshot sstables into its upload directory and load
    them with "nodetool refresh", like the dtests' helper of that name."""
    upload_dir = os.path.join(await get_table_dir(manager, server, ks, cf), "upload")
    shutil.copytree(get_cf_snapshot_saved_dir(base_dir, ks, cf, name), upload_dir, dirs_exist_ok=True)
    await run_nodetool(manager, server, "refresh", ks, cf)


async def describe_table(server, ks: str, cf: str) -> str:
    """The table's description by cqlsh's "describe table", as the dtests'
    get_table_description() did."""
    res = await asyncio.to_thread(run_cqlsh_at, server.ip_addr, 9042, f"describe table {ks}.{cf}", ["-u", "cassandra", "-p", "cassandra"])
    assert res.returncode == 0, res.stderr
    return res.stdout


async def drop_keyspace_and_files(manager: ScyllaClusterManager, server, cql, ks: str) -> None:
    await cql.run_async(f"DROP KEYSPACE {ks}", timeout=60)
    shutil.rmtree(os.path.join(await manager.server_get_workdir(server.server_id), "data", ks))


async def restore_table_by_schema_file(cql, schema_file: str) -> None:
    with open(schema_file) as f:
        await cql.run_async(f.read())


async def basic_snapshot_and_restore(manager: ScyllaClusterManager, copy_dir: str, tables_number: int, cf_param_name: str):
    """The dtests' basic_snapshot_and_restore(): snapshot a keyspace (or its
    tables given with cf_param_name), write more rows, drop and recreate the
    keyspace (its files removed), and restore the snapshots with refresh:
    the tables have the rows written before the snapshot."""
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    cql = cql_one(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    tables = await create_tables(cql, "ks", tables_number)
    await insert_rows(cql, "ks", tables, 0, 100)
    if cf_param_name:
        snapshot_dir = await make_snapshot(manager, server, copy_dir, ks="ks", cf=",".join(tables), cf_param_name=cf_param_name, name="basic")
    else:
        snapshot_dir = await make_snapshot(manager, server, copy_dir, ks="ks", name="basic")
    await insert_rows(cql, "ks", tables, 100, 200)
    for t in tables:
        assert await count_rows(cql, "ks", t) == 200
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await create_tables(cql, "ks", tables_number)
    for t in tables:
        assert await count_rows(cql, "ks", t) == 0
    for t in tables:
        await restore_snapshot_with_refresh(manager, server, snapshot_dir, "ks", t)
        await run_nodetool(manager, server, "refresh", "ks", t)
    for t in tables:
        assert await count_rows(cql, "ks", t) == 100


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_basic_snapshot_and_restore_with_refresh.
# A snapshot of a keyspace of one table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_basic_snapshot_and_restore_with_refresh(manager: ScyllaClusterManager, tmp_path):
    await basic_snapshot_and_restore(manager, str(tmp_path), 1, "")


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_basic_mulitple_tables_snapshot_and_restore_with_refresh.
# A snapshot of five tables given with -cf.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_basic_multiple_tables_snapshot_and_restore_with_refresh(manager: ScyllaClusterManager, tmp_path):
    await basic_snapshot_and_restore(manager, str(tmp_path), 5, "-cf")


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_basic_mulitple_tables_snapshot_using_column_family.
# A snapshot of five tables given with --column-family.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_basic_multiple_tables_snapshot_using_column_family(manager: ScyllaClusterManager, tmp_path):
    await basic_snapshot_and_restore(manager, str(tmp_path), 5, "--column-family")


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_basic_mulitple_tables_snapshot_using_table.
# A snapshot of five tables given with --table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_basic_multiple_tables_snapshot_using_table(manager: ScyllaClusterManager, tmp_path):
    await basic_snapshot_and_restore(manager, str(tmp_path), 5, "--table")


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_snapshot_for_2kc_and_cf_failure.
# A snapshot of a table of a list of keyspaces fails: only one keyspace is
# allowed with a table. As in the dtest, whose insert and count helpers
# always name keyspace ks, the rows of both sessions go to ks.cf0 and both
# counts read ks.cf0 (ks1.cf0 stays empty).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_for_2kc_and_cf_failure(manager: ScyllaClusterManager):
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    cql = cql_one(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await cql.run_async(f"CREATE KEYSPACE ks1 {ks_opts()}")
    tables = await create_tables(cql, "ks", 1)
    await create_tables(cql, "ks1", 1)
    await insert_rows(cql, "ks", tables, 0, 100)
    await insert_rows(cql, "ks", tables, 0, 100)
    assert await count_rows(cql, "ks", tables[0]) == 100
    assert await count_rows(cql, "ks", tables[0]) == 100
    out, err, rc = await run_nodetool(manager, server, "snapshot", "ks,ks1", "-cf", ",".join(tables), "-t", "basic", check=False)
    assert rc != 0
    expected_error = "Only one keyspace allowed when specifying a column family"
    assert expected_error in out or expected_error in err, (out, err)


async def cleaning_snapshot_by_cf(manager: ScyllaClusterManager, snapshot_by_multiple_cf: bool):
    """The dtests' cleaning_snapshot_by_cf(): deleting a snapshot tag of one
    table through the REST API removes that table's snapshot directory and
    keeps the other table's."""
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    cql = cql_one(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await cql.run_async("CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);")
    await cql.run_async("CREATE TABLE ks.cf1 ( key int PRIMARY KEY, val text);")
    await insert_rows(cql, "ks", ["cf"], 0, 100)
    await insert_rows(cql, "ks", ["cf1"], 0, 100)
    await run_nodetool(manager, server, "flush")
    if snapshot_by_multiple_cf:
        await run_nodetool(manager, server, "snapshot", "ks", "-cf", "cf,cf1", "-t", "per_cf")
    else:
        await run_nodetool(manager, server, "snapshot", "ks", "-t", "per_cf")
    async def in_snapshot(cf):
        return os.path.exists(os.path.join(await get_table_dir(manager, server, "ks", cf), "snapshots", "per_cf"))
    assert await in_snapshot("cf")
    assert await in_snapshot("cf1")
    await manager.api.client.delete("/storage_service/snapshots", host=server.ip_addr, params={"tag": "per_cf", "kn": "ks", "cf": "cf"})
    assert not await in_snapshot("cf")
    assert await in_snapshot("cf1")


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_cleaning_snapshot_created_by_ks.
# The snapshot is of the keyspace.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cleaning_snapshot_created_by_ks(manager: ScyllaClusterManager):
    await cleaning_snapshot_by_cf(manager, snapshot_by_multiple_cf=False)


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_cleaning_snapshot_created_by_multiple_cf.
# The snapshot is of the two tables, given with -cf.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cleaning_snapshot_created_by_multiple_cf(manager: ScyllaClusterManager):
    await cleaning_snapshot_by_cf(manager, snapshot_by_multiple_cf=True)


async def prepare_schemas_and_data(cql, num_ks=1, num_cf=1, num_rows=1, column_length=10):
    """The keyspaces ks<i> of tables table_cf<j> of the parallel snapshot
    dtests, with rows written concurrently."""
    for j in range(num_ks):
        await cql.run_async(f"CREATE KEYSPACE ks{j} {ks_opts()}")
        for k in range(num_cf):
            await create_cf(cql, f"ks{j}", f"table_cf{k}")
            st = cql.prepare(f"INSERT INTO ks{j}.table_cf{k} (key, c, v) VALUES (?, ?, ?)")
            await asyncio.gather(*[cql.run_async(st, [str(i), str(i), f"{i}" * column_length]) for i in range(num_rows)])


class SnapshotOperations:
    """The concurrent snapshot operations of the parallel dtests, each a
    sequence of nodetool commands; run() returns their stderr."""
    def __init__(self, manager, server):
        self.manager, self.server = manager, server

    async def nodetool(self, *args) -> str:
        _, err, _ = await run_nodetool(self.manager, self.server, *args, check=False)
        return err

    async def snapshot_all(self) -> list[str]:
        return [await self.nodetool("snapshot", "-t", str(uuid.uuid4()))]

    async def snapshot_per_table(self, num_ks: int, num_cf: int) -> list[str]:
        return [await self.nodetool("snapshot", f"ks{i}.table_cf{j}", "-t", str(uuid.uuid4())) for i in range(num_ks) for j in range(num_cf)]

    async def clear_all(self) -> list[str]:
        return [await self.nodetool("clearsnapshot")]

    async def clear_per_keyspace(self, num_ks: int) -> list[str]:
        return [await self.nodetool("clearsnapshot", f"ks{i}") for i in range(num_ks)]

    async def list(self) -> list[str]:
        return [await self.nodetool("listsnapshots")]


async def run_in_parallel(*ops) -> None:
    """Run the operations concurrently; none may print to stderr."""
    for errs in await asyncio.gather(*ops):
        assert not any(errs), errs


# Concurrent per-table snapshots and per-keyspace clears of one keyspace.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_and_clear_one_keyspace(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql())
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_per_table(1, 1)
    await run_in_parallel(ops.snapshot_per_table(1, 1), ops.clear_per_keyspace(1))


# The same on ten keyspaces of one table each.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_and_clear_10_keyspaces(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=10)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_per_table(10, 1)
    await run_in_parallel(ops.snapshot_per_table(10, 1), ops.clear_per_keyspace(10))


# Concurrent per-table snapshots, per-keyspace clears and a clear of all
# snapshots, on ten keyspaces of ten tables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_clear_and_clear_all(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=10, num_cf=10)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_per_table(10, 10)
    await run_in_parallel(ops.snapshot_per_table(10, 10), ops.clear_per_keyspace(10), ops.clear_all())


# Concurrent snapshots and clears of all keyspaces.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_and_clear_all_keyspaces(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=10, num_cf=10)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_all()
    await run_in_parallel(ops.snapshot_all(), ops.clear_all(), ops.snapshot_all(), ops.clear_all())


# Ported from scylla-dtest snapshot_test.py::TestParallelSnapshotOperations::test_parallel_operations_create_clear_per_ks_and_all.
# Concurrent snapshots and clears of all keyspaces, per-table snapshots and
# per-keyspace clears, on ten keyspaces of ten tables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_operations_create_clear_per_ks_and_all(manager: ScyllaClusterManager):
    server = await manager.server_add(config=SNAPSHOT_CONFIG)
    await prepare_schemas_and_data(cql_one(manager), num_ks=10, num_cf=10, num_rows=1, column_length=10)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_all()
    await run_in_parallel(ops.snapshot_all(), ops.clear_all(), ops.snapshot_per_table(10, 10), ops.clear_per_keyspace(10),
                          ops.snapshot_all(), ops.clear_all())


# Concurrent snapshots, clears and listings of all keyspaces, on 30
# keyspaces of ten tables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_list_and_clear_all_keyspaces(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=30, num_cf=10, num_rows=10)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_all()
    await run_in_parallel(ops.snapshot_all(), ops.clear_all(), ops.list(), ops.snapshot_all(), ops.clear_all(), ops.list())


# The operations mixed concurrently on 15 keyspaces of 15 tables with rows
# of 1000-byte values. The dtest wrote 1000 rows per table; the port
# writes 100, to keep the test short in debug mode.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_operations_with_data(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=15, num_cf=15, num_rows=100, column_length=1000)
    ops = SnapshotOperations(manager, server)
    await ops.snapshot_all()
    await run_in_parallel(ops.snapshot_per_table(15, 15), ops.clear_per_keyspace(15), ops.snapshot_all(), ops.list(), ops.clear_all())


# The operations mixed concurrently while listings and clears of all
# snapshots repeat, on the same data as above.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_parallel_snapshot_operations_with_periodic_list_and_clear(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await prepare_schemas_and_data(manager.get_cql(), num_ks=15, num_cf=15, num_rows=100, column_length=1000)
    ops = SnapshotOperations(manager, server)
    done = asyncio.Event()
    async def periodically(op, interval):
        errs = []
        while not done.is_set():
            errs += await op()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(done.wait(), interval)
        return errs
    await ops.snapshot_all()
    monitor = asyncio.create_task(periodically(ops.list, 1))
    clearer = asyncio.create_task(periodically(ops.clear_all, 2))
    await run_in_parallel(ops.snapshot_all(), ops.snapshot_per_table(15, 15), ops.clear_all(), ops.snapshot_all())
    done.set()
    for t in [monitor, clearer]:
        assert not any(await t)


async def schema_file_created(manager: ScyllaClusterManager, copy_dir: str, tables_number: int):
    """A snapshot of the tables of keyspace ks (given with -cf) has a
    schema.cql for each; after the keyspace is dropped (its files removed)
    and recreated, each schema.cql recreates its table with the same
    description."""
    server = await manager.server_add(config=NO_AUTO_SNAPSHOT_CONFIG)
    cql = cql_one(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    if tables_number == 1:
        tables = ["cf"]
        await create_cf(cql, "ks", "cf", "int", {"val": "text"})
    else:
        tables = await create_tables(cql, "ks", tables_number)
    await insert_rows(cql, "ks", tables, 0, 100)
    base_dir = await make_snapshot(manager, server, copy_dir, ks="ks", cf=",".join(tables), name="basic")
    schema_files, descs = [], []
    for t in tables:
        schema_files.append(get_schema_file_from_snapshot(base_dir, "ks", t, "basic"))
        descs.append(await describe_table(server, "ks", t))
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    for t, schema_file, desc in zip(tables, schema_files, descs):
        await restore_table_by_schema_file(cql, schema_file)
        assert await describe_table(server, "ks", t) == desc


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_schema_file_created.
# A snapshot of one table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_schema_file_created(manager: ScyllaClusterManager, tmp_path):
    await schema_file_created(manager, str(tmp_path), 1)


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_schema_file_created_by_multiple_tables.
# A snapshot of five tables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_schema_file_created_by_multiple_tables(manager: ScyllaClusterManager, tmp_path):
    await schema_file_created(manager, str(tmp_path), 5)


# Restore a table dropped with its keyspace: recreate it from the schema.cql
# of its snapshot (empty, same description), then load the snapshot's
# sstables with refresh: the rows written before the snapshot are back.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restore_by_schema_file_with_refresh(manager: ScyllaClusterManager, tmp_path):
    server = await manager.server_add(config={**CONFIG, "auto_snapshot": False})
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await create_cf(cql, "ks", "cf", "int", {"val": "text"})
    await insert_rows(cql, "ks", ["cf"], 0, 100)
    assert await count_rows(cql, "ks", "cf") == 100
    snapshot_dir = await make_snapshot(manager, server, str(tmp_path), ks="ks", cf="cf", name="basic")
    schema_file = os.path.join(get_cf_snapshot_saved_dir(snapshot_dir, "ks", "cf", "basic"), "schema.cql")
    table_schema = await describe_table(server, "ks", "cf")
    await insert_rows(cql, "ks", ["cf"], 100, 200)
    assert await count_rows(cql, "ks", "cf") == 200
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await cql.run_async(open(schema_file).read())
    assert await describe_table(server, "ks", "cf") == table_schema
    assert await count_rows(cql, "ks", "cf") == 0
    await restore_snapshot_with_refresh(manager, server, snapshot_dir, "ks", "cf", "basic")
    await run_nodetool(manager, server, "refresh", "ks", "cf")
    assert await count_rows(cql, "ks", "cf") == 100


# The schema.cql of a snapshot taken after ALTER TABLE ADD has the new
# column and differs from the one taken before; it recreates the altered
# table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_schema_file_has_altered_table(manager: ScyllaClusterManager, tmp_path):
    server = await manager.server_add(config={**CONFIG, "auto_snapshot": False})
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await create_cf(cql, "ks", "cf", "int", {"val": "text"})
    await insert_rows(cql, "ks", ["cf"], 0, 100)
    base_dir = await make_snapshot(manager, server, str(tmp_path / "1"), ks="ks", cf="cf", name="basic")
    schema_file = os.path.join(get_cf_snapshot_saved_dir(base_dir, "ks", "cf", "basic"), "schema.cql")
    table_desc = await describe_table(server, "ks", "cf")
    await cql.run_async("ALTER TABLE ks.cf ADD val1 text")
    base_dir = await make_snapshot(manager, server, str(tmp_path / "2"), ks="ks", cf="cf", name="basic1")
    new_schema_file = os.path.join(get_cf_snapshot_saved_dir(base_dir, "ks", "cf", "basic1"), "schema.cql")
    altered_desc = await describe_table(server, "ks", "cf")
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await cql.run_async(open(new_schema_file).read())
    restored = await describe_table(server, "ks", "cf")
    assert restored == altered_desc
    assert restored != table_desc
    assert open(schema_file).read() != open(new_schema_file).read()


SNAP_OPTS_KEYSPACES = [f"ks{i}" for i in range(3)]


async def prepare_snapshot_options(manager: ScyllaClusterManager):
    """The TestSnapshotOptions dtests' setup: a node without auto_snapshot
    and three keyspaces of two tables with a row each. Returns the server,
    a session and the names of the system keyspaces."""
    server = await manager.server_add(config={**CONFIG, "auto_snapshot": False})
    cql = manager.get_cql()
    system_keyspaces = [r.keyspace_name for r in await cql.run_async("select keyspace_name from system_schema.keyspaces")]
    # With raft-based topology, auth keeps its data in system and leaves the
    # legacy system_auth keyspace empty, so it has no snapshot (as in the dtest).
    if "system_auth" in system_keyspaces:
        system_keyspaces.remove("system_auth")
    await prepare_schemas_and_data(cql, num_ks=3, num_cf=2)
    return server, cql, system_keyspaces


def snapshot_keyspace_names(snapshot_root: str) -> set[str]:
    return {p.stem for p in pathlib.Path(snapshot_root).glob("*")}


# "nodetool snapshot" without keyspaces snapshots every keyspace, the
# system ones too.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_defaults_to_all_keyspaces(manager: ScyllaClusterManager, tmp_path):
    server, cql, system_keyspaces = await prepare_snapshot_options(manager)
    root = await make_snapshot(manager, server, str(tmp_path))
    assert snapshot_keyspace_names(root) == set(SNAP_OPTS_KEYSPACES + system_keyspaces)


async def snapshot_of_specified_keyspaces_only(manager: ScyllaClusterManager, copy_dir: str, with_kc: bool):
    """A snapshot of all keyspaces but a random one of ks0..ks2, given as
    arguments or with -kc, contains exactly those keyspaces."""
    server, _, _ = await prepare_snapshot_options(manager)
    keyspaces = SNAP_OPTS_KEYSPACES.copy()
    keyspaces.remove(random.choice(keyspaces))
    if with_kc:
        root = await make_snapshot(manager, server, copy_dir, additional_options=["-kc", ",".join(keyspaces)])
    else:
        root = await make_snapshot(manager, server, copy_dir, ks=",".join(keyspaces))
    assert snapshot_keyspace_names(root) == set(keyspaces)


# Ported from scylla-dtest snapshot_test.py::TestSnapshotOptions::test_snapshot_of_specified_keyspaces_only.
# The keyspaces are given as arguments.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_of_specified_keyspaces_only(manager: ScyllaClusterManager, tmp_path):
    await snapshot_of_specified_keyspaces_only(manager, str(tmp_path), with_kc=False)


# Ported from scylla-dtest snapshot_test.py::TestSnapshotOptions::test_snapshot_of_specified_keyspaces_only_with_kc_list_option.
# The keyspaces are given with -kc.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_of_specified_keyspaces_only_with_kc_list_option(manager: ScyllaClusterManager, tmp_path):
    await snapshot_of_specified_keyspaces_only(manager, str(tmp_path), with_kc=True)


# "-t <tag>" names the snapshot, which nodetool prints.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_tagging(manager: ScyllaClusterManager):
    server, _, _ = await prepare_snapshot_options(manager)
    out, _, _ = await run_nodetool(manager, server, "snapshot", "-t", "charybdis")
    assert "Snapshot directory: charybdis" in out


# "--skip-flush" snapshots without flushing the memtable: per "nodetool
# tablestats", the memtable keeps its data and was not switched.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_snapshot_skip_flush(manager: ScyllaClusterManager, tmp_path):
    server, cql, _ = await prepare_snapshot_options(manager)
    insert = cql.prepare("INSERT INTO ks0.table_cf0 (key, c, v) VALUES (?, 'sometext', 'someothertext')")
    for i in range(1, 3):
        await cql.run_async(insert, [str(i)])
    async def memtable_stats():
        out, _, _ = await run_nodetool(manager, server, "tablestats", "ks0.table_cf0")
        return (int(re.search(r"Memtable data size:\s*(\d+)", out).group(1)),
                int(re.search(r"Memtable switch count:\s*(\d+)", out).group(1)))
    size_pre, switches_pre = await memtable_stats()
    await make_snapshot(manager, server, str(tmp_path), ks="ks0", cf="table_cf0", additional_options=["--skip-flush"])
    size_post, switches_post = await memtable_stats()
    assert size_post >= size_pre
    assert switches_post == switches_pre


############################################################################
# Ported from nodetool_additional_test.py: "nodetool snapshot" of all
# keyspaces, a keyspace or a table, with and without a tag, checked against
# the snapshot directories and "nodetool listsnapshots", and cleared.

# Keyspace directories that are not keyspaces.
RESERVED_NAMES = ["view_pending_updates"]


async def snapshot_listed(manager: ScyllaClusterManager, server, ks: str, snapshot: str) -> bool:
    out, _, _ = await run_nodetool(manager, server, "listsnapshots")
    return bool(re.findall(snapshot + r"\s+" + ks, out, re.MULTILINE))


async def listed_snapshots(manager: ScyllaClusterManager, server) -> list:
    out, _, _ = await run_nodetool(manager, server, "listsnapshots")
    return re.findall(r"^\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([\d]+\s[^\s]+)\s+([\d]+\s[^\s]+)\s*$", out, re.MULTILINE)


def check_snapshot_dirs(data_dir: str, snapshot: str, keyspace: str | None = None, column_family: str | None = None) -> None:
    """Every table of the snapshotted keyspaces (all, or the given one, or
    just the given table) has the snapshot directory with a manifest.json,
    and no other table has one. The system schema tables are skipped, as in
    the dtests."""
    for ks in os.listdir(data_dir):
        if not os.path.isdir(os.path.join(data_dir, ks)) or ks in RESERVED_NAMES:
            continue
        for c in os.listdir(os.path.join(data_dir, ks)):
            if ks.startswith("system") and c.startswith("schema"):
                continue
            snapshot_dir = os.path.join(data_dir, ks, c, "snapshots", snapshot)
            if not keyspace or (keyspace == ks and (not column_family or c.startswith(column_family))):
                assert os.path.isdir(snapshot_dir), f"missing {snapshot_dir}"
                assert "manifest.json" in os.listdir(snapshot_dir), f"missing manifest.json in {snapshot_dir}"
            else:
                assert not os.path.isdir(snapshot_dir), f"unexpected {snapshot_dir}"


# A snapshot of all keyspaces, after 1000 stress writes, has a directory
# with a manifest.json in every table and is listed for keyspace1 and
# system; "nodetool clearsnapshot" removes it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_global_snapshot(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    await cassandra_stress(manager, server, "write", "cl=ALL", "no-warmup", "n=1000")
    assert await listed_snapshots(manager, server) == []
    out, _, _ = await run_nodetool(manager, server, "snapshot")
    [snapshot] = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
    check_snapshot_dirs(os.path.join(await manager.server_get_workdir(server.server_id), "data"), snapshot)
    assert await snapshot_listed(manager, server, "keyspace1", snapshot)
    assert await snapshot_listed(manager, server, "system", snapshot)
    await run_nodetool(manager, server, "clearsnapshot")
    assert not await snapshot_listed(manager, server, "keyspace1", snapshot)


# A tagged snapshot of all keyspaces, of keyspace1, of
# system_schema.tables given with -cf, or of system_schema.tables given
# with -kc, after 1000 stress writes: nodetool prints the tag, exactly the
# snapshotted tables have its directory, it is listed, and "nodetool
# clearsnapshot -t" removes it.
async def snapshot_tag(manager: ScyllaClusterManager, keyspace=None, kc=None, column_family=None):
    """The dtests' tst_snapshot("snaptag", ...)."""
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    await cassandra_stress(manager, server, "write", "cl=ALL", "no-warmup", "n=1000")
    assert await listed_snapshots(manager, server) == []
    tag = "snaptag"
    args = ["snapshot", "-t", tag]
    if kc:
        args += ["-kc", kc]
    if column_family:
        args += ["-cf", column_family]
    if keyspace:
        args.append(keyspace)
    out, _, _ = await run_nodetool(manager, server, *args)
    assert re.findall(r"Snapshot directory:\s+([^\s]+)", out, re.MULTILINE)[0] == tag
    if kc:
        keyspace, column_family = kc.split(".")
    check_snapshot_dirs(os.path.join(await manager.server_get_workdir(server.server_id), "data"), tag, keyspace, column_family)
    if keyspace:
        assert await snapshot_listed(manager, server, keyspace, tag)
    else:
        assert await snapshot_listed(manager, server, "keyspace1", tag)
        assert await snapshot_listed(manager, server, "system", tag)
    await run_nodetool(manager, server, "clearsnapshot", "-t" + tag)
    assert not await snapshot_listed(manager, server, "keyspace1", tag)



# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_snapshot_tag.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_tag(manager: ScyllaClusterManager):
    await snapshot_tag(manager)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_snapshot_tag_keyspace.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_tag_keyspace(manager: ScyllaClusterManager):
    await snapshot_tag(manager, keyspace="keyspace1")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_snapshot_tag_keyspace_cf.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_tag_keyspace_cf(manager: ScyllaClusterManager):
    await snapshot_tag(manager, keyspace="system_schema", column_family="tables")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_snapshot_tag_kc.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_tag_kc(manager: ScyllaClusterManager):
    await snapshot_tag(manager, kc="system_schema.tables")


# After "nodetool clearsnapshot" removed a snapshot of all keyspaces,
# another one can be taken.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_after_clearsnapshot(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    await cassandra_stress(manager, server, "write", "cl=ALL", "no-warmup", "n=1000")
    out, _, _ = await run_nodetool(manager, server, "snapshot")
    [snapshot] = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
    assert await snapshot_listed(manager, server, "keyspace1", snapshot)
    await run_nodetool(manager, server, "clearsnapshot")
    assert not await snapshot_listed(manager, server, "keyspace1", snapshot)
    out, _, _ = await run_nodetool(manager, server, "snapshot")
    [snapshot] = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
    assert await snapshot_listed(manager, server, "keyspace1", snapshot)
