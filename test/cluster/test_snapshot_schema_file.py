#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# A snapshot holds the CREATE statement of its table in schema.cql, ported
# from scylla-dtest snapshot_test.py: after the keyspace was dropped (its
# files removed) and recreated, schema.cql recreates the table with the same
# description, into which the snapshot's sstables load back with a refresh;
# for tables of all native types, of a UDT and a frozen UDT, and tables with
# quoted upper case names.

import pytest

from test import DEBUG_MODES
from test.cluster.dtest_helpers import cql_one
from test.cluster.test_nodetool_snapshots import (
    NO_AUTO_SNAPSHOT_CONFIG,
    count_rows,
    create_cf,
    describe_table,
    drop_keyspace_and_files,
    get_schema_file_from_snapshot,
    insert_rows,
    ks_opts,
    make_snapshot,
    restore_snapshot_with_refresh,
    restore_table_by_schema_file,
)
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

# The dtest's column types, with its values for three rows.
NATIVE_TYPES = {
    "bigint": ("10000", "1", "2"),
    "boolean": ("true", "true", "false"),
    "blob": ("textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", "bigintAsBlob(1)"),
    "date": ("currentDate()", "currentDate()", "currentDate()"),
    "decimal": ("10.1", "11.1", "12.2"),
    "double": ("10.1000001", "11.111111", "22.22222"),
    "duration": ("89h1m48s", "11h11m11s", "22h22m22s"),
    "float": ("10.10001", "33.33", "44.44"),
    "inet": ("'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"),
    "int": ("100001", "1", "2"),
    "smallint": ("1", "1", "2"),
    "time": ("currentTime()", "currentTime()", "currentTime()"),
    "timestamp": ("currentTimestamp()", "currentTimestamp()", "currentTimestamp()"),
    "timeuuid": ("currentTimeUUID()", "currentTimeUUID()", "currentTimeUUID()"),
    "tinyint": ("1", "2", "5"),
    "uuid": ("uuid()", "uuid()", "uuid()"),
    "varint": ("1", "4", "5"),
    "text": ("'a'", "'A'", "'b'"),
    "varchar": ("'c'", "'d'", "'E'"),
    "ascii": ("'1'", "'c'", "'$'"),
}
CL_TYPES = list(NATIVE_TYPES)
CREATE_ALL_NATIVE_TYPES = "CREATE TYPE all_native_types ({})".format(", ".join(f"cl_{t} {t}" for t in CL_TYPES))


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def init_cluster(manager: ScyllaClusterManager):
    """The dtests' SnapshotTester.init_cluster(): one node without
    auto_snapshot, and a session."""
    server = await manager.server_add(config=NO_AUTO_SNAPSHOT_CONFIG)
    return server, cql_one(manager)


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_restore_data_for_all_native_data_types_from_snapshot_with_refresh.
# A table with a column of every native type.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restore_data_for_all_native_data_types_from_snapshot_with_refresh(manager: ScyllaClusterManager, tmp_path):
    server, cql = await init_cluster(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    columns = "".join(f"cl_{t} {t}, " for t in CL_TYPES)
    await cql.run_async(f"CREATE TABLE ks.native_types_table ({columns} PRIMARY KEY (cl_{CL_TYPES[0]}))")
    for i in range(3):
        names = ", ".join(f"cl_{t}" for t in CL_TYPES)
        values = ", ".join(v[i] for v in NATIVE_TYPES.values())
        await cql.run_async(f"INSERT INTO ks.native_types_table ({names}) VALUES ({values})")
    assert await count_rows(cql, "ks", "native_types_table") == 3
    snapshots_dir = await make_snapshot(manager, server, str(tmp_path), ks="ks", name="basic")
    table_desc = await describe_table(server, "ks", "native_types_table")
    schema_file = get_schema_file_from_snapshot(snapshots_dir, "ks", "native_types_table", "basic")
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await restore_table_by_schema_file(cql, schema_file)
    assert await count_rows(cql, "ks", "native_types_table") == 0
    restored_table_desc = await describe_table(server, "ks", "native_types_table")
    await restore_snapshot_with_refresh(manager, server, snapshots_dir, "ks", "native_types_table", "basic")
    assert await count_rows(cql, "ks", "native_types_table") == 3
    assert table_desc == restored_table_desc


async def create_and_restore_udt_from_snapshot(manager: ScyllaClusterManager, copy_dir: str, use_frozen: bool):
    """The dtests' create_and_restore_udt_from_snapshot(): as above, with a
    table of a UDT of every native type; the type is created again before
    the table is restored. As in the dtest, the session uses ks (create_ks)
    and the type, table and rows are named unqualified."""
    server, cql = await init_cluster(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await cql.run_async("USE ks")
    await cql.run_async(CREATE_ALL_NATIVE_TYPES)
    udt_type = "frozen<all_native_types>" if use_frozen else "all_native_types"
    await cql.run_async(f"CREATE TABLE table_with_udt (cl_{CL_TYPES[0]} {CL_TYPES[0]}, data {udt_type}, PRIMARY KEY (cl_{CL_TYPES[0]}))")
    for i in range(3):
        udt_values = ", ".join(f"cl_{t}: {v[i]}" for t, v in NATIVE_TYPES.items())
        await cql.run_async(f"INSERT INTO table_with_udt (cl_{CL_TYPES[0]}, data) VALUES ({NATIVE_TYPES[CL_TYPES[0]][i]}, {{{udt_values}}})")
    assert await count_rows(cql, "ks", "table_with_udt") == 3
    snapshots_dir = await make_snapshot(manager, server, copy_dir, ks="ks", name="basic")
    table_desc = await describe_table(server, "ks", "table_with_udt")
    schema_file = get_schema_file_from_snapshot(snapshots_dir, "ks", "table_with_udt", "basic")
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await cql.run_async("USE ks")
    await cql.run_async(CREATE_ALL_NATIVE_TYPES)
    await restore_table_by_schema_file(cql, schema_file)
    assert await count_rows(cql, "ks", "table_with_udt") == 0
    restored_table_desc = await describe_table(server, "ks", "table_with_udt")
    await restore_snapshot_with_refresh(manager, server, snapshots_dir, "ks", "table_with_udt", "basic")
    assert await count_rows(cql, "ks", "table_with_udt") == 3
    assert table_desc == restored_table_desc


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_restore_data_from_snapshot_with_udt_with_refresh.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restore_data_from_snapshot_with_udt_with_refresh(manager: ScyllaClusterManager, tmp_path):
    await create_and_restore_udt_from_snapshot(manager, str(tmp_path), use_frozen=False)


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_restore_data_from_snapshot_with_frozen_udt_with_refresh.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restore_data_from_snapshot_with_frozen_udt_with_refresh(manager: ScyllaClusterManager, tmp_path):
    await create_and_restore_udt_from_snapshot(manager, str(tmp_path), use_frozen=True)


async def create_upper_case_table(cql):
    await cql.run_async('CREATE TABLE ks."UPPER_CASE_CF" ( "KEY" int PRIMARY KEY, "VAL" text);')
    await cql.run_async('INSERT INTO ks."UPPER_CASE_CF" ("KEY", "VAL") VALUES (1, \'ASDFG\');')


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_upper_case_of_table_name_is_saved.
# The schema.cql of a table with a quoted upper case name recreates it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_upper_case_of_table_name_is_saved(manager: ScyllaClusterManager, tmp_path):
    server, cql = await init_cluster(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await create_upper_case_table(cql)
    base_snapshot_dir = await make_snapshot(manager, server, str(tmp_path), ks="ks", cf="UPPER_CASE_CF", name="basic")
    schema_file = get_schema_file_from_snapshot(base_snapshot_dir, "ks", "UPPER_CASE_CF", "basic")
    table_desc = await describe_table(server, "ks", '"UPPER_CASE_CF"')
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await restore_table_by_schema_file(cql, schema_file)
    assert await describe_table(server, "ks", '"UPPER_CASE_CF"') == table_desc


# Ported from scylla-dtest snapshot_test.py::TestSchemaFileInSnapshot::test_upper_case_of_table_name_is_saved_mixed_case.
# The same, with a lower case table (ten rows) in the snapshot too, both
# given with -cf.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_upper_case_of_table_name_is_saved_mixed_case(manager: ScyllaClusterManager, tmp_path):
    server, cql = await init_cluster(manager)
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await create_upper_case_table(cql)
    await create_cf(cql, "ks", "cf", "int", {"val": "text"})
    await insert_rows(cql, "ks", ["cf"], 0, 10)
    base_snapshot_dir = await make_snapshot(manager, server, str(tmp_path), ks="ks", cf="UPPER_CASE_CF,cf", name="basic")
    upper_schema_file = get_schema_file_from_snapshot(base_snapshot_dir, "ks", "UPPER_CASE_CF", "basic")
    upper_table_desc = await describe_table(server, "ks", '"UPPER_CASE_CF"')
    lower_schema_file = get_schema_file_from_snapshot(base_snapshot_dir, "ks", "cf", "basic")
    lower_table_desc = await describe_table(server, "ks", "cf")
    await drop_keyspace_and_files(manager, server, cql, "ks")
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts()}")
    await restore_table_by_schema_file(cql, upper_schema_file)
    upper_restored_table_desc = await describe_table(server, "ks", '"UPPER_CASE_CF"')
    await restore_table_by_schema_file(cql, lower_schema_file)
    lower_restored_table_desc = await describe_table(server, "ks", "cf")
    assert upper_table_desc == upper_restored_table_desc
    assert lower_table_desc == lower_restored_table_desc
