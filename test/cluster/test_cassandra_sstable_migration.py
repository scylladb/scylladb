#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Migration of data from Cassandra by loading the sstables Cassandra wrote
# through the upload directory and "nodetool refresh", ported from
# scylla-dtest migration_test.py (MigrationTestBase / TestMigration), one
# test per dtest. Each test runs for the dtest's four versions - Cassandra
# 2.1 (ka), 2.2 (la), 3.0 with mc and with md sstables - whose sstables are
# in test/resource/sstables/cassandra_migration/<format>/<dataset>. Like the
# dtests, a single node (SimpleSnitch, datacenter1/rack1) runs with hinted
# handoff off and the commitlog synced in batch mode, and the keyspace "ks"
# (RF=1, vnodes) holds the table "cf".

import asyncio
import datetime
import glob
import logging
import os
import re
import shutil
import subprocess
import time
import uuid

import pytest
from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env

logger = logging.getLogger(__name__)

SSTABLES_DIR = os.path.join(os.path.dirname(__file__), '..', 'resource', 'sstables', 'cassandra_migration')

# The versions of TestMigration's select_version fixture, and the sstable
# format of each one's sstables.
VERSIONS = {"2_1_x": "ka", "2_2_x": "la", "3_0_mc": "mc", "3_0_md": "md"}
# BaseHelpers.populate_cluster(): hinted handoff off, batch commitlog
# (ccm's set_configuration_options(batch_commitlog=True)); populate(1) is the
# SimpleSnitch.
CONFIG = {"hinted_handoff_enabled": False, "commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 5,
          "endpoint_snitch": "SimpleSnitch", "tablets_mode_for_new_keyspaces": "disabled"}


@pytest.fixture(params=list(VERSIONS))
def version(request):
    return request.param


# The dtests run every MigrationTestBase test in two classes: TestMigration,
# and TestMigrationUpgradeSSTables, whose load_migrated_tables() also runs
# "nodetool upgradesstables ks cf" and checks the sstable version grew.
UPGRADE_SSTABLES = False


@pytest.fixture(params=[False, True], ids=["migration", "upgrade_sstables"], autouse=True)
def upgrade_sstables(request):
    global UPGRADE_SSTABLES
    UPGRADE_SSTABLES = request.param
    yield request.param
    UPGRADE_SSTABLES = False


SSTABLE_VERSION_RE = re.compile(r"(\w+)-[^-]+-(.+)\.(db|txt|sha1|crc32)")


def get_sstable_version(cf_dir, assert_only_one_version=True):
    """MigrationTestBase.get_sstable_version(): the sstable format of the
    files in the directory."""
    versions = list({m.group(1) for f in os.listdir(cf_dir) if (m := SSTABLE_VERSION_RE.search(f))})
    if assert_only_one_version:
        assert len(versions) == 1, versions
    return versions[0] if versions else None


async def nodetool(manager: ScyllaClusterManager, server, *args):
    """Runs "scylla nodetool" against the node, like ccm's node.nodetool()."""
    exe = await manager.server_get_exe(server.server_id)
    res = await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", server.ip_addr, *args], capture_output=True, text=True)
    assert res.returncode == 0, f"nodetool {' '.join(args)} failed: {res.stdout} {res.stderr}"
    return res.stdout


def session(manager: ScyllaClusterManager, server, keyspace=None):
    """The dtests' patient_cql_connection(node[, keyspace])."""
    cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    if keyspace:
        cql.set_keyspace(keyspace)
    return cql


async def start_cluster_and_get_node1(manager: ScyllaClusterManager, config=None):
    """BaseHelpers.start_cluster_and_get_node1(): populate(1) and start."""
    return await manager.server_add(config={**CONFIG, **(config or {})},
                                    property_file={"dc": "datacenter1", "rack": "rack1"})


async def create_ks_and_cf(manager, node, columns=None, compression=None, compact_storage=False, query=None):
    """BaseHelpers.create_ks_and_cf(): keyspace ks (RF=1), the table (the
    dtests' create_cf("cf", read_repair=0.0, ...) or the given statements),
    and "nodetool flush -- ks"."""
    cql = session(manager, node)
    try:
        cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        cql.execute("USE ks")
        if isinstance(query, str):
            cql.execute(query)
            time.sleep(0.2)
        elif query is not None:
            for q in query:
                cql.execute(q)
                time.sleep(0.2)
        else:
            if columns:
                schema = "key varchar PRIMARY KEY" + "".join(f", {k} {v}" for k, v in columns.items())
            else:
                schema = "key varchar, c varchar, v varchar, PRIMARY KEY(key, c)"
            q = (f"CREATE COLUMNFAMILY cf ({schema}) WITH comment='test cf' "
                 "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}")
            if compression is not None:
                q += f" AND compression = {{ 'sstable_compression': '{compression}Compressor' }}"
            else:
                q += " AND compression = {}"
            q += " AND read_repair_chance=0.000000"
            if compact_storage:
                q += " AND COMPACT STORAGE"
            cql.execute(q)
            cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
            cql.execute("SELECT * FROM cf LIMIT 1")
    finally:
        cql.cluster.shutdown()
    await nodetool(manager, node, "flush", "--", "ks")


async def load_migrated_tables(manager, node, version, dataset, ks="ks", cf="cf"):
    """BaseHelpers.load_migrated_tables(): copy the files of the dataset to
    the table's upload directory and "nodetool refresh -- ks cf"."""
    workdir = await manager.server_get_workdir(node.server_id)
    upload_dir = os.path.join(glob.glob(os.path.join(workdir, 'data', ks, f'{cf}-*'))[0], 'upload')
    os.makedirs(upload_dir, exist_ok=True)
    src_dir = os.path.join(SSTABLES_DIR, VERSIONS[version], dataset)
    for name in os.listdir(src_dir):
        if os.path.isfile(os.path.join(src_dir, name)):
            shutil.copy2(os.path.join(src_dir, name), os.path.join(upload_dir, name))
    await nodetool(manager, node, "refresh", "--", ks, cf)
    if UPGRADE_SSTABLES:
        # TestMigrationUpgradeSSTables.load_migrated_tables()
        cf_dir = os.path.dirname(upload_dir)
        before_sstable_version = get_sstable_version(src_dir, assert_only_one_version=False)
        await nodetool(manager, node, "upgradesstables", ks, cf)
        await nodetool(manager, node, "flush")
        after_sstable_version = get_sstable_version(cf_dir)
        assert after_sstable_version > before_sstable_version or (
            before_sstable_version == after_sstable_version and after_sstable_version in ["mc", "md"]), \
            f"upgradesstable failed to upgrade sstables [before_version={before_sstable_version} after_version={after_sstable_version}]"


def check_number_of_rows(manager, node, expected):
    """MigrationTestBase.check_number_of_rows()."""
    cql = session(manager, node, "ks")
    try:
        result = list(cql.execute("SELECT COUNT(*) FROM cf"))
        assert result[0].count == expected, f"Expected {expected} rows. Got {list(cql.execute('SELECT * FROM ks.cf'))}"
    finally:
        cql.cluster.shutdown()


def get_all_rows_for_check(manager, node):
    cql = session(manager, node, "ks")
    try:
        return list(cql.execute("SELECT * FROM ks.cf"))
    finally:
        cql.cluster.shutdown()


async def run_basic_migration_test(manager, version, dataset, row_content, compression=None, compact_storage=False, sleep=0, query=None):
    """BaseHelpers._run_basic_migration_test()."""
    config = {"enable_create_table_with_compact_storage": True} if compact_storage else None
    node1 = await start_cluster_and_get_node1(manager, config)
    await create_ks_and_cf(manager, node1, columns={"c1": "text", "c2": "text"}, compression=compression,
                           compact_storage=compact_storage, query=query)
    await load_migrated_tables(manager, node1, version, dataset)
    time.sleep(sleep)
    check_number_of_rows(manager, node1, 0 if row_content is None else 1)
    if row_content is not None:
        result = get_all_rows_for_check(manager, node1)
        for k, error_string in [("key", "check partition key"), ("c1", "check column c1"), ("c2", "check column c2"),
                                ("pk", "check partition key"), ("ck", "check clustering key"), ("v1", "check column v1")]:
            if k in row_content:
                assert getattr(result[0], k) == row_content[k], error_string


async def run_migration_test_for_collection(manager, version, dataset, collection_type, collection_content):
    """MigrationTestBase._run_migration_test_for_collection()."""
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, {"messages": collection_type})
    await load_migrated_tables(manager, node1, version, dataset)
    check_number_of_rows(manager, node1, len(collection_content))
    result = get_all_rows_for_check(manager, node1)
    for idx, (key, value) in enumerate(collection_content.items()):
        assert result[idx].key == key, "check partition key"
        assert result[idx].messages == value, "check messages"


MIGRATION_GATE = [pytest.mark.tier3,
                  pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")]


def migration_gate(f):
    for mark in reversed(MIGRATION_GATE):
        f = mark(f)
    return f


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_without_compression.
# INSERT INTO ks.cf (key, c2) VALUES ('abc', 'cde') - uncompressed.
@migration_gate
async def test_migrate_sstable_without_compression(manager: ScyllaClusterManager, version):
    await run_basic_migration_test(manager, version, "without_compression", {"key": "abc", "c1": None, "c2": "cde"})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_lz4_compression.
# INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde') - LZ4.
@migration_gate
async def test_migrate_sstable_with_lz4_compression(manager: ScyllaClusterManager, version):
    await run_basic_migration_test(manager, version, "with_lz4_compression", {"key": "a", "c1": "abc", "c2": "cde"}, compression="LZ4")


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_compact_storage.
@migration_gate
async def test_migrate_sstable_with_compact_storage(manager: ScyllaClusterManager, version):
    await run_basic_migration_test(manager, version, "with_compact_storage", {"key": "a", "c1": "abc", "c2": "cde"}, compact_storage=True)


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_compact_storage_and_composite_key.
# A compact storage table with a clustering key.
@migration_gate
async def test_migrate_sstable_with_compact_storage_and_composite_key(manager: ScyllaClusterManager, version):
    query = "CREATE COLUMNFAMILY  ks.cf (pk varchar, ck1 text, v1 text, PRIMARY KEY (pk, ck1)) WITH COMPACT STORAGE"
    await run_basic_migration_test(manager, version, "with_compact_storage_and_composite_key", {"pk": "a", "ck1": "b", "v1": "abc"},
                                   compact_storage=True, query=query)


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_cell_tombstone.
# INSERT (key, c1, c2) VALUES ('a', 'abc', 'cde'); flush; DELETE c2 FROM ks.cf where key = 'a'.
@migration_gate
async def test_migrate_sstable_with_cell_tombstone(manager: ScyllaClusterManager, version):
    await run_basic_migration_test(manager, version, "with_cell_tombstone", {"key": "a", "c1": "abc", "c2": None})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_range_boundary_tombstone.
# Adjacent range tombstones written as a boundary marker (scylla-tools-java#205).
@migration_gate
async def test_migrate_sstable_with_range_boundary_tombstone(manager: ScyllaClusterManager, version):
    if version in ("2_1_x", "2_2_x"):
        skip_env("Test not supported in version 2.1.x or 2.2.x")
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, query="CREATE COLUMNFAMILY ks.cf (pk int, ck int, PRIMARY KEY (pk, ck))")
    await load_migrated_tables(manager, node1, version, "with_range_boundary_tombstone")
    check_number_of_rows(manager, node1, 1)
    result = get_all_rows_for_check(manager, node1)
    assert result[0].pk == 1, "check partition key"
    assert result[0].ck == 1, "check clustering key"


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_range_tombstone.
# Three rows written, flush, two of them deleted.
@migration_gate
async def test_migrate_sstable_with_range_tombstone(manager: ScyllaClusterManager, version):
    await run_basic_migration_test(manager, version, "with_range_tombstone", {"key": "c", "c1": "abc", "c2": "cde"})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_clustering_key_range_tombstone.
# A clustering range deleted (scylla-tools-java#204).
@migration_gate
async def test_migrate_sstable_with_clustering_key_range_tombstone(manager: ScyllaClusterManager, version):
    if version in ("2_1_x", "2_2_x"):
        skip_env("Test not supported in version 2.1.x or 2.2.x")
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, query="CREATE COLUMNFAMILY ks.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
    await load_migrated_tables(manager, node1, version, "with_clustering_key_range_tombstone")
    check_number_of_rows(manager, node1, 2)
    result = get_all_rows_for_check(manager, node1)
    assert (result[0].pk, result[0].ck, result[0].v) == (1, 1, 1), "check row 1"
    assert (result[1].pk, result[1].ck, result[1].v) == (1, 5, 1), "check row 2"


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_wide_row.
# Rows ('a', 'a', 'b'), ('b', 'a', 'a'), ('b', 'b', 'b') of create_cf()'s (key, c, v) table.
@migration_gate
async def test_migrate_sstable_with_wide_row(manager: ScyllaClusterManager, version):
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1)
    await load_migrated_tables(manager, node1, version, "with_wide_rows")
    check_number_of_rows(manager, node1, 3)
    result = get_all_rows_for_check(manager, node1)
    for row, (key, c, v) in zip(result, [("a", "a", "b"), ("b", "a", "a"), ("b", "b", "b")]):
        assert row.key == key, "check partition key"
        assert row.c == c, "check column c1"
        assert row.v == v, "check column c1"


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_collection_set.
@migration_gate
async def test_migrate_sstable_with_collection_set(manager: ScyllaClusterManager, version):
    await run_migration_test_for_collection(manager, version, "with_collection_set", "set<text>",
                                            {"a": {"hello world", "scylla", "scylladb", "test"}})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_collection_list.
@migration_gate
async def test_migrate_sstable_with_collection_list(manager: ScyllaClusterManager, version):
    await run_migration_test_for_collection(manager, version, "with_collection_list", "list<text>",
                                            {"a": ["scylladb", "scylla", "hello world", "test"]})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_collection_map.
@migration_gate
async def test_migrate_sstable_with_collection_map(manager: ScyllaClusterManager, version):
    await run_migration_test_for_collection(manager, version, "with_collection_map", "map<varchar, text>",
                                            {"a": {"a": "value1", "b": "value2"}})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_frozen_collection_map.
@migration_gate
async def test_migrate_sstable_with_frozen_collection_map(manager: ScyllaClusterManager, version):
    await run_migration_test_for_collection(manager, version, "with_frozen_collection_map", "frozen<map<varchar, text>>",
                                            {"a": {"a": "value1", "b": "value2"}, "b": {"a": "value1", "b": "value2"}})


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_static_cell.
# INSERT (key, s, i) VALUES ('k', 'old', 0); INSERT (key, s, i) VALUES ('k', 'new', 1).
@migration_gate
async def test_migrate_sstable_with_static_cell(manager: ScyllaClusterManager, version):
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, query="CREATE COLUMNFAMILY ks.cf (key varchar, s text STATIC, i int, PRIMARY KEY (key, i)) "
                                                 "WITH comment='test cf' AND read_repair_chance=0.000000")
    await load_migrated_tables(manager, node1, version, "with_static_cell")
    check_number_of_rows(manager, node1, 2)
    result = get_all_rows_for_check(manager, node1)
    for row, i in zip(result, [0, 1]):
        assert row.key == "k", "check partition key"
        assert row.i == i, "check clustering key"
        assert row.s == "new", "check static cell"


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_overlapping_tombstones.
# A partition tombstone of ck1='aaa' overlapping a row tombstone inside it.
@migration_gate
async def test_migrate_sstable_with_overlapping_tombstones(manager: ScyllaClusterManager, version):
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, query="create COLUMNFAMILY  ks.cf (pk text, ck1 text, ck2 text, data text, primary key(pk, ck1, ck2))")
    await load_migrated_tables(manager, node1, version, "with_overlapping_tombstones")
    check_number_of_rows(manager, node1, 1)
    result = get_all_rows_for_check(manager, node1)
    assert (result[0].pk, result[0].ck1, result[0].ck2, result[0].data) == ("pk", "bbb", "aaa", "fff")


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_user_defined_types.
# A column of a UDT nesting another UDT.
@migration_gate
async def test_migrate_sstable_with_user_defined_types(manager: ScyllaClusterManager, version):
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, query=["create type ks.ut1 (f1 text, f2 text)", "create type ks.ut2 (f1 text, f2 frozen<ut1>)",
                                                  "create table ks.cf (id uuid primary key, c frozen<ut2>)"])
    await load_migrated_tables(manager, node1, version, "with_user_types")
    check_number_of_rows(manager, node1, 2)
    result = get_all_rows_for_check(manager, node1)
    assert result[0].id == uuid.UUID("62c36092-82a1-3a00-93d1-46196ee77202"), "check id row 0"
    assert result[0].c == ("a", ("b", "c")), "check c row 0"
    assert result[1].id == uuid.UUID("62c36092-82a1-3a00-93d1-46196ee77242"), "check id row 1"
    assert result[1].c == ("x", ("y", "z")), "check c row 1"


VARIANT_TYPES_TABLE = ("CREATE COLUMNFAMILY ks.cf (aascii ascii,abigint bigint,ablob blob,aboolean boolean,adouble double,"
                       "adecimal decimal,afloat float,ainet inet,aint int,atext text,atimestamp timestamp,atimeuuid timeuuid,"
                       "auuid uuid,avarchar varchar,avarint varint,alist list<int>,amap map<int,int>,aset set<int>,"
                       "PRIMARY KEY (aascii, abigint)) WITH comment='test cf' AND read_repair_chance=0.000000")


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_variant_data_types,
# run by TestMigration::test_migrate_sstable_with_variant_data_types (which only calls it).
# Three rows of a table with columns of most CQL types.
@migration_gate
async def test_migrate_sstable_with_variant_data_types(manager: ScyllaClusterManager, version):
    node1 = await start_cluster_and_get_node1(manager)
    await create_ks_and_cf(manager, node1, query=VARIANT_TYPES_TABLE)
    await nodetool(manager, node1, "flush")
    await load_migrated_tables(manager, node1, version, "with_variant_data_types")
    check_number_of_rows(manager, node1, 3)
    result = get_all_rows_for_check(manager, node1)
    for i in range(3):
        r = result[i]
        assert r.aascii == ("tzach" if i in (0, 1) else "livyatan"), "check ascii column"
        assert r.abigint == 1999 + i, "check bigint column"
        assert r.ablob.hex() == "0000000000000003", "check blob column"
        assert r.aboolean is True, "check boolean column"
        assert r.adecimal == 10, "check decimal column"
        assert r.adouble == 10.10, "check double column"
        assert round(r.afloat, 2) == 11.11, "check afloat column"
        assert r.ainet == "204.202.130.223", "check ainet column"
        assert r.aint == 17, "check inet column"
        assert r.atext == "text", "check text column"
        assert r.atimestamp == datetime.datetime(2016, 8, 30, 7, 1), "check timestamp column"
        assert r.atimeuuid == uuid.UUID("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f"), "check timeuuid column"
        assert r.auuid == uuid.UUID("123e4567-e89b-12d3-a456-426655440000"), "check uuid column"
        assert r.avarchar == "tzachvarchar", "check varchar column"
        assert r.avarint == 17, "check varint column"
        assert r.alist == [1, 2, 3], "check list column"
        assert r.amap == {1: 2}, "check map column"
        assert r.aset == {1, 2, 3, 4}, "check set column"


# Ported from scylla-dtest migration_test.py::MigrationTestBase::test_migrate_sstable_with_counter,
# run by TestMigration::test_migrate_sstable_with_counter (which only calls it).
# Counter sstables load with enable_dangerous_direct_import_of_cassandra_counters (scylladb#2119).
@migration_gate
async def test_migrate_sstable_with_counter(manager: ScyllaClusterManager, version):
    node1 = await start_cluster_and_get_node1(manager, {"enable_dangerous_direct_import_of_cassandra_counters": True})
    await create_ks_and_cf(manager, node1, query="CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, "
                                                 "PRIMARY KEY(first_name, last_name));")
    await load_migrated_tables(manager, node1, version, "with_counter")
