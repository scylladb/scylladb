#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Restoring a table from a snapshot ("nodetool snapshot", then the snapshot's
sstables copied into the recreated table's upload directory and loaded by
"nodetool refresh"): refreshing twice, after a change of the shard count,
after another node took over token ranges, with the schema restored from
DESC ... WITH INTERNALS after a column was dropped and re-added, with the
node killed during the refresh, and from a snapshot taken by Cassandra; and
incremental backups and "nodetool clearsnapshot".

Ported from scylla-dtest backup_restore_tests.py. Like the dtests' nodes,
the nodes use vnodes (256 per node), batch commitlog sync, no hinted
handoff, and auto_snapshot (test.py turns it off), and are stopped
gracefully at the end of each test, as the dtests' teardown did.
"""

import asyncio
import glob
import json
import logging
import os
import random
import shutil

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import get_table_dir, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'hinted_handoff_enabled': False,
          'commitlog_sync': 'batch', 'commitlog_sync_batch_window_in_ms': 5, 'auto_snapshot': True}
SNAPSHOT_NAME = "test-snapshot"
# A Cassandra 2.1 snapshot (ka sstables) of ks.cf holding create_data()'s
# 1000 rows: the dtest's cassandra-sstables/restore-snapshot-from-cassandra.
CASSANDRA_SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), '..', 'resource', 'sstables', 'cassandra_migration', 'ka',
                                      'snapshot_of_1000_rows')


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def prepare(manager: ScyllaClusterManager, cmdline=None):
    """The dtests' prepare(): one node of ccm's populate(1) (SimpleSnitch,
    datacenter1/rack1) with hinted handoff off and batch commitlog."""
    return await manager.server_add(config={**CONFIG, 'endpoint_snitch': 'SimpleSnitch'}, cmdline=cmdline,
                                    property_file={"dc": "datacenter1", "rack": "rack1"})


async def create_ks_and_cf(cql, ks: str = "ks") -> None:
    """The dtests' create_ks() and create_cf("cf", read_repair=0.0, c1, c2)."""
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':1}}")
    await cql.run_async(f"CREATE COLUMNFAMILY {ks}.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                        "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                        "AND read_repair_chance=0.000000")
    await cql.run_async(f"SELECT * FROM {ks}.cf LIMIT 1")


async def insert_c1c2(cql, keys, c1_values, c2_values, ks: str = "ks") -> None:
    insert = cql.prepare(f"INSERT INTO {ks}.cf (key, c1, c2) VALUES (?, ?, ?)")
    insert.consistency_level = ConsistencyLevel.ONE
    await asyncio.gather(*[cql.run_async(insert, [f"k{k}", c1, c2]) for k, c1, c2 in zip(keys, c1_values, c2_values)])


async def create_data(cql, num_keys: int = 1000):
    await create_ks_and_cf(cql)
    keys = list(range(num_keys))
    c1_values = [str(n) for n in range(num_keys)]
    c2_values = [str(n) for n in range(num_keys, 2 * num_keys)]
    await insert_c1c2(cql, keys, c1_values, c2_values)
    return keys, c1_values, c2_values


async def check_rows(cql, keys, c1_values, c2_values) -> None:
    """The dtests' check_rows_on_node(): the row count, and each key's
    values read at ONE."""
    assert list(await cql.run_async("SELECT COUNT(*) FROM ks.cf"))[0].count == len(keys)
    select = cql.prepare("SELECT c1, c2 FROM ks.cf WHERE key=?")
    select.consistency_level = ConsistencyLevel.ONE
    results = await asyncio.gather(*[cql.run_async(select, [f"k{k}"]) for k in keys])
    for result, c1, c2 in zip(results, c1_values, c2_values):
        assert [(r.c1, r.c2) for r in result] == [(c1, c2)]


def delete_cf_sstables(cf_dir: str) -> None:
    """The dtests' delete_cf_sstables(): remove the files (not the
    subdirectories) of a table directory."""
    for f in os.listdir(cf_dir):
        if os.path.isfile(os.path.join(cf_dir, f)):
            os.remove(os.path.join(cf_dir, f))


def get_snapshot_dir(snapshot_name: str, base_dir: str) -> str | None:
    """The dtests' get_snapshot_dir(): the first directory named
    snapshot_name under base_dir."""
    for root, dirs, _ in os.walk(base_dir):
        for name in dirs:
            if name == snapshot_name:
                return os.path.join(root, name)
    return None


def get_non_snapshot_cf_dir(ks_dir: str, snapshot_name: str) -> str | None:
    """The dtests' get_non_snapshot_cf_dir(): the first table directory of
    the keyspace without a snapshot named snapshot_name."""
    for root, dirs, _ in os.walk(ks_dir):
        for d in dirs:
            if not os.path.isdir(os.path.join(root, d, "snapshots", snapshot_name)):
                return os.path.join(root, d)
        break
    return None


def get_all_files_in_dir(dir_path: str) -> set[str]:
    return {f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))}


def get_sstables_files(cf_dir: str) -> set[str]:
    """The dtests' get_sstables_files(): all components of the sstables
    (those with a TOC) in the directory."""
    files = []
    for toc in glob.glob(os.path.join(cf_dir, "*-TOC.txt")):
        files += glob.glob(toc[:-7] + "*")
    return {os.path.basename(f) for f in files}


async def start_nodetool_and_kill_node(manager: ScyllaClusterManager, server, args, message: str | None = None) -> None:
    """The dtests' start_nodetool_and_kill_node(): start the nodetool
    command, wait for the message in the node's log, wait a random time
    below a second and kill the node; the command's failure is ignored."""
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    command = asyncio.create_task(run_nodetool(manager, server, *args, check=False))
    if message:
        await log.wait_for(message, from_mark=mark)
    await asyncio.sleep(random.random())
    await manager.server_stop(server.server_id, convict=False)
    await command


async def snapshot_dir(manager, server) -> str:
    [path] = glob.glob(f"{await manager.server_get_workdir(server.server_id)}/data/ks/cf-*/snapshots/{SNAPSHOT_NAME}")
    return path


async def recreate_and_load_snapshot(manager, server, cql, snapshot: str, refreshes: int = 1) -> None:
    """Recreate ks.cf, flush it, empty its directory, copy the snapshot into
    its upload directory and "nodetool refresh" it, as the dtests did."""
    await create_ks_and_cf(cql)
    await run_nodetool(manager, server, "flush", "--", "ks")
    workdir = await manager.server_get_workdir(server.server_id)
    [cf_dir] = [d for d in glob.glob(f"{workdir}/data/ks/cf-*") if not os.path.isdir(os.path.join(d, "snapshots", SNAPSHOT_NAME))]
    for f in os.listdir(cf_dir):
        if os.path.isfile(os.path.join(cf_dir, f)):
            os.remove(os.path.join(cf_dir, f))
    for f in os.listdir(snapshot):
        shutil.copy2(os.path.join(snapshot, f), os.path.join(cf_dir, "upload", f))


# A dropped table's snapshot loaded into the recreated table brings its rows
# back, and a second refresh changes nothing.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_replay_restore_no_additional_data(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    data = await create_data(cql)
    await run_nodetool(manager, server, "snapshot", "-t", SNAPSHOT_NAME, "-cf", "cf", "--", "ks")
    snapshot = await snapshot_dir(manager, server)
    await cql.run_async(SimpleStatement("DROP KEYSPACE ks"))
    await recreate_and_load_snapshot(manager, server, cql, snapshot)
    for _ in range(2):
        await run_nodetool(manager, server, "refresh", "--", "ks", "cf")
        await check_rows(cql, *data)


# A snapshot taken on one shard is loaded after the node restarted with two.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_restore_snapshot_using_different_smp_setting(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=["--smp", "1"])
    cql = manager.get_cql()
    data = await create_data(cql)
    await run_nodetool(manager, server, "snapshot", "-t", SNAPSHOT_NAME, "-cf", "cf", "--", "ks")
    snapshot = await snapshot_dir(manager, server)
    await cql.run_async(SimpleStatement("DROP KEYSPACE ks"))
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_cmdline(server.server_id, ["--smp", "2"])
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    cql = manager.get_cql()
    await recreate_and_load_snapshot(manager, server, cql, snapshot)
    await run_nodetool(manager, server, "refresh", "--", "ks", "cf")
    await check_rows(cql, *data)


# A snapshot taken on one node is loaded after a second node joined and
# took over part of its token ranges; the table can then be counted.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_restore_snapshot_using_old_token_ownership(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    await create_data(cql)
    await run_nodetool(manager, server, "snapshot", "-t", SNAPSHOT_NAME, "-cf", "cf", "--", "ks")
    snapshot = await snapshot_dir(manager, server)
    await manager.server_add(config=CONFIG)
    await cql.run_async(SimpleStatement("DROP KEYSPACE ks"))
    await recreate_and_load_snapshot(manager, server, cql, snapshot)
    await run_nodetool(manager, server, "refresh", "--", "ks", "cf")
    await cql.run_async(SimpleStatement("SELECT COUNT(*) FROM ks.cf"))


# With the schema saved by DESC KEYSPACE WITH INTERNALS (which records the
# drop of column c2 before its re-adding) and the sstables saved, after the
# keyspace is dropped the restored table has c2 null in the rows written
# before the drop and set in the rows written after the re-add.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_no_data_resurrection_or_loss_after_restore(manager: ScyllaClusterManager, tmp_path):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    num_keys = 10
    await create_data(cql, num_keys)
    await run_nodetool(manager, server, "flush")
    await cql.run_async("ALTER TABLE ks.cf DROP c2")
    await cql.run_async("ALTER TABLE ks.cf ADD c2 text")
    offset = 1000
    await insert_c1c2(cql, range(num_keys, 2 * num_keys), [str(x) for x in range(offset, offset + num_keys)],
                      [str(x) for x in range(offset + num_keys, offset + 2 * num_keys)])
    await run_nodetool(manager, server, "flush")

    await manager.server_stop_gracefully(server.server_id)
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    for f in os.listdir(table_dir):
        if os.path.isfile(os.path.join(table_dir, f)):
            shutil.copy2(os.path.join(table_dir, f), tmp_path)
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    cql = manager.get_cql()
    schema = list(await cql.run_async("DESC KEYSPACE ks WITH INTERNALS"))
    await cql.run_async("DROP KEYSPACE ks")
    await manager.server_stop_gracefully(server.server_id)
    shutil.rmtree(os.path.join(await manager.server_get_workdir(server.server_id), "data", "ks"))
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    cql = manager.get_cql()
    for element in schema:
        for statement in element.create_statement.split(";"):
            if statement.strip():
                await cql.run_async(statement)
    assert list(await cql.run_async("DESC KEYSPACE ks WITH INTERNALS")) == schema
    await manager.server_stop_gracefully(server.server_id)
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    for f in os.listdir(tmp_path):
        shutil.copy2(os.path.join(tmp_path, f), table_dir)
    cql = await restart_start(manager, server)
    result = list(await cql.run_async("SELECT key, c1, c2 FROM ks.cf"))
    old_keys = {f"k{i}" for i in range(num_keys)}
    assert len(result) == num_keys * 2
    for row in result:
        if row.key in old_keys:
            assert row.c2 is None
        else:
            assert row.c2 == f"{offset + int(row.key[1:])}"


async def restart_start(manager, server):
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    return manager.get_cql()


# Ported from scylla-dtest backup_restore_tests.py::TestBackupRestore::test_failure_durring_restore_no_corrupt_data.
# The node is killed while "nodetool refresh" loads a dropped table's
# snapshot into the recreated table (holding other values, flushed by
# "nodetool drain" and removed); with the commitlog removed too, after the
# restart a second refresh loads the snapshot's rows, unmixed.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_failure_durring_restore_no_corrupt_data(manager: ScyllaClusterManager):
    server = await prepare(manager)
    cql = manager.get_cql()
    keys, c1_values, c2_values = await create_data(cql)
    await run_nodetool(manager, server, "snapshot", "-t", SNAPSHOT_NAME, "-cf", "cf", "--", "ks")
    await cql.run_async(SimpleStatement("DROP KEYSPACE ks"))
    await create_ks_and_cf(cql)
    await insert_c1c2(cql, keys, c2_values, c1_values)
    await check_rows(cql, keys, c2_values, c1_values)
    commitlog_dir = json.loads((await cql.run_async(
        "SELECT value FROM system.config WHERE name = 'commitlog_directory'"))[0].value)
    await run_nodetool(manager, server, "drain")

    workdir = await manager.server_get_workdir(server.server_id)
    snapshot = get_snapshot_dir(SNAPSHOT_NAME, workdir)
    assert snapshot is not None, f"Can't find a snapshot directory for {SNAPSHOT_NAME}"
    cf_dir = get_non_snapshot_cf_dir(os.path.join(workdir, "data", "ks"), SNAPSHOT_NAME)
    delete_cf_sstables(cf_dir)
    for f in os.listdir(snapshot):
        shutil.copy2(os.path.join(snapshot, f), os.path.join(cf_dir, "upload", f))
    await start_nodetool_and_kill_node(manager, server, ["refresh", "--", "ks", "cf"],
                                       r"Loading new SSTables for (ks\.cf\.\.\.|keyspace=ks, table=cf,)")
    # tools/commitlog.cleanup(): remove everything in the commitlog directory.
    for f in os.listdir(commitlog_dir):
        path = os.path.join(commitlog_dir, f)
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    cql = manager.get_cql()
    await run_nodetool(manager, server, "refresh", "--", "ks", "cf")
    await check_rows(cql, keys, c1_values, c2_values)


# Ported from scylla-dtest backup_restore_tests.py::TestBackupRestore::test_incremental_backup.
# With "nodetool enablebackup", the flushed sstables are hard-linked into the
# table's backups directory, which "nodetool compact" leaves unchanged.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_incremental_backup(manager: ScyllaClusterManager):
    server = await prepare(manager, cmdline=["--smp", "1"])
    await run_nodetool(manager, server, "enablebackup")
    await create_data(manager.get_cql())
    await run_nodetool(manager, server, "flush", "--", "ks", "cf")
    cf_dir = await get_table_dir(manager, server, "ks", "cf")
    sstables_files1 = get_sstables_files(cf_dir)
    backups1_files = get_sstables_files(os.path.join(cf_dir, "backups"))
    assert sstables_files1 == backups1_files, "backup doesn't contain all sstable files"
    await run_nodetool(manager, server, "compact")
    backups2_files = get_sstables_files(os.path.join(cf_dir, "backups"))
    assert backups1_files == backups2_files, "backup contents changed after a compaction"


# Ported from scylla-dtest backup_restore_tests.py::TestBackupRestore::test_restore_snapshot_from_cassandra.
# A snapshot written by Cassandra 2.1 (ka sstables) is copied to the upload
# directory and loaded by "nodetool refresh": all 1000 rows are there.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_restore_snapshot_from_cassandra(manager: ScyllaClusterManager):
    num_keys = 1000
    c1_values = [f"{x}" for x in range(num_keys)]
    c2_values = [f"{x}" for x in range(num_keys, 2 * num_keys)]
    keys = list(range(num_keys))
    server = await prepare(manager, cmdline=["--smp", "1"])
    await run_nodetool(manager, server, "enablebackup")
    cql = manager.get_cql()
    await create_ks_and_cf(cql)
    await run_nodetool(manager, server, "flush", "--", "ks")
    cf_dir = await get_table_dir(manager, server, "ks", "cf")
    delete_cf_sstables(cf_dir)
    for f in os.listdir(CASSANDRA_SNAPSHOT_DIR):
        shutil.copy2(os.path.join(CASSANDRA_SNAPSHOT_DIR, f), os.path.join(cf_dir, "upload", f))
    await run_nodetool(manager, server, "refresh", "--", "ks", "cf")
    await check_rows(cql, keys, c1_values, c2_values)


# Ported from scylla-dtest backup_restore_tests.py::TestBackupRestore::test_clearsnapshot_options.
# Three snapshots of two keyspaces: "clearsnapshot -t snapshot0" removes
# snapshot0 of both, "clearsnapshot -t snapshot1 -- ks1" snapshot1 of ks1
# only, leaving the other snapshots' files untouched, and "clearsnapshot"
# removes them all.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_clearsnapshot_options(manager: ScyllaClusterManager):
    server = await prepare(manager)
    cql = manager.get_cql()
    for i in range(2):
        await create_ks_and_cf(cql, f"ks{i}")

    num_keys = 1000
    start_key = 0
    for i in range(3):
        c1_values = [f"{x}" for x in range(start_key, start_key + num_keys)]
        c2_values = [f"{x}" for x in range(start_key + num_keys, start_key + 2 * num_keys)]
        keys = range(start_key, start_key + num_keys)
        await insert_c1c2(cql, keys, c1_values, c2_values, ks="ks0")
        await insert_c1c2(cql, keys, c1_values, c2_values, ks="ks1")
        await run_nodetool(manager, server, "snapshot", "-t", f"snapshot{i}", "ks0", "ks1")
        start_key = start_key + num_keys

    workdir = await manager.server_get_workdir(server.server_id)
    ks_dir = [os.path.join(workdir, "data", f"ks{i}") for i in range(2)]
    ks_snapshot_dir = [[None, None, None], [None, None, None]]
    ks_snapshot_files = [[None, None, None], [None, None, None]]
    for i in range(2):
        for j in [1, 2]:
            ks_snapshot_dir[i][j] = get_snapshot_dir(f"snapshot{j}", ks_dir[i])
            assert ks_snapshot_dir[i][j] is not None, f"Can't find a snapshot directory for 'ks{i}.snapshot{j}'"
            ks_snapshot_files[i][j] = get_all_files_in_dir(ks_snapshot_dir[i][j])

    await run_nodetool(manager, server, "clearsnapshot", "-t", "snapshot0")
    for i in range(2):
        assert get_snapshot_dir("snapshot0", ks_dir[i]) is None, f"'ks{i}' snapshot 'snapshot0' has not been deleted!"
    for i in range(2):
        for j in [1, 2]:
            assert get_snapshot_dir(f"snapshot{j}", ks_dir[i]) is not None, f"'ks{i}' snapshot 'snapshot{j}' has been deleted!"
            assert get_all_files_in_dir(ks_snapshot_dir[i][j]) == ks_snapshot_files[i][j], \
                f"'ks{i}' snapshot 'snapshot{j}' directory contents has changed!"

    await run_nodetool(manager, server, "clearsnapshot", "-t", "snapshot1", "--", "ks1")
    assert get_snapshot_dir("snapshot1", ks_dir[1]) is None, "'ks1' snapshot 'snapshot1' has not been deleted!"
    assert get_snapshot_dir("snapshot1", ks_dir[0]) is not None, "'ks0' snapshot 'snapshot1' has been deleted!"
    assert get_all_files_in_dir(ks_snapshot_dir[0][1]) == ks_snapshot_files[0][1], \
        "'ks0' snapshot 'snapshot1' directory contents has changed!"
    for i in range(2):
        assert get_snapshot_dir("snapshot2", ks_dir[i]) is not None, f"'ks{i}' snapshot 'snapshot2' has been deleted!"
        assert get_all_files_in_dir(ks_snapshot_dir[i][2]) == ks_snapshot_files[i][2], \
            f"'ks{i}' snapshot 'snapshot2' directory contents has changed!"

    await run_nodetool(manager, server, "clearsnapshot")
    for i in range(3):
        assert get_snapshot_dir(f"snapshot{i}", workdir) is None, f"'snapshot{i}' has not been deleted!"
