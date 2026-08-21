#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for restoring a table from the snapshot that 'auto_snapshot' takes when the table is
dropped. In every case the table is recreated from the snapshot's 'schema.cql' file; the
sstables are then reloaded either via 'nodetool refresh' (local 'upload' directory) or via the
object-store restore API (the sstables are uploaded to a bucket and downloaded back). The
'load_method' parameter exercises both paths.
"""

import logging
import os
import shutil

import pytest
from cassandra.protocol import InvalidRequest

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

# rf=1, tablets disabled - a single node owns all the data, which keeps the snapshot based
# restore deterministic.
KEYSPACE_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}"

# The sstables are reloaded either from the local 'upload' directory ('refresh') or from an
# object-store bucket ('restore'). Both start from the same table recreated from schema.cql.
LOAD_METHODS = ["refresh", "restore"]


async def table_data_dir(manager: ManagerClient, server, ks: str, cf: str, *, with_snapshots: bool) -> str | None:
    """
    Return the on-disk directory of table `ks.cf`. A dropped table whose snapshot was kept and a
    freshly (re)created table can coexist as two 'cf-<uuid>' directories, so `with_snapshots`
    selects the one that does (the dropped table's snapshot) or does not (the live table) have a
    'snapshots' subdirectory. Returns the FIRST directory that matches (there is only one per
    `with_snapshots` value in these tests), or None if none matches.
    """
    workdir = await manager.server_get_workdir(server.server_id)
    ks_dir = os.path.join(workdir, "data", ks)
    for entry in os.listdir(ks_dir):
        if not entry.startswith(f"{cf}-"):
            continue
        cf_dir = os.path.join(ks_dir, entry)
        has_snapshots = os.path.isdir(os.path.join(cf_dir, "snapshots"))
        if has_snapshots == with_snapshots:
            return cf_dir
    return None


async def auto_snapshot_dir(manager: ManagerClient, server, ks: str, cf: str) -> str:
    """Locate the 'pre-drop-*' snapshot directory created when `ks.cf` was dropped."""
    dropped_cf_dir = await table_data_dir(manager, server, ks, cf, with_snapshots=True)
    assert dropped_cf_dir is not None, f"auto_snapshot directory was not created when dropping {ks}.{cf}"

    snapshots_dir = os.path.join(dropped_cf_dir, "snapshots")
    snapshot_names = [n for n in os.listdir(snapshots_dir) if n.startswith("pre-drop-")]
    assert snapshot_names, f"'pre-drop-*' snapshot was not created when dropping {ks}.{cf}"
    return os.path.join(snapshots_dir, snapshot_names[0])


async def recreate_table_from_schema_cql(cql, ks: str, cf: str, snapshot_dir: str) -> None:
    """Recreate `ks.cf` from the snapshot's schema.cql (CREATE TABLE plus any ALTER statements)."""
    schema_cql_path = os.path.join(snapshot_dir, "schema.cql")
    assert os.path.isfile(schema_cql_path), f"schema.cql not found in the snapshot directory {snapshot_dir}"
    with open(schema_cql_path) as f:
        schema_cql = f.read()
    logger.info("Recreating %s.%s from the snapshot's schema.cql:\n%s", ks, cf, schema_cql)
    for statement in schema_cql.split(";"):
        if statement.strip():
            await cql.run_async(statement)


async def reload_sstables_via_refresh(manager: ManagerClient, server, ks: str, cf: str, snapshot_dir: str) -> None:
    """Copy the snapshot's sstables into the table's 'upload' directory and run refresh."""
    live_cf_dir = await table_data_dir(manager, server, ks, cf, with_snapshots=False)
    assert live_cf_dir is not None, f"restored table {ks}.{cf} directory was not found"
    upload_dir = os.path.join(live_cf_dir, "upload")
    os.makedirs(upload_dir, exist_ok=True)
    for item in os.listdir(snapshot_dir):
        # 'manifest.json' and 'schema.cql' are not sstable components and are ignored by refresh.
        if item in ("manifest.json", "schema.cql"):
            continue
        src = os.path.join(snapshot_dir, item)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(upload_dir, item))

    logger.info("Refreshing %s.%s from the restored sstables", ks, cf)
    await manager.api.load_new_sstables(server.ip_addr, ks, cf)


async def reload_sstables_via_restore(manager: ManagerClient, server, ks: str, cf: str, snapshot_dir: str, s3_storage) -> None:
    """Upload the snapshot's sstables to the object store and load them back via the restore API."""
    prefix = f"{ks}/{cf}/{os.path.basename(snapshot_dir)}"
    bucket = s3_storage.get_resource().Bucket(s3_storage.bucket_name)
    toc_names = []
    for item in os.listdir(snapshot_dir):
        # 'manifest.json' and 'schema.cql' are not sstable components.
        if item in ("manifest.json", "schema.cql"):
            continue
        src = os.path.join(snapshot_dir, item)
        if os.path.isfile(src):
            bucket.upload_file(src, f"{prefix}/{item}")
            if item.endswith("TOC.txt"):
                toc_names.append(item)
    assert toc_names, f"no sstables found in the snapshot directory {snapshot_dir}"

    logger.info("Restoring %s.%s from the object store (prefix=%s, sstables=%s)", ks, cf, prefix, toc_names)
    tid = await manager.api.restore(server.ip_addr, ks, cf, s3_storage.address, s3_storage.bucket_name, prefix, toc_names)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None and status["state"] == "done", f"restore task did not finish successfully: {status}"


async def restore_table_from_auto_snapshot(manager: ManagerClient, server, cql, ks: str, cf: str, *, load_method: str, s3_storage) -> None:
    """
    Restore `ks.cf` from its auto-snapshot: recreate the table from the snapshot's schema.cql and
    reload its data either via 'nodetool refresh' or via the object-store restore API.
    """
    snapshot_dir = await auto_snapshot_dir(manager, server, ks, cf)
    await recreate_table_from_schema_cql(cql, ks, cf, snapshot_dir)
    if load_method == "refresh":
        await reload_sstables_via_refresh(manager, server, ks, cf, snapshot_dir)
    else:
        await reload_sstables_via_restore(manager, server, ks, cf, snapshot_dir, s3_storage)


async def add_server(manager: ManagerClient, s3_storage):
    """Start a single node with auto_snapshot on and the object store configured (used by restore)."""
    config = {"auto_snapshot": True, "object_storage_endpoints": s3_storage.create_endpoint_conf()}
    return await manager.server_add(config=config)


@pytest.mark.parametrize("load_method", LOAD_METHODS)
async def test_auto_snapshot_restore_table(manager: ManagerClient, s3_storage, load_method: str):
    """
    Baseline: a table dropped while 'auto_snapshot' is enabled can be fully restored - schema
    included - directly from its snapshot, and the restored data includes a row that was never
    flushed before the drop.
    """
    server = await add_server(manager, s3_storage)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        cf = "cf"
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (key int PRIMARY KEY, c1 text)")

        insert = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1) VALUES (?, ?)")
        # One flushed row and one row left in the memtable (never flushed) before the drop.
        await cql.run_async(insert, (0, "c1-0"))
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)
        await cql.run_async(insert, (1, "c1-1"))

        await cql.run_async(f"DROP TABLE {ks}.{cf}")

        await restore_table_from_auto_snapshot(manager, server, cql, ks, cf, load_method=load_method, s3_storage=s3_storage)

        rows = {row.key: row for row in await cql.run_async(f"SELECT * FROM {ks}.{cf}")}
        assert rows.keys() == {0, 1}, f"expected keys {{0, 1}} after restore, got {sorted(rows)}"
        assert rows[0].c1 == "c1-0"
        assert rows[1].c1 == "c1-1"


@pytest.mark.parametrize("load_method", LOAD_METHODS)
async def test_auto_snapshot_restore_table_with_dropped_column(manager: ManagerClient, s3_storage, load_method: str):
    """
    A column is dropped after some data was flushed. Unless the snapshot's schema.cql records the
    drop (as 'ALTER TABLE ... DROP ... USING TIMESTAMP'), the old sstable becomes unreadable after
    restore. Verify both rows (flushed and unflushed) are restored and the dropped column is gone.
    """
    server = await add_server(manager, s3_storage)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        cf = "cf"
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (key int PRIMARY KEY, c1 text, c2 text)")

        # Flush a row that carries data for the soon-to-be-dropped column 'c2'.
        await cql.run_async(cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)"), (0, "c1-0", "c2-0"))
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)

        # Dropping the column is a metadata-only change; the already-flushed sstable still holds
        # 'c2' data.
        await cql.run_async(f"ALTER TABLE {ks}.{cf} DROP c2")

        # One more row (without 'c2') left in the memtable (never flushed) before the drop.
        await cql.run_async(cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1) VALUES (?, ?)"), (1, "c1-1"))

        await cql.run_async(f"DROP TABLE {ks}.{cf}")

        await restore_table_from_auto_snapshot(manager, server, cql, ks, cf, load_method=load_method, s3_storage=s3_storage)

        rows_list = list(await cql.run_async(f"SELECT * FROM {ks}.{cf}"))
        assert rows_list, "no rows returned after restore"
        assert "c2" not in rows_list[0]._fields, f"dropped column 'c2' was resurrected: {rows_list[0]._fields}"

        rows = {row.key: row for row in rows_list}
        assert rows.keys() == {0, 1}, f"expected keys {{0, 1}} after restore, got {sorted(rows)}"
        assert rows[0].c1 == "c1-0"
        assert rows[1].c1 == "c1-1"

        # The dropped column must not be queryable.
        with pytest.raises(InvalidRequest):
            await cql.run_async(f"SELECT c2 FROM {ks}.{cf}")


@pytest.mark.parametrize("load_method", LOAD_METHODS)
async def test_auto_snapshot_restore_table_with_dropped_and_readded_column(manager: ManagerClient, s3_storage, load_method: str):
    """
    A column is dropped and then re-added. Unless the snapshot's schema.cql records the drop
    timestamp, the value written before the drop is silently resurrected instead of being masked.
    Verify the pre-drop value reads back as null while the post-re-add value survives.
    """
    server = await add_server(manager, s3_storage)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        cf = "cf"
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (key int PRIMARY KEY, c1 text, c2 text)")

        insert = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
        # Flush a row carrying the pre-drop 'c2' value.
        await cql.run_async(insert, (0, "c1-0", "old-c2-0"))
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)

        await cql.run_async(f"ALTER TABLE {ks}.{cf} DROP c2")
        await cql.run_async(f"ALTER TABLE {ks}.{cf} ADD c2 text")

        # One more row with a post-re-add 'c2' value, left in the memtable (never flushed).
        await cql.run_async(insert, (1, "c1-1", "new-c2-1"))

        await cql.run_async(f"DROP TABLE {ks}.{cf}")

        await restore_table_from_auto_snapshot(manager, server, cql, ks, cf, load_method=load_method, s3_storage=s3_storage)

        rows = {row.key: row for row in await cql.run_async(f"SELECT * FROM {ks}.{cf}")}
        assert rows.keys() == {0, 1}, f"expected keys {{0, 1}} after restore, got {sorted(rows)}"
        # The pre-drop value must be masked (null), the post-re-add value must survive.
        assert rows[0].c1 == "c1-0"
        assert rows[0].c2 is None, f"pre-drop c2 value for key=0 was resurrected: {rows[0].c2}"
        assert rows[1].c1 == "c1-1"
        assert rows[1].c2 == "new-c2-1"
