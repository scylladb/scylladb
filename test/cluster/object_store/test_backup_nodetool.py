#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Backup to and restore from object storage (S3 and GCS) with "scylla
nodetool backup" and "nodetool restore": a table restored after a truncate,
restored into another table, and a restore failing once the bucket was
emptied.

Ported from scylla-dtest backup_restore_object_storage_tests.py. As in the
dtests: two nodes in two racks with vnodes (256 per node), keyspace ks with
RF=2 and table cf1 (the dtests' c1c2 table) holding keys 1..20 written at
ALL, auto_snapshot on (test.py turns it off), failed tasks kept for 60 s, and a snapshot of the table taken before
each test.
"""

import asyncio
import glob
import logging
import os
import re
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import get_table_dir, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

KS, CF, SNAPSHOT = "ks", "cf1", "snapshot"
KEY_RANGE = (1, 21)


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def insert_data(cql, cf: str, key_range: tuple) -> None:
    """The dtests' insert_data_from_ranges(): the c1c2 table (created as by
    create_cf) with keys k<i>, c1 'value<i>' and c2 'other_value<i>' at ALL."""
    await cql.run_async(f"CREATE KEYSPACE IF NOT EXISTS {KS} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':2}}")
    await cql.run_async(f"CREATE COLUMNFAMILY IF NOT EXISTS {KS}.{cf} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                        "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                        "AND read_repair_chance=0.000000 AND dclocal_read_repair_chance='0.0' AND speculative_retry='NONE'")
    await cql.run_async(f"SELECT * FROM {KS}.{cf} LIMIT 1")
    insert = cql.prepare(f"INSERT INTO {KS}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
    insert.consistency_level = ConsistencyLevel.ALL
    await asyncio.gather(*[cql.run_async(insert, [f"k{i}", f"value{i}", f"other_value{i}"]) for i in range(*key_range)])


async def verify_c1c2(cql, cf: str, key_range: tuple) -> None:
    rows = list(await cql.run_async(SimpleStatement(f"select * from {KS}.{cf}", consistency_level=ConsistencyLevel.QUORUM)))
    assert sorted(r.key for r in rows) == sorted(f"k{i}" for i in range(*key_range))
    assert sorted(r.c1 for r in rows) == sorted(f"value{i}" for i in range(*key_range))
    assert sorted(r.c2 for r in rows) == sorted(f"other_value{i}" for i in range(*key_range))


async def verify_lack_of_keys(cql, cf: str, key_range: tuple) -> None:
    existing = {r.key.replace("k", "") for r in await cql.run_async(f"select key from {KS}.{cf}")}
    wrong = existing & {str(n) for n in range(*key_range)}
    assert not wrong, f"{KS}.{cf} contains the keys {wrong}"


def endpoint_config(object_storage) -> list[dict]:
    """The dtests' setup_object_storage(): an S3 endpoint given by host and
    port (credentials from the environment), a GCS one by its URL."""
    if object_storage.type == "S3":
        return [{"name": object_storage.ip, "port": int(object_storage.port), "aws_region": "local"}]
    return object_storage.create_endpoint_conf()


def endpoint_name(object_storage) -> str:
    return object_storage.ip if object_storage.type == "S3" else object_storage.address


def bucket_objects(object_storage, prefix: str) -> list:
    return list(object_storage.get_resource().Bucket(object_storage.bucket_name).objects.filter(Prefix=prefix))


def delete_objects(object_storage, prefix: str) -> None:
    """The dtests' clean_bucket(): delete every object under the prefix."""
    from botocore.exceptions import ClientError
    for obj in bucket_objects(object_storage, prefix):
        try:
            obj.delete()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
                raise


@pytest.fixture
def cluster_options():
    """Extra node options, the dtests' @pytest.mark.cluster_options; a test
    overrides them by parametrizing this name."""
    return {}


@pytest.fixture
async def backup_setup(manager: ScyllaClusterManager, object_storage, cluster_options):
    """The dtests' setup_and_teardown(): the cluster, data and snapshot; the
    bucket prefix is emptied at the end."""
    config = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'task_ttl_in_seconds': 60, 'auto_snapshot': True,
              'object_storage_endpoints': endpoint_config(object_storage), **cluster_options}
    servers = await manager.servers_add(2, config=config, property_file=[{"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}])
    cql = manager.get_cql()
    await insert_data(cql, CF, KEY_RANGE)
    node1 = servers[0]
    await run_nodetool(manager, node1, "snapshot", "-t", SNAPSHOT, "-cf", CF, "--", KS)
    prefix = f"backup/{uuid.uuid1()}"
    yield node1, cql, prefix
    delete_objects(object_storage, prefix)


async def backup_with_nodetool(manager, node1, object_storage, prefix: str) -> list[str]:
    """Back the snapshot up with "nodetool backup"; return the table's TOCs."""
    await run_nodetool(manager, node1, "backup", "--endpoint", endpoint_name(object_storage), "--bucket", object_storage.bucket_name,
                       "--prefix", prefix, "--keyspace", KS, "--table", CF, "--snapshot", SNAPSHOT)
    return [os.path.basename(f) for f in glob.glob(f"{await get_table_dir(manager, node1, KS, CF)}/*-TOC.txt")]


async def restore_with_nodetool(manager, node1, object_storage, prefix: str, tocs: list[str], cf: str = CF, check: bool = True):
    return await run_nodetool(manager, node1, "restore", "--endpoint", endpoint_name(object_storage), "--bucket", object_storage.bucket_name,
                              "--keyspace", KS, "--table", cf, "--prefix", prefix, *tocs, check=check)


async def backup_and_truncate(manager, node1, cql, object_storage, prefix: str) -> list[str]:
    tocs = await backup_with_nodetool(manager, node1, object_storage, prefix)
    assert bucket_objects(object_storage, prefix), "nothing was uploaded"
    await cql.run_async(f"TRUNCATE {KS}.{CF}")
    await verify_lack_of_keys(cql, CF, KEY_RANGE)
    return tocs


# A backup restored after the table was truncated brings its rows back.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_backup_restore_with_nodetool(manager: ScyllaClusterManager, object_storage, backup_setup):
    node1, cql, prefix = backup_setup
    tocs = await backup_and_truncate(manager, node1, cql, object_storage, prefix)
    await restore_with_nodetool(manager, node1, object_storage, prefix, tocs)
    await verify_c1c2(cql, CF, KEY_RANGE)


# A backup of cf1 restored into cf2, which holds a subset of its keys,
# leaves cf2 with all of cf1's rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restore_into_different_table(manager: ScyllaClusterManager, object_storage, backup_setup):
    node1, cql, prefix = backup_setup
    tocs = await backup_and_truncate(manager, node1, cql, object_storage, prefix)
    await insert_data(cql, "cf2", (1, 10))
    await restore_with_nodetool(manager, node1, object_storage, prefix, tocs, cf="cf2")
    await verify_c1c2(cql, "cf2", KEY_RANGE)


# Restoring TOCs whose objects were deleted from the bucket fails with a
# 404 storage_io_error and loads nothing.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restore_after_emptying_bucket(manager: ScyllaClusterManager, object_storage, backup_setup):
    node1, cql, prefix = backup_setup
    tocs = await backup_and_truncate(manager, node1, cql, object_storage, prefix)
    delete_objects(object_storage, prefix)
    out, err, rc = await restore_with_nodetool(manager, node1, object_storage, prefix, tocs, check=False)
    assert rc != 0
    error = {"S3": r"failed: storage_io_error[ :]+\(?S3 request failed. Code: 117. Reason:\s+HTTP code: 404 Not Found\)?.*",
             "GS": r"failed: storage_io_error[ :]+\(?GCP object doesn't exist \(404 Not Found\)\)?.*"}[object_storage.type]
    assert re.search(error, out + err), out + err
    await verify_lack_of_keys(cql, CF, KEY_RANGE)


# The dtest's (argument, error, ignored log lines) of a backup naming
# something that does not exist.
BACKUP_NONEXISTENT = {
    "endpoint": ({"endpoint": "does_not_exist"}, r"(?s).*std::invalid_argument[ :]+\(?endpoint does_not_exist not found\)?.*",
                 [r"sstables_manager - unable to find does_not_exist in configured object-storage endpoints"]),
    "bucket": ({"bucket": "does_not_exist"}, r"(?s).*failed: storage_io_error[ :]+.*(S3|GCP) (object doesn't exist|request failed).*",
               [r"snapshots - Error uploading .* storage_io_error[ :]+.*S3 (object doesn't exist|request failed).*",
                r"snapshots - Error uploading .* storage_io_error[ :]+\(?GCP object doesn't exist \(404 Not Found\)\)?.*",
                r"default_retry_strategy - S3 client encountered non-retryable error\. Reason: The specified bucket is not valid\.\. Code: 100\."]),
    "keyspace": ({"ks": "does_not_exist"},
                 r"data_dictionary::no_such_column_family[ :]+\(?Can't find a column family cf1 in keyspace does_not_exist\)?|"
                 r"std::invalid_argument[ :]+\(?snapshot snapshot not found for table does_not_exist\.cf1\)?", []),
    "table": ({"cf": "does_not_exist"},
              r"data_dictionary::no_such_column_family[ :]+\(?Can't find a column family does_not_exist in keyspace ks\)?|"
              r"std::invalid_argument[ :]+\(?snapshot snapshot not found for table ks\.does_not_exist\)?", []),
    "snapshot": ({"snapshot": "does_not_exist"},
                 r"(?s)failed: std::invalid_argument[ :]+\(?snapshot does not exist at .*/does_not_exist\)?|"
                 r"std::invalid_argument[ :]+\(?snapshot does_not_exist not found for table ks\.cf1\)?", []),
}


# Ported from scylla-dtest backup_restore_object_storage_tests.py::TestBackupRestoreObjectStorage::test_backup_nonexistent_argument.
# "nodetool backup" naming an endpoint, bucket, keyspace, table or snapshot
# that does not exist fails with the dtest's error, and leaves the bucket
# prefix empty.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("argument", list(BACKUP_NONEXISTENT))
async def test_backup_nonexistent_argument(manager: ScyllaClusterManager, object_storage, backup_setup, argument):
    node1, cql, prefix = backup_setup
    arg, error, ignore = BACKUP_NONEXISTENT[argument]
    manager.ignore_log_patterns.extend(ignore)
    args = {"endpoint": endpoint_name(object_storage), "bucket": object_storage.bucket_name, "ks": KS, "cf": CF,
            "prefix": prefix, "snapshot": SNAPSHOT, **arg}
    out, err, rc = await run_nodetool(manager, node1, "backup", "--endpoint", args["endpoint"], "--bucket", args["bucket"],
                                      "--prefix", args["prefix"], "--keyspace", args["ks"], "--table", args["cf"],
                                      "--snapshot", args["snapshot"], check=False)
    assert rc != 0, out
    assert re.search(error, out + err), out + err
    assert not bucket_objects(object_storage, prefix), "the bucket is not empty"


# The dtest's (argument, error, ignored log lines) of a restore naming
# something that does not exist.
RESTORE_NONEXISTENT = {
    "endpoint": ({"endpoint": "does_not_exist"}, r"(?s).*std::invalid_argument[ :]+\(?endpoint does_not_exist not found\)?.*", []),
    "bucket": ({"bucket": "does_not_exist"}, r"(?s).*failed: storage_io_error[ :]+.*(S3|GCP) (object doesn't exist|request failed).*",
               [r"default_retry_strategy - S3 client encountered non-retryable error. Reason: Unknown server error has been encountered. "
                r"HTTP code: 400 Bad Request\. Code: 100\."]),
    "keyspace": ({"ks": "does_not_exist"}, r"(?s).*data_dictionary::no_such_column_family[ :]+\(?Can't find a column family cf1 in keyspace does_not_exist\)?.*", []),
    "table": ({"cf": "does_not_exist"}, r"(?s).*data_dictionary::no_such_column_family[ :]+\(?Can't find a column family does_not_exist in keyspace ks\)?.*", []),
    "prefix": ({"prefix": "does_not_exist"}, r"(?s).*failed: storage_io_error[ :]+.*(S3|GCP) (object doesn't exist|request failed).*",
               [r"default_retry_strategy - S3 client encountered non-retryable error. Reason:\s+HTTP code: 404 Not Found\. Code: 117\."]),
    "tocs": ({"tocs": ["me-does_not_exist1-TOC.txt", "me-does_not_exist2-TOC.txt"]},
             r"(?s).*failed: sstables::malformed_sstable_exception[ :]+\(?invalid version for file .*. Name doesn't match any known version.\)?.*", []),
}


# Ported from scylla-dtest backup_restore_object_storage_tests.py::TestBackupRestoreObjectStorage::test_restore_nonexistent_argument.
# After a backup and a truncate, "nodetool restore" naming an endpoint,
# bucket, keyspace, table or prefix that does not exist, or malformed TOC
# names, fails with the dtest's error and loads nothing.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("argument", list(RESTORE_NONEXISTENT))
@pytest.mark.parametrize("cluster_options", [{"abort_on_malformed_sstable_error": False}], ids=["abort_on_malformed_sstable_error_off"])
async def test_restore_nonexistent_argument(manager: ScyllaClusterManager, object_storage, backup_setup, argument):
    node1, cql, prefix = backup_setup
    tocs = await backup_and_truncate(manager, node1, cql, object_storage, prefix)
    arg, error, ignore = RESTORE_NONEXISTENT[argument]
    manager.ignore_log_patterns.extend(ignore)
    args = {"endpoint": endpoint_name(object_storage), "bucket": object_storage.bucket_name, "ks": KS, "cf": CF,
            "prefix": prefix, "tocs": tocs, **arg}
    out, err, rc = await run_nodetool(manager, node1, "restore", "--endpoint", args["endpoint"], "--bucket", args["bucket"],
                                      "--keyspace", args["ks"], "--table", args["cf"], "--prefix", args["prefix"], *args["tocs"],
                                      check=False)
    assert rc != 0, out
    assert re.search(error, out + err), out + err
    await verify_lack_of_keys(cql, CF, KEY_RANGE)
