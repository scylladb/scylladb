#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Writes and reads through a three-node cluster, ported from scylla-dtest
# putget_test.py. Tables are created like the dtests' create_cf() did, and
# the keyspaces use the cluster's default (tablets), like the dtests.

import asyncio

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest


def create_cf(cql, table, columns=None, compression=None):
    """The dtests' create_cf(): a table with a comment, explicitly enabled
    size-tiered compaction and no compression (or the given compressor),
    checked with a read."""
    if columns:
        schema = "key varchar PRIMARY KEY" + "".join(f", {k} {v}" for k, v in columns.items())
    else:
        schema = "key varchar, c varchar, v varchar, PRIMARY KEY(key, c)"
    cql.execute(f"CREATE COLUMNFAMILY {table} ({schema}) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND " +
                (f"compression = {{ 'sstable_compression': '{compression}Compressor' }}" if compression else "compression = {}"))
    cql.execute(f"SELECT * FROM {table} LIMIT 1")


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_non_local_read(manager: ScyllaClusterManager):
    """Writes and reads at QUORUM through a coordinator which, with RF=3 on
    three nodes in their own racks, holds a copy but cannot answer alone:
    1000 rows written with a prepared statement, each read back."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config={"tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.cf"
        create_cf(cql, table, columns={"c1": "text", "c2": "text"})
        insert = cql.prepare(f"INSERT INTO {table} (key, c1, c2) VALUES (?, 'value1', 'value2')")
        insert.consistency_level = ConsistencyLevel.QUORUM
        execute_concurrent_with_args(cql, insert, [[f"k{k}"] for k in range(1000)], concurrency=20)
        for k in range(1000):
            rows = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM {table} WHERE key='k{k}'", consistency_level=ConsistencyLevel.QUORUM)))
            assert [tuple(r) for r in rows] == [("value1", "value2")]


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_wide_row(manager: ScyllaClusterManager):
    """Slices of a wide row: columns written in batches of 100 updates, then
    read back in slices of 10, 100 and 1000 columns at QUORUM, RF=1 on
    three nodes. 5000 columns instead of the dtest's 500000, so the
    threshold of rows for recording a large partition is lowered to keep it
    a large partition. In the end the nodes flush, as stopping dtest nodes
    do, and the row is read again."""
    servers = await manager.servers_add(3, config={"compaction_rows_count_warning_threshold": 1000, "tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.cf"
        create_cf(cql, table)
        key = "wide"
        batches, per_batch = 50, 100
        for b in range(batches):
            upds = [f"UPDATE {table} SET v='value{i}' WHERE key='k{key}' AND c='c{i:06d}'" for i in range(b * per_batch, (b + 1) * per_batch)]
            cql.execute(SimpleStatement("BEGIN BATCH %s; APPLY BATCH" % "; ".join(upds), consistency_level=ConsistencyLevel.QUORUM))
        total = batches * per_batch
        for size in (10, 100, 1000):
            for x in range(1, (total + 1 - size) // size):
                offset = x * size - 1
                res = list(cql.execute(SimpleStatement(f"SELECT c, v FROM {table} WHERE key='k{key}' AND c >= 'c{offset:06d}' AND c <= 'c{size + offset - 1:06d}'",
                                                       consistency_level=ConsistencyLevel.QUORUM)))
                assert [r.v for r in res] == [f"value{i + offset}" for i in range(size)]
        for s in servers:
            await manager.api.flush_all_keyspaces(s.ip_addr)
        res = list(cql.execute(SimpleStatement(f"SELECT c, v FROM {table} WHERE key='k{key}'", consistency_level=ConsistencyLevel.QUORUM)))
        assert [r.v for r in res] == [f"value{i}" for i in range(total)]


async def put_with_overwrite(manager: ScyllaClusterManager, servers, cql, table: str, nb_keys: int, cl=ConsistencyLevel.QUORUM) -> None:
    """The dtests' _put_with_overwrite(): three rounds of batches of updates
    of every key, each overwriting a subset of the previous round's cells,
    each round flushed on every node."""
    for count, value_mul, ckey_mul in [(100, 1, 1), (50, 4, 2), (20, 20, 5)]:
        for k in range(nb_keys):
            kvs = [f"UPDATE {table} SET v='value{i * value_mul}' WHERE key='k{k}' AND c='c{i * ckey_mul:02d}'" for i in range(count)]
            cql.execute(SimpleStatement("BEGIN BATCH %s APPLY BATCH" % "; ".join(kvs), consistency_level=cl))
            await asyncio.sleep(0.01)
        for s in servers:
            await manager.api.flush_all_keyspaces(s.ip_addr)


def validate_row(res) -> None:
    """The dtests' _validate_row()."""
    assert len(res) == 100
    for i in range(100):
        if i % 5 == 0:
            assert res[i][2] == f"value{i * 4}", f"for {i}, expecting value{i * 4}, got {res[i][2]}"
        elif i % 2 == 0:
            assert res[i][2] == f"value{i * 2}", f"for {i}, expecting value{i * 2}, got {res[i][2]}"
        else:
            assert res[i][2] == f"value{i}", f"for {i}, expecting value{i}, got {res[i][2]}"


async def putget_cluster(manager: ScyllaClusterManager):
    """cluster_setup_3_nodes (datacenter1, rack1-rack3, vnodes) and keyspace ks
    with RF 3, through node1."""
    servers = await manager.servers_add(3, config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256},
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(3)])
    cql = manager.get_cql()
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    return servers, cql


async def putget(manager: ScyllaClusterManager, compression=None) -> None:
    """TestPutGet._putget(): a row hitting three sstables, read by slice."""
    servers, cql = await putget_cluster(manager)
    create_cf(cql, "ks.cf", compression=compression)
    await put_with_overwrite(manager, servers, cql, "ks.cf", 1)
    validate_row(list(cql.execute(SimpleStatement("SELECT * FROM ks.cf WHERE key='k0'", consistency_level=ConsistencyLevel.QUORUM))))


# Ported from scylla-dtest putget_test.py::TestPutGet::test_putget.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_putget(manager: ScyllaClusterManager):
    await putget(manager)


# Ported from scylla-dtest putget_test.py::TestPutGet::test_putget_snappy.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_putget_snappy(manager: ScyllaClusterManager):
    await putget(manager, compression="Snappy")


# Ported from scylla-dtest putget_test.py::TestPutGet::test_putget_deflate.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_putget_deflate(manager: ScyllaClusterManager):
    await putget(manager, compression="Deflate")


# Ported from scylla-dtest putget_test.py::TestPutGet::test_rangeputget.
# 100 keys hitting three sstables each, read back by a full scan (default
# CL) and checked key by key.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rangeputget(manager: ScyllaClusterManager):
    keys = 100
    servers, cql = await putget_cluster(manager)
    create_cf(cql, "ks.cf")
    await put_with_overwrite(manager, servers, cql, "ks.cf", keys)
    rows = list(cql.execute(SimpleStatement("SELECT * FROM ks.cf LIMIT 10000000", consistency_level=ConsistencyLevel.ONE)))
    assert len(rows) == keys * 100
    for _ in range(keys):
        validate_row(rows[:100])
        del rows[:100]

