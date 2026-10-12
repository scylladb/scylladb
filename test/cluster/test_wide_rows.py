#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Large partitions, rows and cells written by memtable flushes and
compactions on real nodes: their records in system.large_partitions,
large_rows and large_cells, the "Writing large ..." warnings, and how
TTL expiry, deletions, threshold changes, materialized views and a node
down change them.

Ported from scylla-dtest wide_rows_test.py. Like the dtests, every test
runs with each of LCS, STCS, TWCS and ICS, on nodes of its own with vnodes,
one rack per node, compaction_large_data_records_per_sstable 1000 and
large_data/mc_writer logging; the records are read from each node's own
system tables. Where a dtest waited 10 s for a warning it expects absent,
the port greps the log instead. Bulk writes of many small rows are sent
concurrently.
"""

import asyncio
import datetime
import glob
import itertools
import logging
import os
import random
import time
from collections import defaultdict
from dataclasses import dataclass

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.test_compaction import wait_for_compactions
from test.cluster.util import get_table_dir, reconnect_driver, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'compaction_large_data_records_per_sstable': 1000}
CMDLINE = ["--logger-log-level", "large_data=debug:mc_writer=trace"]
KS, TABLE = "wide_row", "user_events"
BLOB_SIZE_10K = 1024 * 10
BLOB_SIZE_1MB = 1024 * 1024
DATE = datetime.datetime.now()

# The dtest class is dtest_full, its TWCS and ICS variants also next_gating.
DTEST_FULL = [pytest.mark.tier3, pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")]
NEXT_GATING = [pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")]
pytestmark = pytest.mark.parametrize("strategy", [
    pytest.param("LeveledCompactionStrategy", marks=DTEST_FULL),
    pytest.param("SizeTieredCompactionStrategy", marks=DTEST_FULL),
    pytest.param("TimeWindowCompactionStrategy", marks=NEXT_GATING),
    pytest.param("IncrementalCompactionStrategy", marks=NEXT_GATING)])


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def day(i) -> str:
    return (DATE + datetime.timedelta(i)).strftime("%Y-%m-%d")


async def gather_in_batches(requests, batch=100) -> None:
    """Await the requests (started as they are drawn from the iterable),
    at most `batch` at a time."""
    requests = iter(requests)
    while chunk := list(itertools.islice(requests, batch)):
        await asyncio.gather(*chunk)


@dataclass
class Cluster:
    manager: ScyllaClusterManager
    servers: list
    cql: object
    strategy: str

    @property
    def compaction_option(self) -> str:
        return f"compaction = {{'class': '{self.strategy}'}}"

    async def running(self) -> list:
        running = {s.server_id for s in await self.manager.running_servers()}
        return [s for s in self.servers if s.server_id in running]

    async def nodetool_all(self, *args) -> None:
        for s in await self.running():
            await run_nodetool(self.manager, s, *args)

    async def flush(self) -> None:
        await self.nodetool_all("flush")

    async def compact(self, *args) -> None:
        await self.nodetool_all("compact", *args)

    async def wait_for_compactions(self) -> None:
        for s in await self.running():
            await wait_for_compactions(self.manager, s)

    async def marks(self) -> dict:
        return {s.server_id: await (await self.manager.server_open_log(s.server_id)).mark() for s in self.servers}


async def prepare_cluster(manager: ScyllaClusterManager, strategy: str, nodes: int = 1, rf: int = 1, options: dict | None = None) -> Cluster:
    servers = await manager.servers_add(nodes, config={**CONFIG, **(options or {})}, cmdline=CMDLINE,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(nodes)])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")
    return Cluster(manager, servers, cql, strategy)


async def create_large_partition_table(c: Cluster, table: str, with_static_column: bool = False) -> None:
    static = "static_value text static, " if with_static_column else ""
    await c.cql.run_async(f"CREATE TABLE IF NOT EXISTS {KS}.{table} (userid text, event text, value blob, {static}"
                          f"PRIMARY KEY (userid, event)) with compression = {{ }} and {c.compaction_option}")


async def create_large_partition_data(c: Cluster, table, partition_rows, partitions_num, one_blob_size, start_partition_index) -> dict:
    """Partitions user<k> of partition_rows rows, all written with the
    timestamp of the first (so TWCS keeps a partition in one window)."""
    expected = {}
    value = "a" * one_blob_size
    for k in range(start_partition_index, start_partition_index + partitions_num):
        user = f"user{k}"
        await c.cql.run_async(SimpleStatement(f"UPDATE {KS}.{table} SET value = textAsBlob('{value}') WHERE userid='{user}' and event='{day(0)}'",
                                              consistency_level=ConsistencyLevel.QUORUM))
        expected[user] = (one_blob_size + 8 + 8) * partition_rows
        [row] = await c.cql.run_async(SimpleStatement(f"SELECT writetime(value) as ts FROM {KS}.{table} WHERE userid='{user}' and event='{day(0)}'",
                                                      consistency_level=ConsistencyLevel.QUORUM))
        await gather_in_batches(c.cql.run_async(f"UPDATE {KS}.{table} USING TIMESTAMP {row.ts} SET value = textAsBlob('{value}') "
                                                f"WHERE userid='{user}' and event='{day(i)}'") for i in range(1, partition_rows))
    return expected


async def create_large_row_table(c: Cluster, table: str, columns_num: int) -> None:
    columns = ", ".join(f"value{i} blob" for i in range(columns_num))
    await c.cql.run_async(f"CREATE TABLE IF NOT EXISTS {KS}.{table} (userid text, event text, {columns}, PRIMARY KEY (userid, event)) "
                          f"with compression = {{ }} and {c.compaction_option}")


async def create_large_row_data(c: Cluster, table, rows_num, columns_num, one_blob_size, start_row_index) -> dict:
    """Rows (user<k>, day k) of columns_num blobs of one_blob_size."""
    expected = {}
    value = "a" * int(one_blob_size)
    for k in range(start_row_index, start_row_index + rows_num):
        for i in range(columns_num):
            await c.cql.run_async(f"UPDATE {KS}.{table} SET value{i} = textAsBlob('{value}') WHERE userid='user{k}' and event='{day(k)}'")
        expected[f"user{k}.{day(k)}"] = columns_num * int(one_blob_size)
    return expected


COLLECTION_COLUMN = {"map": "kvmap map<text, text>", "set": "myset set<text>", "list": "mylist list<text>"}
COLLECTION_ADD = {"map": "kvmap['key{j}'] = 'val{j}'", "set": "myset = myset + {{ 'cat{j}' }}", "list": "mylist = mylist + [ '{j}' ]"}
COLLECTION_REMOVE = {"map": "DELETE kvmap['key{i}'] FROM {table} WHERE userid='{user}' and event='{event}'",
                     "set": "UPDATE {table} SET myset = myset - {{ 'cat{i}' }} WHERE userid='{user}' and event='{event}'",
                     "list": "DELETE mylist[{i}] FROM {table} WHERE userid='{user}' and event='{event}'"}


async def create_too_many_rows_data(c: Cluster, table, rows_num, columns_num, one_blob_size, partition_index, start_row_index,
                                    collection_type="map", collection_elements=0, start_collection_element_index=0) -> None:
    value = "a" * int(one_blob_size)
    user = f"user{partition_index}"
    for k in range(start_row_index, start_row_index + rows_num):
        for i in range(columns_num):
            await c.cql.run_async(f"UPDATE {KS}.{table} SET value{i} = textAsBlob('{value}') WHERE userid='{user}' and event='{day(k)}'")
        for j in range(start_collection_element_index, start_collection_element_index + collection_elements):
            await c.cql.run_async(f"UPDATE {KS}.{table} SET {COLLECTION_ADD[collection_type].format(j=j)} WHERE userid='{user}' and event='{day(k)}'")


async def trigger_compaction_by_data_write_and_flush(c: Cluster, entity_type: str, index: int) -> int:
    """Five small writes, each flushed, then a major compaction."""
    for _ in range(5):
        if entity_type == "partition":
            await create_large_partition_data(c, TABLE, 2, 1, 1, index)
        else:
            await create_large_row_data(c, TABLE, 2, 1, 1, index)
        await c.flush()
        await asyncio.sleep(0.5)
    await c.compact()
    return 2


async def cluster_system_state(c: Cluster, entity_type: str, table: str, with_collection: bool = False) -> dict:
    """Per running node: the records of the table in its own
    system.large_<entity>s, read as the dtest does through a new session to
    that node alone, in the keyspace; and the sstables on its disk."""
    state = {}
    for s in await c.running():
        session = await c.manager.get_cql_exclusive(s)
        try:
            session.set_keyspace(KS)
            columns = (["clustering_key"] if entity_type in ("row", "cell") else []) + [f"{entity_type}_size"]
            if entity_type == "partition":
                columns.append("rows")
            if with_collection and entity_type == "cell":
                columns.append("collection_elements")
            rows = list(session.execute(f"select sstable_name, partition_key, {','.join(columns)} from system.large_{entity_type}s "
                                        f"where keyspace_name='{KS}' and table_name='{table}'"))
        finally:
            session.cluster.shutdown()
        info = {"partition_keys": set(), f"{entity_type}_size": defaultdict(int), "rows": defaultdict(int),
                "collection_elements": defaultdict(int), "sstables": set(), "key_appearance": defaultdict(int)}
        for row in rows:
            if entity_type == "partition":
                key = row.partition_key
                info["rows"][key] = row.rows
            else:
                key = f"{row.partition_key}.{row.clustering_key}"
                if with_collection and entity_type == "cell":
                    info["collection_elements"][key] = row.collection_elements
            info["partition_keys"].add(key)
            info[f"{entity_type}_size"][key] += getattr(row, f"{entity_type}_size")
            if entity_type == "cell":
                info["key_appearance"][key] += 1
            info["sstables"].add(os.path.basename(row.sstable_name))
        table_dir = await get_table_dir(c.manager, s, KS, table)
        state[s.server_id] = {"info": info, "sstables_from_disk": {os.path.basename(f) for f in glob.glob(f"{table_dir}/*-Data.db")}}
    return state


async def validate_system_table(c: Cluster, entity_type, table, expected_entity_number, expected_entity_data_size,
                                pk_max_index=None, data_column=None, with_collection=False) -> dict:
    state = await cluster_system_state(c, entity_type, table, with_collection)
    keys = set()
    key_appearance = 0
    for node in state.values():
        keys |= node["info"]["partition_keys"]
        key_appearance = sum(node["info"]["key_appearance"].values())
    count = key_appearance if entity_type == "cell" else len(keys)
    assert count == expected_entity_number, f"expected {expected_entity_number} large {entity_type}s, got {count}"
    if pk_max_index is not None:
        index = (lambda k: int(k.replace("user", ""))) if entity_type == "partition" else (lambda k: int(k.split(".")[0].replace("user", "")))
        wrong = [k for k in keys if index(k) > pk_max_index]
        assert not wrong, f"small {entity_type}s recorded as large: {wrong}"
    data_column = data_column or ("collection_elements" if with_collection else f"{entity_type}_size")
    expected_size = expected_entity_data_size if expected_entity_number else None
    for server_id, node in state.items():
        info = node["info"][data_column]
        if expected_size is None:
            assert not info, f"node {server_id}: unexpected {data_column} records: {dict(info)}"
            continue
        assert info, f"node {server_id}: no {data_column} records"
        for pk, size in info.items():
            expected = expected_size if isinstance(expected_size, int) else expected_size.get(pk)
            assert expected is not None, f"the {entity_type} {pk} is not expected to be large"
            assert expected <= size < expected * 103 / 100, f"{pk}: expected {expected}..+3%, got {size}"
    for server_id, node in state.items():
        if node["info"]["partition_keys"]:
            assert node["info"]["sstables"] <= node["sstables_from_disk"], f"node {server_id}: {node}"
    return state


async def validate_log_warnings(c: Cluster, state, entity_type, table, marks=None, expect_warning=True) -> None:
    marks = marks or {}
    for s in c.servers:
        log = await c.manager.server_open_log(s.server_id)
        writing = f"Writing large {entity_type} {KS}/{table}:"
        node = state.get(s.server_id)
        if not node or not node["info"]["partition_keys"]:
            if not await log.grep(f"Dropping entries from large_{entity_type}s: ks = {KS}, table = {table},", from_mark=marks.get(s.server_id)):
                assert not await log.grep(writing, from_mark=marks.get(s.server_id)), f"unexpected warning {writing} on node {s.server_id}"
            continue
        if expect_warning and node["sstables_from_disk"]:
            await log.wait_for(writing, from_mark=marks.get(s.server_id), timeout=10)
        else:
            assert not await log.grep(writing, from_mark=marks.get(s.server_id)), f"unexpected warning {writing} on node {s.server_id}"


async def set_ttl_on_few_rows_in_partition(c: Cluster, partition_num, expected_partitions, ttl_rows_amount) -> dict:
    userid = f"user{random.randint(0, partition_num - 1)}"
    events = list(await c.cql.run_async(f"SELECT event FROM {KS}.{TABLE} WHERE userid='{userid}'"))
    value = "b" * 1024
    for i, row in enumerate(events[:ttl_rows_amount]):
        await c.cql.run_async(f"UPDATE {KS}.{TABLE} USING TTL 10 SET value = textAsBlob('{value}') WHERE userid='{userid}' and event='{row.event}'")
    await c.flush()
    await asyncio.sleep(15)
    await c.compact(KS, TABLE)
    expected_partitions.pop(userid)
    return expected_partitions


async def set_ttl_on_few_large_rows(c: Cluster, rows_num, columns_num, expected_rows) -> dict:
    userid = f"user{random.randint(0, rows_num - 1)}"
    [row] = list(await c.cql.run_async(f"SELECT event FROM {KS}.{TABLE} WHERE userid='{userid}' LIMIT 1"))
    value = "b" * BLOB_SIZE_10K
    columns = ", ".join(f"value{i} = textAsBlob('{value}')" for i in range(columns_num))
    await c.cql.run_async(f"UPDATE {KS}.{TABLE} USING TTL 10 SET {columns} WHERE userid='{userid}' and event='{row.event}'")
    await c.flush()
    await asyncio.sleep(15)
    await c.compact(KS, TABLE)
    expected_rows.pop(f"{userid}.{row.event}")
    return expected_rows


async def wait_for_view(c: Cluster, view: str, timeout: float = 600) -> None:
    """The dtests' wait_for_view(): all running nodes report the view built
    in system_distributed.view_build_status, read at QUORUM."""
    host_ids = {str(await c.manager.get_host_id(s.server_id)) for s in await c.running()}
    query = SimpleStatement(f"SELECT host_id,status FROM system_distributed.view_build_status WHERE keyspace_name = '{KS}' AND view_name = '{view}'",
                            consistency_level=ConsistencyLevel.QUORUM)
    deadline = time.time() + timeout
    while time.time() < deadline:
        done = {str(r.host_id) for r in await c.cql.run_async(query) if r.status == "SUCCESS"}
        if host_ids <= done:
            return
        await asyncio.sleep(1)
    raise TimeoutError(f"view {view} was not built")


# A partition reaching compaction_rows_count_warning_threshold (200 rows) is
# recorded with its row count once one more row is written.
async def test_too_many_rows(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_row_warning_threshold_mb": 10, "compaction_rows_count_warning_threshold": 200})
    await create_large_row_table(c, TABLE, 14)
    await create_too_many_rows_data(c, TABLE, 200, 14, 128, 0, 0)
    await c.flush()
    await c.wait_for_compactions()
    await validate_system_table(c, "partition", TABLE, 0, 200, data_column="rows")
    await create_too_many_rows_data(c, TABLE, 1, 14, 128, 0, 200)
    await c.flush()
    await c.compact()
    await validate_system_table(c, "partition", TABLE, 1, 201, data_column="rows")


# A collection over compaction_collection_elements_count_warning_threshold
# (10) is recorded in system.large_cells, and no longer after an element is
# deleted and the tombstone purged (tombstone_gc "immediate").
@pytest.mark.parametrize("collection_type", ["map", "set", "list"])
async def test_too_many_collection_elements(manager: ScyllaClusterManager, strategy, collection_type):
    c = await prepare_cluster(manager, strategy, options={"compaction_collection_elements_count_warning_threshold": 10})
    await c.cql.run_async(f"CREATE TABLE IF NOT EXISTS {KS}.{TABLE} (userid text, event text, value0 blob, {COLLECTION_COLUMN[collection_type]}, "
                          f"PRIMARY KEY (userid, event)) with compression = {{ }} and {c.compaction_option}")
    await c.cql.run_async(f"ALTER TABLE {KS}.{TABLE} WITH tombstone_gc = {{'mode': 'immediate'}}")
    await create_too_many_rows_data(c, TABLE, 1, 1, 128, 0, 0, collection_type=collection_type, collection_elements=10)
    await c.flush()
    await c.wait_for_compactions()
    await validate_system_table(c, "cell", TABLE, 0, 10, with_collection=True)
    await create_too_many_rows_data(c, TABLE, 1, 1, 128, 0, 0, collection_type=collection_type, collection_elements=1, start_collection_element_index=10)
    await c.flush()
    await c.compact()
    await validate_system_table(c, "cell", TABLE, 1, 11, with_collection=True)
    await run_nodetool(manager, c.servers[0], "snapshot")
    i = random.randint(0, 10)
    await c.cql.run_async(COLLECTION_REMOVE[collection_type].format(i=i, table=f"{KS}.{TABLE}", user="user0", event=day(0)))
    await c.flush()
    await asyncio.sleep(1)
    await c.compact()
    await wait_for_compactions(manager, c.servers[0])
    await validate_system_table(c, "cell", TABLE, 0, 10, with_collection=True)


# With column_index_size_in_kb 1, a row of 100,000 columns gets a large
# promoted index; random IN reads of 1-3 of them return each one.
async def test_column_index_stress(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"column_index_size_in_kb": 1})
    await c.cql.run_async(f"CREATE TABLE {KS}.test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name)) WITH {c.compaction_option}")
    await gather_in_batches((c.cql.run_async(f"UPDATE {KS}.test_table SET value = {i} WHERE row = 'row0' AND name = 'val{i}';")
                             for i in range(100000)), batch=20)
    async def read():
        names = [str(random.randint(0, 99999)) for _ in range(3)]
        rows = list(await c.cql.run_async(f"SELECT value FROM {KS}.test_table WHERE row='row0' AND name in ('val{names[0]}', 'val{names[1]}', 'val{names[2]}');"))
        assert len(rows) == len(set(names))
    await gather_in_batches((read() for _ in range(10000)), batch=20)


# Static cells of 1-10 MB, flushed ten times with a node down, then the
# node repaired and the tables compacted, do not crash the node (#6780).
async def test_large_row_with_static_cell(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, nodes=3, rf=3, options={"compaction_large_partition_warning_threshold_mb": 1, "hinted_handoff_enabled": False})
    await manager.server_stop_gracefully(c.servers[1].server_id)
    await create_large_partition_table(c, TABLE, with_static_column=True)
    large_data_1_mb = "x" * 1024 * 1024
    for _ in range(10):
        for index in range(1, 11):
            await c.cql.run_async(f"INSERT INTO {KS}.{TABLE} (userid, event, static_value) VALUES ('user{index}', '{day(index)}', "
                                  f"'{large_data_1_mb * random.choice(range(1, 11))}')")
        await c.flush()
    await manager.server_start(c.servers[1].server_id)
    await run_nodetool(manager, c.servers[1], "repair")
    await c.compact()


# Five partitions of 15,000 rows are all recorded as large, with warnings.
async def test_large_partition_detector_multipartition(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_partition_warning_threshold_mb": 1})
    await create_large_partition_table(c, TABLE)
    expected = await create_large_partition_data(c, TABLE, 15000, 5, 1024, 0)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "partition", 5)
    state = await validate_system_table(c, "partition", TABLE, 5, expected)
    await validate_log_warnings(c, state, "partition", TABLE)


async def large_partition_then_ttl(manager, strategy, ttl_rows_amount):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_partition_warning_threshold_mb": 4})
    await create_large_partition_table(c, TABLE)
    expected = await create_large_partition_data(c, TABLE, 10000, 1, 1024, 0)
    await c.flush()
    extra = await trigger_compaction_by_data_write_and_flush(c, "partition", 1)
    state = await validate_system_table(c, "partition", TABLE, 1, expected)
    await validate_log_warnings(c, state, "partition", TABLE)
    expected = await set_ttl_on_few_rows_in_partition(c, 1, expected, ttl_rows_amount)
    await trigger_compaction_by_data_write_and_flush(c, "partition", 1 + extra)
    marks = await c.marks()
    state = await validate_system_table(c, "partition", TABLE, 0, expected)
    await validate_log_warnings(c, state, "partition", TABLE, marks=marks)


# A large partition is no longer recorded once 9,000 of its 10,000 rows
# expired (TTL 10 s) and were compacted away.
async def test_large_partition_detector_with_ttl_on_few_row_in_partition(manager: ScyllaClusterManager, strategy):
    await large_partition_then_ttl(manager, strategy, 9000)


# The same when all its rows expired.
async def test_large_partition_detector_with_ttl_on_partition(manager: ScyllaClusterManager, strategy):
    await large_partition_then_ttl(manager, strategy, 10000)


# Large rows written with one of three nodes down are recorded on the
# others, and still after the node is back and repaired.
async def test_large_row_detector_with_node_stop(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, nodes=3, rf=3, options={"compaction_large_row_warning_threshold_mb": 1, "hinted_handoff_enabled": False})
    await manager.server_stop_gracefully(c.servers[1].server_id)
    await create_large_row_table(c, TABLE, 15)
    expected = await create_large_row_data(c, TABLE, 5, 15, BLOB_SIZE_10K * 10, 0)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "row", 5)
    state = await validate_system_table(c, "row", TABLE, 5, expected)
    await validate_log_warnings(c, state, "row", TABLE)
    await manager.server_start(c.servers[1].server_id)
    await run_nodetool(manager, c.servers[1], "repair")
    state = await validate_system_table(c, "row", TABLE, 5, expected)
    await validate_log_warnings(c, state, "row", TABLE)


# Rows of 1.4 MB are recorded as large rows, with their sizes.
async def test_simple_large_row_detector(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_row_warning_threshold_mb": 1})
    await create_large_row_table(c, TABLE, 14)
    expected = await create_large_row_data(c, TABLE, 5, 14, BLOB_SIZE_10K * 10, 0)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "row", 5)
    state = await validate_system_table(c, "row", TABLE, 5, expected)
    await validate_log_warnings(c, state, "row", TABLE)


# A large row overwritten with TTL'd small values is no longer recorded
# after it expires and is compacted.
async def test_large_row_detector_with_ttl_on_row(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_row_warning_threshold_mb": 1})
    await create_large_row_table(c, TABLE, 20)
    expected = await create_large_row_data(c, TABLE, 2, 20, BLOB_SIZE_10K * 10, 0)
    await c.flush()
    extra = await trigger_compaction_by_data_write_and_flush(c, "row", 2)
    state = await validate_system_table(c, "row", TABLE, 2, expected)
    await validate_log_warnings(c, state, "row", TABLE)
    expected = await set_ttl_on_few_large_rows(c, 2, 20, expected)
    await trigger_compaction_by_data_write_and_flush(c, "row", 2 + extra)
    marks = await c.marks()
    state = await validate_system_table(c, "row", TABLE, 1, expected)
    await validate_log_warnings(c, state, "row", TABLE, marks=marks, expect_warning=False)


# Only the large row is recorded, not the small ones.
async def test_large_row_detector_with_small_rows(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_row_warning_threshold_mb": 1})
    await create_large_row_table(c, TABLE, 12)
    expected = await create_large_row_data(c, TABLE, 1, 12, BLOB_SIZE_10K * 10, 0)
    await create_large_row_data(c, TABLE, 10, 12, 10, 1)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "row", 11)
    state = await validate_system_table(c, "row", TABLE, 1, expected, pk_max_index=0)
    await validate_log_warnings(c, state, "row", TABLE)


# A partition of 600 rows of 1 KB is not recorded or warned about.
async def test_large_partition_detector_small_partition(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy)
    await create_large_partition_table(c, TABLE)
    await create_large_partition_data(c, TABLE, 600, 1, 1024, 0)
    await c.flush()
    state = await validate_system_table(c, "partition", TABLE, 0, None)
    await validate_log_warnings(c, state, "partition", TABLE, expect_warning=False)


# A row of ten 10 KB cells is not recorded or warned about.
async def test_large_row_detector_small_row(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy)
    await create_large_row_table(c, TABLE, 10)
    await create_large_row_data(c, TABLE, 1, 10, BLOB_SIZE_10K, 0)
    await c.flush()
    state = await validate_system_table(c, "row", TABLE, 0, None)
    await validate_log_warnings(c, state, "row", TABLE, expect_warning=False)


# A large partition is recorded for the base table and for a materialized
# view of it, on three nodes (RF=3).
async def test_large_partition_in_materialized_view(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, nodes=3, rf=3, options={"compaction_large_partition_warning_threshold_mb": 1})
    view = f"{TABLE}_view"
    await create_large_partition_table(c, TABLE)
    expected = await create_large_partition_data(c, TABLE, 15000, 1, 1024, 0)
    await c.cql.run_async(f"create materialized view {KS}.{view} as select * from {KS}.{TABLE} where userid is not null and event is not null primary key (userid, event)")
    await wait_for_view(c, view)
    await c.flush()
    await c.compact(KS, TABLE)
    for table in (TABLE, view):
        state = await validate_system_table(c, "partition", table, 1, expected)
        await validate_log_warnings(c, state, "partition", table)


# Only the 1 MB cells are recorded in system.large_cells, not 10 KB ones.
async def test_large_cell_detector_with_small_cells(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy)
    await create_large_row_table(c, TABLE, 3)
    expected = await create_large_row_data(c, TABLE, 10, 3, BLOB_SIZE_1MB, 0)
    await create_large_row_data(c, TABLE, 10, 3, BLOB_SIZE_10K, 10)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "cell", 20)
    state = await validate_system_table(c, "cell", TABLE, 30, expected, pk_max_index=9)
    await validate_log_warnings(c, state, "cell", TABLE)


async def large_cells(manager, strategy, columns_num, rows_number):
    c = await prepare_cluster(manager, strategy)
    await create_large_row_table(c, TABLE, columns_num)
    expected = await create_large_row_data(c, TABLE, rows_number, columns_num, BLOB_SIZE_1MB, 0)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "cell", rows_number)
    state = await validate_system_table(c, "cell", TABLE, columns_num * rows_number, expected)
    await validate_log_warnings(c, state, "cell", TABLE)


# Every 1 MB cell is recorded: 3 columns of 10 rows, 10 of 5, 5 of 10.
async def test_multiple_rows_with_large_cells_detector(manager: ScyllaClusterManager, strategy):
    await large_cells(manager, strategy, 3, 10)


async def test_multiple_columns_with_large_cells_detector(manager: ScyllaClusterManager, strategy):
    await large_cells(manager, strategy, 10, 5)


async def test_multiple_rows_and_columns_with_large_cells_detector(manager: ScyllaClusterManager, strategy):
    await large_cells(manager, strategy, 5, 10)


# Large cells are recorded for the base table and a materialized view of
# it, on three nodes (RF=3).
async def test_large_cell_in_materialized_view(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, nodes=3, rf=3)
    view = f"{TABLE}_view"
    await create_large_row_table(c, TABLE, 2)
    expected = await create_large_row_data(c, TABLE, 3, 2, BLOB_SIZE_1MB, 0)
    await c.cql.run_async(f"create materialized view {KS}.{view} as select * from {KS}.{TABLE} where userid is not null and event is not null primary key (userid, event)")
    await wait_for_view(c, view)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "cell", 3)
    for table in (TABLE, view):
        state = await validate_system_table(c, "cell", table, 6, expected)
        await validate_log_warnings(c, state, "cell", table)


# The large cells of a row overwritten with TTL'd small values are no
# longer recorded after they expire and are compacted.
async def test_large_cell_detector_with_ttl_on_row(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy)
    await create_large_row_table(c, TABLE, 6)
    expected = await create_large_row_data(c, TABLE, 10, 6, BLOB_SIZE_1MB, 0)
    await c.flush()
    extra = await trigger_compaction_by_data_write_and_flush(c, "cell", 10)
    state = await validate_system_table(c, "cell", TABLE, 60, expected)
    await validate_log_warnings(c, state, "cell", TABLE)
    expected = await set_ttl_on_few_large_rows(c, 10, 6, expected)
    await trigger_compaction_by_data_write_and_flush(c, "cell", 10 + extra)
    marks = await c.marks()
    state = await validate_system_table(c, "cell", TABLE, 54, expected)
    await validate_log_warnings(c, state, "cell", TABLE, marks=marks, expect_warning=False)


# With compaction_large_cell_warning_threshold_mb 2, 1 MB cells are not
# recorded and 2 MB ones are.
async def test_large_cell_after_threshold_change(manager: ScyllaClusterManager, strategy):
    small_rows, rows_number, columns_num = 20, 10, 3
    c = await prepare_cluster(manager, strategy, options={"compaction_large_cell_warning_threshold_mb": 2})
    await create_large_row_table(c, TABLE, columns_num)
    await create_large_row_data(c, TABLE, small_rows, columns_num, BLOB_SIZE_1MB, 0)
    await c.flush()
    extra = await trigger_compaction_by_data_write_and_flush(c, "cell", small_rows)
    await validate_system_table(c, "cell", TABLE, 0, 0, pk_max_index=0)
    expected = await create_large_row_data(c, TABLE, rows_number, columns_num, BLOB_SIZE_1MB * 2, small_rows + extra)
    await create_large_row_data(c, TABLE, rows_number, columns_num, BLOB_SIZE_1MB, rows_number + small_rows + extra)
    await c.flush()
    max_pk = small_rows + rows_number + extra - 1
    await trigger_compaction_by_data_write_and_flush(c, "cell", rows_number * 2 + small_rows + extra)
    state = await validate_system_table(c, "cell", TABLE, columns_num * rows_number, expected, pk_max_index=max_pk)
    await validate_log_warnings(c, state, "cell", TABLE)


# The detectors with a major compaction instead of the small-write
# trigger: only the large cells, row or partition are recorded.
async def test_large_cell_detector_with_full_compaction(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy)
    await create_large_row_table(c, TABLE, 3)
    expected = await create_large_row_data(c, TABLE, 3, 3, BLOB_SIZE_1MB, 0)
    await create_large_row_data(c, TABLE, 3, 3, BLOB_SIZE_1MB / 2, 3)
    await c.flush()
    await c.compact(KS, TABLE)
    state = await validate_system_table(c, "cell", TABLE, 9, expected, pk_max_index=2)
    await validate_log_warnings(c, state, "cell", TABLE)


async def test_large_row_detector_with_full_compaction(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_row_warning_threshold_mb": 1})
    await create_large_row_table(c, TABLE, 15)
    expected = await create_large_row_data(c, TABLE, 1, 15, BLOB_SIZE_10K * 10, 0)
    await create_large_row_data(c, TABLE, 10, 15, 10, 1)
    await c.flush()
    await c.compact(KS, TABLE)
    state = await validate_system_table(c, "row", TABLE, 1, expected, pk_max_index=0)
    await validate_log_warnings(c, state, "row", TABLE)


async def test_large_partition_detector_with_full_compaction(manager: ScyllaClusterManager, strategy):
    c = await prepare_cluster(manager, strategy, options={"compaction_large_partition_warning_threshold_mb": 4})
    await create_large_partition_table(c, TABLE)
    expected = await create_large_partition_data(c, TABLE, 10000, 1, 1024, 0)
    await create_large_partition_data(c, TABLE, 3000, 10, 1024, 2)
    await c.flush()
    await c.compact(KS, TABLE)
    await create_large_partition_data(c, TABLE, 3000, 10, 1024, 3)
    await c.flush()
    await c.compact(KS, TABLE)
    state = await validate_system_table(c, "partition", TABLE, 1, expected, pk_max_index=0)
    await validate_log_warnings(c, state, "partition", TABLE)


# Ported from scylla-dtest wide_rows_test.py::TestWideRows::test_large_partition_detector_with_node_stop.
# A large partition written with one of three nodes (RF=3, no hinted
# handoff) down is recorded and warned about on the others, and still after
# the node is back and repaired.
async def test_large_partition_detector_with_node_stop(manager: ScyllaClusterManager, strategy):
    partition_num, extra = 1, 0
    c = await prepare_cluster(manager, strategy, nodes=3, rf=3,
                              options={"compaction_large_partition_warning_threshold_mb": 1, "hinted_handoff_enabled": False})
    node2 = c.servers[1]
    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)
    await create_large_partition_table(c, TABLE)
    expected = await create_large_partition_data(c, TABLE, 10000, partition_num, 1024, 0)
    await c.flush()
    extra += await trigger_compaction_by_data_write_and_flush(c, "partition", partition_num)
    state = await validate_system_table(c, "partition", TABLE, partition_num, expected)
    await validate_log_warnings(c, state, "partition", TABLE)
    await manager.server_start(node2.server_id)
    await manager.servers_see_each_other(c.servers)
    await run_nodetool(manager, node2, "repair")
    extra += await trigger_compaction_by_data_write_and_flush(c, "partition", partition_num + extra)
    state = await validate_system_table(c, "partition", TABLE, partition_num, expected)
    await validate_log_warnings(c, state, "partition", TABLE)


# Ported from scylla-dtest wide_rows_test.py::TestWideRows::test_large_partition_detector_with_small_partitions.
# With compaction_large_partition_warning_threshold_mb 4, a partition of
# 10000 rows of 1 KB is recorded and warned about, and ten of 3000 rows are not.
async def test_large_partition_detector_with_small_partitions(manager: ScyllaClusterManager, strategy):
    partition_num, small_partition_num = 1, 10
    c = await prepare_cluster(manager, strategy, options={"compaction_large_partition_warning_threshold_mb": 4})
    await create_large_partition_table(c, TABLE)
    expected = await create_large_partition_data(c, TABLE, 10000, partition_num, 1024, 0)
    await create_large_partition_data(c, TABLE, 3000, small_partition_num, 1024, partition_num + 1)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "partition", partition_num + small_partition_num)
    state = await validate_system_table(c, "partition", TABLE, partition_num, expected, pk_max_index=partition_num - 1)
    await validate_log_warnings(c, state, "partition", TABLE)


# Ported from scylla-dtest wide_rows_test.py::TestWideRows::test_large_row_in_materialized_view.
# On three nodes (RF=3), a row of 200 columns of 10 KB is recorded as large
# and warned about for the base table and for a materialized view of it.
async def test_large_row_in_materialized_view(manager: ScyllaClusterManager, strategy):
    rows_number, columns_num = 1, 200
    view = f"{TABLE}_view"
    c = await prepare_cluster(manager, strategy, nodes=3, rf=3, options={"compaction_large_row_warning_threshold_mb": 1})
    await create_large_row_table(c, TABLE, columns_num)
    expected = await create_large_row_data(c, TABLE, rows_number, columns_num, BLOB_SIZE_10K, 0)
    await c.cql.run_async(f"create materialized view {KS}.{view} as select * from {KS}.{TABLE} where userid is not null and event is not null primary key (userid, event)")
    await wait_for_view(c, view)
    await c.flush()
    await trigger_compaction_by_data_write_and_flush(c, "row", rows_number)
    for table in (TABLE, view):
        state = await validate_system_table(c, "row", table, rows_number, expected)
        await validate_log_warnings(c, state, "row", table)


# Ported from scylla-dtest wide_rows_test.py::TestWideRows::test_large_cell_and_then_increase_threshold.
# 1 MB cells are recorded as large; after the nodes restart with
# compaction_large_cell_warning_threshold_mb 2 and more 1 MB cells are
# written and compacted, the records are gone.
async def test_large_cell_and_then_increase_threshold(manager: ScyllaClusterManager, strategy):
    small_rows, rows_number, columns_num, extra = 10, 5, 3, 0
    c = await prepare_cluster(manager, strategy)
    await create_large_row_table(c, TABLE, columns_num)
    expected = await create_large_row_data(c, TABLE, rows_number, columns_num, BLOB_SIZE_1MB, 0)
    await c.flush()
    extra += await trigger_compaction_by_data_write_and_flush(c, "cell", rows_number)
    state = await validate_system_table(c, "cell", TABLE, columns_num * rows_number, expected, pk_max_index=rows_number - 1)
    await validate_log_warnings(c, state, "cell", TABLE)

    # The threshold change takes effect with a restart of the nodes.
    for s in c.servers:
        await manager.server_update_config(s.server_id, "compaction_large_cell_warning_threshold_mb", 2, reload=False)
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_start(s.server_id)
    c.cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(c.cql, c.servers, time.time() + 60)

    await c.flush()
    extra += await trigger_compaction_by_data_write_and_flush(c, "cell", rows_number + extra)
    await create_large_row_data(c, TABLE, small_rows, columns_num, BLOB_SIZE_1MB, rows_number + extra)
    await c.flush()
    extra += await trigger_compaction_by_data_write_and_flush(c, "cell", rows_number + small_rows + extra)
    await validate_system_table(c, "cell", TABLE, 0, 0, pk_max_index=0)
