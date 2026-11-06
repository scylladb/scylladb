#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Compaction on a node of its own: tombstones written, kept and purged,
large partition warnings, and the compaction strategies' handling of time
windows, TTLs and reshape.

Ported from scylla-dtest. Like the dtests, the nodes create keyspaces with
vnodes by default, and log compaction at debug level; the sstables are read
with "scylla sstable dump-data", as the dtests' node.dump_sstables() did.
"""

import asyncio
import datetime
import glob
import itertools
import json
import logging
import os
import random
import re
import shutil
import string
import time
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args

from test.cluster.dtest_helpers import dtest_session, start_node_wait_cql
from test.cluster.util import cassandra_stress, dump_sstables, get_table_dir, reconnect_driver, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
# The compaction dtests ran with compaction debug logging.
CMDLINE = ["--logger-log-level", "compaction=debug"]
KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
STRATEGIES = ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"]


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


############################################################################
# Ported from compaction_test.py.

async def compaction_delete(manager: ScyllaClusterManager, strategy: str, build_mode: str):
    """Writes 100 partitions, deletes 10 of them and flushes: the deleted
    keys read as missing and the sstables hold 10 partition tombstones.
    Returns the server, a cql session, a time before which no tombstone
    expires and one after which all of them expired."""
    # Must exceed the write-to-last-"before"-check time; debug is several times slower.
    gc_grace_seconds = 45 if build_mode == "debug" else 25
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {KS_OPTS}")
    await cql.run_async(f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{strategy}'}} "
                        f"and gc_grace_seconds = {gc_grace_seconds} and tombstone_gc = {{'mode': 'timeout'}};")
    for x in range(100):
        await cql.run_async(f"insert into ks.cf (key, val) values ({x},1)")
    await run_nodetool(manager, server, "flush")
    tombstone_expiry_time = time.time() + gc_grace_seconds
    for x in range(10):
        await cql.run_async(f"delete from ks.cf where key = {x}")
    # Sampled after the deletes: the sleep must end past the last tombstone's expiry.
    last_tombstone_expiry = time.time() + gc_grace_seconds
    await run_nodetool(manager, server, "flush")
    for x in range(10):
        assert list(await cql.run_async(f"select * from ks.cf where key = {x}")) == []
    partitions = await dump_sstables(manager, server, "ks", "cf")
    assert sum("tombstone" in p for p in partitions) == 10, partitions
    return server, cql, tombstone_expiry_time, last_tombstone_expiry


async def compact_and_count_tombstones(manager: ScyllaClusterManager, server, cql) -> int:
    # Force an update so that the major compaction has something to do.
    await cql.run_async("insert into ks.cf (key, val) values (99,1);")
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    return sum("tombstone" in p for p in await dump_sstables(manager, server, "ks", "cf"))


# DELETEs flushed to sstables are kept there as partition tombstones.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", STRATEGIES)
async def test_compaction_delete(manager: ScyllaClusterManager, strategy: str, build_mode: str):
    await compaction_delete(manager, strategy, build_mode)


# Partition tombstones survive a major compaction before gc_grace_seconds
# (tombstone_gc mode "timeout", 25 s) passed, and are purged by one after
# the last tombstone expired.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", STRATEGIES)
async def test_compaction_delete_purges_after_gc_grace(manager: ScyllaClusterManager, strategy: str, build_mode: str):
    server, cql, expiry, last_expiry = await compaction_delete(manager, strategy, build_mode)
    assert await compact_and_count_tombstones(manager, server, cql) == 10
    assert time.time() < expiry, "the first major compaction ran after the tombstones expired"
    await asyncio.sleep(max(0, last_expiry - time.time()) + 1)
    assert await compact_and_count_tombstones(manager, server, cql) == 0


# A compaction writing a partition larger than
# compaction_large_partition_warning_threshold_mb (10 MB) logs a "Writing
# large row" warning and records the partition in system.large_partitions
# (CASSANDRA-9643).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_large_compaction_warning(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, 'compaction_large_partition_warning_threshold_mb': 10}, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {KS_OPTS}")
    await cql.run_async("CREATE TABLE ks.large(userid text PRIMARY KEY, properties map<int, text>) with compression = {}")
    strlen = (10 * 1024 * 1024) // 100
    for i in range(200):
        await cql.run_async(f"UPDATE ks.large SET properties[{i}] = '{'x' * strlen}' WHERE userid = 'user'")
    async def check_properties():
        ret = list(await cql.run_async("SELECT properties from ks.large where userid = 'user'"))
        assert len(ret) == 1 and len(ret[0][0].keys()) == 200
    await check_properties()
    await run_nodetool(manager, server, "flush")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await run_nodetool(manager, server, "compact", "ks", "large")
    await log.wait_for(r"Writing large row ks/large:.* \(\d+ bytes\)", from_mark=mark, timeout=180)
    await check_properties()
    [row] = list(await cql.run_async("SELECT * from system.large_partitions"))
    assert row.partition_size == pytest.approx(20974000, 50)
    assert row.partition_key == "user"


############################################################################
# Ported from compaction_additional_test.py.

# The dtests' nodes validated sstable keys, and assumed one compaction
# group per shard.
ADD_CONFIG = {**CONFIG, 'enable_sstable_key_validation': True, 'tablets_initial_scale_factor': 1}
SMP = 2  # test.py's nodes run with --smp 2
RESOURCES = os.path.join(os.path.dirname(__file__), "..", "resource", "sstables")


async def create_ks(cql, name: str, rf: int) -> None:
    await cql.run_async(f"CREATE KEYSPACE {name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")


async def create_c1c2_cf(cql, ks: str, cf: str, compaction: dict | None = None, gc_grace: int | None = None,
                         tombstone_gc: str | None = None, default_ttl: int | None = None) -> None:
    """The dtests' create_cf() of a (key varchar PRIMARY KEY, c1 text, c2
    text) table, including the SELECT it reads the new table with."""
    compaction = compaction or {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}
    query = f"CREATE COLUMNFAMILY {ks}.{cf} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' AND compaction={compaction} AND compression = {{}}"
    if gc_grace is not None:
        query += f" AND gc_grace_seconds={gc_grace}"
    if tombstone_gc is not None:
        query += f" AND tombstone_gc = {{'mode': '{tombstone_gc}'}}"
    if default_ttl is not None:
        query += f" AND default_time_to_live={default_ttl}"
    await cql.run_async(query)
    await cql.run_async(f"SELECT * FROM {ks}.{cf} LIMIT 1")


async def insert_c1c2(cql, keys=None, n=None, ks="ks", cf="cf", timestamp=None, consistency=ConsistencyLevel.QUORUM,
                      concurrency: int = 20) -> None:
    """The dtests' insert_c1c2(): prepared INSERTs of keys k<i>, `concurrency` at a time."""
    keys = list(range(n)) if n is not None else keys
    using_timestamp = f" USING TIMESTAMP {timestamp}" if timestamp is not None else ""
    stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2'){using_timestamp}")
    stmt.consistency_level = consistency
    for i in range(0, len(keys), concurrency):
        await asyncio.gather(*[cql.run_async(stmt, [f"k{k}"]) for k in keys[i:i + concurrency]])


async def wait_for_compactions(manager: ScyllaClusterManager, server, quiesce: float = 0.5, timeout: float = 600) -> None:
    """Like ccm's node.wait_for_compactions(): poll "nodetool compactionstats"
    until no compaction is pending or running for `quiesce` seconds."""
    idle_since = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _, _ = await run_nodetool(manager, server, "compactionstats")
        tasks = sum(int(m.group(1)) for m in re.finditer(r"^\s*- \S+\.\S+: (\d+)$", out, re.MULTILINE))
        tasks += len(re.findall(r"^[0-9a-f]{8}-[0-9a-f]{4}-", out, re.MULTILINE))
        if tasks == 0:
            idle_since = idle_since or time.time()
            if time.time() - idle_since >= quiesce:
                return
        else:
            idle_since = None
        await asyncio.sleep(0.1)
    raise TimeoutError("compactions did not finish")


async def sstable_files(manager: ScyllaClusterManager, server, ks: str, cf: str, component: str = "Data") -> set[str]:
    return set(glob.glob(f"{await get_table_dir(manager, server, ks, cf)}/*-{component}.db"))


async def sstable_stats(manager: ScyllaClusterManager, server, files) -> dict[str, dict]:
    """The "stats" of the given sstables, as printed by "scylla sstable
    dump-statistics" (the dtests' node.dump_sstable_stats())."""
    files = list(files)
    exe = await manager.server_get_exe(server.server_id)
    proc = await asyncio.create_subprocess_exec(exe, "sstable", "dump-statistics", *files, cwd=os.path.dirname(files[0]),
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    assert proc.returncode == 0, err.decode()
    return {os.path.basename(f): v["stats"] for f, v in json.loads(out)["sstables"].items()}


async def set_autocompaction(manager: ScyllaClusterManager, server, ks: str, cf: str, enabled: bool,
                             balancing: bool = True) -> None:
    """The dtests' disable_sstable_modifications() context: tablet load
    balancing and the table's autocompaction off, or back on. With
    balancing=False, the dtests' disable_autocompaction() context: the
    table's autocompaction only."""
    api = manager.api.client
    if enabled:
        await api.post(f"/storage_service/auto_compaction/{ks}", host=server.ip_addr, params={"cf": cf})
        if balancing:
            await api.post("/storage_service/tablets/balancing", host=server.ip_addr, params={"enabled": "true"})
    else:
        if balancing:
            await api.post("/storage_service/tablets/balancing", host=server.ip_addr, params={"enabled": "false"})
        assert await api.get_json(f"/column_family/autocompaction/{ks}:{cf}", host=server.ip_addr)
        await api.delete(f"/storage_service/auto_compaction/{ks}", host=server.ip_addr, params={"cf": cf})


async def memtable_tombstones_setup(cql, rf: int, rows_amount: int, deleted_keys: int) -> None:
    await create_ks(cql, "ks", rf)
    await cql.run_async("CREATE TABLE ks.test (i int PRIMARY KEY, t text)")
    insert = cql.prepare("INSERT INTO ks.test (i, t) values(?, 'skdjhdskjh')")
    await asyncio.gather(*[cql.run_async(insert, [k]) for k in range(rows_amount)])
    delete = cql.prepare("DELETE FROM ks.test where i = ?")
    await asyncio.gather(*[cql.run_async(delete, [k]) for k in range(deleted_keys)])


async def check_flushed_tombstones(manager: ScyllaClusterManager, server) -> None:
    """The deleted partitions are flushed as tombstones, without row data."""
    partitions = await dump_sstables(manager, server, "ks", "test")
    tombstones = [p for p in partitions if "tombstone" in p]
    assert tombstones, "Tombstones were not found"
    for p in tombstones:
        assert "clustering_elements" not in p, f"Unexpectedly found row data in deleted partition: {p}"


# Rows deleted while still in the memtable are flushed as tombstones only.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compact_tombstones_when_memtable_flush_one_node(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await memtable_tombstones_setup(cql, 1, 100, 10)
    await run_nodetool(manager, server, "flush")
    await check_flushed_tombstones(manager, server)
    assert list(await cql.run_async("select count(*) from ks.test"))[0].count == 90


# The same on three nodes (one per rack) with the second one down; it is
# repaired after it comes back.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compact_tombstones_when_memtable_flush_one_node_stopped(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    cql = manager.get_cql()
    await manager.server_stop_gracefully(servers[1].server_id)
    await memtable_tombstones_setup(cql, 3, 100, 10)
    for s in (servers[0], servers[2]):
        await run_nodetool(manager, s, "flush")
    for s in (servers[0], servers[2]):
        await check_flushed_tombstones(manager, s)
    await manager.server_start(servers[1].server_id)
    await run_nodetool(manager, servers[1], "repair")
    assert list(await cql.run_async("select count(*) from ks.test"))[0].count == 90


async def write_n_data_files(manager, server, cql, ks: str, num_of_files: int, num_of_keys: int, timestamp=None) -> None:
    for _ in range(num_of_files):
        await insert_c1c2(cql, n=num_of_keys, ks=ks, timestamp=timestamp, consistency=ConsistencyLevel.ONE)
        await run_nodetool(manager, server, "flush")


# TWCS (with timestamp_resolution MILLISECONDS) does not rewrite the
# sstables of past windows when newer windows are written. The windows are
# past minutes written with explicit timestamps, as in the dtest.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compact_data_by_time_window(manager: ScyllaClusterManager):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    ks = "ks_milliseconds"
    await create_ks(cql, ks, 1)
    await create_c1c2_cf(cql, ks, "cf", compaction={"compaction_window_size": 1, "compaction_window_unit": "MINUTES",
                                                     "timestamp_resolution": "MILLISECONDS", "class": "TimeWindowCompactionStrategy"})
    first_window_start_ms = (int(time.time()) // 60 - 10) * 60 * 1000
    async def write_window(minute):
        await write_n_data_files(manager, server, cql, ks, 7, 10, timestamp=first_window_start_ms + minute * 60 * 1000)
    for minute in range(4):
        await write_window(minute)
    await run_nodetool(manager, server, "compact")
    await wait_for_compactions(manager, server)
    files1 = await sstable_files(manager, server, ks, "cf")
    assert files1
    for minute in range(4, 6):
        await write_window(minute)
    files2 = await sstable_files(manager, server, ks, "cf")
    assert files1.issubset(files2), f"some of the original sstables are missing: expected {files1}, found {files2}"


async def sstables_per_time_window(manager, server, ks: str, cf: str) -> dict[int, list[str]]:
    windows = {}
    for sf, stats in (await sstable_stats(manager, server, await sstable_files(manager, server, ks, cf, "Statistics"))).items():
        for window in range(stats["min_timestamp"] // 1_000_000 // 60, stats["max_timestamp"] // 1_000_000 // 60 + 1):
            windows.setdefault(window, []).append(sf)
    return windows


# A TWCS major compaction keeps time windows apart: each of the three
# windows (of more than min_threshold sstables, read while the node is
# down) ends with at most one sstable per shard.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_major_compaction_with_several_timewindows(manager: ScyllaClusterManager):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_ks(cql, "ks", 1)
    await create_c1c2_cf(cql, "ks", "cf", compaction={"compaction_window_size": "1", "compaction_window_unit": "MINUTES",
                                                       "class": "TimeWindowCompactionStrategy",
                                                       "expired_sstable_check_frequency_seconds": "60", "min_threshold": 7})
    num_of_keys = 1000 * random.randint(1, 3)
    first_key = 0
    first_window_start = (int(time.time()) // 60 - 10) * 60
    for window in range(3):
        for flush in range(8):
            await insert_c1c2(cql, keys=list(range(first_key, first_key + num_of_keys)),
                              timestamp=(first_window_start + window * 60 + flush * 5) * 1_000_000)
            await run_nodetool(manager, server, "flush")
            first_key += num_of_keys // 2
    await manager.server_stop_gracefully(server.server_id)
    before = await sstables_per_time_window(manager, server, "ks", "cf")
    assert len(before) == 3, before
    await manager.server_start(server.server_id)
    await run_nodetool(manager, server, "compact")
    await wait_for_compactions(manager, server)
    after = await sstables_per_time_window(manager, server, "ks", "cf")
    assert len(after) == len(before)
    for window, sstables in after.items():
        assert 0 < len(sstables) <= SMP, f"time window {window}: {sstables}"


# Fully expired TWCS sstables of past windows stay until the next
# expiration check (every second), triggered by a new write, drops them
# all. The dtest's 23 s wait for the TTL (20 s) and gc_grace (1 s) is kept.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compaction_removes_ttld_data_by_time_windows(manager: ScyllaClusterManager):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    ttl, gc_grace = 20, 1
    await create_ks(cql, "ks", 1)
    await create_c1c2_cf(cql, "ks", "cf", gc_grace=gc_grace, tombstone_gc="immediate", default_ttl=ttl,
                         compaction={"compaction_window_size": "1", "compaction_window_unit": "MINUTES",
                                     "class": "TimeWindowCompactionStrategy", "expired_sstable_check_frequency_seconds": "1"})
    first_window_start = (int(time.time()) // 60 - 10) * 60
    for t in range(11):
        await insert_c1c2(cql, n=10, consistency=ConsistencyLevel.ONE, timestamp=(first_window_start + t * 15) * 1_000_000)
        await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "flush")
    await wait_for_compactions(manager, server)
    files1 = await sstable_files(manager, server, "ks", "cf")
    await asyncio.sleep(ttl + gc_grace + 2)
    assert await sstable_files(manager, server, "ks", "cf") == files1
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await insert_c1c2(cql, n=10, consistency=ConsistencyLevel.ONE)
    await run_nodetool(manager, server, "flush")
    await wait_for_compactions(manager, server)
    await log.wait_for(r"Compact ks.cf .* Compacted [0-9]+ sstables to \[\]", from_mark=mark, timeout=5)
    unpurged = files1 & await sstable_files(manager, server, "ks", "cf")
    assert not unpurged, f"original sstables were not purged: {unpurged}"


# A partition tombstone makes a TWCS major compaction merge the sstables
# of all time windows of the partition into one.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_data_from_different_windows_compacted_together(manager: ScyllaClusterManager):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_ks(cql, "ks", 1)
    await cql.run_async("CREATE TABLE ks.tb (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = "
                        "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'compaction_window_size': '1'} "
                        "AND tombstone_gc = {'mode': 'disabled'}")
    insert = cql.prepare("INSERT INTO ks.tb (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?")
    await asyncio.gather(*[cql.run_async(insert, args) for args in
                           [(1, 2, 1, 100000000001), (1, 3, 1, 200000000001), (1, 4, 1, 300000000001), (1, 5, 2, 500000000001)]])
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact", "ks", "tb")
    assert len(await sstable_files(manager, server, "ks", "tb")) == 4, "there should be 1 sstable for each time window"
    await cql.run_async("DELETE FROM ks.tb where pk=1")
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact", "ks", "tb")
    assert len(await sstable_files(manager, server, "ks", "tb")) == 1


def make_uuid_sstable_identifier() -> str:
    """A new UUID sstable generation, like "3fw2_0tj4_46w3k2cpidnirvjy7k"."""
    alphabet = string.digits + string.ascii_lowercase
    def encode(n):
        output = ""
        while n:
            n, index = divmod(n, len(alphabet))
            output += alphabet[index]
        return output[::-1]
    timeuuid = uuid.uuid1()
    seconds, decimicro = divmod(timeuuid.time, 10_000_000)
    delta = datetime.timedelta(seconds=seconds)
    lsb = int.from_bytes(timeuuid.bytes[8:])
    return f"{encode(delta.days):0>4}_{encode(delta.seconds):0>4}_{encode(decimicro):0>5}{encode(lsb):0>13}"


def copy_sstable_file(path: str, generation: str, strategy: str) -> None:
    """Copy an sstable component under a new generation. For ICS, the
    Scylla component is left out, to force a new run identifier."""
    if strategy == "IncrementalCompactionStrategy" and "Scylla.db" in path:
        return
    parts = os.path.basename(path).split("-")
    parts[1] = generation
    dest = os.path.join(os.path.dirname(path), "-".join(parts))
    if strategy == "IncrementalCompactionStrategy" and "TOC.txt" in path:
        with open(path) as r, open(dest, "w") as w:
            w.writelines(line for line in r if "Scylla.db" not in line)
    else:
        shutil.copy(path, dest)


# Sstables found at boot (each sstable copied under 4 new generations, with
# the commitlog removed) are compacted (resharded or reshaped) on boot.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", STRATEGIES)
async def test_compaction_is_started_on_boot(manager: ScyllaClusterManager, strategy: str):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_ks(cql, "ks", 1)
    await cql.run_async(f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{strategy}'}};")
    for x in range(100):
        await cql.run_async(f"insert into ks.cf (key, val) values ({x},1)")
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    await manager.server_stop_gracefully(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)
    for f in glob.glob(os.path.join(workdir, "commitlog", "*")):
        if os.path.isdir(f):
            shutil.rmtree(f)
        else:
            os.remove(f)
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    files = [f for f in glob.glob(f"{table_dir}/*") if os.path.isfile(f)]
    generations = {os.path.basename(f).split("-")[1]: [make_uuid_sstable_identifier() for _ in range(4)] for f in files}
    for f in files:
        for gen in generations[os.path.basename(f).split("-")[1]]:
            copy_sstable_file(f, gen, strategy)
    before = await sstable_files(manager, server, "ks", "cf")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_start(server.server_id)
    await log.wait_for(r"compaction -.*(Compacted|Resharded|Reshaped) [0-9]+ sstables to \[.+/data/ks/cf-.+\]", from_mark=mark, timeout=120)
    assert await sstable_files(manager, server, "ks", "cf") != before, "no compaction on boot"


# A major compaction after TTL and gc_grace passed (tombstone_gc mode
# "immediate") purges the expired rows, leaving only the one newly written.
# The dtest's 6 s wait for the 3 s TTL is kept.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", STRATEGIES)
async def test_compaction_removes_ttld_data_after_gc_period(manager: ScyllaClusterManager, strategy: str):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_ks(cql, "ks", 1)
    await cql.run_async(f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{strategy}'}} "
                        "and tombstone_gc = {'mode': 'immediate'};")
    for x in range(100):
        await cql.run_async(f"insert into ks.cf (key, val) values ({x},1) USING TTL 3")
    await asyncio.sleep(6)
    await cql.run_async("insert into ks.cf (key, val) values (99,1);")
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    partitions = await dump_sstables(manager, server, "ks", "cf")
    assert len(partitions) == 1, partitions


async def create_twcs_table(cql, rf: int = 1, window_unit: str = "MINUTES", window_size: int = 1, ttl: int = 1800,
                            gc_period: int = 1800) -> None:
    """The TWCS dtests' _create_ks_cl_with_twcs()."""
    await create_ks(cql, "ks", rf)
    await cql.run_async(f"CREATE TABLE ks.test (pk int, ck int, v blob, PRIMARY KEY(pk, ck)) WITH default_time_to_live = {ttl} AND "
                        f"gc_grace_seconds = {gc_period} AND compaction = {{'class': 'TimeWindowCompactionStrategy', "
                        f"'compaction_window_unit': '{window_unit}', 'compaction_window_size': {window_size} }} AND "
                        "tombstone_gc = {'mode': 'timeout'}")


async def pks_on_every_shard(manager: ScyllaClusterManager, server, num_pks: int, smp: int = SMP) -> list[int]:
    """The dtests' generate_int32_pks_for_vnodes(): a key owned by each shard
    of the node (found with "scylla types shardof"), and random others."""
    exe = await manager.server_get_exe(server.server_id)
    async def types(*args):
        proc = await asyncio.create_subprocess_exec(exe, "types", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await proc.communicate()
        assert proc.returncode == 0, err.decode()
        return out.decode().strip()
    pks = set()
    for shard in range(smp):
        for candidate in itertools.count(0):
            serialized = await types("serialize", "--full-compound", "-t", "Int32Type", "--", str(candidate))
            out = await types("shardof", "--shards", str(smp), "--full-compound", "-t", "Int32Type", "--", serialized)
            if int(out.rsplit(":", 1)[-1]) == shard:
                pks.add(candidate)
                break
    while len(pks) < num_pks:
        pks.add(random.randint(-2147483647, 2147483647))
    return list(pks)


async def simulate_writes_in_minutes(manager, servers, cql, pks: list[int], duration_minutes: int, start_from_minute: int = 0,
                                     flush_period_seconds: int = 30, size: int = 1) -> None:
    """The dtests' simulate_write_process_in_minutes(): a write to each pk
    for every second of the simulated minutes, USING TIMESTAMP of that
    second, flushing every flush_period_seconds."""
    insert = cql.prepare("INSERT INTO ks.test (pk, ck, v) VALUES (?, ?, ?)USING TIMESTAMP ?")
    v = b"a" * size
    for t in range(start_from_minute * 60, duration_minutes * 60):
        await asyncio.gather(*[cql.run_async(insert, (pk, t, v, t * 1_000_000)) for pk in pks])
        if t % flush_period_seconds == 0:
            for s in servers:
                await run_nodetool(manager, s, "flush")


# With TWCS, a memtable holding 10 minutes of writes is flushed into one
# sstable per window, on each of three nodes (RF=3, one per rack).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_memtable_flush(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    cql = manager.get_cql()
    await create_twcs_table(cql, rf=3)
    pks = await pks_on_every_shard(manager, servers[0], 100)
    await simulate_writes_in_minutes(manager, servers, cql, pks, duration_minutes=10, flush_period_seconds=999999999999)
    for s in servers:
        pre = await sstable_files(manager, s, "ks", "test", "Statistics")
        await run_nodetool(manager, s, "flush")
        await set_autocompaction(manager, s, "ks", "test", False)
        post = await sstable_files(manager, s, "ks", "test", "Statistics")
        assert len(post) - len(pre) > 1, f"expected more than one sstable after flush, lengths are {len(post)} {len(pre)}"
        for sf, stats in (await sstable_stats(manager, s, post)).items():
            lo, hi = stats["min_timestamp"] // 1_000_000, stats["max_timestamp"] // 1_000_000
            assert lo - lo % 60 == hi - hi % 60, f"sstable {sf} spans more than one window: {lo}..{hi}"
        await set_autocompaction(manager, s, "ks", "test", True)


async def check_sstable_time_windows(manager, server, window_size: int, window_unit: str, balancing: bool = True) -> None:
    """Each sstable spans no more than 1.5 windows (the TWCS dtests'
    _check_sstable_timestamps())."""
    await set_autocompaction(manager, server, "ks", "test", False, balancing)
    await run_nodetool(manager, server, "flush", "system_schema")
    files = await sstable_files(manager, server, "ks", "test", "Statistics")
    assert files, "No statistics files"
    margin = 1.5 * window_size * (60 if window_unit == "MINUTES" else 3600)
    for sf, stats in (await sstable_stats(manager, server, files)).items():
        tw = (stats["max_timestamp"] - stats["min_timestamp"]) // 1_000_000
        assert tw <= margin, f"time window of {tw} s is greater than {margin} s: sstable={sf}"
    await set_autocompaction(manager, server, "ks", "test", True, balancing)


# After a TWCS window-size change, the reshape on boot compacts buckets of
# similar-sized small sstables first, and the last bucket takes the rest;
# a major compaction then leaves one sstable, within the new window.
async def reshape_sstables_after_change_window_size(manager: ScyllaClusterManager, new_window_size, new_window_unit, distribution):
    """The TWCS dtests' run_flow_generate_and_reshape_twcs_sstables()."""
    small_file_size = 5_000
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_twcs_table(cql)
    prev_min = 0
    for duration, size in distribution:
        await simulate_writes_in_minutes(manager, [server], cql, [1], duration_minutes=duration, start_from_minute=prev_min, size=size)
        prev_min = duration
    await run_nodetool(manager, server, "compact", "ks", "test")
    await check_sstable_time_windows(manager, server, 1, "MINUTES")
    small, large = set(), set()
    for f in await sstable_files(manager, server, "ks", "test"):
        (small if os.path.getsize(f) <= small_file_size else large).add(os.path.basename(f))
    await run_nodetool(manager, server, "disableautocompaction", "ks", "test")
    await cql.run_async("ALTER TABLE ks.test WITH compaction = {'class': 'TimeWindowCompactionStrategy',"
                        f"'compaction_window_unit': '{new_window_unit}','compaction_window_size': {new_window_size} }}")
    await manager.server_stop_gracefully(server.server_id)
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_start(server.server_id)
    found = await log.grep(r"compaction - \[Reshape ks\.test .*\] Reshaping \[(.*)\]", from_mark=mark)
    assert found, "Reshape was not run"
    def reshaped(match):
        return [os.path.basename(s.split(":")[0].strip()) for s in match.group(1).strip().split(",") if "origin=reshape" not in s]
    for _, match in found[:-1]:
        for sstable in reshaped(match):
            assert sstable in small, f"sstable {sstable} (large: {sstable in large}) in a bucket of small sstables"
            small.remove(sstable)
    for sstable in reshaped(found[-1][1]):
        assert sstable in small or sstable in large, f"sstable {sstable} is neither small nor large"
    await run_nodetool(manager, server, "compact", "ks", "test")
    await check_sstable_time_windows(manager, server, new_window_size, new_window_unit)
    assert len(await sstable_files(manager, server, "ks", "test")) == 1


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_reshape_sstables_different_size_after_change_window_size.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reshape_sstables_different_size_after_change_window_size(manager: ScyllaClusterManager):
    await reshape_sstables_after_change_window_size(manager, 64, "MINUTES", [(16, 10), (32, 1_000_000), (48, 10), (64, 1_000_000)])


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_reshape_sstables_after_change_window_size_when_small_files_more_than_large.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reshape_sstables_after_change_window_size_when_small_files_more_than_large(manager: ScyllaClusterManager):
    await reshape_sstables_after_change_window_size(manager, 100, "MINUTES", [(32, 10), (40, 10), (72, 10), (80, 1_000_000), (96, 10)])


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_reshape_sstables_after_change_window_size_and_unit.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reshape_sstables_after_change_window_size_and_unit(manager: ScyllaClusterManager):
    await reshape_sstables_after_change_window_size(manager, 2, "HOURS", [(32, 10), (40, 1_000_000), (72, 10), (80, 1_000_000), (96, 1)])


VALIDATE_PK19 = r"\x19\x00\x00\x00 \(\{key:\s*pk\{000419000000\},\s*token:\s*-5674409923619649499\}\)"
VALIDATE_PK06 = r"\x06\x00\x00\x00 \(\{key:\s*pk\{000406000000\},\s*token:\s*-5566252076597558760\}\)"
VALIDATE_CK3 = r"\{position:\s*clustered,\s*ckp\{000400000003\},\s*0\}"
VALIDATE_CK5 = r"\{position:\s*clustered,\s*ckp\{000400000005\},\s*0\}"
VALIDATE_START = r"compaction - Scrubbing in validate mode"
VALIDATE_INVALID_PARTITION = (rf"Invalid partition {VALIDATE_PK19},?\s*partition is out-of-order compared to previous partition {VALIDATE_PK06}"
                              rf"|out-of-order partition key {VALIDATE_PK19},?\s*previous partition key was {VALIDATE_PK06}")
VALIDATE_INVALID_ROW = (rf"Invalid clustering row fragment with key 3 \({VALIDATE_CK3}\) in partition {VALIDATE_PK06},?\s*"
                        rf"fragment is out-of-order compared to previous clustered fragment with key 5 \({VALIDATE_CK5}\)"
                        rf"|out-of-order clustering row at position {VALIDATE_CK3} in partition {VALIDATE_PK06},?\s*"
                        rf"previous clustering element was {VALIDATE_CK5} at position clustering row")
VALIDATE_FINISH_INVALID = r"Finished scrubbing in validate mode.*(sstable(\(s\)|s) are|sstable is) invalid"
VALIDATE_FINISH_VALID = r"Finished scrubbing in validate mode.*(sstable(\(s\)|s) are|sstable is) valid"


async def check_log_patterns(manager: ScyllaClusterManager, server, patterns: list[str]) -> None:
    log = await manager.server_open_log(server.server_id)
    missing = [p for p in patterns if not await log.grep(p)]
    assert not missing, f"patterns not found in the log: {missing}"


def files_in(path: str) -> list[str]:
    return sorted(f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))) if os.path.isdir(path) else []


# A validate-mode scrub of an sstable with out-of-order partitions and rows
# reports them and quarantines the sstable unchanged.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_validation_compaction_detects_sstable_corruption(manager: ScyllaClusterManager):
    manager.ignore_log_patterns += ["([Ii]nvalid|out-of-order) clustering row", "([Ii]nvalid|out-of-order) partition", "(Sscrub) compaction ks.cf.*"]
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_ks(cql, "ks", 1)
    await cql.run_async("CREATE COLUMNFAMILY ks.cf (pk text, ck int, s int, v int, PRIMARY KEY(pk, ck)) WITH comment='test cf' "
                        "AND compaction = {'class': 'NullCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    await cql.run_async("SELECT * FROM ks.cf LIMIT 1")
    await run_nodetool(manager, server, "flush")
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    await manager.server_stop_gracefully(server.server_id)
    src = os.path.join(RESOURCES, "sstable_with_invalid_fragment", "ks", "cf-test")
    for f in os.listdir(src):
        shutil.copy(os.path.join(src, f), table_dir)
    await manager.server_start(server.server_id)
    pre_scrub = files_in(table_dir)
    await manager.api.keyspace_scrub_sstables(server.ip_addr, "ks", "VALIDATE", "cf")
    assert files_in(os.path.join(table_dir, "quarantine")) == pre_scrub
    await check_log_patterns(manager, server, [VALIDATE_START, VALIDATE_INVALID_PARTITION, VALIDATE_INVALID_ROW, VALIDATE_FINISH_INVALID])


# A validate-mode scrub of a valid sstable reports it valid and leaves it.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_validation_compaction_with_valid_sstable(manager: ScyllaClusterManager):
    server = await manager.server_add(config=ADD_CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await create_ks(cql, "ks", 1)
    await create_c1c2_cf(cql, "ks", "cf")
    await insert_c1c2(cql, n=10_000)
    await run_nodetool(manager, server, "flush")
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    pre_scrub = files_in(table_dir)
    await manager.api.keyspace_scrub_sstables(server.ip_addr, "ks", "VALIDATE", "cf")
    assert files_in(table_dir) == pre_scrub
    await check_log_patterns(manager, server, [VALIDATE_START, VALIDATE_FINISH_VALID])


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_doesnt_block_user_initiated_upgrade_compaction.
# Disabling autocompaction of a table ("nodetool disableautocompaction", not
# persistent) leaves the other tables' on, and does not stop an upgrade
# compaction the user requests: it runs, and autocompaction stays disabled.
@pytest.mark.parametrize("strategy", [
    pytest.param("LeveledCompactionStrategy", marks=[pytest.mark.tier3, pytest.mark.skip_mode(
        mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")]),
    *(pytest.param(s, marks=pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only"))
      for s in ["SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"]),
])
async def test_disable_autocompaction_doesnt_block_user_initiated_upgrade_compaction(manager: ScyllaClusterManager, strategy):
    server = await manager.server_add(config={**CONFIG, 'repair_hints_batchlog_flush_cache_time_in_ms': 0}, cmdline=CMDLINE)
    cql = manager.get_cql()

    async def autocompaction_enabled(ks, table):
        return await manager.api.client.get_json(f"/column_family/autocompaction/{ks}:{table}", host=server.ip_addr)

    for ks, table in [("ks", "to_disable"), ("ks2", "std1")]:
        cql.execute(f"CREATE KEYSPACE {ks} {KS_OPTS}")
        # The dtests' create_c1c2_table(), which reads the new table once.
        await create_c1c2_cf(dtest_session(cql), ks, table, compaction=f"{{'class':'{strategy}'}}")
    # Like the dtest, with tablet load balancing disabled (a request the node takes on vnodes too).
    await manager.api.disable_tablet_balancing(server.ip_addr)
    insert = cql.prepare("INSERT INTO ks.to_disable (key, c1, c2) VALUES (?, 'value1', 'value2')")
    insert.consistency_level = ConsistencyLevel.ANY
    for chunk in range(10):
        for k in range(chunk * 100, (chunk + 1) * 100):
            cql.execute(insert, [f"k{k}"])
        await manager.api.flush_all_keyspaces(server.ip_addr)
    await run_nodetool(manager, server, "disableautocompaction", "ks", "to_disable")
    assert not await autocompaction_enabled("ks", "to_disable")
    assert await autocompaction_enabled("ks2", "std1")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.api.client.get("/storage_service/keyspace_upgrade_sstables/ks", host=server.ip_addr, params={"cf": "to_disable"})
    await log.wait_for("Upgrade ks.to_disable", from_mark=mark, timeout=60)
    assert not await autocompaction_enabled("ks", "to_disable"), "Expected autocompaction to still be disabled"
    await manager.api.enable_tablet_balancing(server.ip_addr)


############################################################################
# More ports from compaction_additional_test.py, one test per dtest.

# The dtests' prepare(n): ccm populate(n) - SimpleSnitch, every node in
# datacenter1/rack1 - with sstable key validation and one compaction group
# per shard.
DC1 = {"dc": "datacenter1", "rack": "rack1"}
PREPARE_CONFIG = {**ADD_CONFIG, "endpoint_snitch": "SimpleSnitch"}
# Clusters the dtests populate without prepare() have only the module's
# cluster options (compaction debug logging) on top of the defaults.
POPULATE_CONFIG = {**CONFIG, "endpoint_snitch": "SimpleSnitch"}


async def prepare(manager: ScyllaClusterManager, nodes: int, cmdline: list[str] | None = None, config: dict | None = None):
    """The dtests' CompactionAdditionalTester.prepare(): the nodes, and a
    session whose statements default to CL=ONE."""
    servers = await manager.servers_add(nodes, config={**PREPARE_CONFIG, **(config or {})}, cmdline=CMDLINE + (cmdline or []),
                                        property_file=DC1)
    return servers, dtest_session(manager.get_cql())


async def populate_racks(manager: ScyllaClusterManager, racks: int):
    """ccm's populate(generate_cluster_topology(rack_num=racks)).start()."""
    servers = await manager.servers_add(racks, config=CONFIG, cmdline=CMDLINE,
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(racks)])
    return servers, dtest_session(manager.get_cql())


async def wait_for_lines(manager: ScyllaClusterManager, server, pattern: str, count: int, from_mark: int | None = None,
                         timeout: float = 300) -> list:
    """Like ccm's watch_log_for() of `count` copies of one expression: waits
    for `count` log lines matching it, and returns their matches."""
    log = await manager.server_open_log(server.server_id)
    deadline = time.time() + timeout
    while True:
        found = await log.grep(pattern, from_mark=from_mark)
        if len(found) >= count:
            return found[:count]
        if time.time() > deadline:
            raise TimeoutError(f"{count} lines matching {pattern!r} not found, only {len(found)}")
        await asyncio.sleep(0.5)


async def offstrategy_compaction(manager: ScyllaClusterManager, server, ks: str = "ks", cf: str = "test") -> None:
    """The dtests' run_rest_api(node, /storage_service/keyspace_offstrategy_compaction/<ks>?cf=<cf>)."""
    await manager.api.client.post(f"/storage_service/keyspace_offstrategy_compaction/{ks}", host=server.ip_addr, params={"cf": cf})


async def simulate(manager: ScyllaClusterManager, servers, cql, pks, duration_minutes: int = 20, start_from_minute: int = 0,
                   flush_period_seconds: int = 30, size: int = 1) -> int:
    """simulate_writes_in_minutes(), returning the number of rows written."""
    await simulate_writes_in_minutes(manager, servers, cql, pks, duration_minutes, start_from_minute, flush_period_seconds, size)
    return (duration_minutes - start_from_minute) * 60 * len(pks)


async def statistics_files(manager: ScyllaClusterManager, server, ks: str = "ks", cf: str = "test") -> set[str]:
    return await sstable_files(manager, server, ks, cf, "Statistics")


async def sstable_count_is_close_to_time_windows(manager: ScyllaClusterManager, server, time_windows: int, shards: int = 1) -> None:
    """The dtests' _sstable_count_is_close_to_time_windows_multiplied_by_shards_count()."""
    n = len(await statistics_files(manager, server))
    assert time_windows * shards + 2 * shards >= n >= time_windows * shards, \
        f"Wrong sstables count on {server.ip_addr}: {n}. There should be at least one sstable per time window."


async def dump_data_files(manager: ScyllaClusterManager, server, files: list[str], running: bool = True,
                          ks: str = "ks", cf: str = "test") -> list[dict]:
    """The dtests' dump_sstables(node, ks, cf, datafiles): "scylla sstable
    dump-data --merge" of the given sstables, with the autocompaction of
    system_schema and of the table disabled meanwhile if the node runs."""
    if running:
        await manager.api.disable_autocompaction(server.ip_addr, "system_schema")
        await manager.api.disable_autocompaction(server.ip_addr, ks, cf)
    try:
        exe = await manager.server_get_exe(server.server_id)
        proc = await asyncio.create_subprocess_exec(exe, "sstable", "dump-data", "--merge", *files, cwd=os.path.dirname(files[0]),
                                                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await proc.communicate()
        assert proc.returncode == 0, err.decode()
        return json.loads(out)["sstables"]["anonymous"]
    finally:
        if running:
            await manager.api.enable_autocompaction(server.ip_addr, ks, cf)
            await manager.api.enable_autocompaction(server.ip_addr, "system_schema")


# Ported from scylla-dtest compaction_additional_test.py::TestCompactionAdditional::test_compaction_delete_with_smp_change.
# Rows written with one shard, compacted into sstables the node shares
# between its shards after a restart with two, and deleted then, are not
# resurrected: after gc_grace_seconds compactions purge some of the
# tombstones, then all of them.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compaction_delete_with_smp_change(manager: ScyllaClusterManager):
    [node1], cql = await prepare(manager, 1, cmdline=["--smp", "1"])
    await create_ks(cql, "ks", 1)
    gc_grace_seconds = 5
    keys = 100
    cql.execute("create table ks.cf (key int PRIMARY KEY, val int) with compaction = {'class':'SizeTieredCompactionStrategy'} "
                f"and gc_grace_seconds = {gc_grace_seconds} and tombstone_gc = {{'mode': 'immediate'}};")
    for x in range(keys):
        cql.execute(f"insert into ks.cf (key, val) values ({x},1)")
    await run_nodetool(manager, node1, "flush")
    await run_nodetool(manager, node1, "compact")
    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_update_cmdline(node1.server_id, ["--smp", "2"])
    await manager.server_start(node1.server_id)
    cql = dtest_session(await reconnect_driver(manager))

    def compactions_count():
        return cql.execute("select count(*) from system.compaction_history where keyspace_name='ks' and "
                           "columnfamily_name='cf' allow filtering").one()[0]

    compactions_1 = compactions_count()
    compactions_2 = compactions_1
    for x in range(keys):
        cql.execute(f"delete from ks.cf where key = {x}")
    await run_nodetool(manager, node1, "flush")
    await asyncio.sleep(gc_grace_seconds + 1)
    # Past gc_grace_seconds, writes and flushes until a compaction runs on a
    # shard, removing data and tombstones.
    while compactions_1 == compactions_2:
        cql.execute(f"insert into ks.cf (key, val) values ({keys + 1},1);")
        await run_nodetool(manager, node1, "flush")
        compactions_2 = compactions_count()
    await wait_for_compactions(manager, node1)
    num_compactions = compactions_count() - compactions_1

    await manager.server_stop(node1.server_id, convict=False)
    # Only some tombstones are left: the files were resharded, and
    # gc_grace_seconds passed before the compaction.
    files = sorted(await sstable_files(manager, node1, "ks", "cf"))
    partitions = await dump_data_files(manager, node1, files, running=False, ks="ks", cf="cf")
    numfound = sum("tombstone" in p for p in partitions)
    assert numfound <= keys, f"Number of found tombstones {numfound} greater than number of keys {keys}"
    if num_compactions < 2:
        assert numfound > 0, f"Number of found tombstones {numfound} != 0"

    await manager.server_start(node1.server_id)
    cql = dtest_session(await reconnect_driver(manager))
    for x in range(keys):
        assert list(cql.execute(f"select * from ks.cf where key = {x}")) == []
    await wait_for_compactions(manager, node1)
    compactions_1 = compactions_count()
    compactions_2 = compactions_1
    while compactions_1 + 2 > compactions_2:
        await asyncio.sleep(gc_grace_seconds)
        await run_nodetool(manager, node1, "flush")
        await run_nodetool(manager, node1, "compact", "ks")
        compactions_2 = compactions_count()
    await wait_for_compactions(manager, node1)
    partitions = await dump_sstables(manager, node1, "ks", "cf")
    assert sum("tombstone" in p for p in partitions) == 0, "Not all tombstones were removed during compactions"


STRATEGIES_UPGRADE = [
    {"class": "LeveledCompactionStrategy", "sstable_size_in_mb": 1, "min_threshold": 2},
    {"class": "TimeWindowCompactionStrategy", "compaction_window_size": 1, "compaction_window_unit": "MINUTES", "min_threshold": 2},
    {"class": "SizeTieredCompactionStrategy", "bucket_high": 1.5, "bucket_low": 0.5, "min_sstable_size": 1, "min_threshold": 2},
]
STRATEGY_PAIRS = pytest.mark.parametrize("strategy1,strategy2", list(itertools.product(STRATEGIES_UPGRADE, STRATEGIES_UPGRADE)),
                                         ids=lambda s: s["class"])


async def fill_data_by_cs(manager: ScyllaClusterManager, server, n_range=None, start: int = 0, duration_range=None,
                          other_opt=None) -> None:
    """The dtests' fill_data_by_cs(): cassandra-stress write workloads,
    each followed by a flush."""
    other_opt = other_opt if other_opt is not None else ["-rate", "threads=10", "-col", "size=FIXED(1024)"]
    n_range = n_range if n_range is not None else [500, 550, 600, 650]
    opts = []
    for num in n_range:
        opts.append([f"n={num}", "-pop", f"seq={start}..{start + num}"])
        start += num
    for t in duration_range or []:
        opts.append([f"duration={t}s"])
    for opt in opts:
        await cassandra_stress(manager, server, "write", "no-warmup", *opt, *other_opt, create_keyspace=False)
        await run_nodetool(manager, server, "flush")


async def prepare_sstables_of_two_strategies(manager: ScyllaClusterManager, server, cql, strategy1, strategy2) -> tuple[str, str]:
    """The common part of the refresh-after-strategy-change dtests: sstables
    written under strategy1 (some compacted) and for strategy2 (with
    autocompaction off), copied to a backup and to the upload directory, the
    table altered to strategy2 and truncated. Returns the table and backup dirs."""
    cql.execute("DROP KEYSPACE IF EXISTS keyspace1")
    await cassandra_stress(manager, server, "write", "n=0", "no-warmup", "-schema", "replication(factor=1)", "-rate", "threads=1",
                           create_keyspace=False)
    cql.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy1}")
    await fill_data_by_cs(manager, server, n_range=[500, 550, 600, 650])
    await run_nodetool(manager, server, "compact")
    await run_nodetool(manager, server, "disableautocompaction", "keyspace1", "standard1")
    if strategy2["class"] == "TimeWindowCompactionStrategy":
        # An sstable spanning two 1 minute windows.
        await fill_data_by_cs(manager, server, n_range=[], duration_range=[70], other_opt=["-rate", "threads=1", "-col", "size=FIXED(1024)"])
    elif strategy2["class"] == "LeveledCompactionStrategy":
        await fill_data_by_cs(manager, server, n_range=[500, 550, 600, 650, 1000], start=5000)
    else:
        await fill_data_by_cs(manager, server, n_range=[500, 550, 600, 650, 2000, 5000] * 2, start=10000)

    cf_dir = await get_table_dir(manager, server, "keyspace1", "standard1")
    backup_dir = os.path.join(cf_dir, f"backup.{time.time()}")
    os.makedirs(backup_dir)
    for f in files_in(cf_dir):
        shutil.copy2(os.path.join(cf_dir, f), backup_dir)
        shutil.copy2(os.path.join(cf_dir, f), os.path.join(cf_dir, "upload"))
    cql.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy2}")
    cql.execute("TRUNCATE keyspace1.standard1")
    await run_nodetool(manager, server, "enableautocompaction", "keyspace1", "standard1")
    return cf_dir, backup_dir


async def verify_data_by_cs(manager: ScyllaClusterManager, server) -> None:
    await cassandra_stress(manager, server, "read", "n=100", "no-warmup", "-rate", "threads=10", "-col", "size=FIXED(1024)",
                           create_keyspace=False)


# Ported from scylla-dtest compaction_additional_test.py::TestCompactionAdditional::test_refresh_and_restart_after_compaction_strategy_change.
# Sstables written under one compaction strategy are loaded under another,
# by refresh from the upload directory and at restart from the staging
# directory; they are reshaped if they do not fit the new strategy, and all
# the data is read back.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@STRATEGY_PAIRS
async def test_refresh_and_restart_after_compaction_strategy_change(manager: ScyllaClusterManager, strategy1, strategy2):
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    node1 = await manager.server_add(config={**POPULATE_CONFIG, "auto_snapshot": True}, cmdline=CMDLINE, property_file=DC1)
    cql = dtest_session(manager.get_cql())
    cf_dir, backup_dir = await prepare_sstables_of_two_strategies(manager, node1, cql, strategy1, strategy2)
    log = await manager.server_open_log(node1.server_id)

    async def assert_reshape(srcdir, mark, verify_reshape=True, followed_by=None):
        exprs = [r"(Reshape|Reshard) keyspace1.standard1"] + ([followed_by] if followed_by else [])
        try:
            await log.wait_for(*exprs, from_mark=mark, timeout=30)
        except TimeoutError:
            if strategy2["class"] != "SizeTieredCompactionStrategy" and verify_reshape and strategy2["class"] != strategy1["class"]:
                pytest.fail(f"Reshape didn't occur in loading sstables from {srcdir} directory")

    mark = await log.mark()
    await run_nodetool(manager, node1, "refresh", "--", "keyspace1", "standard1")
    await assert_reshape("upload/", mark, followed_by=r"Done loading new SSTables for keyspace=keyspace1.*table=standard1")
    await verify_data_by_cs(manager, node1)

    cql.execute("TRUNCATE keyspace1.standard1")
    mark = await log.mark()
    await manager.server_stop_gracefully(node1.server_id)
    for f in files_in(backup_dir):
        shutil.copy2(os.path.join(backup_dir, f), os.path.join(cf_dir, "staging"))
    await manager.server_start(node1.server_id)
    verify_reshape = not (strategy2["class"] == "LeveledCompactionStrategy" and strategy1["class"] != "LeveledCompactionStrategy")
    await assert_reshape("staging/", mark, verify_reshape=verify_reshape, followed_by=r"Reshaped .* seconds")
    await verify_data_by_cs(manager, node1)


# Ported from scylla-dtest compaction_additional_test.py::TestCompactionAdditional::test_reshard_after_compaction_strategy_and_smp_change.
# Sstables written by a node with one shard, under another compaction
# strategy, are resharded when loaded by refresh after a restart with three
# shards, and at a restart with two shards from the staging directory.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@STRATEGY_PAIRS
async def test_reshard_after_compaction_strategy_and_smp_change(manager: ScyllaClusterManager, strategy1, strategy2):
    if (os.cpu_count() or 1) < 3:
        pytest.skip("This test requires a minimum of 3 cpus")
    logging_args = ["--logger-log-level", "sstables_loader=debug", "--logger-log-level", "sstable_directory=trace",
                    "--logger-log-level", "compaction=debug"]
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    node1 = await manager.server_add(config={**POPULATE_CONFIG, "auto_snapshot": True}, cmdline=["--smp", "1", *logging_args], property_file=DC1)
    cql = dtest_session(manager.get_cql())
    cf_dir, backup_dir = await prepare_sstables_of_two_strategies(manager, node1, cql, strategy1, strategy2)
    log = await manager.server_open_log(node1.server_id)

    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_update_cmdline(node1.server_id, ["--smp", "3"])
    await manager.server_start(node1.server_id)
    cql = dtest_session(await reconnect_driver(manager))
    mark = await log.mark()
    await run_nodetool(manager, node1, "refresh", "--", "keyspace1", "standard1")
    await log.wait_for(r"Reshard keyspace1.standard1", r"Done loading new SSTables for keyspace=keyspace1.*table=standard1",
                       from_mark=mark, timeout=30)
    await verify_data_by_cs(manager, node1)

    cql.execute("TRUNCATE keyspace1.standard1")
    mark = await log.mark()
    await manager.server_stop_gracefully(node1.server_id)
    for f in files_in(backup_dir):
        shutil.copy2(os.path.join(backup_dir, f), os.path.join(cf_dir, "staging"))
    await manager.server_update_cmdline(node1.server_id, ["--smp", "2"])
    await manager.server_start(node1.server_id)
    await log.wait_for(r"Reshard keyspace1.standard1", r"Resharded .* keyspace1.standard1", from_mark=mark, timeout=30)
    await verify_data_by_cs(manager, node1)


# Ported from scylla-dtest compaction_additional_test.py::TestCompactionAdditional::test_major_compaction_processes_tables_in_order_by_size.
# A major compaction of all keyspaces compacts the tables from the smallest
# to the largest.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("cf_sizes", [(100, 10_000, 100_000), (10_000, 100, 100_000), (10_000, 100_000, 100), (100, 100_000, 10_000),
                                      (100_000, 10_000, 100), (100_000, 100, 10_00)],
                         ids=["100-10_000-100_000", "10_000-100-100_000", "10_000-100_000-100", "100-100_000-10_000",
                              "100_000-10_000-100", "100_000-100-10_00"])
async def test_major_compaction_processes_tables_in_order_by_size(manager: ScyllaClusterManager, cf_sizes):
    [node1], cql = await prepare(manager, 1, cmdline=["--smp", "1"])
    cf_names = ["cf_1", "cf_2", "cf_3"]
    await create_ks(cql, "ks", 1)
    sizes = {}
    for size in cf_sizes:
        cf_name = cf_names[cf_sizes.index(size)]
        await create_c1c2_cf(cql, "ks", cf_name)
        await insert_c1c2(cql, n=size, cf=cf_name, consistency=ConsistencyLevel.ONE)
        sizes[cf_name] = size
    await run_nodetool(manager, node1, "flush")
    t = datetime.datetime.fromtimestamp(time.time(), tz=datetime.UTC)
    await run_nodetool(manager, node1, "compact")
    await wait_for_compactions(manager, node1)

    rows = [r for r in cql.execute("SELECT columnfamily_name, compacted_at, keyspace_name FROM system.compaction_history")
            if r.keyspace_name == "ks" and r.compacted_at.replace(tzinfo=datetime.UTC) >= t]
    compaction_time = {}
    for r in rows:
        if r.columnfamily_name not in compaction_time or r.compacted_at > compaction_time[r.columnfamily_name]:
            compaction_time[r.columnfamily_name] = r.compacted_at
    by_time = sorted(sizes, key=lambda cf: (compaction_time.get(cf), sizes[cf]))
    by_size = sorted(sizes, key=lambda cf: sizes[cf])
    assert by_time == by_size, "The list of rows sorted by size is not identical to the list of rows sorted by compaction time"


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_streaming_during_adding_node_with_boostrap.
# The data a node receives when it bootstraps into a TWCS table is
# segregated by window, by one off-strategy compaction (reshape) per shard.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_streaming_during_adding_node_with_boostrap(manager: ScyllaClusterManager):
    time_windows = 20
    [node1], cql = await prepare(manager, 1)
    await create_twcs_table(cql, rf=1)
    pks = await pks_on_every_shard(manager, node1, 100)
    await simulate(manager, [node1], cql, pks, duration_minutes=time_windows)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")

    node2 = await manager.server_add(config=PREPARE_CONFIG, cmdline=CMDLINE, property_file=DC1)
    shard_count = SMP
    log2 = await manager.server_open_log(node2.server_id)
    await log2.wait_for(*[rf"\[shard {i}(?::\w+)?\].*Done with off-strategy compaction for ks.test" for i in range(shard_count)],
                        timeout=60)
    await check_sstable_time_windows(manager, node2, 1, "MINUTES")
    await sstable_count_is_close_to_time_windows(manager, node2, time_windows, shard_count)
    reshapes = await log2.grep(r"compaction - \[Reshape ks.test .*\] Reshaped")
    assert len(reshapes) == shard_count, "There should be only one table reshape per shard"


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_streaming_decommission.
# The data a node receives from a decommissioned node is segregated by
# window after off-strategy compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_streaming_decommission(manager: ScyllaClusterManager):
    [node1, node2], cql = await prepare(manager, 2)
    await create_twcs_table(cql, rf=1)
    pks = await pks_on_every_shard(manager, node1, 100)
    await simulate(manager, [node1, node2], cql, pks, duration_minutes=20)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    await check_sstable_time_windows(manager, node2, 1, "MINUTES")
    await run_nodetool(manager, node1, "decommission")
    await offstrategy_compaction(manager, node2)
    await check_sstable_time_windows(manager, node2, 1, "MINUTES")


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_streaming_on_repair.
# The data a node missed while it was down, received by repair, is
# segregated by window after off-strategy compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_streaming_on_repair(manager: ScyllaClusterManager):
    [node1, node2], cql = await populate_racks(manager, 2)
    await create_twcs_table(cql, rf=2)
    pks = await pks_on_every_shard(manager, node1, 100)
    await manager.api.disable_tablet_balancing(node1.ip_addr)
    await simulate(manager, [node1, node2], cql, pks, duration_minutes=10)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES", balancing=False)
    await check_sstable_time_windows(manager, node2, 1, "MINUTES", balancing=False)
    await manager.server_stop_gracefully(node2.server_id)
    await simulate(manager, [node1], cql, pks, duration_minutes=20, start_from_minute=10)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES", balancing=False)
    await manager.server_start(node2.server_id)
    await run_nodetool(manager, node2, "repair", "ks")
    await run_nodetool(manager, node2, "flush")
    await manager.api.enable_tablet_balancing(node1.ip_addr)
    await offstrategy_compaction(manager, node2)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    await check_sstable_time_windows(manager, node2, 1, "MINUTES")


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_data_is_segregated_during_off_strategy.
# A repair-based bootstrap does not segregate the data it receives (#9199):
# off-strategy compaction does, from a few sstables per shard.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_data_is_segregated_during_off_strategy(manager: ScyllaClusterManager):
    rbno = {"enable_repair_based_node_ops": True, "allowed_repair_based_node_ops": "bootstrap"}
    [node1], cql = await prepare(manager, 1, config=rbno)
    await create_twcs_table(cql, rf=1)
    duration_minutes = 5
    pks = await pks_on_every_shard(manager, node1, 30)
    await simulate(manager, [node1], cql, pks, duration_minutes=duration_minutes)

    node2 = await manager.server_add(config={**PREPARE_CONFIG, **rbno}, cmdline=CMDLINE, property_file=DC1)
    # There is a rare chance that all the rows of node2 end up in one shard.
    matches = await wait_for_lines(manager, node2, r'stats: repair_reason=bootstrap, keyspace=ks, tables=\["test"\].*rx_row_nr=([\d]+),',
                                   SMP)
    shards_with_data = sum(int(m.group(1)) > 0 for _, m in matches)
    assert shards_with_data > 0, "Expected data in at least one shard of node2, but rx_row_nr is 0 for both the shards"
    await wait_for_lines(manager, node2, "Done with off-strategy compaction for ks.test", shards_with_data)
    for line, m in await wait_for_lines(manager, node2, r"Starting off-strategy compaction for ks.test.*?([\d]+) candidates were found",
                                        shards_with_data, timeout=0):
        assert int(m.group(1)) <= 30, f"There were too many sstables for off-strategy compaction. #9199\n{line}"
    await sstable_count_is_close_to_time_windows(manager, node2, duration_minutes, shards_with_data)


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_streaming_on_rebuild_multidc.
# The data a node of a new DC receives by rebuild is segregated by window
# after off-strategy compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_streaming_on_rebuild_multidc(manager: ScyllaClusterManager):
    config = {**CONFIG, "enable_sstable_key_validation": True}
    node1 = await manager.server_add(config=config, cmdline=CMDLINE, property_file={"dc": "dc1", "rack": "rack1"})
    cql1 = await manager.get_cql_exclusive(node1)
    cql1.execute("CREATE KEYSPACE ks  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1':1}")
    cql1.execute("CREATE TABLE ks.test (pk int, ck int, v blob, PRIMARY KEY(pk, ck)) WITH compaction = {"
                 "'class': 'TimeWindowCompactionStrategy','compaction_window_unit': 'MINUTES','compaction_window_size': 1}")
    cql = dtest_session(manager.get_cql())
    pks = await pks_on_every_shard(manager, node1, 100)
    await simulate(manager, [node1], cql, pks, duration_minutes=10)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")

    node2 = await manager.server_add(config=config, cmdline=CMDLINE, property_file={"dc": "dc2", "rack": "rack1"})
    await manager.server_sees_other_server(node1.ip_addr, node2.ip_addr)
    log2 = await manager.server_open_log(node2.server_id)
    await log2.wait_for("init - Scylla.*initialization completed")
    cql2 = dtest_session(await manager.get_cql_exclusive(node2))
    cql2.execute("ALTER KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
    mark = await log2.mark()
    res = await run_nodetool(manager, node2, "rebuild", "dc1", check=False)
    assert res[2] == 0 or "rebuild is in progress" in res[1], f"unexpected rebuild errors: {res}"
    await log2.wait_for("Streaming for rebuild successful|rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
    await offstrategy_compaction(manager, node2)
    await log2.wait_for("Starting off-strategy compaction for ks.test", from_mark=mark)
    await log2.wait_for("Done with off-strategy compaction for ks.test", from_mark=mark)
    await wait_for_compactions(manager, node2)
    await check_sstable_time_windows(manager, node2, 1, "MINUTES")


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_streaming_sstables_with_several_timewindows.
# Sstables holding two windows each are streamed to two new nodes
# (auto_bootstrap off), which segregate them by window.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_streaming_sstables_with_several_timewindows(manager: ScyllaClusterManager):
    [node1, _], cql = await populate_racks(manager, 2)
    await create_twcs_table(cql, rf=2)
    pks = await pks_on_every_shard(manager, node1, 100)
    await simulate(manager, await manager.running_servers(), cql, pks, duration_minutes=10, flush_period_seconds=120)
    max_timewindow = 1.5 * 2 * 60
    for sf, stats in (await sstable_stats(manager, node1, await statistics_files(manager, node1))).items():
        tw = (stats["max_timestamp"] - stats["min_timestamp"]) // 1_000_000
        assert tw <= max_timewindow, f"timewindow {tw} of {sf} is greater than {max_timewindow}"
    new_nodes = [await manager.server_add(config={**CONFIG, "auto_bootstrap": False}, cmdline=CMDLINE,
                                          property_file={"dc": "datacenter1", "rack": f"rack{i + 1}"}) for i in range(2)]
    for new_node in new_nodes:
        await check_sstable_time_windows(manager, new_node, 1, "MINUTES")


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_rebuild_node_streaming.
# A node whose data was removed gets it back by rebuild, segregated by
# window after off-strategy compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_rebuild_node_streaming(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager, 3)
    node3 = servers[2]
    await create_twcs_table(cql, rf=3)
    pks = await pks_on_every_shard(manager, servers[0], 100)
    await simulate(manager, servers, cql, pks, duration_minutes=10, flush_period_seconds=20)
    for s in servers:
        await check_sstable_time_windows(manager, s, 1, "MINUTES")
    await manager.server_stop_gracefully(node3.server_id)
    shutil.rmtree(os.path.join(await manager.server_get_workdir(node3.server_id), "data", "ks"), ignore_errors=True)
    await manager.server_start(node3.server_id)
    for s in servers[:2]:
        await manager.server_sees_other_server(s.ip_addr, node3.ip_addr)
    log3 = await manager.server_open_log(node3.server_id)
    mark = await log3.mark()
    await run_nodetool(manager, node3, "rebuild")
    await log3.wait_for("Streaming for rebuild successful|rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
    await offstrategy_compaction(manager, node3)
    await log3.wait_for("Done with off-strategy compaction for ks", from_mark=mark)
    await check_sstable_time_windows(manager, node3, 1, "MINUTES")


async def delete_rows_in_previous_time_windows(manager: ScyllaClusterManager, server, start_window_for_delete, end_window_for_delete,
                                               del_ck_per_window, write_mutaion_from_minute, num_windows_with_del_mutation,
                                               partitions) -> int:
    """The TWCS dtests' delete_rows_in_previous_time_windows(): deletes the
    first del_ck_per_window rows of each window, USING TIMESTAMP of a later
    window, then flushes."""
    cql = dtest_session(manager.get_cql())
    stmt = cql.prepare("DELETE FROM ks.test USING TIMESTAMP ? where pk =? and ck=?")
    sec = write_mutaion_from_minute * 60
    delta = num_windows_with_del_mutation * 60 // int(end_window_for_delete - start_window_for_delete)
    for i in range(int(start_window_for_delete * 60), int(end_window_for_delete * 60), 60):
        for ck in range(i, i + del_ck_per_window):
            await asyncio.to_thread(execute_concurrent_with_args, cql, stmt, [(sec * 1_000_000, pk, ck) for pk in partitions])
        sec += delta
    await run_nodetool(manager, server, "flush")
    return del_ck_per_window * len(partitions) * (end_window_for_delete - start_window_for_delete)


async def are_rows_in_sstable(manager: ScyllaClusterManager, server, data_file: str, partition_key, cluster_keys) -> bool:
    """The TWCS dtests' are_rows_in_sstable()."""
    found = False
    for partition in await dump_data_files(manager, server, [data_file]):
        if int(partition["key"]["value"]) == partition_key:
            cks = {int(row["key"]["value"]) for row in partition["clustering_elements"] if row["type"] == "clustering-row"}
            found = found or set(cluster_keys).issubset(cks)
    return found


async def deleted_rows_in_sstables_exist(manager: ScyllaClusterManager, server, partition_key, cluster_keys) -> bool:
    for f in sorted(await sstable_files(manager, server, "ks", "test")):
        if await are_rows_in_sstable(manager, server, f, partition_key, cluster_keys):
            return True
    return False


async def compact_ks(manager: ScyllaClusterManager, server) -> None:
    await run_nodetool(manager, server, "compact", "ks")


async def twcs_single_shard(manager: ScyllaClusterManager, num_pks, duration: int, gc_period: int = 1800):
    """A one-shard node with a TWCS table written for `duration` minutes,
    flushed every 10 s, and its pks and rows written."""
    [node1], cql = await prepare(manager, 1, cmdline=["--smp", "1"])
    await create_twcs_table(cql, rf=1, gc_period=gc_period)
    pks = num_pks if isinstance(num_pks, list) else await pks_on_every_shard(manager, node1, num_pks, smp=1)
    total = await simulate(manager, [node1], cql, pks, duration_minutes=duration, flush_period_seconds=10)
    return node1, cql, pks, total


async def count_sstables(manager: ScyllaClusterManager, server) -> int:
    return len(await statistics_files(manager, server))


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_compact_several_timewindows_after_delete_all_rows_in_old_timewindows.
# Major compaction compacts the sstables of all windows with the one of the
# deletions of all their rows: only that one is left.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compact_several_timewindows_after_delete_all_rows_in_old_timewindows(manager: ScyllaClusterManager):
    node1, cql, pks, _ = await twcs_single_shard(manager, 1, 5)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    await compact_ks(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == 5
    await delete_rows_in_previous_time_windows(manager, node1, 0, 5, 60, 5, 1, pks)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == 6
    await compact_ks(manager, node1)
    n = await count_sstables(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert n == 1
    current_rows = list(cql.execute("select * from ks.test"))
    assert current_rows == [], f"Some rows were resurrected {current_rows}"


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_compact_several_timewindows_after_delete_several_rows_per_timewindow.
# Deleting some rows of every window keeps every window's sstable through a
# major compaction.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compact_several_timewindows_after_delete_several_rows_per_timewindow(manager: ScyllaClusterManager):
    node1, cql, pks, total_row = await twcs_single_shard(manager, 4, 5)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    await compact_ks(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == 5
    total_deleted_rows = await delete_rows_in_previous_time_windows(manager, node1, 0, 5, 20, 5, 1, pks)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == 6
    await compact_ks(manager, node1)
    n = await count_sstables(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert n == 6
    current_rows = list(cql.execute("select * from ks.test"))
    assert total_row - total_deleted_rows == len(current_rows), f"Some rows were resurrected {len(current_rows)}"


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_compaction_remove_deleted_rows_in_previous_time_window.
# Rows of a window deleted in the next window stay in the sstables until
# gc_grace_seconds passed; then a major compaction purges them.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compaction_remove_deleted_rows_in_previous_time_window(manager: ScyllaClusterManager):
    gc_period = 15
    node1, cql, pks, total_row = await twcs_single_shard(manager, [1], 1, gc_period=gc_period)
    await compact_ks(manager, node1)
    await wait_for_compactions(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == 1

    deletion_time = time.time()
    total_deleted_rows = await delete_rows_in_previous_time_windows(manager, node1, 0, 1, 20, 1, 1, pks)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    cluster_keys = list(range(20))
    assert await deleted_rows_in_sstables_exist(manager, node1, 1, cluster_keys), f"Keys {cluster_keys} are deleted from sstables"
    assert await count_sstables(manager, node1) == 2

    # The first compaction keeps the tombstones: gc_grace_seconds did not pass.
    await compact_ks(manager, node1)
    await wait_for_compactions(manager, node1)
    n = await count_sstables(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert n == 2
    current_rows = list(cql.execute("select * from ks.test"))
    assert total_row - total_deleted_rows == len(current_rows), f"Some rows were resurrected {len(current_rows)}"
    assert await deleted_rows_in_sstables_exist(manager, node1, 1, cluster_keys), f"Keys {cluster_keys} are deleted from sstables"

    await asyncio.sleep(max(0, gc_period - (time.time() - deletion_time) + 1))
    await compact_ks(manager, node1)
    await wait_for_compactions(manager, node1)
    assert not await deleted_rows_in_sstables_exist(manager, node1, 1, cluster_keys), f"Keys {cluster_keys} are left in sstables"
    n = await count_sstables(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert n == 1


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_compact_several_timewindows_after_delete_rows_in_first_timewindow.
# Deleting all the rows of the first window lets a major compaction remove
# that window's sstable; the others stay.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_compact_several_timewindows_after_delete_rows_in_first_timewindow(manager: ScyllaClusterManager):
    duration = 5
    number_of_partitions = 5
    node1, cql, partitions, total_rows = await twcs_single_shard(manager, number_of_partitions, duration)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    await compact_ks(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == duration
    await delete_rows_in_previous_time_windows(manager, node1, 0, 1, 60, duration, 1, partitions)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert await count_sstables(manager, node1) == duration + 1
    await compact_ks(manager, node1)
    n = await count_sstables(manager, node1)
    await check_sstable_time_windows(manager, node1, 1, "MINUTES")
    assert n == duration
    current_rows = list(cql.execute("select * from ks.test"))
    assert total_rows - number_of_partitions * 60 == len(current_rows), f"Some rows were resurrected {len(current_rows)}"


def tw_query_results(cql, pks) -> list[dict]:
    """The TWCS dtest's get_tw_query_results()."""
    queries = [
        f"SELECT * FROM ks.test WHERE pk = {pks[0]} and ck > {30 * 60}",
        f"SELECT * FROM ks.test WHERE pk = {pks[-1]} and ck > {45 * 60}",
        f"SELECT * FROM ks.test WHERE pk = {pks[30 // 2]} and ck > {55 * 60}",
        f"SELECT * FROM ks.test WHERE pk in ({','.join(str(pk) for pk in pks[5:10])}) and ck > {2 * 60} and ck < {4 * 60}",
        f"SELECT * FROM ks.test WHERE pk in ({','.join(str(pk) for pk in pks[25:27])}) and ck > {33 * 60} and ck < {34 * 60}",
        "SELECT * FROM ks.test",
    ]
    return [{"query": q, "result": [row._asdict() for row in cql.execute(q)]} for q in queries]


# Ported from scylla-dtest compaction_additional_test.py::TestTimeWindowDataSegregation::test_enable_disable_optimized_query_for_twcs.
# Queries of a TWCS table return the same rows with
# enable_optimized_twcs_queries on and off.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_enable_disable_optimized_query_for_twcs(manager: ScyllaClusterManager):
    [node1, _], cql = await populate_racks(manager, 2)
    await create_twcs_table(cql, rf=2, ttl=5 * 60 * 20, window_size=5)
    pks = await pks_on_every_shard(manager, node1, 30)
    await simulate(manager, await manager.running_servers(), cql, pks, duration_minutes=60, flush_period_seconds=100)

    def set_optimized(enable: bool):
        cql.execute("ALTER TABLE ks.test with compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES',"
                    f"'compaction_window_size': 5,'enable_optimized_twcs_queries': {'true' if enable else 'false'} }}")

    enabled = tw_query_results(cql, pks)
    set_optimized(False)
    disabled = tw_query_results(cql, pks)
    assert enabled == disabled
    set_optimized(True)
    enabled = tw_query_results(cql, pks)
    assert enabled == disabled


LCS_OPTIONS = {"class": "LeveledCompactionStrategy", "sstable_size_in_mb": 1}


async def table_levels_after_convergence(manager: ScyllaClusterManager, server) -> list[int]:
    """The LCS dtests' _get_table_levels_after_convergence()."""
    levels = []
    for _ in range(10):
        new_levels = await manager.api.client.get_json("/column_family/sstables/per_level/ks:cf", host=server.ip_addr)
        if levels == new_levels:
            break
        levels = new_levels
        await wait_for_compactions(manager, server)
    return levels


async def check_space_amplification(manager: ScyllaClusterManager, server, levels: list[int]) -> None:
    """The LCS dtests' _check_space_amplification(): the data of a level and
    the one below it is at most 1.2 times the data of the level."""
    sstable_info = await manager.api.client.get_json("/storage_service/sstable_info", host=server.ip_addr,
                                                     params={"keyspace": "ks", "cf": "cf"})
    data_size = [0] * len(levels)
    for sstable in sstable_info[0]["sstables"]:
        data_size[sstable["level"]] += sstable["size"]
    amplification = []
    for i in range(len(levels) - 1):
        if data_size[i + 1] != 0:
            amplification.append((data_size[i + 1] + data_size[i]) / data_size[i + 1])
        elif data_size[i] == 0:
            amplification.append(1)
        else:
            pytest.fail(f"Invalid sstable info: {levels=}, {data_size=}")
    assert all(a < 1.2 for a in amplification), f"{amplification=} {levels=} {data_size=}"


async def lcs_prepare(manager: ScyllaClusterManager, compaction: dict):
    [node], cql = await prepare(manager, 1)
    await create_ks(cql, "ks", 1)
    await create_c1c2_cf(cql, "ks", "cf", compaction=compaction)
    return node, cql


# Ported from scylla-dtest compaction_additional_test.py::TestLCSSSTablePromotion::test_lcs_sstable_promotion.
# LCS promotes sstables only until a level is 10 times larger than the one
# below it (Scylla commit 9de7abdc): space amplification stays bounded.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_lcs_sstable_promotion(manager: ScyllaClusterManager):
    node, cql = await lcs_prepare(manager, LCS_OPTIONS)
    await insert_c1c2(cql, n=50_000, concurrency=50)
    await run_nodetool(manager, node, "flush")
    await check_space_amplification(manager, node, await table_levels_after_convergence(manager, node))


# Ported from scylla-dtest compaction_additional_test.py::TestLCSSSTablePromotion::test_lcs_table_promotion_major_compaction.
# The same after a major compaction of data written with autocompaction off.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_lcs_table_promotion_major_compaction(manager: ScyllaClusterManager):
    node, cql = await lcs_prepare(manager, LCS_OPTIONS)
    await run_nodetool(manager, node, "disableautocompaction", "ks", "cf")
    await insert_c1c2(cql, n=50_000, concurrency=50)
    await run_nodetool(manager, node, "flush")
    await manager.api.keyspace_compaction(node.ip_addr, "ks", "cf")
    await check_space_amplification(manager, node, await table_levels_after_convergence(manager, node))


# Ported from scylla-dtest compaction_additional_test.py::TestLCSSSTablePromotion::test_lcs_table_promotion_after_stcs_migration.
# The same after a table written with STCS switches to LCS.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_lcs_table_promotion_after_stcs_migration(manager: ScyllaClusterManager):
    node, cql = await lcs_prepare(manager, {"class": "SizeTieredCompactionStrategy"})
    for n in [5_000, 15_000, 30_000, 50_000, 80_000]:
        await insert_c1c2(cql, n=n, concurrency=50)
        await run_nodetool(manager, node, "flush")
    cql.execute(f"ALTER TABLE ks.cf WITH compaction={LCS_OPTIONS}")
    await run_nodetool(manager, node, "refresh", "ks", "cf")
    await check_space_amplification(manager, node, await table_levels_after_convergence(manager, node))


############################################################################
# More ports from compaction_test.py, one test per dtest.

# TestCompaction's nodes: the class's cluster_options and compaction debug
# logging (CMDLINE), with vnodes like the dtests.
TC_CONFIG = {**CONFIG, 'repair_hints_batchlog_flush_cache_time_in_ms': 0}
TC_POPULATE_CONFIG = {**TC_CONFIG, "endpoint_snitch": "SimpleSnitch"}
# TestCompaction is parametrized by the base table's strategy: LCS is in
# dtest_full only (tier3, release and debug); the others are next_gating
# too (release only).
TC_STRATEGIES = [
    pytest.param("LeveledCompactionStrategy", marks=[pytest.mark.tier3, pytest.mark.skip_mode(
        mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")]),
    *(pytest.param(s, marks=pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only"))
      for s in ["SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"]),
]
PROPAGATION_DELAY_IN_SECONDS = 5


async def tc_populate_racks(manager: ScyllaClusterManager, racks: int, config: dict | None = None):
    """ccm's populate(generate_cluster_topology(rack_num=racks)).start()
    with TestCompaction's options."""
    return await manager.servers_add(racks, config={**TC_CONFIG, **(config or {})}, cmdline=CMDLINE,
                                     property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(racks)])


async def tc_create_ks(cql, name: str, rf: int) -> None:
    """The dtests' create_ks(): the keyspace, schema agreement and USE."""
    await create_ks(cql, name, rf)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"USE {name}")


async def tc_count_deleted(manager: ScyllaClusterManager, cql, server) -> int:
    """TestCompaction.count_deleted(): write key 99 through the session,
    flush and major-compact the node, and count its partition tombstones."""
    cql.execute("insert into ks.cf (key, val) values (99,1);")
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    partitions = await dump_sstables(manager, server, "ks", "cf")
    return sum("tombstone" in p for p in partitions)


async def tc_verify_deleted(manager: ScyllaClusterManager, cql, server, num_deleted_rows: int) -> None:
    count = await tc_count_deleted(manager, cql, server)
    assert count == num_deleted_rows, f"Error: expected {num_deleted_rows} deleted partitions but found {count} on {server.ip_addr}"


async def tc_delete_keys(manager: ScyllaClusterManager, servers, verify_deleted: bool = True, num: int = 10) -> None:
    """TestCompaction._delete_keys(): delete keys 0..num-1 through a new
    session to node1; with verify_deleted, flush every node and check that
    the keys read as missing."""
    cql = dtest_session(await manager.get_cql_exclusive(servers[0]))
    for idx in range(num):
        cql.execute(f"delete from ks.cf where key = {idx}")
    if verify_deleted:
        for s in servers:
            await run_nodetool(manager, s, "flush")
        for idx in range(num):
            assert list(cql.execute(f"select * from ks.cf where key = {idx}")) == []


async def tc_compaction_delete_tombstone_gc(manager: ScyllaClusterManager, strategy: str, build_mode: str, tombstone_gc_mode="repair",
                                            racks: int = 2, rf: int = 2, delete_keys: bool = True, partition_num: int = 100):
    """TestCompaction._test_compaction_delete_tombstone_gc(): a cluster of
    one node per rack, an RF keyspace, a table with the tombstone_gc mode,
    100 keys flushed on every node, then (optionally) 10 keys deleted.
    Returns the servers, the expiry deadline sampled before the deletes,
    and the expiry time after them."""
    servers = await tc_populate_racks(manager, racks, {"tablets_initial_scale_factor": 1})
    cql = dtest_session(manager.get_cql())
    await tc_create_ks(cql, "ks", rf)
    if tombstone_gc_mode == "timeout":
        gc_grace_seconds = 45 if build_mode in ("debug", "dev") else 30
    else:
        gc_grace_seconds = 5
    cql.execute(f"create table ks.cf (key int PRIMARY KEY, val int) "
                f"with tombstone_gc = {{'mode':'{tombstone_gc_mode}', 'propagation_delay_in_seconds':'{PROPAGATION_DELAY_IN_SECONDS}'}} "
                f"and compaction = {{'class':'{strategy}'}} and gc_grace_seconds = {gc_grace_seconds};")
    for x in range(partition_num):
        cql.execute(f"insert into ks.cf (key, val) values ({x},1)")
    for s in servers:
        await run_nodetool(manager, s, "flush")
    expiry_deadline = time.time() + gc_grace_seconds
    if delete_keys:
        await tc_delete_keys(manager, servers)
    return servers, expiry_deadline, time.time() + gc_grace_seconds


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_keys_with_ttl_present.
# Rows with a long TTL in an RF=2 table on two nodes survive a flush, a
# major compaction and a restart of each node, whatever the tombstone_gc
# mode.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
@pytest.mark.parametrize("tombstone_gc_mode", ["repair", "timeout", "disabled", "immediate"])
async def test_keys_with_ttl_present(manager: ScyllaClusterManager, strategy, tombstone_gc_mode):
    partition_num, gc_grace_seconds = 100, 5
    servers = await tc_populate_racks(manager, 2)
    cql = dtest_session(manager.get_cql())
    await tc_create_ks(cql, "ks", 2)
    cql.execute(f"create table ks.cf (key int PRIMARY KEY, val int) "
                f"with tombstone_gc = {{'mode':'{tombstone_gc_mode}', 'propagation_delay_in_seconds':'{PROPAGATION_DELAY_IN_SECONDS}'}} "
                f"and compaction = {{'class':'{strategy}'}} and gc_grace_seconds = {gc_grace_seconds};")
    for x in range(partition_num):
        cql.execute(f"insert into ks.cf (key, val) values ({x},1) USING TTL 100000")
    for s in servers:
        await run_nodetool(manager, s, "flush")
        await run_nodetool(manager, s, "compact")
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_start(s.server_id)
    cql = dtest_session((await manager.get_ready_cql(servers))[0])
    cql.execute("USE ks")
    logger.debug(f"all rows = {sorted(list(r) for r in cql.execute('select * from ks.cf;'))}")
    for idx in range(partition_num):
        assert [list(r) for r in cql.execute(f"select * from ks.cf where key = {idx}")] == [[idx, 1]]


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_compaction_delete_tombstone_gc.
# Tombstones of an RF=2 table on two nodes are purged by major compaction
# as their tombstone_gc mode allows: immediately, after gc_grace_seconds,
# after a repair, or never.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
@pytest.mark.parametrize("tombstone_gc_mode", ["repair", "timeout", "disabled", "immediate"])
async def test_compaction_delete_tombstone_gc(manager: ScyllaClusterManager, strategy, tombstone_gc_mode, build_mode):
    servers, expiry_deadline, expiry_time = await tc_compaction_delete_tombstone_gc(manager, strategy, build_mode, tombstone_gc_mode)
    node1, node2 = servers
    cql = dtest_session(await manager.get_cql_exclusive(node1))
    if tombstone_gc_mode == "immediate":
        # A tombstone is purged only once its expiry is before the query
        # time, in seconds.
        await asyncio.sleep(1)
        await tc_verify_deleted(manager, cql, node1, 0)
        await tc_verify_deleted(manager, cql, node2, 0)
    elif tombstone_gc_mode == "timeout":
        counts = [await tc_count_deleted(manager, cql, n) for n in (node1, node2)]
        now = time.time()
        assert expiry_deadline > now, f"missed tombstone expiration time: {now} >= {expiry_deadline}"
        assert counts == [10, 10], f"Error: expected 10 deleted partitions but found {counts}"
    else:
        await tc_verify_deleted(manager, cql, node1, 10)
        await tc_verify_deleted(manager, cql, node2, 10)

    await asyncio.sleep(max(0, expiry_time - time.time()) + 1)
    expected = 0 if tombstone_gc_mode in ("immediate", "timeout") else 10
    await tc_verify_deleted(manager, cql, node1, expected)
    await tc_verify_deleted(manager, cql, node2, expected)

    await run_nodetool(manager, node1, "repair", "ks", "cf")
    expected = 10 if tombstone_gc_mode == "disabled" else 0
    await tc_verify_deleted(manager, cql, node1, expected)
    await tc_verify_deleted(manager, cql, node2, expected)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_delete_tombstone_gc_node_down.
# With tombstone_gc mode "repair" on an RF=4 table of four nodes: a repair
# that fails because node4 is down purges no tombstones on the other nodes;
# once node4 is back, a primary-range repair on every node and an
# off-strategy compaction let major compactions purge them all.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_delete_tombstone_gc_node_down(manager: ScyllaClusterManager, strategy, build_mode):
    partition_num = 10
    servers, _, _ = await tc_compaction_delete_tombstone_gc(manager, strategy, build_mode, "repair", racks=4, rf=4,
                                                           delete_keys=False, partition_num=partition_num)
    node1, node2, node3, node4 = servers
    await manager.server_stop(node4.server_id, convict=False)
    await manager.others_not_see_server(node4.ip_addr)
    await tc_delete_keys(manager, servers, verify_deleted=False, num=partition_num // 2)
    for s in [node1, node2, node3]:
        await run_nodetool(manager, s, "flush")

    cql = dtest_session(await manager.get_cql_exclusive(node1))
    before = sum([await tc_count_deleted(manager, cql, s) for s in [node1, node2, node3]])
    _, _, returncode = await run_nodetool(manager, node1, "repair", "ks", "cf", check=False)
    assert returncode != 0, "expected the repair with node4 down to fail"
    after = sum([await tc_count_deleted(manager, cql, s) for s in [node1, node2, node3]])
    assert after == before

    await manager.server_start(node4.server_id)
    await manager.servers_see_each_other(await manager.running_servers())
    from cassandra.cluster import EXEC_PROFILE_DEFAULT
    cql4 = manager.con_gen([node4.ip_addr]).connect()
    cql4.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.QUORUM
    await asyncio.gather(*[run_nodetool(manager, s, "repair", "ks", "cf", "--partitioner-range") for s in servers])
    repair_history = list(cql4.execute("SELECT table_name from system.repair_history"))
    assert any("cf" in repair for repair in repair_history)
    # The dtest passes params={"cf", cf}, a set, which requests sends as
    # "c=f": the off-strategy compaction of the whole keyspace.
    await manager.api.client.post("/storage_service/keyspace_offstrategy_compaction/ks", host=node4.ip_addr)
    for s in servers:
        await tc_verify_deleted(manager, cql4, s, 0)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_sstable_deletion.
# With tombstone_gc mode "immediate", a major compaction after every row was
# deleted leaves no sstable of the table.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_sstable_deletion(manager: ScyllaClusterManager, strategy):
    server = await manager.server_add(config=TC_POPULATE_CONFIG, cmdline=CMDLINE, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = dtest_session(manager.get_cql())
    await tc_create_ks(cql, "ks", 1)
    cql.execute(f"create table cf (key int PRIMARY KEY, val int) with tombstone_gc = {{'mode': 'immediate'}} and compaction= {{'class':'{strategy}'}}")
    for x in range(100):
        cql.execute(f"insert into cf (key, val) values ({x},{x})")
    await run_nodetool(manager, server, "flush")
    for x in range(100):
        cql.execute(f"delete from cf where key = {x}")
    await asyncio.sleep(1)  # so that the tombstones gc-expire
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact", "ks", "cf")
    await wait_for_lines(manager, server, r".*(Compact|Major) ks.cf .*Compacted", SMP, from_mark=mark)
    await asyncio.sleep(1)
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    found = [f for f in os.listdir(table_dir) if "Data" in f]
    assert not found, f"Expected no SSTables in {table_dir}, but found: {found}"


async def tc_clear_and_start(manager: ScyllaClusterManager, server) -> None:
    """ccm's cluster.clear() (stop, and remove the node's data and commit
    log directories), a 5 s sleep, and cluster.start(). The driver is
    reconnected: the old session's keyspace is gone with the data, and the
    dtest opens a new session after it anyway."""
    manager.driver_close()
    await manager.server_stop_gracefully(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)
    for d in ["data", "commitlog"]:
        shutil.rmtree(os.path.join(workdir, d), ignore_errors=True)
    await asyncio.sleep(5)
    await start_node_wait_cql(manager, server)
    await manager.driver_connect()


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_compaction_strategy_switching.
# For each other strategy: a table of the test's strategy with flushed rows
# and later deletions is altered to it, and reads the same rows; then the
# node is cleared and started again.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_compaction_strategy_switching(manager: ScyllaClusterManager, strategy):
    strategies = [s for s in STRATEGIES if s != strategy]
    server = await manager.server_add(config=TC_POPULATE_CONFIG, cmdline=CMDLINE, property_file={"dc": "datacenter1", "rack": "rack1"})
    for strat in strategies:
        cql = dtest_session((await manager.get_ready_cql([server]))[0])
        await tc_create_ks(cql, "ks", 1)
        cql.execute(f"create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {{'class':'{strategy}'}};")
        for x in range(100):
            cql.execute(f"insert into ks.cf (key, val) values ({x},1)")
        await run_nodetool(manager, server, "flush")
        for x in range(10):
            cql.execute(f"delete from cf where key = {x}")
        cql.execute(f"alter table ks.cf with compaction = {{'class':'{strat}'}};")
        for x in range(11, 100):
            assert [list(r) for r in cql.execute(f"select * from ks.cf where key ={x}")] == [[x, 1]]
        for x in range(10):
            assert list(cql.execute(f"select * from cf where key = {x}")) == []
        await run_nodetool(manager, server, "flush")
        await tc_clear_and_start(manager, server)


PRIMARY_KS, PRIMARY_TABLE = "ks", "to_disable"
SECONDARY_KS, SECONDARY_TABLE = "ks2", "std1"
EMPTY_ERROR_MESSAGE = "Query didn't return any rows"


async def is_autocompaction_enabled(manager: ScyllaClusterManager, server, ks: str, table: str) -> bool:
    """The dtests' is_autocompaction_enabled()."""
    return await manager.api.client.get_json(f"/column_family/autocompaction/{ks}:{table}", host=server.ip_addr)


async def tc_disable_autocompaction(manager: ScyllaClusterManager, server, ks: str, table: str, verify: bool = True) -> None:
    """TestCompaction.disable_autocompaction()."""
    await run_nodetool(manager, server, "disableautocompaction", ks, table)
    if verify:
        assert not await is_autocompaction_enabled(manager, server, ks, table), "Expected to have autocompaction disabled but got it is enabled"
        assert await is_autocompaction_enabled(manager, server, SECONDARY_KS, SECONDARY_TABLE), \
            "Other keyspaces and tables are not supposed to be affected by disableautocompaction"


async def tc_enable_autocompaction(manager: ScyllaClusterManager, server, ks: str, table: str, verify: bool = True) -> None:
    """TestCompaction.enable_autocompaction()."""
    await run_nodetool(manager, server, "enableautocompaction", "--", ks, table)
    if verify:
        assert await is_autocompaction_enabled(manager, server, ks, table), "Expected to have autocompaction enabled but got it is disabled"
        assert await is_autocompaction_enabled(manager, server, SECONDARY_KS, SECONDARY_TABLE), "Expected to have autocompaction enabled but got it is disabled"


async def prepare_testbed(manager: ScyllaClusterManager, strategy: str, with_compaction: str | None = None):
    """TestCompaction.prepate_testbed(): populate(1), and the tables
    ks.to_disable (create_c1c2_table with the given compaction, by default
    the test's strategy) and ks2.std1 (the test's strategy)."""
    server = await manager.server_add(config=TC_POPULATE_CONFIG, cmdline=CMDLINE, property_file={"dc": "datacenter1", "rack": "rack1"})
    for ks, table, compaction in [(PRIMARY_KS, PRIMARY_TABLE, with_compaction), (SECONDARY_KS, SECONDARY_TABLE, None)]:
        cql = dtest_session(manager.get_cql())
        await tc_create_ks(cql, ks, 1)
        await create_c1c2_cf(cql, ks, table, compaction=compaction or f"{{'class':'{strategy}'}}")
    return server


async def tc_fill_table_with_data(manager: ScyllaClusterManager, server, ks: str, table: str, keys: int = 1000) -> None:
    """TestCompaction.fill_table_with_data(): 100 keys at a time, written at
    CL=ANY, each 100 followed by a flush. The dtest uses a session of its
    own, so no USE here: it would change the keyspace of the shared session."""
    cql = dtest_session(manager.get_cql())
    for i in range(0, keys, 100):
        await insert_c1c2(cql, keys=list(range(i, min(i + 100, keys))), ks=ks, cf=table, consistency=ConsistencyLevel.ANY)
        await run_nodetool(manager, server, "flush")


def last_compaction_timestamp(cql, cf_name: str, since_timestamp=None) -> int:
    timestamp_filter = f"AND compacted_at > {since_timestamp} " if since_timestamp else ""
    query = f"SELECT MAX(compacted_at) FROM system.compaction_history WHERE columnfamily_name = '{cf_name}' {timestamp_filter}ALLOW FILTERING"
    result = next(iter(cql.execute(query)))[0]
    timestamp = int(1000 * datetime.datetime.timestamp(result)) if result else None
    if not timestamp:
        raise RuntimeError(EMPTY_ERROR_MESSAGE)
    return timestamp


async def retry_last_compaction_timestamp(cql, cf_name: str, timeout: float = 2, since_timestamp=None) -> int:
    """The dtests' retry_till_success(last_compaction_timestamp, timeout=2)."""
    deadline = time.time() + timeout
    while True:
        try:
            return last_compaction_timestamp(cql, cf_name, since_timestamp)
        except Exception:
            if time.time() > deadline:
                raise
            await asyncio.sleep(0.25)


async def assert_table_did_not_compact(cql, cf_name: str, since_timestamp=None) -> None:
    with pytest.raises(RuntimeError, match=EMPTY_ERROR_MESSAGE):
        await retry_last_compaction_timestamp(cql, cf_name, since_timestamp=since_timestamp)


async def assert_table_compacted(cql, cf_name: str, since_timestamp=None) -> int:
    return await retry_last_compaction_timestamp(cql, cf_name, since_timestamp=since_timestamp)


async def set_load_balancing(manager: ScyllaClusterManager, server, enabled: bool) -> None:
    """The dtests' disable_load_balancing() context."""
    await manager.api.client.post("/storage_service/tablets/balancing", host=server.ip_addr,
                                  params={"enabled": "true" if enabled else "false"})


async def fill_both_tables(manager: ScyllaClusterManager, server) -> None:
    await tc_fill_table_with_data(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await tc_fill_table_with_data(manager, server, SECONDARY_KS, SECONDARY_TABLE)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_nodetool.
# "nodetool disableautocompaction" of a table stops its compactions, not
# those of another table, and "nodetool enableautocompaction" resumes them.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_disable_autocompaction_nodetool(manager: ScyllaClusterManager, strategy):
    server = await prepare_testbed(manager, strategy)
    await set_load_balancing(manager, server, False)
    cql = dtest_session(manager.get_cql())
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE)
    await tc_enable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_compacted(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)
    await set_load_balancing(manager, server, True)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_schema.
# A table created with compaction 'enabled': 'false' does not compact, also
# after a restart, until altered to 'enabled': 'true'.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_disable_autocompaction_schema(manager: ScyllaClusterManager, strategy):
    server = await prepare_testbed(manager, strategy, with_compaction=f"{{'class':'{strategy}', 'enabled':'false'}}")
    await set_load_balancing(manager, server, False)
    cql = dtest_session(manager.get_cql())
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE)
    # Still disabled after a restart.
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    cql = dtest_session((await manager.get_ready_cql([server]))[0])
    cql.execute("use ks")
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE)
    cql.execute(f"ALTER TABLE {PRIMARY_TABLE} with compaction = {{'class': '{strategy}', 'enabled': 'true'}}")
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await tc_enable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_compacted(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)
    await set_load_balancing(manager, server, True)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_alter.
# ALTER TABLE ... compaction 'enabled': 'false' stops the table's
# compactions, and 'enabled': 'true' resumes them.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_disable_autocompaction_alter(manager: ScyllaClusterManager, strategy):
    server = await prepare_testbed(manager, strategy)
    cql = dtest_session(manager.get_cql())
    cql.execute("use ks")
    cql.execute(f"ALTER TABLE to_disable WITH compaction = {{'class':'{strategy}', 'enabled':'false'}}")
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE)
    cql.execute(f"ALTER TABLE {PRIMARY_TABLE} WITH compaction = {{'class':'{strategy}', 'enabled':'true'}}")
    await fill_both_tables(manager, server)
    await assert_table_compacted(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_alter_and_nodetool.
# Autocompaction disabled with nodetool stays disabled after an ALTER of
# the table's compaction options, until enabled with nodetool.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_disable_autocompaction_alter_and_nodetool(manager: ScyllaClusterManager, strategy):
    server = await prepare_testbed(manager, strategy)
    await set_load_balancing(manager, server, False)
    cql = dtest_session(manager.get_cql())
    cql.execute("use ks")
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE)
    cql.execute(f"ALTER TABLE {PRIMARY_TABLE} WITH compaction = {{'class':'{strategy}', 'tombstone_threshold':0.9}}")
    cql.execute(f"insert into {PRIMARY_TABLE} (key, c1, c2) values ('99', 'hello', 'hello')")
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)
    await tc_enable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_compacted(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)
    await set_load_balancing(manager, server, True)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_without_params.
# "nodetool disableautocompaction" / "enableautocompaction" without a
# keyspace or table apply to all tables.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_disable_autocompaction_without_params(manager: ScyllaClusterManager, strategy):
    server = await prepare_testbed(manager, strategy)
    await set_load_balancing(manager, server, False)
    cql = dtest_session(manager.get_cql())
    await run_nodetool(manager, server, "disableautocompaction")
    assert not await is_autocompaction_enabled(manager, server, PRIMARY_KS, PRIMARY_TABLE), "Expected to have autocompaction disabled but got it is enabled"
    assert not await is_autocompaction_enabled(manager, server, SECONDARY_KS, SECONDARY_TABLE), \
        "All keyspaces and tables are expected to be affected by disableautocompaction"
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    # As in the dtest, the timestamp of a table that did not compact is None.
    timestamp = None
    await assert_table_did_not_compact(cql, SECONDARY_TABLE)
    await run_nodetool(manager, server, "enableautocompaction")
    assert await is_autocompaction_enabled(manager, server, PRIMARY_KS, PRIMARY_TABLE), "Expected to have autocompaction enabled but got it is disabled"
    assert await is_autocompaction_enabled(manager, server, SECONDARY_KS, SECONDARY_TABLE), \
        "All keyspaces and tables are expected to be affected by enableautocompaction"
    await fill_both_tables(manager, server)
    await assert_table_compacted(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)
    await set_load_balancing(manager, server, True)


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_twice.
# Disabling autocompaction of a table twice in a row does not fail, and
# keeps it disabled.
@pytest.mark.parametrize("strategy", TC_STRATEGIES)
async def test_disable_autocompaction_twice(manager: ScyllaClusterManager, strategy):
    server = await prepare_testbed(manager, strategy)
    await set_load_balancing(manager, server, False)
    cql = dtest_session(manager.get_cql())
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE)
    timestamp = await assert_table_compacted(cql, SECONDARY_TABLE)
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await tc_disable_autocompaction(manager, server, PRIMARY_KS, PRIMARY_TABLE)
    await fill_both_tables(manager, server)
    await assert_table_did_not_compact(cql, PRIMARY_TABLE, since_timestamp=timestamp)
    await assert_table_compacted(cql, SECONDARY_TABLE, since_timestamp=timestamp)
    await set_load_balancing(manager, server, True)


async def flush_small_sstables(manager: ScyllaClusterManager, server, cql, ks: str, cf: str) -> None:
    """The dtest's _flush_small_sstables(): 16 flushed sstables of 100 rows
    of five 1 KB columns each, with the rows' timestamps spread over three
    minutes, so that every sstable spans several one-minute TWCS windows
    (TWCS reshapes such an sstable however few sstables there are)."""
    insert = cql.prepare(f'INSERT INTO {ks}.{cf} (key, "C0", "C1", "C2", "C3", "C4") VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?')
    now = int(time.time() * 1_000_000)
    for flush in range(16):
        args = [((flush * 100 + i).to_bytes(8, "big"), *(os.urandom(1024) for _ in range(5)), now - (i % 3) * 60_000_000) for i in range(100)]
        execute_concurrent_with_args(cql, insert, args, raise_on_first_error=True)
        await run_nodetool(manager, server, "flush")


# Ported from scylla-dtest compaction_test.py::TestCompaction::test_disable_autocompaction_doesnt_block_user_initiated_reshape_compaction.
# With autocompaction disabled, a refresh of a few small sstables written
# under STCS into the table switched to TWCS, followed by an off-strategy
# compaction, still reshapes them. The dtest runs only with its
# LeveledCompactionStrategy parameter (not next_gating).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("strategy", ["LeveledCompactionStrategy"])
async def test_disable_autocompaction_doesnt_block_user_initiated_reshape_compaction(manager: ScyllaClusterManager, strategy):
    twcs = {"class": "TimeWindowCompactionStrategy", "compaction_window_size": 1, "compaction_window_unit": "MINUTES", "max_threshold": 2, "min_threshold": 2}
    stcs = {"class": "SizeTieredCompactionStrategy", "bucket_high": 1.5, "bucket_low": 0.5, "min_sstable_size": 1, "max_threshold": 2, "min_threshold": 2}
    ks, cf = "keyspace1", "standard1"
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    server = await manager.server_add(config={**TC_POPULATE_CONFIG, "auto_snapshot": True}, cmdline=CMDLINE, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = dtest_session(manager.get_cql())
    cql.execute("DROP KEYSPACE IF EXISTS keyspace1")
    # The dtests' create_stress_compatible_table().
    cql.execute("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    cql.execute("""CREATE TABLE keyspace1.standard1(
        key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob
        ) WITH bloom_filter_fp_chance = 0.01
        AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
        AND comment = ''
        AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '100'}
        AND compression = {}
        AND crc_check_chance = 1.0
        AND default_time_to_live = 0
        AND gc_grace_seconds = 864000
        AND max_index_interval = 2048
        AND memtable_flush_period_in_ms = 0
        AND min_index_interval = 128
        AND speculative_retry = '99.0PERCENTILE'""")
    await set_load_balancing(manager, server, False)
    cql.execute(f"ALTER TABLE {ks}.{cf} WITH compaction={stcs}")
    await tc_disable_autocompaction(manager, server, ks, cf, verify=False)
    await flush_small_sstables(manager, server, cql, ks, cf)
    table_dir = await get_table_dir(manager, server, ks, cf)
    upload_dir = os.path.join(table_dir, "upload")
    os.makedirs(upload_dir, exist_ok=True)
    for name in os.listdir(table_dir):
        if os.path.isfile(os.path.join(table_dir, name)):
            shutil.copy2(os.path.join(table_dir, name), os.path.join(upload_dir, name))
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    cql.execute(f"TRUNCATE {ks}.{cf}")
    cql.execute(f"ALTER TABLE {ks}.{cf} WITH compaction={twcs}")
    await run_nodetool(manager, server, "refresh", "--", ks, cf)
    # With tablets, refresh only adds the sstables to the maintenance set and
    # off-strategy compaction reshapes them after a 5-minute timer: trigger
    # it now. Without tablets, refresh already reshaped them.
    await offstrategy_compaction(manager, server, ks, cf)
    await log.wait_for("Reshaped", from_mark=mark, timeout=600)
    await set_load_balancing(manager, server, True)
