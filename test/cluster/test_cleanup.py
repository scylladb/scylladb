#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# "nodetool cleanup" of vnodes keyspaces, ported from scylla-dtest
# cleanup_test.py: cleanup keeps the data a node owns, after a restart and
# after a node joined; its temporary disk usage stays below twice the table's;
# restoring a snapshot taken before a node joined and replaying a stale
# commitlog after a crash do not resurrect cleaned up data; dropping the
# keyspace during its cleanup does not fail the cleanup. Like the dtests,
# the tests use the dtests' c1c2 tables, written at CL=ALL, and nodes with
# 2 shards.

import asyncio
import glob
import logging
import os
import re
import shutil
import time

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.test_compaction import wait_for_compactions
from test.cluster.util import dump_sstables, get_table_dir, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Clusters the dtests populate with a node count instead of a topology use
# the SimpleSnitch, with every node in datacenter1/rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}
DC1_RACK1 = {"dc": "datacenter1", "rack": "rack1"}
# The dtests' nodes run with 2 shards, as test.py's do by default.
SMP = 2


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def cql_timeout(build_mode, seconds):
    """The dtests' cql_timeout(): longer in slower build modes."""
    return seconds * (3 if build_mode in DEBUG_MODES else 1 if build_mode == "release" else 2)


def node_config(timeout=None, simple_snitch=False):
    config = {"tablets_mode_for_new_keyspaces": "disabled"}
    if timeout:
        config["range_request_timeout_in_ms"] = timeout * 1000
    if simple_snitch:
        config.update(SIMPLE_SNITCH)
    return config


def connect(manager: ScyllaClusterManager):
    cql = manager.get_cql()
    # Statements default to consistency level ONE, as in dtest sessions.
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


async def execute_concurrent(cql, statement, args_list, concurrency=20):
    """Like the driver's execute_concurrent_with_args()."""
    for i in range(0, len(args_list), concurrency):
        await asyncio.gather(*[cql.run_async(statement, args) for args in args_list[i:i + concurrency]])


async def create_c1c2_table(cql, ks, cf, compaction="{'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}"):
    """The dtests' create_cf() / create_c1c2_table()."""
    await cql.run_async(f"CREATE COLUMNFAMILY {ks}.{cf} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                        f"AND compaction = {compaction} AND compression = {{}}")
    await cql.run_async(f"SELECT * FROM {ks}.{cf} LIMIT 1")


async def insert_c1c2(cql, keys, consistency=ConsistencyLevel.QUORUM, ks="ks", cf="cf"):
    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2')")
    insert.consistency_level = consistency
    await execute_concurrent(cql, insert, [[f"k{k}"] for k in keys])


async def delete_c1c2(cql, n, consistency=ConsistencyLevel.QUORUM, ks="ks", cf="cf"):
    delete = cql.prepare(f"DELETE FROM {ks}.{cf} WHERE key=?")
    delete.consistency_level = consistency
    await execute_concurrent(cql, delete, [[f"k{k}"] for k in range(n)])


async def prepare(manager: ScyllaClusterManager, nodes, num_keys, timeout=None, consistency=ConsistencyLevel.ALL,
                  amount_of_tables=1, rf=None):
    """The dtests' prepare(): a cluster of `nodes` nodes (a count: SimpleSnitch,
    datacenter1/rack1; or a list of dc/rack property files), keyspace ks with
    tables cf0.. of num_keys keys each."""
    if isinstance(nodes, int):
        servers = await manager.servers_add(nodes, config=node_config(timeout, simple_snitch=True), property_file=DC1_RACK1)
    else:
        servers = await manager.servers_add(len(nodes), config=node_config(timeout), property_file=nodes)
    cql = connect(manager)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf or len(servers)}}}")
    for i in range(amount_of_tables):
        await create_c1c2_table(cql, "ks", f"cf{i}")
    for i in range(amount_of_tables):
        await insert_c1c2(cql, range(num_keys), consistency=consistency, cf=f"cf{i}")
    return servers, cql


async def nodetool_all(manager: ScyllaClusterManager, *args):
    """ccm's cluster.nodetool(): run the command on every running node."""
    for server in await manager.running_servers():
        await run_nodetool(manager, server, *args)


async def wait_for_compactions_all(manager: ScyllaClusterManager):
    for server in await manager.running_servers():
        await wait_for_compactions(manager, server)


async def count_rows(cql, timeout):
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).request_timeout = timeout
    return (await cql.run_async("select count(*) from ks.cf0;", timeout=timeout))[0][0]


# Ported from scylla-dtest cleanup_test.py::TestCleanup::test_cleanup.
# Cleanup on a single node, after it restarted, keeps all its keys.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cleanup(manager: ScyllaClusterManager, build_mode):
    num_keys = 100000 if build_mode not in DEBUG_MODES else 10000
    timeout = cql_timeout(build_mode, 300)
    [node1], _ = await prepare(manager, 1, num_keys, timeout)

    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_start(node1.server_id)

    await run_nodetool(manager, node1, "cleanup")

    await manager.driver_connect(server=node1)
    assert await count_rows(connect(manager), timeout) == num_keys


# Ported from scylla-dtest cleanup_test.py::TestCleanup::test_cluster_cleanup.
# After a fourth node joins a 3-rack cluster (RF=3), cleanup on all nodes
# keeps all the keys.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cluster_cleanup(manager: ScyllaClusterManager, build_mode):
    num_keys = 100000 if build_mode not in DEBUG_MODES else 10000
    timeout = cql_timeout(build_mode, 300)
    topology = [{"dc": "dc1", "rack": r} for r in ["r1", "r2", "r3"]]
    servers, cql = await prepare(manager, topology, num_keys, timeout, rf=3)

    node4 = await manager.server_add(config=node_config(timeout), property_file={"dc": "dc1", "rack": "r1"})
    await manager.servers_see_each_other(servers + [node4])

    await nodetool_all(manager, "cleanup")

    assert await count_rows(cql, timeout) == num_keys


def list_of_sstables(table_dir):
    """The data files of the table with their disk usage (blocks, not the
    logical size: scylla extends a file before writing it in place), and
    the total."""
    ret = {}
    for file in glob.glob(os.path.join(table_dir, "*-Data.db")):
        try:
            ret[file] = os.stat(file).st_blocks * 512
        except FileNotFoundError as ex:
            logger.info("File %s was not found: %s", file, ex)
    return ret, sum(ret.values())


# Ported from scylla-dtest cleanup_test.py::TestCleanup::test_cleanup_space_amplification.
# After a second node joined and the cluster restarted, the cleanup of the
# first node never takes more than twice the disk space of the table's data
# files, and frees space.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_cleanup_space_amplification(manager: ScyllaClusterManager, build_mode):
    num_keys = 100000 if build_mode not in DEBUG_MODES else 10000
    timeout = cql_timeout(build_mode, 300)
    [node1], _ = await prepare(manager, 1, num_keys, timeout)

    node2 = await manager.server_add(config=node_config(timeout, simple_snitch=True), property_file=DC1_RACK1)
    await manager.servers_see_each_other([node1, node2])
    await nodetool_all(manager, "flush")
    await asyncio.gather(*(manager.server_stop_gracefully(server.server_id) for server in [node1, node2]))
    await asyncio.gather(*[manager.server_start(server.server_id) for server in [node1, node2]])
    await manager.servers_see_each_other([node1, node2])
    table_dir = await get_table_dir(manager, node1, "ks", "cf0")
    sstables_before, size_before = list_of_sstables(table_dir)

    cleanup = asyncio.create_task(run_nodetool(manager, node1, "cleanup"))
    while not cleanup.done():
        sstables_during, size_during = list_of_sstables(table_dir)
        assert size_during <= size_before * 2, \
            f"Temporary space amplification must be less than 2x during cleanup, before=({sstables_before}, {size_before}) and during=({sstables_during}, {size_during})"
        await asyncio.sleep(0)
    await cleanup
    sstables_after, size_after = list_of_sstables(table_dir)
    assert size_before > size_after, \
        f"Cleanup is supposed to decrease disk utilisation, before=({sstables_before}, {size_before}) and after=({sstables_after}, {size_after})"


# Ported from scylla-dtest cleanup_test.py::TestCleanup::test_cluster_restore_no_resurrection.
# A snapshot is taken on two nodes (RF=1), a third node joins and the old
# nodes are cleaned up; all data is deleted and purged, and the snapshots
# are restored with "nodetool refresh". After the third node is removed,
# only the keys that were readable from the old nodes are back: refresh
# loads only the data the node owns (#11933).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_cluster_restore_no_resurrection(manager: ScyllaClusterManager, tmp_path):
    num_keys = 1000
    servers, cql = await prepare(manager, 2, num_keys, rf=1)
    node1, node2 = servers
    await cql.run_async("ALTER TABLE ks.cf0 WITH tombstone_gc = {'mode': 'immediate'}")

    async def existing_keys():
        res = []
        key_exists_query = cql.prepare("SELECT key from ks.cf0 WHERE key = ?")
        key_exists_query.consistency_level = ConsistencyLevel.ONE
        for i in range(num_keys):
            try:
                if await cql.run_async(key_exists_query, [f"k{i}"]):
                    res.append(i)
            except Unavailable:
                pass
        return res

    assert await existing_keys() == list(range(num_keys))

    # The dtests' make_snapshot(): "nodetool snapshot", then a copy of the
    # snapshot directory.
    snapshot_name = "pre_bootstrap"
    snapshot_dirs = {}
    for server in servers:
        await run_nodetool(manager, server, "snapshot", "ks", "-t", snapshot_name)
        table_dir = await get_table_dir(manager, server, "ks", "cf0")
        snapshot_dirs[server.server_id] = tmp_path / f"node{server.server_id}"
        shutil.copytree(os.path.join(table_dir, "snapshots", snapshot_name), snapshot_dirs[server.server_id])

    new_node = await manager.server_add(config=node_config(simple_snitch=True), property_file=DC1_RACK1)
    await manager.servers_see_each_other(servers + [new_node])

    await nodetool_all(manager, "cleanup", "ks")

    await manager.server_stop_gracefully(new_node.server_id)
    existing = await existing_keys()
    await manager.server_start(new_node.server_id)

    await delete_c1c2(cql, num_keys, cf="cf0")
    query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
    assert (await cql.run_async(query))[0][0] == 0

    # Since #14870 tombstones are purged only once the commitlog holds no
    # data they shadow: flush.
    for server in [node1, node2]:
        await run_nodetool(manager, server, "flush")

    await asyncio.sleep(1)
    await nodetool_all(manager, "compact")
    await wait_for_compactions_all(manager)

    # The dtests' restore_snapshot_with_refresh().
    for server in [node1, node2]:
        table_dir = await get_table_dir(manager, server, "ks", "cf0")
        shutil.copytree(snapshot_dirs[server.server_id], os.path.join(table_dir, "upload"), dirs_exist_ok=True)
        await run_nodetool(manager, server, "refresh", "ks", "cf0")

    new_node_hostid = await manager.get_host_id(new_node.server_id)
    await manager.server_stop_gracefully(new_node.server_id)
    await run_nodetool(manager, node1, "removenode", str(new_node_hostid))

    assert await existing_keys() == existing


async def num_partitions_of(manager: ScyllaClusterManager, server, ks, cf):
    """Flush and compact the table and count the partitions of its sstables."""
    await run_nodetool(manager, server, "flush", ks, cf)
    await run_nodetool(manager, server, "compact", ks, cf)
    await wait_for_compactions(manager, server)
    return len(await dump_sstables(manager, server, ks, cf))


# Ported from scylla-dtest cleanup_test.py::TestCleanup::test_no_resurrection_by_replaying_stale_commitlog.
# After a second node joins, the first one cleans up cf0 and is killed; cf1's
# unflushed writes keep its commitlog segments alive. Once the second node
# is gone and the first one restarts, replaying the commitlog does not
# bring back the cleaned up partitions (#4734).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_no_resurrection_by_replaying_stale_commitlog(manager: ScyllaClusterManager):
    num_keys = 10000
    # RF=1, so the keys of node1 are those node2 does not have.
    [node1], cql = await prepare(manager, 1, num_keys, amount_of_tables=2, rf=1)
    ks, cf = "ks", "cf0"

    assert list((await cql.run_async(f"SELECT COUNT(*) FROM {ks}.{cf}"))[0]) == [num_keys]
    await cql.run_async("ALTER TABLE ks.cf0 WITH gc_grace_seconds=0")

    node2 = await manager.server_add(config=node_config(simple_snitch=True), property_file=DC1_RACK1)
    await manager.servers_see_each_other([node1, node2])

    # node1 does not own the tokens of node2 any more.
    await run_nodetool(manager, node1, "cleanup", ks, cf)

    await manager.server_stop(node1.server_id, convict=False)

    expected_num_keys = num_keys - await num_partitions_of(manager, node2, ks, cf)
    await manager.server_stop(node2.server_id, convict=False)

    await manager.server_start(node1.server_id)
    actual_num_keys = await num_partitions_of(manager, node1, ks, cf)

    assert actual_num_keys == expected_num_keys


# Ported from scylla-dtest cleanup_test.py::TestCleanup::test_drop_table_during_cleanup.
# Dropping the keyspace once the cleanup of all its tables started on every
# shard (one table being cleaned up, the others pending) does not fail the
# cleanup, unless with the compaction of the last table stopped by the
# truncate (#12007).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_drop_table_during_cleanup(manager: ScyllaClusterManager, build_mode):
    num_tables = 3
    # Cleanup goes through the tables by their data size, so the later ones
    # get more keys.
    factor = 10000 if build_mode not in DEBUG_MODES else 1000
    table_keys = [factor * i for i in [3, 4, 5]]

    manager.ignore_log_patterns.append("task_manager - Failed force_keyspace_cleanup of ks.cf2: compaction::compaction_stopped_exception[ :]+\\(?Compaction for ks/cf2 was stopped due to: truncate\\)?")

    config = node_config(simple_snitch=True) | {"auto_snapshot": False}
    node1 = await manager.server_add(config=config, property_file=DC1_RACK1, cmdline=["--logger-log-level", "task_manager=debug"])
    cql = connect(manager)
    ks = "ks"
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    for i in range(num_tables):
        cf = f"cf{i}"
        num_keys = table_keys[i]
        flush_every = num_keys // 10
        await create_c1c2_table(cql, ks, cf, compaction="{'class': 'SizeTieredCompactionStrategy', 'max_threshold': 2, 'min_threshold': 2}")
        await run_nodetool(manager, node1, "disableautocompaction", ks, cf)

        for start_key in range(0, num_keys, flush_every):
            await insert_c1c2(cql, range(start_key, min(start_key + flush_every, num_keys)), cf=cf)
            await run_nodetool(manager, node1, "flush", ks, cf)

    log = await manager.server_open_log(node1.server_id)
    mark = await log.mark()

    async def drop_keyspace():
        expected_cleanups = num_tables * SMP
        deadline = time.time() + 600
        while len(await log.grep("Starting force_keyspace_cleanup", from_mark=mark)) < expected_cleanups:
            if time.time() > deadline:
                pytest.fail("Deadline for starting cleanups has elapsed")
        await cql.run_async(f"DROP KEYSPACE {ks}")

    drop = asyncio.create_task(drop_keyspace())

    _, err, rc = await run_nodetool(manager, node1, "cleanup", ks, check=False)
    if rc != 0:
        assert re.search(r"compaction::compaction_stopped_exception[ :]+\(?Compaction for ks/cf2 was stopped due to: truncate\)?", err), err

    await drop
