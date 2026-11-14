#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Operator-facing node operations on vnode-based keyspaces, driven through the REST API.

The tests cover what `nodetool` does for operators:
 - `nodetool cleanup` with no arguments (a local, not cluster-wide, cleanup of all keyspaces),
 - `nodetool rebuild [--force] [<source_dc>]`, with repair-based and with streaming-based rebuild,
 - `nodetool repair` with `--hosts` and `--partitioner-range` (`-pr`), with the result polled
   with the legacy non-blocking repair status call,
 - repair while one of the replicas is down.

All keyspaces use vnodes: cleanup, rebuild and the repair options above apply to vnode keyspaces.
"""

import asyncio
import logging
import time

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

VNODES = "AND tablets = {'enabled': false}"
REBUILD_TIMEOUT = 600


async def insert_rows(cql, ks: str, nr_rows: int, value: str = "", cl=ConsistencyLevel.ALL) -> None:
    stmt = cql.prepare(f"INSERT INTO {ks}.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = cl
    batch = 100
    for start in range(0, nr_rows, batch):
        await asyncio.gather(*[cql.run_async(stmt, [k, f"{k}{value}"])
                               for k in range(start, min(nr_rows, start + batch))])


async def count_rows(cql, ks: str, cl, host=None) -> int:
    stmt = SimpleStatement(f"SELECT count(*) FROM {ks}.t", consistency_level=cl)
    rows = await cql.run_async(stmt, host=host, timeout=120)
    return rows[0].count


async def local_keys(cql, host, ks: str) -> set[int]:
    """The partition keys of {ks}.t which are stored on `host` itself (memtables and sstables)."""
    rows = await cql.run_async(f"SELECT pk, partition_region FROM MUTATION_FRAGMENTS({ks}.t)",
                               host=host, timeout=120)
    return {r.pk for r in rows if r.partition_region == 0}  # partition start


async def wait_for_repair(manager: ScyllaClusterManager, node_ip: str, ks: str, sequence_number: int) -> str:
    """Wait for a repair started with repair_async, checking its state with the legacy status call.

    The legacy GET /storage_service/repair_async/{keyspace}?id= call does not wait, and reports
    SUCCESSFUL for a repair that is not running yet, so the end of the repair is awaited with
    /storage_service/repair_status, and the legacy call is checked against its result.
    """
    status = await manager.api.repair_async_status(node_ip, ks, sequence_number)
    assert status in ("RUNNING", "SUCCESSFUL", "FAILED"), f"unexpected repair status {status}"
    final_status = await manager.api.client.get_json("/storage_service/repair_status", host=node_ip,
                                                     params={"id": str(sequence_number)})
    status = await manager.api.repair_async_status(node_ip, ks, sequence_number)
    assert status == final_status, f"legacy status {status} != status {final_status} of finished repair"
    return status


async def test_local_cleanup_all(manager: ScyllaClusterManager):
    """`nodetool cleanup` with no arguments, i.e. POST /storage_service/cleanup_all?global=false.

    It runs a global cleanup compaction task on the node, marks the node as clean in the
    topology and must not use more than twice the disk space of the table while it runs.
    Ported from dtest's cleanup_test.py (test_cleanup, test_cluster_cleanup and
    test_cleanup_space_amplification).
    """
    nr_rows = 5000
    # The size of an sstable is measured with st_size. Data.db is opened with seastar's
    # sloppy_size option, so its first write extends the file to the 1 MiB size hint,
    # before that much data is written. Each shard may be writing one cleanup output at
    # a time, so allow 1 MiB of such space per shard on top of the 2x bound. This is
    # only the hint while every output is smaller than 1 MiB. Larger outputs are
    # extended by doubling, which the bound does not allow for.
    smp = 2
    sloppy_size_hint = 1 << 20
    servers = await manager.servers_add(3, cmdline=[f"--smp={smp}"], auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 2}} {VNODES}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v text)")
        await insert_rows(cql, ks, nr_rows, value="x" * 100)
        for s in servers:
            await manager.api.keyspace_flush(s.ip_addr, ks)

        # The new node takes over some of the ranges of every old node, so all
        # of them are marked as needing cleanup.
        await manager.server_add(cmdline=[f"--smp={smp}"], property_file={"dc": "dc1", "rack": "rack1"})

        for s in servers:
            log = await manager.server_open_log(s.server_id)
            mark = await log.mark()
            size_before = await manager.server_get_sstables_disk_usage(s.server_id, ks, "t")
            assert 0 < size_before < sloppy_size_hint, \
                f"the outputs of cleanup must be smaller than the sloppy size hint: before={size_before}"

            cleanup = asyncio.create_task(manager.api.cleanup_all(s.ip_addr, global_=False))
            max_size_during = 0
            while not cleanup.done():
                size_during = await manager.server_get_sstables_disk_usage(s.server_id, ks, "t")
                max_size_during = max(max_size_during, size_during)
                assert size_during <= 2 * size_before + smp * sloppy_size_hint, \
                    f"Temporary space amplification during cleanup must be less than 2x: before={size_before} during={size_during}"
                await asyncio.sleep(0.01)
            await cleanup

            size_after = await manager.server_get_sstables_disk_usage(s.server_id, ks, "t")
            logger.info(f"node {s.server_id}: size before cleanup={size_before}, max during={max_size_during}, after={size_after}")
            assert size_after < size_before, \
                f"Cleanup should reduce the disk usage of node {s.server_id}: before={size_before} after={size_after}"
            await log.wait_for("cleanup needed flag is reset by force", from_mark=mark, timeout=60)

            tasks = await manager.api.get_tasks(s.ip_addr, "compaction")
            cleanup_tasks = [t for t in tasks if t["type"] == "global cleanup compaction"]
            assert len(cleanup_tasks) == 1, f"expected one global cleanup compaction task, got {tasks}"
            assert cleanup_tasks[0]["scope"] == "global"
            assert cleanup_tasks[0]["state"] == "done"

        # The node is clean now, so a second cleanup has no flag to reset.
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()
        await manager.api.cleanup_all(servers[0].ip_addr, global_=False)
        await log.wait_for("cannot reset cleanup flag when it is clean", from_mark=mark, timeout=60)

        # `nodetool upgradesstables` also filters out data the node does not own,
        # using the vnode ranges of the node.
        await manager.api.keyspace_upgrade_sstables(servers[0].ip_addr, ks)

        assert await count_rows(cql, ks, ConsistencyLevel.ALL) == nr_rows


@pytest.mark.parametrize("rf", [1, 2])
async def test_rebuild_source_dc_safe_unsafe(manager: ScyllaClusterManager, rf: int):
    """Rebuild a node of dc2, after its data was lost, from different sources.

    dc2 has two nodes. Rebuilding a node counts it as lost, so with rf=1 dc2 is not
    a safe source, and the rebuild is refused unless forced. Forcing it loses the data
    that only the rebuilt node had.
    Ported from dtest's repair_based_node_operations_test.py::test_safe_unsafe_rebuild.
    It also rebuilds with streaming (rebuild not in allowed_repair_based_node_ops).
    """
    nr_rows = 1000
    config = {
        "enable_repair_based_node_ops": True,
        "allowed_repair_based_node_ops": "bootstrap,replace,removenode,decommission,rebuild",
    }
    dc1 = await manager.servers_add(2, config=config, property_file={"dc": "dc1", "rack": "r1"})
    dc2 = await manager.servers_add(2, config=config, property_file={"dc": "dc2", "rack": "r1"})
    all_servers = dc1 + dc2
    cql = manager.get_cql()
    node = dc2[0]

    async def get_host(server: ServerInfo):
        return (await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60))[0]

    # The audit keyspace is created when the first node starts, with replicas in dc1 only.
    # Rebuilding from dc2 is unsafe for it, so give it replicas in both datacenters,
    # like the test keyspace.
    if await cql.run_async("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'audit'"):
        await cql.run_async(f"ALTER KEYSPACE audit WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}, 'dc2': {rf}}}")

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}, 'dc2': {rf}}} {VNODES}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v text)")
        await insert_rows(cql, ks, nr_rows)
        for s in all_servers:
            await manager.api.keyspace_flush(s.ip_addr, ks)

        baseline = await local_keys(cql, await get_host(node), ks)
        logger.info(f"node {node.server_id} has {len(baseline)} keys out of {nr_rows}")
        assert 0 < len(baseline) <= nr_rows
        if rf == 2:
            assert len(baseline) == nr_rows

        async def wipe(config_options: dict | None = None) -> None:
            """Lose the data of the table on the rebuilt node."""
            await manager.server_stop_gracefully(node.server_id)
            await manager.server_wipe_sstables(node.server_id, ks, "t")
            if config_options:
                await manager.server_update_config(node.server_id, config_options=config_options)
            await manager.server_start(node.server_id)
            await manager.servers_see_each_other(all_servers)
            assert await local_keys(cql, await get_host(node), ks) == set()

        async def rebuild(source_dc: str | None = None, force: bool = False) -> None:
            logger.info(f"rebuild node {node.server_id} source_dc={source_dc} force={force}")
            await manager.api.rebuild_node(node.ip_addr, timeout=REBUILD_TIMEOUT, source_dc=source_dc, force=force)

        async def check_restored() -> None:
            assert await local_keys(cql, await get_host(node), ks) == baseline

        log = await manager.server_open_log(node.server_id)

        # dc1 lost no node, so it is a safe source.
        await wipe()
        await rebuild(source_dc="dc1")
        await check_restored()

        # dc2 lost the rebuilt node. With rf=1 the ranges of the rebuilt node
        # have no other replica in dc2.
        await wipe()
        mark = await log.mark()
        if rf == 1:
            with pytest.raises(HTTPError, match="rebuild failed"):
                await rebuild(source_dc="dc2")
            await log.wait_for("It is advised to select another datacenter", from_mark=mark, timeout=60)
            await log.wait_for("use the --force option to enforce using source_dc=dc2", from_mark=mark, timeout=60)
            # The refused rebuild restored nothing, so the next rebuild can run
            # without wiping again. It retries the failed rebuild of this node.
            assert await local_keys(cql, await get_host(node), ks) == set()
        else:
            await rebuild(source_dc="dc2")
            await check_restored()
            await wipe()

        # Without source_dc all nodes are used.
        mark = await log.mark()
        await rebuild()
        await check_restored()
        if rf == 1:
            await log.wait_for("retry streaming after previous attempt failed", from_mark=mark, timeout=60)

        # --force makes the node use dc2 anyway. With rf=1 the data which was
        # only on the rebuilt node is lost.
        await wipe()
        mark = await log.mark()
        await rebuild(source_dc="dc2", force=True)
        host = await get_host(node)
        if rf == 1:
            await log.wait_for("using source_dc anyway according to the force option", from_mark=mark, timeout=60)
            assert await local_keys(cql, host, ks) == set()
            assert await count_rows(cql, ks, ConsistencyLevel.LOCAL_ONE, host=host) == nr_rows - len(baseline)
        else:
            await check_restored()
            assert await count_rows(cql, ks, ConsistencyLevel.LOCAL_ONE, host=host) == nr_rows

        # Rebuild with streaming instead of repair.
        await wipe(config_options={"allowed_repair_based_node_ops": "bootstrap,replace,removenode,decommission"})
        mark = await log.mark()
        await rebuild(source_dc="dc1")
        await log.wait_for("streaming for rebuild successful", from_mark=mark, timeout=60)
        await check_restored()
        assert await count_rows(cql, ks, ConsistencyLevel.ALL) == nr_rows


async def test_repair_async_hosts_primary_range_and_status(manager: ScyllaClusterManager):
    """`nodetool repair` options of vnode keyspaces, with the legacy repair status call.

    Covers --hosts, --partitioner-range (primaryRange), an empty keyspace and an unknown
    repair id. Ported from dtest's repair_additional_test.py (repair_async and repair_status),
    and from the `repair -pr` calls in migration_test.py and update_cluster_layout_tests.py.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    ip0, ip1, ip2 = (s.ip_addr for s in servers)
    log = await manager.server_open_log(servers[0].server_id)
    replication = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 3}} {VNODES}"
    async with new_test_keyspace(manager, replication) as ks, \
               new_test_keyspace(manager, replication) as empty_ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v text)")
        await insert_rows(cql, ks, 100)

        # Repair only with node 1.
        mark = await log.mark()
        seq = await manager.api.repair_async(ip0, ks, {"hosts": f"{ip0},{ip1}"})
        assert await wait_for_repair(manager, ip0, ks, seq) == "SUCCESSFUL"
        repair_log = await log.grep(rf"repair\[.*\]: starting user-requested repair for keyspace {ks}, repair id {seq}, options .*hosts",
                                    from_mark=mark)
        assert repair_log, "the repair should be logged with its hosts option"

        # Primary range only (nodetool repair -pr).
        mark = await log.mark()
        seq = await manager.api.repair_async(ip0, ks, {"primaryRange": "true"})
        assert await wait_for_repair(manager, ip0, ks, seq) == "SUCCESSFUL"
        assert await log.grep("primary-range repair", from_mark=mark)

        # primaryRange=false is a full repair.
        mark = await log.mark()
        seq = await manager.api.repair_async(ip0, ks, {"primaryRange": "false"})
        assert await wait_for_repair(manager, ip0, ks, seq) == "SUCCESSFUL"
        assert not await log.grep("primary-range repair", from_mark=mark)

        # The node running the repair must be one of the hosts.
        mark = await log.mark()
        try:
            seq = await manager.api.repair_async(ip0, ks, {"hosts": f"{ip1},{ip2}"})
        except HTTPError as e:
            assert "The current host must be part of the repair" in e.message
        else:
            assert await wait_for_repair(manager, ip0, ks, seq) == "FAILED"
            await log.wait_for("The current host must be part of the repair", from_mark=mark, timeout=60)

        # A keyspace without tables has nothing to repair.
        mark = await log.mark()
        seq = await manager.api.repair_async(ip0, empty_ks)
        assert await wait_for_repair(manager, ip0, empty_ks, seq) == "SUCCESSFUL"
        await log.wait_for("completed successfully: no tables to repair", from_mark=mark, timeout=60)

        # The status of a repair which was never started.
        with pytest.raises(HTTPError, match="unknown repair id 99999"):
            await manager.api.repair_async_status(ip0, ks, 99999)

        assert await count_rows(cql, ks, ConsistencyLevel.ALL) == 100


async def test_repair_with_replica_down(manager: ScyllaClusterManager):
    """Repair while one of the replicas is down.

    The repair skips the down replica, so it is partial and fails. With tombstone_gc
    mode 'repair' the repair also skips flushing hints and batchlog on the down node.
    Once the node is back, the repair succeeds.
    Ported from dtest's compaction_test.py::test_delete_tombstone_gc_node_down and
    wide_rows_test.py::test_large_row_with_static_cell.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    ip0 = servers[0].ip_addr
    down = servers[2]
    log = await manager.server_open_log(servers[0].server_id)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 3}} {VNODES}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v text) WITH tombstone_gc = {{'mode': 'repair'}}")
        await insert_rows(cql, ks, 100)

        await manager.server_stop_gracefully(down.server_id)
        await manager.server_not_sees_other_server(ip0, down.ip_addr)

        mark = await log.mark()
        seq = await manager.api.repair_async(ip0, ks)
        assert await wait_for_repair(manager, ip0, ks, seq) == "FAILED"
        await log.wait_for("Skipped sending repair_flush_hints_batchlog due to nodes_down", from_mark=mark, timeout=60)
        await log.wait_for(f"keyspace={ks}, table=t, .*status=partial", from_mark=mark, timeout=60)

        await manager.server_start(down.server_id)
        await manager.servers_see_each_other(servers)

        mark = await log.mark()
        seq = await manager.api.repair_async(ip0, ks)
        assert await wait_for_repair(manager, ip0, ks, seq) == "SUCCESSFUL"
        assert not await log.grep("status=partial", from_mark=mark)
        assert await count_rows(cql, ks, ConsistencyLevel.ALL) == 100
