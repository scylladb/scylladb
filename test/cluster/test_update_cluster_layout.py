#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Changes of the cluster layout ported from scylla-dtest
update_cluster_layout_tests.py: growing and shrinking a cluster, bootstraps
of two nodes started together and of nodes receiving large rows,
decommissions (of a whole rack, of a node whose data was deleted, of a node
killed while it streams), removenode with dead nodes ignored or not and
with the coordinator or a streaming peer killed, a bootstrapping node
killed and removed from gossip, and a node coming back with a new IP.
Also: reads, writes and schema changes while a node joins or leaves, nodes
killed while a node joins or leaves, removenode and decommission aborted by
killing a node (with no garbage left in group0 and the token ring), the
latest copy of the data picked by repair-based node operations, counters
updated with nodes down, and a node stopped early in its restart.

As in the dtests, the keyspaces use vnodes, hinted handoff is off and the
commitlog is in batch mode; data is checked on each node alone, with the
other nodes stopped.
"""
import asyncio
import logging
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut, Unavailable, WriteTimeout
from cassandra.cluster import EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable, default_lbp_factory
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import ConstantSpeculativeExecutionPolicy, FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test.cluster.dtest_helpers import cql_one, exclusive_one, DTEST_CONFIG, NO_HINTS_BATCH_COMMITLOG, C1C2, check_rows_on_node, create_cf, \
    create_ks, flush_all, insert_c1c2, nodetool, nodetool_status, query_c1c2, topology, wait_for_nodes_status
from test.cluster.util import BANNED_NOTIFICATION, cassandra_stress, get_coordinator_host_ids, stop_servers_gracefully, \
    wipe_server_data
from test.cqlpy.util import run_cqlsh_at
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {**DTEST_CONFIG, **NO_HINTS_BATCH_COMMITLOG}
BOOTSTRAP_START_LOG_PAT = r"raft_topology - start streaming"


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)



async def start_no_wait(manager: ScyllaClusterManager, server):
    """Starts a node added with start=False without waiting for it to join,
    like ccm's node.start(no_wait=True); returns its log and the log mark
    from before the start."""
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_start(server.server_id, expected_server_up_state=ServerUpState.PROCESS_STARTED,
                               connect_driver=False)
    return log, mark


async def count_from(manager: ScyllaClusterManager, server, query: str) -> int:
    cql = await exclusive_one(manager, server)
    return len(list(cql.execute(query)))


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("iterations,node_count,rf", [(3, 1, 1), (2, 3, 1), (3, 1, 2), (2, 3, 2)])
async def test_iterative_add_decommission_nodes(manager: ScyllaClusterManager, iterations, node_count, rf):
    """
    A cluster with one node in each of RF racks grows by node_count nodes in
    one rack, which are then decommissioned but the last one, in a loop;
    in the end the first node is decommissioned too. No row is lost.
    """
    servers = await manager.servers_add(rf, config=CONFIG, property_file=topology({f"rack{r + 1}": 1 for r in range(rf)}))
    node1 = servers[0]
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)

    query = SimpleStatement("SELECT key FROM ks.cf limit 3000", consistency_level=ConsistencyLevel.ALL)
    added = []
    node_i = None
    for iteration in range(1, iterations + 1):
        rack = f"rack{iteration % rf + 1}"
        for i in range(1, node_count + 1):
            node_i = await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": rack})
            added.append((node_i, rack))
            session_i = await exclusive_one(manager, node_i)
            base = iteration * 100000 + i * 2000
            insert_c1c2(session_i, keys=range(base, base + 100), consistency=ConsistencyLevel.ALL)
        expected = iteration * node_count * 100 + 1000
        cql, _ = await manager.get_ready_cql(await manager.running_servers())
        assert len(list(cql.execute(query))) == expected

        # Decommission the nodes of the rack, but the last one added.
        running = {s.server_id for s in await manager.running_servers()}
        in_rack = [s for s, r in [(s, f"rack{n + 1}") for n, s in enumerate(servers)] + added
                   if r == rack and s != node_i and s.server_id != node1.server_id and s.server_id in running]
        for s in in_rack:
            await manager.decommission_node(s.server_id)
        assert await count_from(manager, node_i, "SELECT key FROM ks.cf limit 3000") == expected

    await manager.decommission_node(node1.server_id)
    assert await count_from(manager, node_i, "SELECT key FROM ks.cf limit 3000") == iterations * node_count * 100 + 1000


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_decommission_with_multi_rack(manager: ScyllaClusterManager):
    """Nodes of a rack are decommissioned one by one, emptying the rack;
    every row is still there."""
    servers = await manager.servers_add(5, config=DTEST_CONFIG,
                                        property_file=topology({"RACK1": 2, "RACK2": 1, "RACK3": 2}, dc="DC1"))
    node1, _, _, node4, node5 = servers
    cql = await exclusive_one(manager, node1)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2)
    insert_c1c2(cql, keys=range(500), consistency=ConsistencyLevel.ALL)
    await manager.decommission_node(node5.server_id)
    await manager.decommission_node(node4.server_id)
    cql = await exclusive_one(manager, node1)
    assert cql.execute("SELECT count(*) FROM ks.cf").one()[0] == 500


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_case", [0, 1, 2], ids=["case_0", "case_1", "case_2"])
async def test_simple_add_two_nodes_in_parallel(manager: ScyllaClusterManager, test_case: int):
    """
    Two nodes start bootstrapping into a one-node cluster at about the same
    time: the second starts right after the first one began bootstrapping
    (case 0), 5 to 30 seconds later (case 1), or up to 30 seconds after
    the first one started streaming (case 2). Both join, and each node then
    has all the data.
    """
    location = {"dc": "datacenter1", "rack": "rack1"}
    node1 = await manager.server_add(config=CONFIG, property_file=location)
    cql = cql_one(manager)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    num_keys = 500000
    insert_c1c2(cql, keys=range(num_keys), consistency=ConsistencyLevel.ONE)

    node2 = await manager.server_add(start=False, config=CONFIG, property_file=location)
    node3 = await manager.server_add(start=False, config=CONFIG, property_file=location)
    log2, mark2 = await start_no_wait(manager, node2)
    await log2.wait_for(BOOTSTRAP_START_LOG_PAT, from_mark=mark2)
    mark = await log2.mark()
    if test_case == 1:
        await asyncio.sleep(5 + random.random() * 25)
    elif test_case == 2:
        try:
            await log2.wait_for(BOOTSTRAP_START_LOG_PAT, from_mark=mark, timeout=30)
        except TimeoutError:
            pass
        await asyncio.sleep(random.random() * 30)

    log3, mark3 = await start_no_wait(manager, node3)
    await log3.wait_for("initialization completed", from_mark=mark3)
    nodes = [node1, node2, node3]
    for s in nodes:
        log = await manager.server_open_log(s.server_id)
        await log.wait_for("Starting listening for CQL clients")
    await manager.servers_see_each_other(nodes)

    node2_session = await exclusive_one(manager, node2)
    insert_c1c2(node2_session, keys=range(num_keys, 2 * num_keys), consistency=ConsistencyLevel.THREE)
    for s in nodes:
        await check_rows_on_node(manager, s, 2 * num_keys)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_decommission_node_2(manager: ScyllaClusterManager):
    """
    Rows deleted with tombstone_gc=immediate do not come back when the node
    that received part of them by bootstrap, and compacted them away, is
    decommissioned after a cleanup of the first node: the row cache entries
    of the ranges the first node gave away were invalidated.
    """
    location = {"dc": "datacenter1", "rack": "rack1"}
    node1 = await manager.server_add(config=CONFIG, property_file=location)
    cql = await exclusive_one(manager, node1)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0, tombstone_gc="immediate")
    await nodetool(manager, node1, "flush")
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)
    await nodetool(manager, node1, "flush")

    node2 = await manager.server_add(config=CONFIG, property_file=location)
    await nodetool(manager, node1, "cleanup")
    await manager.servers_see_each_other([node1, node2])
    cql = await exclusive_one(manager, node1)

    assert len(list(cql.execute("SELECT * FROM ks.cf limit 2000;"))) == 1000
    for i in range(0, 1000, 100):
        cql.execute("DELETE from ks.cf where key in ('k%s');" % "','k".join(str(x) for x in range(i, i + 100)))
    assert list(cql.execute("SELECT * FROM ks.cf limit 2000;")) == []

    await flush_all(manager, [node1, node2])
    await nodetool(manager, node2, "compact")
    await manager.server_restart(node2.server_id)
    await manager.decommission_node(node2.server_id)
    await nodetool(manager, node1, "flush")
    cql = await exclusive_one(manager, node1)
    assert list(cql.execute("SELECT * FROM ks.cf")) == []


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_kill_node_while_decommissioning(manager: ScyllaClusterManager):
    """
    A node killed while it streams its data during decommission makes the
    decommission fail; started again, it is back in the cluster with its
    data. Decommission runs without the small table optimization of
    repair-based node operations, as in the dtest. As in the dtest,
    streaming is throttled to 1 MB/s and node2 holds about 60 MB, so the
    decommission is still streaming when node2 is killed; streaming is
    unthrottled before node2 starts again, and the topology is quiesced at
    the end, so no tablet migration back to node2 is cut short.
    """
    config = {**CONFIG, "enable_small_table_optimization_for_rbno": False, "stream_io_throughput_mb_per_sec": 1}
    servers = await manager.servers_add(3, config=config, property_file=topology({"rack1": 3}),
                                        cmdline=["--logger-log-level", "stream_session=debug",
                                                 "--logger-log-level", "stream_blob=debug"])
    node1, node2, node3 = servers
    cql = cql_one(manager)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    # ~60 MB, well over the 10 MB seastar lets through before the throttle applies
    values = ["x" * 3000] * 10000
    insert_c1c2(cql, keys=range(10000), consistency=ConsistencyLevel.ONE, c1_values=values, c2_values=values)

    manager.ignore_log_patterns += [r"seastar::rpc::closed_error", r"Failed to handle STREAM_MUTATION_FRAGMENTS",
                                    r"Decommission failed", r"decommission.*failed"]
    log = await manager.server_open_log(node2.server_id)
    mark = await log.mark()
    decommission = asyncio.create_task(nodetool(manager, node2, "decommission", check=False))
    await log.wait_for(r"stream_blob - stream_sstables\[|Beginning stream session|sync data for keyspace=.*, status=started",
                       from_mark=mark)
    await manager.server_stop(node2.server_id, convict=False)
    assert (await asyncio.wait_for(decommission, 120)).returncode != 0

    # Wait for the coordinator to fail the decommission before restarting node2.
    cql = await exclusive_one(manager, node1)
    deadline = time.time() + 120
    while not any(r.done and r.error for r in cql.execute(
            "SELECT done, error FROM system.topology_requests WHERE request_type='leave' ALLOW FILTERING")):
        assert time.time() < deadline, "node2 decommission did not fail"
        await asyncio.sleep(1)

    # Unthrottle streaming, so the rejoin and the tablet migrations back to node2 stay fast.
    for node in (node1, node3):
        cqln = await exclusive_one(manager, node)
        cqln.execute("UPDATE system.config SET value = '0' WHERE name = 'stream_io_throughput_mb_per_sec'")
    await manager.server_update_config(node2.server_id, "stream_io_throughput_mb_per_sec", 0)
    await manager.server_start(node2.server_id)
    cql2 = await exclusive_one(manager, node2)
    assert len(list(cql2.execute("SELECT * FROM ks.cf", timeout=60))) == 10000
    await wait_for_nodes_status(manager, node1, ["UN", "UN", "UN"], timeout=0)
    await manager.api.quiesce_topology(node1.ip_addr)


async def prepare_removenode_cluster(manager: ScyllaClusterManager, racks: dict[str, int], rf: int, keys: int):
    servers = await manager.servers_add(sum(racks.values()), config=CONFIG, property_file=topology(racks))
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(keys), consistency=ConsistencyLevel.ALL)
    return servers


REMOVENODE_REJECTED = "removenode: Rejected removenode operation for node"


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("removenode_method,ignore_two_nodes", [("api", False), ("api", True),
                                                                ("nodetool", False), ("nodetool", True)])
async def test_simple_removenode_3(manager: ScyllaClusterManager, removenode_method: str, ignore_two_nodes: bool):
    """
    removenode of a dead node of rack1 while other nodes of rack2 (and
    rack3) are down is rejected, and succeeds once they are ignored; through
    the REST API or nodetool.
    """
    servers = await prepare_removenode_cluster(manager, {"rack1": 3, "rack2": 2, "rack3": 2}, rf=3, keys=1000)
    node1, node2, _, node4, node5, *_ = servers
    node2_hostid = await manager.get_host_id(node2.server_id)
    ignore = [node2_hostid]
    await manager.server_stop_gracefully(node2.server_id)
    cql = await exclusive_one(manager, node1)
    assert len(list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.TWO)))) == 1000

    ignore.append(await manager.get_host_id(node5.server_id))
    await manager.server_stop_gracefully(node5.server_id)
    if ignore_two_nodes:
        ignore.append(await manager.get_host_id(node4.server_id))
        await manager.server_stop_gracefully(node4.server_id)
    manager.ignore_log_patterns += ["raft_topology - Removenode failed. See earlier errors",
                                    "raft_topology - raft_topology_cmd.*failed with: raft::request_aborted",
                                    "Rejected removenode operation"]

    async def removenode(ignore_hostids):
        if removenode_method == "api":
            params = {"host_id": node2_hostid}
            if ignore_hostids:
                params["ignore_nodes"] = ",".join(ignore_hostids)
            return await manager.api.client.post("/storage_service/remove_node", params=params, host=node1.ip_addr)
        options = ["--ignore-dead-nodes", ",".join(ignore_hostids)] if ignore_hostids else []
        return await nodetool(manager, node1, "removenode", *options, node2_hostid)

    # Rejected while node5 is down and not ignored.
    with pytest.raises(Exception) as exc:
        await removenode([])
    if removenode_method == "nodetool":
        assert REMOVENODE_REJECTED in str(exc.value)
    # Succeeds with the dead nodes ignored.
    await removenode(ignore)


async def removenode_and_kill(manager: ScyllaClusterManager, to_kill_coordinator: bool):
    """
    On five nodes in three racks with RF=3, removenode of a dead node is
    interrupted by killing the removenode coordinator, or another node,
    once it starts streaming. The operation is rolled back; the removed node
    is banned anyway, and removenode run again succeeds. In the end, every
    node runs a repair.
    """
    manager.ignore_log_patterns += [r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception",
                                    "raft_topology - Removenode failed. See earlier errors",
                                    r"[Rr]epair.*mandatory neighbor=.* is not alive",
                                    r"raft_topology - raft_topology_cmd stream_ranges failed with: .*repair_reason=removenode",
                                    BANNED_NOTIFICATION]
    servers = await prepare_removenode_cluster(manager, {"rack1": 2, "rack2": 1, "rack3": 2}, rf=3, keys=10000)
    node1, node2, node3, node4, node5 = servers
    node2_hostid = await manager.get_host_id(node2.server_id)
    await manager.server_stop_gracefully(node2.server_id)
    logs = {s.server_id: await manager.server_open_log(s.server_id) for s in servers}
    marks = {s.server_id: await logs[s.server_id].mark() for s in servers}

    async def kill():
        victim = node1 if to_kill_coordinator else node5
        await logs[victim.server_id].wait_for(BOOTSTRAP_START_LOG_PAT, from_mark=marks[victim.server_id])
        await manager.server_stop(victim.server_id, convict=False)

    killer = asyncio.create_task(kill())
    try:
        await manager.api.client.post("/storage_service/remove_node", params={"host_id": node2_hostid}, host=node1.ip_addr)
    except Exception as e:  # noqa: BLE001
        logger.info("removenode failed, as expected when node1 is killed: %s", e)
    await killer

    alive = [node3, node4]
    if to_kill_coordinator:
        alive.append(node5)
        # Find the new topology coordinator and wait for it to roll back.
        new_coordinator = None
        deadline = time.time() + 60
        while new_coordinator is None:
            for s in alive:
                if await logs[s.server_id].grep("raft_topology - start topology coordinator fiber", from_mark=marks[s.server_id]):
                    new_coordinator = s
                    break
            else:
                assert time.time() < deadline, "no new topology coordinator"
                await asyncio.sleep(0.5)
        await logs[new_coordinator.server_id].wait_for("raft_topology - complete rollback", from_mark=marks[new_coordinator.server_id],
                                                       timeout=60)
        await manager.server_start(node1.server_id)
        alive.append(node1)
    else:
        alive.append(node1)
        await manager.server_start(node5.server_id)
        alive.append(node5)

    # node2 is banned from the cluster even though removenode failed.
    await manager.server_start(node2.server_id, expected_error=BANNED_NOTIFICATION)
    assert (await nodetool(manager, node2, "rebuild", check=False)).returncode != 0
    # It can be removed again.
    await nodetool(manager, node1, "removenode", node2_hostid)

    await nodetool_status(manager, node3)
    for s in alive:
        await nodetool(manager, s, "repair")
    await nodetool_status(manager, node3)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_removenode_4(manager: ScyllaClusterManager):
    """removenode with its coordinator killed in the middle."""
    await removenode_and_kill(manager, to_kill_coordinator=True)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_removenode_5(manager: ScyllaClusterManager):
    """removenode with a streaming peer killed in the middle."""
    await removenode_and_kill(manager, to_kill_coordinator=False)


def gossip_heartbeat(output: str, ip: str) -> int:
    """The heartbeat of the given node in `nodetool gossipinfo` output."""
    current = None
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("/"):
            current = line[1:]
        elif current == ip and line.startswith("heartbeat:"):
            return int(line.split(":")[1])
    raise AssertionError(f"no heartbeat of {ip} in {output}")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_remove_node_from_gossip(manager: ScyllaClusterManager):
    """
    A node killed while bootstrapping is removed from the cluster, and the
    gossip heartbeat of the other nodes keeps advancing afterwards: the
    gossip on_remove callbacks do not deadlock.
    """
    manager.ignore_log_patterns += [r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception",
                                    "Startup failed"]
    # The dtests' default ring_delay_ms, 10 s (test.py's is 0): as in the
    # dtest, the rollback of node4's bootstrap waits for it before node4 is
    # removed.
    config = {**CONFIG, "ring_delay_ms": 10000}
    servers = await manager.servers_add(3, config=config, property_file=topology({"rack1": 2, "rack2": 1}))
    node1 = servers[0]
    cql = cql_one(manager)
    create_ks(cql, "ks", 2)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(2000), consistency=ConsistencyLevel.TWO)

    node4 = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack2"},
                                     cmdline=["--logger-log-level", "stream_session=debug"])
    log4, mark4 = await start_no_wait(manager, node4)
    await log4.wait_for(BOOTSTRAP_START_LOG_PAT, r"Beginning stream session|sync data for keyspace=ks, status=started",
                        from_mark=mark4)
    await manager.server_stop(node4.server_id, convict=False)

    log1 = await manager.server_open_log(node1.server_id)
    await log1.wait_for(rf"Finished to force remove node ({node4.ip_addr}|[0-9a-f-]{{36}})", timeout=120)

    heartbeat_1 = gossip_heartbeat((await nodetool(manager, node1, "gossipinfo")).stdout, node1.ip_addr)
    await asyncio.sleep(3)
    heartbeat_2 = gossip_heartbeat((await nodetool(manager, node1, "gossipinfo")).stdout, node1.ip_addr)
    assert heartbeat_2 > heartbeat_1


async def add_node_and_check_rows(manager: ScyllaClusterManager, node1, nr_rows: int) -> None:
    """Bootstraps a node into rack2 and checks that each node alone has all the rows."""
    node2 = await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack2"})
    await check_rows_on_node(manager, node2, nr_rows)
    await check_rows_on_node(manager, node1, nr_rows)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_add_node_with_large_partition2(manager: ScyllaClusterManager):
    """A node bootstraps 100 rows of 250 columns of 1 KB each, with RF=2."""
    nr_columns, nr_rows, column_size = 250, 100, 1024
    node1 = await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = cql_one(manager)
    create_ks(cql, "ks", 2)
    create_cf(cql, "ks.cf", {f"c{i}": "text" for i in range(1, nr_columns + 1)}, read_repair=0.0)
    # The dtests' insert_c1cn().
    columns = [f"c{i}" for i in range(1, nr_columns + 1)]
    stmt = cql.prepare(f"INSERT INTO ks.cf (key, {', '.join(columns)}) VALUES (?, {', '.join('?' for _ in columns)}) ")
    stmt.consistency_level = ConsistencyLevel.ONE
    values = ["x" * column_size] * nr_columns
    await asyncio.gather(*[cql.run_async(stmt, [f"k{k}", *values]) for k in range(nr_rows)])
    await add_node_and_check_rows(manager, node1, nr_rows)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_add_node_with_large_partition3(manager: ScyllaClusterManager):
    """A node bootstraps rows alternating 10 KB + 300 KB and 1 KB + 3 KB
    values, with RF=2."""
    node1 = await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = cql_one(manager)
    create_ks(cql, "ks", 2)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    nr_rows = 100
    c1s = ["a" * 1024 * 10 if n % 2 == 0 else "c" * 1024 for n in range(nr_rows)]
    c2s = ["b" * 1024 * 300 if n % 2 == 0 else "d" * 1024 * 3 for n in range(nr_rows)]
    insert_c1c2(cql, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, c1_values=c1s, c2_values=c2s, concurrency=10)
    await add_node_and_check_rows(manager, node1, nr_rows)


async def check_peer_and_local_table(manager: ScyllaClusterManager, servers) -> None:
    """Every node has the same system.peers + system.local, with its own
    host ID and address in system.local."""
    for attempt in range(20):
        try:
            peers_and_local = []
            for s in servers:
                await nodetool_status(manager, s)
                cql = await exclusive_one(manager, s)
                peers = [list(r) for r in cql.execute("SELECT host_id, peer FROM system.peers")]
                local = [list(r) for r in cql.execute("SELECT host_id, broadcast_address FROM system.local")]
                peers_and_local.append(str(sorted(peers + local, key=str)))
                assert len(peers) == len(servers) - 1
                assert len(local) == 1
                assert str(local[0][0]) == str(await manager.get_host_id(s.server_id))
                assert str(local[0][1]) == s.ip_addr
            assert len(set(peers_and_local)) == 1
            return
        except AssertionError:
            if attempt == 19:
                raise
            await asyncio.sleep(1)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("rf", [2, 3], ids=["rack=rf=2", "rack=rf=3"])
async def test_change_node_ip(manager: ScyllaClusterManager, rf):
    """
    A node restarted with a new IP address is seen with that address and its
    host ID by every node, in nodetool status and system.peers, and the data
    of RF=1 and RF=<racks> keyspaces is still there.
    """
    servers = await manager.servers_add(rf, config=CONFIG, property_file=topology({f"rack{r + 1}": 1 for r in range(rf)}))
    node1, target = servers[0], servers[-1]
    cl = {2: ConsistencyLevel.TWO, 3: ConsistencyLevel.THREE}[rf]
    cql = cql_one(manager)
    create_ks(cql, "ks1", 1)
    create_cf(cql, "ks1.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, ks="ks1", keys=range(1000), consistency=ConsistencyLevel.ONE)
    create_ks(cql, f"ks{rf}", rf)
    create_cf(cql, f"ks{rf}.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, ks=f"ks{rf}", keys=range(1000), consistency=cl)

    await manager.server_stop_gracefully(target.server_id)
    new_ip = await manager.server_change_ip(target.server_id)
    await manager.server_start(target.server_id)
    target_hostid = str(await manager.get_host_id(target.server_id))
    servers = await manager.running_servers()
    for s in servers:
        for n in await nodetool_status(manager, s):
            if n["address"] == new_ip:
                assert n["host_id"] == target_hostid
    await check_peer_and_local_table(manager, servers)

    cql = await exclusive_one(manager, node1)
    for k in range(1000):
        query_c1c2(cql, k, ConsistencyLevel.ONE, ks="ks1")
        query_c1c2(cql, k, cl, ks=f"ks{rf}")


# The dtests below are ported one test each, step by step. Their clusters
# use vnodes and the dtests' quick failure detector; the options each dtest
# sets come on top.
VNODES = {"tablets_mode_for_new_keyspaces": "disabled", "phi_convict_threshold": 5}
# set_configuration_options(..., batch_commitlog=True)
BATCH_COMMITLOG = {"commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 5}
# The dtests' default_config_options().
DEFAULT_OPTIONS = {"hinted_handoff_enabled": False, "enable_sstable_key_validation": True}
# The dtests' pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False).
NO_SMALL_TABLE_OPTIMIZATION = {"enable_small_table_optimization_for_rbno": False}
STREAM_SESSION_DEBUG = ["--logger-log-level", "stream_session=debug"]
SKIP_DEV_DEBUG = pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")


def rbno_options(enable: bool, ops: list[str] | None = None) -> dict:
    """The dtests' repair_based_node_ops_config_options()."""
    options = {**DEFAULT_OPTIONS, "enable_repair_based_node_ops": enable}
    if enable:
        options["allowed_repair_based_node_ops"] = ",".join(ops or ["bootstrap", "replace", "removenode", "decommission", "rebuild"])
    return options


def generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=1, dc_name_prefix="datacenter", rack_name_prefix="rack"):
    """The dtests' generate_cluster_topology()."""
    return {f"{dc_name_prefix}{i + 1}": {f"{rack_name_prefix}{j + 1}": nodes_per_rack for j in range(rack_num)}
            for i in range(dc_num)}


async def populate(manager: ScyllaClusterManager, dcs: dict[str, dict[str, int]], config: dict, cmdline=None):
    """ccm's cluster.populate({dc: {rack: nodes}}).start(), nodes in ccm's order."""
    property_files = [{"dc": dc, "rack": rack} for dc, racks in dcs.items() for rack, n in racks.items() for _ in range(n)]
    return await manager.servers_add(len(property_files), config=config, cmdline=cmdline, property_file=property_files)


async def others_of(manager: ScyllaClusterManager, server):
    return [s for s in await manager.running_servers() if s.server_id != server.server_id]


async def stop_node(manager: ScyllaClusterManager, server, gently: bool = True, wait_other_notice: bool = False):
    """ccm's node.stop(): graceful, or a kill with gently=False; with
    wait_other_notice, until the other running nodes see the node down."""
    others = await others_of(manager, server)
    if gently:
        await manager.server_stop_gracefully(server.server_id)
    else:
        await manager.server_stop(server.server_id, convict=False)
    if wait_other_notice:
        for s in others:
            await manager.server_not_sees_other_server(s.ip_addr, server.ip_addr)


async def start_node(manager: ScyllaClusterManager, server, wait_other_notice: bool = False, **kwargs):
    """ccm's node.start(); with wait_other_notice, until the other running
    nodes see the node up."""
    await manager.server_start(server.server_id, **kwargs)
    if wait_other_notice:
        for s in await others_of(manager, server):
            await manager.server_sees_other_server(s.ip_addr, server.ip_addr)


async def stop_if_running(manager: ScyllaClusterManager, server):
    """ccm's node.stop() of a node whose process may have exited already."""
    if server.server_id in {s.server_id for s in await manager.running_servers()}:
        try:
            await manager.server_stop_gracefully(server.server_id)
        except Exception:  # noqa: BLE001
            await manager.server_stop(server.server_id, convict=False)


async def nodetool_removenode(manager: ScyllaClusterManager, server, removed, host_id) -> None:
    """ccm's node.nodetool("removenode <host id>")."""
    await nodetool(manager, server, "removenode", str(host_id))
    manager.cluster.server_mark_removed(removed.server_id)


async def wait_for_node_streaming(manager: ScyllaClusterManager, server, keyspace: str = ".*", from_mark=None):
    """The dtests' wait_for_node_streaming(): the joining node logged that it
    started streaming, and streaming or repair of a keyspace."""
    log = await manager.server_open_log(server.server_id)
    await log.wait_for(BOOTSTRAP_START_LOG_PAT, f"Beginning stream session|sync data for keyspace={keyspace}, status=started",
                       from_mark=from_mark, timeout=600)


async def statuses_of(manager: ScyllaClusterManager, server, *args: str) -> list[str]:
    out = (await nodetool(manager, server, "status", *args)).stdout
    return sorted(re.findall(r"^([UD][NLJM])\s+[\d.]+\s", out, re.MULTILINE))


async def verify_nodes_status(manager: ScyllaClusterManager, server, *alternatives: list[str]) -> None:
    """tools.status.verify_nodes_status(): `nodetool status` of the server
    lists nodes with one of the given lists of statuses."""
    actual = await statuses_of(manager, server)
    assert any(actual == sorted(a) for a in alternatives), f"found statuses: {actual}"


async def wait_for_statuses(manager: ScyllaClusterManager, server, *alternatives: list[str], timeout: float = 90) -> None:
    """tools.status.wait_for_nodes_status(), with alternatives."""
    deadline = time.time() + timeout
    while True:
        try:
            await verify_nodes_status(manager, server, *alternatives)
            return
        except AssertionError:
            if time.time() > deadline:
                raise
            await asyncio.sleep(1)


def in_thread(func, *args):
    """Runs a blocking function in a thread, like the dtests' executor.submit()."""
    return asyncio.create_task(asyncio.to_thread(func, *args))


def query_c1c2_concurrent(cql, keys, consistency, value: str, cf: str = "cf") -> None:
    """The dtests' query_c1c2_concurrent() with c1 = c2 = value for every key."""
    stmt = cql.prepare(f"SELECT c1, c2 FROM {cf} WHERE key=?")
    stmt.consistency_level = consistency
    for ok, rows in execute_concurrent_with_args(cql, stmt, [[f"k{k}"] for k in keys], concurrency=20):
        assert ok, rows
        assert [tuple(r) for r in rows] == [(value, value)], rows


def create_ks_without_tablets(cql, name: str, rf: int) -> None:
    """The dtests' create_ks(session, name, rf, tablets=0)."""
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}} "
                "and tablets={'enabled':false}")


def cql_session(manager: ScyllaClusterManager, server, keyspace: str | None = None):
    """The dtests' patient_cql_connection(node[, keyspace]): a session of its
    own, with the node as contact point, at CL=ONE, and the driver's default
    load balancing policy, a new one for each session as there (con_gen()'s
    default is one policy object every session shares, so one session's view
    of the nodes going down and up leaks into another's)."""
    cql = manager.con_gen([server.ip_addr], load_balancing_policy=default_lbp_factory()).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    if keyspace:
        cql.set_keyspace(keyspace)
    return cql


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_add_node.
# A node added in a new rack of an RF=1 keyspace, which is then altered to
# RF=2 and repaired, holds all the data, as does the first node; with
# stream_plan_ranges_fraction=1 the keyspace is bootstrapped in one stream plan.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("test_stream_plan_ranges_fraction", [False, True])
async def test_simple_add_node(manager: ScyllaClusterManager, test_stream_plan_ranges_fraction):
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    if test_stream_plan_ranges_fraction:
        config.update({"stream_plan_ranges_fraction": 1, "enable_repair_based_node_ops": False})
    [node1] = await populate(manager, generate_cluster_topology(), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)
    await check_rows_on_node(manager, node1, 1000)

    node2 = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack2"})
    cql = await exclusive_one(manager, node2)
    # The dtests' get_replication_options(): with vnodes, the RF is a number.
    cql.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='ks'")
    cql.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2};")
    await manager.server_sees_other_server(node1.ip_addr, node2.ip_addr)
    await manager.server_sees_other_server(node2.ip_addr, node1.ip_addr)
    for node in (node1, node2):
        await nodetool(manager, node, "repair", "-pr")
    cql.execute("use ks;")
    insert_c1c2(cql, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)
    await check_rows_on_node(manager, node2, 2000)
    await check_rows_on_node(manager, node1, 2000)

    if test_stream_plan_ranges_fraction:
        log2 = await manager.server_open_log(node2.server_id)
        assert len(await log2.grep("Streaming plan for Bootstrap-ks-index-")) == 1


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_add_node_with_large_partition1.
# A node bootstraps 100 rows of a 100 KB and a 300 KB value, with RF=2.
@SKIP_DEV_DEBUG
async def test_add_node_with_large_partition1(manager: ScyllaClusterManager):
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    [node1] = await populate(manager, {"datacenter1": {"rack1": 1}}, config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 2)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    nr_rows = 100
    c1s = ["a" * 1024 * 100] * nr_rows
    c2s = ["b" * 1024 * 300] * nr_rows
    insert_c1c2(cql, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, c1_values=c1s, c2_values=c2s, concurrency=10)
    node2 = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack2"})
    await check_rows_on_node(manager, node2, nr_rows)
    await check_rows_on_node(manager, node1, nr_rows)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_add_node_with_large_partition4.
# A node bootstraps 100 partitions of 3000 rows each, with RF=2.
@SKIP_DEV_DEBUG
async def test_add_node_with_large_partition4(manager: ScyllaClusterManager):
    config = {**VNODES, "range_request_timeout_in_ms": 300000, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    nr_partitions, rows_per_partition = 100, 3000
    nr_rows = nr_partitions * rows_per_partition
    [node1] = await populate(manager, {"datacenter1": {"rack1": 1}}, config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 2)
    cql.execute("CREATE TABLE ks.test (pk varchar, ck varchar, PRIMARY KEY(pk, ck))")
    stmt = cql.prepare("INSERT into ks.test (pk, ck) VALUES (?, ?)")
    for pk in range(nr_partitions):
        for ck in range(rows_per_partition):
            cql.execute(stmt, (f"key{pk}", f"row{ck}"))
    node2 = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack2"})
    await check_rows_on_node(manager, node2, nr_rows, cf="test")
    await check_rows_on_node(manager, node1, nr_rows, cf="test")


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_restart_streaming_node_while_bootstrapping.
# A node of the joining node's rack killed while it streams to it makes the
# bootstrap fail; the killed node starts again, the cluster forgets the
# failed node, which joins once wiped, and every row is still there.
@SKIP_DEV_DEBUG
async def test_simple_restart_streaming_node_while_bootstrapping(manager: ScyllaClusterManager):
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    servers = await populate(manager, generate_cluster_topology(rack_num=3), config)
    node1, node2, _ = servers
    cql = cql_one(manager)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    num_keys = 1000
    insert_c1c2(cql, keys=range(num_keys), consistency=ConsistencyLevel.THREE)
    # More data, to make streaming longer.
    create_ks_without_tablets(cql, "ks1", 3)
    create_cf(cql, "ks1.cf1", C1C2, read_repair=0.0)
    num_additional_keys = 100000
    insert_c1c2(cql, keys=range(num_additional_keys), consistency=ConsistencyLevel.THREE, ks="ks1", cf="cf1")
    await flush_all(manager, servers)

    node4 = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack2"},
                                     cmdline=STREAM_SESSION_DEBUG)
    log4, mark4 = await start_no_wait(manager, node4)
    await wait_for_node_streaming(manager, node4, "ks1", from_mark=mark4)
    await manager.get_host_id(node4.server_id)

    manager.ignore_log_patterns += [
        rf"[Rr]epair.*mandatory neighbor={node2.ip_addr} is not alive",
        r"Startup failed:.*Failed to repair for keyspace=ks[1-3]?",
        r"Startup failed: std::runtime_error[ :]+.*\(?repair .* failed",
        r"Startup failed: std::runtime_error[ :]+.*rpc::closed_error",
        r"Startup failed: seastar::sleep_aborted",
        r"Startup failed: std::runtime_error[ :]+.*ranges failed",
        r"Startup failed: std::runtime_error.*Failed stream ranges",
        rf"stream_session .* Failed to handle STREAM_MUTATION_FRAGMENTS .* peer={node2.ip_addr}",
        r"storage_service .* fail to update tokens for .*: exceptions::mutation_write_failure_exception",
        r"storage_service .* Operation failed",
        r"Abort bootstrap operation",
        r"Startup failed: seastar::rpc::closed_error",
        r"Startup failed: streaming::stream_exception[ :]+\(?Stream failed\)?",
        r"bootstrap.* failed.* std::runtime_error[ :]+.*(rpc::closed_error|repair.*failed)",
        r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)",
        r"raft_topology - drain rpc failed, proceed to fence old writes",
        r"raft_topology - transition_state::left_token_ring, raft_topology_cmd::command::barrier failed",
        r"raft_topology - raft_topology_cmd.*failed",
    ]
    await manager.server_stop(node2.server_id, convict=False)
    await log4.wait_for("Stream failed|Startup failed|sync data for keyspace=ks[1-3]?, status=failed", from_mark=mark4,
                        timeout=900)
    await stop_if_running(manager, node4)

    nodes = servers
    for node in nodes:
        if node.server_id not in {s.server_id for s in await manager.running_servers()}:
            await start_node(manager, node, wait_other_notice=True)
    for node in nodes:
        await wait_for_statuses(manager, node, ["UN"] * len(nodes))

    await wipe_server_data(manager, node4.server_id)
    await start_node(manager, node4, wait_other_notice=True)
    cql = cql_one(manager)
    for key in range(num_keys):
        query_c1c2(cql, key, ks="ks", cf="cf")
    for key in range(num_additional_keys):
        query_c1c2(cql, key, ks="ks1", cf="cf1")


async def stress_keyspaces(manager: ScyllaClusterManager, nodes_and_keyspaces, rf: int) -> None:
    """The dtests' node.stress(["write", "n=5000", "no-warmup", "-schema",
    "replication(factor=<rf>) keyspace=<ks>"]): more data, to make streaming
    longer; cassandra-stress creates the keyspace."""
    for node, ks in nodes_and_keyspaces:
        await cassandra_stress(manager, node, "write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace={ks}",
                               create_keyspace=False)


KILLED_WHILE_BOOTSTRAPPING = r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)"


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_kill_new_node_while_bootstrapping.
# A node killed while it bootstraps (repair-based) does not join: the
# cluster is back to its 3 nodes, with all the data.
@SKIP_DEV_DEBUG
async def test_simple_kill_new_node_while_bootstrapping(manager: ScyllaClusterManager):
    manager.ignore_log_patterns += [KILLED_WHILE_BOOTSTRAPPING]
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG, **NO_SMALL_TABLE_OPTIMIZATION}
    node1, node2, node3 = await populate(manager, generate_cluster_topology(rack_num=3), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)
    await stress_keyspaces(manager, [(node1, "ks1"), (node2, "ks2"), (node3, "ks3")], rf=3)

    # The 4th node goes to the rack of node 4 % 3.
    new = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack2"},
                                   cmdline=STREAM_SESSION_DEBUG)
    _, mark = await start_no_wait(manager, new)
    await wait_for_node_streaming(manager, new, from_mark=mark)
    await wait_for_statuses(manager, node1, ["UN", "UN", "UN", "UJ"])
    await manager.server_stop(new.server_id, convict=False)
    await wait_for_statuses(manager, node1, ["UN", "UN", "UN"])

    assert len(list(cql.execute("SELECT * FROM ks.cf"))) == 1000


def write_until_stopped(cql, stop: threading.Event, state: dict, cl: ConsistencyLevel) -> None:
    """The dtests' writer thread: new keys until stopped, failing only on a
    client-side timeout."""
    while not stop.is_set():
        statement = SimpleStatement("INSERT INTO ks.cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" % state["keys"],
                                    consistency_level=cl, retry_policy=FallthroughRetryPolicy())
        try:
            cql.execute(statement)
            state["keys"] += 1
        except Unavailable as e:
            logger.debug(f"exception thrown Unavailable {e}")
        except WriteTimeout as e:
            logger.debug(f"exception thrown WriteTimeout {e}")
        except OperationTimedOut as e:
            state["failed"] = f"Server side exception not thrown, driver side exception thrown OperationTimeout {e}"
            logger.debug(state["failed"])


async def kill_new_node_while_writing(manager: ScyllaClusterManager, cql, new, watchers, waiting, cl: ConsistencyLevel,
                                      statuses):
    """Starts the new node with writes going on, kills it once it streams,
    waits for the `waiting` nodes to list the given statuses, and checks the
    writes: none timed out on the client side, and all the acknowledged keys
    (and maybe the one in flight) are readable."""
    state = {"keys": 1000, "failed": None}
    stop = threading.Event()
    writer = in_thread(write_until_stopped, cql, stop, state, cl)
    _, mark = await start_no_wait(manager, new)
    await wait_for_node_streaming(manager, new, from_mark=mark)
    await stop_node(manager, new, gently=False, wait_other_notice=True)
    for node in waiting:
        await wait_for_statuses(manager, node, *statuses)
    stop.set()
    await writer
    assert state["failed"] is None

    for node in watchers:
        status = (await nodetool(manager, node, "status", "ks")).stdout
        logger.debug(f"nodetool status from {node.ip_addr}: {status}")
    keys = state["keys"]
    rows = list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.QUORUM)))
    assert keys <= len(rows) <= keys + 1, f"Expected between {keys} and {keys + 1} rows, but got {len(rows)}"


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_kill_new_node_while_bootstrapping_with_parallel_writes.
# A node killed while it bootstraps, with QUORUM writes going on: writes
# fail only on the server side, and every acknowledged one is readable.
@SKIP_DEV_DEBUG
async def test_simple_kill_new_node_while_bootstrapping_with_parallel_writes(manager: ScyllaClusterManager):
    manager.ignore_log_patterns += [KILLED_WHILE_BOOTSTRAPPING]
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG, **NO_SMALL_TABLE_OPTIMIZATION}
    node1, node2, node3 = await populate(manager, generate_cluster_topology(rack_num=3), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ALL)
    await stress_keyspaces(manager, [(node1, "ks1"), (node2, "ks2"), (node3, "ks3")], rf=3)

    new = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack2"},
                                   cmdline=STREAM_SESSION_DEBUG)
    await kill_new_node_while_writing(manager, cql, new, [node1, node2, node3], [node1, node2, node3], ConsistencyLevel.QUORUM,
                                      [["UN", "UN", "UN"], ["UN", "UN", "UN", "DN"]])


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_kill_new_node_while_bootstrapping_with_parallel_writes_in_multidc.
# The same with two DCs of one node and EACH_QUORUM writes, the new node
# joining the second DC.
@SKIP_DEV_DEBUG
async def test_simple_kill_new_node_while_bootstrapping_with_parallel_writes_in_multidc(manager: ScyllaClusterManager):
    manager.ignore_log_patterns += [KILLED_WHILE_BOOTSTRAPPING]
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG, **NO_SMALL_TABLE_OPTIMIZATION}
    node1, node2 = await populate(manager, {"dc1": {"rack1": 1}, "dc2": {"rack1": 1}}, config)
    cql = cql_one(manager)
    create_ks(cql, "ks", {"dc1": 1, "dc2": 1})
    create_cf(cql, "ks.cf", C1C2)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.EACH_QUORUM)
    await stress_keyspaces(manager, [(node1, "ks1"), (node2, "ks2")], rf=1)

    new = await manager.server_add(start=False, config=config, property_file={"dc": "dc2", "rack": "rack1"},
                                   cmdline=STREAM_SESSION_DEBUG)
    await kill_new_node_while_writing(manager, cql, new, [node1, node2], [node1], ConsistencyLevel.EACH_QUORUM,
                                      [["UN", "UN"], ["UN", "UN", "DN"]])


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_add_new_node_while_adding_data.
# Rows written while a node bootstraps (by streaming or repair) are all
# readable once it joined.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("rf,bootstrap_method", [(1, "streaming"), (1, "rbno"), (2, "streaming"), (2, "rbno")])
async def test_add_new_node_while_adding_data(manager: ScyllaClusterManager, rf: int, bootstrap_method: str):
    consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
    config = {**VNODES, **rbno_options(bootstrap_method == "rbno", ops=["bootstrap"]), **BATCH_COMMITLOG}
    num_keys = 4000
    await populate(manager, {"dc1": {"rack1": 1, "rack2": 2}}, config)
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(num_keys // 2), consistency=consistency)

    node4 = await manager.server_add(start=False, config=config, property_file={"dc": "dc1", "rack": "rack1"},
                                     cmdline=STREAM_SESSION_DEBUG)
    log4, mark4 = await start_no_wait(manager, node4)
    await wait_for_node_streaming(manager, node4, from_mark=mark4)
    insert_c1c2(cql, keys=range(num_keys // 2, num_keys), consistency=consistency)
    await log4.wait_for("Starting listening for CQL clients", from_mark=mark4)

    result = list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=consistency)))
    assert len(result) == num_keys
    for k in range(num_keys):
        query_c1c2(cql, k, consistency)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_add_new_node_while_schema_changes.
# A keyspace dropped while a node bootstraps it (by streaming or repair)
# does not fail the bootstrap, and a keyspace created and written meanwhile
# is readable afterwards.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("enable_repair_based_node_ops", [False, True])
async def test_simple_add_new_node_while_schema_changes(manager: ScyllaClusterManager, enable_repair_based_node_ops: bool):
    rf = 1
    # auto_snapshot: Scylla's default, as on the dtest nodes; test.py turns it off.
    config = {**VNODES, **rbno_options(enable_repair_based_node_ops), **BATCH_COMMITLOG, "auto_snapshot": True}
    node1, _, _ = await populate(manager, {"datacenter1": {"rack1": 3}}, config)
    # The dtest's own session to node1 (it switches it to the keyspaces).
    cql = cql_session(manager, node1)
    # Without tablets, else it would be streamed by tablet load balancing
    # after the bootstrap.
    create_ks_without_tablets(cql, "ks", rf)
    # create_ks() switches the session to the keyspace.
    cql.execute("USE ks")
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(4000), consistency=ConsistencyLevel.ONE)

    def run(min_keys):
        list(cql.execute(SimpleStatement("DROP KEYSPACE ks")))
        create_ks(cql, "ks1", rf)
        cql.execute("USE ks1")
        create_cf(cql, "ks1.cf1", C1C2, read_repair=0.0)
        for i in range(min_keys):
            cql.execute(SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i),
                                        consistency_level=ConsistencyLevel.ONE))

    node4 = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack1"},
                                     cmdline=STREAM_SESSION_DEBUG)
    log4 = await manager.server_open_log(node4.server_id)
    mark4 = await log4.mark()
    node4_start = asyncio.create_task(start_node(manager, node4, wait_other_notice=True))
    await wait_for_node_streaming(manager, node4, from_mark=mark4)

    manager.ignore_log_patterns += [
        r"ks=ks, cf=cf, .*no_such_column_family",
        r"raft_topology - .*node state is bootstrapping",
        r"no_such_keyspace.*\(Can't find a keyspace ks\)",
        "Startup failed",
    ]
    min_keys = 100
    t = in_thread(run, min_keys)
    if enable_repair_based_node_ops:
        bootstrap_success_msg = "completed successfully, keyspace=ks"
        bootstrap_failed_msg = r"sync data for keyspace=ks, status=failed: keyspace does not exist any more|no_such_keyspace.*\(Can't find a keyspace ks\)"
        bootstrap_skipped_msg = r"keyspace=ks does not exist any more, ignoring it|keyspace ks does not exist, skipping"
        await log4.wait_for(f"{bootstrap_success_msg}|{bootstrap_failed_msg}|{bootstrap_skipped_msg}", from_mark=mark4)
    await node4_start
    session = cql_session(manager, node4)
    await t

    with pytest.raises(InvalidRequest, match="Keyspace ks does not exist"):
        session.execute("SELECT * FROM ks.cf")
    result = list(session.execute(SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=ConsistencyLevel.ONE)))
    assert len(result) >= min_keys, f"Expected at least {min_keys} items in table"


def select_until_stopped(cql, stop: threading.Event, num_keys: int, consistency, speculative: bool = False) -> None:
    """The dtests' background SELECT loop: every read returns all the rows."""
    if speculative:
        query = SimpleStatement("SELECT * FROM ks.cf", is_idempotent=True)
        query.consistency_level = consistency
    else:
        query = cql.prepare("SELECT * FROM ks.cf")
        query.consistency_level = consistency
    while not stop.is_set():
        if speculative:
            result = list(cql.execute(query, timeout=60, execution_profile="speculative"))
        else:
            result = list(cql.execute(query))
        assert len(result) == num_keys
        time.sleep(0.01)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_add_new_node_while_query_info.
# Reads while a node bootstraps return all the rows.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("rf", [1, 2])
async def test_simple_add_new_node_while_query_info(manager: ScyllaClusterManager, rf):
    consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    await populate(manager, generate_cluster_topology(rack_num=rf), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    num_keys = 2000
    insert_c1c2(cql, keys=range(num_keys), consistency=consistency)

    node4 = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack1"},
                                     cmdline=STREAM_SESSION_DEBUG)
    log4, mark4 = await start_no_wait(manager, node4)
    await wait_for_node_streaming(manager, node4, from_mark=mark4)
    stop = threading.Event()
    t = in_thread(select_until_stopped, cql, stop, num_keys, consistency)
    await log4.wait_for("Starting listening for CQL clients", from_mark=mark4)
    stop.set()
    await t

    result = list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=consistency)))
    assert len(result) == num_keys
    for k in range(num_keys):
        query_c1c2(cql, k, consistency)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_add_new_node_while_add_new_table.
# A table created and written before, during or after a node bootstraps
# has all its rows.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("when", ["before", "during", "after"])
async def test_add_new_node_while_add_new_table(manager: ScyllaClusterManager, when):
    rf = 1
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    nodes = await populate(manager, generate_cluster_topology(rack_num=3), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(4000), consistency=ConsistencyLevel.ONE)
    manager.ignore_log_patterns += [r"raft_topology - streaming for tablet .* failed",
                                    "raft_topology - raft_topology_cmd.*failed with: raft::request_aborted"]

    def run(min_keys):
        create_ks(cql, "ks1", rf)
        create_cf(cql, "ks1.cf1", C1C2, read_repair=0.0)
        for i in range(min_keys):
            cql.execute(SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i),
                                        consistency_level=ConsistencyLevel.ALL))

    min_keys = 1000
    t = None
    if when == "before":
        t = in_thread(run, min_keys)
    node4 = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack1"},
                                     cmdline=STREAM_SESSION_DEBUG)
    log4, mark4 = await start_no_wait(manager, node4)
    await wait_for_node_streaming(manager, node4, from_mark=mark4)
    if when == "during":
        t = in_thread(run, min_keys)
    for node in nodes:
        await manager.server_sees_other_server(node.ip_addr, node4.ip_addr)
        await manager.server_sees_other_server(node4.ip_addr, node.ip_addr)
    await log4.wait_for("Starting listening for CQL clients", from_mark=mark4)
    if when == "after":
        # The driver must know node4 before the table is created, see
        # https://github.com/scylladb/scylladb/issues/21371
        deadline = time.time() + 60
        while not ((host := cql.cluster.metadata.get_host(node4.ip_addr)) is not None and host.is_up):
            assert time.time() < deadline, f"Waiting for driver to discover node4 ({node4.ip_addr})"
            await asyncio.sleep(0.5)
        t = in_thread(run, min_keys)
    await t

    result = list(cql.execute(SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=ConsistencyLevel.ONE)))
    assert len(result) >= min_keys, f"Expected at least {min_keys} items in table"


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_decommission_node_1.
# With RF=1, the node left after a decommission has every row, and the
# decommissioned node takes no CQL connection.
@SKIP_DEV_DEBUG
async def test_simple_decommission_node_1(manager: ScyllaClusterManager):
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    node1, node2 = await populate(manager, generate_cluster_topology(nodes_per_rack=2), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)

    await nodetool(manager, node2, "decommission")
    with pytest.raises(NoHostAvailable):
        manager.con_gen([node2.ip_addr]).connect()
    await stop_node(manager, node2)
    await check_rows_on_node(manager, node1, 1000, restart=False)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_kill_remained_node_while_decommissioning.
# Another node killed while a node decommissions makes the decommission
# fail; started again, it rejoins and all the rows are there.
@SKIP_DEV_DEBUG
async def test_simple_kill_remained_node_while_decommissioning(manager: ScyllaClusterManager):
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG, **NO_SMALL_TABLE_OPTIMIZATION}
    node1, node2, node3 = await populate(manager, generate_cluster_topology(nodes_per_rack=3), config,
                                         cmdline=STREAM_SESSION_DEBUG)
    cql = cql_one(manager)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)
    log2 = await manager.server_open_log(node2.server_id)
    mark = await log2.mark()

    decommission = asyncio.create_task(nodetool(manager, node2, "decommission", check=False))
    await log2.wait_for("Beginning stream session|sync data for keyspace=.*, status=started", from_mark=mark)
    await verify_nodes_status(manager, node1, ["UN", "UL", "UN"])

    await manager.server_stop(node1.server_id, convict=False)
    await wait_for_statuses(manager, node2, ["DN", "UN", "UN"])
    await wait_for_statuses(manager, node3, ["DN", "UN", "UN"])
    manager.ignore_log_patterns += [
        "decommission.*Operation failed",
        "raft_topology - Decommission failed.",
        r"raft_topology - raft_topology_cmd.*failed with: raft::request_aborted[ :]+\(?Request is aborted by a caller\)?",
        r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is decommissioning\)",
        r"raft_topology - raft_topology_cmd.*failed with: (?:seastar::abort_requested_exception[ :]+\(?abort requested\)?|abort requested)",
        r"seastar::rpc::closed_error[ :]+\(?connection is closed\)?",
    ]
    await start_node(manager, node1, wait_other_notice=True)

    result = list(cql.execute("SELECT * FROM ks.cf"))
    assert len(result) == 1000, "Should have 1000 items in table"
    await decommission


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_decommission_node_while_adding_info.
# Rows written while a node decommissions are all readable afterwards.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("rf", [1, 2])
async def test_simple_decommission_node_while_adding_info(manager: ScyllaClusterManager, rf):
    consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    servers = await populate(manager, {"datacenter1": {"rack1": 1, "rack2": 2}}, config)
    node2 = servers[1]
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(2000), consistency=consistency)

    def run():
        insert_c1c2(cql, keys=range(2000, 4000), consistency=consistency)
        result = list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=consistency)))
        assert len(result) == 4000, "should have 4000 items in table"

    t = in_thread(run)
    await nodetool(manager, node2, "decommission")
    await t
    await stop_node(manager, node2)
    result = list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=consistency)))
    assert len(result) == 4000, "should have 4000 items in table"
    for k in range(4000):
        query_c1c2(cql, k, consistency)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_decommission_node_while_query_info.
# Reads while a node decommissions return all the rows.
@SKIP_DEV_DEBUG
@pytest.mark.parametrize("rf", [1, 2])
async def test_simple_decommission_node_while_query_info(manager: ScyllaClusterManager, rf):
    consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    servers = await populate(manager, {"datacenter1": {"rack1": 1, "rack2": 2}}, config)
    node1, node2 = servers[:2]
    cql = cql_session(manager, node1)
    cql.cluster.add_execution_profile("speculative", ExecutionProfile(
        speculative_execution_policy=ConstantSpeculativeExecutionPolicy(delay=5, max_attempts=10)))
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    num_keys = 2000
    insert_c1c2(cql, keys=range(num_keys), consistency=consistency)

    stop = threading.Event()
    t = in_thread(select_until_stopped, cql, stop, num_keys, consistency, True)
    await nodetool(manager, node2, "decommission")
    stop.set()
    await t

    query = SimpleStatement("SELECT * FROM ks.cf", consistency_level=consistency)
    assert len(list(cql.execute(query))) == num_keys
    await stop_node(manager, node2)
    assert len(list(cql.execute(query))) == num_keys
    for k in range(num_keys):
        query_c1c2(cql, k, consistency)


async def prepare_cluster(manager: ScyllaClusterManager, dcs, rf: int, keys_range: int):
    """The dtests' _prepare_cluster()."""
    servers = await populate(manager, dcs, {**VNODES, "hinted_handoff_enabled": False, **BATCH_COMMITLOG})
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(keys_range), consistency=ConsistencyLevel.ALL)
    return servers


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_removenode_1.
# With RF=2 over two racks, one rack holds all the data; removenode of a
# node of the other rack keeps every row available at CL=TWO.
@SKIP_DEV_DEBUG
async def test_simple_removenode_1(manager: ScyllaClusterManager):
    node1, _, node3, node4 = await prepare_cluster(manager, generate_cluster_topology(rack_num=2, nodes_per_rack=2),
                                                   rf=2, keys_range=100)
    cql = cql_one(manager)
    node3_hostid = await manager.get_host_id(node3.server_id)
    await stop_node(manager, node3, wait_other_notice=True)
    await stop_node(manager, node4, wait_other_notice=True)
    assert len(list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.ONE)))) == 100
    await start_node(manager, node4, wait_other_notice=True)
    await nodetool_removenode(manager, node1, node3, node3_hostid)
    await asyncio.sleep(2)
    assert len(list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.TWO)))) == 100
    insert_c1c2(cql, keys=range(120), consistency=ConsistencyLevel.TWO)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_simple_removenode_2.
# removenode of the only replica of some data (RF=1) succeeds, and writes at
# CL=ALL work afterwards.
@SKIP_DEV_DEBUG
async def test_simple_removenode_2(manager: ScyllaClusterManager):
    node1, node2, _ = await prepare_cluster(manager, generate_cluster_topology(nodes_per_rack=3), rf=1, keys_range=10)
    cql = cql_one(manager)
    node2_hostid = await manager.get_host_id(node2.server_id)
    await stop_node(manager, node2, wait_other_notice=True)
    try:
        list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.ONE)))
    except Unavailable:
        pass
    await nodetool_removenode(manager, node1, node2, node2_hostid)
    insert_c1c2(cql, keys=range(10), consistency=ConsistencyLevel.ALL)
    assert len(list(cql.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.ONE)))) == 10


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_removenode_rejoin.
# A node removed with removenode, restarted, is told it is banned and does
# not come back.
@SKIP_DEV_DEBUG
async def test_removenode_rejoin(manager: ScyllaClusterManager):
    node1, _node2, node3 = await populate(manager, {"datacenter1": {"rack1": 1, "rack2": 2}}, VNODES)
    node3_hostid = await manager.get_host_id(node3.server_id)
    await stop_node(manager, node3, wait_other_notice=True)
    await nodetool_removenode(manager, node1, node3, node3_hostid)
    manager.ignore_log_patterns += ["gossip - is_safe_for_restart:", "Startup failed: std::runtime_error"]
    log3 = await manager.server_open_log(node3.server_id)
    mark3 = await log3.mark()
    start = asyncio.create_task(manager.server_start(node3.server_id, expected_error="banned"))
    await log3.wait_for("received notification of being banned from the cluster from", from_mark=mark3, timeout=30)
    cluster_state = await nodetool_status(manager, node1)
    assert node3.ip_addr not in [status["address"] for status in cluster_state], "Remove node returned to cluster"
    await start


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_increment_decrement_counters_in_threads_nodes_restarted.
# 120 threads increment (or decrement) 2 counters 500 times at CL=ONE,
# first with one of the three replicas down, then with two of them down;
# once all are back, every replica returns the exact sums.
@SKIP_DEV_DEBUG
async def test_increment_decrement_counters_in_threads_nodes_restarted(manager: ScyllaClusterManager):
    nodes = await populate(manager, generate_cluster_topology(rack_num=3), {**VNODES, **DEFAULT_OPTIONS})
    cql = cql_session(manager, nodes[0])
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", {"c": "counter"})
    cql.shutdown()

    nb_increment = 500
    nb_counter = 2

    def run(connection, decrement):
        _return = dict.fromkeys(range(nb_counter), 0)
        for _ in range(nb_increment):
            for c in range(nb_counter):
                op = "-" if decrement else "+"
                connection.execute(SimpleStatement("UPDATE cf SET c = c %s 1 WHERE key = 'counter%i'" % (op, c),
                                                   consistency_level=ConsistencyLevel.ONE))
                _return[c] += -1 if decrement else 1
                time.sleep(0.01)
        return _return

    num_threads = 120
    result = dict.fromkeys(range(nb_counter), 0)

    async def run_threads(sessions_of_thread):
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(run, sessions_of_thread(x), (x % len(nodes)) == 0) for x in range(num_threads)]
            for f in futures:
                t_result = await asyncio.wrap_future(f)
                for k in result:
                    result[k] += t_result[k]

    sessions = [cql_session(manager, node, "ks") for node in nodes]
    await stop_node(manager, nodes[2], wait_other_notice=True)
    await run_threads(lambda x: sessions[x % (len(nodes) - 1)])
    await start_node(manager, nodes[2], wait_other_notice=True)
    await stop_node(manager, nodes[0], wait_other_notice=True)
    await stop_node(manager, nodes[1], wait_other_notice=True)
    sessions[2] = cql_session(manager, nodes[2], "ks")
    await run_threads(lambda x: sessions[2])
    await start_node(manager, nodes[0], wait_other_notice=True)
    await start_node(manager, nodes[1], wait_other_notice=True)

    keys = ",".join("'counter%i'" % c for c in range(nb_counter))
    query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.ALL)
    res = list(sessions[0].execute(query))
    assert res == list(sessions[1].execute(query)), "different counter values in node0 and node1"
    assert res == list(sessions[2].execute(query)), "different counter values in node0 and node2"
    values = {r[0]: r[1] for r in res}
    for c in range(nb_counter):
        assert result[c] == values[f"counter{c}"], "Expecting counter%i = %i, got %i" % (c, result[c], values[f"counter{c}"])
    for s in sessions:
        s.shutdown()


async def write_latest_copies(manager: ScyllaClusterManager, node1, node2, node3=None):
    """The verify_latest_copy dtests' writes: 1000 keys with value 0 on all
    nodes, then 500 keys with value 1 on node1 only and 500 with value 2 on
    node2 only (with node3, if given, down since before them)."""
    cql = cql_one(manager)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    cs = ["0"] * 1000
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.TWO if node3 is None else ConsistencyLevel.THREE,
                c1_values=cs, c2_values=cs)
    if node3 is not None:
        await stop_node(manager, node3)
    await stop_node(manager, node2)
    session = cql_session(manager, node1)
    cs = ["1"] * 500
    insert_c1c2(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
    session.shutdown()
    await start_node(manager, node2)
    await stop_node(manager, node1)
    session = cql_session(manager, node2)
    cs = ["2"] * 500
    insert_c1c2(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
    session.shutdown()
    await start_node(manager, node1)


def check_latest_copies(session, consistency) -> None:
    query_c1c2_concurrent(session, range(500), consistency, "1")
    query_c1c2_concurrent(session, range(500, 1000), consistency, "2")


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_verify_latest_copy_add_node.
# A node bootstrapped by repair into RF=3 gets the latest version of every
# row, which each of the two old nodes missed part of.
@SKIP_DEV_DEBUG
async def test_verify_latest_copy_add_node(manager: ScyllaClusterManager):
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG}
    node1, node2 = await populate(manager, {"datacenter1": {"rack1": 1, "rack2": 1}}, config)
    await write_latest_copies(manager, node1, node2)
    node3 = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack3"})
    session = cql_session(manager, node3)
    session.execute("use ks;")
    await stop_node(manager, node1)
    await stop_node(manager, node2)
    check_latest_copies(session, ConsistencyLevel.ONE)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_verify_latest_copy_replace_node.
# A node replacing a dead one by repair gets the latest version of every row.
@SKIP_DEV_DEBUG
async def test_verify_latest_copy_replace_node(manager: ScyllaClusterManager):
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG}
    node1, node2, node3 = await populate(manager, generate_cluster_topology(rack_num=3), config)
    await write_latest_copies(manager, node1, node2, node3)
    node4 = await manager.server_add(replace_cfg=ReplaceConfig(replaced_id=node3.server_id, reuse_ip_addr=False, use_host_id=True),
                                     config=config, property_file={"dc": "datacenter1", "rack": "rack3"})
    session = cql_session(manager, node4)
    session.execute("use ks;")
    await stop_node(manager, node1)
    await stop_node(manager, node2)
    check_latest_copies(session, ConsistencyLevel.ONE)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_verify_latest_copy_rebuild_node.
# A node that missed all the writes gets the latest version of every row by
# a repair-based rebuild.
@SKIP_DEV_DEBUG
async def test_verify_latest_copy_rebuild_node(manager: ScyllaClusterManager):
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG}
    node1, node2, node3 = await populate(manager, generate_cluster_topology(rack_num=3), config)
    await write_latest_copies(manager, node1, node2, node3)
    await start_node(manager, node3)
    await nodetool(manager, node3, "rebuild")
    session = cql_session(manager, node3)
    session.execute("use ks;")
    await stop_node(manager, node1)
    await stop_node(manager, node2)
    check_latest_copies(session, ConsistencyLevel.ONE)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_verify_latest_copy_decommission_node.
# After a repair-based decommission of node1, node2 and node3 return the
# latest version of every row at CL=TWO.
@SKIP_DEV_DEBUG
async def test_verify_latest_copy_decommission_node(manager: ScyllaClusterManager):
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG}
    node1, node2, node3 = await populate(manager, generate_cluster_topology(rack_num=3), config)
    await write_latest_copies(manager, node1, node2, node3)
    await start_node(manager, node3)
    await nodetool(manager, node1, "decommission")
    session = cql_session(manager, node3)
    session.execute("use ks;")
    check_latest_copies(session, ConsistencyLevel.TWO)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_verify_latest_copy_removenode_node.
# Each quarter of the rows is last written with a different node down;
# after a repair-based removenode of node2, node1 and node3 return the latest
# version of every row at CL=TWO.
@SKIP_DEV_DEBUG
async def test_verify_latest_copy_removenode_node(manager: ScyllaClusterManager):
    config = {**VNODES, **rbno_options(True), **BATCH_COMMITLOG}
    node1, node2, node3, node4 = await populate(manager, {"datacenter1": {"rack1": 2, "rack2": 1, "rack3": 1}}, config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    cs = ["0"] * 1000
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

    # Each round: the node to stop, the one to start before, the coordinator.
    for value, (down, up, coordinator) in enumerate([(node4, None, node1), (node3, node4, node4),
                                                     (node2, node3, node3), (node1, node2, node2)], start=1):
        if up is not None:
            await start_node(manager, up)
        await stop_node(manager, down)
        session = cql_session(manager, coordinator)
        cs = [str(value)] * 250
        start = (value - 1) * 250
        insert_c1c2(session, keys=range(start, start + 250), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        session.shutdown()

    hostid = await manager.get_host_id(node2.server_id)
    await stop_node(manager, node2, wait_other_notice=True)
    await start_node(manager, node1)
    await nodetool_removenode(manager, node1, node2, hostid)
    session = cql_session(manager, node1)
    session.execute("use ks;")
    await stop_node(manager, node4)
    for value in range(1, 5):
        start = (value - 1) * 250
        query_c1c2_concurrent(session, range(start, start + 250), ConsistencyLevel.TWO, str(value))


def gossipinfo(output: str) -> dict[str, dict[str, str]]:
    """The dtests' nodetool_gossipinfo(): {endpoint: {state: value}}."""
    result = {}
    current = None
    for line in output.splitlines():
        if line.startswith("/"):
            current = result.setdefault(line[1:].strip(), {})
        elif current is not None and ":" in line:
            key, value = line.strip().split(":", 1)
            current[key] = value
    return result


async def wait_for_gossip_shutdown(manager: ScyllaClusterManager, nodes, endpoint: str, timeout: float = 120) -> None:
    """The dtests' wait_for(is_shutdown, step=10): no node's `nodetool
    gossipinfo` shows the endpoint in a state other than shutdown."""
    deadline = time.time() + timeout
    while True:
        found = False
        for node in nodes:
            gs = gossipinfo((await nodetool(manager, node, "gossipinfo")).stdout)
            if endpoint in gs and "shutdown" not in gs[endpoint].get("STATUS", ""):
                found = True
        if not found:
            return
        assert time.time() < deadline, f"{endpoint} is not in gossip status shutdown"
        await asyncio.sleep(10)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_decommission_after_changing_node_ip.
# A node restarted with a new IP can be decommissioned; its old IP is then
# in gossip state shutdown, and a new node can join.
@SKIP_DEV_DEBUG
async def test_decommission_after_changing_node_ip(manager: ScyllaClusterManager):
    servers = await populate(manager, generate_cluster_topology(rack_num=3), VNODES)
    node3 = servers[2]
    await stop_node(manager, node3)
    old_ip3 = node3.ip_addr
    await manager.server_change_ip(node3.server_id)
    await start_node(manager, node3)
    # The node's ServerInfo carries its new address only after the restart.
    node3 = next(s for s in await manager.running_servers() if s.server_id == node3.server_id)
    for s in await others_of(manager, node3):
        await manager.server_sees_other_server(s.ip_addr, node3.ip_addr)
    deadline = time.time() + 120
    while (await nodetool(manager, node3, "decommission", check=False)).returncode != 0:
        assert time.time() < deadline, "decommission of node3 did not succeed"
        await asyncio.sleep(1)
    await wait_for_gossip_shutdown(manager, await manager.running_servers(), old_ip3)
    await manager.server_add(config=VNODES, property_file={"dc": "datacenter1", "rack": "rack3"})


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_replace_after_changing_node_ip.
# A node restarted with a new IP and stopped again can be replaced by host id.
@SKIP_DEV_DEBUG
async def test_replace_after_changing_node_ip(manager: ScyllaClusterManager):
    node1, node2, node3 = await populate(manager, generate_cluster_topology(rack_num=3), VNODES)
    await manager.get_host_id(node3.server_id)
    await stop_node(manager, node3)
    old_ip3 = node3.ip_addr
    await manager.server_change_ip(node3.server_id)
    await start_node(manager, node3)
    # The node's ServerInfo carries its new address only after the restart.
    node3 = next(s for s in await manager.running_servers() if s.server_id == node3.server_id)
    for s in await others_of(manager, node3):
        await manager.server_sees_other_server(s.ip_addr, node3.ip_addr)
    await stop_node(manager, node3, wait_other_notice=True)
    await wait_for_gossip_shutdown(manager, [node1, node2], old_ip3)
    await manager.server_add(replace_cfg=ReplaceConfig(replaced_id=node3.server_id, reuse_ip_addr=False, use_host_id=True),
                             config=VNODES, property_file={"dc": "datacenter1", "rack": "rack3"})


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_change_node_ip_full_cluster_down.
# All nodes change their IPs while the whole cluster is down: started
# again, every node lists each new IP with the node's host id.
@SKIP_DEV_DEBUG
async def test_change_node_ip_full_cluster_down(manager: ScyllaClusterManager):
    config = {**VNODES, **DEFAULT_OPTIONS, **BATCH_COMMITLOG}
    servers = await populate(manager, generate_cluster_topology(rack_num=3), config)
    cql = cql_one(manager)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(cql, keys=range(1000), consistency=ConsistencyLevel.ONE)
    origin_hostids = {s.server_id: str(await manager.get_host_id(s.server_id)) for s in servers}

    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    for s in servers:
        await manager.server_change_ip(s.server_id)
    for s in servers:
        await manager.server_start(s.server_id)

    servers = await manager.running_servers()
    hostids = {s.server_id: str(await manager.get_host_id(s.server_id)) for s in servers}
    ips = {s.ip_addr: s.server_id for s in servers}
    for node in servers:
        for n in await nodetool_status(manager, node):
            if n["address"] in ips:
                server_id = ips[n["address"]]
                assert n["host_id"] == hostids[server_id]
                assert origin_hostids[server_id] == hostids[server_id]


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayout::test_decommission_after_decreasing_rf.
# After the RF is lowered from 3 to 2, a node is decommissioned, a node
# added and the RF raised back to 3 with a repair: each node alone has all
# the data.
@SKIP_DEV_DEBUG
async def test_decommission_after_decreasing_rf(manager: ScyllaClusterManager):
    servers = await populate(manager, generate_cluster_topology(rack_num=3, dc_name_prefix="dc"),
                             {**VNODES, "hinted_handoff_enabled": "false"})
    node1, node2, node3 = servers
    num_keys = 500

    def verify_data(session, consistency_level=ConsistencyLevel.QUORUM):
        for key in range(num_keys):
            query_c1c2(session, key, consistency=consistency_level)

    async def alter_ks(session, rf: int):
        """The dtests' change_schema_safely()."""
        session.execute(f"ALTER KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}}}", timeout=300)
        for s in await manager.running_servers():
            await (await manager.server_open_log(s.server_id)).wait_for("Schema version changed", timeout=300)
        assert session.cluster.control_connection.wait_for_schema_agreement(wait_time=300)

    session = cql_session(manager, node1)
    create_ks(session, "ks", {"dc1": 3})
    session.execute("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                    "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                    "AND speculative_retry='NONE'")
    session.execute("SELECT * FROM ks.cf LIMIT 1")
    insert_c1c2(session, keys=range(num_keys))
    await alter_ks(session, 2)
    await nodetool(manager, node3, "decommission")
    await stop_node(manager, node3)
    verify_data(session)
    node4 = await manager.server_add(config={**VNODES, "hinted_handoff_enabled": "false"},
                                     property_file={"dc": "dc1", "rack": "rack3"})
    for s in (node1, node2):
        await manager.server_sees_other_server(s.ip_addr, node4.ip_addr)
    await alter_ks(session, 3)
    session.shutdown()

    await nodetool(manager, node4, "repair", "ks")
    all_nodes = [node1, node2, node4]
    for node in all_nodes:
        other_nodes = [n for n in all_nodes if n is not node]
        await asyncio.gather(*(manager.server_stop_gracefully(n.server_id) for n in other_nodes))
        session = cql_session(manager, node)
        verify_data(session, consistency_level=ConsistencyLevel.ONE)
        session.shutdown()
        for n in other_nodes:
            await start_node(manager, n, wait_other_notice=True)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestStopNodeEarly::test_stop_node_while_restarting.
# A node stopped (killed or gracefully) early in its restart - before or
# after its peer sees it up - leaves the peer serving all the data.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("gently,wait_other_notice", [(False, False), (False, True), (True, False), (True, True)])
async def test_stop_node_while_restarting(manager: ScyllaClusterManager, gently, wait_other_notice):
    node1, node2 = await populate(manager, generate_cluster_topology(rack_num=2), VNODES)
    session = cql_session(manager, node2)
    create_ks(session, "ks", 2)
    create_cf(session, "ks.cf", C1C2, read_repair=0.0)
    insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

    await stop_node(manager, node1, wait_other_notice=True)
    log1 = await manager.server_open_log(node1.server_id)
    mark1 = await log1.mark()
    log2 = await manager.server_open_log(node2.server_id)
    mark2 = await log2.mark()
    host_id1 = await manager.get_host_id(node1.server_id)
    await manager.server_start(node1.server_id, expected_server_up_state=ServerUpState.PROCESS_STARTED, connect_driver=False)
    if wait_other_notice:
        # The other node must see node1 up first, to see it down while it stops.
        await log2.wait_for(f"({node1.ip_addr}|{host_id1}) is now UP", from_mark=mark2)
    else:
        # A point early enough in the startup, before gossip starts.
        await log1.wait_for("starting view update generator", from_mark=mark1)

    manager.ignore_log_patterns += ["Failed to start a Raft group"]
    if gently:
        try:
            await manager.server_stop_gracefully(node1.server_id)
        except RuntimeError as e:
            # Before the node handles SIGTERM, the signal ends it at once.
            if "exit code: -15" not in str(e):
                raise
            await manager.server_stop(node1.server_id, convict=False)
    else:
        await manager.server_stop(node1.server_id, convict=False)
    if wait_other_notice:
        await manager.server_not_sees_other_server(node2.ip_addr, node1.ip_addr)

    result = list(session.execute("SELECT * FROM ks.cf"))
    assert len(result) == 1000


# The TestUpdateClusterLayoutWithRaftTopology dtests: a cluster of one node
# per rack, plus a second node in rack3, populated by cassandra-stress.
RAFT_TOPOLOGY_CONFIG = {**VNODES, "ring_delay_ms": 3000,
                        "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission",
                        **NO_SMALL_TABLE_OPTIMIZATION}
NUM_OF_RACKS = pytest.mark.parametrize("num_of_racks", [pytest.param(3, id="3_racks"), pytest.param(5, id="5_racks")])


def log_message_ids(val):
    """The dtests' generate_test_name()."""
    return val.replace(" ", "_")


async def prepare_raft_cluster(manager: ScyllaClusterManager, num_of_racks: int):
    """The dtests' prepare_cluster() and run_stress_to_populate_cluster().
    As with the dtests' allow_log_errors, errors in the logs are allowed."""
    manager.ignore_log_patterns.append(".*")
    dcs = generate_cluster_topology(rack_num=num_of_racks)
    dcs["datacenter1"]["rack3"] += 1
    servers = await populate(manager, dcs, RAFT_TOPOLOGY_CONFIG)
    finder = TopologyCoordinatorFinder(manager)
    await finder.wait_topology_coordinator_elected()
    rf = num_of_racks
    await cassandra_stress(manager, servers[0], *f"write cl=QUORUM n=4000 -schema replication(factor={rf}) -col size=fixed(200) n=FIXED(5)".split(),
                           create_keyspace=False)
    return servers, finder


class TopologyCoordinatorFinder:
    """The dtests' tools.raft_topology.TopologyCoordinatorFinder."""

    def __init__(self, manager: ScyllaClusterManager):
        self.manager = manager
        self.num_of_elections = 0

    async def history(self):
        ids = await get_coordinator_host_ids(self.manager)
        self.num_of_elections = len(ids)
        return ids

    async def get_topology_coordinator_node(self):
        latest = str((await self.history())[0])
        for s in await self.manager.all_servers():
            try:
                host_id = str(await self.manager.get_host_id(s.server_id))
            except RuntimeError:
                continue
            if host_id == latest:
                return s
        raise AssertionError(f"no node with host id {latest}")

    async def wait_topology_coordinator_elected(self, timeout: float = 10):
        current = self.num_of_elections
        deadline = time.time() + timeout
        while True:
            await self.history()
            if self.num_of_elections > current:
                return
            assert time.time() < deadline, "Wait topology coordinator reelection..."
            await asyncio.sleep(1)


def cqlsh_rows(server, cmd: str) -> list[str]:
    """The rows of a cqlsh query, as tools.cluster.get_group0_members() parses them."""
    res = run_cqlsh_at(server.ip_addr, 9042, cmd)
    # cqlsh always warns on stderr here (no pytz for TZ=UTC), so failure is
    # told by the exit code, not by stderr output as with ccm's cqlsh.
    if res.returncode != 0:
        return []
    lines = []
    for line in res.stdout.strip().split("\n"):
        if not line.strip():
            break
        lines.append(line.strip())
    return lines[2:]


async def get_group0_members(server) -> list[dict]:
    rows = await asyncio.to_thread(cqlsh_rows, server, "select value from system.scylla_local where key = 'raft_group0_id'")
    if not rows:
        return []
    rows = await asyncio.to_thread(cqlsh_rows, server, "select server_id, can_vote from system.raft_state "
                                   f"where group_id = {rows[0]} and disposition = 'CURRENT'")
    members = []
    for line in rows:
        server_id, can_vote = line.split("|")
        members.append({"host_id": server_id.strip(), "is_voter": can_vote.strip() == "True"})
    return members


async def get_token_ring_members(manager: ScyllaClusterManager, server) -> list[dict]:
    result = await manager.api.client.get_json("/storage_service/host_id", host=server.ip_addr)
    return [{"host_ip": m.get("key"), "host_id": m.get("value")} for m in result or []]


async def verify_group0_and_token_ring_members(manager: ScyllaClusterManager, server, expected_num_of_members: int) -> None:
    """The dtests' verify_group0_and_token_ring_members()."""
    group0_members = await get_group0_members(server)
    assert expected_num_of_members == len(group0_members), f"group0 members: {group0_members}"
    token_ring_members = await get_token_ring_members(manager, server)
    assert expected_num_of_members == len(token_ring_members), f"token ring members: {token_ring_members}"
    assert any(m["is_voter"] for m in group0_members), "No voters in group0"
    assert not {m["host_id"] for m in group0_members} - {m["host_id"] for m in token_ring_members}


async def wait_for_token_ring_and_group0_members(manager: ScyllaClusterManager, server, expected_num_of_members: int,
                                                 timeout: float = 120) -> None:
    """The dtests' wait_for_token_ring_and_group0_consistency()."""
    deadline = time.time() + timeout
    while True:
        try:
            await verify_group0_and_token_ring_members(manager, server, expected_num_of_members)
            return
        except Exception:  # noqa: BLE001
            if time.time() > deadline:
                raise
            await asyncio.sleep(1)


async def mark_logs(manager: ScyllaClusterManager, servers) -> dict:
    return {s.server_id: ((log := await manager.server_open_log(s.server_id)), await log.mark()) for s in servers}


async def wait_for_removenode(removed_node_id: str, log_marks: dict, timeout: float = 600) -> bool:
    """The dtests' wait_for_removenode(): whether the removenode succeeded,
    or else was rolled back or cancelled, as any node logs."""
    expr = (rf"(?P<success>raft_topology - updating topology state: finished removing node {removed_node_id})"
            rf"|(?P<rollback>raft_topology - updating topology state: complete rollback of {removed_node_id} to state normal after removing failure)"
            r"|(?P<cancel>raft_topology - updating topology state: cancel all topology requests)")
    deadline = time.time() + timeout
    while True:
        assert time.time() < deadline, f"timeout waiting for removenode {removed_node_id} to finish"
        for log, mark in log_marks.values():
            matches = await log.grep(expr, from_mark=mark)
            if matches:
                return matches[0][1].group("success") is not None
        await asyncio.sleep(0.1)


async def new_node_in_rack3(manager: ScyllaClusterManager):
    return await manager.server_add(config=RAFT_TOPOLOGY_CONFIG, property_file={"dc": "datacenter1", "rack": "rack3"})


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayoutWithRaftTopology::test_no_garbage_members_left_after_abort_removenode_by_kill_coordinator.
# The topology coordinator, running removenode, is killed at a given point
# and restarted: the removenode either finished, or is run again; then no
# garbage is left in group0 and the token ring, and a new node can join.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@NUM_OF_RACKS
@pytest.mark.parametrize("log_message", [
    r"raft_topology - updating topology state: start removenode",
    r"repair - removenode_with_repair",
    r"removenode: waiting for completion",
    r"raft_group0 - making server.*non-voter",
    r"raft_topology - start streaming",
    r"raft_topology - streaming completed",
], ids=log_message_ids)
async def test_no_garbage_members_left_after_abort_removenode_by_kill_coordinator(manager: ScyllaClusterManager, log_message,
                                                                                  num_of_racks):
    servers, finder = await prepare_raft_cluster(manager, num_of_racks)
    num_of_nodes = len(servers)
    coordinator = await finder.get_topology_coordinator_node()
    node3 = servers[2]
    node3_hostid = str(await manager.get_host_id(node3.server_id))
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)

    await stop_node(manager, node3, wait_other_notice=True)
    log_marks = await mark_logs(manager, servers)
    removenode = asyncio.create_task(nodetool(manager, coordinator, "removenode", node3_hostid, check=False))
    log, mark = log_marks[coordinator.server_id]
    await log.wait_for(log_message, from_mark=mark)
    await stop_node(manager, coordinator, gently=False, wait_other_notice=True)
    await start_node(manager, coordinator, wait_other_notice=True)
    await finder.wait_topology_coordinator_elected()
    coordinator = await finder.get_topology_coordinator_node()

    if not await wait_for_removenode(node3_hostid, log_marks):
        # node3 is banned now, so remove it before a new node joins.
        await nodetool(manager, coordinator, "removenode", node3_hostid)
    manager.cluster.server_mark_removed(node3.server_id)
    await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes - 1)
    await new_node_in_rack3(manager)
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)
    await removenode


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayoutWithRaftTopology::test_no_garbage_members_left_after_abort_removenode_by_kill_peer_node.
# A node other than the coordinator is killed at a given point of a
# removenode and restarted: the removenode either finished, or failed and is
# run again; then no garbage is left and a new node can join.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@NUM_OF_RACKS
@pytest.mark.parametrize("log_message", [
    r"raft_topology - updating topology state: start removenode",
    r"repair - removenode_with_repair",
    r"raft_group0 - making server.*non-voter",
    r"raft_topology - start streaming",
    r"raft_topology - streaming completed",
], ids=log_message_ids)
async def test_no_garbage_members_left_after_abort_removenode_by_kill_peer_node(manager: ScyllaClusterManager, log_message,
                                                                                num_of_racks):
    servers, finder = await prepare_raft_cluster(manager, num_of_racks)
    num_of_nodes = len(servers)
    coordinator = await finder.get_topology_coordinator_node()
    node3 = servers[2]
    node3_hostid = str(await manager.get_host_id(node3.server_id))
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)
    peers = [s for s in servers if s.server_id not in (node3.server_id, coordinator.server_id)]

    await stop_node(manager, node3)
    log_marks = await mark_logs(manager, servers)
    removenode = asyncio.create_task(nodetool(manager, coordinator, "removenode", node3_hostid, check=False))
    log, mark = log_marks[coordinator.server_id]
    await log.wait_for(log_message, from_mark=mark)
    await stop_node(manager, peers[0], gently=False, wait_other_notice=True)
    succeeded = await wait_for_removenode(node3_hostid, log_marks)
    await start_node(manager, peers[0], wait_other_notice=True)

    if not succeeded:
        await log.wait_for("raft_topology - Removenode failed. See earlier errors", from_mark=mark, timeout=60)
        # node3 is banned now, so remove it before a new node joins.
        await nodetool(manager, coordinator, "removenode", node3_hostid)
    manager.cluster.server_mark_removed(node3.server_id)
    await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes - 1)
    await new_node_in_rack3(manager)
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)
    await removenode


async def streaming_completed_for_decommission(manager: ScyllaClusterManager, coordinator, from_mark) -> bool:
    """The dtests' is_streaming_completed_for_decommission()."""
    log = await manager.server_open_log(coordinator.server_id)
    try:
        await log.wait_for("raft_topology - updating topology state: decommissioning: streaming completed",
                           from_mark=from_mark, timeout=60)
        return True
    except TimeoutError:
        return False


DECOMMISSION_LOG_MESSAGES = pytest.mark.parametrize("log_message", [
    "raft_topology - streaming completed",
    "storage_service - DECOMMISSIONING: done",
    "raft_topology - start streaming",
    r"repair - repair.*: completed successfully",
], ids=log_message_ids)


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayoutWithRaftTopology::test_no_garbage_left_after_abort_decommission_by_kill_decommissioned_node.
# The decommissioning node is killed at a given point: after streaming
# completed it is removed anyway, before that it is rolled back and comes
# back; no garbage is left either way, and a new node can join.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@NUM_OF_RACKS
@DECOMMISSION_LOG_MESSAGES
async def test_no_garbage_left_after_abort_decommission_by_kill_decommissioned_node(manager: ScyllaClusterManager, log_message,
                                                                                     num_of_racks):
    servers, finder = await prepare_raft_cluster(manager, num_of_racks)
    num_of_nodes = len(servers)
    coordinator = await finder.get_topology_coordinator_node()
    node3 = servers[2]
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)

    marks = await mark_logs(manager, servers)
    decommission = asyncio.create_task(nodetool(manager, node3, "decommission", check=False))
    log3, mark3 = marks[node3.server_id]
    await log3.wait_for(log_message, from_mark=mark3)
    host_id3 = await manager.get_host_id(node3.server_id)
    await manager.server_stop(node3.server_id, convict=False)

    if await streaming_completed_for_decommission(manager, coordinator, marks[coordinator.server_id][1]):
        for s in servers:
            if s.server_id != node3.server_id:
                log, mark = marks[s.server_id]
                await log.wait_for(rf"gossip - Removed endpoint ({node3.ip_addr}|{host_id3})", from_mark=mark, timeout=20)
        await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes - 1)
    else:
        await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes)
        await manager.server_start(node3.server_id)
        await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes)
        num_of_nodes += 1
    await new_node_in_rack3(manager)
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)
    await decommission


# Ported from scylla-dtest update_cluster_layout_tests.py::TestUpdateClusterLayoutWithRaftTopology::test_no_garbage_left_after_abort_decomission_by_kill_coordinator.
# The topology coordinator is killed at a given point of a decommission and
# restarted: a new coordinator finishes the decommission if streaming had
# completed, or else the node stays; no garbage is left either way, and a new
# node can join.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@NUM_OF_RACKS
@DECOMMISSION_LOG_MESSAGES
async def test_no_garbage_left_after_abort_decomission_by_kill_coordinator(manager: ScyllaClusterManager, log_message,
                                                                           num_of_racks):
    servers, finder = await prepare_raft_cluster(manager, num_of_racks)
    num_of_nodes = len(servers)
    node3 = servers[2]
    coordinator = await finder.get_topology_coordinator_node()
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)

    marks = await mark_logs(manager, servers)
    decommission = asyncio.create_task(nodetool(manager, node3, "decommission", check=False))
    log3, mark3 = marks[node3.server_id]
    await log3.wait_for(log_message, from_mark=mark3)
    host_id3 = await manager.get_host_id(node3.server_id)
    await manager.server_stop(coordinator.server_id, convict=False)

    streaming_done = await streaming_completed_for_decommission(manager, coordinator, marks[coordinator.server_id][1])
    try:
        await log3.wait_for("raft_topology - Decommission failed. See earlier errors", from_mark=mark3, timeout=90)
        decommission_failed = True
    except TimeoutError:
        decommission_failed = False

    await manager.server_start(coordinator.server_id)
    alive = [s for s in servers if s.server_id not in (node3.server_id, coordinator.server_id)]
    for s in alive:
        await manager.server_sees_other_server(s.ip_addr, coordinator.ip_addr)
        await manager.server_sees_other_server(coordinator.ip_addr, s.ip_addr)
    await finder.wait_topology_coordinator_elected()
    old_coordinator, coordinator = coordinator, await finder.get_topology_coordinator_node()
    if not streaming_done:
        streaming_done = await streaming_completed_for_decommission(manager, coordinator, marks[coordinator.server_id][1])

    if streaming_done and not decommission_failed:
        for s in servers:
            if s.server_id not in (node3.server_id, old_coordinator.server_id):
                log, mark = marks[s.server_id]
                await log.wait_for(rf"gossip - Removed endpoint ({node3.ip_addr}|{host_id3})", from_mark=mark, timeout=30)
        await log3.wait_for("DECOMMISSIONING: done", from_mark=mark3, timeout=30)
        coordinator = await finder.get_topology_coordinator_node()
        await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes - 1)
        await stop_node(manager, node3)
    else:
        await wait_for_token_ring_and_group0_members(manager, coordinator, num_of_nodes)
        num_of_nodes += 1
    await new_node_in_rack3(manager)
    await verify_group0_and_token_ring_members(manager, coordinator, num_of_nodes)
    await decommission
