#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Replacing dead nodes, ported from scylla-dtest replace_address_test.py:
# the replacing node takes over the tokens of a node that was killed or shut
# down, with a different or the same IP address, while writes go on, with
# RF=1 data that cannot be fully recovered, and a replace of a live node is
# rejected. Like the dtest class, the tests that start their cluster through
# init_cluster() run with repair-based node operations enabled and disabled.
# Keyspaces use vnodes, like the dtests. Data is written by cassandra-stress
# with the dtests' command lines, and tokens are read with "nodetool ring",
# like in the dtests. At the end of each test the nodes are stopped
# gracefully, like at the dtests' teardown.

import asyncio
import logging
import threading
import time
import uuid

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, ReadFailure, ReadTimeout, Unavailable
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import SimpleStatement

from test.cluster.dtest_helpers import flush_all
from test.cluster.util import BANNED_NOTIFICATION, cassandra_stress, run_nodetool, stop_servers_gracefully
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
RBO = pytest.mark.parametrize("rbo", [True, False], ids=["rbo_enabled", "rbo_disabled"])
QUERY_FAILURES = (Unavailable, ReadTimeout, ReadFailure, OperationTimedOut)


def rbo_config(rbo: bool, **extra):
    return {**VNODES, "enable_repair_based_node_ops": rbo, **extra}


# A ccm cluster populated with a number of nodes, without a topology, uses
# SimpleSnitch: all nodes are in datacenter1, rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}


def racks(*names, dc="dc1"):
    return [{"dc": dc, "rack": r} for r in names]


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def nodetool(manager: ScyllaClusterManager, server, *args) -> str:
    return (await run_nodetool(manager, server, *args))[0]


async def sorted_tokens(manager: ScyllaClusterManager, server, address) -> list[str]:
    """The tokens of the node at `address`, from "nodetool ring" run on `server`."""
    out = await nodetool(manager, server, "ring")
    return sorted(line.split()[-1] for line in out.splitlines() if line.split() and line.split()[0] == address)


def create_stress_compatible_table(cql, rf):
    """The dtests' create_stress_compatible_table(): cassandra-stress's table,
    created ahead of cassandra-stress."""
    cql.execute(f"CREATE KEYSPACE keyspace1 WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf} }}")
    cql.execute("""CREATE TABLE keyspace1.standard1(
        key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob,
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


async def stop(manager: ScyllaClusterManager, server, gently: bool):
    """Stops a node and waits until the others see it down."""
    if gently:
        await manager.server_stop_gracefully(server.server_id)
    else:
        await manager.server_stop(server.server_id, convict=False)
    await manager.others_not_see_server(server.ip_addr)


def replace_cfg(server, reuse_ip=False, **kwargs):
    return ReplaceConfig(replaced_id=server.server_id, reuse_ip_addr=reuse_ip, use_host_id=True, **kwargs)


async def cassandra_stress_whitelist(args, whitelist, timeout):
    """cassandra-stress with its own "-node whitelist <hosts>" option, which
    test.cluster.util.cassandra_stress() cannot express: like ccm's
    node.stress(), the command line is the dtest's, with the native protocol
    and credentials added."""
    proc = await asyncio.create_subprocess_exec("cassandra-stress", *args, "-node", "whitelist", ",".join(whitelist),
                                                "-mode", "native", "cql3", "user=cassandra", "password=cassandra",
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), timeout)
    assert proc.returncode == 0, f"cassandra-stress {' '.join(args)} failed: {out.decode()}"
    return out.decode()


async def replace_node_test(manager: ScyllaClusterManager, gently: bool, rbo: bool):
    """
    The dtests' _replace_node_test(): a node that was killed or shut down
    gracefully is replaced: reads at CL=THREE fail while it is down and work
    again after the replace, the replacing node owns its tokens, and the
    replaced node, started again, is not part of the cluster.
    """
    servers = await manager.servers_add(3, config=rbo_config(rbo), property_file=racks("r1", "r2", "r3"))
    node1, node2, node3 = servers
    tokens = await sorted_tokens(manager, node3, node3.ip_addr)
    host_id3 = await manager.get_host_id(node3.server_id)

    await cassandra_stress(manager, node1, "write", "n=10000", "-schema", "replication(factor=3)", rf=3)
    cql = manager.get_cql()
    query = SimpleStatement("select * from keyspace1.standard1 LIMIT 1", consistency_level=ConsistencyLevel.THREE)
    initial = list(cql.execute(query))

    await stop(manager, node3, gently)
    with pytest.raises(QUERY_FAILURES):
        cql.execute(query)

    node4 = await manager.server_add(replace_cfg(node3), config=rbo_config(rbo), property_file=node3.property_file())
    assert sorted(cql.execute(query)) == sorted(initial)
    assert await sorted_tokens(manager, node4, node4.ip_addr) == tokens
    assert await sorted_tokens(manager, node1, node3.ip_addr) == []

    # The replaced node is banned from the cluster, the others do not list it.
    if gently:
        await manager.server_start(node3.server_id, expected_error=BANNED_NOTIFICATION)
    else:
        # Like the dtest, only wait until the node answers its host id: the
        # killed node replays its commitlog first, and it may still be up
        # (not yet notified of its ban) when the test ends.
        await manager.server_start(node3.server_id, connect_driver=False,
                                   expected_server_up_state=ServerUpState.HOST_ID_QUERIED)
    for node in (node1, node2, node4):
        status = await nodetool(manager, node, "status")
        assert host_id3 not in status
        assert node3.ip_addr not in status


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_stopped_node.
# A killed node is replaced.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_stopped_node(manager: ScyllaClusterManager, rbo: bool):
    await replace_node_test(manager, gently=False, rbo=rbo)


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_shutdown_node.
# A node shut down gracefully is replaced (CASSANDRA-9871).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_shutdown_node(manager: ScyllaClusterManager, rbo: bool):
    await replace_node_test(manager, gently=True, rbo=rbo)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_node_using_new_ip_then_shut_down(manager: ScyllaClusterManager, rbo: bool):
    """
    A killed node is replaced by a node with a new IP address, which is then
    killed while a minute of QUORUM writes of cassandra-stress goes on: with
    two of three replicas alive the writes keep succeeding.
    """
    node1, node2 = await manager.servers_add(2, config=rbo_config(rbo), property_file=racks("r1", "r2"))
    # Added on its own, like in the dtest, so that it is not a seed.
    node3 = await manager.server_add(config=rbo_config(rbo), property_file=racks("r3")[0])
    await stop(manager, node3, gently=False)
    node4 = await manager.server_add(replace_cfg(node3), config=rbo_config(rbo), property_file=node3.property_file())

    stress = asyncio.create_task(cassandra_stress(
        manager, node1, "write", "cl=QUORUM", "duration=1m", "-rate", "threads=10 throttle=1000/s", "-log", "interval=5",
        "-schema", "replication(factor=3) keyspace=keyspace2"))
    await manager.server_stop(node4.server_id, convict=False)
    await manager.others_not_see_server(node4.ip_addr)
    await stress


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_active_node(manager: ScyllaClusterManager, rbo: bool):
    """A replace of a node that is alive is rejected."""
    servers = await manager.servers_add(3, config=rbo_config(rbo, **SIMPLE_SNITCH))
    node3 = servers[2]
    host_id3 = await manager.get_host_id(node3.server_id)
    await manager.server_add(replace_cfg(node3, wait_dead=False), config=rbo_config(rbo, **SIMPLE_SNITCH),
                             expected_error=f"tried to replace alive node {host_id3}")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_with_background_workload(manager: ScyllaClusterManager, rbo: bool):
    """
    A killed node is replaced while QUORUM writes go on, and they go on for
    at least 30 seconds after the replace: no node logs "unknown endpoint"
    (#4705). Rack r3 has two nodes, so that it keeps a normal node during the
    replace. The writes of cassandra-stress go to the nodes that stay alive,
    at 500 per second, first for 100 seconds and then in 30-second runs.
    """
    servers = await manager.servers_add(4, config=rbo_config(rbo, failure_detector_timeout_in_ms=5000),
                                        property_file=racks("r1", "r2", "r3", "r3"))
    node1, node3 = servers[0], servers[2]
    logger.info(await nodetool(manager, node1, "status"))

    cql = manager.get_cql()
    create_stress_compatible_table(cql, rf=3)
    whitelist = [s.ip_addr for s in servers if s is not node3]
    replace_done = None

    async def run_stress(duration):
        await cassandra_stress_whitelist(["write", f"duration={duration}s", "no-warmup", "cl=QUORUM", "-rate",
                                          "threads=2 throttle=500/s", "-schema", "replication(factor=3)",
                                          "-pop", "seq=1..1000"], whitelist, timeout=duration + 60)

    async def workload():
        await run_stress(100)
        while replace_done is None or time.time() - replace_done < 30:
            await run_stress(30)

    task = asyncio.create_task(workload())
    deadline = time.time() + 120
    while cql.execute("SELECT * FROM keyspace1.standard1 LIMIT 1").one() is None:
        assert time.time() < deadline, "the background workload did not write its first row"
        await asyncio.sleep(0.5)

    await stop(manager, node3, gently=False)
    await manager.server_add(replace_cfg(node3), config=rbo_config(rbo, failure_detector_timeout_in_ms=5000),
                             property_file=node3.property_file())
    replace_done = time.time()
    await asyncio.wait_for(task, 300)

    for s in await manager.all_servers():
        log = await manager.server_open_log(s.server_id)
        assert not await log.grep("unknown endpoint", max_count=3)


async def replace_node_with_schema_rf_1(manager: ScyllaClusterManager, rbo: bool, gently: bool):
    """
    The dtests' replace_node_with_schema_rf_1(): a node that was killed or
    shut down gracefully is replaced while a keyspace has RF=1: the replace
    succeeds and moves the tokens; streaming-based replace warns that it has
    no source for ranges of the RF=1 keyspace (#6351).
    """
    servers = await manager.servers_add(3, config=rbo_config(rbo, **SIMPLE_SNITCH))
    node1, node3 = servers[0], servers[2]
    node3_tokens = await sorted_tokens(manager, node3, node3.ip_addr)
    await cassandra_stress(manager, node1, "write", "n=10000", "-schema", "replication(factor=1)", rf=1)

    await stop(manager, node3, gently=gently)
    node4 = await manager.server_add(replace_cfg(node3), config=rbo_config(rbo, **SIMPLE_SNITCH))
    if not rbo:
        log = await manager.server_open_log(node4.server_id)
        await log.wait_for(r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation", timeout=60)

    assert await sorted_tokens(manager, node4, node4.ip_addr) == node3_tokens
    assert await sorted_tokens(manager, node1, node3.ip_addr) == []
    assert node4.ip_addr in [s.ip_addr for s in await manager.running_servers()]


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_stopped_node_with_schema_rf_1.
# A killed node is replaced while a keyspace has RF=1.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_stopped_node_with_schema_rf_1(manager: ScyllaClusterManager, rbo: bool):
    await replace_node_with_schema_rf_1(manager, rbo, gently=False)


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_shutdown_node_with_schema_rf_1.
# A node shut down gracefully is replaced while a keyspace has RF=1.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_shutdown_node_with_schema_rf_1(manager: ScyllaClusterManager, rbo: bool):
    await replace_node_with_schema_rf_1(manager, rbo, gently=True)


KS_RF3 ="CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
CREATE_CF = ("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
             "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
             "AND read_repair_chance=0.000000")


def insert_c1c2(cql, keys, cl, values=None):
    if values is None:
        stmt = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        args = [[f"k{k}"] for k in keys]
    else:
        stmt = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, ?, ?)")
        args = [[f"k{k}", v, v] for k, v in zip(keys, values)]
    stmt.consistency_level = cl
    execute_concurrent_with_args(cql, stmt, args, concurrency=20)


async def five_nodes_with_data(manager: ScyllaClusterManager, config=None, values=None):
    """Five nodes in racks r1:2, r2:2, r3:1 and an RF=3 table written at CL=ALL
    through the fifth node, the only one in r3."""
    servers = await manager.servers_add(5, config={**VNODES, **(config or {})},
                                        property_file=racks("r1", "r1", "r2", "r2", "r3"))
    node5 = servers[4]
    cql = manager.con_gen([node5.ip_addr]).connect()
    cql.execute(KS_RF3)
    cql.execute(CREATE_CF)
    # The dtests' create_cf() checks that the table exists by reading it.
    cql.execute("SELECT * FROM ks.cf LIMIT 1")
    n = len(values) if values else 1000
    insert_c1c2(cql, range(n), ConsistencyLevel.ALL, values)
    return servers


async def wait_force_removed(manager: ScyllaClusterManager, nodes, address, host_id):
    for s in nodes:
        log = await manager.server_open_log(s.server_id)
        await log.wait_for(f"gossip - Finished to force remove node ({address}|{host_id})", timeout=120)


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_node_diff_ip.
# The only node of a rack, shut down gracefully, is replaced by a node with a
# different IP address, whose tokens are the replaced node's and which every
# node force-removes from gossip.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_replace_node_diff_ip(manager: ScyllaClusterManager):
    servers = await five_nodes_with_data(manager)
    node1, node5 = servers[0], servers[4]
    host_id5 = await manager.get_host_id(node5.server_id)
    node5_tokens = await sorted_tokens(manager, node5, node5.ip_addr)
    await stop(manager, node5, gently=True)

    node6 = await manager.server_add(replace_cfg(node5), config=VNODES, property_file=node5.property_file())
    await wait_force_removed(manager, servers[:4] + [node6], node5.ip_addr, host_id5)
    assert await sorted_tokens(manager, node6, node6.ip_addr) == node5_tokens
    assert await sorted_tokens(manager, node1, node5.ip_addr) == []


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_node_same_ip.
# The only node of a rack, shut down gracefully, is replaced by a node with
# its IP address and an empty data directory.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_replace_node_same_ip(manager: ScyllaClusterManager):
    # Sometimes we got closed_error for the old RPC client for the old node5.  Just ignore it.
    manager.ignore_log_patterns.append(r".*seastar::rpc::closed_error[ :]+\(?connection is closed\)?.*")
    servers = await five_nodes_with_data(manager)
    node5 = servers[4]
    await stop(manager, node5, gently=True)
    await manager.server_add(replace_cfg(node5, reuse_ip=True), config=VNODES, property_file=node5.property_file())


def connect(manager: ScyllaClusterManager, server):
    """A session like the dtests' patient_cql_connection(node): token aware
    (the driver's default policy) with the node as contact point, statements
    defaulting to consistency level ONE."""
    cql = manager.con_gen([server.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


TAKE_WRITE_CONFIG = {**VNODES, "stream_io_throughput_mb_per_sec": 8}
ROUNDS, KEYS_PER_ROUND = 100, 1000


async def total_writes(manager: ScyllaClusterManager, nodes) -> dict:
    """scylla_database_total_writes of each node, read in the given order,
    like the dtests' get_node_metrics() loop."""
    writes = {}
    for s in nodes:
        writes[s.server_id] = (await manager.metrics.query(s.ip_addr)).get("scylla_database_total_writes")
        logger.debug("scylla_database_total_writes: %s=%s", s.ip_addr, writes[s.server_id])
    return writes


async def take_write_test(manager: ScyllaClusterManager, reuse_ip: bool):
    """
    The dtests' test_replace_node_{diff,same}_ip_take_write(): the only node
    of a rack is shut down gracefully and replaced while QUORUM writes of
    1000 keys per round, from a thread, go on: the replacing node, a replica
    of every key, counts writes from them in scylla_database_total_writes
    during the data sync. Streaming is throttled to 8 MB/s and 30 MB are
    loaded first, so that the data sync takes a while.
    """
    values = ["x" * 2500] * 6000  # 30MB in total, the table is not compressed
    servers = await five_nodes_with_data(manager, config={"stream_io_throughput_mb_per_sec": 8}, values=values)
    node1, node5 = servers[0], servers[4]
    host_id5 = await manager.get_host_id(node5.server_id)
    await stop(manager, node5, gently=True)

    completed_rounds = 0
    stop_writes = threading.Event()

    def insert_data(session, rounds, keys_per_round):
        nonlocal completed_rounds
        session.execute("use ks;")
        # The diff_ip dtest writes rounds 0..rounds-1, the same_ip one rounds 1..rounds.
        for i in (range(1, rounds + 1) if reuse_ip else range(rounds)):
            if stop_writes.is_set():
                break
            start = i * keys_per_round
            insert_c1c2(session, range(start, start + keys_per_round), ConsistencyLevel.QUORUM)
            completed_rounds += 1
        return rounds

    new = await manager.server_add(replace_cfg(node5, reuse_ip), config=TAKE_WRITE_CONFIG,
                                   property_file=node5.property_file(), start=False)
    log = await manager.server_open_log(new.server_id)
    mark = await log.mark()
    start_task = asyncio.create_task(manager.server_start(new.server_id))
    await log.wait_for("raft_topology - start streaming", from_mark=mark)

    write_task = asyncio.create_task(asyncio.to_thread(insert_data, connect(manager, node1), ROUNDS, KEYS_PER_ROUND))

    # The order in which the dtests read the nodes' metrics.
    order = [new, *reversed(servers[:4])] if reuse_ip else [new, *servers[:4]]
    writes_started = (await total_writes(manager, order))[new.server_id]
    rounds_started = completed_rounds
    await log.wait_for("raft_topology - streaming completed", from_mark=mark)
    writes_done = (await total_writes(manager, order))[new.server_id]
    rounds_done = completed_rounds

    rounds_during_replace = rounds_done - rounds_started
    assert rounds_during_replace >= 1
    # The new node is a replica of every key, so it should have received ~1000
    # writes per round completed during the data sync. Require only a fraction
    # of that to tolerate rounds that straddle the window boundaries.
    assert writes_done - writes_started >= rounds_during_replace * KEYS_PER_ROUND // 10

    stop_writes.set()
    # Wait for the new node to finish the replace
    await start_task
    connect(manager, new)
    if not reuse_ip:
        await wait_force_removed(manager, servers[:4] + [new], node5.ip_addr, host_id5)
    await write_task


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_node_diff_ip_take_write.
# The replacing node, with a new IP address, takes writes during the data sync.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_replace_node_diff_ip_take_write(manager: ScyllaClusterManager):
    await take_write_test(manager, reuse_ip=False)


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_node_same_ip_take_write.
# The replacing node, with the replaced node's IP address, takes writes during
# the data sync.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_replace_node_same_ip_take_write(manager: ScyllaClusterManager):
    await take_write_test(manager, reuse_ip=True)


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_node_using_the_same_ip_then_shut_down.
# A killed node is replaced by a node with its IP address, which is then
# killed while a minute of QUORUM writes of cassandra-stress goes on: with two
# of three replicas alive the writes keep succeeding.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_node_using_the_same_ip_then_shut_down(manager: ScyllaClusterManager, rbo: bool):
    # see https://github.com/scylladb/scylladb/issues/15713
    manager.ignore_log_patterns.append("raft::transport_error")
    node1, node2 = await manager.servers_add(2, config=rbo_config(rbo), property_file=racks("r1", "r2"))
    # Added on its own, like in the dtest, so that it is not a seed.
    node3 = await manager.server_add(config=rbo_config(rbo), property_file=racks("r3")[0])
    await stop(manager, node3, gently=False)
    node4 = await manager.server_add(replace_cfg(node3, reuse_ip=True), config=rbo_config(rbo),
                                     property_file=node3.property_file())

    stress = asyncio.create_task(cassandra_stress(
        manager, node1, "write", "cl=QUORUM", "duration=1m", "-rate", "threads=10 throttle=1000/s", "-log", "interval=5",
        "-schema", "replication(factor=3) keyspace=keyspace2"))
    await manager.server_stop(node4.server_id, convict=False)
    await manager.others_not_see_server(node4.ip_addr)
    await stress


def assert_row_count(cql, table, expected, consistency_level=ConsistencyLevel.ONE):
    """The dtests' assert_row_count()."""
    count = cql.execute(SimpleStatement(f"SELECT count(*) FROM {table}", consistency_level=consistency_level),
                        timeout=120).one()[0]
    assert count == expected, f"Expected a row count of {expected} in table '{table}', but got {count}"


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_serve_writes_during_bootstrap.
# The replacing node serves writes while data is streamed (or repaired) into
# it, so that when the replace completes it has up-to-date data: rows written
# during the data sync are there when only the new node is left running.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_serve_writes_during_bootstrap(manager: ScyllaClusterManager, rbo: bool):
    servers = await manager.servers_add(3, config=rbo_config(rbo), property_file=racks("r1", "r2", "r3"))
    node1, node2, node3 = servers
    session = connect(manager, node1)
    keyspace_name, table_name = "ks", "cf"
    # The dtests' create_ks()
    session.execute(f"CREATE KEYSPACE {keyspace_name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':3}}")
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    session.execute(f"USE {keyspace_name}")
    session.execute(f"USE {keyspace_name}")  # the dtest USEs it again after create_ks()
    session.execute(f"CREATE TABLE {table_name} (pk int, ck int, v int, primary key (pk, ck))")

    keys = 300  # 30 in debug mode, where the dtest does not run
    rows = 500
    total_rows = keys * rows
    insert_stmt = session.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)")
    data = []

    def insert(pks, cks):
        for i in pks:
            for k in cks:
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

    await asyncio.to_thread(insert, range(keys), range(rows))
    await flush_all(manager, servers)
    tokens = await sorted_tokens(manager, node3, node3.ip_addr)
    assert_row_count(session, table_name, total_rows)

    await stop(manager, node3, gently=True)
    node4 = await manager.server_add(replace_cfg(node3), config=rbo_config(rbo), property_file=node3.property_file(),
                                     cmdline=["--logger-log-level", "stream_session=debug"], start=False)
    log = await manager.server_open_log(node4.server_id)
    mark = await log.mark()
    start_task = asyncio.create_task(manager.server_start(node4.server_id))
    await log.wait_for("raft_topology - start streaming", "Beginning stream session|sync data for keyspace=ks, status=started",
                       from_mark=mark, timeout=600)

    # Insert 1000 rows more.
    await asyncio.to_thread(insert, range(keys, keys + 10), range(rows, rows + 100))
    assert_row_count(session, table_name, total_rows + 1000, consistency_level=ConsistencyLevel.QUORUM)

    await log.wait_for("initialization completed", from_mark=mark)
    await start_task
    assert await sorted_tokens(manager, node4, node4.ip_addr) == tokens
    assert await sorted_tokens(manager, node1, node3.ip_addr) == []

    # stop all nodes except the new one
    for node in [node1, node2]:
        await stop(manager, node, gently=True)

    # validate data
    await nodetool(manager, node4, "flush")
    session = connect(manager, node4)
    session.execute(f"USE {keyspace_name}")
    assert_row_count(session, table_name, total_rows + 1000)
    rows_read = [list(r) for r in session.execute(SimpleStatement(f"select * from {table_name}", consistency_level=ConsistencyLevel.ONE))]
    assert sorted(rows_read) == sorted(data)


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_nonexistent_node.
# A node replacing a host ID that is not in the cluster fails to start.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_nonexistent_node(manager: ScyllaClusterManager, rbo: bool):
    await manager.servers_add(3, config=rbo_config(rbo, **SIMPLE_SNITCH))
    replace_node_host_id = str(uuid.uuid4())
    await manager.server_add(config=rbo_config(rbo, **SIMPLE_SNITCH, replace_node_first_boot=replace_node_host_id),
                             expected_error=f"Replaced node with Host ID {replace_node_host_id} not found")


# Ported from scylla-dtest replace_address_test.py::TestReplaceAddress::test_replace_first_boot.
# A node shut down gracefully is replaced: reads at CL=THREE fail while it is
# down and work again after the replace, the replacing node owns its tokens,
# the replaced node cannot start again, and after the replacing node is
# killed and restarted, reads work and it still owns the same tokens.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@RBO
async def test_replace_first_boot(manager: ScyllaClusterManager, rbo: bool):
    config = rbo_config(rbo, range_request_timeout_in_ms=10000)
    servers = await manager.servers_add(3, config=config, property_file=racks("r1", "r2", "r3"))
    node1, node2, node3 = servers
    tokens = await sorted_tokens(manager, node3, node3.ip_addr)

    await cassandra_stress(manager, node1, "write", "n=10000", "-schema", "replication(factor=3)", rf=3)
    session = connect(manager, node1)
    query = SimpleStatement("select * from keyspace1.standard1 LIMIT 1", consistency_level=ConsistencyLevel.THREE)
    initial = list(session.execute(query))

    await stop(manager, node3, gently=True)
    with pytest.raises(QUERY_FAILURES):
        session.execute(query, timeout=30)

    node4 = await manager.server_add(replace_cfg(node3), config=config, property_file=node3.property_file())
    assert sorted(session.execute(query)) == sorted(initial)
    assert await sorted_tokens(manager, node4, node4.ip_addr) == tokens
    assert await sorted_tokens(manager, node1, node3.ip_addr) == []

    # The replaced node cannot start again: it is banned from the cluster.
    await manager.server_start(node3.server_id, expected_error=BANNED_NOTIFICATION)

    # Wait for the other nodes to observe both sides of the restart.
    await manager.server_stop(node4.server_id, convict=False)
    for s in (node1, node2):
        await manager.server_not_sees_other_server(s.ip_addr, node4.ip_addr)
    await manager.server_start(node4.server_id)
    for s in (node1, node2):
        await manager.server_sees_other_server(s.ip_addr, node4.ip_addr)

    assert sorted(session.execute(query)) == sorted(initial)
    # Restarting the node does not move the tokens again.
    assert await sorted_tokens(manager, node4, node4.ip_addr) == tokens
    assert await sorted_tokens(manager, node1, node3.ip_addr) == []
