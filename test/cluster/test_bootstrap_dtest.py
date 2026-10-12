#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Bootstrap scenarios ported from scylla-dtest bootstrap_test.py, one test per
dtest: adding a node, off-strategy compaction of the data a node receives
by repair-based bootstrap, a bootstrap with one token per node during reads,
reading through a bootstrapped node the data written before it joined,
LOCAL_QUORUM writes while a node bootstraps in another DC, a decommissioned
or failed node joining again once wiped, restarting a whole cluster after a
graceful or forced stop, killing a node while it bootstraps under a write
workload, and bootstraps rejected while a node is down or without seeds.

The workloads run cassandra-stress with the dtests' command lines; the
keyspaces use vnodes, as in the dtests.
"""
import asyncio
import logging
import random
import re
import threading
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from test.cluster.dtest_helpers import cql_one, dtest_session, exclusive_one, DTEST_CONFIG, C1C2, create_cf, create_ks, flush_all, insert_c1c2, \
    nodetool, query_c1c2, topology, wait_for_nodes_status
from test.cluster.util import cassandra_stress, reconnect_driver, stop_servers_gracefully, wipe_server_data
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

BOOTSTRAP_START_LOG_PAT = r"raft_topology - start streaming"
BOOTSTRAP_DONE_LOG_PAT = r"raft_topology - streaming completed"

# ccm's populate(N), with no topology, configures SimpleSnitch.
SIMPLE_SNITCH = {**DTEST_CONFIG, "endpoint_snitch": "SimpleSnitch"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


@pytest.fixture(autouse=True)
def bootstrap_log_patterns(manager: ScyllaClusterManager):
    """The log errors TestBootstrap ignores in every test."""
    manager.ignore_log_patterns += [
        # This one occurs when trying to send the migration to a node that
        # hasn't started yet, and when it does, it gets replayed and
        # everything is fine.
        r"Can\'t send migration request: node.*is down",
        # Streaming errors during bootstrap.
        r"Exception encountered during startup",
        r"Streaming error occurred",
    ]


async def cluster_stop(manager: ScyllaClusterManager, gently: bool = True):
    """ccm's cluster.stop(): all the running nodes stop in parallel,
    gracefully or killed."""
    servers = await manager.running_servers()
    if gently:
        await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    else:
        await asyncio.gather(*(manager.server_stop(s.server_id, convict=False) for s in servers))


async def start_no_wait(manager: ScyllaClusterManager, server):
    """Starts a node added with start=False without waiting for it to join,
    like ccm's node.start(no_wait=True); returns its log and the log mark
    from before the start."""
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_start(server.server_id, expected_server_up_state=ServerUpState.PROCESS_STARTED,
                               connect_driver=False)
    return log, mark


async def get_space_used(manager: ScyllaClusterManager, server, table_name="cf") -> int:
    """The dtest's get_space_used(): "Space used (total)" of the table in
    `nodetool cfstats`."""
    output = (await nodetool(manager, server, "cfstats")).stdout
    if output.find(table_name) != -1:
        output = output[output.find(table_name):]
        output = output[output.find("Space used (total)"):]
        return int(output[output.find(":") + 1:output.find("\n")].strip())
    return -1


def assert_almost_equal(*args, error=0.16):
    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error) or vmin == vmax, f"values not within {error * 100:.2f}% of the max: {args}"


class Runner(threading.Thread):
    """The dtests' go(): runs func(i) once a second in a thread until it
    fails or is stopped; check() raises its failure."""

    def __init__(self, func, sleep=1.0):
        super().__init__(daemon=True)
        self.func = func
        self.sleep = sleep
        self.error = None
        self.stopped = False

    def run(self):
        i = 0
        while not self.stopped:
            try:
                self.func(i)
            except Exception as e:  # noqa: BLE001
                self.error = e
                return
            i += 1
            time.sleep(self.sleep)

    def stop(self):
        self.stopped = True
        self.join()
        self.check()

    def check(self):
        if self.error is not None:
            raise self.error


async def validate_off_strategy_started(log, keyspace: str, table: str, from_mark: int):
    """The dtest's _validate_off_strategy_started(): the node logs the start
    of off-strategy compaction of the table after the start of streaming."""
    off_strategy_message = f"Starting off-strategy compaction for {keyspace}.{table}"
    bootstrap_status = None
    off_strategy_run = False
    for line, _ in await log.grep(f"{off_strategy_message}|{BOOTSTRAP_START_LOG_PAT}", from_mark=from_mark):
        if re.search(BOOTSTRAP_START_LOG_PAT, line):
            bootstrap_status = "START"
        if bootstrap_status == "START" and off_strategy_message in line:
            off_strategy_run = True
            break
    assert off_strategy_run, "off-strategy was not started during bootstrap"


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_add_node.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_add_node(manager: ScyllaClusterManager):
    """A third node joins a two-node cluster; then the cluster stops."""
    await manager.servers_add(2, config=SIMPLE_SNITCH)
    await manager.server_add(config=SIMPLE_SNITCH)
    await cluster_stop(manager)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_start_stop_node(manager: ScyllaClusterManager):
    """Three nodes start; the first stops and the others see it down; then
    the cluster stops."""
    servers = await manager.servers_add(3, config=SIMPLE_SNITCH)
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.others_not_see_server(servers[0].ip_addr)
    await stop_servers_gracefully(manager)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_off_strategy_during_bootstrap.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_off_strategy_during_bootstrap(manager: ScyllaClusterManager):
    """A node bootstrapping by repair puts the sstables it receives aside and
    reshapes them with off-strategy compaction, instead of compacting the
    table while streaming; it gets all the data of the single-node cluster
    (RF=1) it joins."""
    config = {**SIMPLE_SNITCH, "enable_repair_based_node_ops": True}
    keys = 10000
    keyspace_name = "ks"
    table_name = "cf"

    node1 = await manager.server_add(config=config)
    cql = cql_one(manager)
    create_ks(cql, keyspace_name, 1)
    create_cf(cql, f"{keyspace_name}.{table_name}", C1C2)
    insert_statement = cql.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, c1, c2) VALUES (?, 'value1', 'value2')")
    execute_concurrent_with_args(cql, insert_statement, [[f"k{k}"] for k in range(keys)])
    await nodetool(manager, node1, "flush")

    node2 = await manager.server_add(config=config, start=False)
    log = await manager.server_open_log(node2.server_id)
    await manager.server_start(node2.server_id)

    await validate_off_strategy_started(log, keyspace_name, table_name, from_mark=0)

    bootstrap_status = None
    for line, _ in await log.grep(f"Compacted .* sstables to |{BOOTSTRAP_START_LOG_PAT}|{BOOTSTRAP_DONE_LOG_PAT}"):
        if re.search(BOOTSTRAP_START_LOG_PAT, line):
            bootstrap_status = "START"
        elif re.search(BOOTSTRAP_DONE_LOG_PAT, line):
            bootstrap_status = "END"
            break
        assert not (bootstrap_status == "START" and f"Compact {keyspace_name}.{table_name} " in line and "Compacted " in line), \
            "Unexpected compaction of test table occurred during bootstrap, off-strategy doesn't work"
    assert bootstrap_status == "END"

    cql = cql_one(manager)
    rows = list(cql.execute(SimpleStatement("SELECT count(*) from ks.cf", consistency_level=ConsistencyLevel.ONE)))
    assert [list(r) for r in rows] == [[keys]]


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_simple_bootstrap.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_bootstrap(manager: ScyllaClusterManager):
    """With one token per node, balanced (ccm's balanced_tokens(2)), reads
    at CL=ONE keep working while a second node bootstraps, and after a
    cleanup the data (RF=1) is split evenly between the two nodes."""
    tokens = [i * (2**64 // 2) - 2**63 for i in range(2)]
    config = {**SIMPLE_SNITCH, "num_tokens": 1}
    keys = 10000

    node1 = await manager.server_add(config={**config, "initial_token": tokens[0]})
    cql = cql_one(manager)
    create_ks(cql, "ks", 1)
    create_cf(cql, "ks.cf", C1C2)
    insert_statement = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
    execute_concurrent_with_args(cql, insert_statement, [[f"k{k}"] for k in range(keys)])

    await nodetool(manager, node1, "flush")
    await nodetool(manager, node1, "compact")
    data_total_size_node1 = await get_space_used(manager, node1)
    logger.info(f"before={data_total_size_node1}")

    # Reads of the inserted data during the whole bootstrap must not fail.
    reader = Runner(lambda _: query_c1c2(cql, random.randint(0, keys - 1), ConsistencyLevel.ONE))
    reader.start()
    try:
        node2 = await manager.server_add(config={**config, "initial_token": tokens[1]})
        await nodetool(manager, node2, "flush")
        await nodetool(manager, node2, "compact")

        reader.check()
        await nodetool(manager, node1, "cleanup")
        await nodetool(manager, node1, "compact")
        await asyncio.sleep(0.5)
        reader.check()
    finally:
        reader.stopped = True
        await asyncio.to_thread(reader.join)

    data_total_size_node1_after = await get_space_used(manager, node1)
    data_total_size_node2_after = await get_space_used(manager, node2)
    logger.info(f"before={data_total_size_node1}, after={data_total_size_node1_after} + {data_total_size_node2_after}")
    assert_almost_equal(data_total_size_node1, data_total_size_node1_after + data_total_size_node2_after, error=0.3)
    assert_almost_equal(data_total_size_node1_after, data_total_size_node2_after, error=0.3)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_read_from_bootstrapped_node(manager: ScyllaClusterManager):
    """A bootstrapped node returns the data written before it joined.
    CASSANDRA-6648."""
    servers = await manager.servers_add(3, config=SIMPLE_SNITCH)
    await cassandra_stress(manager, servers[0], "write", "n=10000", "-rate", "threads=8")
    cql = cql_one(manager)
    table = "keyspace1.standard1"
    original_rows = list(cql.execute(f"SELECT * FROM {table}"))

    node4 = await manager.server_add(config=SIMPLE_SNITCH)
    exclusive = await exclusive_one(manager, node4)
    assert list(exclusive.execute(f"SELECT * FROM {table}")) == original_rows


LOCAL_QUORUM_STRESS_PROFILE = """
# Create the keyspace and table
keyspace: keyspace1
keyspace_definition: |
  CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
table: users
table_definition:
  CREATE TABLE users (
    username text,
    first_name text,
    last_name text,
    email text,
    PRIMARY KEY(username)
  ) WITH compaction = {'class':'SizeTieredCompactionStrategy'};
insert:
  partitions: fixed(1)
  batchtype: UNLOGGED
queries:
  read:
    cql: select * from users where username = ?
    fields: samerow
"""


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_local_quorum_bootstrap.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_local_quorum_bootstrap(manager: ScyllaClusterManager, tmp_path):
    """Writes at LOCAL_QUORUM succeed while a node bootstraps in the other
    DC. CASSANDRA-8058."""
    node1 = await manager.server_add(config=DTEST_CONFIG, property_file={"dc": "dc1", "rack": "rack1"})
    await manager.server_add(config=DTEST_CONFIG, property_file={"dc": "dc2", "rack": "rack1"})

    stress_config = tmp_path / "stress.yaml"
    stress_config.write_text(LOCAL_QUORUM_STRESS_PROFILE)
    await cassandra_stress(manager, node1, "user", f"profile={stress_config}", "n=2000000", "ops(insert=1)", "-rate", "threads=50",
                           create_keyspace=False)

    node3 = await manager.server_add(config=DTEST_CONFIG, property_file={"dc": "dc2", "rack": "rack1"}, start=False)
    await start_no_wait(manager, node3)
    await asyncio.sleep(3)

    output = await cassandra_stress(manager, node1, "user", f"profile={stress_config}", "ops(insert=1)", "n=500000", "cl=LOCAL_QUORUM",
                                    "-rate", "threads=5", "-errors", "retries=2", create_keyspace=False)
    assert re.search("Operation.+error inserting key.+Exception", output) is None, "Error during stress while bootstrapping"

    # Avoid reporting bootstrap errors in logs
    await manager.server_stop(node3.server_id, convict=False)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_decommissioned_wiped_node_can_join.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_decommissioned_wiped_node_can_join(manager: ScyllaClusterManager):
    """A node that joined and was decommissioned joins again once its data
    is wiped, bootstrapping with off-strategy compaction of the data it
    receives. CASSANDRA-9765."""
    servers = await manager.servers_add(3, config=SIMPLE_SNITCH)
    keyspace_name = "keyspace1"
    table_name = "standard1"
    query = f"SELECT * FROM {keyspace_name}.{table_name}"

    node1 = servers[0]
    await cassandra_stress(manager, node1, "write", "n=10K", "-rate", "threads=8")
    original_rows = [list(r) for r in cql_one(manager).execute(query)]

    # A new node, bootstrapping (not a seed).
    node4 = await manager.server_add(config=SIMPLE_SNITCH)
    cql = cql_one(manager)
    rows = [list(r) for r in cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM))]
    assert sorted(rows) == sorted(original_rows)

    # Decommission the new node and wipe its data.
    node4_host_id = await manager.get_host_id(node4.server_id)
    log1 = await manager.server_open_log(node1.server_id)
    mark = await log1.mark()
    await nodetool(manager, node4, "decommission")
    await manager.server_stop_gracefully(node4.server_id)
    await log1.wait_for(f"({re.escape(node4.ip_addr)}|{node4_host_id}) is now (dead|DOWN)", from_mark=mark)
    await wipe_server_data(manager, node4.server_id)

    # Now it starts again, and is allowed to join.
    log4 = await manager.server_open_log(node4.server_id)
    mark = await log4.mark()
    await manager.server_start(node4.server_id)
    assert await log4.grep(BOOTSTRAP_START_LOG_PAT, from_mark=mark)
    await validate_off_strategy_started(log4, keyspace_name, table_name, from_mark=mark)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_failed_bootstrap_wiped_node_can_join.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_failed_bootstrap_wiped_node_can_join(manager: ScyllaClusterManager):
    """A node killed while it bootstraps (streaming is throttled to 1 MB/s
    so it is still streaming) is removed by the cluster, and once its data
    is wiped it joins. CASSANDRA-9765."""
    node1 = await manager.server_add(config=SIMPLE_SNITCH)
    # Enough data for the bootstrap to be killed while streaming.
    await cassandra_stress(manager, node1, "write", "n=10k", "-rate", "threads=8")
    await nodetool(manager, node1, "flush")
    cql = cql_one(manager)
    list(cql.execute("SELECT * FROM keyspace1.standard1"))

    node2 = await manager.server_add(config={**SIMPLE_SNITCH, "stream_io_throughput_mb_per_sec": 1}, start=False)
    log1 = await manager.server_open_log(node1.server_id)
    mark = await log1.mark()
    # KillOnBootstrap: kill node2 when it starts streaming.
    log2, mark2 = await start_no_wait(manager, node2)
    await log2.wait_for(r"Starting to bootstrap|raft topology: start streaming|raft_topology - start streaming", from_mark=mark2)
    await manager.server_stop(node2.server_id, convict=False)

    # The other nodes notice it.
    await log1.wait_for(f"Finished to force remove node ({re.escape(node2.ip_addr)}|[0-9a-f-]{{36}})", from_mark=mark)

    await wipe_server_data(manager, node2.server_id)
    # On rejoin the streaming throughput is not limited, to speed up the test.
    await manager.server_update_config(node2.server_id, "stream_io_throughput_mb_per_sec", 0)

    manager.ignore_log_patterns += [
        "raft topology: CDC generation publisher fiber got error exceptions::unavailable_exception",
        "raft_topology - CDC generation publisher fiber got error exceptions::unavailable_exception",
        r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)",
    ]
    mark2 = await log2.mark()
    await manager.server_start(node2.server_id)
    assert await log2.grep(BOOTSTRAP_START_LOG_PAT, from_mark=mark2)


async def full_cluster_recovery_after_stop(manager: ScyllaClusterManager, gently, num_of_nodes, rf):
    """The dtest's _full_cluster_recovery_after_stop(): all the nodes of a
    cluster with RF racks stop together, gracefully or killed, after the
    data is flushed, and start again: no data is lost."""
    racks = {f"rack{r + 1}": num_of_nodes // rf for r in range(rf)}
    servers = await manager.servers_add(num_of_nodes, config=DTEST_CONFIG, property_file=topology(racks, dc="dc1"))
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2)
    insert_c1c2(cql, keys=range(10000), consistency=ConsistencyLevel.QUORUM)
    await flush_all(manager, servers)

    await cluster_stop(manager, gently=gently)
    for s in servers:
        await manager.server_start(s.server_id)
    cql = dtest_session(await reconnect_driver(manager))
    for key in range(10000):
        query_c1c2(cql, key, ConsistencyLevel.QUORUM)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_full_cluster_recovery_after_forcibly_stop_3_nodes_rf_3.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_full_cluster_recovery_after_forcibly_stop_3_nodes_rf_3(manager: ScyllaClusterManager):
    await full_cluster_recovery_after_stop(manager, gently=False, num_of_nodes=3, rf=3)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_full_cluster_recovery_after_gentle_stop_6_nodes_rf_2.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_full_cluster_recovery_after_gentle_stop_6_nodes_rf_2(manager: ScyllaClusterManager):
    await full_cluster_recovery_after_stop(manager, gently=True, num_of_nodes=6, rf=2)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_full_cluster_recovery_after_forcibly_stop_4_nodes_rf_1.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_full_cluster_recovery_after_forcibly_stop_4_nodes_rf_1(manager: ScyllaClusterManager):
    await full_cluster_recovery_after_stop(manager, gently=False, num_of_nodes=4, rf=1)


async def write_in_background(manager: ScyllaClusterManager, server, duration_seconds: int):
    """The dtest's cassandra-stress write at CL=TWO for the given time, into
    the keyspace "keyspace" with RF=2, which cassandra-stress creates."""
    return await cassandra_stress(manager, server, "write", "cl=TWO", f"duration={duration_seconds}s", "-rate", "threads=10",
                                  "-log", "interval=5", "-schema", "replication(factor=2) keyspace=keyspace",
                                  create_keyspace=False)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("is_gracefully", [True, False], ids=["gracefully", "force"])
async def test_cluster_become_unavailable_when_kill_node_during_bootstrap(manager: ScyllaClusterManager, is_gracefully):
    """
    On two nodes with RF=2, a third node is stopped, gracefully or killed,
    while it streams during bootstrap under writes at CL=TWO. The other
    nodes remove it from gossip, and writes at CL=TWO succeed again.
    Bootstrap uses repair-based node operations, as in the dtest.
    """
    # As on the dtest's nodes (conf/scylla.yaml), writes time out after 2 s,
    # and ring_delay_ms is the dtests' default 10 s (test.py's is 0), which
    # the bootstrap's rollback waits for: meanwhile a write to the stopped
    # pending replica fails, where test.py's 30 s timeout would outlast the
    # rollback.
    config = {**DTEST_CONFIG, "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission",
              "write_request_timeout_in_ms": 2000, "ring_delay_ms": 10000}
    manager.ignore_log_patterns += [
        r"bootstrap.*failed",
        r"raft_topology - raft_topology_cmd.*failed with: (?:seastar::abort_requested_exception|abort requested)",
    ]
    servers = await manager.servers_add(2, config=config, property_file=[{"dc": "dc1", "rack": r} for r in ["r1", "r2"]])
    node1 = servers[0]
    # Data in the keyspace, so the new node bootstraps it.
    await write_in_background(manager, node1, 5)

    writes = asyncio.create_task(write_in_background(manager, node1, 30))
    new_node = await manager.server_add(start=False, config=config, property_file={"dc": "dc1", "rack": "r1"})
    log = await manager.server_open_log(new_node.server_id)
    mark = await log.mark()
    await manager.server_start(new_node.server_id, expected_server_up_state=ServerUpState.PROCESS_STARTED,
                               connect_driver=False)
    await log.wait_for(BOOTSTRAP_START_LOG_PAT, from_mark=mark)
    await log.wait_for(r"Beginning stream session|sync data for keyspace=keyspace, status=started", from_mark=mark)

    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await l.mark() for l in logs]
    if is_gracefully:
        await manager.server_stop_gracefully(new_node.server_id)
    else:
        await manager.server_stop(new_node.server_id, convict=False)
    for l, m in zip(logs, marks):
        await l.wait_for(rf"gossip - Removed endpoint ({new_node.ip_addr}|[0-9a-f-]{{36}})", from_mark=m)

    # cassandra-stress may fail at CL=TWO while the stopped node was a pending replica.
    try:
        await writes
    except AssertionError as e:
        logger.info("cassandra-stress failed while the bootstrap was rolled back: %s", e)
    # Once the bootstrap is rolled back, it succeeds.
    await write_in_background(manager, node1, 5)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_reject_node_bootstrap_no_gossip.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_reject_node_bootstrap_no_gossip(manager: ScyllaClusterManager):
    """After a full cluster restart with node3 still down, a new node4 is
    rejected; once node3 is back, node4 (wiped) joins, and with node3
    stopped again, the data is readable at QUORUM through node4."""
    expected_error = (r"Startup failed:.* has gossip status=UNKNOWN|Startup failed:.*topology coordinator rejected request to join the cluster"
                      r"|received notification of being banned from the cluster")
    manager.ignore_log_patterns += [expected_error]

    node1, node2, node3 = await manager.servers_add(3, config=DTEST_CONFIG,
                                                    property_file=topology({"rack1": 1, "rack2": 1, "rack3": 1}, dc="dc1"))
    session = await exclusive_one(manager, node1)
    n_of_keys = 1000
    create_ks(session, "ks", 3)
    create_cf(session, "ks.cf", C1C2)
    insert_c1c2(session, keys=range(n_of_keys), consistency=ConsistencyLevel.ALL)

    await cluster_stop(manager)
    await manager.server_start(node1.server_id)
    await manager.server_start(node2.server_id)

    # ccm's cluster.new_node(4, data_center=1, rack=2): datacenter "1", rack "2".
    node4 = await manager.server_add(config=DTEST_CONFIG, property_file={"dc": "1", "rack": "2"}, start=False)
    await manager.server_start(node4.server_id, expected_error=expected_error)

    await manager.server_start(node3.server_id)
    await wipe_server_data(manager, node4.server_id)
    await manager.server_start(node4.server_id)
    await manager.server_stop_gracefully(node3.server_id)
    session = await exclusive_one(manager, node4)
    for k in range(n_of_keys):
        query_c1c2(session, k, consistency=ConsistencyLevel.QUORUM)


# Ported from scylla-dtest bootstrap_test.py::TestBootstrap::test_reject_bootstrap_wiped_node_misspelled_seeds.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_reject_bootstrap_wiped_node_misspelled_seeds(manager: ScyllaClusterManager):
    """
    A stopped node, wiped and restarted with the same IP address and
    cluster name but without the other nodes as seeds (as when the "seeds"
    key of its configuration is misspelled), must not join the cluster:
    "nodetool status" of the other nodes keeps showing it down.
    The node is given itself as its only seed: server_start() rewrites the
    seeds of the configuration file, so a hand-edited key would not last.
    Reproduces https://github.com/scylladb/scylla-enterprise/issues/3523
    """
    node1, node2, node3 = await manager.servers_add(3, config=SIMPLE_SNITCH)
    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)
    await wipe_server_data(manager, node2.server_id)

    await manager.server_start(node2.server_id, seeds=[node2.ip_addr],
                               expected_server_up_state=ServerUpState.PROCESS_STARTED, connect_driver=False)

    for node in [node1, node3]:
        await wait_for_nodes_status(manager, node, ["UN", "DN", "UN"])

    await cluster_stop(manager)
