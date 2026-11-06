#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Alternator Streams on a multi-node cluster, ported from scylla-dtest
# alternator_stream_tests.py: every node serves the same stream, the stream
# keeps its records across topology changes, ListStreams pages by its Limit,
# and the open and closed shards of the stream follow the CDC generations
# made by nodes joining and leaving. test/alternator covers the API on a
# single node.
#
# Like the dtests' clusters: the nodes are started one by one, each in a
# rack of its own, keyspaces use vnodes, writes always go through LWT
# (alternator_write_isolation: always), hinted handoff is off and
# ring_delay_ms is 5 s.

import asyncio
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from copy import copy

import boto3
import botocore
import pytest
import requests
from boto3.dynamodb.types import TypeDeserializer

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

ALTERNATOR_PORT = 8080
DTEST_STREAMS_CONFIG = {
    "alternator_port": ALTERNATOR_PORT,
    "alternator_write_isolation": "always",
    "tablets_mode_for_new_keyspaces": "disabled",
    "experimental_features": ["cdc", "alternator-streams"],
    "ring_delay_ms": 5000,
    "hinted_handoff_enabled": False,
}
TABLE_NAME = "user_table"
NUM_OF_ITEMS = 100
HASH_KEY_NAME = "hash_key"
HASH_SCHEMA = {
    "KeySchema": [{"AttributeName": HASH_KEY_NAME, "KeyType": "HASH"}],
    "AttributeDefinitions": [{"AttributeName": HASH_KEY_NAME, "AttributeType": "S"}],
}
KEYS_ONLY = {"StreamSpecification": {"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"}}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def boto_params():
    """The dtests' Alternator Streams boto3 parameters (5 retries)."""
    return dict(aws_access_key_id="alternator", aws_secret_access_key="None", region_name="None", verify=False,
                config=botocore.client.Config(retries={"max_attempts": 5}, read_timeout=300))


def alternator_url(server):
    return f"http://{server.ip_addr}:{ALTERNATOR_PORT}"


def dynamodb(server):
    return boto3.resource("dynamodb", endpoint_url=alternator_url(server), **boto_params())


def dynamodbstreams(server):
    return boto3.client("dynamodbstreams", endpoint_url=alternator_url(server), **boto_params())


def wait_for_alternator(server, timeout=300):
    """The dtests' wait_for_alternator(): GET / until the node answers."""
    deadline = time.time() + timeout
    while True:
        try:
            requests.get(f"{alternator_url(server)}/", verify=False).raise_for_status()
            return
        except requests.exceptions.ConnectionError:
            if time.time() > deadline:
                raise
        time.sleep(1)


async def add_node(manager: ScyllaClusterManager, dc, rack, extra_config=None, cmdline=None):
    server = await manager.server_add(config=DTEST_STREAMS_CONFIG | (extra_config or {}), cmdline=cmdline,
                                      property_file={"dc": dc, "rack": rack})
    await asyncio.to_thread(wait_for_alternator, server)
    return server


async def prepare_cluster(manager: ScyllaClusterManager, build_mode, num_of_nodes=3, topo=None, extra_config=None):
    """The dtests' prepare_dynamodb_cluster() of the streams tests
    (populate_sequentially()): one rack per node in dc1 unless a topology
    {dc: {rack: nodes}} is given, nodes started one by one."""
    if topo is None:
        topo = {"dc1": {f"rack{r}": 1 for r in range(1, num_of_nodes + 1)}}
    layout = [(dc, rack) for dc, racks in topo.items() for rack, n in racks.items() for _ in range(n)]
    servers = []
    for i, (dc, rack) in enumerate(layout):
        # The first node gets populate_sequentially()'s extra arguments.
        cmdline = ["--blocked-reactor-notify-ms", "1000000" if build_mode == "debug" else "100"] if i == 0 else None
        servers.append(await add_node(manager, dc, rack, extra_config, cmdline))
    return servers


def create_table(server, table_name=TABLE_NAME):
    resource = dynamodb(server)
    table = resource.create_table(TableName=table_name, BillingMode="PAY_PER_REQUEST", **HASH_SCHEMA, **KEYS_ONLY)
    resource.meta.client.get_waiter("table_exists").wait(TableName=table_name)
    resource.meta.client.describe_table(TableName=table_name)
    return table


def wait_for_active_stream(server, table_name=TABLE_NAME, timeout=60):
    """The stream ARN and label of the table, once its stream is enabled."""
    client = dynamodbstreams(server)
    for _ in range(timeout):
        for stream in client.list_streams(TableName=table_name)["Streams"]:
            arn = stream["StreamArn"]
            if arn:
                describe = client.describe_stream(StreamArn=arn)["StreamDescription"]
                if describe.get("StreamStatus", "ENABLED") == "ENABLED":
                    return arn, stream["StreamLabel"]
        time.sleep(1)
    raise AssertionError(f"The stream ARN of '{table_name}' table not found")


def create_items(num_of_items=NUM_OF_ITEMS):
    return [{HASH_KEY_NAME: f"test{i}", "x": {"hello": f"world{i}"}} for i in range(num_of_items)]


def batch_write(server, new_items, table_name=TABLE_NAME, ignore_errors=False):
    """The dtests' batch_write_actions(): errors while queueing the items
    are ignored on request, the final flush's are not."""
    table = dynamodb(server).Table(table_name)
    with table.batch_writer() as batch:
        try:
            for item in new_items:
                batch.put_item(item)
        except Exception as error:
            if not ignore_errors:
                raise
            logger.info(f"Continuing after exception: {error}")
    return table


def prefill_table(server, table_name=TABLE_NAME, num_of_items=NUM_OF_ITEMS):
    create_table(server, table_name)
    arn = wait_for_active_stream(server, table_name)[0]
    batch_write(server, create_items(num_of_items), table_name)
    return arn


class StoppableThread:
    """The dtests' StoppableThread: runs the target in a loop until stopped;
    join() stops it and re-raises the exception that ended it, if any."""

    def __init__(self, target, **kwargs):
        self._stop_event = threading.Event()
        self.target = target
        self.kwargs = kwargs
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future = self.pool.submit(self.run)

    def run(self):
        while not self._stop_event.is_set():
            self.target(**self.kwargs)

    def join(self):
        self._stop_event.set()
        try:
            return self.future.result()
        finally:
            self.pool.shutdown(wait=True)


def run_write_stress(server, num_of_items, table_name=TABLE_NAME):
    """The dtests' run_write_stress(ignore_errors=True)."""
    return StoppableThread(lambda: batch_write(server, create_items(num_of_items), table_name, ignore_errors=True))


def get_responses(server, stream_arn, num_of_requests, timeout=None):
    """The dtests' get_responses(): reads the stream's records through the
    node from the start of every shard until num_of_requests records were
    read, or the time window passed."""
    client = dynamodbstreams(server)
    soft_timeout = 60
    hard_timeout = timeout or 60 * 5

    def read():
        responses, shard_iterators, next_iterators = [], [], []
        describe_stream = client.describe_stream(StreamArn=stream_arn)
        while True:
            for shard in describe_stream["StreamDescription"]["Shards"]:
                shard_iterators.append(client.get_shard_iterator(
                    StreamArn=stream_arn, ShardId=shard["ShardId"], ShardIteratorType="AT_SEQUENCE_NUMBER",
                    SequenceNumber=shard["SequenceNumberRange"]["StartingSequenceNumber"])["ShardIterator"])
            last_shard = describe_stream["StreamDescription"].get("LastEvaluatedShardId")
            if not last_shard:
                break
            describe_stream = client.describe_stream(StreamArn=stream_arn, ExclusiveStartShardId=last_shard)
        stop = False
        start_time = time.time()
        while len(responses) < num_of_requests and not stop:
            for shard_iterator in shard_iterators:
                elapsed_time = time.time() - start_time
                if elapsed_time > soft_timeout:
                    time.sleep(10)
                if elapsed_time > hard_timeout:
                    stop = True
                    break
                response = client.get_records(ShardIterator=shard_iterator, Limit=1000)
                if response.get("NextShardIterator"):
                    next_iterators.append(response["NextShardIterator"])
                if response.get("Records"):
                    responses.extend(response["Records"])
            shard_iterators = copy(next_iterators)
            next_iterators.clear()
        return responses

    start_time = time.time()
    responses = read()
    while len(responses) < num_of_requests and hard_timeout - (time.time() - start_time) > 0:
        time.sleep(hard_timeout - (time.time() - start_time))
        responses = read()
    return responses


def inserted_keys(responses):
    """The dtests' extract_data_from_responses() of the INSERT records of a
    KEYS_ONLY stream, as a set of hash keys."""
    deserializer = TypeDeserializer()
    return {deserializer.deserialize(r["dynamodb"]["Keys"][HASH_KEY_NAME])
            for r in responses if r["eventName"] == "INSERT"}


def verify_items(server, stream_arn, expected_items):
    """Reads len(expected_items) records of the stream through the node and
    checks they are the INSERTs of the expected items' keys."""
    responses = get_responses(server, stream_arn, len(expected_items))
    expected = {item[HASH_KEY_NAME] for item in expected_items}
    got = inserted_keys(responses)
    assert got == expected, f"missing keys: {expected - got}, unexpected keys: {got - expected}"


class StreamsTable:
    """The dtests' StreamsTable: the shards of a stream as seen through one node."""

    def __init__(self, stream_arn, server):
        self.stream_arn = stream_arn
        self.client = dynamodbstreams(server)
        self.shards = []
        self.update_shards()

    def update_shards(self):
        describe_stream = self.client.describe_stream(StreamArn=self.stream_arn)
        shards = describe_stream["StreamDescription"]["Shards"]
        last_shard = describe_stream["StreamDescription"].get("LastEvaluatedShardId")
        while last_shard:
            describe_stream = self.client.describe_stream(StreamArn=self.stream_arn, ExclusiveStartShardId=last_shard)
            shards.extend(describe_stream["StreamDescription"]["Shards"])
            last_shard = describe_stream["StreamDescription"].get("LastEvaluatedShardId")
        logger.info(f"Shards [{len(self.shards)}] updated to [{len(shards)}], open shards "
                    f"[{self.count_open_shards()}] updated to [{self.count_open_shards(shards)}]")
        self.shards = shards

    @staticmethod
    def is_shard_open(shard):
        return not shard["SequenceNumberRange"].get("EndingSequenceNumber")

    def count_open_shards(self, shards=None):
        return len([s for s in (shards or self.shards) if self.is_shard_open(s)])

    def open_shards(self):
        return [s for s in self.shards if self.is_shard_open(s)]


async def wait_for_topology_log(manager: ScyllaClusterManager, server, log, marks, pattern, nodetool_state, timeout):
    """The dtests' wait_for_running_decommission() / wait_for_running_add_node():
    waits for the raft topology coordinator's log line on the node, or for
    "nodetool status" to show a node in the given state."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await log.grep(pattern, from_mark=marks["mark"]):
            marks["mark"] = await log.mark()
            return
        out, _, _ = await run_nodetool(manager, server, "status")
        if nodetool_state in out:
            marks["mark"] = await log.mark()
            return
        await asyncio.sleep(2)
    raise AssertionError(f"No {nodetool_state} topology change detected on {server.ip_addr} within {timeout}s")


async def wait_for_running_decommission(manager: ScyllaClusterManager, server, log, marks, timeout=1800):
    await wait_for_topology_log(manager, server, log, marks, "raft_topology - updating topology state: start decommission",
                                "UL", timeout)


async def wait_for_running_add_node(manager: ScyllaClusterManager, server, log, marks, timeout=900):
    await wait_for_topology_log(manager, server, log, marks, "raft_topology - updating topology state: bootstrap: accept node",
                                "UJ", timeout)


# Ported from scylla-dtest alternator_stream_tests.py::TestAlternatorStreams::test_verify_all_nodes_have_same_stream.
# The INSERT records of 100 items written through node1 are all read from
# the stream through each of the three nodes.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_verify_all_nodes_have_same_stream(manager: ScyllaClusterManager, build_mode):
    servers = await prepare_cluster(manager, build_mode, num_of_nodes=3)
    stream_arn = await asyncio.to_thread(prefill_table, servers[0])
    expected_items = create_items()
    for server in servers:
        await asyncio.to_thread(verify_items, server, stream_arn, expected_items)


# Ported from scylla-dtest alternator_stream_tests.py::TestAlternatorStreams::test_verify_stream_records_after_topology_changed.
# The stream keeps all the INSERT records as nodes join, node1 is
# decommissioned, a node restarts and a node is killed: after each step
# 100 more items are written through another node and all the records so
# far are read through a node.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_verify_stream_records_after_topology_changed(manager: ScyllaClusterManager, build_mode):
    extra_config = {"uuid_sstable_identifiers_enabled": False}
    cluster_size = 3
    node1, node2, node3 = await prepare_cluster(manager, build_mode, num_of_nodes=cluster_size, extra_config=extra_config)
    await asyncio.to_thread(create_table, node1)
    items = create_items(num_of_items=400)
    stream_arn = (await asyncio.to_thread(wait_for_active_stream, node1))[0]

    # Two new nodes, in rack1 and rack2.
    node4, node5 = [await add_node(manager, "dc1", f"rack{((idx - 1) % cluster_size) + 1}", extra_config)
                    for idx in range(cluster_size + 1, cluster_size + 3)]

    expected_items = items[:100]
    await asyncio.to_thread(batch_write, node1, items[:100])
    await asyncio.to_thread(verify_items, node1, stream_arn, expected_items)

    expected_items.extend(items[100:200])
    await asyncio.to_thread(batch_write, node2, items[100:200])
    await manager.decommission_node(node1.server_id)
    await asyncio.to_thread(verify_items, node4, stream_arn, expected_items)

    expected_items.extend(items[200:300])
    await asyncio.to_thread(batch_write, node3, items[200:300])
    await manager.server_stop_gracefully(node4.server_id)
    await manager.others_not_see_server(node4.ip_addr)
    await manager.server_start(node4.server_id)
    await manager.servers_see_each_other(await manager.running_servers())
    await asyncio.to_thread(wait_for_alternator, node4)
    await asyncio.to_thread(verify_items, node3, stream_arn, expected_items)

    expected_items.extend(items[300:400])
    await asyncio.to_thread(batch_write, node4, items[300:400])
    # ccm's cluster.remove(node4, wait_other_notice=True) kills the node.
    await manager.server_stop(node4.server_id, convict=False)
    await manager.others_not_see_server(node4.ip_addr)
    await asyncio.to_thread(verify_items, node5, stream_arn, expected_items)


# Ported from scylla-dtest alternator_stream_tests.py::TestAlternatorStreams::test_list_streams_limit_parameter.
# ListStreams through random nodes pages the streams of 20 tables by Limit
# values 5, 7, 3, 1 and 4: each page has Limit streams, together they are
# the streams of all the tables, and the page after the last one is empty.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_list_streams_limit_parameter(manager: ScyllaClusterManager, build_mode):
    servers = await prepare_cluster(manager, build_mode, num_of_nodes=3)
    node1 = servers[0]
    table_names = [TABLE_NAME + str(idx) for idx in range(20)]

    def check():
        for table_name in table_names:
            create_table(node1, table_name)
        tables_arns = [wait_for_active_stream(node1, table_name)[0] for table_name in table_names]
        total_limited_stream_responses = []
        last_evaluated_stream_arn = None
        for limit in [5, 7, 3, 1, 4]:
            params = {"Limit": limit}
            if last_evaluated_stream_arn:
                params["ExclusiveStartStreamArn"] = last_evaluated_stream_arn
            client = dynamodbstreams(random.choice(servers))
            result = client.list_streams(**params)
            assert len(result["Streams"]) == limit, f"Got {len(result['Streams'])} streams for limit {limit}"
            total_limited_stream_responses += result["Streams"]
            last_evaluated_stream_arn = result["LastEvaluatedStreamArn"]
        assert sorted(tables_arns) == sorted(s["StreamArn"] for s in total_limited_stream_responses)
        empty_streams_list = client.list_streams(ExclusiveStartStreamArn=last_evaluated_stream_arn)["Streams"]
        assert len(empty_streams_list) == 0, f"Got streams after the last evaluated stream: {empty_streams_list}"

    await asyncio.to_thread(check)


# Ported from scylla-dtest alternator_stream_tests.py::TestAlternatorStreams::test_updated_shards_during_add_decommission_node.
# While items are written and the last node is repeatedly decommissioned
# and bootstrapped back into its rack, the open shards of the stream change
# once a full add-node and decommission cycle was seen: each topology
# change makes a new CDC generation (vnodes).
# The dtest is not next_gating: https://github.com/scylladb/scylladb/issues/15260
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_updated_shards_during_add_decommission_node(manager: ScyllaClusterManager, build_mode):
    topo = {"dc1": {"rack1": 1, "rack2": 1, "rack3": 2}}
    # A re-bootstrapped node has incomplete tablet stats, so the next drain
    # waits for this refresh (default 60s).
    extra_config = {"tablet_load_stats_refresh_interval_in_seconds": 1}
    servers = await prepare_cluster(manager, build_mode, num_of_nodes=4, topo=topo, extra_config=extra_config)
    node1 = servers[0]
    stream_arn = await asyncio.to_thread(prefill_table, node1)
    write_stress = run_write_stress(node1, num_of_items=1000)
    # Seed the log positions before the churn starts, so that only its
    # cycles are matched (node1's log already has a "bootstrap: accept node"
    # line for each node of the initial cluster).
    log = await manager.server_open_log(node1.server_id)
    add_node_marks = {"mark": await log.mark()}
    decommission_marks = {"mark": await log.mark()}

    stop_churn = asyncio.Event()

    async def decommission_then_add_node():
        """The dtests' run_decommission_then_add_node() in a loop: the last
        node is decommissioned and a node bootstraps into its rack."""
        node, running = servers[-1], True
        while not stop_churn.is_set():
            # A node that failed to bootstrap is not running: it is not
            # decommissioned again, only replaced by a new bootstrap.
            if running:
                try:
                    await manager.decommission_node(node.server_id)
                except Exception as error:
                    logger.info(f"Decommissioning {node.ip_addr} failed with: {error}")
                    continue
                running = False
            try:
                node = await add_node(manager, "dc1", "rack3", extra_config)
            except Exception as error:
                logger.info(f"Bootstrapping a node into rack3 failed with: {error}")
                continue
            running = True
            await asyncio.sleep(5)

    churn = asyncio.create_task(decommission_then_add_node())
    try:
        await wait_for_running_decommission(manager, node1, log, decommission_marks)
        streams_table = await asyncio.to_thread(StreamsTable, stream_arn, node1)

        async def open_shards_after_topology_change():
            await wait_for_running_add_node(manager, node1, log, add_node_marks)
            await wait_for_running_decommission(manager, node1, log, decommission_marks)
            await asyncio.to_thread(streams_table.update_shards)
            assert streams_table.count_open_shards(), "No open shards found"
            return streams_table.open_shards()

        def canonical(shards):
            return sorted(json.dumps(s, sort_keys=True) for s in shards)

        for _ in range(7):
            current_open_shards = streams_table.open_shards()
            if canonical(current_open_shards) != canonical(await open_shards_after_topology_change()):
                break
            await asyncio.sleep(1)
        else:
            raise AssertionError("The open shards did not change after topology changes")
    finally:
        stop_churn.set()
        try:
            await churn
        finally:
            await asyncio.to_thread(write_stress.join)


# Ported from scylla-dtest alternator_stream_tests.py::TestAlternatorStreams::test_added_node_gets_closed_shards.
# While items are written, a node bootstraps; the stream read through the
# new node then has closed shards (with an EndingSequenceNumber).
# The dtest is not next_gating: https://github.com/scylladb/scylladb/issues/15260
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_added_node_gets_closed_shards(manager: ScyllaClusterManager, build_mode):
    servers = await prepare_cluster(manager, build_mode, num_of_nodes=3)
    node1 = servers[0]
    stream_arn = await asyncio.to_thread(prefill_table, node1)
    stress_thread = run_write_stress(node1, num_of_items=1000)
    try:
        # ccm's new_node() takes the first node's dc and rack.
        node4 = await add_node(manager, "dc1", "rack1")
        streams_table = await asyncio.to_thread(StreamsTable, stream_arn, node4)
        for _ in range(90):
            await asyncio.to_thread(streams_table.update_shards)
            if len(streams_table.shards) > streams_table.count_open_shards():
                break
            await asyncio.sleep(2)
        else:
            raise AssertionError(f"New node {node4.ip_addr} has no closed shards")
    finally:
        await asyncio.to_thread(stress_thread.join)
