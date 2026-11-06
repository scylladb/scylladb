#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Alternator requests on multi-node clusters, ported from scylla-dtest
# alternator_tests.py: queries by key conditions and filters while a node
# is down, conditional updates under each write isolation from two
# datacenters, scans while nodes restart, items of every DynamoDB type,
# nested attribute updates while nodes are decommissioned and added,
# tracing of each request, ADD of mismatched types, concurrent ADDs to one
# item, and enforced authorization across nodes.
#
# Like the dtests' clusters: each node is in a rack of its own, keyspaces
# use vnodes, and writes always go through LWT (alternator_write_isolation:
# always), which is the dtests' node configuration.

import asyncio
import logging
import operator
import random
import string
import threading
import time
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from enum import Enum, auto
from uuid import UUID

import boto3
import boto3.dynamodb.types
import botocore
import pytest
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import InvalidRequest

from test.cluster.util import stop_servers_gracefully
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

ALTERNATOR_PORT = 8080
DTEST_ALTERNATOR_CONFIG = {
    "alternator_port": ALTERNATOR_PORT,
    "alternator_write_isolation": "always",
    "tablets_mode_for_new_keyspaces": "disabled",
    # As on dtest nodes: deleting a table snapshots it.
    "auto_snapshot": True,
}
TABLE_NAME = "user_table"
NUM_OF_ITEMS = 100
DEFAULT_STRING_LENGTH = 5
HASH_KEY_NAME = "hash_key"
RANGE_KEY_NAME = "range_key"


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def schema(range_type=None, hash_name=HASH_KEY_NAME, range_name=RANGE_KEY_NAME):
    key_schema = [{"AttributeName": hash_name, "KeyType": "HASH"}]
    attrs = [{"AttributeName": hash_name, "AttributeType": "S"}]
    if range_type:
        key_schema.append({"AttributeName": range_name, "KeyType": "RANGE"})
        attrs.append({"AttributeName": range_name, "AttributeType": range_type})
    return {"KeySchema": key_schema, "AttributeDefinitions": attrs}


HASH_SCHEMA = schema()
HASH_AND_NUM_RANGE_SCHEMA = schema("N")
HASH_AND_STR_RANGE_SCHEMA = schema("S")
HASH_AND_BINARY_RANGE_SCHEMA = schema("B")
CONDITION_EXPRESSION_SCHEMA = {
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "c", "KeyType": "RANGE"}],
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "c", "AttributeType": "N"}],
}


class WriteIsolation(Enum):
    ALWAYS_USE_LWT = "always_use_lwt"
    FORBID_RMW = "forbid_rmw"
    ONLY_RMW_USES_LWT = "only_rmw_uses_lwt"
    UNSAFE_RMW = "unsafe_rmw"


def random_string(length, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(length))


def dynamodb(ip, secret="None"):
    """The dtests' boto3 resource: user "alternator", the role's salted hash
    as secret (a placeholder unless authorization is enforced)."""
    return boto3.resource("dynamodb", endpoint_url=f"http://{ip}:{ALTERNATOR_PORT}",
                          aws_access_key_id="alternator", aws_secret_access_key=secret, region_name="None", verify=False,
                          config=botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300))


def create_table(resource, table_name=TABLE_NAME, table_schema=HASH_SCHEMA):
    table = resource.create_table(TableName=table_name, BillingMode="PAY_PER_REQUEST", **deepcopy(table_schema))
    resource.meta.client.get_waiter("table_exists").wait(TableName=table_name)
    resource.meta.client.describe_table(TableName=table_name)
    return table


def table_exists(resource, table_name):
    try:
        resource.meta.client.describe_table(TableName=table_name)
        return True
    except resource.meta.client.exceptions.ResourceNotFoundException:
        return False


def delete_table(resource, table_name):
    resource.Table(table_name).delete()
    resource.meta.client.get_waiter("table_not_exists").wait(TableName=table_name)


def batch_write(table, new_items):
    with table.batch_writer() as batch:
        for item in new_items:
            batch.put_item(item)
    return table


def create_items(num_of_items=NUM_OF_ITEMS):
    return [{HASH_KEY_NAME: f"test{i}", "x": {"hello": f"world{i}"}} for i in range(num_of_items)]


def prefill_table(resource, table_name=TABLE_NAME, num_of_items=NUM_OF_ITEMS):
    return batch_write(create_table(resource, table_name), create_items(num_of_items))


def scan_table(table, consistent_read=True, **kwargs):
    response = table.scan(ConsistentRead=consistent_read, **kwargs)
    items = response["Items"]
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], ConsistentRead=consistent_read, **kwargs)
        items.extend(response["Items"])
    return items


def full_query(table, consistent_read=True, **kwargs):
    kwargs["ConsistentRead"] = consistent_read
    response = table.query(**kwargs)
    items = response["Items"]
    while "LastEvaluatedKey" in response:
        response = table.query(ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)
        items.extend(response["Items"])
    return items


def normalized(value):
    """Numbers compare as Decimal, as the dtests' DeepDiff with
    ignore_numeric_type_changes does."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float, Decimal)):
        return Decimal(value)
    if isinstance(value, dict):
        return {k: normalized(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalized(v) for v in value]
    if isinstance(value, set):
        return {normalized(v) for v in value}
    return value


def canonical(value):
    """A representation independent of the order of dict keys."""
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k!r}: {canonical(v)}" for k, v in sorted(value.items())) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(canonical(v) for v in value) + "]"
    if isinstance(value, set):
        return "{" + ", ".join(sorted(canonical(v) for v in value)) + "}"
    if isinstance(value, Decimal):
        return repr(value.normalize())
    return repr(value)


def same_items(expected, actual, ignore_order=True):
    expected, actual = normalized(expected), normalized(actual)
    if ignore_order:
        return sorted(map(canonical, expected)) == sorted(map(canonical, actual))
    return expected == actual


def set_write_isolation(table, isolation):
    arn = table.meta.client.describe_table(TableName=table.name)["Table"]["TableArn"]
    table.meta.client.tag_resource(ResourceArn=arn, Tags=[{"Key": "system:write_isolation", "Value": isolation.value}])


def datetimes_to_utc(value):
    """The value - typically a boto3 response - with every datetime in it in
    UTC. boto3 returns times in the local time zone, each with its own
    tzlocal(), and such datetimes in the hour repeated when daylight saving
    time ends never compare equal."""
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, dict):
        return {k: datetimes_to_utc(v) for k, v in value.items()}
    if isinstance(value, list):
        return [datetimes_to_utc(v) for v in value]
    return value


def table_conf(resource, table_name):
    client = resource.meta.client
    describe = datetimes_to_utc(client.describe_table(TableName=table_name)["Table"])
    return describe, client.list_tags_of_resource(ResourceArn=describe["TableArn"])["Tags"]


def wait_table_schema_synced(resources, table_name, timeout=30):
    """The dtests' is_table_schema_synced(): the nodes describe the table and
    its tags the same way."""
    deadline = time.time() + timeout
    while True:
        confs = [table_conf(r, table_name) for r in resources]
        if all(c == confs[0] for c in confs[1:]):
            return
        assert time.time() < deadline, f"table {table_name} schema not synced"
        time.sleep(0.5)


async def prepare_cluster(manager, num_of_nodes=3, dcs=1, topo=None, extra_config=None):
    """The dtests' prepare_dynamodb_cluster(): one rack per node in each DC,
    unless a topology {dc: {rack: nodes}} is given."""
    config = DTEST_ALTERNATOR_CONFIG | (extra_config or {})
    if topo is None:
        topo = {f"dc{d}": {f"rack{r}": 1 for r in range(1, num_of_nodes + 1)} for d in range(1, dcs + 1)}
    property_files = [{"dc": dc, "rack": rack} for dc, racks in topo.items() for rack, n in racks.items() for _ in range(n)]
    connect_opts = {}
    if config.get("authenticator"):
        connect_opts = {"auth_provider": PlainTextAuthProvider(username="cassandra", password="cassandra")}
    return await manager.servers_add(len(property_files), config=config, property_file=property_files,
                                     driver_connect_opts=connect_opts)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_read_key_condition_expression(manager: ScyllaClusterManager):
    """A query by a key condition finds all the items of a partition while
    one of the three nodes is down."""
    servers = await prepare_cluster(manager, 3)
    resource = dynamodb(servers[0].ip_addr)
    create_table(resource, table_schema=CONDITION_EXPRESSION_SCHEMA)
    pk = random_string(DEFAULT_STRING_LENGTH)
    items = [{"pk": pk, "c": Decimal(i), "a": random_string(DEFAULT_STRING_LENGTH)} for i in range(12)]
    table = batch_write(resource.Table(TABLE_NAME), items)
    with table.batch_writer() as batch:
        batch.put_item({"pk": random_string(DEFAULT_STRING_LENGTH), "c": 123, "a": random_string(DEFAULT_STRING_LENGTH)})
    await manager.server_stop_gracefully(servers[1].server_id)
    got = full_query(table, KeyConditionExpression="pk=:pk", ExpressionAttributeValues={":pk": pk})
    assert same_items(items, got)


def nested_item(level, idx):
    if level == 1:
        return {"a": str(idx), "level1": {"hello": f"world{idx}"}}
    return {"a": str(idx), f"level{level}": nested_item(level - 1, idx)}


def nested_items(num_of_items, levels):
    return [{HASH_KEY_NAME: f"test{i}", "x": nested_item(levels, i)} for i in range(num_of_items)]


def update_nested_attribute(sub_item, levels, updated_level, value):
    if updated_level == 1:
        sub_item[f"level{levels}"].update({"a": value})
        return sub_item
    return {"a": sub_item["a"], f"level{levels}": update_nested_attribute(sub_item[f"level{levels}"], levels - 1, updated_level - 1, value)}


def update_table_nested_item(table, levels, start_index, num_of_items):
    """The dtests' update_table_nested_items(): update a random nested level
    of a random item and read the item back."""
    idx = random.randint(start_index, start_index + num_of_items)
    key = {HASH_KEY_NAME: f"test{idx}"}
    item = table.get_item(ConsistentRead=True, Key=key)
    if "Item" not in item:
        return
    item = item["Item"]
    updated_level = random.randint(1, levels)
    level_path = ".".join(f"level{levels - level}" for level in range(updated_level))
    value = random_string(DEFAULT_STRING_LENGTH)
    table.update_item(Key=key, UpdateExpression=f"SET x.{level_path}.a = :val1", ExpressionAttributeValues={":val1": value})
    updated = table.get_item(ConsistentRead=True, Key=key)["Item"]
    expected = {HASH_KEY_NAME: f"test{idx}", "x": update_nested_attribute(item["x"], levels, updated_level, value)}
    assert updated == expected


class Background:
    """Runs a function in a loop in a thread until stopped, like the dtests'
    StoppableThread."""

    def __init__(self, target, **kwargs):
        self.stop_event = threading.Event()
        self.error = None

        def run():
            try:
                while not self.stop_event.is_set():
                    target(**kwargs)
            except Exception as e:  # noqa: BLE001
                self.error = e
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def join(self):
        self.stop_event.set()
        self.thread.join()
        if self.error:
            raise self.error


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_nested_attributes(manager: ScyllaClusterManager):
    """
    Updates of nested attributes, each read back, while other items are
    written and read and the last node is decommissioned and a new one is
    added, over and over.
    """
    servers = await prepare_cluster(manager, topo={"dc1": {"rack1": 1, "rack2": 1, "rack3": 2}})
    levels, num_of_items = 10, 4000
    res1, res2, res3 = (dynamodb(s.ip_addr) for s in servers[:3])
    create_table(res1)
    batch_write(res1.Table(TABLE_NAME), nested_items(num_of_items * 2, levels))
    write_stress = Background(lambda: batch_write(res1.Table(TABLE_NAME), nested_items(num_of_items, levels)))
    read_stress = Background(lambda: [res2.Table(TABLE_NAME).get_item(ConsistentRead=True, Key={HASH_KEY_NAME: f"test{i}"})
                                      for i in range(num_of_items * 2)])
    stop = asyncio.Event()
    nodes = list(servers)

    async def decommission_then_add_node():
        while not stop.is_set():
            node = nodes.pop()
            await manager.decommission_node(node.server_id)
            nodes.append(await manager.server_add(config=DTEST_ALTERNATOR_CONFIG, property_file={"dc": "dc1", "rack": "rack3"}))
            await asyncio.sleep(5)

    decommission = asyncio.create_task(decommission_then_add_node())

    def updates():
        for _ in range(num_of_items):
            update_table_nested_item(res3.Table(TABLE_NAME), levels, num_of_items, num_of_items)
    try:
        await asyncio.to_thread(updates)
    finally:
        stop.set()
        await asyncio.to_thread(write_stress.join)
        await asyncio.to_thread(read_stress.join)
        await decommission


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_filter_expression(manager: ScyllaClusterManager):
    """A scan with a filter expression from the other datacenter finds the
    matching items while a node of the first one is down."""
    servers = await prepare_cluster(manager, 3, dcs=2)
    res1, res_dc2 = dynamodb(servers[0].ip_addr), dynamodb(servers[3].ip_addr)
    create_table(res1, table_schema=HASH_AND_NUM_RANGE_SCHEMA)
    items = [{HASH_KEY_NAME: f"{h}", RANGE_KEY_NAME: r} for h in range(10) for r in range(10)]
    batch_write(res1.Table(TABLE_NAME), items)
    selected = random.choice(items)[RANGE_KEY_NAME]
    wait_table_schema_synced([res1, res_dc2], TABLE_NAME)
    await manager.server_stop_gracefully(servers[0].server_id)
    expected = [item for item in items if item[RANGE_KEY_NAME] >= selected]
    got = scan_table(res_dc2.Table(TABLE_NAME), FilterExpression=Attr(RANGE_KEY_NAME).gte(selected))
    assert same_items(expected, got)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_update_condition_expression_and_write_isolation(manager: ScyllaClusterManager):
    """Conditional updates from both datacenters run with LWT, and are
    rejected while the table's write isolation forbids read-modify-write."""
    servers = await prepare_cluster(manager, 3, dcs=2)
    res1 = dynamodb(servers[0].ip_addr)
    table = prefill_table(res1)
    await manager.server_stop_gracefully(servers[1].server_id)
    res_dc2 = dynamodb(servers[3].ip_addr)
    dc2_table = res_dc2.Table(TABLE_NAME)

    new_pk = random_string(DEFAULT_STRING_LENGTH)
    table.update_item(Key={HASH_KEY_NAME: new_pk}, AttributeUpdates={"a": {"Value": 1, "Action": "PUT"}})
    update_c_2 = dict(Key={HASH_KEY_NAME: new_pk}, UpdateExpression="SET c = :val", ConditionExpression="attribute_exists (a)",
                      ExpressionAttributeValues={":val": 2})
    rmw_disabled = "Read-modify-write operations are disabled"
    set_write_isolation(table, WriteIsolation.FORBID_RMW)
    wait_table_schema_synced([res1, res_dc2], TABLE_NAME)
    with pytest.raises(ClientError, match=rmw_disabled):
        dc2_table.update_item(**update_c_2)

    set_write_isolation(table, WriteIsolation.ALWAYS_USE_LWT)
    wait_table_schema_synced([res1, res_dc2], TABLE_NAME)
    dc2_table.update_item(**update_c_2)
    update_c_3 = dict(Key={HASH_KEY_NAME: new_pk}, UpdateExpression="SET c = :val", ConditionExpression="attribute_not_exists (b)",
                      ExpressionAttributeValues={":val": 3})
    with pytest.raises(ClientError, match=rmw_disabled):
        set_write_isolation(table, WriteIsolation.FORBID_RMW)
        table.update_item(**update_c_3)

    set_write_isolation(table, WriteIsolation.ONLY_RMW_USES_LWT)
    table.update_item(**update_c_3)
    assert table.get_item(Key={HASH_KEY_NAME: new_pk}, ConsistentRead=True)["Item"]["c"] == 3
    with pytest.raises(ClientError, match="ConditionalCheckFailedException"):
        table.update_item(Key={HASH_KEY_NAME: new_pk}, UpdateExpression="SET c = :val", ConditionExpression="attribute_not_exists (a)",
                          ExpressionAttributeValues={":val": 4})


class TypeMode(Enum):
    NUMBER = auto()
    BOOL = auto()
    NONE = auto()
    STRING = auto()
    BINARY = auto()
    LIST = auto()
    DICT = auto()
    MIXED = auto()


class DataGenerator:
    """The dtests' AlternatorDataGenerator: items with attributes of each
    DynamoDB type, named after the type."""

    def __init__(self):
        self.items_count = 0
        self.precision = Decimal("0" + str(10 ** (getcontext().prec - 1)) + "1")

    def key(self):
        return {HASH_KEY_NAME: f"test{self.items_count - 1}"}

    @staticmethod
    def random_text():
        chars = string.ascii_letters + string.digits + string.punctuation
        return "".join(random.choice(chars) for _ in range(random.randint(1, 30)))

    def number(self):
        self.items_count += 1
        if self.items_count % 2 == 0:
            return self.key() | {"number": random.randint(0, 1000)}
        return self.key() | {"number": Decimal.from_float(random.random()).quantize(self.precision)}

    def bool(self):
        self.items_count += 1
        return self.key() | {"bool": bool(random.randint(0, 1))}

    def none(self):
        self.items_count += 1
        return self.key() | {"none": None}

    def string(self):
        self.items_count += 1
        return self.key() | {"string": self.random_text()}

    def binary(self):
        self.items_count += 1
        return self.key() | {"binary": boto3.dynamodb.types.Binary(self.random_text().encode())}

    def list(self):
        item = []
        original = self.items_count + 1
        for _ in range(random.randint(0, 30)):
            item.append([self.number, self.bool, self.none, self.string, self.binary][self.items_count % 5]())
        self.items_count = original
        return self.key() | {"list": item}

    def dict(self):
        original = self.items_count + 1
        item = {HASH_KEY_NAME: f"test{original - 1}", "number": self.number(), "bool": self.bool(), "none": self.none(),
                "string": self.string(), "binary": self.binary(), "list": self.list()}
        self.items_count = original
        return item

    def items(self, num_of_items, mode):
        makers = {TypeMode.NUMBER: self.number, TypeMode.BOOL: self.bool, TypeMode.NONE: self.none, TypeMode.STRING: self.string,
                  TypeMode.BINARY: self.binary, TypeMode.LIST: self.list, TypeMode.DICT: self.dict}
        if mode != TypeMode.MIXED:
            return [makers[mode]() for _ in range(num_of_items)]
        options = list(makers.values())
        result = [options[i % len(options)]() for i in range(num_of_items)]
        self.items_count = 0
        return result


async def full_scan_table_while_restart_each_nodes(manager: ScyllaClusterManager, threads_num=None):
    """
    The dtests' scan stress (run_scan_stress()) on node1 while each other
    node is stopped and started (_reboot_nodes_while_running_stress()).
    """
    servers = await prepare_cluster(manager, 3)
    node1 = servers[0]
    table = prefill_table(dynamodb(node1.ip_addr))

    def full_scan():
        # The scan stress thread scans the table (in threads_num parallel
        # segments) and then compares the result with compare_table_items_data(),
        # which the dtest library does not have: the thread ends after its
        # first scan, and the dtest never reads its outcome.
        if threads_num:
            with ThreadPoolExecutor(max_workers=threads_num) as executor:
                scans = [executor.submit(scan_table, table, TotalSegments=threads_num, Segment=i) for i in range(threads_num)]
                for f in scans:
                    f.result()
        else:
            scan_table(table)

    scanner = asyncio.create_task(asyncio.to_thread(full_scan))
    for server in servers[1:]:
        await manager.server_stop_gracefully(server.server_id)
        await manager.server_start(server.server_id)
        await manager.servers_see_each_other(await manager.running_servers())
    await asyncio.gather(scanner, return_exceptions=True)


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_full_scan_table_while_restart_each_nodes.
# A full scan of 100 items through node1 while each other node is stopped
# and started again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_full_scan_table_while_restart_each_nodes(manager: ScyllaClusterManager):
    await full_scan_table_while_restart_each_nodes(manager)


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_full_parallel_scan_table_while_restart_each_nodes.
# A parallel scan (4 segments) of 100 items through node1 while each other
# node is stopped and started again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_full_parallel_scan_table_while_restart_each_nodes(manager: ScyllaClusterManager):
    await full_scan_table_while_restart_each_nodes(manager, threads_num=4)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_dynamo_types(manager: ScyllaClusterManager):
    """Items with attributes of each DynamoDB type are written and read back
    unchanged."""
    generator = DataGenerator()
    generator.number()
    servers = await prepare_cluster(manager, 3)
    resource = dynamodb(servers[0].ip_addr)
    table = create_table(resource)
    all_items = []
    for mode in TypeMode:
        items = generator.items(random.randint(1, 10), mode)
        all_items += items
        batch_write(table, items)
        assert same_items(all_items, scan_table(table))


COMPARE_OPS = {"LE": operator.le, "LT": operator.lt, "GE": operator.ge, "GT": operator.gt}


async def check_comparison_query_key_conditions_options(manager: ScyllaClusterManager, scan_index_forward=True):
    """The dtests' _check_comparison_query_key_conditions_options(): queries
    by each comparison of a number sort key (EQ, LE, LT, GE, GT) return the
    matching items in ascending or, with ScanIndexForward false, descending
    order."""
    servers = await prepare_cluster(manager, 3)
    resource = dynamodb(servers[0].ip_addr)
    if table_exists(resource, TABLE_NAME):
        delete_table(resource, TABLE_NAME)
    table = create_table(resource, table_schema=HASH_AND_NUM_RANGE_SCHEMA)
    items = [{HASH_KEY_NAME: f"{h}", RANGE_KEY_NAME: r} for h in range(random.randint(1, 10)) for r in range(random.randint(1, 10))]
    batch_write(table, items)
    selected = random.choice(items)
    same_hash = [item for item in items if item[HASH_KEY_NAME] == selected[HASH_KEY_NAME]]
    for op in ["EQ", "LE", "LT", "GE", "GT"]:
        key_condition = {
            HASH_KEY_NAME: {"AttributeValueList": [selected[HASH_KEY_NAME]], "ComparisonOperator": "EQ"},
            RANGE_KEY_NAME: {"AttributeValueList": [selected[RANGE_KEY_NAME]], "ComparisonOperator": op},
        }
        got = full_query(table, KeyConditions=key_condition, ScanIndexForward=scan_index_forward)
        if op == "EQ":
            expected = [selected]
        else:
            ordered = same_hash if scan_index_forward else same_hash[::-1]
            expected = [item for item in ordered if COMPARE_OPS[op](item[RANGE_KEY_NAME], selected[RANGE_KEY_NAME])]
        assert same_items(expected, got, ignore_order=False), key_condition


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_check_comparison_query_key_condition_options.
# Queries by EQ, LE, LT, GE and GT of a number sort key, in ascending order.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_check_comparison_query_key_condition_options(manager: ScyllaClusterManager):
    await check_comparison_query_key_conditions_options(manager)


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_check_comparison_query_key_condition_options_without_scan_index_forward.
# Queries by EQ, LE, LT, GE and GT of a number sort key, in descending order.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_check_comparison_query_key_condition_options_without_scan_index_forward(manager: ScyllaClusterManager):
    await check_comparison_query_key_conditions_options(manager, scan_index_forward=False)


async def check_string_query_key_conditions_options(manager: ScyllaClusterManager, scan_index_forward=True):
    """The dtests' _check_string_query_key_conditions_options(): queries by
    BEGINS_WITH and BETWEEN on a string sort key and on a binary one, over
    all 256 one-character keys, in ascending or descending order."""
    range_values = [chr(c) for c in range(256)]
    regular_items = [{HASH_KEY_NAME: f"{h}", RANGE_KEY_NAME: r} for h in range(random.randint(1, 10)) for r in range_values]
    binary_items = []
    for item in deepcopy(regular_items):
        item[RANGE_KEY_NAME] = boto3.dynamodb.types.Binary(item[RANGE_KEY_NAME].encode())
        binary_items.append(item)
    servers = await prepare_cluster(manager, 3)
    resource = dynamodb(servers[0].ip_addr)

    for table_schema in [HASH_AND_STR_RANGE_SCHEMA, HASH_AND_BINARY_RANGE_SCHEMA]:
        is_binary = table_schema is HASH_AND_BINARY_RANGE_SCHEMA
        if table_exists(resource, TABLE_NAME):
            delete_table(resource, TABLE_NAME)
        table = create_table(resource, table_schema=table_schema)
        items = binary_items if is_binary else regular_items
        selected_hash = random.choice(items)[HASH_KEY_NAME]
        same_hash = [item for item in items if item[HASH_KEY_NAME] == selected_hash]
        batch_write(table, items)

        def value(item):
            return item[RANGE_KEY_NAME].value.decode() if is_binary else item[RANGE_KEY_NAME]

        for op in ["BEGINS_WITH", "BETWEEN"]:
            if op == "BETWEEN":
                low, high = sorted(random.choices(range_values, k=2))
                selected_values = [low, high]
                expected = [item for item in same_hash if low <= value(item) <= high]
            else:
                selected_values = [random.choice(range_values)]
                expected = [item for item in same_hash if value(item).startswith(selected_values[0])]
            if not scan_index_forward:
                expected = expected[::-1]
            if is_binary:
                selected_values = [boto3.dynamodb.types.Binary(v.encode()) for v in selected_values]
            key_condition = {
                HASH_KEY_NAME: {"AttributeValueList": [selected_hash], "ComparisonOperator": "EQ"},
                RANGE_KEY_NAME: {"AttributeValueList": selected_values, "ComparisonOperator": op},
            }
            got = full_query(table, KeyConditions=key_condition, ScanIndexForward=scan_index_forward)
            assert same_items(expected, got, ignore_order=False), key_condition


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_check_string_and_binary_query_key_condition_options.
# Queries by BEGINS_WITH and BETWEEN of a string and of a binary sort key,
# in ascending order.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_check_string_and_binary_query_key_condition_options(manager: ScyllaClusterManager):
    await check_string_query_key_conditions_options(manager)


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_check_string_and_binary_query_key_condition_options_without_scan_index_forward.
# Queries by BEGINS_WITH and BETWEEN of a string and of a binary sort key,
# in descending order.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_check_string_and_binary_query_key_condition_options_without_scan_index_forward(manager: ScyllaClusterManager):
    await check_string_query_key_conditions_options(manager, scan_index_forward=False)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_cluster_traces(manager: ScyllaClusterManager):
    """
    With trace probability 1, PutItem, GetItem, UpdateItem and DeleteItem of
    items of mixed types, sent to each node in turn, leave trace sessions
    whose events show, in order, the request and its LWT rounds to the other
    nodes (or, for GetItem, the read), read through Alternator from the
    system_traces tables.
    """
    servers = await prepare_cluster(manager, 3)
    resources = [dynamodb(s.ip_addr) for s in servers]
    create_table(resources[0])
    items = DataGenerator().items(10, TypeMode.MIXED)
    n = len(servers)
    expected_messages = {
        "PutItem": [("PutItem", 1), ("prepare_ballot: sending prepare", n - 1), ("accept_proposal: send accept proposal", n - 1), ("CAS successful", 1)],
        "GetItem": [("GetItem", 1), ("Creating read executor for token", 1), ("Querying is done", 1)],
        "UpdateItem": [("UpdateItem", 1), ("accept_proposal: send accept proposal", n - 1), ("prune: send prune of", n - 1), ("CAS successful", 1)],
        "DeleteItem": [("DeleteItem", 1), ("accept_proposal: send accept proposal", n - 1), ("prune: send prune of", n - 1), ("CAS successful", 1)],
    }
    methods = list(expected_messages)

    for s in servers:
        await manager.api.set_trace_probability(s.ip_addr, 1.0)
    for idx, item in enumerate(items):
        table = resources[idx % n].Table(TABLE_NAME)
        key = {HASH_KEY_NAME: item[HASH_KEY_NAME]}
        table.put_item(Item=item)
        table.get_item(Key=key, ConsistentRead=True)
        table.update_item(Key=key)
        table.delete_item(Key=key)
    for s in servers:
        await manager.api.set_trace_probability(s.ip_addr, 0.0)

    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    host_id_of_ip = {s.ip_addr: h for s, h in zip(servers, host_ids)}
    prefix = ".scylla.alternator.system_traces."

    def traces_sorted():
        events_table = resources[0].Table(f"{prefix}events")
        sessions = scan_table(resources[0].Table(f"{prefix}sessions"))
        return [(s, full_query(events_table, KeyConditionExpression="session_id = :s", ExpressionAttributeValues={":s": s["session_id"]}))
                for s in sorted(sessions, key=lambda s: UUID(s["session_id"], version=1).time)]

    def check_all_events():
        events_by_action = {}
        idx = 0
        for session, events in traces_sorted():
            method = methods[idx % len(methods)]
            if session["request"] != f"Alternator {method}":
                continue
            events_by_action.setdefault(method, []).append(events)
            idx += 1
        node_ids = set(host_ids)
        for method, messages in expected_messages.items():
            all_traces = events_by_action.get(method)
            assert all_traces, f"traces for {method} weren't found"
            for action_idx, traces in enumerate(all_traces):
                source = host_ids[action_idx % n]
                trace_idx = 0
                for message, number in messages:
                    result, count, found = [], number, False
                    while trace_idx < len(traces):
                        trace = traces[trace_idx]
                        trace_idx += 1
                        activity = trace["activity"]
                        if message in activity and source == host_id_of_ip[trace["source"]]:
                            if message != activity:
                                result.append(activity)
                            count -= 1
                            if count == 0:
                                found = True
                                break
                    assert found, f'trace message "{message}" not found from {source}'
                    if result:
                        if method == "GetItem":
                            for msg in result:
                                ids = {i.strip() for i in msg.split("[", maxsplit=1)[1].split("]", maxsplit=1)[0].split(",")}
                                assert not node_ids ^ ids, f"{method}: node ids {node_ids ^ ids} not found"
                        else:
                            ids = {msg.rsplit(" ", maxsplit=1)[1] for msg in result}
                            assert node_ids ^ ids == {source}, f'"{message}" not sent to all the other nodes'

    # Traces are flushed every 2 seconds, and lost trace writes may arrive
    # later through hints.
    for attempt in range(20):
        try:
            check_all_events()
            return
        except AssertionError:
            if attempt == 19:
                raise
            await asyncio.sleep(1)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("value", ["1", [1], {1}, Decimal(1)], ids=["str_value", "list_value", "set_value", "decimal_value"])
async def test_exception_error_for_update_item_with_add_action(manager: ScyllaClusterManager, value):
    """An ADD of a value of another type than the attribute's fails with a
    ValidationException."""
    other_types = {str, Decimal, set, list} - {type(value)}
    item = {HASH_KEY_NAME: "test_1", "value": value}
    servers = await prepare_cluster(manager, 1)
    table = create_table(dynamodb(servers[0].ip_addr))
    batch_write(table, [item])
    for var_type in other_types:
        add_value = var_type([1]) if var_type in (set, list) else var_type(1)
        with pytest.raises(ClientError, match=r"ValidationException.*[Oo]perand type"):
            table.update_item(Key={HASH_KEY_NAME: "test_1"}, AttributeUpdates={"value": {"Value": add_value, "Action": "ADD"}})


# Ported from scylla-dtest alternator_tests.py::TesterAlternator::test_update_items_from_multiple_threads.
# On one node, with the table's write isolation always_use_lwt, 10000 (3000
# in debug mode) AttributeUpdates ADDs of a number, a one-element set or a
# one-element list to the same item from 3 threads all take effect: the
# number is the sum of the added values, the set and the list have all the
# added elements.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("value", ["[1]", "{1}", "1"])
async def test_update_items_from_multiple_threads(manager: ScyllaClusterManager, build_mode, value):
    value = literal_eval(value)
    num_of_items = 10000 if build_mode != "debug" else 3000
    num_of_threads = 3
    var_type = type(value)
    item = {HASH_KEY_NAME: "test_1", "value": value}
    is_value_iter = var_type in (set, list)
    add_operations = [{HASH_KEY_NAME: item[HASH_KEY_NAME], "value": var_type([idx]) if is_value_iter else var_type(idx)}
                      for idx in range(num_of_items)]
    expected_value = Decimal(sum(sum(op["value"] if is_value_iter else [op["value"]]) for op in add_operations))
    if not isinstance(value, set):
        expected_value += value[0] if is_value_iter else value

    servers = await prepare_cluster(manager, 1)
    resource = dynamodb(servers[0].ip_addr)
    table = create_table(resource)
    # The dtests' set_write_isolation() reads the table's description and
    # tags before and after tagging it.
    table_conf(resource, TABLE_NAME)
    set_write_isolation(table, WriteIsolation.ALWAYS_USE_LWT)
    table_conf(resource, TABLE_NAME)
    batch_write(table, [item])

    def update_items(items):
        """The dtests' update_items(action="ADD")."""
        t = resource.Table(TABLE_NAME)
        for update_item in items:
            t.update_item(Key={HASH_KEY_NAME: update_item[HASH_KEY_NAME]},
                          AttributeUpdates={k: {"Value": v, "Action": "ADD"} for k, v in update_item.items() if k != HASH_KEY_NAME})

    timeout = 180 * (3 if build_mode == "debug" else 1)  # the dtests' cql_timeout(180)

    def run_updates():
        with ThreadPoolExecutor(max_workers=num_of_threads) as pool:
            chunk_size = num_of_items // num_of_threads
            threads = [pool.submit(update_items, add_operations[i:i + chunk_size]) for i in range(0, len(add_operations), chunk_size)]
            for thread in threads:
                thread.result(timeout=timeout)

    await asyncio.to_thread(run_updates)
    result = scan_table(table)
    assert len(result) == 1
    if is_value_iter:
        result[0]["value"] = sum(result[0]["value"])
    assert result[0]["value"] == expected_value, "The value is not the sum of the values added through the threads"


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_alternator_enforce_authorization(manager: ScyllaClusterManager):
    """
    With authorization enforced, a request signed with an unknown key is
    rejected; a superuser role created through CQL on one node can create a
    table through another node, signing with the role's salted hash; a wrong
    hash is rejected again. Four nodes, as the dtest uses with tablets
    support, so that RF=3 leaves a spare node.
    """
    servers = await prepare_cluster(manager, 4, extra_config={
        "alternator_enforce_authorization": True,
        "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
        "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
    })
    with pytest.raises(ClientError, match="UnrecognizedClientException"):
        create_table(dynamodb(servers[0].ip_addr))

    # An exclusive connection, so that the role is read back from the node that created it.
    cluster = manager.con_gen([servers[0].ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([servers[0].ip_addr]),
                              auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    try:
        cql = cluster.connect()
        cql.execute("CREATE ROLE alternator WITH PASSWORD = 'password' AND login = true AND superuser = true")
        salted_hash = None
        for ks in ("system", "system_auth_v2", "system_auth"):
            try:
                row = cql.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role=%s;", ("alternator",)).one()
            except InvalidRequest:
                continue
            if row:
                salted_hash = row.salted_hash
                break
    finally:
        safe_driver_shutdown(cluster)

    other = dynamodb(servers[-1].ip_addr, salted_hash)
    end_time = time.time() + 1
    while True:
        try:
            create_table(other)
            break
        except ClientError as e:
            if "UnrecognizedClientException" not in str(e) or time.time() >= end_time:
                raise
            time.sleep(0.01)

    with pytest.raises(ClientError, match="UnrecognizedClientException"):
        create_table(dynamodb(servers[-1].ip_addr, "None"), table_name="table2")
