#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
import os
import pathlib
import random
import shutil
import string
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING

import boto3
import botocore.client
import pytest
import requests
from deepdiff import DeepDiff
from requests.exceptions import ConnectionError

from test.cluster.dtest.alternator.utils import schemas
from test.cluster.dtest.dtest_class import Tester, get_ip_from_node
from test.cluster.dtest.tools.cluster import new_node
from test.cluster.dtest.tools.sslkeygen import create_self_signed_x509_certificate

if TYPE_CHECKING:
    from typing import Any

    from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
    from mypy_boto3_dynamodb.service_resource import Table

    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode


logger = logging.getLogger(__name__)

# DynamoDB's "AttributeValue", but as decoded by boto3 into Python types,
# not the JSON serialization.

type AttributeValueTypeDef = bytes | bytearray | str | int | Decimal | bool | set[int] | set[Decimal] | set[str] | \
                             set[bytes] | set[bytearray] | list[Any] | dict[str, Any] | None

ALTERNATOR_SNAPSHOT_FOLDER = pathlib.Path(__file__).with_name("alternator") / "snapshot"
TABLE_NAME = "user_table"
NUM_OF_NODES = 3
NUM_OF_ITEMS = 100
NUM_OF_ELEMENTS_IN_SET = 20
ALTERNATOR_PORT = 8080
ALTERNATOR_SECURE_PORT = 8043
DEFAULT_STRING_LENGTH = 5
# https://github.com/scylladb/scylla/issues/4480 - according Nadav the table name contains dash char and
# 32-byte UUID string -> 222 + 1 + 32 = 255 (The longest dynanodb's table name)
LONGEST_TABLE_SIZE = 222
SHORTEST_TABLE_SIZE = 3


class WriteIsolation(Enum):
    ALWAYS_USE_LWT = "always_use_lwt"
    FORBID_RMW = "forbid_rmw"
    ONLY_RMW_USES_LWT = "only_rmw_uses_lwt"
    UNSAFE_RMW = "unsafe_rmw"


class TableConf:
    """
    The dynamodb table metadata of schema and tags as seen by a table of a specific node resource
    """

    def __init__(self, table: DynamoDBServiceResource.Table):
        self.table = table
        self.describe = table.meta.client.describe_table(TableName=table.name)["Table"]
        self.arn = self.describe["TableArn"]
        self.tags = table.meta.client.list_tags_of_resource(ResourceArn=self.arn)["Tags"]

    def update(self):
        self.describe = self.table.meta.client.describe_table(TableName=self.table.name)["Table"]
        self.tags = self.table.meta.client.list_tags_of_resource(ResourceArn=self.arn)["Tags"]
        logger.debug(f"{self.table.name} {self.table.meta.client.meta.endpoint_url} tags: {self.tags}")
        logger.debug(f"{self.table.name} {self.table.meta.client.meta.endpoint_url} describe: {self.describe}")

    def __eq__(self, other_table):
        self.update()
        other_table.update()
        if isinstance(other_table, self.__class__):
            return self.__dict__ == other_table.__dict__
        else:
            return False


def set_write_isolation(table: DynamoDBServiceResource.Table, isolation: WriteIsolation | str):
    isolation = isolation if not isinstance(isolation, WriteIsolation) else isolation.value
    table_conf = TableConf(table=table)
    tags = [{"Key": "system:write_isolation", "Value": isolation}]
    table.meta.client.tag_resource(ResourceArn=table_conf.arn, Tags=tags)
    table_conf.update()


class AlternatorApi:
    def __init__(self, resource: DynamoDBServiceResource, client: DynamoDBClient, stream=None):
        self.resource = resource
        self.client = client
        self.stream = stream


class Gsi:
    ATTRIBUTE_NAME = "g_s_i"
    ATTRIBUTE_DEFINITION = {"AttributeName": ATTRIBUTE_NAME, "AttributeType": "S"}
    NAME = f"hello_{ATTRIBUTE_NAME}"
    CONFIG = dict(
        GlobalSecondaryIndexes=[
            {
                "IndexName": NAME,
                "KeySchema": [
                    {"AttributeName": ATTRIBUTE_NAME, "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ]
    )


class StoppableThread:
    """Thread class with a stop() method. it runs the given "target" function in a loop
    until the 'stop-event' is set."""

    def __init__(self, target, kwargs=None):
        self._stop_event = threading.Event()
        self.target = target
        self.target_name = target.__name__
        self.kwargs = kwargs or {}
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future = None

    def stop(self) -> None:
        self._stop_event.set()

    def join(self):
        self.stop()
        return self.future.result()

    def start(self) -> None:
        self.future = self.pool.submit(self.run)

    def run(self) -> None:
        logger.debug(f"Running {self.target_name}...")
        while not self._stop_event.is_set():
            logger.debug(f"Running {self.target_name}...")
            self.target(**self.kwargs)
            logger.debug(f"{self.target_name} is completed!")
        logger.debug(f"{self.target_name} is stopped!")


class BaseAlternator(Tester):
    _nodes_url_list = None
    keyspace_name_template = "alternator_{}"
    _table_primary_key = schemas.HASH_KEY_NAME
    _table_primary_key_format = "test{}"

    salted_hash = "None"

    alternator_urls = {}
    alternator_apis = {}
    clear_resources_methods = []

    @pytest.fixture(scope="function", autouse=True)
    def clear_resources(self):
        yield
        for resource_method in self.clear_resources_methods:
            resource_method()
        self.clear_resources_methods.clear()

    @property
    def boto_config(self):
        return botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300)

    @property
    def dynamo_params(self):
        p = dict(aws_access_key_id="alternator", aws_secret_access_key=self.salted_hash, region_name="None", verify=False, config=self.boto_config)
        if self.is_encrypted:
            p["verify"] = self.cert_file
        return p

    def _get_alternator_api_url(self, node: ScyllaNode) -> None:
        if self.is_encrypted:
            self.alternator_urls[node.name] = f"https://{get_ip_from_node(node=node)}:{ALTERNATOR_SECURE_PORT}"
        else:
            self.alternator_urls[node.name] = f"http://{get_ip_from_node(node=node)}:{ALTERNATOR_PORT}"

    def get_alternator_api_url(self, node: ScyllaNode) -> str:
        if node.name not in self.alternator_urls:
            self._get_alternator_api_url(node=node)
        return self.alternator_urls[node.name]

    def wait_for_alternator(self, node: ScyllaNode = None, timeout: int = 300) -> None:
        nodes = self.cluster.nodelist() if node is None else [node]
        node_urls = {}
        for node in nodes:  # noqa: PLR1704
            node_urls[node.name] = f"{self.get_alternator_api_url(node=node)}/"

        def probe(nodes, allow_connection_error=True):
            remaining = []
            for node in nodes:
                if not node.is_running():
                    raise RuntimeError(f"Node {node.name} is not running")
                url = node_urls[node.name]
                try:
                    r = requests.get(url, verify=False)
                    if r.ok:
                        del node_urls[node.name]
                        continue
                    else:
                        r.raise_for_status()
                except ConnectionError:
                    if not allow_connection_error:
                        raise
                remaining.append(node)
            return remaining

        start_time = time.time()
        nodes = probe(nodes)
        while nodes:
            time.sleep(1)
            last_try = (time.time() - start_time) >= timeout
            nodes = probe(nodes, allow_connection_error=not last_try)

    def _add_api_for_node(self, node: ScyllaNode, timeout: int = 300) -> None:
        self.wait_for_alternator(node=node, timeout=timeout)
        node_alternator_address = self.get_alternator_api_url(node=node)
        self.alternator_apis[node.name] = AlternatorApi(
            resource=boto3.resource(service_name="dynamodb", endpoint_url=node_alternator_address, **self.dynamo_params), client=boto3.client(service_name="dynamodb", endpoint_url=node_alternator_address, **self.dynamo_params)
        )

    def get_dynamodb_api(self, node: ScyllaNode, timeout: int = 300) -> AlternatorApi:
        if node.name not in self.alternator_apis:
            self._add_api_for_node(node=node, timeout=timeout)
        return self.alternator_apis[node.name]

    def prepare_dynamodb_cluster(
        self,
        num_of_nodes: int = NUM_OF_NODES,
        is_multi_dc: bool = False,
        is_encrypted: bool = False,
        extra_config: dict | None = None,
        timeout: int = 300,
    ) -> None:
        logger.debug(f"Populating a cluster with {num_of_nodes} nodes for {"single DC" if not is_multi_dc else "multi DC"}..")

        self.alternator_urls = {}
        self.alternator_apis = {}
        self.is_encrypted = is_encrypted

        cluster_config = {
            "start_native_transport": True,
            "alternator_write_isolation": "always",
        }

        if self.is_encrypted:
            tmpdir = tempfile.mkdtemp(prefix="alternator-encryption-")
            self.clear_resources_methods.append(lambda: shutil.rmtree(tmpdir))
            self.cert_file = os.path.join(tmpdir, "scylla.crt")
            key_file = os.path.join(tmpdir, "scylla.key")
            cluster_config["alternator_encryption_options"] = {
                "certificate": self.cert_file,
                "keyfile": key_file,
            }
            cluster_config["alternator_https_port"] = ALTERNATOR_SECURE_PORT
        else:
            cluster_config["alternator_port"] = ALTERNATOR_PORT

        if extra_config:
            cluster_config.update(extra_config)

        logger.debug(f"configure_dynamodb_cluster: {cluster_config}")
        self.cluster.set_configuration_options(cluster_config)

        self.cluster.populate([num_of_nodes, num_of_nodes] if is_multi_dc else num_of_nodes)

        if self.is_encrypted:
            create_self_signed_x509_certificate(
                test_path="",
                cert_file=self.cert_file,
                key_file=key_file,
                ip_list=[str(server.ip_addr) for server in self.cluster.manager.all_servers()],
            )

        logger.debug("Starting cluster..")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        for node in self.cluster.nodelist():
            self._add_api_for_node(node=node, timeout=timeout)

    # pylint:disable=too-many-arguments
    def create_table(
        self,
        node: ScyllaNode,
        table_name: str = TABLE_NAME,
        schema: tuple | dict | None = None,
        wait_until_table_exists: bool = True,
        create_gsi: bool = False,
        **kwargs,
    ) -> Table:
        if schema is None:
            schema = schemas.HASH_SCHEMA
        if isinstance(schema, tuple):
            schema = dict(schema)
        # so the mutations happen in this function are not visible from its
        # caller
        schema = deepcopy(schema)
        stream = kwargs.pop("stream_specification", {})
        if create_gsi:
            schema["AttributeDefinitions"].append(Gsi.ATTRIBUTE_DEFINITION)
            schema.update(Gsi.CONFIG)
        dynamodb_api = self.get_dynamodb_api(node=node)
        logger.debug(f"Creating a new table '{table_name}' using node '{node.name}'..")
        table = dynamodb_api.resource.create_table(TableName=table_name, BillingMode="PAY_PER_REQUEST", **schema, **stream, **kwargs)
        if wait_until_table_exists:
            waiter = dynamodb_api.client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)
        logger.info(f"The table '{table_name}' successfully created..")
        response = dynamodb_api.client.describe_table(TableName=table_name)
        logger.debug(f"Table's schema and configuration are: {response}")
        return table

    def delete_table(self, table_name: str, node: ScyllaNode) -> None:
        node_ks_path = self.get_table_folder(table_name=table_name, node=node)
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        logger.debug(f"Removing table '{table_name}'")
        table.delete()
        waiter = dynamodb_api.client.get_waiter("table_not_exists")
        waiter.wait(TableName=table_name)
        logger.debug(f"Removing table keyspace folder '{node_ks_path}' from node '{node.name}'")
        # since `node.rmtree` remove only the content of the folder, we'll rmnode the parent
        node.rmtree(path=pathlib.Path(node_ks_path).parent)

    def _create_nested_items(self, level: int, item_idx: int):
        if level == 1:
            return {"a": str(item_idx), "level1": {"hello": f"world{item_idx}"}}
        return {"a": str(item_idx), f"level{level}": self._create_nested_items(level=level - 1, item_idx=item_idx)}

    def create_nested_items(self, num_of_items: int = NUM_OF_ITEMS, nested_attributes_levels: int = 3) -> list[dict[str, str]]:
        return [{self._table_primary_key: self._table_primary_key_format.format(item_idx), "x": self._create_nested_items(level=nested_attributes_levels, item_idx=item_idx)} for item_idx in range(num_of_items)]

    def create_items(
        self,
        primary_key: str | None = None,
        num_of_items: int = NUM_OF_ITEMS,
        use_set_data_type: bool = False,
        expiration_sec: int | None = None,
        random_start_index: bool = False,
    ) -> list[dict[str, str]]:
        primary_key = primary_key or self._table_primary_key
        if not random_start_index:
            items_range = range(num_of_items)
        else:
            start_index = random.randint(0, num_of_items * 10)
            items_range = range(start_index, start_index + num_of_items)
        if use_set_data_type:
            items = [{primary_key: self._table_primary_key_format.format(item_idx), "x": {"hello": f"world{item_idx}"}, "hello_set": set([f"s{idx}" for idx in range(NUM_OF_ELEMENTS_IN_SET)])} for item_idx in items_range]
        else:
            items = [{primary_key: self._table_primary_key_format.format(item_idx), "x": {"hello": f"world{item_idx}"}} for item_idx in items_range]

        if expiration_sec:
            expiration = int(time.time()) + expiration_sec
            for item in items:
                item.update({"expiration": expiration})
        return items

    # pylint:disable=too-many-arguments
    def batch_write_actions(  # noqa: PLR0913
        self,
        table_name: str,
        node: ScyllaNode,
        new_items: list[dict[str, Any]] | None = None,
        delete_items: list[dict[str, str]] | None = None,
        schema: tuple | dict = schemas.HASH_SCHEMA,
        ignore_errors: bool = False,
        verbose=True,
    ):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table_keys = [key["AttributeName"] for key in schema[0][1]]
        assert new_items or delete_items, "should pass new_items or delete_items, other it's a no-op"
        new_items, delete_items = new_items or [], delete_items or []
        if new_items:
            logger.debug(f"Adding new {len(new_items)} items to table '{table_name}'..")
        if delete_items:
            logger.debug(f"Deleting {len(delete_items)} items from table '{table_name}'..")

        table = dynamodb_api.resource.Table(name=table_name)
        with table.batch_writer() as batch:
            try:
                for item in new_items:
                    batch.put_item(item)
                for item in delete_items:
                    batch.delete_item({key: item[key] for key in table_keys})
            except Exception as error:
                if ignore_errors:
                    logger.info(f"Continuing after exception: {error}")
                else:
                    raise error
        return table

    def scan_table(self, table_name: str, node: ScyllaNode, threads_num: int | None = None, consistent_read: bool = True, **kwargs) -> list[dict[str, AttributeValueTypeDef]]:
        scan_result, is_parallel_scan = [], threads_num and threads_num > 0
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        kwargs["ConsistentRead"] = consistent_read

        def _scan_table(part_scan_idx=None) -> list[dict[str, AttributeValueTypeDef]]:
            parallel_params = {}

            if is_parallel_scan:
                parallel_params = {"TotalSegments": threads_num, "Segment": part_scan_idx}
                logger.debug(f"Starting parallel scan part '{part_scan_idx + 1}' on table '{table_name}'")
            else:
                logger.debug(f"Starting full scan on table '{table_name}'")

            response = table.scan(**parallel_params, **kwargs)
            result = response["Items"]
            while "LastEvaluatedKey" in response:
                response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], **parallel_params, **kwargs)
                result.extend(response["Items"])

            return result

        if is_parallel_scan:
            with ThreadPoolExecutor(max_workers=threads_num) as executor:
                threads = [executor.submit(_scan_table, part_idx) for part_idx in range(threads_num)]
                scan_result = [thread.result() for thread in threads]
            return list(chain(*scan_result)) if len(scan_result) > 1 else scan_result
        return _scan_table()

    def is_table_schema_synced(self, table_name: str, nodes: list[ScyllaNode]) -> bool:
        logger.debug(f"Checking table {table_name} schema sync on nodes:")
        for node in nodes:
            logger.debug(node.name)
        assert len(nodes) > 1, "A minimum of 2 nodes is required for checking schema sync."
        nodes_table_conf = [TableConf(self.get_table(table_name=table_name, node=node)) for node in nodes]
        for idx, table_conf in enumerate(nodes_table_conf[:-1]):
            if not table_conf == nodes_table_conf[idx + 1]:
                return False
        return True

    @staticmethod
    def get_table_folder(table_name: str, node: ScyllaNode) -> str:
        node_data_folder_path = os.path.join(node.get_path(), "data")
        table_folder_name = next((name for name in os.listdir(node_data_folder_path) if name.endswith(table_name)), None)
        if table_folder_name is None:
            raise FileNotFoundError(f"The folder of table '{table_name}' not found in following path '{node_data_folder_path}'")
        table_folder_path = os.path.join(node_data_folder_path, table_folder_name)
        scylla_table_files = next((name for name in os.listdir(table_folder_path) if name.startswith(table_name)), None)
        if scylla_table_files is None:
            raise FileNotFoundError(f"The folder that contain Scylla files for table '{table_name}' not found in following path '{scylla_table_files}'")
        return os.path.join(table_folder_path, scylla_table_files)

    def create_snapshot(self, table_name: str, snapshot_folder: str, node: ScyllaNode) -> None:
        keyspace = self.keyspace_name_template.format(table_name)
        logger.debug(f"Making Alternator snapshot for node '{node.name}'..")
        logger.debug(node.nodetool(f"snapshot {keyspace} -t {table_name} "))
        node_table_folder_path = self.get_table_folder(table_name=table_name, node=node)
        node_snapshot_folder_path = os.path.join(node_table_folder_path, "snapshots", table_name)

        logger.debug(f"Creating local snapshot folder in following path '{snapshot_folder}' and moving all snapshot files to this folder..")
        for file_name in os.listdir(node_snapshot_folder_path):
            shutil.copyfile(src=os.path.join(node_snapshot_folder_path, file_name), dst=os.path.join(snapshot_folder, file_name))

    def load_snapshot_and_refresh(self, table_name: str, node: ScyllaNode, snapshot_folder: str = ""):
        keyspace_folder_path = self.get_table_folder(table_name=table_name, node=node)
        snapshot_folder = snapshot_folder or os.path.join(keyspace_folder_path, "snapshots", table_name)
        upload_folder = os.path.join(keyspace_folder_path, "upload")
        if not os.path.exists(path=snapshot_folder):
            raise NotADirectoryError(f"The snapshot folder '{snapshot_folder}' not exists")
        if not os.listdir(snapshot_folder):
            raise IsADirectoryError(f"The snapshot folder '{snapshot_folder}' not contain any files")

        if os.path.isdir(upload_folder):
            node.rmtree(upload_folder)
        os.makedirs(name=upload_folder, exist_ok=True)
        logger.debug(f"Loading snapshot files from folder '{snapshot_folder}' to '{upload_folder}'..")
        for file_name in os.listdir(snapshot_folder):
            shutil.copyfile(src=os.path.join(snapshot_folder, file_name), dst=os.path.join(upload_folder, file_name))
        refresh_cmd = f"refresh -- {self.keyspace_name_template.format(table_name)} {table_name}"
        logger.debug(f"Running following refresh cmd '{refresh_cmd}'..")
        node.nodetool(refresh_cmd)
        node.repair()

    def compare_table_data(  # noqa: PLR0913
        self,
        expected_table_data: list[dict[str, str]],
        table_name: str | None = None,
        node: ScyllaNode = None,
        ignore_order: bool = True,
        consistent_read: bool = True,
        table_data: list[dict[str, str]] | None = None,
        **kwargs,
    ) -> DeepDiff:
        if not table_data:
            table_data = self.scan_table(table_name=table_name, node=node, ConsistentRead=consistent_read, **kwargs)
        return DeepDiff(t1=expected_table_data, t2=table_data, ignore_order=ignore_order, ignore_numeric_type_changes=True)

    def _run_stress(self, table_name: str, node: ScyllaNode, target, num_of_item: int = NUM_OF_ITEMS, **kwargs) -> StoppableThread:
        params = dict(table_name=table_name, node=node, num_of_items=num_of_item)
        for key, val in kwargs.items():
            params.update({key: val})
        stress_thread = StoppableThread(target=target, kwargs=params)

        self.clear_resources_methods.append(lambda: stress_thread.join())
        logger.debug(f"Start Alternator stress of {stress_thread.target_name}..\n Using parameters of: {stress_thread.kwargs}")
        stress_thread.start()
        return stress_thread

    def run_decommission_then_add_node(self):
        node_to_remove = self.cluster.nodelist()[-1]
        logger.info(f"Decommissioning {node_to_remove.name}..")
        try:
            node_to_remove.decommission()
        except Exception as error:  # noqa: BLE001
            logger.info(f"Decommissioning {node_to_remove.name} failed with: {error}")
            return
        logger.info(f"Adding new node to cluster..")
        node = new_node(self.cluster, bootstrap=True)
        node.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info(f"Node successfully added!")
        time.sleep(5)

    def run_create_table(self):
        try:
            node1 = self.cluster.nodelist()[0]
            self.create_table(table_name=random_string(length=10), node=node1, wait_until_table_exists=False)
        except Exception:  # noqa: BLE001
            pass

    def run_create_table_thread(self) -> StoppableThread:
        create_table_thread = StoppableThread(target=self.run_create_table)
        self.clear_resources_methods.append(lambda: create_table_thread.join())
        create_table_thread.start()
        return create_table_thread

    def run_decommission_add_node_thread(self) -> StoppableThread:
        decommission_thread = StoppableThread(target=self.run_decommission_then_add_node)
        self.clear_resources_methods.append(lambda: decommission_thread.join())
        logger.debug(f"Start decommission thread of {decommission_thread.target_name}..")
        decommission_thread.start()
        return decommission_thread

    def run_read_stress(
        self,
        table_name: str,
        node: ScyllaNode,
        num_of_item: int = NUM_OF_ITEMS,
        verbose: bool = True,
        consistent_read: bool = True,
        **kwargs,
    ) -> StoppableThread:
        return self._run_stress(table_name=table_name, node=node, target=self.get_table_items, num_of_item=num_of_item, verbose=verbose, consistent_read=consistent_read, **kwargs)

    def run_write_stress(  # noqa: PLR0913
        self,
        table_name: str,
        node: ScyllaNode,
        num_of_item: int = NUM_OF_ITEMS,
        ignore_errors=False,
        use_set_data_type: bool = False,
        verbose=False,
        **kwargs,
    ) -> StoppableThread:
        return self._run_stress(table_name=table_name, node=node, target=self.put_table_items, num_of_item=num_of_item, ignore_errors=ignore_errors, use_set_data_type=use_set_data_type, verbose=verbose, **kwargs)

    def run_delete_set_elements_stress(
        self,
        table_name: str,
        node: ScyllaNode,
        num_of_item: int = NUM_OF_ITEMS,
        verbose: bool = True,
        consistent_read: bool = True,
        **kwargs,
    ) -> StoppableThread:
        return self._run_stress(table_name=table_name, node=node, target=self.update_table_delete_set_elements, num_of_item=num_of_item, verbose=verbose, consistent_read=consistent_read, **kwargs)

    def get_table(self, table_name: str, node: ScyllaNode):
        return self.get_dynamodb_api(node=node).resource.Table(name=table_name)

    def put_table_items(  # noqa: PLR0913
        self,
        table_name: str,
        node: ScyllaNode,
        num_of_items: int = NUM_OF_ITEMS,
        ignore_errors: bool = False,
        use_set_data_type: bool = False,
        verbose=True,
        **kwargs,
    ):
        if nested_attributes_levels := kwargs.get("nested_attributes_levels"):
            items = self.create_nested_items(num_of_items=num_of_items, nested_attributes_levels=nested_attributes_levels)
        else:
            expiration_sec = kwargs["expiration_sec"] if "expiration_sec" in kwargs else None
            random_start_index = kwargs["random_start_index"] if "random_start_index" in kwargs else False
            items = self.create_items(num_of_items=num_of_items, use_set_data_type=use_set_data_type, expiration_sec=expiration_sec, random_start_index=random_start_index)

        self.batch_write_actions(table_name=table_name, node=node, new_items=items, ignore_errors=ignore_errors, verbose=verbose)

    def update_table_delete_set_elements(  # noqa: PLR0913
        self,
        table_name: str,
        node: ScyllaNode,
        num_of_items: int = NUM_OF_ITEMS,
        verbose: bool = True,
        consistent_read: bool = True,
        random_start_index: bool = False,
    ):  # pylint:disable=too-many-locals
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        logger.debug("Starting queries of: %s items with ConsistentRead = %s", num_of_items, consistent_read)
        # random_start_index means not writing data to the exact same indexes. thus, it has a factor of 10x bigger
        # range to randomly choose from. then every cycle may write to a different token range and not necessarily
        # override all existing previous data.
        random_range_factor = 10
        start_index = 0 if not random_start_index else random.randint(0, num_of_items * random_range_factor)
        end_index = start_index + num_of_items
        if verbose:
            logger.debug("First Item in range: %s", table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: f"test{start_index}"}))
            logger.debug("Last Item in range: %s", table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: f"test{end_index - 1}"}))

        for idx in range(start_index, end_index):
            key = {self._table_primary_key: f"test{idx}"}
            item = table.get_item(ConsistentRead=consistent_read, Key=key)
            # Delete few of the item's set elements if existed.
            if item and "Item" in item and "hello_set" in item["Item"]:
                if hello_set := item["Item"]["hello_set"]:
                    count = random.randint(1, min(len(hello_set), 7))
                    sub_items_to_delete = random.sample(list(hello_set), count)
                    table.update_item(Key=key, AttributeUpdates={"hello_set": {"Action": "DELETE", "Value": set(sub_items_to_delete)}})

    def get_table_items(
        self,
        table_name: str,
        node: ScyllaNode,
        num_of_items: int = NUM_OF_ITEMS,
        verbose: bool = True,
        consistent_read: bool = True,
    ) -> list:
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        logger.debug(f"Starting queries of: {num_of_items} items with ConsistentRead = {consistent_read}")
        if verbose:
            logger.debug("First Item in range: {}".format(table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: "test0"})))
            logger.debug("Last Item in range: {}".format(table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: f"test{num_of_items - 1}"})))

        return [table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: f"test{idx}"}) for idx in range(num_of_items)]

    def prefill_dynamodb_table(self, node: ScyllaNode, table_name: str = TABLE_NAME, num_of_items: int = NUM_OF_ITEMS, **kwargs):
        self.create_table(table_name=table_name, node=node, **kwargs)
        new_items = self.create_items(num_of_items=num_of_items)
        return self.batch_write_actions(table_name=table_name, node=node, new_items=new_items)


def random_string(length: int, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choices(chars, k=length))


def generate_put_request_items(num_of_items: int = NUM_OF_ITEMS, add_gsi: bool = False) -> list[dict[str, str | dict[str, str]]]:
    logger.debug(f"Generating {num_of_items} put request items..")
    put_request_items = list()  # type: list[dict[str, str | dict[str, str]]]
    for idx in range(num_of_items):
        item = {schemas.HASH_KEY_NAME: f"test{idx}", "other": random_string(length=DEFAULT_STRING_LENGTH), "x": {"hello": f"world{idx}"}}
        if add_gsi:
            item[Gsi.ATTRIBUTE_NAME] = random_string(length=1)
        put_request_items.append(item)
    return put_request_items


def full_query(table, consistent_read=True, **kwargs):
    """
    A dynamodb table query that can also be extended with parameters like 'KeyConditions'
    :param table:  the dynamodb table object to run query on
    :param consistent_read: Strongly consistent reads
    :param kwargs: for adding any other optional dynamodb params
    :return: A list of query result items.
    """
    response = table.query(**kwargs)
    items = response["Items"]
    kwargs["ConsistentRead"] = consistent_read

    while "LastEvaluatedKey" in response:
        response = table.query(ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)
        items.extend(response["Items"])
    return items
