#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import os
import random
import string
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from pprint import pformat

import pytest
import requests
from botocore.exceptions import ClientError, EndpointConnectionError
from ccmlib.scylla_node import ScyllaNode
from deepdiff import DeepDiff

from alternator.utils import schemas
from alternator_utils import (
    ALTERNATOR_SECURE_PORT,
    ALTERNATOR_SNAPSHOT_FOLDER,
    DEFAULT_STRING_LENGTH,
    LONGEST_TABLE_SIZE,
    NUM_OF_ELEMENTS_IN_SET,
    NUM_OF_ITEMS,
    SHORTEST_TABLE_SIZE,
    TABLE_NAME,
    BaseAlternator,
    Gsi,
    WriteIsolation,
    full_query,
    generate_put_request_items,
    random_string,
    set_write_isolation,
)
from dtest_class import get_ip_from_node, wait_for
from tools.cluster import new_node
from tools.retrying import retrying

logger = logging.getLogger(__name__)


class SlowQueriesLoggingError(Exception):
    pass


class ConcurrencyLimitNotExceededError(Exception):
    pass


class TesterAlternator(BaseAlternator):
    def _num_of_nodes_for_test(self, rf):
        assert rf >= 3
        nr_nodes = rf
        if "tablets" in self.scylla_features:
            # Alternator uses RF=3 on clusters with three or more live nodes,
            # and writes uses CF=QUORUM. also, tablets enforces the RF
            # constraints. so, we need to add one spare node to the cluster, so
            # that we are allowed to decommission a node.
            nr_nodes += 1
        return nr_nodes

    def test_load_older_snapshot_and_refresh(self):
        """
        The test loading older snapshot files and checking the refresh command works
        Test will:
           - Create a cluster of 3 nodes.
           - Load older snapshot files
           - Execute nodetool `refresh` command
           - Verify after `refresh` commands are equal to snapshot data
        """
        table_name, node_idx = TABLE_NAME, 0
        snapshot_folder = os.path.join(ALTERNATOR_SNAPSHOT_FOLDER, "scylla4.0.rc1")

        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[node_idx]
        self.create_table(table_name=table_name, node=node1)
        table_data = self.create_items()
        self.load_snapshot_and_refresh(table_name=table_name, node=node1, snapshot_folder=snapshot_folder)
        diff = self.compare_table_data(table_name=table_name, expected_table_data=table_data, node=node1)
        assert not diff, f"The following items are missing:\n{pformat(diff)}"

    def test_create_snapshot_and_refresh(self, request):
        """
        The test checks the behavior of the `snapshot` and `refresh` commands for Alternator
        Test will:
          - Create a cluster of 3 nodes.
          - Create a new table with 100 items.
          - Create a snapshot of the table we created earlier.
          - Copy the files from the `snapshot` command and moved it to the table folder and used the `refresh`
            command to load the data.
          - Verify the data before `snapshot` and after `refresh` commands are equal.
        """
        table_name, num_of_items = TABLE_NAME, NUM_OF_ITEMS

        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]
        self.create_table(table_name=table_name, node=node1)
        new_items = self.create_items(num_of_items=num_of_items)
        self.batch_write_actions(table_name=table_name, node=node1, new_items=new_items)
        data_before_refresh = self.scan_table(table_name=table_name, node=node1)

        snapshot_folder = tempfile.mkdtemp()
        request.addfinalizer(lambda: node1.rmtree(snapshot_folder))
        self.create_snapshot(table_name=TABLE_NAME, node=node1, snapshot_folder=snapshot_folder)
        self.delete_table(table_name=table_name, node=node1)
        self.create_table(table_name=table_name, node=node1)
        self.load_snapshot_and_refresh(table_name=table_name, node=node1, snapshot_folder=snapshot_folder)
        diff = self.compare_table_data(table_name=table_name, expected_table_data=data_before_refresh, node=node1)
        assert not diff, f"The following items are missing:\n{pformat(diff)}"

    def test_dynamo_gsi(self):
        self.prepare_dynamodb_cluster(num_of_nodes=4)
        node1 = self.cluster.nodelist()[0]
        self.create_table(node=node1, create_gsi=True)
        logger.info("Writing Alternator data on a table with GSI")
        items = generate_put_request_items(num_of_items=NUM_OF_ITEMS, add_gsi=True)
        node_resource_table = self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)

        node = self.cluster.nodelist()[1]
        logger.info(f"Stopping {node.name} before testing GSI query")
        node.stop()

        logger.info("Testing and validating a query using GSI")
        gsi_filtered_val = items[random.randint(0, NUM_OF_ITEMS - 1)][Gsi.ATTRIBUTE_NAME]
        expected_items = [item for item in items if item[Gsi.ATTRIBUTE_NAME] == gsi_filtered_val]
        key_condition = {Gsi.ATTRIBUTE_NAME: {"AttributeValueList": [gsi_filtered_val], "ComparisonOperator": "EQ"}}
        result_items = full_query(node_resource_table, IndexName=Gsi.NAME, KeyConditions=key_condition)
        diff_result = DeepDiff(t1=result_items, t2=expected_items, ignore_order=True)
        assert not diff_result, f"The following items differs:\n{pformat(diff_result)}"

    def test_drain_during_dynamo_load(self):
        """
        1. Create a load of read + update-items delete-set-elements
        2. Run nodetool drain for one node.
        3. The test verifies that Alternator queries load runs ok during drain, no db-node errors / core-dumps etc.
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, _, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)

        items = self.create_items(num_of_items=NUM_OF_ITEMS, use_set_data_type=True)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        read_and_delete_set_elements_thread = self.run_delete_set_elements_stress(table_name=TABLE_NAME, node=node1)
        logger.info(f"Start drain for: {node3.name}")
        node3.drain()
        logger.info("Drain finished")
        read_and_delete_set_elements_thread.join()

    def test_decommission_during_dynamo_load(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        alternator_consistent_stress = self.run_read_stress(table_name=TABLE_NAME, node=node1)
        logger.info(f"Start first decommission during consistent Alternator-load for: {node2.name}")
        node2.decommission()
        logger.info("Decommission finished")
        alternator_consistent_stress.join()
        alternator_non_consistent_stress = self.run_read_stress(table_name=TABLE_NAME, node=node1, consistent_read=False)
        logger.info(f"Start a second decommission during non-consistent alternator-load for: {node3.name}")
        node3.decommission()
        logger.info("Decommission finished")
        alternator_non_consistent_stress.join()

        logger.info("Check that the correct error is returned for a consistent read where cluster has no quorum")
        with pytest.raises(expected_exception=(ClientError,), match="Cannot achieve consistency level for cl LOCAL_QUORUM"):
            self.get_table_items(table_name=TABLE_NAME, node=node1, num_of_items=10, consistent_read=True)

        logger.info("Check that the correct error is returned for a resource of a decommissioned node")
        dynamodb_api_node2 = self.get_dynamodb_api(node=node2)
        # pylint:disable = attribute-defined-outside-init
        self.node2_resource_table = dynamodb_api_node2.resource.Table(TABLE_NAME)
        with pytest.raises(expected_exception=(EndpointConnectionError,), match="Could not connect to the endpoint URL"):
            self.get_table_items(table_name=TABLE_NAME, node=node2, num_of_items=10, consistent_read=True)

    def test_dynamo_reads_after_repair(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2 = self.cluster.nodelist()[:2]
        logger.info(f"Adding data for all nodes except {node2.name}...")
        node2.flush()
        logger.info(f"Stopping {node2.name}")
        node2.stop(wait_other_notice=True)
        self.create_table(table_name=TABLE_NAME, node=node1)

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)

        logger.info(f"Starting {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.info(f"starting repair on {node2.name}...")
        info = node2.repair()
        logger.info(f"{info[0]}\n{info[1]}")
        logger.info(f"Reading Alternator queries from node {node2.name}")
        self.get_table_items(table_name=TABLE_NAME, node=node2)

    def test_dynamo_queries_on_multi_dc(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3, is_multi_dc=True)
        dc1_node = self.cluster.nodelist()[0]
        self.create_table(table_name=TABLE_NAME, node=dc1_node)

        logger.info(f"Writing Alternator queries to node {dc1_node.name} on data-center {dc1_node.data_center}")
        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=dc1_node, new_items=items)

        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != dc1_node.data_center)
        logger.info(f"Reading Alternator queries from node {dc2_node.name} on data-center {dc2_node.data_center}")
        wait_for(func=lambda: not self.compare_table_data(expected_table_data=items, table_name=TABLE_NAME, node=dc2_node, consistent_read=False), timeout=5 * 60, text="Waiting until the DC2 will contain all items that insert in DC1")

    def test_dynamo_reads_after_new_node_repair(self):
        num_of_nodes = self._num_of_nodes_for_test(rf=3)
        self.prepare_dynamodb_cluster(num_of_nodes=num_of_nodes)
        node1, node2, node3, *_ = self.cluster.nodelist()
        logger.info("Adding data for all nodes")
        self.prefill_dynamodb_table(node=node1)
        logger.info(f"Decommissioning {node3.name}")
        node3.decommission()
        logger.info("Add node4..")
        node4 = new_node(self.cluster, bootstrap=True)
        logger.info("Start node4..")
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info(f"starting repair on {node4.name}...")
        stdout, stderr = node4.repair()
        logger.info(f"nodetool repair : stdout={stdout}, stderr={stderr}")
        logger.info(f"Stopping {node1.name}")
        node1.stop(wait_other_notice=True)
        logger.info(f"Stopping {node2.name}")
        node2.stop(wait_other_notice=True)
        tested_node = node4
        logger.info(f"Reading Alternator queries from node {tested_node.name}")
        self.get_table_items(table_name=TABLE_NAME, node=tested_node, consistent_read=False)

    def test_batch_with_auto_snapshot_false(self):
        """Test triggers scylladb/scylladb#6995"""

        self.prepare_dynamodb_cluster(num_of_nodes=1, extra_config=dict(auto_snapshot=False))
        node1 = self.cluster.nodelist()[0]
        table = self.create_table(node=node1, schema=schemas.CONDITION_EXPRESSION_SCHEMA)
        load = "x" * 10240
        with table.batch_writer() as batch:
            for i in range(10000):
                batch.put_item({"pk": random_string(length=DEFAULT_STRING_LENGTH), "c": i, "a": load})
        self.delete_table(TABLE_NAME, node1)

    def test_update_condition_unused_entries_short_circuit(self):
        """
        A test for https://github.com/scylladb/scylla/issues/6572 plus a multi DC configuration
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3, is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        logger.info("Creating a table..")
        table = self.create_table(table_name=TABLE_NAME, node=node1)
        new_pk_val = random_string(length=DEFAULT_STRING_LENGTH)
        logger.info("simple update item")
        table.update_item(Key={self._table_primary_key: new_pk_val}, AttributeUpdates={"a": {"Value": 1, "Action": "PUT"}})
        conditional_update_short_circuit = dict(
            Key={self._table_primary_key: new_pk_val},
            ConditionExpression="#name1 = :val1 OR #name2 = :val2 OR :val3 = :val2",
            UpdateExpression="SET #name3 = :val3",
            ExpressionAttributeNames={"#name1": "a", "#name2": "b", "#name3": "c"},
            ExpressionAttributeValues={":val1": 1, ":val2": 2, ":val3": 3},
        )
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        dc2_table = self.get_table(table_name=TABLE_NAME, node=dc2_node)
        wait_for(self.is_table_schema_synced, timeout=30, text="Waiting until table schema is updated", table_name=TABLE_NAME, nodes=[node1, dc2_node])
        node1.stop()
        logger.info("Testing and validating an update query using key condition expression")
        logger.info(f"ConditionExpression update of short circuit is: {conditional_update_short_circuit}")
        dc2_table.update_item(**conditional_update_short_circuit)
        dc2_node.stop()
        node1.start()

        self.wait_for_alternator(node=node1)
        logger.info(f"Reading Alternator queries from node {node1.name} on data-center {node1.data_center}")
        item = table.get_item(Key={self._table_primary_key: new_pk_val}, ConsistentRead=True)["Item"]
        assert item == {self._table_primary_key: new_pk_val, "a": 1, "c": 3}

    def test_modified_tag_is_propagated_to_other_dc(self):
        self.prepare_dynamodb_cluster(num_of_nodes=1, is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        table = self.prefill_dynamodb_table(node=node1)
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        logger.info("Check that updating write-isolation tag on one DC is propagated to a node of the other DC (dc2)")
        set_write_isolation(table, WriteIsolation.FORBID_RMW)
        wait_for(self.is_table_schema_synced, timeout=30, step=3, text="Waiting until table schema is updated", table_name=TABLE_NAME, nodes=[node1, dc2_node])

    def test_read_system_tables_via_dynamodb_api(self):
        """
        make sure we could only read system tables via dynamodb api

        https://github.com/scylladb/scylla/issues/6122
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        all_nodes = self.cluster.nodelist()

        # check each node peer are the node we expect
        for node in all_nodes:
            results = self.scan_table(".scylla.alternator.system.peers", node=node)
            peers = set(item["peer"] for item in results)

            # get all the other nodes ip addresses
            other_nodes_ips = set(get_ip_from_node(n) for n in set(all_nodes).difference({node}))

            assert peers == other_nodes_ips, f"peers in {node.name} are not as expected {other_nodes_ips}"

            logger.info("trying to write into system table, which should be readonly")
            with pytest.raises(expected_exception=(ClientError,), match="ResourceNotFoundException"):
                self.batch_write_actions(table_name=".scylla.alternator.system.peers", new_items=[dict(pk=1)], node=node)

    def test_table_name_with_dot_prefix(self):
        valid_dynamodb_chars = list(string.digits) + list(string.ascii_uppercase) + ["_", "-", "."]
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]

        table_name_with_dot_prefix = "." + "".join(random.choices(valid_dynamodb_chars, k=min(random.choice(range(SHORTEST_TABLE_SIZE, LONGEST_TABLE_SIZE + 1)), 100)))
        logger.info("Creating new table with dot ('.') char prefix")
        self.create_table(node=node1, table_name=table_name_with_dot_prefix)
        cmd = f"tablestats alternator_{table_name_with_dot_prefix}"
        logger.info(f"Executing the following command '{cmd}' (expected to fail)")
        try:
            node1.nodetool(cmd)
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            assert "Unknown keyspace: alternator_" in msg, msg

        # The slash at the end tells nodetool is required
        # when the keyspace contains dot(s)
        cmd = f"{cmd}/"
        logger.info(f"Executing the following command '{cmd}'")
        node1.nodetool(cmd)

    @pytest.mark.parametrize("create_gsi", [False, True])
    def test_alternator_nodetool_tablestats(self, create_gsi):
        """nodetool_additional_test.py::TesterAlternator::test_cfstats_syntax
        tests "nodetool cfstats" with various combinations of keyspace
        and table names. In this test we check the same thing but on an
        Alternator table, and using the new command name "tablestats".
        With with_gsi=True, this test to reproduces enterprise issue #3522,
        where "nodetool tablestats" produces no output if we have an
        Alternator GSI.
        """
        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]

        table_name = "some_long_table_name_abc"
        self.create_table(node=node1, table_name=table_name, create_gsi=create_gsi)

        logger.info('Trying "nodetool tablestats" without parameters')
        ret = node1.nodetool("tablestats")
        assert table_name in str(ret)

        logger.info('Trying "nodetool tablestats" of keyspace')
        # Note that Alternator always creates a keyspace whose name is
        # the table's name with the extra prefix "alternator_".
        ret = node1.nodetool(f"tablestats alternator_{table_name}")
        assert table_name in str(ret)

        logger.info('Trying "nodetool tablestats" of keyspace.table')
        ret = node1.nodetool(f"tablestats alternator_{table_name}.{table_name}")
        assert table_name in str(ret)

    @pytest.mark.parametrize("create_gsi", [False, True])
    def test_alternator_nodetool_info(self, create_gsi):
        """Tests that "nodetool info" works on Alternator tables.
        When create_gsi=True, it reproduces Scylla Enterprise issue #3512,
        where "nodetool info" throws an exception if an Alternator
        GSI exists.
        """
        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]
        self.create_table(node=node1, table_name="tbl", create_gsi=create_gsi)
        node1.nodetool("info")
        # We don't check anything specific in the output, just that
        # "nodetool info" didn't fail as it used to.

    def test_putitem_contention(self):  # pylint:disable=too-many-locals
        """
        This test reproduces issue #7218, where PutItem operations sometimes
        lost part of the item being written - some attributes were lost, and
        the name of other attributes replaced by empty strings. The problem
        happenes when the write-isolation policy is LWT and there is
        contention of writes to the same partition (not necessarily the same
        item) happening on more than one coordinator.
        To reproduce this contention, we need to start (at least) two nodes,
        and connect to them concurrently from two threads.
        """
        # Create table with 1 node cluster so that it gets RF=1.
        # Otherwise, with RF=2 the contention will cause write timeouts.
        # With rf_rack_valid_keyspaces being enabled by default, RF is equal to number of nodes.
        # Prior to that, if number of nodes is smaller than 3, alternator creates the keyspace with RF=1.
        self.prepare_dynamodb_cluster(num_of_nodes=1)
        [node1] = self.cluster.nodelist()
        node1_resource = self.get_dynamodb_api(node=node1).resource

        # Create the table, access it through the two connections, r1 and r2:
        table_name = "test_putitem_contention_table"
        table_r1 = node1_resource.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "p", "KeyType": "HASH"}, {"AttributeName": "c", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}, {"AttributeName": "c", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = node1_resource.meta.client.get_waiter("table_exists")
        waiter.wait(TableName=table_name)

        node2 = new_node(self.cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node2_resource = self.get_dynamodb_api(node=node2).resource
        table_r2 = node2_resource.Table(table_name)

        def writes(tab, rang):
            for i in rang:
                tab.put_item(Item={"p": "hi", "c": f"item{i}", "v1": "dog", "v2": "cat"})

        # Create two writing threads, each writing 1000 *different* rows to
        # one different connection:
        total_items = 2000
        executor = ThreadPoolExecutor(max_workers=2)
        thread1 = executor.submit(writes, tab=table_r1, rang=range(1000))
        thread2 = executor.submit(writes, tab=table_r2, rang=range(1000, 2000))
        thread1.result()
        thread2.result()
        # Scan the table, looking for broken items (issue #7218)
        n_items = 0
        n_bad_items = 0

        def check_item(item):
            nonlocal n_items
            nonlocal n_bad_items
            n_items = n_items + 1
            if not "v1" in item or not "v2" in item:
                n_bad_items = n_bad_items + 1
                print(f"Bad item: {item}")

        def check_items(items):
            for item in items:
                check_item(item)

        response = table_r1.scan(ConsistentRead=True)
        check_items(response["Items"])
        while "LastEvaluatedKey" in response:
            response = table_r1.scan(ExclusiveStartKey=response["LastEvaluatedKey"], ConsistentRead=True)
            check_items(response["Items"])
        assert n_items == total_items
        assert n_bad_items == 0

    def test_tls_connection(self):
        """
        Create a HTTPS (SSL/TLS) connection, and verify the test can create a table and insert data into it.
        Also, check the log file for each node contains a log that only the HTTPS (SSL/TLS) connection is open, and the
        unsecured connection is closed.
        """
        new_items = []
        table_name = TABLE_NAME

        logger.info('Configuring secured Alternator session with "self signed x509 certificate"')
        self.prepare_dynamodb_cluster(num_of_nodes=1, is_encrypted=True)
        nodes = self.cluster.nodelist()
        node1 = nodes[0]

        logger.debug("Create table")
        self.create_table(table_name=table_name, node=node1)
        for node_idx, node in enumerate(nodes):
            node.grep_log(f"Alternator server listening on {get_ip_from_node(node=node)}, HTTP port OFF, HTTPS port {ALTERNATOR_SECURE_PORT}")
            new_items = self.create_items(num_of_items=(node_idx + 1) * 10)
            self.batch_write_actions(table_name=table_name, node=node, new_items=new_items)
        self.compare_table_data(expected_table_data=new_items, table_name=table_name, node=node1)

    def test_limit_concurrent_requests(self):
        """
        Test Support limiting the number of concurrent requests in alternator.
        Verifies https://github.com/scylladb/scylla/issues/7294
        Test scenario:
        1) Configure cluster requests-concurrency-limit to a low number.
        2) Issue Alternator 'heavy' requests concurrently (create-table)
        3) wait for RequestLimitExceeded error response.
        """
        concurrent_requests_limit = 5
        extra_config = {"max_concurrent_requests_per_shard": concurrent_requests_limit, "num_tokens": 1}
        self.prepare_dynamodb_cluster(num_of_nodes=1, extra_config=extra_config)
        node1 = self.cluster.nodelist()[0]
        create_tables_threads = []
        for tables_num in range(concurrent_requests_limit * 5):
            create_tables_threads.append(self.run_create_table_thread())

        @retrying(num_attempts=150, sleep_time=0.2, allowed_exceptions=ConcurrencyLimitNotExceededError, message="Running create-table request")
        def wait_for_create_table_request_failure():
            try:
                self.create_table(table_name=random_string(length=10), node=node1, wait_until_table_exists=False)
            except Exception as error:
                if "RequestLimitExceeded" in error.args[0]:
                    return
                raise
            raise ConcurrencyLimitNotExceededError

        wait_for_create_table_request_failure()

        for thread in create_tables_threads:
            thread.join()

    @staticmethod
    def _set_slow_query_logging_api(run_on_node: ScyllaNode, is_enable: bool = True, threshold: int | None = None):
        """
        :param run_on_node: node to send the REST API command.
        :param is_enable: enable/disable slow-query logging
        :param threshold: a numeric value for the minimum duration to a query as slow.
        """
        enable = "true" if is_enable else "false"
        api_cmd = f"http://{run_on_node.address()}:10000/storage_service/slow_query?enable={enable}"
        logger.info(f"Send restful api: {api_cmd}")
        result = requests.post(api_cmd)
        result.raise_for_status()
        if threshold is not None:
            api_cmd = f"http://{run_on_node.address()}:10000/storage_service/slow_query?threshold={threshold}"
            logger.info(f"Send restful api: {api_cmd}")
            result = requests.post(api_cmd)
            result.raise_for_status()
        api_cmd = f"http://{run_on_node.address()}:10000/storage_service/slow_query"
        logger.info(f"Send restful api: {api_cmd}")
        response = requests.get(api_cmd)
        response_json = response.json()
        if response_json["enable"] != is_enable:
            raise SlowQueriesLoggingError(f"Got unexpected slow-query-logging values. enable: {response_json['enable']}")
        if threshold is not None and response_json["threshold"] != threshold:
            raise SlowQueriesLoggingError(f"Got unexpected threshold value: {response_json['threshold']}")

    @retrying(num_attempts=100, sleep_time=0.2, allowed_exceptions=SlowQueriesLoggingError, message="wait_for_slow_query_logs")
    def wait_for_slow_query_logs(self, node):
        """
        Wait for a non-empty query of the scylla.alternator.system_traces.node_slow_log table.
        :param node: the node for running table full scan
        :return: full scan of node_slow_log table
        """
        results = self.scan_table(".scylla.alternator.system_traces.node_slow_log", node=node, consistent_read=False)
        if results:
            return results
        raise SlowQueriesLoggingError

    @staticmethod
    def is_found_in_slow_queries_log(name: str, log_result: list) -> bool:
        """
        :param name: name of alternator operation name of object name to search for.
        :param log_result: a slow-query-logging table full scan result to search in.
        :return: is name found in the given result full-scan query.
        """
        if not any(name in result_op["parameters"] for result_op in log_result):
            logger.info(f"{name} is not found in slow-query-log result\n Log result start: {log_result[0]}\n Log result end: {log_result[-1]}")
            return False
        return True

    def create_tables(self, count: int, node) -> list:
        """

        :param count: number of tables to create
        :param node: the node to run create-table commands on.
        :return: a list of table names.
        """
        table_names = []
        for _ in range(count):
            name = random_string(length=10)
            self.create_table(table_name=name, node=node)
            table_names.append(name)
        return table_names

    @retrying(num_attempts=100, sleep_time=0.2, allowed_exceptions=SlowQueriesLoggingError, message="wait_for_a_specific_slow_query_logs")
    def wait_for_create_table_slow_query_logs(self, node, table_names: list):
        """
        Verify all created tables are logged as slow-query operation.
        """
        logger.info("Get logging results for 'createTable' queries")
        results = self.wait_for_slow_query_logs(node=node)
        create_table_results = [res for res in results if "CreateTable" in res["parameters"]]
        for table in table_names:
            if not self.is_found_in_slow_queries_log(name=table, log_result=create_table_results):
                raise SlowQueriesLoggingError(f"Table {table} not found in slow-query-log full-scan")

    def test_slow_query_logging(self):
        """
        Test slow query logging for alternator queries.
        Verifies https://github.com/scylladb/scylla/pull/8298
        Test scenario:
        1) Configure slow-query threshold to a low number.
        2) Issue Alternator long-enough queries
        3) Verify slow queries of create-table are logged.
        4) Verify slow queries of create-table are stopped being logged after set to disabled.
        """
        # Setting a small enough threshold value that is lower than createTable operation minimum duration.
        slow_query_threshold = 2
        num_of_nodes = self._num_of_nodes_for_test(rf=3)
        self.prepare_dynamodb_cluster(num_of_nodes=num_of_nodes)
        node1 = self.cluster.nodelist()[0]
        logger.info("Running background stress and topology changes..")
        self.create_table(table_name=TABLE_NAME, node=node1)
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        decommission_thread = self.run_decommission_add_node_thread()

        logger.info("Enable slow-query-logging with specified threshold")
        self._set_slow_query_logging_api(run_on_node=node1, threshold=slow_query_threshold)
        logger.info("Execute 5 'createTable' slow-enough queries")
        first_table_names = self.create_tables(count=5, node=node1)
        logger.info("Verify all created tables are found in log results")
        self.wait_for_create_table_slow_query_logs(node=node1, table_names=first_table_names)
        logger.info("Disable slow-query-logging")
        self._set_slow_query_logging_api(run_on_node=node1, is_enable=False)
        logger.info("Execute 5 additional 'createTable' slow-enough queries after slow-query-logging is disabled")
        second_table_names = self.create_tables(count=5, node=node1)
        results = self.wait_for_slow_query_logs(node=node1)
        logger.info("Verify latter created tables are not found in log results")
        for table_name in second_table_names:
            assert not self.is_found_in_slow_queries_log(name=table_name, log_result=results), f"Found unexpected logged slow query: {table_name}"

        stress_thread.join()
        decommission_thread.join()

    def test_delete_elements_from_a_set(self):
        """
        Verifies https://github.com/scylladb/scylla/commit/253387ea07962d4fd8cb221eb90298b9127caf9f
        alternator: implement AttributeUpdates DELETE operation with Value
        Test scenario:
        1) Generate a load with set-type data.
        2) Issue topology-change operations (add/remove node)
        3) Run AttributeUpdates DELETE operation on a set of strings.
        4) Verify the data can be read and no unexpected errors.
        """
        num_of_nodes = self._num_of_nodes_for_test(rf=3)
        self.prepare_dynamodb_cluster(num_of_nodes=num_of_nodes)
        node1 = self.cluster.nodelist()[0]
        num_of_items = 300
        items = self.create_items(num_of_items=num_of_items, use_set_data_type=True)
        self.create_table(table_name=TABLE_NAME, node=node1)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        logger.info("Running background stress and topology changes..")
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=num_of_items, use_set_data_type=True, ignore_errors=True)
        decommission_thread = self.run_decommission_add_node_thread()

        logger.info("Run AttributeUpdates DELETE operations")
        self.update_table_delete_set_elements(table_name=TABLE_NAME, node=node1, num_of_items=num_of_items, consistent_read=False)
        stress_thread.join()
        self.update_table_delete_set_elements(table_name=TABLE_NAME, node=node1, num_of_items=num_of_items, consistent_read=False)
        decommission_thread.join()
        logger.info("Reading all existing data after delete-operations are completed")
        items = self.get_table_items(table_name=TABLE_NAME, node=node1, consistent_read=False, num_of_items=num_of_items)
        assert [item for item in items if NUM_OF_ELEMENTS_IN_SET > len(item["Item"]["hello_set"]) > 0]
