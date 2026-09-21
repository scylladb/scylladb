#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import operator
import os
import random
import string
import tempfile
import time
import threading
from ast import literal_eval
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
from pprint import pformat
from typing import Any

import boto3.dynamodb.types
import pytest
import requests
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError, EndpointConnectionError
from cassandra import InvalidRequest
from ccmlib.scylla_node import NodetoolError, ScyllaNode
from deepdiff import DeepDiff

from alternator.utils import schemas
from alternator.utils.data_generator import AlternatorDataGenerator, TypeMode
from alternator_utils import (
    ALTERNATOR_SECURE_PORT,
    ALTERNATOR_SNAPSHOT_FOLDER,
    DEFAULT_STRING_LENGTH,
    LONGEST_TABLE_SIZE,
    NUM_OF_ELEMENTS_IN_SET,
    NUM_OF_ITEMS,
    NUM_OF_NODES,
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
from dtest_class import get_ip_from_node, read_barrier, wait_for
from tools.cluster import new_node
from tools.misc import set_trace_probability
from tools.retrying import retrying
from upgrade_test import UpgradeTester, upgrade_matrix_for_alternator
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
        # node2 may start serving requests before it has pulled the schema of the table created
        # while it was down. Issue a read barrier to make sure it sees the latest schema before repair.
        read_barrier(node2)
        logger.info(f"starting repair on {node2.name}...")
        node2.repair(options=[self.keyspace_name_template.format(TABLE_NAME), TABLE_NAME])
        logger.info(f"repair finished on {node2.name}")
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
        node4.repair(options=[self.keyspace_name_template.format(TABLE_NAME), TABLE_NAME])
        logger.info(f"repair finished on {node4.name}")
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
        # Wait for cross-DC replication to reach both live DC1 nodes
        # before stopping dc2_node.  The LWT commit uses LOCAL_QUORUM,
        # which only guarantees DC2 persistence; replication to DC1 is
        # async background work.  Without this wait, stopping dc2_node
        # can drop in-flight RPCs to DC1 while CAS mutations don't
        # store hints.  We must confirm both live DC1 replicas have the
        # data so that the later ConsistentRead=True (LOCAL_QUORUM)
        # read on restarted node1 is guaranteed to succeed.
        # See https://scylladb.atlassian.net/browse/SCYLLADB-1267
        dc1_live_nodes = [
            node for node in self.cluster.nodelist()
            if node.data_center == node1.data_center and node.server_id != node1.server_id
        ]
        dc1_live_tables = [self.get_table(table_name=TABLE_NAME, node=node) for node in dc1_live_nodes]
        wait_for(
            lambda: all(
                t.get_item(
                    Key={self._table_primary_key: new_pk_val}, ConsistentRead=False
                ).get("Item", {}).get("c") == 3
                for t in dc1_live_tables
            ),
            timeout=60,
            text="Waiting for cross-DC replication of conditional update to both live DC1 nodes",
        )
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
        # Keep the limit low to avoid exhausting LSA memory on the 1GB test node
        # when multiple CreateTable requests (Raft + schema + flush) run concurrently.
        concurrent_requests_limit = 3
        extra_config = {"max_concurrent_requests_per_shard": concurrent_requests_limit, "num_tokens": 1}
        self.prepare_dynamodb_cluster(num_of_nodes=1, extra_config=extra_config)
        node1 = self.cluster.nodelist()[0]
        stop_workers = threading.Event()

        def run_create_table_until_limited() -> None:
            while not stop_workers.is_set():
                try:
                    self.create_table(table_name=random_string(length=10), node=node1, wait_until_table_exists=False)
                except Exception as error:  # noqa: BLE001
                    if "RequestLimitExceeded" in str(error):
                        stop_workers.set()
                        return
                    raise

        with ThreadPoolExecutor(max_workers=concurrent_requests_limit * 5) as executor:
            create_table_futures = [executor.submit(run_create_table_until_limited) for _ in range(concurrent_requests_limit * 5)]

            if not stop_workers.wait(timeout=30):
                raise ConcurrencyLimitNotExceededError

            stop_workers.set()
            for future in create_table_futures:
                future.result(timeout=60)

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

    def test_ttl_with_load_and_decommission(self):
        """
        1. Configure a table with TTL enabled and an 'expiration' column.
        2. Create a load of read/write/update-items delete-set-elements
        3. Run a loop of flush and compaction on cluster nodes.
        4. Verify all data is eventually deleted on cluster nodes.
        """
        ttl_polling_interval = 4
        if self.cluster.scylla_mode != "debug":
            nodes = 4
            topo = {"dc1": {"rack1": 1, "rack2": 1, "rack3": 2}}
        else:
            nodes = 2
            topo = {"dc1": {"rack1": 2}}
        self.prepare_dynamodb_cluster(num_of_nodes=nodes, extra_config={"alternator_ttl_period_in_seconds": ttl_polling_interval}, topo=topo)
        node1, *_ = self.cluster.nodelist()
        table = self.create_table(node=node1)
        # Enable TTL for table
        self.get_dynamodb_api(node=node1).client.update_time_to_live(TableName=table.name, TimeToLiveSpecification={"AttributeName": "expiration", "Enabled": True})
        expiration_sec = 10
        num_of_items = 100
        logger.info("Running background stress and topology changes..")
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=num_of_items, use_set_data_type=True, expiration_sec=expiration_sec, random_start_index=True, ignore_errors=True)
        read_and_delete_set_elements_thread = self.run_delete_set_elements_stress(table_name=TABLE_NAME, node=node1, random_start_index=True)
        # hardcoded decommission that repeats picking logic of run_decommission_add_node_thread
        node_to_decommission = self.cluster.nodelist()[-1]
        decommission_thread = self.run_decommission_add_node_thread()

        logger.info("Run flush and compaction on cluster nodes")
        iterations = 3 if self.cluster.scylla_mode != "debug" else 1

        def get_live_nodes():
            return [node for node in self.cluster.nodelist()
                    if node.is_live() and node.server_id != node_to_decommission.server_id]

        nodes_for_maintenance = get_live_nodes()

        for _ in range(iterations):
            try:
                random.choice(nodes_for_maintenance).flush()
                random.choice(nodes_for_maintenance).compact()
            except NodetoolError as exc:
                error_message = str(exc)
                valid_errors = ["ConnectException", "Connection refused", "status code 404 Not Found"]
                if not any(err in error_message for err in valid_errors):
                    raise

        logger.info("Stopping background stress and decommission")
        read_and_delete_set_elements_thread.join()
        stress_thread.join()
        decommission_thread.join()

        logger.info("Verify all data is eventually deleted")

        @retrying(num_attempts=expiration_sec + ttl_polling_interval, sleep_time=2, allowed_exceptions=TtlNotExpiredError, message="Wait for TTL expiration threshold of a configured table")
        def wait_for_deletion_post_ttl_expiration(node: ScyllaNode):
            """
            Wait for TTL expiration threshold of a configured table.
            :param node: the node for running table full scan
            """
            table_data = self.scan_table(table_name=TABLE_NAME, node=node, ConsistentRead=False)
            logger.info("scan table result: %s", table_data)
            if not table_data:
                return
            raise TtlNotExpiredError
        # Check 2 nodes for TTL expiration of all data
        for db_node in self.cluster.nodelist()[:1]:
            wait_for_deletion_post_ttl_expiration(node=db_node)

    def test_read_key_condition_expression(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        self.create_table(node=node1, schema=schemas.CONDITION_EXPRESSION_SCHEMA)
        logger.info("Writing Alternator items of the same partition key")
        pk_condition_value = random_string(length=DEFAULT_STRING_LENGTH)
        items = [{"pk": pk_condition_value, "c": Decimal(i), "a": random_string(length=DEFAULT_STRING_LENGTH)} for i in range(12)]
        table = self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        logger.info("Writing an extra different partition key")
        with table.batch_writer() as batch:
            batch.put_item({"pk": random_string(length=DEFAULT_STRING_LENGTH), "c": 123, "a": random_string(length=DEFAULT_STRING_LENGTH)})
        node = self.cluster.nodelist()[1]
        logger.info(f"Stopping {node.name} before testing key condition expression query")
        node.stop()
        logger.info("Testing and validating a query using key condition expression")
        got_condition_items = full_query(table, KeyConditionExpression="pk=:pk", ExpressionAttributeValues={":pk": pk_condition_value})
        diff_result = DeepDiff(t1=items, t2=got_condition_items, ignore_order=True)
        assert not diff_result, f"The following items differs:\n{pformat(diff_result)}"

    def test_nested_attributes(self):
        """
        Test scenario:
        1) Create a cluster, a table, fill it with items of nested attributes.
        2) Run background 'noise': write-stress, read-stress and decommission-add-node-thread.
        3) Run a main loop of alternator update queries which updates nested attributes.
        4) After each update query, the item is read again by a query and verified to have the expected updated data.
        """
        topo = {"dc1": {"rack1": 1, "rack2": 1, "rack3": 2}}
        self.prepare_dynamodb_cluster(num_of_nodes=4, topo=topo)
        node1, node2, node3 = self.cluster.nodelist()[:3]
        self.create_table(node=node1)
        nested_attributes_levels = 10
        num_of_items = 4000
        self.put_table_items(table_name=TABLE_NAME, node=node1, nested_attributes_levels=nested_attributes_levels, num_of_items=num_of_items * 2)
        write_stress = self.run_write_stress(table_name=TABLE_NAME, node=node1, nested_attributes_levels=nested_attributes_levels, num_of_item=num_of_items)
        get_items_thread = self.run_read_stress(table_name=TABLE_NAME, node=node2, num_of_item=num_of_items * 2, consistent_read=True)
        decommission_thread = self.run_decommission_add_node_thread()
        for _ in range(num_of_items):
            self.update_table_nested_items(table_name=TABLE_NAME, node=node3, consistent_read=True, nested_attributes_levels=nested_attributes_levels, start_index=num_of_items, num_of_items=num_of_items)

        write_stress.join()
        get_items_thread.join()
        decommission_thread.join()

    def test_write_isolation_during_stress(self):
        """
        Modify tables write-isolation during stress
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        logger.info("Adding data for tables of all write-isolation types")
        conf_workloads = []
        for isolation in WriteIsolation:
            table_name = f"{TABLE_NAME}_{isolation.value}"
            table = self.prefill_dynamodb_table(node=node1, table_name=table_name)
            set_write_isolation(table=table, isolation=isolation)
            conf_workloads.append({"table": table, "write_stress": self.run_write_stress(table_name=table_name, node=node1), "read_stress": self.run_read_stress(table_name=table_name, node=node1)})

        cycles = 3
        for cycle in range(1, cycles + 1):
            for conf in conf_workloads:
                logger.info(f"cycle {cycle}/{cycles}: modifying {conf['table']}")
                set_write_isolation(table=conf["table"], isolation=random.choice(list(WriteIsolation)))
            time.sleep(5)

        for conf in conf_workloads:
            conf["write_stress"].join()
            conf["read_stress"].join()

    def test_filter_expression(self):
        self.prepare_dynamodb_cluster(is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        schema = schemas.HASH_AND_NUM_RANGE_SCHEMA
        self.create_table(node=node1, schema=schema)
        hash_key_name, range_key_name = schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME
        items = [{hash_key_name: f"{hash_value}", range_key_name: range_value} for hash_value in range(10) for range_value in range(10)]
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items, schema=schema)
        selected_range_value = random.choice(items)[range_key_name]
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        wait_for(self.is_table_schema_synced, timeout=30, text="Waiting until table schema is updated", table_name=TABLE_NAME, nodes=[node1, dc2_node])
        node1.stop()
        logger.info("Testing a query using filter expression")
        expected_items = [item for item in items if item[range_key_name] >= selected_range_value]
        diff = self.compare_table_data(table_name=TABLE_NAME, expected_table_data=expected_items, node=dc2_node, FilterExpression=Attr(range_key_name).gte(selected_range_value))
        assert not diff, f"The following items differs:\n{pformat(diff)}"

    def test_update_condition_expression_and_write_isolation(self):
        """
        See that using conditional update queries run correctly when LWT is enabled.
        Check that they can't run when LWT is disabled for table, when using "forbid_lwt" write-isolation.
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3, is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        node2 = next(node for node in self.cluster.nodelist() if node.data_center == node1.data_center and node.name != node1.name)
        logger.info("Adding data for all nodes from DC1")
        table = self.prefill_dynamodb_table(node=node1)

        logger.info(f"Stopping {node2.name} (before testing update query with key condition expression)")
        node2.stop()

        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        dynamodb_api = self.get_dynamodb_api(node=dc2_node)
        dc2_table = dynamodb_api.resource.Table(name=TABLE_NAME)

        logger.info("Testing and validating an update query using key condition expression")
        new_pk_val = random_string(length=DEFAULT_STRING_LENGTH)
        logger.info("simple update from dc1")
        table.update_item(Key={self._table_primary_key: new_pk_val}, AttributeUpdates={"a": {"Value": 1, "Action": "PUT"}})
        logger.info("ConditionExpression update from dc2:")
        conditional_update_c_2 = dict(Key={self._table_primary_key: new_pk_val}, UpdateExpression="SET c = :val", ConditionExpression="attribute_exists (a)", ExpressionAttributeValues={":val": 2})
        logger.info(conditional_update_c_2)
        logger.info("Check that conditional update fails on write-isolation 'forbid' mode (dc2)")
        set_write_isolation(table, WriteIsolation.FORBID_RMW)
        wait_for(self.is_table_schema_synced, timeout=30, text="Waiting until table schema is updated", table_name=TABLE_NAME, nodes=[node1, dc2_node])
        msg_rmw_is_disabled = "Read-modify-write operations are disabled"
        with pytest.raises(expected_exception=(ClientError,), match=msg_rmw_is_disabled):
            res = dc2_table.update_item(**conditional_update_c_2)
            logger.info(str(res))

        set_write_isolation(table, WriteIsolation.ALWAYS_USE_LWT)
        wait_for(self.is_table_schema_synced, timeout=30, text="Waiting until table schema is updated", table_name=TABLE_NAME, nodes=[node1, dc2_node])
        dc2_table.update_item(**conditional_update_c_2)
        logger.info("ConditionExpression update from dc1:")
        conditional_update_c_3 = dict(Key={self._table_primary_key: new_pk_val}, UpdateExpression="SET c = :val", ConditionExpression="attribute_not_exists (b)", ExpressionAttributeValues={":val": 3})
        logger.info(conditional_update_c_3)
        with pytest.raises(expected_exception=(ClientError,), match=msg_rmw_is_disabled):
            set_write_isolation(table, WriteIsolation.FORBID_RMW)
            table.update_item(**conditional_update_c_3)

        set_write_isolation(table, WriteIsolation.ONLY_RMW_USES_LWT)
        table.update_item(**conditional_update_c_3)
        assert table.get_item(Key={self._table_primary_key: new_pk_val}, ConsistentRead=True)["Item"]["c"] == 3
        with pytest.raises(expected_exception=(ClientError,), match="ConditionalCheckFailedException"):
            table.update_item(Key={self._table_primary_key: new_pk_val}, UpdateExpression="SET c = :val", ConditionExpression="attribute_not_exists (a)", ExpressionAttributeValues={":val": 4})

    def _reboot_nodes_while_running_stress(self, node):
        for _node in self.cluster.nodelist():
            if _node.name != node.name:
                logger.info(f"Stopping node '{_node.name}'")
                _node.stop()
                logger.info(f"Starting node '{_node.name}'")
                _node.start(wait_other_notice=True, wait_for_binary_proto=True)

    def test_full_scan_table_while_restart_each_nodes(self):
        """
        Checks scan logic while each node is stopping and after that starting
        """
        table_name, num_of_items, node_idx = TABLE_NAME, NUM_OF_ITEMS, 0
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[node_idx]
        self.prefill_dynamodb_table(node=node1, table_name=table_name, num_of_items=num_of_items)
        alternator_scan_thread = self.run_scan_stress(table_name=table_name, node=node1)

        logger.info("Starting Alternator scan stress..")
        alternator_scan_thread.start()
        self._reboot_nodes_while_running_stress(node=node1)

    def test_full_parallel_scan_table_while_restart_each_nodes(self):
        """
        Checks parallel scan logic while each node is stopping and after that starting
        """
        table_name, num_of_items, node_idx, threads_num = TABLE_NAME, NUM_OF_ITEMS, 0, 4
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[node_idx]
        self.prefill_dynamodb_table(node=node1, table_name=table_name, num_of_items=num_of_items)
        alternator_scan_thread = self.run_scan_stress(table_name=table_name, node=node1, threads_num=threads_num)

        logger.info("Starting Alternator scan stress..")
        alternator_scan_thread.start()
        self._reboot_nodes_while_running_stress(node=node1)

    def test_full_parallel_scan_table_while_insert_update_delete_items(self):
        """
        Checks parallel scan logic while each node is stopping and after that starting and in the background there is
         a thread that creates and updates items.
        """
        table_name, num_of_items, node_idx, threads_num = TABLE_NAME, NUM_OF_ITEMS, 0, 4
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[node_idx]
        self.prefill_dynamodb_table(node=node1, table_name=table_name, num_of_items=num_of_items)
        self.update_items(table_name=table_name, node=node1)
        alternator_scan_thread = self.run_scan_stress(table_name=table_name, node=node1, threads_num=threads_num, is_compare_scan_result=False)

        insert_update_thread = self.run_delete_insert_update_item_stress(table_name=table_name, node=node1)
        logger.info("Starting Alternator create and update items stress..")
        insert_update_thread.start()

        logger.info("Starting Alternator scan stress..")
        alternator_scan_thread.start()
        self._reboot_nodes_while_running_stress(node=node1)

    def test_dynamo_types(self):
        """
        Create items with each of DynamoDB supported types and verify:
            * No errors while inserting items to DB
            * Get all the values from DB and compare to what we know we inserted
        """
        all_items = []
        data_generator = AlternatorDataGenerator(primary_key=self._table_primary_key, primary_key_format=self._table_primary_key_format)
        data_generator.create_random_number_item()
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[0]
        self.create_table(table_name=TABLE_NAME, node=node1)

        for mode in TypeMode:
            items = data_generator.create_multiple_items(num_of_items=random.randint(1, 10), mode=mode)
            all_items += items
            logger.info(f"Adding {len(items)} {data_generator.get_mode_name(mode)} items to table '{TABLE_NAME}'..")
            self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
            diff = self.compare_table_data(table_name=TABLE_NAME, expected_table_data=all_items, node=node1)
            assert not diff, f"The following items are missing:\n{pformat(diff)}"

    def _check_comparison_query_key_conditions_options(self, scan_index_forward=True):  # pylint:disable=too-many-locals
        schema = schemas.HASH_AND_NUM_RANGE_SCHEMA
        python_compare_op_dict = {"LE": operator.le, "LT": operator.lt, "GE": operator.ge, "GT": operator.gt}
        hash_key_name, range_key_name = schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME
        table_name, node_idx = TABLE_NAME, 0

        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node = self.cluster.nodelist()[node_idx]
        if self.is_table_exists(table_name=table_name, node=node):
            self.delete_table(table_name=table_name, node=node)
        self.create_table(node=node, table_name=table_name, schema=schema)

        items = [{hash_key_name: f"{hash_value}", range_key_name: range_value} for hash_value in range(random.randint(1, 10)) for range_value in range(random.randint(1, 10))]
        node_resource_table = self.batch_write_actions(table_name=table_name, node=node, new_items=items, schema=schema)
        selected_item = random.choice(items)
        selected_hash_value = selected_item[hash_key_name]
        selected_range_value = selected_item[range_key_name]
        all_selected_hash_items = [item for item in items if selected_hash_value == item[hash_key_name]]

        for compare_op in ["EQ", "LE", "LT", "GE", "GT"]:
            key_condition = {
                hash_key_name: {"AttributeValueList": [selected_hash_value], "ComparisonOperator": "EQ"},
                range_key_name: {"AttributeValueList": [selected_range_value], "ComparisonOperator": compare_op},
            }
            query_result = full_query(node_resource_table, KeyConditions=key_condition, ScanIndexForward=scan_index_forward)
            if compare_op == "EQ":
                expected_items = [selected_item]
            else:
                py_compare_op = python_compare_op_dict[compare_op]
                expected_items = [item for item in all_selected_hash_items[:: -1 if not scan_index_forward else 1] if py_compare_op(item[range_key_name], selected_range_value)]

            logger.info(f"Running query with key condition '{key_condition}'")
            diff = DeepDiff(t1=expected_items, t2=query_result, ignore_numeric_type_changes=True)
            assert not diff, f"The following items differs:\n{pformat(diff)}"

    def test_check_comparison_query_key_condition_options(self):
        """
        Check the result of Query for each "EQ", "LE", "LT", "GE" and "GT" comparison operation when ScanIndexForward is
        True (The resulting order is ascending)
        """
        self._check_comparison_query_key_conditions_options()

    def test_check_comparison_query_key_condition_options_without_scan_index_forward(self):
        """
        Check the result of Query for each "EQ", "LE", "LT", "GE" and "GT" comparison operation when ScanIndexForward is
        True (The resulting order is descending)
        """
        self._check_comparison_query_key_conditions_options(scan_index_forward=False)

    def _check_string_query_key_conditions_options(self, scan_index_forward=True):
        secondary_key_values = [chr(char_value) for char_value in range(256)]
        binary_items = []
        hash_key_name, range_key_name = schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME
        table_name, node_idx = TABLE_NAME, 0
        regular_items = [{hash_key_name: f"{hash_value}", range_key_name: range_value} for hash_value in range(random.randint(1, 10)) for range_value in secondary_key_values]
        for item in deepcopy(regular_items):
            item[range_key_name] = boto3.dynamodb.types.Binary(item[range_key_name].encode())
            binary_items.append(item)

        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node = self.cluster.nodelist()[node_idx]

        def test_logic(schema):  # pylint:disable=too-many-locals
            is_binary_mode = bool(schema == schemas.HASH_AND_BINARY_RANGE_SCHEMA)
            logger.info(f"Check Query string comparison for item with {'binary' if is_binary_mode else 'string'}")
            if self.is_table_exists(table_name=table_name, node=node):
                self.delete_table(table_name=table_name, node=node)
            self.create_table(node=node, table_name=table_name, schema=schema)
            items = binary_items if is_binary_mode else regular_items

            selected_hash_value = random.choice(items)[hash_key_name]
            all_selected_hash_items = [_item for _item in items if _item[hash_key_name] == selected_hash_value]
            node_resource_table = self.batch_write_actions(table_name=table_name, node=node, new_items=items)

            for compare_op in ["BEGINS_WITH", "BETWEEN"]:
                expected_items = []
                if compare_op == "BETWEEN":
                    selected_range_values = random.choices(population=secondary_key_values, k=2)
                    low, high = selected_range_values[0], selected_range_values[1]
                    if high < low:
                        # Swap between 2 variables
                        low, high = high, low
                        selected_range_values = [low, high]

                    for _item in all_selected_hash_items:
                        range_value = _item[range_key_name].value.decode() if is_binary_mode else _item[range_key_name]
                        if low <= range_value <= high:
                            expected_items.append(_item)
                elif compare_op == "BEGINS_WITH":
                    selected_range_values = [random.choice(secondary_key_values)]
                    for _item in all_selected_hash_items:
                        range_value = _item[range_key_name].value.decode() if is_binary_mode else _item[range_key_name]
                        if range_value.startswith(selected_range_values[0]):
                            expected_items.append(_item)
                else:
                    raise ValueError(f"The following value '{compare_op}' not supported")

                if not scan_index_forward:
                    expected_items = expected_items[::-1]
                if is_binary_mode:
                    selected_range_values = [boto3.dynamodb.types.Binary(val.encode()) for val in selected_range_values]

                key_condition = {
                    hash_key_name: {"AttributeValueList": [selected_hash_value], "ComparisonOperator": "EQ"},
                    range_key_name: {"AttributeValueList": selected_range_values, "ComparisonOperator": compare_op},
                }
                query_result = full_query(node_resource_table, KeyConditions=key_condition, ScanIndexForward=scan_index_forward)
                logger.info(f"Running query with key condition '{key_condition}'")
                diff = DeepDiff(t1=expected_items, t2=query_result)
                assert not diff, f"The following items differs:\n{pformat(diff)}"

        test_logic(schema=schemas.HASH_AND_STR_RANGE_SCHEMA)
        test_logic(schema=schemas.HASH_AND_BINARY_RANGE_SCHEMA)

    def test_check_string_and_binary_query_key_condition_options(self):
        """
        Check the result of Query for each "BEGINS_WITH" and "BETWEEN" comparison operation when ScanIndexForward is
        True (The resulting order is ascending).

        For each HASH key generator all combinations of all digits (0-9) and chars (a-z and A-Z).
        """
        self._check_string_query_key_conditions_options()

    def test_check_string_and_binary_query_key_condition_options_without_scan_index_forward(self):
        """
        Check the result of Query for each "BEGINS_WITH" and "BETWEEN" comparison operation when ScanIndexForward is
        True (The resulting order is descending).

        For each HASH key generator all combinations of all digits (0-9) and chars (a-z and A-Z).
        """
        self._check_string_query_key_conditions_options(scan_index_forward=False)

    def test_cluster_traces(self):  # pylint:disable=too-many-locals,too-many-statements  # noqa: PLR0915
        """
        The test inserts items of different types and checks the traces for each of the following actions: "PutItem",
         "GetItem", "UpdateItem", and "DeleteItem".
        Also, for each action taken, the test checks for the following:
         1. For each action, the test checks a list of trace messages that should exist.
         2. The order of the traces should be according to the order of the expected traces variable (In other words,
          the test checks for the first message, and once is found, the test continue to find for the next message.
          Until the test found all the messages in that order).
         3. Some of the traces are supposed to appear under each node.
        """
        table_name = TABLE_NAME
        num_of_items = 10
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        nodes = self.cluster.nodelist()
        self.create_table(node=nodes[0], table_name=table_name)
        data_generator = AlternatorDataGenerator(primary_key=self._table_primary_key, primary_key_format=self._table_primary_key_format)
        items = data_generator.create_multiple_items(num_of_items=num_of_items, mode=TypeMode.MIXED)
        # This dictionary contains the "traces" messages for each method. Also, the order of messages is important!
        # During the test, we expect to find the messages in the order they are displayed in the list.
        # For example: For the "put_item" method, the test expects to see first the "PutItem" message.
        # The next message should be the "accept_proposal: send accept proposal" message.
        # Finally, the test expects to see "CAS successful" message (if one of those messages does not appear in
        #  this order, the test will fail).
        expected_messages_dict = {
            # Last thing, the numbers that appear next to a message indicate the number of different messages we are
            #  looking for. That is, if the number is not 1, we expect to see the message from several different nodes.
            "PutItem": [("PutItem", 1), ("prepare_ballot: sending prepare", len(nodes) - 1), ("accept_proposal: send accept proposal", len(nodes) - 1), ("CAS successful", 1)],
            "GetItem": [("GetItem", 1), ("Creating read executor for token", 1), ("Querying is done", 1)],
            "UpdateItem": [("UpdateItem", 1), ("accept_proposal: send accept proposal", len(nodes) - 1), ("prune: send prune of", len(nodes) - 1), ("CAS successful", 1)],
            "DeleteItem": [("DeleteItem", 1), ("accept_proposal: send accept proposal", len(nodes) - 1), ("prune: send prune of", len(nodes) - 1), ("CAS successful", 1)],
        }
        method_name_by_method_idx = {method_idx: method_name for method_idx, method_name in enumerate(expected_messages_dict)}
        expected_traces_number = len(expected_messages_dict)

        set_trace_probability(nodes=nodes, probability_value=1.0)
        for item_idx, item in enumerate(items):
            node = nodes[item_idx % len(nodes)]
            item_key = {self._table_primary_key: item[self._table_primary_key]}
            for method_name in expected_messages_dict:
                if method_name == "PutItem":
                    self.put_item(node=node, item=item, table_name=table_name)
                elif method_name == "GetItem":
                    self.get_item(node=node, item_key=item_key, table_name=table_name, consistent_read=True)
                elif method_name == "UpdateItem":
                    self.update_item(node=node, item_key=item_key, table_name=table_name)
                elif method_name == "DeleteItem":
                    self.delete_item(node=node, item_key=item_key, table_name=table_name)
                else:
                    raise KeyError(f'The following "{method_name}" method name not supported!')

        set_trace_probability(nodes=nodes, probability_value=0.0)

        # Retrying is needed because:
        # 1) the traces are not flushed immediately, but every 2s at most,
        # 2) hinted handoff activates every 10s at most, so if writes (CL=ANY)
        #    to system_traces' replicas are lost, the traces may appear with a
        #    0-10s delay.
        #
        # 10 attempts with 1s sleep time were not enough. See scylladb#25491.
        @retrying(num_attempts=20, sleep_time=1, allowed_exceptions=(AssertionError,))
        def try_checking_all_events():
            all_traces_events = self.get_all_traces_events_sorted()
            node_ids = {node.hostid() for node in nodes}
            # The following action order for each item is: "PutItem", "GetItem", "UpdateItem", and "DeleteIte" (this
            #  order is order of "expected_messages_dict" keys).
            # Therefore, for each action, we need to get a list with "len(items)" cells.
            events_by_action = {}
            events_idx = 0
            for tracing_session, events in all_traces_events:
                method_name = method_name_by_method_idx[events_idx % expected_traces_number]
                expected_request = f"Alternator {method_name}"
                # Ignore sessions which were not initiated by the tested operations.
                if expected_request != tracing_session["request"]:
                    logger.debug(f"Expected request: {expected_request}. Got: {tracing_session['request']}. Skipping.")
                    continue
                events_by_action.setdefault(method_name, []).append(events)
                events_idx += 1

            def verify_traces_messages(method_name):  # pylint:disable=too-many-locals
                all_traces = events_by_action.get(method_name)
                expected_messages = expected_messages_dict.get(method_name)
                assert all_traces, f"traces for {method_name} wasn't found"
                assert expected_messages, f"expected_messages for {method_name} wasn't found"

                # The "all_traces" variable contains the list of traces in the order of action ("get_item" or "pu_item")
                #  we did. The test enters multiple items. Thus, need over on the traces for each item entered.
                for action_idx, traces in enumerate(all_traces):
                    source = nodes[action_idx % len(nodes)].hostid()
                    trace_idx = 0
                    # This loop goes over the messages that should appear within the traces in the order of the
                    # "expected_messages".
                    for expected_message_details in expected_messages:
                        result = []
                        expected_message, expected_message_number = expected_message_details
                        count = expected_message_number
                        is_message_found = False
                        # This loop goes through all the traces once and tries to find the expected messages.
                        # Therefore, if the message not found or the while loop ends, this is means that a test failed
                        #  because the messages order was incorrect or the expected message changed.
                        while trace_idx < len(traces):
                            trace = traces[trace_idx]
                            trace_idx += 1
                            event_msg = trace["activity"]
                            if expected_message in event_msg and source == next(x for x in nodes if x.address() == trace["source"]).hostid():
                                if expected_message != event_msg:
                                    result.append(event_msg)
                                count -= 1
                                if count == 0:
                                    is_message_found = True
                                    break
                        assert is_message_found, f'The following "{expected_message}" trace message not found from "{source}" source!'
                        if result:
                            if method_name in ["PutItem", "UpdateItem", "DeleteItem"]:
                                ids = {msg.rsplit(" ", maxsplit=1)[1] for msg in result}
                                _diff = node_ids ^ ids
                                assert _diff == {source}, f'The "{expected_message}" message not sent from "{expected_message_number}" nodes (The message in missing in the following "{_diff}" IPs'
                            elif method_name == "GetItem":
                                for msg in result:
                                    ids = set(node_id.strip() for node_id in msg.split("[", maxsplit=1)[1].split("]", maxsplit=1)[0].split(","))
                                    _diff = node_ids ^ ids
                                    assert not _diff, f'The following node ids "{_diff}" not found in "{method_name}" event'
                            else:
                                raise AssertionError(f'The following "{method_name}" method name not supported!')

            for method_name in expected_messages_dict:
                logger.info(f'Verifying all traces of "{method_name}" method name')
                verify_traces_messages(method_name=method_name)

        try_checking_all_events()

    @pytest.mark.single_node
    @pytest.mark.parametrize("value", ["1", [1], {1}, Decimal(1)], ids=["str_value", "list_value", "set_value", "decimal_value"])
    def test_exception_error_for_update_item_with_add_action(self, value):
        """
        The test checks that the item cannot update with another item with a different variable type.
        The test verifies that "ValidationException" error has been received for each of ADD types.
        """
        # The test is negative, and it examines negative cases. Therefore, the variable type of "value" should be
        # removed (two items with the same type aren't negative case).
        valid_add_types = {str, Decimal, set, list} - {type(value)}
        item = {
            self._table_primary_key: "test_1",
            "value": value,
        }
        invalid_add_operations = [
            {
                self._table_primary_key: item[self._table_primary_key],
                "value": var_type([1]) if var_type in (set, list) else var_type(1),
            }
            for var_type in valid_add_types
        ]
        table_name = TABLE_NAME

        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]
        self.create_table(table_name=table_name, node=node1)

        logger.info("Inserting '%s' item", str(item))
        self.batch_write_actions(table_name=table_name, node=node1, new_items=[item])

        for invalid_add_operation in invalid_add_operations:
            logger.info("Adding invalid item with '%s' type to '%s' type", type(invalid_add_operation), type(item))
            with pytest.raises(ClientError, match=r"ValidationException.*[Oo]perand type"):
                self.update_items(table_name=table_name, node=node1, items=[invalid_add_operation], primary_key=self._table_primary_key, action="ADD")

    @pytest.mark.single_node
    @pytest.mark.parametrize(
        "value,isolation",
        [
            (value, isolation)
            for value in ("[1]", "{1}", "1")
            # not testing WriteIsolation.UNSAFE_RMW, can be used for debuuging
            for isolation in (WriteIsolation.ALWAYS_USE_LWT,)
        ],
    )
    def test_update_items_from_multiple_threads(self, value: str, isolation: WriteIsolation):
        """
        Add multiple values to same item from multiple threads and verify the result:
         * Number  - The result should contain the sum of the values from all items added.
         * Set - The result should contain the set of the values from all items added without duplicates.
         * List - The result should contain the list of the values from all items added with duplicates.
        """
        value = literal_eval(value)
        num_of_items = 10000 if self.cluster.scylla_mode != "debug" else 3000
        num_of_threads = 3
        var_type = type(value)
        item = {
            self._table_primary_key: "test_1",
            "value": value,
        }
        is_value_iter = var_type in (set, list)
        add_operations = [
            {
                self._table_primary_key: item[self._table_primary_key],
                "value": var_type([idx]) if is_value_iter else var_type(idx),
            }
            for idx in range(num_of_items)
        ]
        expected_item = {
            self._table_primary_key: "test_1",
            "value": Decimal(sum(sum(add_operation["value"] if is_value_iter else [add_operation["value"]]) for add_operation in add_operations)),
        }
        if not isinstance(value, set):
            expected_item["value"] += value[0] if is_value_iter else value
        table_name = TABLE_NAME

        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]
        table = self.create_table(table_name=table_name, node=node1)
        set_write_isolation(table=table, isolation=isolation)

        logger.info("Inserting '%s' item", str(item))
        self.batch_write_actions(table_name=table_name, node=node1, new_items=[item])

        logger.info("Executing '%d' add operations in parallel from '%d' threads", len(add_operations), num_of_threads)
        with ThreadPoolExecutor(max_workers=num_of_threads) as pool:
            threads = []
            chunk_size = num_of_items // num_of_threads
            for chunk_idx in range(0, len(add_operations), chunk_size):
                threads.append(pool.submit(self.update_items, table_name=table_name, node=node1, items=add_operations[chunk_idx : chunk_idx + chunk_size], primary_key=self._table_primary_key, action="ADD"))
            timeout = self.cql_timeout(180)
            logger.info(f"Waiting {timeout} seconds until all threads will finish")
            for thread in threads:
                thread.result(timeout=timeout)

        result = self.scan_table(table_name=table_name, node=node1)
        assert len(result) == 1
        # If the result is a list or set, one needs to go through it to summarize all the values because the insert
        # was in parallel, from several different threads, it is not possible to check the order of the result.
        if is_value_iter:
            result[0]["value"] = sum(result[0]["value"])

        assert result[0]["value"] == expected_item["value"], "The value of the result is not equal to the sum of the values added through the threads"

    def test_alternator_enforce_authorization(self, request: pytest.FixtureRequest):
        """
        Test that Alternator enforces authorization when the appropriate setting is enabled.
        Relevant role is created on one node and then used on another node to create a table.
        Test also negative case when request with wrong salted_hash is rejected.
        """
        num_of_nodes = self._num_of_nodes_for_test(rf=3)

        self.prepare_dynamodb_cluster(
            num_of_nodes=num_of_nodes,
            extra_config={
                "alternator_enforce_authorization": True,
                "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
                "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
            },
        )
        node1 = self.cluster.nodelist()[0]

        # initial failure case without any roles
        with pytest.raises(ClientError, match="UnrecognizedClientException"):
            self.create_table(table_name=TABLE_NAME, node=node1)

        # create role, and extract it's salted_hash
        # driver doesn't wait for all the nodes to know it is done - use 'exclusive' connection to guarantee fetching from the same node
        with self.patient_exclusive_cql_connection(node1, user="cassandra", password="cassandra") as cql_session:
            cql_session.execute("CREATE ROLE alternator WITH PASSWORD = 'password' AND login = true AND superuser = true")
            for ks in ("system", "system_auth_v2", "system_auth"):
                try:
                    response = cql_session.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role=%s;", ("alternator",)).one()
                except InvalidRequest:
                    continue
                if response:
                    self.salted_hash = response.salted_hash
                    break

        # since this test is change a class attribute, it should be reverted
        request.addfinalizer(lambda: setattr(self, "salted_hash", "None"))

        # clear url/api dicts, so they can be recreated with the updated salted_hash
        self.alternator_urls = {}
        self.alternator_apis = {}

        # check that credentials are eventually propagated to other node
        other_node = self.cluster.nodelist()[-1]

        def retry_create_table(end_time, sleep_interval):
            while time.time() < end_time:
                try:
                    self.create_table(table_name=TABLE_NAME, node=other_node)
                    return
                except ClientError as e:
                    if "UnrecognizedClientException" in str(e):
                        time.sleep(sleep_interval)
                    else:
                        raise
            self.create_table(table_name=TABLE_NAME, node=other_node)

        retry_create_table(end_time=time.time() + 1, sleep_interval=0.01)

        # failure case with wrong password salted hash
        self.salted_hash = "None"

        # clear url/api dicts, so they can be recreated with the updated salted_hash
        self.alternator_urls = {}
        self.alternator_apis = {}

        with pytest.raises(ClientError, match="UnrecognizedClientException"):
            self.create_table(table_name="table2", node=other_node)


class TtlNotExpiredError(Exception):
    pass


class TestAlternatorBackwardsCompatibility(BaseAlternator, UpgradeTester):
    """
    A class for testing Alternator backwards compatibility.
    """

    __test__ = True
    upgrade_path = upgrade_matrix_for_alternator
    init_version = upgrade_path[0]

    @staticmethod
    def _sort_by_attributes(item_list: list[dict[str, Any]], attrs: list[str] | None = None):
        """
        Sort the item_list according to a key function that maps each item to
        (item[attrs[0]], item[attrs[1]], ...).
        """
        if attrs is None:
            attrs = [schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME]
        item_list.sort(key=lambda item: tuple([item[attr] for attr in attrs]))

    def test_lsi_gsi_regular_column_upgrades(self, dtest_config):
        """
        Test that a table with a GSI and an LSI created on an older version of
        Scylla works correctly. <2025.4 Scylla makes a top-level regular column
        for the LSI keys, and <2025.2 Scylla does this also for GSI keys which
        are not part of the base table's partition key. Newer versions store
        them in :attrs.
        """
        self.clone_upgrade_path(dtest_config)
        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node = self.cluster.nodelist()[0]

        table = self.create_table(node, schema=schemas.HASH_AND_STR_RANGE_GSI_LSI_SCHEMA)
        items = [
            {
                schemas.HASH_KEY_NAME: f"pk{i}",
                schemas.RANGE_KEY_NAME: f"ck{i}",
                schemas.LSI_KEY_NAME: f"lsi{i}",
                schemas.GSI_KEY_NAME: f"gsi{i}",
                "data": f"data{i}",
            }
            for i in range(len(self.current_upgrade_path))
        ]
        self._sort_by_attributes(items)
        self.batch_write_actions(TABLE_NAME, node, new_items=items, schema=schemas.HASH_AND_STR_RANGE_GSI_LSI_SCHEMA)

        for i, version in enumerate(self.current_upgrade_path):
            pk = f"pk{i}"
            ck = f"ck{i}"
            empty_item = {schemas.HASH_KEY_NAME: pk, schemas.RANGE_KEY_NAME: ck}
            new_data = f"newdata{i}"
            new_data_lsi = f"{new_data}_lsi"
            new_data_gsi = f"{new_data}_gsi"

            # Upgrade the node.
            logger.info(f"****** START UPGRADE TEST FROM {node.node_scylla_version} TO {version} ******")
            node.upgrade(upgrade_to_version=version)
            logger.info(f"Node {node.name} has been upgraded. Current version is {node.node_scylla_version}")

            # Replace the item with the one with only HASH and RANGE keys to
            # clear the LSI attribute.
            assert self.get_item(node, item_key=empty_item) == items[i]
            self.put_item(node, item=empty_item)
            assert self.get_item(node, item_key=empty_item) == empty_item
            # The item should be gone from the LSI and GSI.
            assert full_query(table, IndexName=schemas.LSI_INDEX_NAME, KeyConditionExpression=f"{schemas.HASH_KEY_NAME} = :p", ExpressionAttributeValues={":p": f"pk{i}"}) == []
            # GSIs don't allow ConsistentRead, and GSI updates are asynchronous anyway.
            assert full_query(table, consistent_read=False, IndexName=schemas.GSI_INDEX_NAME, KeyConditionExpression=f"{schemas.GSI_KEY_NAME} = :p", ExpressionAttributeValues={":p": f"gsi{i}"}) == []

            # Set new LSI, GSI and non-key attributes.
            self.update_item(
                node=node,
                item_key=empty_item,
                UpdateExpression=f"SET data = :val, {schemas.LSI_KEY_NAME} = :lsi, {schemas.GSI_KEY_NAME} = :gsi",
                ExpressionAttributeValues={":val": new_data, ":lsi": new_data_lsi, ":gsi": new_data_gsi},
            )
            items[i] = {**empty_item, "data": new_data, schemas.LSI_KEY_NAME: new_data_lsi, schemas.GSI_KEY_NAME: new_data_gsi}

            # Check that no content of the table has changed because of the
            # node upgrade and that the modifications of items[i] didn't affect
            # the other items.
            table_content = self.scan_table(TABLE_NAME, node)
            self._sort_by_attributes(table_content)
            diff = DeepDiff(t1=items, t2=table_content)
            assert not diff, f"The following items differ:\n{pformat(diff)}"

            logger.info("****** FINISHED UPGRADE TEST TO %s ******", version)
