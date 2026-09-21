import json
import logging
import os
import re
from time import sleep
from typing import Any

import pytest
import yaml
from cassandra import InvalidRequest, WriteFailure
from cassandra.cluster import Session
from ccmlib.cluster import Cluster
from ccmlib.node import Node

from dtest_class import Tester, create_cf, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.data import create_c1c2_table, insert_c1c2
from tools.marks import unmark, with_feature

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class SystemTableBase(Tester):
    KEYSPACE_NAME = "system"

    def prepare_cluster(self, nodes: list[int] | int | dict, options: dict[str, str | bool | int] | None = None, jvm_args: list[str] | None = None) -> Cluster:
        logger.debug("Preparing the cluster...")
        cluster = self.cluster
        if options:
            cluster.set_configuration_options(values=options)
        cluster.populate(nodes).start(jvm_args=jvm_args)
        logger.debug("Cluster has been prepared...")
        return cluster

    @staticmethod
    def run_query_on_node(session: Session, query: str) -> list:
        logger.debug('Running query "%s"...', query)
        return session.execute(query).current_rows

    @staticmethod
    def create_tables_with_data(  # noqa: PLR0913
        session: Session,
        keyspace_name: str,
        replication_factor: dict[str, int] | int,
        table_name_prefix: str,
        number_of_tables: int,
        number_of_rows: int,
    ) -> list[str]:
        logger.info("Creating a new keyspace '%s'...", keyspace_name)
        create_ks(session=session, name=keyspace_name, rf=replication_factor)

        tables = []

        for table_index in range(number_of_tables):
            table_name = f"{table_name_prefix}_{table_index + 1}"
            full_table_name = f"{keyspace_name}.{table_name}"
            logger.info("Creating a new table '%s'...", full_table_name)
            create_c1c2_table(session=session, cf=full_table_name)

            logger.info("Populating %s with data...", full_table_name)
            insert_c1c2(session=session, n=number_of_rows, ks=keyspace_name, cf=table_name)
            tables.append(full_table_name)
            logger.info("Table '%s' has been created and populated...", full_table_name)

        return tables

    @staticmethod
    def is_number(value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False


@pytest.mark.dtest_full
class TestClusterStatusTable(SystemTableBase):
    TABLE_NAME = "cluster_status"
    SELECT_QUERY = f"select * from {SystemTableBase.KEYSPACE_NAME}.{TABLE_NAME};"

    @staticmethod
    def parse_query_output(query_output) -> dict[str, dict[str, Any]]:
        node_data = {}
        for row in query_output:
            node_data[row.peer] = {
                "dc": row.dc,
                "host_id": row.host_id,
                "owns": row.owns,
                "status": row.status,
                "up": row.up,
            }
        return node_data

    def check_running_node_status(self, node_status: dict[str, Any], node_to_check: Node):
        node_ip_address = node_to_check.address()
        node_dc = node_to_check.get_datacenter_name()
        node_id = node_to_check.hostid()
        logger.info("Checking if the node %s has correct status in %s.%s...", node_ip_address, self.KEYSPACE_NAME, self.TABLE_NAME)
        assert node_status["dc"] == node_dc, f"Expected to get 'dc={node_dc}' for node {node_ip_address}, but didn't get it!"
        assert str(node_status["host_id"]) == node_id, f"Expected to get 'id={node_id}' for node {node_ip_address}, but didn't get it!"
        assert node_status["status"].upper() == "NORMAL", f"Expected to get 'status=NORMAL' for node {node_ip_address}, but didn't get it!"
        assert node_status["up"], f"Expected to see node {node_ip_address} in running state, but it seems to be down!"

    def test_content_all_nodes_are_up(self):
        """
        The cluster_status table content when all nodes are up

         peer       | dc          | host_id                              | load        | owns     | status | tokens | up
        ------------+-------------+--------------------------------------+-------------+----------+--------+--------+------
         172.17.0.3 | datacenter1 | ff5361b6-d190-4582-a424-64dc4e2f768d |      576966 | 0.468747 | NORMAL |    256 | True
         172.17.0.2 | datacenter1 | d2191d83-1158-4215-999b-363500703235 | 1.05503e+06 | 0.531253 | NORMAL |    256 | True

        The test scenario
        1. Create the cluster of 3 nodes
        2. Check the table content for all 3 nodes.
        """

        cluster = self.prepare_cluster(nodes=3)
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        for node in cluster.nodelist():
            node_ip_address = node.address()
            assert parsed_query_result.get(node_ip_address), f"There should the row for the node {node_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it wasn't found!"
            self.check_running_node_status(node_status=parsed_query_result[node_ip_address], node_to_check=node)

    def test_content_add_new_node(self):
        """
        The test scenario
        1. Create the cluster of 1 node
        2. Add a new node to the cluster
        3. Check table content: the new row with the information about the new node should be inserted into the table
        4. Check the sum of "owns" values is equal to 1 (since there are no special RF settings).
        5. Check the table content for the new node.
        """
        cluster = self.prepare_cluster(nodes=1)
        node1 = cluster.nodelist()[0]

        logger.debug("Adding a new node to the cluster...")
        node2 = cluster.new_node(2, auto_bootstrap=True, add_node=True)
        node2.start()
        node2_ip_address = node2.address()
        logger.info("The new node %s has been started.", node2_ip_address)

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert len(parsed_query_result) == len(cluster.nodes), "The table has wrong number of rows!"

        assert parsed_query_result.get(node2_ip_address), f"There should the row for the node {node2_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it wasn't found!"

        owns = round(sum([row["owns"] for row in parsed_query_result.values()]), 4)

        assert owns == 1, "The sum of values in the 'owns' column should equal 1"

        self.check_running_node_status(node_status=parsed_query_result[node2_ip_address], node_to_check=node2)

    def test_content_stop_node(self):
        """
        The cluster_status table content when one node is down
         peer       | dc          | host_id                              | load        | owns     | status   | tokens | up
        ------------+-------------+--------------------------------------+-------------+----------+----------+--------+-------
         172.17.0.3 | datacenter1 | ff5361b6-d190-4582-a424-64dc4e2f768d |      588499 | 0.309867 | shutdown |    256 | False
         172.17.0.2 | datacenter1 | d2191d83-1158-4215-999b-363500703235 | 1.06626e+06 | 0.368409 |   NORMAL |    256 |  True
         172.17.0.4 | datacenter1 | 9c51fae6-8a9b-41a3-910f-4afb5e3489db |      934981 | 0.321724 |   NORMAL |    256 |  True

        The test scenario
        1. Create the cluster of 2 nodes
        2. Stop the 2nd node
        3. Check state and status of the stopped node in the table.
        """

        cluster = self.prepare_cluster(nodes=2)

        node1, node2 = cluster.nodelist()
        node2_ip_address = node2.address()

        logger.debug("Stopping the node %s...", node2_ip_address)
        node2.stop()
        logger.info("The node %s has been stopped...", node2_ip_address)

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert not parsed_query_result[node2_ip_address]["up"], f"The node {node2_ip_address} is down, but in {self.KEYSPACE_NAME}.{self.TABLE_NAME} it has 'up = True'."

        assert parsed_query_result[node2_ip_address]["status"].upper() == "SHUTDOWN", f"Wrong status of node {node2_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME} table!"

    def test_content_remove_node(self):
        """
        The cluster_status table content when one node is removed
         peer       | dc          | host_id                              | load        | owns     | status  | tokens | up
        ------------+-------------+--------------------------------------+-------------+----------+---------+--------+-------
         172.17.0.3 |        null |                                 null |      588499 |     null | removed |      0 | False
         172.17.0.2 | datacenter1 | d2191d83-1158-4215-999b-363500703235 | 1.06626e+06 | 0.529747 |  NORMAL |    256 |  True
         172.17.0.4 | datacenter1 | 9c51fae6-8a9b-41a3-910f-4afb5e3489db |      934981 | 0.470253 |  NORMAL |    256 |  True

        The test scenario
        1. Create the cluster of 3 nodes
        2. Remove the 3nd node
        3. Check state and status of the removed node in the table.
        """

        cluster = self.prepare_cluster(nodes=3)

        node1, node2, _node3 = cluster.nodelist()
        node2_ip_address = node2.address()
        node2_host_id = node2.hostid()

        logger.debug("Stopping %s", node2.name)
        node2.stop(gently=False, wait_other_notice=True)

        logger.debug("Removing the node %s from the cluster...", node2_ip_address)
        node1.removenode(hid=node2_host_id)
        logger.info("The node %s has been removed from the cluster...", node2_ip_address)

        sleep(10)

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        if "consistent-topology-changes" not in self.scylla_features:
            assert not parsed_query_result[node2_ip_address]["up"], f"The node {node2_ip_address} was removed, but in {self.KEYSPACE_NAME}.{self.TABLE_NAME} it has 'up = True'."
            assert parsed_query_result[node2_ip_address]["status"].upper() == "REMOVED", f"Wrong status of node {node2_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME} table!"
        else:
            assert node2_ip_address not in parsed_query_result.keys(), f"Node {node2_ip_address} still listed in {self.KEYSPACE_NAME}.{self.TABLE_NAME} table! But should be immediately removed in raft-topology mode"

    def test_content_multi_dc_all_nodes_are_up(self):
        """
        The cluster_status table content when all nodes are up in multi-dc cluster
         peer       | dc  | host_id                              | load   | owns     | status | tokens | up
        ------------+-----+--------------------------------------+--------+----------+--------+--------+------
         127.0.75.1 | dc1 | 7b13cb68-68ae-4dde-bfdb-96a637616cda | 111808 | 0.339353 | NORMAL |    256 | True
         127.0.75.2 | dc2 | 877f2731-ed8a-49c8-a377-32a790c0dd6b | 149536 | 0.309136 | NORMAL |    256 | True
         127.0.75.3 | dc2 | c6106ead-b8a1-40b4-b20d-9f5c5cdd1e54 | 215766 | 0.351511 | NORMAL |    256 | True

        The test scenario
        1. Create the multi-dc cluster of 2 nodes (one node in each dc)
        2. Check table content for both nodes.
        3. Add a new node to the cluster in the datacenter dc2
        4. Check table content: the new row should be inserted into the table
        5. Check the sum of "owns" values is equal to 1.
        6. Check the table content for the new node.
        """

        cluster = self.prepare_cluster(nodes=[1, 1])
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

            for node in cluster.nodelist():
                node_ip_address = node.address()
                assert parsed_query_result.get(node_ip_address), f"There should the row for the node {node_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it wasn't found!"
                self.check_running_node_status(node_status=parsed_query_result[node_ip_address], node_to_check=node)

            logger.debug("Adding a new node to the cluster...")
            node3 = cluster.new_node(3, auto_bootstrap=True, add_node=True, data_center="dc2")
            node3.start()
            node3_ip_address = node3.address()
            logger.info("The new node %s has been started.", node3_ip_address)

            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert parsed_query_result.get(node3_ip_address), f"There should the row for the node {node3_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it wasn't found!"

        owns = round(sum([row["owns"] for row in parsed_query_result.values()]), 4)

        assert owns == 1, "The sum of values in the 'owns' column should equal 1"

        self.check_running_node_status(node_status=parsed_query_result[node3_ip_address], node_to_check=node3)


@pytest.mark.dtest_full
class TestTokenRingTable(SystemTableBase):
    """
    Example of the table content
    -----------------------------
     keyspace_name | start_token          | endpoint   | dc          | end_token            | rack
    ---------------+----------------------+------------+-------------+----------------------+-------
     test_keyspace | -1028636990904053927 | 172.17.0.2 | datacenter1 |  -840980620915277404 | rack1
     test_keyspace | -1059882504016989347 | 172.17.0.2 | datacenter1 | -1028636990904053927 | rack1
     test_keyspace | -1083305362326820612 | 172.17.0.2 | datacenter1 | -1059882504016989347 | rack1
     test_keyspace | -1083467855096097310 | 172.17.0.2 | datacenter1 | -1083305362326820612 | rack1

    """

    TABLE_NAME = "token_ring"
    TEST_KEYSPACE = "test_keyspace"
    TEST_TABLE = "test_table"

    def create_test_keyspace(self, session: Session, replication_factor: dict[str, int] | int, create_table: bool = False):
        logger.debug("Creating a new keyspace '%s'...", self.TEST_KEYSPACE)
        create_ks(session=session, name=self.TEST_KEYSPACE, rf=replication_factor)
        logger.info("New keyspace '%s' has been created", self.TEST_KEYSPACE)
        if create_table:
            create_cf(session=session, name=f"{self.TEST_KEYSPACE}.{self.TEST_TABLE}")

    def table_name_restriction(self, session: Session):
        """Return an equal restriction on `table_name` if the `token_ring` table has this clustering column.

        It will return `AND table_name = '<ALL>'` if `token_ring` has the new `tabl_name` clustering column.
        This snippet can then be injected into queries to make them work with both the old and new schema.
        """
        res = session.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'system' and table_name = 'token_ring' and column_name = 'table_name'")
        if bool(list(res)):
            if "tablets" in self.scylla_features:
                return f"AND table_name = '{self.TEST_TABLE}'"
            else:
                return "AND table_name = '<ALL>'"
        else:
            return ""

    @pytest.mark.single_node
    def test_content_one_node_create_and_drop_keyspace(self):
        """
        The test scenario
        1. Create 1 node
        2. Create a keyspace.
        3. Check there are rows inserted into the table for this keyspace.
        4. Drop the keyspace
        5. Check there are no rows for dropped keyspace in the table.
        """
        cluster = self.prepare_cluster(nodes=1)
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            # tablets require a table to be created as well as the keyspace
            # for the keyspace to be listed in system.token_ring
            create_table = "tablets" in self.scylla_features
            self.create_test_keyspace(session=session, replication_factor=1, create_table=create_table)

            select_query = f"select distinct keyspace_name from {self.KEYSPACE_NAME}.{self.TABLE_NAME};"
            query_result = self.run_query_on_node(session=session, query=select_query)

            found_keyspaces = [row.keyspace_name for row in query_result]
            logger.info("Checking the records for keyspace '%s' in table %s.%s...", self.TEST_KEYSPACE, self.KEYSPACE_NAME, self.TABLE_NAME)
            assert self.TEST_KEYSPACE in found_keyspaces, f"Token ranges for the keyspace '{self.TEST_KEYSPACE}' weren't found in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}!"

            logger.debug("Dropping keyspace '%s'...", self.TEST_KEYSPACE)
            query_to_run = f"drop keyspace {self.TEST_KEYSPACE};"
            self.run_query_on_node(session=session, query=query_to_run)
            logger.debug("Keyspace '%s' has been dropped", self.TEST_KEYSPACE)

            query_result = self.run_query_on_node(session=session, query=select_query)

        found_keyspaces = [row.keyspace_name for row in query_result]
        logger.info("Checking the records for keyspace '%s' in table %s.%s...", self.TEST_KEYSPACE, self.KEYSPACE_NAME, self.TABLE_NAME)
        assert self.TEST_KEYSPACE not in found_keyspaces, f"Found token ranges for the non-existing keyspace '{self.TEST_KEYSPACE}' in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}!"

    # tablets require enough nodes to satisfy RF
    @pytest.mark.required_features("!tablets")
    def test_content_increasing_cluster(self):
        """
        When a new node is added to the cluster it is assigned a range of tokens for the keyspace, if the total number
        of nodes in the cluster is equal (or lower) the replication factor of that keyspace.
        If it happens the IP address of the node should appear in 'endpoint' column of the 'system.token_ring' table.

        The test scenario:
        1. Create a cluster of 2 nodes
        2. Create a keyspace with RF=3
        3. Select one token range and check that it is assigned to 2 nodes
        4. Add the 3rd node and check that selected token range is assigned to 3 nodes.
        5. Add the 4th node and check that selected token range is still assigned to 3 nodes.
        """
        cluster = self.prepare_cluster(nodes=2)
        node1 = cluster.nodelist()[0]
        replication_factor = 3

        with self.patient_cql_connection(node1) as session:
            self.create_test_keyspace(session=session, replication_factor=replication_factor)

            logger.debug("Getting one token range to check...")
            query_to_run = f"select start_token from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where keyspace_name = '{self.TEST_KEYSPACE}' limit 1;"
            query_result = self.run_query_on_node(session=session, query=query_to_run)
            start_token = query_result[0].start_token

            query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where keyspace_name='{self.TEST_KEYSPACE}' {self.table_name_restriction(session)} and start_token='{start_token}';"

            logger.info("Checking the number assigned nodes for selected token range...")
            assert len(self.run_query_on_node(session=session, query=query_to_run)) == len(cluster.nodes), f"The token range starting from {start_token} should be assigned to {len(cluster.nodes)} nodes."

            for node_index in [3, 4]:
                logger.debug("Adding the %drd node to the cluster...", node_index)
                cluster.new_node(node_index, auto_bootstrap=True, add_node=True).start()
                logger.info("The %drd node has been added to the cluster.", node_index)

                logger.info("Checking the number assigned nodes for selected token range...")
                assert len(self.run_query_on_node(session=session, query=query_to_run)) == replication_factor, f"The token range starting from {start_token} should be assigned to {replication_factor} nodes."

    def test_content_multi_dc_cluster(self):
        """
        Test scenario
        1. Create a multi DC cluster of 4 nodes (2 DCs with 2 nodes each)
        2. Create a keyspace with RF={DC1: 2, DC2: 1}
        3. Select one token range and check it is assigned to 3 nodes in different DCs
        """
        cluster = self.prepare_cluster(nodes={"dc1": {"rack1": 1, "rack2": 1}, "dc2": {"rack1": 2}})
        node1 = cluster.nodelist()[0]
        dc_name_1 = "dc1"
        dc_name_2 = "dc2"

        with self.patient_cql_connection(node1) as session:
            # tablets require a table to be created as well as the keyspace
            # for the keyspace to be listed in system.token_ring
            create_table = "tablets" in self.scylla_features
            self.create_test_keyspace(session=session, replication_factor={dc_name_1: 2, dc_name_2: 1}, create_table=create_table)

            logger.debug("Getting one token range to check...")
            query_to_run = f"select start_token from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where keyspace_name = '{self.TEST_KEYSPACE}' limit 1;"
            query_result = self.run_query_on_node(session=session, query=query_to_run)
            start_token = query_result[0].start_token

            query_to_run = f"select dc from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where keyspace_name = '{self.TEST_KEYSPACE}' {self.table_name_restriction(session)} and start_token = '{start_token}';"
            query_result = self.run_query_on_node(session=session, query=query_to_run)

        dc_list = [row.dc for row in query_result]
        logger.info("Checking the content of %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
        assert len(dc_list) == 3, f"The token range starting from {start_token} should be assigned to 3 nodes."
        assert dc_name_1 in dc_list and dc_name_2 in dc_list, f"The token range starting from {start_token} should be assigned to nodes from '{dc_name_1}' and '{dc_name_2}'."

    def test_content_match_on_all_nodes(self):
        """
        Test scenario:
        1. Create cluster of 3 nodes
        2. Create keyspace with RF=2
        3. Get the content of system.token_ring for created keyspace on the node1
        4. Compare it with the table content from all the other nodes.
        """

        cluster = self.prepare_cluster(nodes={"datacenter1": {"rack1": 1, "rack2": 2}})

        node1 = cluster.nodelist()[0]
        node1_ip_address = node1.address()

        query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where keyspace_name = '{self.TEST_KEYSPACE}'"
        with self.patient_exclusive_cql_connection(node1) as session:
            self.create_test_keyspace(session=session, replication_factor=2)

            logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node1_ip_address)
            test_ks_token_set = self.run_query_on_node(session=session, query=query_to_run)
        for node in cluster.nodelist()[1:]:
            node_ip_address = node.address()
            with self.patient_exclusive_cql_connection(node) as session:
                logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
                assert test_ks_token_set == self.run_query_on_node(session=session, query=query_to_run), f"The set of token ranges does not match on the nodes {node1_ip_address} and {node_ip_address}!"

    def test_content_stop_and_replace_node(self):
        """
        1. Create cluster of 3 nodes
        2. Create keyspace with RF=2
        3. Get the content of system.token_ring for created keyspace
        4. Select the token ranges only for node3
        5. Stop node3 and replace it with node4
        6. Get the token ranges from system.token_ring for created keyspace only for node4
        7. Compare token ranges for node 3 and 4
        """
        cluster = self.prepare_cluster(nodes={"datacenter1": {"rack1": 1, "rack2": 2}})
        node3 = cluster.nodelist()[2]
        node3_ip_address = node3.address()

        query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where keyspace_name = '{self.TEST_KEYSPACE}'"
        with self.patient_cql_connection(node3) as session:
            self.create_test_keyspace(session=session, replication_factor=2)

            logger.debug("Getting table content of %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
            test_ks_token_set = self.run_query_on_node(session=session, query=query_to_run)

        node3_token_set = [row.start_token for row in test_ks_token_set if row.endpoint == node3_ip_address]

        logger.info("Replacing node...")
        logger.debug("Stopping node %s...", node3_ip_address)
        node3.stop(wait_other_notice=True)
        logger.debug("Starting new node as replacement of %s...", node3_ip_address)
        node4 = cluster.new_node(4, auto_bootstrap=True, is_seed=False, add_node=True, data_center=str(node3.data_center), rack=str(node3.rack))
        node4.start(replace_node_host_id=node3.hostid())
        node4_ip_address = node4.address()

        with self.patient_exclusive_cql_connection(node4) as session:
            logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node4_ip_address)
            test_ks_token_set = self.run_query_on_node(session=session, query=query_to_run)

        node4_token_set = [row.start_token for row in test_ks_token_set if row.endpoint == node4_ip_address]
        assert node3_token_set == node4_token_set, "The token ranges before and after node replacement do not match!"


@pytest.mark.dtest_full
class TestVersionsTable(SystemTableBase):
    TABLE_NAME = "versions"

    @pytest.mark.single_node
    def test_content(self):
        """
        Table content example:
         key   | build_id                                 | build_mode | version
        -------+------------------------------------------+------------+------------------------------
        local | 20d9fa2c6020017f4afdc4941c0cca9c9a29d94a |    release | 5.0.rc1-0.20220206.891990ec0

        Test scenario:
        1. Create one-node cluster
        2. Run select query on system.versions table
        3. Verify the content of the table
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        with self.patient_cql_connection(node) as session:
            logger.info("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
            query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where key = 'local';"
            output = self.run_query_on_node(session=session, query=query_to_run)[0]

        scylla_build_id = node.scylla_build_id
        scylla_version = node.node_scylla_version
        scylla_mode = node.scylla_mode()

        logger.info("Verifying the content of the table %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
        assert output.key, "The 'key' attribute must not be empty!"
        assert output.build_id == scylla_build_id, f"The build ids do not match! Expected: {scylla_build_id} Got: {output.build_id}"
        assert output.build_mode == scylla_mode, f"The build modes do not match! Expected: {scylla_mode} Got: {output.build_mode}"
        assert output.version == scylla_version, f"The Scylla versions do not match! Expected: {scylla_version} Got: {output.version}"


@pytest.mark.dtest_full
class TestProtocolServersTable(SystemTableBase):
    """
    Table content example:
     name             | listen_addresses                        | protocol | protocol_version
    ------------------+-----------------------------------------+----------+------------------
    native transport  | ['172.17.0.2:9042', '172.17.0.2:19042'] |      cql |            3.3.1
           alternator |                                      [] | dynamodb |       2012-08-10
                redis |                                      [] |     RESP |              2.0
    """

    TABLE_NAME = "protocol_servers"

    @pytest.mark.parametrize(
        "mode,scylla_yaml_options,port",
        [
            pytest.param("default", None, None, id="default"),
            pytest.param("alternator", {"alternator_port": "8000", "alternator_write_isolation": "only_rmw_uses_lwt"}, "8000", id="alternator"),
            pytest.param("redis", {"redis_port": "6379"}, "6379", marks=pytest.mark.skip_if(with_feature("tablets")), id="redis"),
        ],
    )
    @pytest.mark.single_node
    def test_content(self, mode: str, scylla_yaml_options: dict[str, str] | None, port: str | None):
        """
        Test scenario:
        1. Create one-node cluster
        2. Run select query on system.protocol_servers table
        3. Verify the content of the table
        4. Repeat steps 1-3 for each mode:
            - default - no special features enabled
            - alternator - Alternator (DynamoDB API) enabled
        """
        cluster = self.prepare_cluster(nodes=1, options=scylla_yaml_options)
        node = cluster.nodelist()[0]
        node.watch_log_for("init - serving")
        node_ip_address = node.address()

        with self.patient_cql_connection(node) as session:
            logger.info("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            table_content_query = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};"
            table_content = self.run_query_on_node(session=session, query=table_content_query)

            logger.info("Getting CQL version from system.local...")
            protocols_query = "select cql_version from system.local"

            protocols = self.run_query_on_node(session=session, query=protocols_query)[0]

        expected_content = {
            "native transport": {"listen_addresses": [f"{node_ip_address}:9042", f"{node_ip_address}:19042"], "protocol": "cql", "protocol_version": protocols.cql_version},
            "alternator": {"listen_addresses": [f"{node_ip_address}:{port}"] if mode == "alternator" else [], "protocol": "dynamodb"},
            "redis": {"listen_addresses": [f"{node_ip_address}:{port}"] if mode == "redis" else [], "protocol": "RESP"},
        }

        logger.info("Verifying the content of the table %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
        for row in table_content:
            if row.name == "rpc":
                # ignore the row for thrift
                continue
            expected_row = expected_content.get(row.name)

            assert expected_row, f"Unexpected value in column 'name': {row.name}"
            assert expected_row["listen_addresses"] == row.listen_addresses, f"Unexpected value in column 'listen_addresses': {row.listen_addresses}"
            assert expected_row["protocol"] == row.protocol, f"Unexpected value in column 'protocol': {row.protocol}"

            if row.name in ["native transport", "rpc"]:
                assert expected_row["protocol_version"] == row.protocol_version, f"Unexpected value in column 'protocol': {row.protocol_version}"


@pytest.mark.dtest_full
class TestSnapshotsTable(SystemTableBase):
    """
    Table content example:
     keyspace_name | table_name | snapshot_name | live | total
    ---------------+------------+---------------+------+--------
             my_ks | test_table | 1649001070121 |    0 | 655360
    """

    TABLE_NAME = "snapshots"
    TEST_KEYSPACE = "test_keyspace"

    @pytest.mark.parametrize("mode,number_of_tables", [("one_table", 1), ("keyspace", 5)], ids=["one_table", "keyspace"])
    @pytest.mark.single_node
    def test_content_create_and_remove_snapshot(self, mode: str, number_of_tables: int):
        """
        Test scenario:
        1. Create one-node cluster
        2. Create a new keyspace and table(s) with data (1 table for 'one_table' mode and 5 tables for 'keyspace' mode).
        3. Create snapshot for the created table ('one_table' mode) or for entire keyspace ('keyspace' mode)
        using the 'nodetool snapshot' command.
        4. Select and verify the content of system.snapshots table.
        5. Remove the snapshot.
        6. Verify the table of system.snapshots is empty.
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        with self.patient_cql_connection(node) as session:
            logger.info("Creating a new keyspace '%s' and %d table(s) with data...", self.TEST_KEYSPACE, number_of_tables)
            tables = self.create_tables_with_data(session=session, keyspace_name=self.TEST_KEYSPACE, replication_factor=1, table_name_prefix="table", number_of_tables=number_of_tables, number_of_rows=10)

            logger.info("Creating a snapshot with 'nodetool snapshot' command...")
            out, _ = node.nodetool(cmd=f"snapshot {self.TEST_KEYSPACE if mode == 'keyspace' else tables[0]}")

            # Parsing of 'nodetool snapshot' output to get snapshot_name
            # The example of the output:
            # "Requested creating snapshot(s) for [test_keyspace.table_1] with snapshot name [1649077599088]
            # and options {skipFlush=false}
            # Snapshot directory: 1649077599088"
            snapshot_name = re.search(r"(with snapshot name \[)(\d+)(])", out).group(2)

            logger.info("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
            query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};"
            table_content = self.run_query_on_node(session=session, query=query_to_run)
            snapshot_tables = sorted([f"{row.keyspace_name}.{row.table_name}" for row in table_content])

            logger.info("Verifying the content of the table %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
            assert snapshot_tables == tables, f"Expected to get snapshot data for tables: {tables}, but {self.KEYSPACE_NAME}.{self.TABLE_NAME} contains data for {snapshot_tables}!"
            for row in table_content:
                assert row.snapshot_name == snapshot_name, f"Found unexpected snapshot name: {row.snapshot_name}!"
                assert isinstance(row.live, int | float), "Unexpected value type in 'live' column!"
                assert isinstance(row.total, int | float), "Unexpected value type in 'total' column!"

            logger.info("Removing a snapshot with 'nodetool clearsnapshot' command...")
            node.nodetool(cmd=f"clearsnapshot -t {snapshot_name}")

            logger.info("Verifying the table %s.%s is empty...", self.KEYSPACE_NAME, self.TABLE_NAME)
            content_after_removal = self.run_query_on_node(session=session, query=query_to_run)
            assert not content_after_removal, f"The table is supposed to be empty, but it contains the following data: {content_after_removal}!"

    def test_content_auto_snapshot(self):
        """
        Test scenario:
        1. Create cluster of 3 nodes with auto_snapshot = true
        2. Create a new keyspace with RF=3 and table with data.
        3. Create snapshot for the table by truncating the created table.
        4. Using the exclusive cql connection verify the created snapshot has been represented
        in the table on each node.
        """
        cluster_topology_3_nodes = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster = self.prepare_cluster(nodes=cluster_topology_3_nodes, options={"auto_snapshot": "true"})
        node = cluster.nodelist()[0]

        with self.patient_cql_connection(node) as session:
            logger.info("Creating a new keyspace '%s' and a table with data...", self.TEST_KEYSPACE)
            table = self.create_tables_with_data(session=session, keyspace_name=self.TEST_KEYSPACE, replication_factor=3, table_name_prefix="table", number_of_tables=1, number_of_rows=10)[0]

            logger.info("Truncating the table '%s'...", table)
            self.run_query_on_node(session=session, query=f"truncate table {table};")

        query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};"
        for node in cluster.nodelist():
            with self.patient_exclusive_cql_connection(node) as session:
                logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
                table_content = self.run_query_on_node(session=session, query=query_to_run)
            assert len(table_content) == 1, f"Expected to get 1 row from the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but got {len(table_content)} rows!"
            assert table == f"{table_content[0].keyspace_name}.{table_content[0].table_name}", f"Expected to get the snapshot information for the table {table}, but didn't get it!"


@pytest.mark.dtest_full
class TestRuntimeInfoTable(SystemTableBase):
    TABLE_NAME = "runtime_info"
    TEST_KEYSPACE = "test_keyspace"

    @pytest.mark.single_node
    def test_default_content(self):
        """
        Test scenario:
        1. Start one Scylla node (all parameters are default except predefined memory value)
        2. Run select query on system.runtime_info table
        3. Verify the content of the table
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        expected_content = {
            "cache": ["entries", "hit_rate_recent", "hit_rate_total", "hits", "memory_free", "memory_total", "memory_used", "misses", "requests_recent", "requests_total"],
            "memory": ["free", "total", "used"],
            "generic": ["gossip_active", "incremental_backup_enabled", "load", "trace_probability", "uptime"],
            "memtable": ["entries", "memory_free", "memory_total", "memory_used"],
        }

        # ensure uptime > 0
        sleep(1)

        with self.patient_cql_connection(node) as session:
            logger.info("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
            query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};"
            table_content = self.run_query_on_node(session=session, query=query_to_run)

        table_content_dict = {}

        logger.info("Verifying the content of %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
        for row in table_content:
            logger.debug(f"group='{row.group}' item='{row.item}' value='{row.value}'")
            table_content_dict.setdefault(row.group, []).append(row.item)
            if row.item == "gossip_active":
                assert row.value == "true", f"The value='{row.value}' is unexpected for item='{row.item}'!"
            elif row.item == "incremental_backup_enabled":
                assert row.value == "false", f"The value='{row.value}' is unexpected for item='{row.item}'!"
            elif row.item == "uptime":
                assert re.match(r"\d+ seconds", row.value), f"The value='{row.value}' is unexpected for item='{row.item}'!"
                assert int(row.value.replace(" seconds", "")) > 0, "Uptime for a node should be more than 0!"
            elif row.group == "memory" and row.item == "total":
                if node.scylla_mode() != "debug":
                    total_memory = int(row.value)
                    # see also seastar/src/core/resource.cc: allocate(configuration&)
                    memory_alignment = 2 << 20
                    # Seastar always aligns the memory allocated for each shard to
                    # memory_alignment to the first aligned size smaller than the
                    # allocated size. so, if "node.memory() / node.smp()" is not aligned,
                    # the memory allocated for each shard would be smaller than this size.
                    # hence the total size of memory reported by Scylla would be smaller
                    # than the specified total memory size.
                    min_size = node.memory() - node.smp() * memory_alignment
                    assert min_size < total_memory <= node.memory(), f"Unexpected memory value: {row.value}"
            else:
                assert self.is_number(value=row.value), f"The type of value='{row.value}' for item='{row.item}' is not number (integer or float)!"

        for group, item_list in expected_content.items():
            assert table_content_dict.get(group), f"Records for group='{group}' were not found in the table!"
            assert sorted(item_list) == sorted(table_content_dict.get(group)), f"Unexpected list of items for group='{group}'!"

    @pytest.mark.parametrize(
        "item,default_state,changed_state,changing_command,reverting_command",
        [("gossip_active", "true", "false", "disablegossip", "enablegossip"), ("incremental_backup_enabled", "false", "true", "enablebackup", "disablebackup")],
        ids=["gossip_active", "incremental_backup_enabled"],
    )
    @pytest.mark.single_node
    def test_content_toggle_item(
        self,
        item: str,
        default_state: str,
        changed_state: str,
        changing_command: str,
        reverting_command: str,
    ):
        """
        Test scenario:
        1. Start one Scylla node
        2. Change default state of the selected item (gossip_active or incremental_backup_enabled)
           using nodetool command
        3. Select and verify status of the selected item from the table system.runtime_info
        4. Revert default state of the selected item
        5. Select and verify status of the selected item from system.runtime_info one more time
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]
        node_ip_address = node.address()

        logger.info("Changing default state of %s on the node %s...", item, node_ip_address)
        node.nodetool(cmd=changing_command)

        with self.patient_cql_connection(node) as session:
            logger.info("Getting %s state from the table %s.%s on node %s...", item, self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            query_to_run = f"select value from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where group = 'generic' and item = '{item}';"
            item_state_changed = self.run_query_on_node(session=session, query=query_to_run)[0].value

            logger.info("Verifying the %s state...", item)
            assert item_state_changed == changed_state, f"Expected to get state '{changed_state}' for {item}, but it has '{item_state_changed}' state!"

            logger.info("Reverting default state of %s on the node %s...", item, node_ip_address)
            node.nodetool(cmd=reverting_command)

            logger.info("Getting %s state from the table %s.%s on node %s...", item, self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            item_state_reverted = self.run_query_on_node(session=session, query=query_to_run)[0].value

            logger.info("Verifying the %s state...", item)
            assert item_state_reverted == default_state, f"Expected to get state '{default_state}' for {item}, but it has '{item_state_reverted}' state!"

    @pytest.mark.single_node
    def test_cache_metrics(self):
        """
        Test scenario:
        1. Start one-node Scylla cluster
        2. Create one new table with data
        3. Select the cache metrics values from the table (group='cache')
        4. Flush the memtable for created table
        5. Select the cache metrics again and compare the results
        6. Send requests to the cashed data from flushed table
        7. Select the cache metrics one more time and compare the values
        """

        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]
        node_ip_address = node.address()

        number_of_rows = 100

        with self.patient_cql_connection(node) as session:
            logger.info("Creating a new keyspace '%s' and 1 table with data...", self.TEST_KEYSPACE)
            table = self.create_tables_with_data(session=session, keyspace_name=self.TEST_KEYSPACE, replication_factor=1, table_name_prefix="table", number_of_tables=1, number_of_rows=number_of_rows)[0]

            logger.info("Getting the cache metrics values from %s.%s on node %s before flush...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            query_to_run = f"select item, value from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where group = 'cache';"
            result = self.run_query_on_node(session=session, query=query_to_run)
            metrics_before_flush = {row.item: int(row.value) for row in result if row.item not in ["hit_rate_recent", "hit_rate_total"]}

            logger.info("Flushing the table %s...", table)
            node.nodetool(cmd=f"flush {table.replace('.', ' ')}")

            logger.info("Getting the cache metrics values from %s.%s on node %s after flush...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            result = self.run_query_on_node(session=session, query=query_to_run)
            metrics_after_flush = {row.item: int(row.value) for row in result if row.item not in ["hit_rate_recent", "hit_rate_total"]}

        logger.info("Comparing the results...")
        assert metrics_after_flush["entries"] - metrics_before_flush["entries"] >= number_of_rows
        assert metrics_after_flush["memory_used"] > metrics_before_flush["memory_used"]

        with self.patient_cql_connection(node) as session:
            logger.info("Sending request to the cached data...")
            self.run_query_on_node(session=session, query=f"select * from {table}")

            logger.info("Getting the cache metrics values from %s.%s on node %s after sending request...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            result = self.run_query_on_node(session=session, query=query_to_run)
            metrics_after_request = {row.item: int(row.value) for row in result if row.item not in ["hit_rate_recent", "hit_rate_total"]}

        logger.info("Comparing the results...")
        assert metrics_after_request["requests_total"] > metrics_after_flush["requests_total"]
        assert metrics_after_request["hits"] > metrics_after_flush["hits"]
        assert metrics_after_request["misses"] == metrics_after_flush["misses"]
        assert metrics_after_request["requests_total"] == metrics_after_request["hits"] + metrics_after_request["misses"]

    @unmark.next_gating
    @pytest.mark.xfail(reason="https://github.com/scylladb/scylla/issues/10340")
    @pytest.mark.single_node
    def test_memtable_metrics(self):
        """
        Test scenario:
        1. Start one-node Scylla cluster
        2. Select the current memtable metrics values from the table (group='memtable')
        3. Create one new table with data
        4. Select the memtable metrics again and compare the results
        """

        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]
        node_ip_address = node.address()

        with self.patient_cql_connection(node) as session:
            logger.info("Getting the memtable metrics values from %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            query_to_run = f"select item, value from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where group = 'memtable';"
            result = self.run_query_on_node(session=session, query=query_to_run)
            metrics_before = {row.item: int(row.value) for row in result}

            logger.info("Creating a new keyspace '%s' and 1 table with data...", self.TEST_KEYSPACE)
            self.create_tables_with_data(session=session, keyspace_name=self.TEST_KEYSPACE, replication_factor=1, table_name_prefix="table", number_of_tables=1, number_of_rows=100)

            logger.info("Getting the memtable metrics values from %s.%s on node %s after creating the new table...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            result = self.run_query_on_node(session=session, query=query_to_run)
            metrics_after = {row.item: int(row.value) for row in result}

        logger.info("Comparing the results...")
        assert metrics_after["entries"] > metrics_before["entries"]
        assert metrics_after["memory_used"] > metrics_before["memory_used"]


@pytest.mark.dtest_full
class TestConfigTable(SystemTableBase):
    """
    Table content example:
     name                                            | source   | type              | value
    -------------------------------------------------+----------+-------------------+----------------------------------
                       alternator_encryption_options |  default |        string map |                                {}
               native_shard_aware_transport_port_ssl |  default |           integer |                             19142
                                        cluster_name |  default |            string |                                ""
                       enable_sstable_key_validation |  default |              bool |                             false
                              saved_caches_directory | internal |            string |    "/var/lib/scylla/saved_caches"
           large_memory_allocation_warning_threshold |  default |           integer |                           1048576
                               sstable_summary_ratio |  default |            double |                            0.0005
                         listen_on_broadcast_address |  default |              bool |                             false
                               data_file_directories | internal |       string list |          ["/var/lib/scylla/data"]
                                            api_port |   config |           integer |                             10000
                                     prometheus_port |  default |           integer |                              9180
                        enable_repair_based_node_ops |  default |              bool |                              true
                                     hints_directory | internal |            string |           "/var/lib/scylla/hints"
                              memtable_flush_writers |  default |           integer |                                 1
                            compaction_static_shares |  default |             float |                                 0
                                      developer_mode |      cli |              bool |                              true
                                  prometheus_address |      cli |            string |                      "127.0.21.1"
    """

    TABLE_NAME = "config"

    @pytest.mark.single_node
    def test_content_values_types(self):
        """
        1. Create a one-node Scylla cluster
        2. Select content from 'system.config' table
        3. Verify the types of values in the 'value' column
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        with self.patient_cql_connection(node) as session:
            logger.info("Getting content of %s.%s table on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
            table_content = self.run_query_on_node(session=session, query=f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};")

        logger.info("Verifying the values' types...")
        for row in table_content:
            if row.type in ["integer", "double", "float"]:
                assert self.is_number(row.value), f"It seems the type of value for '{row.name}' is not '{row.type}'."
            elif row.type == "bool":
                assert row.value.lower() in ["true", "false"], f"It seems the type of value for '{row.name}' is not '{row.type}'."
            elif row.type == "string list":
                assert re.match(r"\[.*]", row.value), f"It seems the type of value for '{row.name}' is not '{row.type}'."
            elif row.type == "string map":
                assert re.match(r"\{.*}", row.value), f"It seems the type of value for '{row.name}' is not '{row.type}'."
            elif row.type == "string":
                assert row.value.isprintable(), f"It seems the type of value for '{row.name}' is not '{row.type}'."
            else:
                assert row.value, "The value is empty!"

    @pytest.mark.single_node
    def test_content_source_config(self):
        """
        1. Create a one-node Scylla cluster
        2. Select content from 'system.config' table
        3. Verify the values of the parameters with source='config' match the values from scylla.yaml
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]
        node_ip_address = node.address()

        logger.info("Getting content of scylla.yaml on node %s...", node_ip_address)
        config_file_path = os.path.join(node.get_path(), "conf/scylla.yaml")
        with open(file=config_file_path, encoding="utf-8") as file:
            scylla_yaml_content = yaml.safe_load(file)

        for key, value in scylla_yaml_content.items():
            scylla_yaml_content[key] = str(value).lower() if isinstance(value, bool) else str(value)

        with self.patient_cql_connection(node) as session:
            logger.info("Getting content of %s.%s table on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            table_content = self.run_query_on_node(session=session, query=f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};")

        config_properties = [row for row in table_content if row.source == "config"]
        json_rows = ["seed_provider", "experimental_features"]

        logger.info("Verifying values of config parameters...")
        for row in config_properties:
            assert scylla_yaml_content.get(row.name), f"Could not find the parameter '{row.name}' in scylla.yaml!"

            scylla_yaml_value = scylla_yaml_content[row.name]
            row_value = row.value

            if row.name == "seed_provider" and str(row_value) == '"seed_provider_type"':
                # allow older scylla which does not provide this info
                continue

            if row.name in json_rows:
                row_value = str(json.loads(row_value))
            else:
                row_value = row_value.strip('"').replace('"', "'")

            # Configuration options that are based on `enum_option` like `restriction mode`
            # are specified using e.g. `false`, `true`, and `warn` in scylla.yaml, but in
            # system.config the corresponding values are `0`, `1`, and `warn`.
            # Convert 0 -> false and 1 -> true for the check to pass.
            enum_option_types = {"restriction mode", "tablets mode"}
            if row.type in enum_option_types:
                if row_value == "1":
                    row_value = "true"
                if row_value == "0":
                    row_value = "false"

            assert scylla_yaml_value == row_value, f"Wrong value for name='{row.name}' in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}. Expected: {scylla_yaml_value}. Got: {row_value}. Row={row}"

    @pytest.mark.single_node
    def test_content_source_cli(self):
        """
        1. Create a one-node Scylla cluster
        2. Select content from 'system.config' table
        3. Verify the values of the parameters with source='cli' match the values of startup CLI arguments
        """
        logger.debug("Preparing the cluster...")
        cluster = self.cluster
        started_node_data = cluster.populate(1).start()[0]
        logger.debug("Cluster has been prepared...")

        node = cluster.nodelist()[0]
        node_ip_address = node.address()

        logger.info("Getting startup CLI args on node %s...", node_ip_address)
        startup_args = started_node_data[1].args

        with self.patient_cql_connection(node) as session:
            logger.info("Getting content of %s.%s table on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node_ip_address)
            table_content = self.run_query_on_node(session=session, query=f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME};")

        cli_properties = [row for row in table_content if row.source == "cli"]

        logger.info("Verifying values of CLI parameters...")
        for row in cli_properties:
            # The example of the list of Scylla CLI arguments saved in 'startup_args' variable:
            # ['--api-address', '127.0.21.1', '--developer-mode', 'true', '--prometheus-address', '127.0.21.1']
            # The name of the parameter from the table is transformed to match the name of the CLI parameter
            arg_name = f"--{row.name}".replace("_", "-")
            row_value = row.value.strip('"').replace('"', "'")
            assert arg_name in startup_args, f"Could not find the parameter '{arg_name}' in Scylla startup arguments!"

            startup_arg_value = startup_args[startup_args.index(arg_name) + 1]
            if row_value == "true":
                assert startup_arg_value in ["1", "true"], f"Wrong value for name='{row.name}' in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}. Expected: '1' or 'true'. Got: {row_value}."
            elif row_value == "false":
                assert startup_arg_value in ["0", "false"], f"Wrong value for name='{row.name}' in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}. Expected: '0' or 'false'. Got: {row_value}."
            else:
                assert row_value == startup_arg_value, f"Wrong value for name='{row.name}' in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}. Expected: {startup_arg_value}. Got: {row_value}."

    @pytest.mark.parametrize(
        "statement,error_message",
        [
            ("set source = 'default' where name = 'api_port'", "option value is required"),
            ("set source = 'default', value = '15000' where name = 'api_port'", "option source is not updateable"),
            ("set type = 'bool', value = '15000' where name = 'api_port'", "option type is immutable"),
            ("set value = '15000' where name = 'api_port'", "option is not live-updateable"),
            ("set value = '15000' where name = 'some_generic_name'", "no such option"),
            pytest.param("set value = 'true' where name='failure_detector_timeout_in_ms'", "Operation failed for system.config", marks=[pytest.mark.xfail(reason="https://github.com/scylladb/scylla/issues/10394"), unmark.next_gating]),
        ],
        ids=["no_value_provided", "source_not_updatable", "type_not_updatable", "parameter_not_live_updatable", "wrong_parameter_name", "wrong_value_type"],
    )
    @pytest.mark.single_node
    def test_invalid_update(self, statement: str, error_message: str):
        """
        Test scenario:
        1. Create a one-node Scylla cluster
        2. Run unacceptable update query fot 'system.config' table
        3. Verify the exception was raised
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        with self.patient_cql_connection(node) as session:
            query_to_run = f"update {self.KEYSPACE_NAME}.{self.TABLE_NAME} {statement};"

            logger.info("Trying to run update query '%s' on the node %s...", query_to_run, node.address())

            with pytest.raises(WriteFailure) as exc_info:
                self.run_query_on_node(session=session, query=query_to_run)
            assert error_message in str(exc_info.value), f"Returned message '{exc_info.value!s}' doesn't contain '{error_message}'!"

    @pytest.mark.single_node
    def test_content_update_value(self):
        """
        Test scenario:
        1. Create a one-node Scylla cluster
        2. Update the parameter in 'system.config' table
        3. Verify the updated value of the parameter and the changed source (after update it should be 'cql').
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        parameters_to_update = {
            "compaction_enforce_min_threshold": "true",
            "failure_detector_timeout_in_ms": "30000",
            "max_hinted_handoff_concurrency": "10",
            "enable_repair_based_node_ops": "false",
            "allowed_repair_based_node_ops": "some_value",
            "force_gossip_generation": "5",
            "abort_on_internal_error": "true",
            "max_partition_key_restrictions_per_query": "150",
            "max_clustering_key_restrictions_per_query": "150",
            "max_memory_for_unlimited_query_soft_limit": "1572864",
            "max_memory_for_unlimited_query_hard_limit": "157286400",
            "max_concurrent_requests_per_shard": "430000000",
            "strict_allow_filtering": "1",
            "flush_schema_tables_after_modification": "false",
            "restrict_replication_simplestrategy": "warn",
            "restrict_dtcs": "0",
        }

        # TODO: remove the following filtering when the fix for
        #  the issue https://github.com/scylladb/scylla/issues/10047 becomes the part of the Scylla build
        for parameter in ["strict_allow_filtering", "restrict_replication_simplestrategy", "restrict_dtcs"]:
            parameters_to_update.pop(parameter)

        errors = {}

        with self.patient_cql_connection(node) as session:
            for parameter, value in parameters_to_update.items():
                logger.info("Updating parameter '%s' in the table %s.%s on the node %s...", parameter, self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
                value_for_update = "0" if value == "false" else "1" if value == "true" else value
                try:
                    self.run_query_on_node(session=session, query=f"update {self.KEYSPACE_NAME}.{self.TABLE_NAME} set value = '{value_for_update}' where name = '{parameter}';")
                except (WriteFailure, InvalidRequest) as exc:
                    errors[parameter] = f"Could not update the parameter '{parameter}'. The error message: {exc}"

                logger.info("Checking updated parameter '%s'...", parameter)
                updated_parameter = self.run_query_on_node(session=session, query=f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where name = '{parameter}';")[0]

                logger.info("Validating values...")
                updated_parameter_value = updated_parameter.value.strip('"')
                if updated_parameter_value != value and not errors.get(parameter):
                    errors[parameter] = f"Wrong value for the updated parameter '{parameter}'! Expected: '{value}', got: '{updated_parameter_value}'"
                if updated_parameter.source != "cql" and not errors.get(parameter):
                    errors[parameter] = f"Wrong source for the updated parameter '{parameter}'! Expected: 'cql', got: '{updated_parameter.source}'"
        assert not errors, f"Got the following errors:\n{list(errors.values())}"

    @pytest.mark.single_node
    def test_content_disable_update(self):
        """
        Test scenario:
        1. Create a one-node Scylla cluster
        2. Set the parameter 'enable_cql_config_updates' to false in 'system.config' table
        3. Try to update the parameter 'enable_cql_config_updates' back to true and verify that update is now impossible
           and the exception is raised.
        """
        cluster = self.prepare_cluster(nodes=1)
        node = cluster.nodelist()[0]

        error_message = "this virtual table doesn't allow updates"

        with self.patient_cql_connection(node) as session:
            logger.info("Setting parameter 'enable_cql_config_updates' to false in the table %s.%s on the node %s...", self.KEYSPACE_NAME, self.TABLE_NAME, node.address())
            self.run_query_on_node(session=session, query=f"update {self.KEYSPACE_NAME}.{self.TABLE_NAME} set value = '0' where name = 'enable_cql_config_updates';")

            logger.info("Trying to change the value of 'enable_cql_config_updates' back to true...")
            with pytest.raises(WriteFailure) as exc_info:
                self.run_query_on_node(session=session, query=f"update {self.KEYSPACE_NAME}.{self.TABLE_NAME} set value = '1' where name = 'enable_cql_config_updates';")
            assert error_message in str(exc_info.value), f"Returned message '{exc_info.value!s}' doesn't contain '{error_message}'!"
