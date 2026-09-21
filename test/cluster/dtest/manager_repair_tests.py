import logging
import re
import time
from pprint import pformat

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, WaitTimeoutExpiredError, create_cf, create_ks
from dtest_scylla_manager import (
    HostHealth,
    HostRestStatus,
    NodeStatus,
    RepairTask,
    ScyllaManagerError,
    ScyllaManagerMixin,
    ScyllaManagerTool,
    Status,
    TaskStatus,
)
from tools.assertions import assert_all
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2
from tools.retrying import retrying

CLUSTER_NAME = "cluster1"

logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaMgmtRepair(Tester, ScyllaManagerMixin):
    KEYSPACE_NAME = "ks"

    def test_manager_sanity(self):
        self._initiate_cluster_with_data()
        node1, node2, _node3 = self.cluster.nodelist()

        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        logger.debug("Test rename cluster from: {} to: {}".format(cluster_name, cluster_name + "_renamed"))
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name=f"{cluster_orig_name}_renamed")
        assert mgr_cluster.name == cluster_orig_name + "_renamed", "Cluster name wasn't changed after update command"

        logger.debug("Test cluster Repair task")
        mgr_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id)
        task_final_status = mgr_task.wait_and_get_final_status()
        assert task_final_status == TaskStatus.DONE, f"Task: {mgr_task.id} final status is: {mgr_task.status!s}."

        logger.debug("Test cluster Health-Check task")
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        logger.debug(f"Health-check task history is: {healthcheck_task.history}")
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            host_health: HostHealth = host_health  # noqa: PLW0127, PLW2901
            assert host_health.node_status == NodeStatus.UP, "Not all hosts status is 'UP'"
            assert host_health.rest.status == HostRestStatus.UP, "Not all hosts REST status is 'UP'"

        # Check for sctool status change after scylla node down
        sleep = 20
        node2.stop(wait_other_notice=True)
        logger.debug(f"Health-check next run is: {healthcheck_task.next_run.isoformat()}")
        logger.debug(f"Sleep {sleep} seconds, waiting for health-check task to run after node down")
        time.sleep(sleep)

        dict_host_health = mgr_cluster.get_hosts_health()
        empty_status = Status()
        node_ip = node2.address()
        node_details: HostHealth = dict_host_health[node_ip]
        assert node_details.node_status == NodeStatus.DOWN, f"Host: {node_ip} status is not 'DOWN'"
        assert node_details.cql == empty_status, f"Host: {node_ip} CQL status is not 'DOWN'"
        assert node_details.rest == empty_status, f"Host: {node_ip} REST status is not 'DOWN'"

        node2.start()

    def _initiate_cluster_with_data(self):
        logger.debug("Starting cluster and inserting data...")

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, _node2, _node3 = self.config_and_create_cluster(topology=topology_layout)
        # Start a cluster of three nodes, and create a keyspace with RF=3, and

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)

        session.execute("CREATE TABLE cf (name text, pet text, age int, PRIMARY KEY ((name), pet)) WITH compression = {} AND read_repair_chance = 0.0;")

        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'kitty', 5)", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'adamdami', 1)", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

    def _insert_data_range_to_specific_node(self, node_to_insert, nodes_to_shut_down, keyspace_name, data_ranges):
        logger.debug(f"Stopping the following node(s): {[node.name for node in nodes_to_shut_down]}")
        [node.stop(wait_other_notice=True) for node in nodes_to_shut_down]

        session = self.patient_cql_connection(node_to_insert, keyspace_name)
        for cf_name, cf_range in data_ranges.items():
            if keyspace_name is None:
                ks, cf = cf_name.split(".")
            else:
                ks, cf = keyspace_name, cf_name
            insert_c1c2(session=session, keys=cf_range, consistency=ConsistencyLevel.ONE, c1_values=["value1"] * len(cf_range), c2_values=["value2"] * len(cf_range), ks=ks, cf=cf)
        node_to_insert.flush()

        logger.debug(f"Starting the following node(s) (again) : {[node.name for node in nodes_to_shut_down]}")
        [node.start(wait_other_notice=True, wait_for_binary_proto=True) for node in nodes_to_shut_down]

    def _assert_multiple_row_ranges_from_specific_node(self, node_to_query, nodes_to_shut_down, keyspace_name, tables_and_row_count_dict):
        logger.debug(f"Stopping the following nodes: {[node.name for node in nodes_to_shut_down]}")
        [node.stop(wait_other_notice=True) for node in nodes_to_shut_down]

        session = self.patient_cql_connection(node_to_query, keyspace=keyspace_name)
        for table_name, expected_row_range in tables_and_row_count_dict.items():
            expected_row_list = [[f"k{key}", "value1", "value2"] for key in expected_row_range]
            assert_all(session=session, query=f"select key, c1, c2 from {table_name};", expected=expected_row_list, ignore_order=True)

        logger.debug(f"Starting the following nodes (again) : {[node.name for node in nodes_to_shut_down]}")
        [node.start(wait_other_notice=True, wait_for_binary_proto=True) for node in nodes_to_shut_down]

    def test_manager_repair_multi_cfs(self):
        """
        Repairing all of the cfs in a keyspace
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2, and a table cf.
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        node1, node2 = self.config_and_create_cluster(topology=topology_layout)
        session = self.patient_cql_connection(node1)
        create_ks(session, self.KEYSPACE_NAME, 2)

        # Create 4 tables
        for i in range(1, 5):
            create_cf(session, "cf%d" % i, read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")

        num_of_keys = 10
        # Data for all tables. Will be used to validate the repaired data
        data_range = {f"cf{i}": range(i * i, i * i + num_of_keys) for i in range(1, 5)}

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2], keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        # Checking that all of tables that are stored on node2 are empty
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict={table_name: [] for table_name, key_range in data_range.items()})

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        logger.debug("Run repair on node 2")
        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, keyspace_list=self.KEYSPACE_NAME)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=data_range)

    def test_manager_repair_two_keyspaces(self):
        """
        Test parameter "-K"  - the repair all tables in the keyspace. We start two nodes and a
        keyspace with RF=2.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        node1, node2 = self.config_and_create_cluster(topology=topology_layout)
        session = self.patient_cql_connection(node1)

        # Create 2 keyspaces, with 1 table in each
        for i in range(1, 3):
            create_ks(session, "ks%d" % i, 2)
            create_cf(session, "ks%d.cf" % i, read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")

        num_of_keys = 10
        # Data for all tables. Will be used to validate the repaired data
        data_range = {}
        for i in range(1, 3):
            data_range["ks%d.cf" % i] = range(i * i, i * i + num_of_keys)

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2], keyspace_name=None, data_ranges=data_range)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1], keyspace_name=None, tables_and_row_count_dict={table_name: [] for table_name, key_range in data_range.items()})

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        logger.debug("Run repair on node 2")
        repair_task = mgr_cluster.repair_api.repair(keyspace_list="ks*", cluster_name=mgr_cluster.id)

        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1], keyspace_name=None, tables_and_row_count_dict=data_range)

    def test_repairing_host_with_several_hosts(self):
        """
        Test the parameter --with_hosts with several nodes, to see that when it's being used,
        the repair will only use the specified nodes, and not others.
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=4, nodes_per_rack=1)
        node1, node2, node3, node4 = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(node1)
        create_ks(session, self.KEYSPACE_NAME, 4)

        # Create 3 tables
        create_cf(session, "first_cf_to_repair", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        create_cf(session, "second_cf_to_repair", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        create_cf(session, "third_cf_to_repair", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")

        # Data for each table. Will be used to validate the repaired data
        first_range_to_repair = {"first_cf_to_repair": range(1, 11)}
        second_range_to_repair = {"second_cf_to_repair": range(11, 21)}
        third_range_to_repair = {"third_cf_to_repair": range(21, 31)}

        self.cluster.flush()

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2, node3, node4], keyspace_name=self.KEYSPACE_NAME, data_ranges=first_range_to_repair)
        self._insert_data_range_to_specific_node(node_to_insert=node2, nodes_to_shut_down=[node1, node3, node4], keyspace_name=self.KEYSPACE_NAME, data_ranges=second_range_to_repair)
        self._insert_data_range_to_specific_node(node_to_insert=node3, nodes_to_shut_down=[node1, node2, node4], keyspace_name=self.KEYSPACE_NAME, data_ranges=third_range_to_repair)

        @retrying(num_attempts=60, sleep_time=1, allowed_exceptions=ScyllaManagerError)
        def _add_cluster_with_retry():
            logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
            return manager_tool.add_cluster(node=node1, name=cluster_name)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        mgr_cluster = _add_cluster_with_retry()

        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, keyspace_list=self.KEYSPACE_NAME)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        # Each of nodes 1-3 inserted a few set of rows, and after the repair node4 should contain all rows
        self._assert_multiple_row_ranges_from_specific_node(
            node_to_query=node4,
            nodes_to_shut_down=[node1, node2, node3],
            keyspace_name=self.KEYSPACE_NAME,
            tables_and_row_count_dict=dict(list(first_range_to_repair.items()) + list(second_range_to_repair.items()) + list(third_range_to_repair.items())),
        )

    def test_repairing_a_downed_node(self):
        """
        Test that when the repair is executed on a stopped node, the task will fail
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, _node2, node3 = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(node1)
        create_ks(session, self.KEYSPACE_NAME, 3)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        # Create 2 tables
        for i in range(1, 3):
            create_cf(session, "cf%d" % i, read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")

        node3.stop(wait_other_notice=True)
        try:
            _ = mgr_cluster.repair_api.repair(keyspace_list=self.KEYSPACE_NAME, cluster_name=mgr_cluster.id)
            self.fail("Repairing an unavailable node did not fail as expected")
        except ScyllaManagerError as err:
            assert "repairing DCs with down nodes won't be successful" in err.args[0], f"Unexpected error: {err.args[0]}"

    def test_repair_dc_by_name(self):
        """
        Test the repair command with the '--dc-list' flag, expecting that the repair will be executed on the
        specified dc and it alone.
        """
        number_of_dcs = 3
        amount_of_nodes_in_dc = 2
        topology_layout = generate_cluster_topology(dc_num=number_of_dcs, rack_num=amount_of_nodes_in_dc, nodes_per_rack=1, dc_name_prefix="dc")
        dc1_node1, dc1_node2, dc2_node1, dc2_node2, dc3_node1, dc3_node2 = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(dc1_node1)
        dc_replication_dict = {f"dc{i}": amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        data_range = {"cf1": range(1, 11)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node1, nodes_to_shut_down=[dc2_node1, dc2_node2, dc3_node1, dc3_node2], keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=dc1_node1, name=cluster_name)

        repair_task = mgr_cluster.repair_api.repair(dc_names=["dc1", "dc2"], keyspace_list=self.KEYSPACE_NAME, cluster_name=mgr_cluster.id)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node1, nodes_to_shut_down=[dc1_node1, dc1_node2, dc3_node1, dc3_node2], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc3_node1, nodes_to_shut_down=[dc1_node1, dc1_node2, dc2_node1, dc2_node2], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict={"cf1": []})

    def test_repair_multiple_dc_by_name(self):
        """
        Test the repair command with the '--dc-list' flag, expecting that the repair will be executed on the
        specified dc's and them alone.
        """
        number_of_dcs = 4
        amount_of_nodes_in_dc = 1
        topology_layout = generate_cluster_topology(dc_num=number_of_dcs, rack_num=amount_of_nodes_in_dc, nodes_per_rack=1, dc_name_prefix="dc")
        dc1_node, dc2_node, dc3_node, dc4_node = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(dc1_node)
        dc_replication_dict = {f"dc{i}": amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        data_range = {"cf1": range(1, 11)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node, nodes_to_shut_down=[dc2_node, dc3_node, dc4_node], keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=dc1_node, name=cluster_name)

        repair_task = mgr_cluster.repair_api.repair(dc_names=["dc1", "dc2", "dc3"], keyspace_list=self.KEYSPACE_NAME, cluster_name=mgr_cluster.id)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node, nodes_to_shut_down=[dc1_node, dc3_node, dc4_node], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc3_node, nodes_to_shut_down=[dc1_node, dc2_node, dc4_node], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc4_node, nodes_to_shut_down=[dc1_node, dc2_node, dc3_node], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict={"cf1": []})

    def test_cross_dc_repair(self):
        """
        Test that executes a repair on a node by using a node from a different dc, expecting success.
        """
        number_of_dcs = 2
        amount_of_nodes_in_dc = 1
        topology_layout = generate_cluster_topology(dc_num=number_of_dcs, rack_num=amount_of_nodes_in_dc, nodes_per_rack=1, dc_name_prefix="dc")
        dc1_node, dc2_node = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(dc1_node)
        dc_replication_dict = {f"dc{i}": amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        data_range = {"cf1": range(1, 11)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node, nodes_to_shut_down=[dc2_node], keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=dc1_node, name=cluster_name)

        repair_task = mgr_cluster.repair_api.repair(keyspace_list=self.KEYSPACE_NAME, cluster_name=mgr_cluster.id)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node, nodes_to_shut_down=[dc1_node], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=data_range)

    def test_repairing_from_local_dc_only(self):
        """
        When executing a repair with only one dc in the '--dc-list' flag, the repair should only be executed among
        the nodes in the specified dc, and it should not involve any other node. The following test makes sure of that.
        """
        number_of_dcs = 2
        amount_of_nodes_in_dc = 2
        topology_layout = generate_cluster_topology(dc_num=number_of_dcs, rack_num=amount_of_nodes_in_dc, nodes_per_rack=1, dc_name_prefix="dc")
        dc1_node1, dc1_node2, dc2_node1, dc2_node2 = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(dc1_node1)
        dc_replication_dict = {f"dc{i}": amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        create_cf(session, "cf_dc1", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        create_cf(session, "cf_dc2", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        first_data_range = {"cf_dc1": range(1, 11)}
        second_data_range = {"cf_dc2": range(11, 21)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node1, nodes_to_shut_down=[dc1_node2, dc2_node1, dc2_node2], keyspace_name=self.KEYSPACE_NAME, data_ranges=first_data_range)
        self._insert_data_range_to_specific_node(node_to_insert=dc2_node1, nodes_to_shut_down=[dc1_node1, dc1_node2, dc2_node2], keyspace_name=self.KEYSPACE_NAME, data_ranges=second_data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=dc1_node1, name=cluster_name)

        repair_task = mgr_cluster.repair_api.repair(dc_names=["dc2"], keyspace_list=self.KEYSPACE_NAME, cluster_name=mgr_cluster.id)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node2, nodes_to_shut_down=[dc1_node1, dc1_node2, dc2_node1], keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=dict(second_data_range, cf_dc1=[]))

    @pytest.mark.skip("Times out in the jenkins job")
    def test_fail_fast(self):
        """
        When the '--fail-fast' flag is used on a repair command, the task should immediately fail upon error,
        and if it's not used, the task will not immediately fail. The following test checks both cases.
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, node2, node3 = self.config_and_create_cluster(topology=topology_layout)

        session = self.patient_cql_connection(node1)
        create_ks(session, self.KEYSPACE_NAME, 3)
        create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
        data_range = {"cf1": range(1, 1111)}
        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2, node3], keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        repair_task_fail_fast = mgr_cluster.repair_api.repair(keyspace_list=self.KEYSPACE_NAME, is_fail_fast=True, cluster_name=mgr_cluster.id)
        logger.debug("Stopping the node used for the repair, expecting the repair task (that uses fail-fast) to reach the status of 'ERROR' soon after")
        repair_task_fail_fast.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=10)
        logger.debug(f"task {repair_task_fail_fast.id} has reached the status of RUNNING, shutting down the host with which the task is using to repair")
        node1.stop(wait_other_notice=True)
        repair_task_fail_fast.wait_for_status(list_status=[TaskStatus.ERROR], timeout=120, step=3)

        node1.start(wait_for_binary_proto=False, wait_other_notice=False)

        repair_task = mgr_cluster.repair_api.repair(keyspace_list=self.KEYSPACE_NAME, cluster_name=mgr_cluster.id)
        logger.debug(f"Stopping the node used for the repair. Since The repair does not use the 'fail-fast' flag, the new task, {repair_task.id}, is not expected to reach the 'ERROR' status soon")
        repair_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=10)
        node1.stop(wait_other_notice=True)
        try:
            repair_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=240, step=10)
        except WaitTimeoutExpiredError:
            pass
        else:
            assert True, "Even without --fail-fast flag, the repair task failed in a short time"

    def test_repair_task_update_arguments(self):
        """
        Updating a repair task after its first run (changing the target table. Keyspace, etc.) and running it again.
        Verify that on its second run, the repair will act upon its updated parameters, rather than the parameters it
         received when it was created
        """

        def is_keyspace_in_sctool_info(task, keyspace_name):
            assert repair_task.properties["keyspace"] == keyspace_name

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        node1 = self.config_and_create_cluster(topology=topology_layout)[0]
        session = self.patient_cql_connection(node1)
        for idx in range(1, 3):
            create_ks(session, "ks%d" % idx, 2)
            create_cf(session, "cf%d" % idx)

        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node1, name="cluster1")
        mgr_cluster.repair_task_list[0].enabled(is_enabled=False)
        logger.debug("Create repair task with keyspace ks1")
        repair_task = mgr_cluster.repair_api.repair(keyspace_list="ks1", cluster_name=mgr_cluster.id)
        logger.debug("Verify the repair runs for keyspace ks1")
        is_keyspace_in_sctool_info(repair_task, "ks1")
        logger.debug("Update repair task with a different keyspace: ks2")
        repair_task.stop()
        repair_task.update(keyspace_list="ks2")
        repair_task.start()
        logger.debug("Test repair task updated argument values keyspace: ks2")
        is_keyspace_in_sctool_info(repair_task, "ks2")

    def test_intensity_and_parallel(self):
        """
        Executing a repair with a specified intensity, and executing a task status
        Expected:
         The value of the intensity arg of the task status will be included in the output of task info
        """
        cluster_size = 2
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=cluster_size, nodes_per_rack=1)
        node1, *_ = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        keyspace_name = "keyspace1"
        table_name = "cf1"
        intensity = 2
        parallel_value = 1

        with self.patient_cql_cluster_session(node1) as session:
            create_ks(session=session, name=keyspace_name, rf=cluster_size)
            create_cf(session=session, name=table_name)

        logger.info(f"Creating new repair task with following values: 'keyspace'={keyspace_name}, intensity='{intensity}' and parallel='{parallel_value}'")
        repair_task = mgr_cluster.repair_api.repair(keyspace_list=keyspace_name, intensity=intensity, parallel=parallel_value, cluster_name=mgr_cluster.id)
        logger.info(f"Checking the 'intensity', 'parallel' and 'keyspace' parameters are exists under task info")
        task_properties = repair_task.properties
        assert task_properties["keyspace"] == keyspace_name, "The value of the `keyspace` property should be '{}' and not '{}'".format(keyspace_name, task_properties["keyspace"])
        assert task_properties["intensity"] == intensity, "The value of the `intensity` property should be '{}' and not '{}'".format(intensity, task_properties["intensity"])
        assert task_properties["parallel"] == parallel_value, "The value of the `parallel` property should be '{}' and not '{}'".format(parallel_value, task_properties["parallel"])

    def test_small_table_threshold_parameter(self):
        """
        * Check the small table threshold setting is working correctly.
        Expected: Only one repair command has been executed for a table with a threshold less or equal than that
         specified in the command.
        """
        topology_layout = {"dc1": {"rack1": 2, "rack2": 1}}
        nodes = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=nodes[0], name=CLUSTER_NAME)
        keyspace_name = "keyspace1"
        table_name = "cf1"
        keyspace_table_and_key_range = {keyspace_name: {table_name: (1, 21)}}

        logger.info(f"Creating a new table with following values: '{pformat(keyspace_table_and_key_range)}")
        self.insert_data_from_ranges(healthy_node=nodes[1], keyspace_table_and_key_range=keyspace_table_and_key_range, rf=2)
        logger.info(f"Stopping the node '{nodes[0].name}")
        nodes[0].stop()
        stress_command = ["write", "no-warmup", "n=3000", "-schema", f"keyspace={keyspace_name}", "-rate", "threads=50", "-pop", "seq=1..3000"]
        logger.info(f"Starting a stress command form node '{nodes[0].name}' with following parameters: \n'{pformat(stress_command)}")
        nodes[1].stress(stress_command)

        marks = [node.mark_log() for node in nodes]
        logger.info(f"Starting the node '{nodes[0].name}")
        nodes[0].start()
        logger.info(f"Starting a stress command with following parameters: '{stress_command}")
        repair_task = mgr_cluster.repair_api.repair(keyspace_list=keyspace_name, small_table_threshold="100M", cluster_name=mgr_cluster.id)
        list_status = [TaskStatus.DONE]
        logger.info(f"Waiting until the status of the repair task will be '{list_status}'")
        repair_task.wait_for_status(list_status=list_status, timeout=40, step=3)

        logger.info(f"Verify the repair happens (only once) for required table '{keyspace_name}.{table_name}'")

        if "tablets" in self.scylla_features:
            repair_log_message = r"Repair \d+ out of \d+ tablets: table="
            table_name_pattern = re.compile(r"table=[^.\s]+\.([^.\s]+)")
        else:
            repair_log_message = f"starting user-requested repair for keyspace {keyspace_name}, repair id"
            table_name_pattern = re.compile(r'"columnFamilies": "(.*?)",')

        logs = []
        for node, mark in zip(nodes, marks):
            for line in node.grep_log(repair_log_message, from_mark=mark):
                if line:
                    logs.append(line[0])
        table_names = [table_name_pattern.findall(log)[0] for log in logs]

        assert len(table_names) == len(set(logs)), "More than one repair was executed"
        assert table_name in table_names, f"The '{repair_log_message}' message for keyspace '{keyspace_name}.{table_name}' not found!\nThe following logs are found {pformat(logs)} "

    def test_repair_host_flag_appears_in_task_info(self):
        """
        New in manager 3.0
        The test creates a repair task with a specified host to repair,
        and then makes sure that the host flag appear in the output of the
        sctool info command.
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, host=node1.address())
        task_properties = repair_task.properties
        assert "host" in task_properties and task_properties["host"] == node1.address(), f"The sctool info command should have reported '{node1.address()}' as the host of the task, but it did not:\n{repair_task.info(parse_table_res=False)}"

    @pytest.mark.required_features("!tablets")
    def test_ignore_down_hosts_with_one_down_host(self):
        """
        The test starts a cluster and takes down one of the nodes.
        Afterwards, we start a normal repair, expecting it to fail, and another repair with the ignore-down-hosts
        parameter, expecting the task to succeed.
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, _node2, node3 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        keyspace_name = "keyspace1"
        table_name = "cf1"

        with self.patient_cql_cluster_session(node1) as session:
            create_ks(session=session, name=keyspace_name, rf=3)
            create_cf(session=session, name=table_name)

        node3.stop(wait_other_notice=True)

        try:
            _ = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, keyspace_list=keyspace_name, num_retries=1)
            self.fail("Should not be able to start repair task when host in DN")
        except ScyllaManagerError as err:
            assert "repairing DCs with down nodes won't be successful" in err.args[0], f"Unexpected error: {err.args[0]}"

        ignoring_repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, keyspace_list=keyspace_name, ignore_down_hosts=True)
        ignoring_repair_task.wait_and_get_final_status(step=5)
        assert ignoring_repair_task.status == TaskStatus.DONE, "Even with ignore-down-hosts parameter, the repair task has failed when one of the nodes was DN"

    def test_ignore_down_hosts_with_fully_functioning_cluster(self):
        """
        The test creates a regular cluster, and starts a repair task with the ignore-down-hosts parameter.
        The repair is expected to be successful, even though all of the nodes were UN.
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, *_ = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        keyspace_name = "keyspace1"
        table_name = "cf1"

        with self.patient_cql_cluster_session(node1) as session:
            create_ks(session=session, name=keyspace_name, rf=3)
            create_cf(session=session, name=table_name)

        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, keyspace_list=keyspace_name, ignore_down_hosts=True)
        repair_task.wait_and_get_final_status(step=5)
        assert repair_task.status == TaskStatus.DONE, "A repair with ignore-down-hosts parameter has failed, even when the entire cluster was UN"

    @staticmethod
    def _get_repaired_keyspace_list(task: RepairTask):
        # The table in the repair progress contains 4 columns, so the lists that describes those lines will have 4 items
        # Like so:
        # Run:		5dfbc8df-5169-11ee-99a9-f4ee08c9cc47
        # Status:		DONE
        # Start time:	12 Sep 23 15:39:08 IDT
        # End time:	12 Sep 23 15:39:09 IDT
        # Duration:	1s
        # Progress:	100%
        # Datacenters:
        #   - datacenter1
        # +-------------------------------+--------------------------------+----------+----------+
        # | Keyspace                      |                          Table | Progress | Duration |
        # +-------------------------------+--------------------------------+----------+----------+
        # | ks                            |                             cf | 100%     | 0s       |
        # +-------------------------------+--------------------------------+----------+----------+
        # | scylla_manager                |                     backup_run | 100%     | 0s       |
        # | scylla_manager                |            backup_run_progress | 100%     | 0s       |
        # | scylla_manager                |                        cluster | 100%     | 0s       |
        # | scylla_manager                |                         drawer | 100%     | 0s       |
        # | scylla_manager                |                 gocqlx_migrate | 100%     | 0s       |
        # | scylla_manager                |                     repair_run | 100%     | 0s       |
        # | scylla_manager                |            repair_run_progress | 100%     | 0s       |
        # +-------------------------------+--------------------------------+----------+----------+
        #
        # Translates to:
        # 05 = {list: 1} ['Progress: 100%']
        # 06 = {list: 1} ['Datacenters:']
        # 07 = {list: 1} ['- datacenter1']
        # 08 = {list: 4} ['Keyspace', 'Table', 'Progress', 'Duration']
        # 09 = {list: 4} ['ks', 'cf', '100%', '0s']
        # 10 = {list: 4} ['scylla_manager', 'backup_run', '100%', '0s']
        # 11 = {list: 4} ['scylla_manager', 'backup_run_progress', '100%', '0s']
        # 12 = {list: 4} ['scylla_manager', 'cluster', '100%', '0s']
        parsed_progress, _ = task.progress_details()
        table_lines = [line for line in parsed_progress if len(line) == 4]
        keyspace_names = [line[0] for line in table_lines]
        return keyspace_names

    def test_system_traces_excluded_from_repair(self):
        self._initiate_cluster_with_data()
        node1 = self.cluster.nodelist()[0]
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id)
        task_final_status = repair_task.wait_and_get_final_status()
        assert task_final_status == TaskStatus.DONE, f"Task: {repair_task.id} final status is: {repair_task.status!s}."
        keyspace_names = self._get_repaired_keyspace_list(repair_task)
        assert "system_traces" not in keyspace_names, f"even though its suppose to be excluded, system_traces was included in the standard repair:\n{repair_task.full_progress_string()}"
