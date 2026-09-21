import logging

import pytest
from cassandra import OperationTimedOut
from cassandra.cluster import ConsistencyLevel, NoHostAvailable
from ccmlib.node import Status

from dtest_class import Tester, create_cf, create_ks
from dtest_setup import DTestSetup
from repair_based_node_operations_test import RepairBasedNodeOperationsScenarios
from tools.cluster import run_rest_api
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, query_c1c2
from tools.marks import with_feature
from tools.retrying import retrying
from tools.status import wait_for_nodes_status

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestClusterReplacement(Tester):
    num_keys = 10000
    cluster_topology = [3, 3]

    def replace_node_by_add_and_decommission(self, old_node):
        logger.info(f"Replace {old_node.name}")
        logger.info("Adding a new node")
        logger.debug(f"Printing len of nodelist: {len(self.cluster.nodelist())}")
        new_node = self.cluster.new_node(len(self.cluster.nodelist()) + 1, data_center=old_node.data_center)
        logger.info(f"Starting {new_node.name}")
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info(f"{new_node.name} started")
        logger.info(f"Decommission {old_node.name}")
        old_node.decommission()
        logger.info(f"{old_node.name} was decommissioned")
        for node in self.cluster.nodelist():
            if node.status != Status.DECOMMISSIONED:
                node.cleanup()

    def replace_dead_node_by_remove_and_add(self, old_node):
        old_node_host_id = old_node.hostid()
        old_node.stop(gently=False, wait_other_notice=True)
        for node in self.cluster.nodelist()[::-1]:
            if node.is_running():
                assert node != old_node  # The replaced node must be dead
                logger.info(f"Removing {old_node.name}/{old_node_host_id} using {node.name}")
                node.removenode(old_node_host_id)
                break
        logger.info("Adding a new node")
        new_node = self.cluster.new_node(len(self.cluster.nodelist()) + 1, data_center=old_node.data_center, rack=old_node.rack)
        logger.info(f"Starting {new_node.name}")
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info(f"{new_node.name} started")

    def _insert_data(self, node, rf, n_of_keys=None):
        with self.patient_exclusive_cql_connection(node) as session:
            create_ks(session, name="ks", rf=rf)
            create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
            insert_c1c2(session, n=n_of_keys or self.num_keys, consistency=ConsistencyLevel.ALL)

    def _verify_data_integrity(self, n_of_keys=None, node=None):
        if not node:
            node = self.cluster.nodelist()[-1]

        # Co-located debug nodes can still be settling (CPU-starved, replicas DOWN in
        # gossip) right after the last replacement, failing verify with a heartbeat
        # timeout. Wait for the ring to settle first. See SCYLLADB-2754.
        live_nodes = [n for n in self.cluster.nodelist() if n.is_live()]
        logger.debug(f"Waiting for all {len(live_nodes)} live nodes to report UN before verifying on {node.name}")
        wait_for_nodes_status(node, ["UN"] * len(live_nodes), timeout=300)

        logger.debug(f"Verifying data integrity on {node.name}")
        # Generous timeout + retry transient errors so a single CPU stall during the
        # long sequential scan does not fail the test.
        with self.patient_cql_connection(node, request_timeout=120) as session:

            @retrying(num_attempts=5, sleep_time=2, allowed_exceptions=(OperationTimedOut, NoHostAvailable), message="verify query")
            def _query(key):
                query_c1c2(session, key, consistency=ConsistencyLevel.ONE)

            for key in range(n_of_keys or self.num_keys):
                _query(key)

    @pytest.mark.next_gating
    def test_rolling_cluster_replacement_sequentially_live_nodes(self):
        """
        This test uses the field strategy to replace a node.
        Add a new node to the cluster and then decommission the old node
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, rf=1, n_of_keys=n_of_keys)
        for node in self.cluster.nodelist():
            self.replace_node_by_add_and_decommission(old_node=node)
        self._verify_data_integrity(n_of_keys)

    def test_rolling_cluster_replacement_sequentially_live_nodes_multi_dc(self):
        """
        This test uses the field strategy to replace a node in a multi dc cluster.
        Add a new node to the cluster and then decommission the old node.
        Replace all nodes on dc2 then move to replace nodes on dc1.
        """
        cluster = self.cluster
        cluster.populate(self.cluster_topology).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, rf={"dc1": 1, "dc2": 1}, n_of_keys=n_of_keys)
        for node in self.cluster.nodelist()[::-1]:
            self.replace_node_by_add_and_decommission(old_node=node)
        self._verify_data_integrity(n_of_keys)

    @pytest.mark.next_gating
    def test_rolling_cluster_replacement_sequentially_dead_nodes(self):
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2, dc_name_prefix="dc", rack_name_prefix="r")
        cluster_topology["dc1"]["r1"] = 2
        cluster.populate(cluster_topology).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, n_of_keys=n_of_keys, rf=2)
        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        for node in self.cluster.nodelist():
            new_node = rbnos.replace_node(replaced_node=node)
            cluster.add_seed(new_node)

        self._verify_data_integrity(n_of_keys)

    def test_rolling_cluster_replacement_sequentially_dead_nodes_multi_dc(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += ["Could not retrieve CDC streams with timestamp"]
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=2, rack_num=2, nodes_per_rack=1, dc_name_prefix="dc", rack_name_prefix="r")
        cluster.populate(cluster_topology).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, rf={"dc1": 2, "dc2": 2}, n_of_keys=n_of_keys)
        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        for node in self.cluster.nodelist()[::-1]:
            new_node = rbnos.replace_node(replaced_node=node)
            cluster.add_seed(new_node)
        self._verify_data_integrity(n_of_keys)

    @pytest.mark.require("scylladb/scylladb#16826")
    def test_rolling_cluster_replacement_sequentially_dead_nodes_multi_dc_rf_1(self):
        """
        This test uses the network topology strategy to replace a node in a multi dc cluster.
        Replication is configured to have 1 replica in each data center.
        All nodes in the cluster are replaced, one at a time.
        Reproduces https://github.com/scylladb/scylladb/issues/16826
        """
        cluster = self.cluster
        cluster.populate([2, 2]).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, rf={"dc1": 1, "dc2": 1}, n_of_keys=n_of_keys)
        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        for node in self.cluster.nodelist()[::-1]:
            new_node = rbnos.replace_node(replaced_node=node)
            cluster.add_seed(new_node)
            self._verify_data_integrity(n_of_keys)

    @pytest.mark.next_gating
    def test_rolling_cluster_replacement_sequentially_dead_nodes_remove_and_add(self):
        """
        This test uses the field strategy to replace a node.
        Remove a node and then add a new node to replace it
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2, nodes_per_rack=2, dc_name_prefix="dc", rack_name_prefix="r")
        cluster.populate(cluster_topology).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, rf=2, n_of_keys=n_of_keys)
        for node in self.cluster.nodelist():
            self.replace_dead_node_by_remove_and_add(old_node=node)
        self._verify_data_integrity(n_of_keys)

    @pytest.mark.next_gating
    @pytest.mark.required_features("tablets")
    def test_rack_loss_recovery(self):
        """
        The purpose of the test is to verify that we can recover from a total loss of a rack, where
        we also have no ability to add new nodes in that rack, so replacing nodes is not an option.
        The recovery in this scenario is done by bootstrapping nodes in a new rack, and changing keyspace
        replication to use the new rack instead of the lost one.

        Scenario:
        1. Create a single-DC cluster with 3 racks, 2 nodes per rack
        2. Create a keyspace with RF=3, one table, populate with data
        3. Down one rack
        4. Mark nodes in the rack as permanently down using removenode (expected to fail in a later phase)
        5. Alter keyspace replication factor to exclude the rack
        6. Remove the nodes in the downed rack
        7. Add a new rack with 2 new nodes
        8. Alter keyspace replication to include the new rack (back to effective RF=3)
        9. Verify data integrity
        """

        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=3, nodes_per_rack=2, dc_name_prefix="dc", rack_name_prefix="rack")
        cluster.populate(cluster_topology).start()

        live_node = self.cluster.nodelist()[0]
        self._insert_data(live_node, rf={"dc1": ["rack1", "rack2", "rack3"]}, n_of_keys=self.num_keys)

        stopped = []
        stopped_hosts = set()
        for n in self.cluster.nodelist():
            if n.rack == "rack3":
                n.stop(gently=False, wait_other_notice=True)
                stopped.append(n)
                stopped_hosts.add(n.hostid())

        assert len(stopped) == 2

        live_node = next(node for node in self.cluster.nodelist() if node.hostid() not in stopped_hosts)

        # Mark as "excluded" so that ALTER can proceed.
        dead = ",".join(stopped_hosts)
        logger.info(f"Excluding stopped nodes: {dead}")
        run_rest_api(live_node, "/storage_service/exclude_node", params={"hosts": dead})

        session = self.patient_exclusive_cql_connection(live_node)

        # 120s was observed to be too low in debug mode, so increase the timeout
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2']};", timeout=300)

        for n in stopped:
            live_node.nodetool(f"removenode {n.hostid()}")

        node7 = self.cluster.new_node(len(self.cluster.nodelist()) + 1, data_center="dc1", rack="rack4")
        node7.start(wait_for_binary_proto=True, wait_other_notice=True)
        node8 = self.cluster.new_node(len(self.cluster.nodelist()) + 1, data_center="dc1", rack="rack4")
        node8.start(wait_for_binary_proto=True, wait_other_notice=True)

        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2', 'rack4']};")

        self._verify_data_integrity(self.num_keys, node=live_node)

    @pytest.mark.next_gating
    def test_rolling_cluster_replacement_sequentially_dead_nodes_remove_and_add_multi_dc(self):
        """
        This test uses the field strategy to replace a node in a multi dc cluster.
        Remove a node and then add a new node to replace it
        Replace all nodes on dc2 then move to replace nodes on dc1.
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=2, rack_num=2, nodes_per_rack=2, dc_name_prefix="dc", rack_name_prefix="r")
        cluster.populate(cluster_topology).start()
        node1 = self.cluster.nodelist()[0]
        n_of_keys = self.num_keys
        self._insert_data(node1, rf={"dc1": 2, "dc2": 2}, n_of_keys=n_of_keys)
        for node in self.cluster.nodelist()[::-1]:
            self.replace_dead_node_by_remove_and_add(old_node=node)
        self._verify_data_integrity(n_of_keys)
