import logging
import time

import pytest
from cassandra.cluster import Session
from ccmlib.scylla_cluster import ScyllaNode

from dtest_class import create_ks, wait_for
from dtest_config import DTestConfig
from ics_compaction_test import create_table
from rolling_upgrade_test import RollingUpgradeBase
from tools.cluster import run_rest_api
from tools.cluster_topology import generate_cluster_topology
from tools.marks import with_feature
from tools.raft_topology import TopologyCoordinatorFinder, get_raft_group_id, get_raft_snapshot_id
from tools.scylla_defines import CompactionStrategy
from upgrade_test import upgrade_matrix_from_last_release_version

LOGGER = logging.getLogger(__name__)


class TopologyOperationWithMixedCLusterTest(RollingUpgradeBase):
    __test__ = True
    _multiprocess_can_split_ = False

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]
    row_end_index = 100

    @pytest.mark.skip
    def test_rolling_upgrade(self, dtest_config: DTestConfig):
        """Skip test"""

    def create_cluster(self, cluster_topology: int | dict[str, dict], dtest_config: DTestConfig) -> Session:
        self.clone_upgrade_path(dtest_config)
        self.experimental_features = dtest_config.experimental_features
        memory = 1024
        session = self.init_cluster(cluster_topology, jvm_args=["--memory", f"{memory}M"])
        self.prepare_schema(session, row_end_index=self.row_end_index)
        return session

    def get_node_index(self, node: ScyllaNode) -> int:
        return self.cluster.nodelist().index(node)

    def remove_node(self, removed_node: ScyllaNode, verification_node: ScyllaNode):
        removing_host_id = removed_node.hostid()
        removed_node.stop(wait_other_notice=True)
        verification_node.removenode(removing_host_id)
        LOGGER.info("Node %s with old version was removed", removed_node.name)

        LOGGER.debug("Remove node from cluster node list")
        del self.cluster.nodes[removed_node.name]

    @pytest.mark.dtest_full
    def test_add_remove_node(self, dtest_config: DTestConfig):
        cluster_topology = generate_cluster_topology(rack_num=3, nodes_per_rack=2)
        session = self.create_cluster(cluster_topology, dtest_config=dtest_config)
        base_node_version = self.init_version
        node1, node2, node3, node4, node5, node6 = self.cluster.nodelist()

        for version in self.current_upgrade_path:
            LOGGER.info(f"****** START UPGRADE TEST FROM {base_node_version} TO {version} ******")
            row_end_index = self.row_end_index
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)
            self.run_upgrade(node_index=self.get_node_index(node1), upgrade_to_version=version, upgrade_type="upgrade")
            row_end_index += self.row_end_index
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)
            self.run_upgrade(node_index=self.get_node_index(node2), upgrade_to_version=version, upgrade_type="upgrade")
            row_end_index += self.row_end_index
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            LOGGER.info("Add new node with target version %s", version)
            node7 = self.add_new_node(version, dtest_config, datacenter=node1.data_center, rack=node1.rack)
            node7.start(wait_other_notice=True)

            LOGGER.info("Remove node %s with %s version from upgraded node %s", node3.name, base_node_version, node1.name)
            self.remove_node(removed_node=node3, verification_node=node1)

            LOGGER.info("Upgrade rest of nodes")

            for node in [node4, node5, node6]:
                self.run_upgrade(node_index=self.get_node_index(node), upgrade_to_version=version, upgrade_type="upgrade")

            self.upgrade_and_verify_sstable()
            LOGGER.info(f"****** FINISHED UPGRADE TEST FROM {base_node_version} TO {self.cluster.nodelist()[0].node_scylla_version} ******")
            base_node_version = version

    def trigger_snapshot(self, node: ScyllaNode):
        with self.exclusive_cql_connection(node=node) as exclusive_session:
            prev_raft_snapshot_id = get_raft_snapshot_id(exclusive_session)
            group_id = get_raft_group_id(exclusive_session)
            run_rest_api(node, cmd=f"/raft/trigger_snapshot/{group_id}", api_method="POST", params={})
            wait_for(lambda: prev_raft_snapshot_id != get_raft_snapshot_id(exclusive_session), timeout=30)

    @pytest.mark.next_gating
    @pytest.mark.require(condition=with_feature("consistent-topology-changes"))
    def test_trigger_snapshot_transfer(self, dtest_config: DTestConfig):
        cluster_topology = generate_cluster_topology(rack_num=3, nodes_per_rack=2)
        session = self.create_cluster(cluster_topology, dtest_config=dtest_config)
        base_node_version = self.init_version
        node1, node2, node3, node4, node5, node6 = self.cluster.nodelist()

        for version in self.current_upgrade_path:
            LOGGER.info(f"****** START UPGRADE TEST FROM {base_node_version} TO {version} ******")
            row_end_index = self.row_end_index
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)
            self.run_upgrade(node_index=self.get_node_index(node1), upgrade_to_version=version, upgrade_type="upgrade")
            row_end_index += self.row_end_index
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)
            self.run_upgrade(node_index=self.get_node_index(node2), upgrade_to_version=version, upgrade_type="upgrade")
            row_end_index += self.row_end_index
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)
            LOGGER.info("Stop node with old version")
            node3.stop(wait_other_notice=True)
            # Workaround: refresh session after stopping a node, because the driver's control
            # connection may have been on the stopped node, causing subsequent schema operations
            # to fail with ConnectionShutdown.
            # Root cause: https://github.com/scylladb/python-driver/issues/604
            session = self.get_session()
            LOGGER.info("Create schema changes")
            create_ks(session, name="ks2", rf=3)
            create_table(session, table_name="table1", keyspace_name="ks2", compaction_strategy=CompactionStrategy.SIZE_TIERED)
            LOGGER.info("Trigger new snapshot with rest api")
            topology_coordinator = TopologyCoordinatorFinder(self).get_topology_coordinator_node()
            self.trigger_snapshot(topology_coordinator)
            LOGGER.info("Start stopped node with base version")
            log_mark = node3.mark_log()
            node3.start(wait_other_notice=True)
            find_errors = node3.grep_log(r"ERROR.*database - regular column", from_mark=log_mark)
            assert not find_errors, f"Found next errors: {find_errors}"
            LOGGER.info("Upgrade rest nodes")
            for node in [node3, node4, node5, node6]:
                self.run_upgrade(node_index=self.get_node_index(node), upgrade_to_version=version, upgrade_type="upgrade")
            self.upgrade_and_verify_sstable()
            LOGGER.info(f"****** FINISHED UPGRADE TEST FROM {base_node_version} TO {self.cluster.nodelist()[0].node_scylla_version} ******")
            base_node_version = version

        row_end_index += self.row_end_index
        session = self.get_session()
        self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)
