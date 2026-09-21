import logging

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaNode
from ccmlib.utils.version import ComparableScyllaVersion

from tools.assertions import assert_all, assert_invalid, assert_one, assert_row_count
from tools.cluster_topology import generate_cluster_topology
from tools.marks import unmark
from upgrade_test import (
    UpgradeTester,
    upgrade_matrix_from_last_release_version,
)

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.require("jira:SCYLLADB-2062")
class TestSchemaChanges(UpgradeTester):
    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]
    ks = "test_upgrades"
    cf = "cf"

    @unmark.next_gating  # https://github.com/scylladb/scylla-enterprise/issues/3237
    def test_schema_and_data_on_mixed_versions_cluster(self, dtest_config):  # noqa: PLR0915
        """Check schema changes on a partly upgraded cluster.

        The test flow:

          1) Upgrade one of two nodes;
          2) Create a new keyspace and a new table on upgraded node;
          3) Check if it will be propagated to the old node;
          4) Do the same for ALTER TABLE, DROP TABLE and DROP KEYSPACE;
          5) Repeat 2-4 in the opposite direction: do a schema change on old version node and check it on upgraded.
        """
        self.clone_upgrade_path(dtest_config)
        cluster_topology = generate_cluster_topology(rack_num=2)
        self.init_cluster(cluster_topology)
        node1: ScyllaNode = self.cluster.nodelist()[0]
        node2: ScyllaNode = self.cluster.nodelist()[1]

        with self.patient_cql_connection(node1) as session:
            raft_topology_enabled = self.is_consistent_topology_changes_enabled(session)

        for version in self.current_upgrade_path:
            node_for_upgrade = self.cluster.nodelist()[0]
            # version = self.current_upgrade_path[0]
            logger.info("****** START UPGRADE TEST FROM %s TO %s ******", node_for_upgrade.node_scylla_version, version)
            logger.info(
                "Upgrade %s node to from '%s' to '%s' version",
                node_for_upgrade.name,
                node_for_upgrade.node_scylla_version,
                version,
            )
            node_for_upgrade.upgrade(upgrade_to_version=version)
            logger.info("****** FINISHED UPGRADE TO %s ******", version)

            logger.debug("Upgraded node: %s, not upgraded node: %s", node1.name, node2.name)

            assert node1.get_node_scylla_version() != node2.get_node_scylla_version(), f"Nodes have same version {node2.get_node_scylla_version()}"

            for schema_change_node, verification_node in ((node1, node2), (node2, node1)):
                logger.info("Do CREATE TABLE schema change on node %s", schema_change_node.name)
                with self.patient_exclusive_cql_connection(schema_change_node) as session:
                    logger.debug("Creating keyspace and table on node %s", schema_change_node.name)
                    session.execute(f"CREATE KEYSPACE {self.ks} WITH replication={{'class': 'NetworkTopologyStrategy', 'replication_factor': '2'}}")
                    session.execute(f"CREATE TABLE {self.ks}.{self.cf} (a int primary key, b int)")

                    logger.debug("Insert 200 rows on node %s", schema_change_node.name)
                    expected = []
                    for i in range(200):
                        session.execute(
                            SimpleStatement(
                                query_string=f"INSERT INTO {self.ks}.{self.cf} (a, b) VALUES ({i}, {i + 1})",
                                consistency_level=ConsistencyLevel.ALL,
                            )
                        )
                        expected.append([i, i + 1])

                logger.debug("Check data on node %s", verification_node.name)
                with self.patient_exclusive_cql_connection(verification_node) as session:
                    assert_row_count(
                        session=session,
                        table_name=f"{self.ks}.{self.cf}",
                        expected=len(expected),
                        consistency_level=ConsistencyLevel.ALL,
                    )
                    assert_all(
                        session=session,
                        query=f"SELECT * FROM {self.ks}.{self.cf}",
                        expected=expected,
                        cl=ConsistencyLevel.ALL,
                        ignore_order=True,
                    )
                    for i in range(200):
                        assert_one(
                            session=session,
                            query=f"SELECT * FROM {self.ks}.{self.cf} WHERE a = {i}",
                            expected=[i, i + 1],
                            cl=ConsistencyLevel.ALL,
                        )

                logger.info("Do ALTER TABLE schema change on node %s", schema_change_node.name)
                with self.patient_exclusive_cql_connection(schema_change_node) as session:
                    logger.debug("Add a column to the table on node %s", schema_change_node.name)
                    session.execute(f"ALTER TABLE {self.ks}.{self.cf} ADD c int")

                    logger.debug("Update data in the altered table")
                    expected = []
                    for i in range(200):
                        session.execute(
                            SimpleStatement(
                                query_string=f"UPDATE {self.ks}.{self.cf} SET c = {i + 2} WHERE a = {i}",
                                consistency_level=ConsistencyLevel.ALL,
                            )
                        )
                        expected.append([i, i + 1, i + 2])

                logger.debug("Check data on node %s", verification_node.name)
                with self.patient_exclusive_cql_connection(verification_node) as session:
                    assert_row_count(
                        session=session,
                        table_name=f"{self.ks}.{self.cf}",
                        expected=len(expected),
                        consistency_level=ConsistencyLevel.ALL,
                    )
                    assert_all(
                        session=session,
                        query=f"SELECT * FROM {self.ks}.{self.cf}",
                        expected=expected,
                        cl=ConsistencyLevel.ALL,
                        ignore_order=True,
                    )
                    for i in range(200):
                        assert_one(
                            session=session,
                            query=f"SELECT * FROM {self.ks}.{self.cf} WHERE a = {i}",
                            expected=[i, i + 1, i + 2],
                            cl=ConsistencyLevel.ALL,
                        )

                logger.info("Do DROP TABLE schema change on node %s", schema_change_node.name)
                with self.patient_exclusive_cql_connection(schema_change_node) as session:
                    logger.debug("Drop table on node %s", schema_change_node.name)
                    session.execute(f"DROP TABLE {self.ks}.{self.cf}")

                logger.debug("Check if the table dropped on node %s", verification_node.name)
                with self.patient_exclusive_cql_connection(verification_node) as session:
                    assert_invalid(session=session, query=f"SELECT * FROM {self.ks}.{self.cf}", expected=InvalidRequest)

                logger.info("Do DROP KEYSPACE schema change")
                with self.patient_exclusive_cql_connection(schema_change_node) as session:
                    logger.debug("Drop keyspace on node %s", schema_change_node.name)
                    session.execute(f"DROP KEYSPACE {self.ks}")

                logger.debug("Check if the keyspace dropped on node %s", verification_node.name)
                with self.patient_exclusive_cql_connection(verification_node) as session:
                    assert_invalid(session=session, query=f"USE {self.ks}", expected=InvalidRequest)

            logger.info(
                "Upgrade %s node to from '%s' to '%s' version",
                node2.name,
                node2.node_scylla_version,
                version,
            )
            node2.upgrade(upgrade_to_version=version)
            logger.info("****** FINISHED UPGRADE TO %s ******", version)
            supports_post_raft_procedures = ComparableScyllaVersion(node1.node_scylla_version) >= ComparableScyllaVersion("2026.1-dev")

            # if test run with gossip topology, don't run raft topology upgrade procedure
            if not raft_topology_enabled and "force_gossip_topology_changes" not in self.cluster._config_options:
                if supports_post_raft_procedures:
                    self.wait_upgrade_schema_on_raft_finished(nodes=[node1, node2])
                self.enable_raft_topology(nodes=[node1, node2])
                if supports_post_raft_procedures:
                    self.wait_for_sl_v2(nodes=[node1, node2])
                raft_topology_enabled = True

        # run raft topology upgrade procedure for latest version
        # if test was started in gossip topology mode
        if not raft_topology_enabled and "force_gossip_topology_changes" in self.cluster._config_options:
            supports_post_raft_procedures = ComparableScyllaVersion(node1.node_scylla_version) >= ComparableScyllaVersion("2026.1-dev")
            if supports_post_raft_procedures:
                self.wait_upgrade_schema_on_raft_finished(nodes=[node1, node2])
            self.enable_raft_topology(nodes=[node1, node2])
            if supports_post_raft_procedures:
                self.wait_for_sl_v2(nodes=[node1, node2])
