import datetime
import logging
from dataclasses import dataclass

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import ConsistencyLevel, Session
from cassandra.metadata import LocalStrategy, NetworkTopologyStrategy, ReplicationStrategy, SimpleStrategy
from cassandra.query import BatchStatement, SimpleStatement
from ccmlib import scylla_repository
from ccmlib.scylla_cluster import ScyllaCluster, ScyllaNode

from dtest_class import create_ks
from dtest_config import DTestConfig
from tools.cluster import new_node
from tools.data import get_keyspace_metadata, rows_to_list
from tools.rackdc import update_properties
from upgrade_test import UpgradeTester, upgrade_matrix_from_last_enterprise_release_version

logger = logging.getLogger(__name__)

"""
This test checks that the audit.audit_log table is migrated to the new schema
on upgrade. The upgrade should only be performed when the audit table
replication strategy is SimpleStrategy. If the replication strategy is
NetworkTopologyStrategy, or other, the upgrade should not be performed.
"""


@pytest.mark.require("scylladb/scylla-enterprise#3399")
@pytest.mark.next_gating
class TestAuditTableMigration(UpgradeTester):
    __test__ = True
    upgrade_path = upgrade_matrix_from_last_enterprise_release_version
    init_version = upgrade_path[0]
    ks = "test_upgrades"
    cf = "cf"

    audit_default_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def prepare(self, create_keyspace=True, nodes=1, rf=1, audit_settings=audit_default_settings, **kwargs):
        """
        Prepares the cluster for the upgrade test.
        """

        logger.debug(f"Preparing cluster with {nodes} node(s): rf={rf} audit_settings={audit_settings}")

        cluster = self.cluster

        start_rpc = kwargs.pop("start_rpc", False)
        if start_rpc:
            cluster.set_configuration_options(values={"start_rpc": True})

        cluster.set_configuration_options(values=audit_settings)

        reload_config = kwargs.pop("reload_config", False)
        if reload_config:
            # The cluster is restarted to reload the config file.
            cluster.stop()
            cluster.start(wait_for_binary_proto=True)

        if not cluster.nodelist():
            if type(nodes) is int:
                nodes = [nodes]
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            session.execute("DROP KEYSPACE IF EXISTS ks")
            create_ks(session, "ks", rf)
        return session

    def verify_audit_keyspace_type(self, node, expected_strategy_type, expected_rf=None):
        with self.patient_exclusive_cql_connection(node) as session:
            session.cluster.refresh_schema_metadata()

            metadata = get_keyspace_metadata(session, "audit")
            logger.info("%s: metadata replication strategy: %s", node.name, metadata.replication_strategy)

            assert isinstance(metadata.replication_strategy, expected_strategy_type), f"Replication strategy is not an instance of {expected_strategy_type}"
            if expected_rf is not None:
                if expected_strategy_type == NetworkTopologyStrategy:
                    rfs = metadata.replication_strategy.dc_replication_factors
                    for _, rf in rfs.items():
                        assert rf == expected_rf
                elif expected_strategy_type == SimpleStrategy:
                    assert metadata.replication_strategy.replication_factor == expected_rf

    def test_schema_changes_on_partially_upgraded_cluster(self, dtest_config):
        """Check that audit migration changes the schema on a partially upgraded
        cluster. I.e. a cluster in which only 1 out of 2 nodes has been upgraded
        will have the audit table schema changed on both nodes.

        The test flow:

          1) Upgrade one of two nodes;
          2) Verify that the schema changes (audit table) are propagated to the old node;
          3) Upgrade the second node;
          4) Verify that the schema changes (audit table) are still correct;

        """

        self.clone_upgrade_path(dtest_config)
        self.prepare(create_keyspace=True, reload_config=True, nodes=2)

        node_for_upgrade = self.cluster.nodelist()[0]
        self.verify_audit_keyspace_type(node_for_upgrade, SimpleStrategy, expected_rf=None)

        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )
        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        node1: ScyllaNode = self.cluster.nodelist()[0]
        node2: ScyllaNode = self.cluster.nodelist()[1]

        logger.debug("Upgraded node: %s, not upgraded node: %s", node1.name, node2.name)

        logger.info("Node 1 version: %s", node1.get_node_scylla_version())
        logger.info("Node 2 version: %s", node2.get_node_scylla_version())

        assert node1.get_node_scylla_version() != node2.get_node_scylla_version()

        self.verify_audit_keyspace_type(node_for_upgrade, NetworkTopologyStrategy, expected_rf=3)

        node_for_upgrade = self.cluster.nodelist()[1]
        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )
        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        logger.info("Node 1 version: %s", node1.get_node_scylla_version())
        logger.info("Node 2 version: %s", node2.get_node_scylla_version())

        assert node1.get_node_scylla_version() == node2.get_node_scylla_version()

        self.verify_audit_keyspace_type(node_for_upgrade, NetworkTopologyStrategy, expected_rf=3)

    strategies = [NetworkTopologyStrategy, SimpleStrategy, LocalStrategy]
    replication_factors = [2, 3, 5]

    @pytest.mark.parametrize("replication_factor", replication_factors)
    @pytest.mark.parametrize("strategy", strategies)
    def test_should_not_upgrade_from_strategy_other_than_simple(self, dtest_config, strategy, replication_factor):
        """Check schema changes in a cluster with the replication strategy already set.

        The test flow:

            1) Start a cluster with the default replication strategy modified manually;
            2) Upgrade one of the nodes;
            3) Verify that the schema changes are not changed unless the replication strategy is SimpleStrategy;

        """

        self.clone_upgrade_path(dtest_config)
        self.prepare(create_keyspace=False, reload_config=True, nodes=1)

        with self.patient_exclusive_cql_connection(self.cluster.nodelist()[0]) as session:
            qry = f"ALTER KEYSPACE audit WITH REPLICATION = {{ 'class' : '{strategy.__name__}', 'replication_factor' : {replication_factor} }}"
            session.execute(qry)

        node_for_upgrade = self.cluster.nodelist()[0]
        self.verify_audit_keyspace_type(node_for_upgrade, strategy, expected_rf=replication_factor)

        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )
        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        expected_strategy = strategy
        expected_rf = replication_factor
        if strategy == SimpleStrategy:
            expected_strategy = NetworkTopologyStrategy
            expected_rf = 3

        self.verify_audit_keyspace_type(node_for_upgrade, expected_strategy, expected_rf=expected_rf)

    @pytest.mark.parametrize("node_count", [1, 2, 3, 5])
    def test_reading_audit_after_migration_works(self, dtest_config, node_count):
        """Check reads are possible with the new consistency level after an upgrade.

        The test flow:

          1) Upgrade one of three nodes;
          1a) Flush the audit table; (optional)
          2) Verify that the schema changes are propagated to the old nodes;
          3) Verify that there are three replicas of the audit table (nodetool, cl queries);

        """

        audit_settings = self.audit_default_settings.copy()
        audit_settings["audit_keyspaces"] = "ks,system"

        logger.info("dtest_config: %s", dtest_config)
        self.clone_upgrade_path(dtest_config)
        self.prepare(create_keyspace=True, reload_config=True, nodes=node_count, audit_settings=audit_settings)

        nodes = self.cluster.nodelist()
        node_for_upgrade = nodes[0]

        with self.patient_exclusive_cql_connection(node_for_upgrade) as session:
            self.verify_audit_keyspace_type(node_for_upgrade, SimpleStrategy)

            # write to the audit table
            session.execute("CREATE TABLE IF NOT EXISTS ks.test (id int PRIMARY KEY, val int)")
            for i in range(64):
                with self.patient_exclusive_cql_connection(self.cluster.nodelist()[-1]) as other_session:
                    qry = "INSERT INTO ks.test (id, val) VALUES (%d, %d)"

                    stmt = BatchStatement()
                    for _ in range(1024):
                        v = i * 1024 + _
                        stmt.add(qry % (v, v))
                    other_session.execute(stmt)

        old_row = None
        with self.patient_cql_connection(node_for_upgrade) as session:
            stmt = SimpleStatement("SELECT * FROM audit.audit_log WHERE operation = 'SELECT * FROM system.peers' LIMIT 1 ALLOW FILTERING", consistency_level=ConsistencyLevel.ONE)
            old_row = rows_to_list(session.execute(stmt))[0]

        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )
        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        for node in nodes[1:]:
            assert node_for_upgrade.get_node_scylla_version() != node.get_node_scylla_version()
        versions = [node.get_node_scylla_version() for node in nodes]
        assert len(set(versions[1:])) <= 1

        logger.debug("Upgraded node: %s", node_for_upgrade.name)
        logger.debug("Not upgraded nodes: %s", [node.name for node in nodes[1:]])

        with self.patient_exclusive_cql_connection(node_for_upgrade) as session:
            self.verify_audit_keyspace_type(node_for_upgrade, NetworkTopologyStrategy, expected_rf=3)

            first_row = rows_to_list(session.execute("SELECT * FROM audit.audit_log LIMIT 1"))[0]

            expected_replicas = min(3, len(nodes))

            def row_to_key(row):
                return f"{str(row[0])[0:10]}:{row[1]}"

            ret = nodes[0].nodetool(f"getendpoints audit audit_log {row_to_key(first_row)}", True)[0]
            assert len(ret.split()) == expected_replicas

            ret = nodes[0].nodetool(f"getendpoints audit audit_log {row_to_key(old_row)}", True)[0]
            assert len(ret.split()) == expected_replicas

        # executing the following queries from a non-upgraded node (if possible)
        with self.patient_exclusive_cql_connection(nodes[-1]) as session:
            query = f"SELECT * FROM audit.audit_log WHERE date = '{old_row[0]}' AND node = '{old_row[1]}' AND event_time = {old_row[2]} ALLOW FILTERING"

            audit_rows = rows_to_list(session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)))
            num_rows = len(audit_rows)

            if node_count >= 2:
                audit_rows = rows_to_list(session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)))
                assert len(audit_rows) == num_rows

            if node_count >= 3:
                audit_rows = rows_to_list(session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.THREE)))
                assert len(audit_rows) == num_rows

    def test_upgrade_multi_dc(self, dtest_config):
        """Check that the schema changes are propagated to all the nodes in a multi-dc cluster.

        The test flow:

          1) Upgrade one of three nodes;
          2) Verify that the schema changes are propagated to the old nodes (audit table);
          3) Verify that there are three replicas of the audit table (nodetool, cl queries);

        """

        self.clone_upgrade_path(dtest_config)
        self.prepare(create_keyspace=False, reload_config=True, nodes=[2, 2])

        node_for_upgrade = self.cluster.nodelist()[0]
        self.verify_audit_keyspace_type(node_for_upgrade, SimpleStrategy)

        # write to the audit table
        with self.patient_exclusive_cql_connection(self.cluster.nodelist()[2]) as session:
            session.execute("SELECT * FROM system.peers")

        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )
        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        self.verify_audit_keyspace_type(node_for_upgrade, NetworkTopologyStrategy, expected_rf=3)

    @pytest.mark.parametrize("upgrade_node_with_audit_off", [True, False])
    def test_upgrade_with_audit_off_on_one_node(self, dtest_config, upgrade_node_with_audit_off):
        """Check schema changes on a partly upgraded cluster.

        The test flow:

          1) Upgrade one of the nodes;
          2) Disable audit on one of the nodes;
          3) Verify that the schema changes are propagated to all the nodes if
                the node with audit on is upgraded;
        """

        audit_settings = self.audit_default_settings.copy()
        audit_settings["audit_keyspaces"] = "ks,system"

        self.clone_upgrade_path(dtest_config)
        self.prepare(create_keyspace=False, reload_config=True, nodes=2, audit_settings=audit_settings)

        node_for_upgrade = self.cluster.nodelist()[0]

        self.verify_audit_keyspace_type(node_for_upgrade, SimpleStrategy)

        # write to the audit table
        with self.patient_exclusive_cql_connection(self.cluster.nodelist()[1]) as session:
            session.execute("SELECT * FROM system.peers")

        node_without_audit = self.cluster.nodelist()[0 if upgrade_node_with_audit_off else 1]
        node_without_audit.stop()
        node_without_audit.set_configuration_options(values={"audit": None})
        node_without_audit.start(wait_for_binary_proto=True)

        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )

        # turn off audit on one node
        node_for_upgrade.stop()

        node_for_upgrade.start(wait_for_binary_proto=True)

        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        expected_strategy = NetworkTopologyStrategy if not upgrade_node_with_audit_off else SimpleStrategy
        expected_rf = 3 if not upgrade_node_with_audit_off else 1
        self.verify_audit_keyspace_type(node_for_upgrade, expected_strategy, expected_rf=expected_rf)

        if upgrade_node_with_audit_off:
            node_without_audit.stop()
            node_without_audit.set_configuration_options(values={"audit": "table"})
            node_without_audit.start(wait_for_binary_proto=True)

            self.verify_audit_keyspace_type(node_without_audit, NetworkTopologyStrategy, expected_rf=3)

    def test_upgrade_new_node_audit_table_has_network_topology_strategy(self, dtest_config):
        """Check that the schema changes are propagated to new nodes

        The test flow:

          1) Upgrade one-node cluster;
          2) Add a new node;
          3) Verify that the schema changes are propagated to the new node (audit table);
        """

        self.clone_upgrade_path(dtest_config)
        self.prepare(create_keyspace=False, reload_config=True, nodes=[1])

        node_for_upgrade = self.cluster.nodelist()[0]
        self.verify_audit_keyspace_type(node_for_upgrade, SimpleStrategy)

        version = self.current_upgrade_path[0]
        logger.info(
            "Upgrade %s node to from '%s' to '%s' version",
            node_for_upgrade.name,
            node_for_upgrade.node_scylla_version,
            version,
        )

        # turn off audit on one node
        node_for_upgrade.stop()

        node_for_upgrade.start(wait_for_binary_proto=True)

        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info("****** FINISHED UPGRADE TO %s ******", version)

        self.verify_audit_keyspace_type(node_for_upgrade, NetworkTopologyStrategy, expected_rf=3)

        added_node = self.add_new_node(version=version, dtest_config=dtest_config)
        added_node.start(wait_for_binary_proto=True)

        assert len(self.cluster.nodelist()) == 2
        # added_node = self.cluster.nodelist()[1]
        assert added_node == self.cluster.nodelist()[1]

        self.verify_audit_keyspace_type(added_node, NetworkTopologyStrategy, expected_rf=3)

    @staticmethod
    def _change_cluster_version(cluster: ScyllaCluster, version: str):
        logger.debug(f"Change cluster version to {version}")
        cdir, _ = scylla_repository.setup(version)
        cluster.set_install_dir(cdir)

    def add_new_node(self, version: str, dtest_config: DTestConfig) -> ScyllaNode:
        self._change_cluster_version(self.cluster, version)
        logger.info(f"Add new node to cluster with version {version}")
        node = new_node(self.cluster)

        self._change_cluster_version(self.cluster, dtest_config.scylla_version)
        logger.debug(f"Node scylla version: {node.node_scylla_version}")
        return node
