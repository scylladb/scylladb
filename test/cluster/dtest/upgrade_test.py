import copy
import logging
import os
import shutil
import subprocess
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib import scylla_repository
from ccmlib.common import SCYLLA_CONF, get_default_scylla_yaml, get_version_from_build
from ccmlib.scylla_cluster import ScyllaCluster, ScyllaNode
from ccmlib.utils.version import ComparableScyllaVersion
from filelock import FileLock

from dtest_class import Tester, create_cf, create_ks, wait_for
from dtest_config import DTestConfig
from dtest_setup import DTestSetup
from tools.assertions import assert_all
from tools.cluster import enable_views_with_tablets_experimental_feature, new_node, run_rest_api
from tools.cluster_topology import generate_cluster_topology
from tools.data import simulate_write_process_in_minutes
from tools.marks import issue_open
from tools.session import get_enabled_features, get_supported_features, wait_reconnection

logger = logging.getLogger(__name__)

upgrade_matrix_full_path = ["release:2025.1", "release:2025.3", "release:2025.4", "release:2026.1"]
upgrade_matrix_from_last_release_version = ["release:2026.1"]
# Starting from 2025.1, versions are unified (source-available). No separate OSS/Enterprise.
upgrade_matrix_enterprise_full_path = upgrade_matrix_full_path
upgrade_matrix_from_last_enterprise_release_version = upgrade_matrix_from_last_release_version
# Keep some version <2025.2, because we test GSI/LSI table format. It changed
# in 2025.2 and in 2025.4, for GSI and LSI correspondingly.
upgrade_matrix_for_alternator = ["release:2025.1", "release:2025.4"]


def tablets_supported(scylla_version: str) -> bool:
    ccm_repo_cache_dir, _ = scylla_repository.setup(scylla_version)
    scylla_yaml = get_default_scylla_yaml(ccm_repo_cache_dir)
    # tablets are enabled in scylla.yaml
    # either using the legacy "enable_tablets" boolean option
    # or "tablets_mode_for_new_keyspaces" that can be set to "disabled", "enabled", or "enforced"
    return scylla_yaml.get("enable_tablets", False) or scylla_yaml.get("tablets_mode_for_new_keyspaces", "disabled") != "disabled"


class UpgradeTester(Tester):
    __test__ = False

    init_version: str
    upgrade_path: list
    _version_under_test = ""
    current_upgrade_path: list

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns += [
            # from sdcm.sct_events.group_common_events.ignore_upgrade_schema_errors
            "Failed to load schema",
            "Failed to pull schema",
            # https://github.com/scylladb/scylla/issues/7817
            r"Could not retrieve CDC streams with timestamp",
        ]

    @pytest.fixture(scope="session")
    def dtest_config(self, request, tmp_path_factory, worker_id):
        """
        override dtest_config fixture, so we should start the initial cluster the the correct release version (i.e. not the version under test)
        also in charge of making sure we downloaded and cached all the needed versions
        """
        dtest_config = DTestConfig()
        dtest_config.setup(request)

        self.add_current_version_to_upgrade_path(dtest_config)
        # put the current_upgrade_path on the dtest_config related to this class,
        # so it can be clone later in the test
        # using `self.clone_upgrade_path()`
        dtest_config.current_upgrade_path = self.create_upgrade_path()

        if worker_id == "master":
            # not executing in with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            self.download_all_relocatables(dtest_config.current_upgrade_path)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / f"download_version_for_{self.__class__.__name__!s}"
            with FileLock(str(fn) + ".lock", timeout=20 * 60.0):  # lock for 20min max
                if not fn.is_file():
                    self.download_all_relocatables(dtest_config.current_upgrade_path)
                    fn.touch()

        dtest_config.scylla_version = self.init_version
        yield dtest_config

    def get_timewindow_compaction_settings(self, optimize_enabled: bool = True):
        optimized_settings = ""
        if not optimize_enabled:
            optimized_settings = ",'enable_optimized_twcs_queries': false"

        return f"{{'class': 'TimeWindowCompactionStrategy', \
                'compaction_window_unit': 'MINUTES', \
                'compaction_window_size': 5 {optimized_settings}}}"

    def clone_upgrade_path(self, dtest_config):
        self.current_upgrade_path = copy.deepcopy(dtest_config.current_upgrade_path)
        logger.debug(f"current_upgrade_path: {self.current_upgrade_path}")
        # Remove first version from the path as it'a already used
        self.current_upgrade_path.pop(0)
        logger.debug(f"current_upgrade_path after pop: {self.current_upgrade_path}")

    def init_cluster(self, cluster_topology: int | dict[str, dict], additional_config=None, jvm_args=None, skip_session=False) -> Session | None:
        if "tablets" in self.scylla_features:
            if "experimental_features" in self.cluster._config_options:
                if "tablets" in self.cluster._config_options["experimental_features"]:
                    self.cluster._config_options["experimental_features"].remove("tablets")
        enable_views_with_tablets_experimental_feature(self.cluster._config_options, scylla_version=self.init_version)

        if additional_config is not None:
            self.cluster.set_configuration_options(values=additional_config)

        if "force_gossip_topology_changes" not in self.cluster._config_options:
            self.cluster._config_options.update(DTestSetup.get_tablets_config(tablets_supported(self.init_version)))

        self.cluster.populate(cluster_topology).start(wait_for_binary_proto=True, jvm_args=jvm_args)

        if skip_session:
            return None
        else:
            session = self.patient_cql_connection(self.cluster.nodelist()[0])
            return session

    @staticmethod
    def download_all_relocatables(current_upgrade_path):
        logger.info(f"Prepare (download) all versions start: {current_upgrade_path}")

        with ThreadPoolExecutor(max_workers=len(current_upgrade_path), thread_name_prefix="RelocatableDownload") as tp:
            threads = []
            for version in current_upgrade_path:
                logger.info(f"Download relocatables for version: {version}")
                threads.append(tp.submit(scylla_repository.setup, version))
                # Allow to start download before call the next
                sleep(3)

            for thread in threads:
                thread.result(timeout=3600)

        logger.info("Prepare (download) all versions finished")

    def validate_data(self, session: Session, row_start_index: int, row_end_index: int, flush: bool = True) -> None:
        if flush:
            self.cluster.flush()

        assert_all(session=session, query="select key, val1, val2 from ks.cf", expected=self.data(start=row_start_index, end=row_end_index), cl=ConsistencyLevel.QUORUM, ignore_order=True)

    def validate_twcs_data(self, session: Session, expected_results):
        current_results = self.get_twcs_data(session)
        assert current_results == expected_results

    def prepare_schema(  # noqa: PLR0913
        self,
        session: Session,
        keyspace_name: str = "ks",
        table_name: str = "cf",
        rf: int = 3,
        row_start_index: int = 1,
        row_end_index: int = 100,
    ):
        create_ks(session=session, name=keyspace_name, rf=rf)
        create_cf(session=session, name=table_name, key_type="int", columns={"val1": "int", "val2": "int"})

        self.insert_rows(session=session, start=row_start_index, end=row_end_index)

    def prepare_twcs_schema(self, session: Session, keyspace_name: str = "ks", table_name: str = "cf_twcs", rf: int = 3):
        create_ks(session=session, name=keyspace_name, rf=rf)
        create_cf(session=session, name=f"{table_name}", key_name="pk", key_type="int", compaction=self.get_timewindow_compaction_settings(), columns={"ck": "int", "v": "blob"}, primary_key="pk, ck")
        self.tw_pks, _ = simulate_write_process_in_minutes(self.cluster, session, keyspace_name, table_name)
        self.tw_data = self.get_twcs_data(session)

    def get_twcs_data(self, session: Session, max_time_minute: int = 20):
        queries = [
            f"SELECT * FROM ks.cf_twcs WHERE pk = {self.tw_pks[0]} and ck > {(max_time_minute - 5) * 60}",
            f"SELECT * FROM ks.cf_twcs WHERE pk = {self.tw_pks[-1]} and ck < {(max_time_minute - 15) * 60}",
            f"SELECT * FROM ks.cf_twcs WHERE pk = {self.tw_pks[len(self.tw_pks) // 2]} and ck > {(max_time_minute - 1) * 60}",
        ]
        pk_set = ",".join([str(pk) for pk in self.tw_pks[3:7]])
        queries.append(f"SELECT * FROM ks.cf_twcs WHERE pk in ({pk_set}) and ck > {2 * 60} and ck < {4 * 60}")
        pk_set = ",".join([str(pk) for pk in self.tw_pks[len(self.tw_pks) - 2 : len(self.tw_pks)]])
        queries.append(
            f"SELECT * FROM ks.cf_twcs WHERE pk in ({pk_set}) and \
                       ck > {(max_time_minute - 6) * 60} and ck < {(max_time_minute - 5) * 60}"
        )

        result = []
        for query in queries:
            res = list(session.execute(query))
            result.append(res)
        return result

    def insert_rows(self, session: Session, start: int, end: int, keyspace_name: str = "ks", cf: str = "cf") -> None:
        logger.info(f"Insert rows from {start} to {end}")
        insert_statement = session.prepare(f"INSERT INTO {keyspace_name}.{cf} (key, val1, val2) VALUES (?, ?, ?)")
        args = self.data(start, end)
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def insert_data_and_validate(self, session: Session, row_end_index: int, flush: bool = True) -> None:
        logger.info("Add more 100 rows")
        self.insert_rows(session=session, start=row_end_index - 100, end=row_end_index)
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=flush)

    def data(self, start: int, end: int) -> list:
        return [[r, r + 1, r + 2] for r in range(start, end)]

    def current_version(self, dtest_config) -> str:
        version_under_test = dtest_config.scylla_version
        assert version_under_test, "Expected SCYLLA_VERSION parameter but it isn't supplied. The test can't be run"
        return version_under_test

    def add_current_version_to_upgrade_path(self, dtest_config) -> None:
        current_version = self.current_version(dtest_config)
        if current_version not in self.upgrade_path:
            self.upgrade_path.append(current_version)

    def create_upgrade_path(self) -> list:
        current_upgrade_path = copy.deepcopy(self.upgrade_path)
        return current_upgrade_path

    @staticmethod
    def is_consistent_topology_changes_supported(session):
        return "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES" in get_supported_features(session)

    @staticmethod
    def is_consistent_topology_changes_enabled(session):
        return "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES" in get_enabled_features(session)

    @staticmethod
    def is_raft_cluster_management_supported(session):
        return "SUPPORTS_RAFT_CLUSTER_MANAGEMENT" in get_supported_features(session)

    def enable_raft_topology(self, nodes: list[ScyllaNode] | None = None, timeout: int = 120, connection_kwargs: dict | None = None):
        """Enable raft topology feature on cluster nodes"""
        if not nodes:
            nodes = self.cluster.nodelist()
        if connection_kwargs is None:
            connection_kwargs = {}

        with self.patient_cql_connection(nodes[0], **connection_kwargs) as session:
            if not self.is_consistent_topology_changes_supported(session):
                logger.debug("Consistent topology changes feature is not supported")
                return

        logger.debug("Waiting until consistent topology changes will be enable on all nodes")
        for node in nodes:
            with self.patient_exclusive_cql_connection(node, **connection_kwargs) as session:
                wait_for(self.is_consistent_topology_changes_enabled, timeout=timeout, session=session)

        node: ScyllaNode = nodes[0]
        logger.debug("Send request to start topology upgrade")
        run_rest_api(node, api_method="POST", cmd="/storage_service/raft_topology/upgrade")

        def is_upgrade_done(node: ScyllaNode) -> bool:
            result = run_rest_api(node, api_method="GET", cmd="/storage_service/raft_topology/upgrade")
            result_json = result.json() if result.text else "{}"
            return result_json == "DONE".lower()

        for node in nodes:
            wait_for(is_upgrade_done, step=1, text="Check upgrade status to...", timeout=timeout, node=node)

    @staticmethod
    def _change_cluster_version(cluster: ScyllaCluster, version: str):
        logger.debug(f"Change cluster version to {version}")
        cdir, _ = scylla_repository.setup(version)
        cluster.set_install_dir(cdir)

    def add_new_node(self, version: str, dtest_config: DTestConfig, datacenter: str | None = None, rack: str | None = None) -> ScyllaNode:
        self._change_cluster_version(self.cluster, version)
        logger.info(f"Add new node to cluster with version {version}")
        if "tablets" in self.scylla_features:
            self.cluster._config_options.update(DTestSetup.get_tablets_config(tablets_supported(version)))
        enable_views_with_tablets_experimental_feature(self.cluster._config_options, scylla_version=version)

        # Get an existing node's scylla.yaml to preserve cluster-wide config
        # (e.g. sstable format settings) that may differ between versions.
        existing_nodes = [n for n in self.cluster.nodelist()]
        existing_conf = None
        if existing_nodes:
            existing_conf_path = os.path.join(existing_nodes[0].get_conf_dir(), SCYLLA_CONF)
            if os.path.exists(existing_conf_path):
                existing_conf = existing_conf_path

        node = new_node(self.cluster, data_center=datacenter, rack=rack)

        # Copy scylla.yaml from an existing node so the new node inherits the
        # same configuration (including sstable format flags) as the rest of
        # the cluster, then re-apply node-specific settings on top.
        if existing_conf:
            new_conf_path = os.path.join(node.get_conf_dir(), SCYLLA_CONF)
            shutil.copy(existing_conf, new_conf_path)
            node.update_yaml()

        self._change_cluster_version(self.cluster, dtest_config.scylla_version)
        logger.debug(f"Node scylla version: {node.node_scylla_version}")
        return node

    def get_session(self, node=None):
        if not node:
            node = self.cluster.nodelist()[0]
        return self.patient_cql_connection(node)

    def wait_upgrade_schema_on_raft_finished(self, nodes: list[ScyllaNode] | None = None, timeout: int = 600, connection_kwargs: dict | None = None):
        """Wait raft schema upgrade procedure finished

        After all nodes have been upgraded to version with raft cluster management, raft schema management
        upgrade procedure start automatically. Need to wait while it finished for each node before
        any schema/topology changes

        """
        if not nodes:
            nodes = self.cluster.nodelist()
        if connection_kwargs is None:
            connection_kwargs = {}

        def is_upgrade_schema_on_raft_done(session):
            query = "SELECT value FROM system.scylla_local WHERE key = 'group0_upgrade_state'"
            result = session.execute(query).one()
            return result.value == "use_post_raft_procedures" if result else False

        with self.patient_cql_connection(nodes[0], **connection_kwargs) as session:
            if not self.is_raft_cluster_management_supported(session):
                logger.debug("Raft cluster management feature is not supported")
                return

        for node in nodes:
            with self.patient_exclusive_cql_connection(node, **connection_kwargs) as session:
                wait_for(is_upgrade_schema_on_raft_done, timeout=timeout, session=session)

    def wait_for_sl_v2(self, nodes: list[ScyllaNode] | None = None, timeout: int = 600, connection_kwargs: dict | None = None):
        if not nodes:
            nodes = self.cluster.nodelist()
        if connection_kwargs is None:
            connection_kwargs = {}

        def is_sl_v2_enabled(session):
            result = session.execute("SELECT value FROM system.scylla_local WHERE key = 'service_level_version'").one()
            return result.value == "2" if result else False

        for node in nodes:
            with self.patient_exclusive_cql_connection(node, **connection_kwargs) as session:
                wait_for(is_sl_v2_enabled, step=1, text="Check service levels v2 status", timeout=timeout, session=session)


class BaseTests(UpgradeTester):
    __test__ = False

    @pytest.mark.require("jira:SCYLLADB-1884")
    @pytest.mark.no_boot_speedups
    def test_cluster_upgrade(self, dtest_config):
        """
        Test upgrade all nodes in the cluster sequentially.
        Prefill the table before upgrade and validate the data is not corrupted
        """
        self.clone_upgrade_path(dtest_config)
        cluster_topology = generate_cluster_topology(rack_num=3)
        session = self.init_cluster(cluster_topology)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)
        node1: ScyllaNode = self.cluster.nodelist()[0]
        raft_topology_enabled = False

        for version in self.current_upgrade_path:
            if not raft_topology_enabled and ComparableScyllaVersion(node1.node_scylla_version) >= ComparableScyllaVersion("2026.1-dev"):
                logger.info("Enable raft topology if it's not enabled yet")
                self.wait_upgrade_schema_on_raft_finished()
                self.enable_raft_topology()
                self.wait_for_sl_v2()
                raft_topology_enabled = True

            logger.info(f"****** START UPGRADE TEST FROM {node1.node_scylla_version} TO {version} ******")

            logger.info(f"Upgrade all nodes to from '{node1.node_scylla_version}' to '{version}' version")
            self.cluster.upgrade_cluster(version)
            # because all nodes was upgraded and each node was stop/started, \
            # session could lost host info and need to be reconnected.
            wait_reconnection(session)
            # Validate existent data
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=False)

            logger.info("Add more 100 rows")
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            logger.info(f"Cluster has been upgraded. Current cluster version is {node1.node_scylla_version}")

            logger.info(f"****** FINISHED UPGRADE TO {version} ******")

        session.cluster.shutdown()

    def test_one_node_upgrade(self, dtest_config):
        """
        Test upgrade one node.
        1. Prefill the table before upgrade
        2. Upgrade one node
        3. Validate the data is not corrupted
        4. Add new data and validate
        """
        self.clone_upgrade_path(dtest_config)
        cluster_topology = generate_cluster_topology(rack_num=3)
        session = self.init_cluster(cluster_topology)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node_for_upgrade = self.cluster.nodelist()[1]

        for version in self.current_upgrade_path:
            logger.info(f"****** START UPGRADE TEST FROM {node_for_upgrade.node_scylla_version} TO {version} ******")
            logger.info(f"Upgrade {node_for_upgrade.name} node to from '{node_for_upgrade.node_scylla_version}' to '{version}' version")
            node_for_upgrade.upgrade(upgrade_to_version=version)

            # Validate existent data
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=False)

            logger.info("Add more 100 rows")
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            logger.info(f"Node {node_for_upgrade.name} has been upgraded. Current node version is {node_for_upgrade.node_scylla_version}")

            logger.info(f"****** FINISHED UPGRADE TO {version}******")

        session.cluster.shutdown()

    @pytest.mark.require("jira:SCYLLADB-1884")
    def test_upgrade_cluster_nodes_with_twcs(self, dtest_config):
        """
        Test upgrade all nodes in the cluster sequentially.
        Create schema with table with twcs
        Prefill the table before upgrade and validate the data is not corrupted
        during upgrade enable/disable optimized queries for twcs
        and validate that data no corrupted and returned same results
        """
        self.clone_upgrade_path(dtest_config)
        cluster_topology = generate_cluster_topology(rack_num=3)
        session = self.init_cluster(cluster_topology)
        self.prepare_twcs_schema(session)
        expected_data = self.get_twcs_data(session)
        [node1, node2, node3] = self.cluster.nodelist()
        raft_topology_enabled = self.is_consistent_topology_changes_enabled(session)
        for version in self.current_upgrade_path:
            logger.info(f"****** START UPGRADE TEST FROM {node1.node_scylla_version} TO {version} ******")

            supports_post_raft_procedures = ComparableScyllaVersion(node1.node_scylla_version) >= ComparableScyllaVersion("2026.1-dev")
            if not raft_topology_enabled and "force_gossip_topology_changes" not in self.cluster._config_options and supports_post_raft_procedures:
                self.wait_upgrade_schema_on_raft_finished()
                self.enable_raft_topology()
                self.wait_for_sl_v2()
                raft_topology_enabled = True

            logger.info(f"Upgrade 1st node to from '{node1.node_scylla_version}' to '{version}' version")
            node1.upgrade(version)

            logger.info("Disable optimized queries")
            session.execute(f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=False)}", timeout=600)

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info(f"Upgrade 2nd node to from '{node2.node_scylla_version}' to '{version}' version")
            node2.upgrade(version)

            logger.info("Enable optimized queries")
            session.execute(f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=True)}", timeout=600)

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info(f"Upgrade 3rd node to from '{node3.node_scylla_version}' to '{version}' version")
            node3.upgrade(version)

            logger.info("Enable optimized queries")
            session.execute(f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=False)}", timeout=600)

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info("Enable optimized queries")
            session.execute(f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=True)}", timeout=600)

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info(f"****** FINISHED UPGRADE TO {version} ******")

        session.cluster.shutdown()


@pytest.mark.dtest_full
class TestUpgradeFullPath(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_full_path
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_upgrade_cluster_nodes_with_twcs(self):
        pass


@pytest.mark.dtest_full
class TestUpgradeOneNode(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_cluster_upgrade(self):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_upgrade_cluster_nodes_with_twcs(self):
        pass


@pytest.mark.dtest_full
class TestUpgradeClusterWithEnableDisableTWCSQueries(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_cluster_upgrade(self):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass
