import logging
import os.path
import time
from concurrent.futures._base import Future
from concurrent.futures.thread import ThreadPoolExecutor

import pytest
import yaml
from ccmlib.scylla_cluster import ScyllaNode
from ccmlib.utils.version import ComparableScyllaVersion

from tools.cluster import enable_views_with_tablets_experimental_feature
from tools.cluster_topology import generate_cluster_topology
from tools.misc import dump_sstables
from tools.rest_clients import SystemServiceClient
from tools.stress import assert_cs_success
from upgrade_test import UpgradeTester, tablets_supported, upgrade_matrix_from_last_release_version

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class RollingUpgradeBase(UpgradeTester):
    __test__ = False

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    @pytest.mark.use_cassandra_stress
    def test_rolling_upgrade(self, dtest_config):
        self.clone_upgrade_path(dtest_config)
        self.experimental_features = dtest_config.experimental_features
        memory = 2048
        cluster_topology = generate_cluster_topology(rack_num=3)
        session = self.init_cluster(cluster_topology, jvm_args=["--memory", f"{memory}M"])
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)
        raft_topology_enabled = self.is_consistent_topology_changes_enabled(session)

        base_node__version = self.init_version
        executor = ThreadPoolExecutor(max_workers=2)

        for version in self.current_upgrade_path:
            logger.debug(f"****** START ROLLBACK TEST FROM {base_node__version} TO {version} ******")
            # Run write load in parallel with first node upgrade
            write_thread = self.run_stress(node=self.cluster.nodelist()[1], stress_command=self.write_stress_command(stress_duration_minutes=2, rf=3), executor=executor)

            # First node upgrade
            self.run_upgrade(node_index=0, upgrade_to_version=version, upgrade_type="upgrade")

            # Validate write stress
            self.validate_stress(stress_thread=write_thread, stress_type="write")

            # Start read stress load
            read_thread = self.run_stress(node=self.cluster.nodelist()[0], stress_command=self.read_stress_command(stress_duration_minutes=1), executor=executor)

            # Insert data and validate existent data
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            # Validate read stress
            self.validate_stress(stress_thread=read_thread, stress_type="read")

            # Start read stress load
            read_thread = self.run_stress(node=self.cluster.nodelist()[0], stress_command=self.read_stress_command(stress_duration_minutes=2), executor=executor)

            # Second node upgrade
            self.run_upgrade(node_index=1, upgrade_to_version=version, upgrade_type="upgrade")

            # Insert data and validate existent data
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            # Validate read stress
            self.validate_stress(stress_thread=read_thread, stress_type="read")

            # Start read stress load
            read_thread = self.run_stress(node=self.cluster.nodelist()[0], stress_command=self.read_stress_command(stress_duration_minutes=2), executor=executor)

            # Second node rollback
            self.run_upgrade(node_index=1, upgrade_to_version=base_node__version, upgrade_type="rollback")

            # Insert data and validate existent data
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            # Validate read stress
            self.validate_stress(stress_thread=read_thread, stress_type="read")

            # Upgrade 2d and 3th nodes
            self.run_upgrade(node_index=1, upgrade_to_version=version, upgrade_type="upgrade")
            self.run_upgrade(node_index=2, upgrade_to_version=version, upgrade_type="upgrade")

            # Upgrade sstables (if available)
            self.upgrade_and_verify_sstable()

            logger.debug(f"****** FINISHED ROLLBACK TEST FROM {base_node__version} TO {self.cluster.nodelist()[0].node_scylla_version} ******")

            base_node__version = version

        if "consistent-topology-changes" in self.scylla_features:
            if not raft_topology_enabled:
                supports_post_raft_procedures = ComparableScyllaVersion(self.cluster.nodelist()[0].node_scylla_version) >= ComparableScyllaVersion("2026.1-dev")
                if supports_post_raft_procedures:
                    self.wait_upgrade_schema_on_raft_finished()
                self.enable_raft_topology()
                if supports_post_raft_procedures:
                    self.wait_for_sl_v2()
            # insert data and validate data after upgrade
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

        executor.shutdown()
        session.cluster.shutdown()

    def check_schema_agreement(self):
        for node in self.cluster.nodelist():
            logger.debug("Check schema version on node %s", node.name)
            with self.exclusive_cql_connection(node) as session:
                session.cluster.control_connection.wait_for_schema_agreement(wait_time=60)

    @staticmethod
    def write_stress_command(stress_duration_minutes: int, rf: int = 3) -> list:
        return ["write", f"cl=QUORUM", f"duration={stress_duration_minutes}m", "-rate", "threads=10", "-log", "interval=5", "-schema", f"replication(factor={rf})", *RollingUpgradeBase.stress_retry_on_error_options()]

    @staticmethod
    def read_stress_command(stress_duration_minutes: int) -> list:
        return ["read", f"duration={stress_duration_minutes}m", "no-warmup", "-rate", "threads=2", "-pop", "seq=1...10000"]

    @staticmethod
    def stress_retry_on_error_options():
        return ["-errors", "retries=10", "delay-policy=exponential", "min-delay-ms=20", "max-delay-ms=20000"]

    @staticmethod
    def run_stress(node: ScyllaNode, stress_command: list, executor: ThreadPoolExecutor) -> Future:
        logger.debug(f"Executing the following {stress_command[0]} stress command '{stress_command}'")
        return executor.submit(lambda: node.stress(stress_command))

    def run_upgrade(self, node_index: int, upgrade_to_version: str, upgrade_type: str):
        node_for_upgrade = self.cluster.nodelist()[node_index]
        logger.debug(f"{upgrade_type.capitalize()} {node_for_upgrade.name} node to from  '{node_for_upgrade.node_scylla_version}' to '{upgrade_to_version}' version")

        options = dict(enable_tablets=tablets_supported(upgrade_to_version))
        enable_views_with_tablets_experimental_feature(self.cluster._config_options, scylla_version=upgrade_to_version)
        if "experimental_features" in self.cluster._config_options:
            self.cluster.set_configuration_options(values=dict(experimental_features=self.cluster._config_options["experimental_features"]))

        if upgrade_type == "rollback":
            if "tablets" in self.scylla_features:
                self.cluster.set_configuration_options(values=options)
            node_for_upgrade.upgrader.upgrade(upgrade_version=upgrade_to_version)
        elif upgrade_type == "upgrade":
            if "tablets" in self.scylla_features:
                self.cluster.set_configuration_options(values=options)
            node_for_upgrade.upgrade(upgrade_to_version=upgrade_to_version)
        else:
            raise ValueError(f"Unsupported upgrade type value '{upgrade_type}'")

        self.check_schema_agreement()

    def validate_stress(self, stress_thread: Future, stress_type: str) -> None:
        logger.debug(f"Waiting until {stress_type} stress thread will finish running")
        results = stress_thread.result(timeout=300)
        # stress_to_log() already prints the stdout to logger, so no needs to
        # print it again
        assert_cs_success(results)

    def get_highest_supported_sstable_version(self):
        """
        find the highest sstable format version supported in the cluster

        :return:
        """
        output = []
        for node in self.cluster.nodelist():
            client = SystemServiceClient(node)
            highest_version = client.get_highest_supported_sstable_version()
            if highest_version is not None:
                output.append(highest_version)
            else:
                ## Fallback to support older versions
                match = node.grep_log(r"Feature (.*)_SSTABLE_FORMAT is enabled")
                version = [m[1].group(1).lower() for m in match] if match else []
                output.extend(version)
        return max(set(output))

    def get_highest_chosen_sstable_version(self):
        """
        Find the highest sstable format version chosen for use in the cluster.

        Unlike get_highest_supported_sstable_version which returns the highest
        format a node can support, this returns the version that is actually
        selected for writing new sstables (considering feature flags and config).

        Falls back to get_highest_supported_sstable_version for older versions
        that don't have the chosen_sstable_version API.

        :return: the chosen sstable format version string
        """
        output = []
        for node in self.cluster.nodelist():
            client = SystemServiceClient(node)
            chosen_version = client.get_chosen_sstable_version()
            if chosen_version is not None:
                output.append(chosen_version)
        if output:
            return max(set(output))
        return self.get_highest_supported_sstable_version()

    def upgradesstables_if_command_available(self):
        upgradesstables_available = []
        for node in self.cluster.nodelist():
            upgradesstables_available.append(node.upgradesstables_if_command_available())

        return all(upgradesstables_available)

    def upgradesstables(self):
        for node in self.cluster.nodelist():
            node.nodetool(cmd="upgradesstables -a")

    def wait_for_sstables_upgrade(self, expected_sstable_format_version, timeout=60):
        all_tables_upgraded = True

        logger.debug("Start waiting for upgardesstables to finish")
        start_time = time.time()
        finished = False
        while not finished:
            for node in self.cluster.nodelist():
                try:
                    sstable_versions = node.check_node_sstables_format()
                    assert len(sstable_versions) == 1, f"expected all table format to be the same found {sstable_versions}"
                    assert next(iter(sstable_versions)) == expected_sstable_format_version, f"expected to format version to be '{expected_sstable_format_version}', found '{next(iter(sstable_versions))}'"
                except Exception:
                    if time.time() - start_time > timeout:
                        raise
                    all_tables_upgraded = False

            if all_tables_upgraded:
                finished = True

    def upgrade_and_verify_sstable(self):
        chosen_sstable_version = self.get_highest_chosen_sstable_version()
        upgradesstables_available = self.upgradesstables_if_command_available()
        if upgradesstables_available:
            logger.debug("Upgrading sstables if new version is available")
            self.upgradesstables()
            self.wait_for_sstables_upgrade(chosen_sstable_version)

            # Verify sstabledump
            logger.debug('Starting "scylla sstable dump-data" to verify correctness of sstables')
            # When tablets are enabled the load balancer may migrate and merge tablets after all
            # nodes finish upgrading, so a given node (e.g. nodelist()[0]) is not guaranteed to
            # hold any local sstables for ks.cf. Iterate over all nodes and use the first one
            # that actually has data.
            # the default ks and cf used by UpgradeTester.prepare_schema()
            jsoninfo = []
            for node in self.cluster.nodelist():
                jsoninfo = dump_sstables(node, "ks", "cf")
                if jsoninfo:
                    break
            assert jsoninfo, "Failed to create sstable dump"

    def remove_experimental_features_from_yaml(self, node: ScyllaNode, experimental_features: list[str]):
        scylla_yaml = os.path.join(node.get_conf_dir(), "scylla.yaml")
        with open(scylla_yaml) as fp:
            data = yaml.safe_load(fp)

        data["experimental_features"] = list(set(self.experimental_features) - set(experimental_features))

        with open(scylla_yaml, "w") as fp:
            yaml.safe_dump(data, fp)

    def add_experimental_features_from_yaml(self, node: ScyllaNode, experimental_features: list[str]):
        scylla_yaml = os.path.join(node.get_conf_dir(), "scylla.yaml")
        with open(scylla_yaml) as fp:
            data = yaml.safe_load(fp)

        data["experimental_features"] = list(set(self.experimental_features) | set(experimental_features))

        with open(scylla_yaml, "w") as fp:
            yaml.safe_dump(data, fp)


@pytest.mark.dtest_full
@pytest.mark.require("jira:SCYLLADB-2062")
class TestRollingUpgrade(RollingUpgradeBase):
    __test__ = True
    _multiprocess_can_split_ = False

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]
