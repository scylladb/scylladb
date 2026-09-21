"""
Dtest for configuration runtime

Reference:
- https://github.com/scylladb/scylla/wiki/Updateable-Configuration
- https://github.com/scylladb/scylla/issues/2517
  Scylla should be able to re-read the configuration file without restart). #2517
- https://github.com/scylladb/scylla/commit/2abe015150431c61ab5e0d1bd60be9c7b3517cd1
  database: allow live update of the compaction_enforce_min_threshold config item
- https://github.com/scylladb/scylla/commit/eb496b5eaee29748f810dde00ef1d7c673b73a43
  Merge "Allow changing configuration at runtime" from Avi
"""

import logging
import os
import signal

import pytest
import requests

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node
from dtest_setup_overrides import DTestSetupOverrides
from tools.data import insert_c1c2
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.fixture(scope="function", autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    dtest_setup_overrides = DTestSetupOverrides()
    dtest_setup_overrides.cluster_options = ImmutableMapping(
        {
            "logger_log_level": {"compaction": "debug"}  # so we see compaction start/end log messages
        }
    )
    return dtest_setup_overrides


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestUpdateableConfig(Tester):
    """
    Scylla supported to change some of configuration in runtime, this
    test tested both supported and unsupported parameters.
    """

    @staticmethod
    def trigger_reload_config(node):
        """
        Signalling the scylla process with SIGHUP to trigger the configuration change effective
        """
        node.kill(signal.SIGHUP)

    def change_and_verify_config(self, node, param, value, verify_response):
        """
        Change configuration in scylla.yaml and make it effective. The updated value is verified by
        API.
        """
        mark = node.mark_log()
        logger.info(f"Change configuration {param}")
        response = requests.get(f"http://{get_ip_from_node(node)}:{node.api_port}/v2/config/{param}")
        logger.info(f"Original value before change: {response.text}")
        node.set_configuration_options({param: value})
        self.trigger_reload_config(node)
        node.watch_log_for("completed re-reading configuration file", from_mark=mark)

        logger.info(f"Using the API to validate the configuration change, expected: {verify_response}")
        response = requests.get(f"http://{get_ip_from_node(node)}:{node.api_port}/v2/config/{param}")
        assert response.text == verify_response, f"response: {response.text}, expected: {verify_response}"

    @pytest.mark.dtest_debug
    @pytest.mark.use_cassandra_stress
    def test_compaction_enforce_min_threshold(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        node1.stress(["write", "n=10000", "-rate", "threads=8"])
        with self.patient_cql_connection(node1) as session:
            session.execute(
                """
                ALTER TABLE keyspace1.standard1 WITH compaction = {
                    'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }
            """
            )

        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", True, "true")
        node1.stress(["mixed", "n=10000", "-rate", "threads=8"])

        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", False, "false")
        node1.stress(["mixed", "n=10000", "-rate", "threads=8"])

    def test_verify_min_threshold(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        min_threshold = 5
        insert_keys_num = 1

        create_ks(session=session, name="ks", rf=1)
        create_cf(session=session, name="cf", columns={"c1": "text", "c2": "text"})
        session.execute(
            """
            ALTER TABLE ks.cf WITH compaction = {
                'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : %d }
        """
            % min_threshold
        )

        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", True, "true")
        mark = node1.mark_log()
        compact_log = rf"compaction -.*Compacting \[{os.path.join(node1.get_path(), 'data/ks/cf')}"

        for _ in range(min_threshold - 1):
            insert_c1c2(session, n=insert_keys_num)
            node1.flush()
        try:
            node1.watch_log_for(compact_log, from_mark=mark, timeout=10)
        except Exception as ex:  # noqa: BLE001
            logger.info(ex)
            assert "Missing: ['compaction -.*Compacting" in str(ex)

        insert_c1c2(session, n=insert_keys_num)
        node1.flush()
        logger.info("Reach to min threshold, expect compact to be triggered")
        node1.watch_log_for(compact_log, from_mark=mark, timeout=10)

        mark = node1.mark_log()
        logger.info("Execute compact to clean the threshold counting")
        node1.compact()
        node1.watch_log_for(compact_log, from_mark=mark, timeout=10)

        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", False, "false")
        mark = node1.mark_log()
        insert_c1c2(session, n=insert_keys_num)
        node1.flush()
        logger.info("compaction_enforce_min_threshold is disabled, expect compact to be triggered by one insert")
        node1.watch_log_for(compact_log, from_mark=mark, timeout=10)

    @pytest.mark.require("#5382")
    def test_auto_adjust_flush_quota(self):
        """
        auto_adjust_flush_quota isn't a supported updateable parameter.
        """
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        logger.info("default auto_adjust_flush_quota is `false`, try to set it to false first")
        self.change_and_verify_config(node1, "auto_adjust_flush_quota", True, "false")
        self.change_and_verify_config(node1, "auto_adjust_flush_quota", False, "false")

    @pytest.mark.use_cassandra_stress
    @pytest.mark.require("#5384")
    def test_sighup_flood(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        node1.stress(["write", "n=10000", "-rate", "threads=8"])
        sighup_num = 10000

        logger.info(f"Sending %s SIGHUP signal to scylla process ...{sighup_num}")
        for _ in range(sighup_num):
            self.trigger_reload_config(node1)
        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", True, "true")
        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", False, "false")

    def test_without_config_file(self):
        """
        Test updateable config without config file.
        """
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        config_file_path = os.path.join(node1.get_path(), "conf/scylla.yaml")
        logger.info("Rename config file to test updateable config without config file")
        os.rename(config_file_path, f"{config_file_path}.backup")

        mark = node1.mark_log()
        self.trigger_reload_config(node1)
        err1 = "Could not read configuration file"
        err2 = "failed to re-read configuration file: std::invalid_argument"
        node1.watch_log_for(err1, from_mark=mark)
        node1.watch_log_for(err2, from_mark=mark)

        self.check_errors(node1, [err1, err2])

        logger.info("Recover the config file")
        os.rename(f"{config_file_path}.backup", config_file_path)
        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", True, "true")
        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", False, "false")

    @pytest.mark.single_node
    @pytest.mark.parametrize("live_cql_updates_enabled", (True, False))
    def test_blocking_config_runtime_updates(self, live_cql_updates_enabled):
        """
        Tests if users are allowed to update configuration parameters' values via CQL,
        i.e. by updating system.config virtual table.
        Modifying configuration parameters by other means, i.e. by sending a signal or calling API, is still allowed.
        """
        self.cluster.set_configuration_options(values={"live_updatable_config_params_changeable_via_cql": live_cql_updates_enabled})

        self.cluster.populate([1]).start()
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        # updates through config file reload are allowed
        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", True, "true")
        self.change_and_verify_config(node1, "compaction_enforce_min_threshold", False, "false")

        # updates through API calls are allowed
        requests.post(f"http://{node1.address()}:{node1.api_port}/task_manager/ttl?ttl=5")
        orig_value = requests.post(f"http://{node1.address()}:{node1.api_port}/task_manager/ttl?ttl=6")
        prev_value = requests.post(f"http://{node1.address()}:{node1.api_port}/task_manager/ttl?ttl=7")

        assert orig_value.text != prev_value.text

        # updates via CQL will not work if the configuration option is set to False
        if live_cql_updates_enabled:
            session.execute("UPDATE system.config SET value='2' WHERE name='task_ttl_in_seconds'")
        else:
            with pytest.raises(Exception):
                session.execute("UPDATE system.config SET value='2' WHERE name='task_ttl_in_seconds'")
