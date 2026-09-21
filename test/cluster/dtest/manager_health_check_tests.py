import logging
import os
import re
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from alternator_utils import ALTERNATOR_PORT, WriteIsolation
from dtest_class import Tester, get_ip_from_node
from dtest_scylla_manager import (
    AlternatorStatus,
    CqlStatus,
    HostHealth,
    HostRestStatus,
    Memory,
    NodeStatus,
    ScyllaManagerError,
    ScyllaManagerMixin,
    ScyllaManagerTool,
    Status,
    TaskStatus,
)
from tools.cluster_topology import generate_cluster_topology
from tools.docker_utils import running_in_podman
from tools.iptables import IPTable, IPTableRule
from tools.misc import generate_ssl_stores
from tools.retrying import retrying

CLUSTER_NAME = "cluster1"

logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestManagerHealthCheck(Tester, ScyllaManagerMixin):
    def get_manager_cluster(self):
        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name}")
        manager_cluster = manager_tool.add_cluster(node=self.cluster.nodelist()[0], name=cluster_name)
        return manager_cluster

    def test_auto_gen_cql_health_check_task(self):
        """
        ver: 1.4
        verify that auto generated health check task is created and verify default interval
        """
        self._template_auto_gen_health_check_task(health_check_type="cql")

    def _template_auto_gen_health_check_task(self, health_check_type, extra_config_options=None):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        self.config_and_create_cluster(topology=topology_layout, extra_config_options=extra_config_options)
        manager_cluster = self.get_manager_cluster()

        manager_version = manager_cluster.scylla_manager.version
        default_interval = 15 if manager_version < "3.7.0-dev" else 60  # seconds

        if health_check_type == "cql":
            healthcheck_task = manager_cluster.get_healthcheck_task()
        else:
            healthcheck_task = manager_cluster.get_healthcheck_alternator_task()
        next_run_seconds = (healthcheck_task.next_run - datetime.now(UTC)).seconds
        assert next_run_seconds < default_interval
        assert TaskStatus.ERROR.value not in healthcheck_task.status.value

    def test_down_node_isnt_pinged(self):
        """
        When a node is DN, the manager should not ping the agent to check the node's CQL and REST statuses,
        but instead it should just skip them, and in the output sctool cluster status it should just mark them as '-'
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, _node2, node3 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        node3.stop(wait_other_notice=True)
        cluster_status = mgr_cluster.get_hosts_health()
        downed_node_data: HostHealth = cluster_status[node3.address()]
        regular_node_data: HostHealth = cluster_status[node1.address()]
        assert downed_node_data.cql == downed_node_data.rest == Status(), "The manager pinged a node while it was DN, while it should skip any DN nodes"
        assert regular_node_data.cql.status == CqlStatus.UP and regular_node_data.rest.status == HostRestStatus.UP, "The status of an UN node is not UP"

    def test_auto_gen_health_check_alternator_task(self):
        """
        ver: 2.2
        verify that auto generated alternator health check task is created and verify default interval
        """
        self._template_auto_gen_health_check_task(health_check_type="alternator")

    def test_down_alternator_node_isnt_pinged(self):
        """
        ver: 2.2
        When a node is DN, the manager should not ping the agent to check the node's CQL, REST and Alternator statuses,
        but instead it should just skip them, and in the output sctool cluster status it should just mark them as '-'
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, _, node3 = self.config_and_create_cluster(topology=topology_layout, extra_config_options=dict(alternator_port=ALTERNATOR_PORT, alternator_write_isolation=WriteIsolation.ALWAYS_USE_LWT.value))
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        node3.stop(wait_other_notice=True)
        cluster_status = mgr_cluster.get_hosts_health()
        downed_node_data: HostHealth = cluster_status[node3.address()]
        regular_node_data: HostHealth = cluster_status[node1.address()]
        assert downed_node_data.alternator == downed_node_data.cql == downed_node_data.rest == Status(), "The manager pinged a node while it was DN, while it should skip any DN nodes"
        assert regular_node_data.alternator.status == AlternatorStatus.UP and regular_node_data.cql.status == CqlStatus.UP and regular_node_data.rest.status == HostRestStatus.UP, "The status of an UN node is not UP"

    def test_health_check_metrics(self):
        """
        Test healthcheck new metrics (OS CPU, total OS MEM, uptime and version of Scylla and Agent)
        """
        cluster_size = 3
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=cluster_size, nodes_per_rack=1)
        nodes = self.config_and_create_cluster(topology=topology_layout)
        node1 = nodes[0]
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        manager_version = mgr_cluster.scylla_manager.version
        manager_version.v_pre_release = None
        manager_version.v_build = None
        rc_version_pattern = r"-rc\d*"
        scylla_version = self.cluster.version().replace("-dev", "")
        scylla_version = re.sub(rc_version_pattern, "", scylla_version)

        logger.info(f"Stopping the node '{node1.name}'")
        node1.stop()
        logger.info("Extracting all cluster status details")
        cluster_status = mgr_cluster.get_hosts_health()

        for node in nodes:
            node_details: HostHealth = cluster_status[node.address()]
            if node is not node1:
                node_status = NodeStatus.UP
                assert node_details.node_status == node_status, f"The status of node '{node1.name}' is not in '{node_status}'"
                assert node_details.cql.status == CqlStatus.UP, f"The CQL is not in '{CqlStatus.UP}' status"
                assert node_details.rest.status == HostRestStatus.UP, f"The REST is not in '{HostRestStatus.UP}' status"
                assert str(node_details.scylla_version).startswith(scylla_version), f"The Scylla version does not contain the '{scylla_version}' prefix"
                assert str(manager_version) == node_details.agent_version, f"The manager version '{manager_version}' is different than agent version '{node_details.agent_version}'"
            else:
                node_status = NodeStatus.DOWN
                empty_state = Status()
                assert node_details.node_status == node_status, f"The state of node '{node1.name}' is not in '{node_status}'"
                assert node_details.cql == empty_state, "The 'CQL' value is not empty"
                assert node_details.rest == empty_state, "The 'REST' value is not empty"
                assert node_details.cql == empty_state, "The 'Uptime' value is not empty"
                assert node_details.memory == Memory(), "The 'Memory' value is not empty"
                assert node_details.scylla_version is None, "The Scylla version should be empty"
                assert node_details.agent_version is None, "The agent version should be empty"

    @pytest.mark.skipif(condition=running_in_podman(), reason="can't use iptables within podman")
    def test_http_status_codes(self, request):
        """
        Block the following ports Alternator(8080), CQL(9042), and REST(10000).
        Verify that the Manager's output displays "TIMEOUT" for each port.

        :type request: pytest.FixtureRequest
        """

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        nodes = self.config_and_create_cluster(topology=topology_layout, extra_config_options=dict(alternator_port=ALTERNATOR_PORT, alternator_write_isolation=WriteIsolation.ALWAYS_USE_LWT.value))
        node1 = nodes[0]
        rest_of_the_nodes = nodes[1:]
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        iptables_obj = IPTable(chain_name=__name__)
        request.addfinalizer(iptables_obj.delete_chain)
        iptables_obj.create_new_chain()
        normal_expected_states = HostHealth(
            datacenter_name=None,
            address=None,
            host_id=None,
            status=NodeStatus.UP,
            alternator_status=AlternatorStatus.UP,
            alternator_timeout=None,
            alternator_timeout_type="ms",
            cql_status=CqlStatus.UP,
            cql_timeout=None,
            cql_timeout_type="ms",
            rest_status=HostRestStatus.UP,
            rest_timeout=None,
            rest_timeout_type="ms",
        )

        @retrying(num_attempts=10, sleep_time=4, allowed_exceptions=(ScyllaManagerError,))
        def _get_hosts_health():
            return mgr_cluster.get_hosts_health()

        def _verify_port_is_blocked(rule, expected_states):
            for node in rest_of_the_nodes:
                ip_address = get_ip_from_node(node=node)
                expected_states.address = ip_address
                rule.destination = f"{ip_address}/32"
                iptables_obj.add_rule(rule=rule)
                cluster_status = _get_hosts_health()
                assert expected_states == cluster_status[ip_address]
                iptables_obj.delete_rule(rule=rule)

        logger.info('Blocking the "CQL" port for all nodes without first node')
        states = deepcopy(normal_expected_states)
        states.cql = Status(status=CqlStatus.TIMEOUT, uptime=None, uptime_type="ms")
        _verify_port_is_blocked(rule=IPTableRule(protocol="tcp", destination_port=9042, target="DROP"), expected_states=states)

        logger.info('Blocking the "Alternator" port for all nodes without first node')
        states = deepcopy(normal_expected_states)
        states.alternator = Status(status=AlternatorStatus.TIMEOUT, uptime=None, uptime_type="ms")
        _verify_port_is_blocked(rule=IPTableRule(protocol="tcp", destination_port=8080, target="DROP"), expected_states=states)

        logger.info('Blocking the "REST" port for all nodes without first node')
        states = deepcopy(normal_expected_states)
        states.rest = Status(status=HostRestStatus.TIMEOUT, uptime=None, uptime_type="ms")
        _verify_port_is_blocked(rule=IPTableRule(protocol="tcp", destination_port=10000, target="DROP"), expected_states=states)

    @pytest.mark.xfail(reason="https://github.com/scylladb/scylla-manager/issues/3889")
    def test_error_cql_status_when_ssl_is_activated(self, secondary_cluster):
        """
        The test starts a (second) cluster with ssl disabled, and adds it to the manager.

        Afterwards, the test enables ssl encryption for the cluster, without updating the manager,
        and because of that the manager cannot communicate with the cluster through cql.

        At the end, the test requests the status of the cluster from the manager,
        and makes sure that the cql status of all of the nodes is ERROR, and that proper
        error messages were printed for each of the nodes, since the manager ('s agents)
        fail to communicate with the cluster due to the missing ssl keys.

        Introduced in manager 2.5
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        self.config_and_create_cluster(topology=topology_layout)  # cluster to be used as the manager's backend

        generate_ssl_stores(self.test_path)
        options = {
            "enabled": True,
            "certificate": os.path.join(self.test_path, "ccm_node.pem"),
            "keyfile": os.path.join(self.test_path, "ccm_node.key"),
            "truststore": os.path.join(self.test_path, "ccm_node.cer"),
            "require_client_auth": True,
        }
        secondary_topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        secondary_cluster_nodes = self.config_and_create_cluster(topology=secondary_topology_layout, cluster=secondary_cluster, extra_config_options={"client_encryption_options": options})
        secondary_mgr_cluster = self._create_mgr_cluster(node=secondary_cluster_nodes[0], name="second_cluster")
        cluster_status = secondary_mgr_cluster.get_hosts_health()
        for host_health in cluster_status.values():
            assert host_health.cql.status == CqlStatus.ERROR, f"cql status of {host_health.address} was suppose to be {CqlStatus.ERROR}, but instead the manager reported it as {host_health.cql.status}"
            assert "SSL" in host_health.error_messages[0] and "not found" in host_health.error_messages[0], (
                f"cql status of {host_health.address} is indeed {CqlStatus.ERROR}, but the error message is unclear:\n{host_health.error_messages[0]}"
            )
