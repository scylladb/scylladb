import logging
from datetime import datetime, timedelta
from time import sleep

import pytest
import requests

from dtest_class import Tester, wait_for
from dtest_scylla_manager import (
    HostHealth,
    NodeStatus,
    ScyllaManagerError,
    ScyllaManagerMixin,
    ScyllaManagerTool,
    TaskStatus,
)
from tools.cluster_topology import generate_cluster_topology

logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaManagerClusterMgmt(Tester, ScyllaManagerMixin):
    def test_adding_cluster_while_its_down(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, _node2, _node3 = self.cluster.nodelist()

        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug(f"trying to add an offline cluster to scylla-manager, named: {cluster_name}")
        self.cluster.stop()

        try:
            manager_tool.add_cluster(node=node1, name=cluster_name)
        except ScyllaManagerError as err:
            assert "connection refused" in err.args[0].lower(), "Received an irrelevant ScyllaManagerError when trying to add an offline cluster"
            return
        assert False, "Expected to fail when adding an offline cluster to the manager, but didn't"

    def test_add_more_than_one_scylla_cluster(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, _node3 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)

        cluster_name1 = "cluster1"
        cluster_name2 = "cluster2"
        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name1}")
        manager_tool.add_cluster(node=node1, name=cluster_name1)

        logger.debug(f"Add a cluster to scylla-manager, named: {cluster_name2}")
        manager_tool.add_cluster(node=node2, name=cluster_name2)

        logger.debug(manager_tool.cluster_list)
        expected_list = [cluster_name1, cluster_name2]
        assert sorted(manager_tool.parsed_cluster_list) == sorted(expected_list), f"""The list of clusters managed by the manager differ from the expected list:
            Expected:{expected_list}
            In actuality:{manager_tool.parsed_cluster_list}"""

    def _wait_until_task_has_started(self, repair_task, num_of_retries=100, interval=5):
        for i in range(num_of_retries):
            if repair_task.status == "RUNNING":
                return True
            sleep(interval)
        assert False, f"Timeout: The task {repair_task.id} did not start"

    def _node_inwhich_repair_started(self, node_list, timeout=100, step=10):
        start = datetime.now()
        while datetime.now() - start < timedelta(seconds=timeout):
            for node in node_list:
                repair_beginning_message_results = node.grep_log(expr="starting user-requested repair")
                if repair_beginning_message_results:
                    return node
            sleep(step)
        return False

    def test_removing_node_from_managed_cluster(self):
        removed_node_details: HostHealth

        def has_removed_node_reached_dn(removed_node_address):
            nonlocal removed_node_details
            all_nodes_details = mgr_cluster.get_hosts_health()
            removed_node_details = all_nodes_details[removed_node_address]
            return removed_node_details.node_status == NodeStatus.DOWN

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        node1, _, node3 = self.config_and_create_cluster(topology=topology_layout)
        step = 3
        timeout = 10 * step

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        self.cluster.remove(node3)
        wait_for(func=has_removed_node_reached_dn, step=step, text="Node status has yet to reach DN", timeout=timeout, removed_node_address=node3.address())
        assert removed_node_details.node_status == NodeStatus.DOWN, f"Even after an ample time window (>30s), the status of the removed node did not reach {NodeStatus.DOWN}, but instead remained in {removed_node_details.node_status}"
        assert removed_node_details.cql.status is None, f"The CQL status of the removed node is {removed_node_details.cql.status}, while it should be empty"
        assert removed_node_details.rest.status is None, f"The Rest status of the removed node is {removed_node_details.rest.status}, while it should be empty"

    def cluster_list(self):
        pass

    def test_sctool_status_of_two_clusters_when_one_is_faulty(self, secondary_cluster):
        """
        The test verifies that when one of the managed clusters is unreachable,
        `sctool status` will still show the status of all of the clusters.
        """

        def cause_fault_in_one_cluster_and_verify_status_of_other(healthy_mgr_cluster, cluster_to_fault_node_list):
            for node in cluster_to_fault_node_list:
                node.stop_scylla_manager_agent(gently=False)

            sctool_status_output, _ = healthy_mgr_cluster.sctool.run("status", is_verify_errorless_result=False, parse_table_res=False)

            assert f"Cluster: {healthy_mgr_cluster.name} ({healthy_mgr_cluster.id})" in sctool_status_output, (
                f"When one cluster was unreachable by the manager, there was no report on the status of the healthy cluster: {sctool_status_output}"
            )

            for node in cluster_to_fault_node_list:
                node.start_scylla_manager_agent()

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        primary_cluster_nodes = self.config_and_create_cluster(topology=topology_layout)
        primary_mgr_cluster = self._create_mgr_cluster(node=primary_cluster_nodes[0], name="cluster1")

        secondary_cluster_nodes = self.config_and_create_cluster(topology=topology_layout, cluster=secondary_cluster)
        secondary_mgr_cluster = self._create_mgr_cluster(node=secondary_cluster_nodes[0], name="second_cluster")

        cause_fault_in_one_cluster_and_verify_status_of_other(healthy_mgr_cluster=primary_mgr_cluster, cluster_to_fault_node_list=secondary_cluster_nodes)
        cause_fault_in_one_cluster_and_verify_status_of_other(healthy_mgr_cluster=secondary_mgr_cluster, cluster_to_fault_node_list=primary_cluster_nodes)

    def test_change_agent_port(self):
        """
        The test starts a normal cluster and manager, and afterwards reconfigures
        the agents' listening port and restarts them across the board.
        Afterwards the uses the sctool cluster update command to make the manager server
        acknowledge the new port, expecting success.
        At the end, we rerun the healthcheck task and make sure that both nodes
        are reported to be UP.

        * It's important to note that when we update the agent config, the agent process is restarted,
          and ccm makes sure that the agent uses the new assigned port.
        """
        new_port = 8989
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node_list = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node_list[0], name="cluster1")

        for node in node_list:
            node.update_agent_config(new_settings={"https": f"{node.address()}:{new_port}"}, restart_agent_after_change=True)
        mgr_cluster.update(port=new_port)

        mgr_cluster.get_healthcheck_task().start(continue_task=False)  # Rerunning healthcheck
        cluster_health = mgr_cluster.get_hosts_health()

        for node_address, node_health in cluster_health.items():
            assert node_health.node_status == NodeStatus.UP, f"After changing the port of the agent, the manager falsely reports that node {node_address} is down"

    def test_rest_api_status_nonexistent_task(self):
        """
        This test was created to cover https://github.com/scylladb/scylla/pull/9578 scenario
        The test checks the status of a non-existing repair and expects it to be 404
        """
        expected_status_of_nonexistent_task = 404
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _ = self.config_and_create_cluster(topology=topology_layout)
        self._create_mgr_cluster(node=node1, name="cluster1")
        address = self.cluster._scylla_manager._get_api_address()
        url = f"http://{address}/storage_service/repair_status?id=1"
        repair_get_request = requests.get(url=url)
        assert repair_get_request.status_code == expected_status_of_nonexistent_task, f"Wrong status of repair get REST API command: {repair_get_request.status_code} instead of {expected_status_of_nonexistent_task}"
