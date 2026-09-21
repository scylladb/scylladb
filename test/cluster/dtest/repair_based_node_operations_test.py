import logging
import re
from collections.abc import Callable
from dataclasses import dataclass

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.protocol import Unavailable
from ccmlib.node import NodetoolError
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from dtest_setup import DTestSetup
from tools.cluster import get_group0_members
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, query_c1c2
from tools.files import wipe_node_keyspace_directory
from tools.marks import issue_open, unmark_if, with_feature
from tools.raft_topology import TopologyCoordinatorFinder

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@dataclass
class RBNOperation:
    operation: Callable[[], ScyllaNode]
    operation_name: str
    repair_on_tested_node: bool
    repair_on_all_nodes: bool


class RepairBasedNodeOperationsScenarios:
    # Cover for feature: https://github.com/scylladb/scylla/commit/97bb2e47ff004b32b2d72f1b1f085710a14cb4e2

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [r".*Could not retrieve CDC streams"]

    def __init__(self, tester: Tester):
        # RBNO supports 5 operations: bootstrap, replace, removenode, decommission and rebuild
        # Define default behaviour for RBNO for all supported operations

        # replace:
        # It is used to replace a dead node. The token ring does not change. Replacing node pulls data from only
        # one of the replicas.
        self.replace_scenario = RBNOperation(
            operation=self.replace_node,
            operation_name="replace",  # used for validation in the log
            repair_on_tested_node=True,
            repair_on_all_nodes=False,
        )

        # replace:
        # replace with allow ignoring dead nodes:
        # Three nodes are dead. Replacing node is allowed when there are two dead nodes.
        self.replace_with_dead_nodes_scenario = RBNOperation(
            operation=self.replace_node_when_two_nodes_dead,
            operation_name="replace",  # used for validation in the log
            repair_on_tested_node=True,
            repair_on_all_nodes=False,
        )

        # replace:
        # replace all nodes in DC2 with allow ignoring dead nodes in DC2.
        self.replace_with_whole_dc_dead_nodes_scenario = RBNOperation(
            operation=self.replace_node_when_whole_dc_nodes_dead,
            operation_name="replace",  # used for validation in the log
            repair_on_tested_node=True,
            repair_on_all_nodes=False,
        )

        # replace:
        # replace dead node after cluster restart:
        # One node is dead, but cluster restarts loses its endpoint_state
        self.replace_dead_node_after_cluster_restart_scenario = RBNOperation(
            operation=self.replace_node_after_cluster_restart,
            operation_name="replace",  # used for validation in the log
            repair_on_tested_node=True,
            repair_on_all_nodes=False,
        )

        # replace:
        # replace with allow ignoring dead nodes after cluster restart:
        # Three nodes are dead. Replacing node is allowed when there are two dead nodes.
        self.replace_with_dead_nodes_after_cluster_restart_scenario = RBNOperation(
            operation=self.replace_node_when_two_nodes_dead_after_cluster_restart,
            operation_name="replace",  # used for validation in the log
            repair_on_tested_node=True,
            repair_on_all_nodes=False,
        )

        # safe_unsafe_rebuild:
        # Use multi-dc cluster to test safe/unsafe rebuild operation when another DC's node data is wiped-out.
        self.safe_unsafe_rebuild_scenario = RBNOperation(
            operation=self.safe_unsafe_rebuild,
            operation_name="rebuild",  # used for validation in the log
            repair_on_tested_node=True,
            repair_on_all_nodes=False,
        )

        # bootstrap:
        # It is used to add a new node into the cluster. The token ring changes.
        # New node pulls data from existing nodes that are losing the token ranges.
        self.bootstrap_scenario = RBNOperation(operation=self.bootstrap, operation_name="bootstrap", repair_on_tested_node=True, repair_on_all_nodes=False)
        # removenode:
        # It is used to remove a dead node out of the cluster. Existing nodes pull data from other existing nodes
        # for the new ranges it owns. It pulls from one of the replicas which might not be the latest copy.
        self.removenode_scenario = RBNOperation(operation=self.removenode, operation_name="removenode", repair_on_tested_node=False, repair_on_all_nodes=True)
        # removenode was rejected:
        # if node is up or node is down, but gossiper has status for node is up, removenode operation
        # should be rejected
        self.removenode_rejecting_scenario = RBNOperation(operation=self.removenode_rejecting, operation_name="removenode", repair_on_tested_node=False, repair_on_all_nodes=False)

        # decommission:
        # It is used to remove a live node from the cluster. Token ring changes. It does not suffer from the
        # “latest replica” issue. The leaving node pushes data to existing nodes.
        self.decommission_scenario = RBNOperation(operation=self.decommission, operation_name="decommission", repair_on_tested_node=True, repair_on_all_nodes=False)
        # rebuild:
        # It is used to get all the data this node owns from other existing nodes.
        # It pulls data from only one of the replicas which might not be the latest copy.
        self.rebuild_scenario = RBNOperation(operation=self.rebuild, operation_name="rebuild", repair_on_tested_node=True, repair_on_all_nodes=False)

        self.tester = tester
        self.operations_flow = [self.rebuild_scenario, self.removenode_scenario, self.bootstrap_scenario, self.removenode_rejecting_scenario, self.replace_scenario, self.decommission_scenario]

    def add_node(
        self,
        dc: str | None = None,
        rack: str | None = None,
        replace_node_host_id: str | None = None,
        is_seed: bool = False,
        ignore_dead_nodes: str = "",
    ) -> ScyllaNode:
        new_node_index = int(self.tester.cluster.nodelist()[-1].name.replace("node", "")) + 1
        new_node = self.tester.cluster.new_node(i=new_node_index, data_center=dc, rack=rack, is_seed=is_seed)

        jvm_args = self.tester.jvm_args
        if ignore_dead_nodes:
            # overwrite if older entires of ignore-dead-nodes-for-replace are present
            if "--ignore-dead-nodes-for-replace" in jvm_args:
                idx = jvm_args.index("--ignore-dead-nodes-for-replace")
                jvm_args[idx + 1] = ignore_dead_nodes
            else:
                jvm_args.extend(["--ignore-dead-nodes-for-replace", ignore_dead_nodes])

        new_node.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=jvm_args, replace_node_host_id=replace_node_host_id)

        return new_node

    def replace_node(self, dc: str | None = None, rack: str | None = None, replaced_node=None, is_seed: bool = False) -> ScyllaNode:
        if replaced_node is None:
            replaced_node = self.tester.cluster.nodelist()[-1]
        replaced_node_address = replaced_node.address()
        replaced_node_host_id = replaced_node.hostid()
        logger.debug(f"Stop replaced {replaced_node.name} ({replaced_node_host_id}/{replaced_node_address})")
        replaced_node.stop(wait_other_notice=True)

        if dc is None:
            dc = replaced_node.data_center
        if rack is None:
            rack = replaced_node.rack
        logger.debug(f"Add new node in dc={dc} rack={rack}")
        new_node = self.add_node(replace_node_host_id=replaced_node_host_id, dc=dc, rack=rack, is_seed=is_seed)
        logger.debug(f"Added new node {new_node.name} ({new_node.address()})")

        self.tester.cluster.remove(node=replaced_node, wait_other_notice=True, remove_node_dir=False)
        # wait in logs for confirmation that node was removed from cluster
        # otherwise decommission scenario from RepairBasedNodeOperationsScenarios will fail :
        # Cannot start: nodes={127.0.84.4} needed for decommission operation are down.
        # It is highly recommended to fix the down nodes and try again.
        if "consistent-topology-changes" in self.tester.scylla_features:
            log_message_to_wait = f"gossip - Finished to force remove node ({replaced_node_address}|{replaced_node_host_id})"
        else:
            log_message_to_wait = "has been silent for 30000ms, removing"
        new_node.watch_log_for(log_message_to_wait)
        return new_node

    def replace_node_when_two_nodes_dead(self, **kwargs) -> ScyllaNode:
        replaced_node, dead_node1, dead_node2 = self.tester.cluster.nodelist()[-3:]
        replaced_node_address = replaced_node.address()
        replaced_node_host_id = replaced_node.hostid()
        dead_node1_address = dead_node1.address()
        dead_node1_host_id = dead_node1.hostid()
        dead_node2_address = dead_node2.address()
        dead_node2_host_id = dead_node2.hostid()
        logger.debug(f"Stop 3 nodes: {replaced_node_host_id}/{replaced_node_address}, {dead_node1_host_id}/{dead_node1_address}, {dead_node2_host_id}/{dead_node2_address}")
        for node in [replaced_node, dead_node1, dead_node2]:
            node.stop(wait_other_notice=True)

        logger.debug(f"Add a new node that replaces {replaced_node_host_id}/{replaced_node_address}")
        new_node = self.add_node(replace_node_host_id=replaced_node_host_id, dc=replaced_node.data_center, rack=replaced_node.rack, ignore_dead_nodes=f"{dead_node1_host_id},{dead_node2_host_id}")
        logger.debug(f"Added the new node {new_node.name} ({new_node.address()})")

        self.tester.cluster.remove(node=replaced_node, wait_other_notice=True, remove_node_dir=False)

        # Ignored nodes are banned in raft-topology mode.
        # They cannot be restarted any longer (they will not be able to communicate with the cluster)
        if "consistent-topology-changes" not in self.tester.scylla_features:
            logger.debug(f"Start {dead_node1_address} and {dead_node2_address} nodes")
            for node in [dead_node1, dead_node2]:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

        return new_node

    def replace_node_when_whole_dc_nodes_dead(self, **kwargs) -> ScyllaNode:
        dc2_nodes = [n for n in self.tester.cluster.nodelist() if n.data_center == "DC2"]
        logger.debug(f"Stop DC2 nodes:")
        self.tester.cluster.stop_nodes(dc2_nodes, wait_other_notice=True)
        replaced_nodes = list()
        ignore_dead_nodes = list()
        for replaced_node in dc2_nodes:
            replaced_node_address = replaced_node.address()
            replaced_node_host_id = replaced_node.hostid()
            replaced_nodes.append(replaced_node)
            # get rest of the nodes from dc2_nodes into ignore_dead_nodes list
            ignore_dead_nodes = ",".join([node.hostid() for node in dc2_nodes if node not in replaced_nodes])
            logger.debug(f"Add a new node that replaces {replaced_node_host_id}/{replaced_node_address}")
            new_node = self.add_node(replace_node_host_id=replaced_node_host_id, dc=replaced_node.data_center, rack=replaced_node.rack, ignore_dead_nodes=f"{ignore_dead_nodes}")
            logger.debug(f"Added the new node {new_node.name} ({new_node.address()})")
            self.tester.cluster.remove(node=replaced_node, wait_other_notice=False, remove_node_dir=False)
        return new_node

    def replace_node_after_cluster_restart(self, **kwargs) -> ScyllaNode:
        replaced_node = self.tester.cluster.nodelist()[-1]
        replaced_node_address = replaced_node.address()
        replaced_node_host_id = replaced_node.hostid()
        logger.debug("Stop all nodes")
        self.tester.cluster.stop_nodes(gently=False, wait_other_notice=False)
        logger.debug(f"Restart all nodes except {replaced_node_host_id}/{replaced_node_address}")
        for node in self.tester.cluster.nodelist():
            if node != replaced_node:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug(f"Replace {replaced_node_host_id}/{replaced_node_address} in place")
        replaced_node.clear()
        self.tester.cluster.seeds = self.tester.cluster.nodelist()[:1]
        replaced_node.import_config_files()
        replaced_node.start(wait_other_notice=True, wait_for_binary_proto=True, replace_node_host_id=replaced_node_host_id)

        for node in self.tester.cluster.nodelist():
            if node.is_running():
                out, err = node.nodetool("status")
                logger.info(f"{node.name} nodetool status:\n{out}\nerr:\n{err}")
                out, err = node.nodetool("gossipinfo")
                logger.info(f"{node.name} nodetool gossipinfo:\n{out}\nerr:\n{err}")

        return replaced_node

    def replace_node_when_two_nodes_dead_after_cluster_restart(self, **kwargs) -> ScyllaNode:
        def _get_nodes_to_stop(nodes_to_stop_count: int) -> list[ScyllaNode]:
            nodes_all = self.tester.cluster.nodelist()

            if "force_gossip_topology_changes" in self.tester.cluster._config_options:
                # when using the gossip topology, the limited voter changes do not apply
                # - there is no topology coordinator and all nodes are becoming raft voters
                return nodes_all[-nodes_to_stop_count:]

            remaining_nodes_to_stop = nodes_to_stop_count
            # limited voters: not all nodes might be voters
            # we must ensure that we don't lose the majority of voters
            group0_members = get_group0_members(nodes_all[0])
            voter_members = {member["host_id"] for member in group0_members if member["is_voter"]}

            voters_count = len([node for node in nodes_all if node.hostid() in voter_members])
            remaining_voters_allowed_to_stop = (voters_count - 1) // 2
            nodes_to_stop = []

            topology_coordinator = TopologyCoordinatorFinder(self.tester).get_topology_coordinator_node()

            for node in reversed(nodes_all):
                if not remaining_nodes_to_stop:
                    # we got enough nodes to stop
                    break
                if node.hostid() == topology_coordinator.hostid():
                    # skip the topology coordinator
                    continue
                if node.hostid() in voter_members:
                    if not remaining_voters_allowed_to_stop:
                        # skip the voter if we can't stop any more voters
                        continue
                    remaining_voters_allowed_to_stop -= 1
                nodes_to_stop.append(node)
                remaining_nodes_to_stop -= 1

            assert len(nodes_to_stop) == nodes_to_stop_count, f"Expected {nodes_to_stop_count} nodes to stop, got {len(nodes_to_stop)}"
            return nodes_to_stop

        replaced_node, dead_node1, dead_node2 = _get_nodes_to_stop(3)  # pylint: disable=unbalanced-tuple-unpacking
        replaced_node_address = replaced_node.address()
        replaced_node_host_id = replaced_node.hostid()
        dead_node1_address = dead_node1.address()
        dead_node1_host_id = dead_node1.hostid()
        dead_node2_address = dead_node2.address()
        dead_node2_host_id = dead_node2.hostid()
        logger.debug("Stop all nodes")
        self.tester.cluster.stop_nodes(gently=False, wait_other_notice=False)
        logger.debug(f"Restart all nodes except 3: {replaced_node_host_id}/{replaced_node_address}, {dead_node1_host_id}/{dead_node1_address}, {dead_node2_host_id}/{dead_node2_address}")
        for node in self.tester.cluster.nodelist():
            if node not in [replaced_node, dead_node1, dead_node2]:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug(f"Replace {replaced_node_host_id}/{replaced_node_address} in place")
        replaced_node.clear()
        self.tester.cluster.seeds = self.tester.cluster.nodelist()[:1]
        replaced_node.import_config_files()
        jvm_args = self.tester.jvm_args
        jvm_args.extend(["--ignore-dead-nodes-for-replace", f"{dead_node1_host_id},{dead_node2_host_id}"])
        replaced_node.start(wait_other_notice=True, wait_for_binary_proto=True, replace_node_host_id=replaced_node_host_id, jvm_args=jvm_args)

        for node in self.tester.cluster.nodelist():
            if node.is_running():
                out, err = node.nodetool("status")
                logger.info(f"{node.name} nodetool status:\n{out}\nerr:\n{err}")
                out, err = node.nodetool("gossipinfo")
                logger.info(f"{node.name} nodetool gossipinfo:\n{out}\nerr:\n{err}")

        return replaced_node

    def verify_data_integrity(self, node: ScyllaNode, rows: int = 1000):
        logger.debug(f"Verifying data integrity on {node.name}")
        with self.tester.patient_cql_connection(node) as session:
            for key in range(rows):
                query_c1c2(session, key, consistency=ConsistencyLevel.QUORUM)

    def safe_unsafe_rebuild(self, **kwargs) -> ScyllaNode:
        dc2_nodes = [n for n in self.tester.cluster.nodelist() if n.data_center == "DC2"]
        node = dc2_nodes[0]
        rf = kwargs.get("rf", 1)

        dc1_nodes = [n for n in self.tester.cluster.nodelist() if n.data_center == "DC1"]
        logger.debug(f"Wiping out node {node.name} keyspace from dc {node.data_center} with RF: {rf}")
        for dc, expect_success in [
            ("DC1", True),  # DC1 is safe to rebuild from
            ("DC2", rf > 1),  # DC2 is safe only if rf is greater than the number of lost nodes
            (None, True),  # Global rebuild (using all nodes to sync with) always succeeds
            ("force", True),  # nodetool should succeed with the --force option, but data verification would fail later
        ]:
            src_dc = "--force DC2" if dc == "force" else dc
            self.perform_rebuild(node, src_dc, expect_success=expect_success)
            if rf == 1 and dc == "force":
                with pytest.raises(Unavailable, match="Cannot achieve consistency level for cl QUORUM"):
                    self.tester.cluster.stop_nodes(dc1_nodes, wait_other_notice=True)
                    self.verify_data_integrity(node)
                    self.tester.cluster.start_nodes(dc1_nodes, wait_other_notice=True)
            else:
                self.verify_data_integrity(node)

        # return the node that was rebuilt
        return dc2_nodes[0]

    def perform_rebuild(self, node, source_dc, expect_success):
        wipe_node_keyspace_directory(node, "ks")
        node.start(wait_other_notice=True)
        rebuild_cmd = f"rebuild {source_dc}" if source_dc else "rebuild"

        if expect_success:
            node.nodetool(rebuild_cmd)
        else:
            with pytest.raises(NodetoolError):
                node.nodetool(rebuild_cmd)

    def bootstrap(self, dc: str | None = None, rack: str | None = None) -> ScyllaNode:
        logger.debug(f"Add new node in dc={dc} rack={rack}")
        new_node = self.add_node(dc=dc, rack=rack)
        logger.debug(f"Added new node {new_node.name} ({new_node.address()})")
        return new_node

    def removenode(self, wait_stop=True, **kwargs) -> ScyllaNode | None:
        removenode_reject_msg = r"Rejected removenode operation.*the node being removed is alive, maybe you should use decommission instead"
        remove_node = self.tester.cluster.nodelist()[-1]
        remove_node_host_id = remove_node.hostid()
        logger.debug(f"Stopping node {remove_node.name} (host id {remove_node_host_id})")

        marks = [(node, node.mark_log()) for node in self.tester.cluster.nodelist() if node.is_live() and node != remove_node]
        remove_node.stop(gently=False, wait_other_notice=wait_stop)
        logger.debug(f"Remove node {remove_node.name} (host id {remove_node_host_id})")
        if wait_stop:
            self.tester.cluster.nodelist()[0].removenode(remove_node_host_id)
            logger.debug(f"Node {remove_node.name} (host id {remove_node_host_id}) removed")
        else:
            with pytest.raises(NodetoolError) as nodetool_exc:
                self.tester.cluster.nodelist()[0].removenode(remove_node_host_id)
                if not re.match(removenode_reject_msg, nodetool_exc):
                    logger.error(f"Nodetool removenode failed with error: {nodetool_exc}")
                    raise nodetool_exc

            logger.debug(f"Nodetool removenode failed as expected: {nodetool_exc}")
            logger.debug(f"Waiting for all node to see {remove_node} as down")
            for node, mark in marks:
                node.watch_log_for_death(remove_node, from_mark=mark)
            logger.debug(f"Restarting {remove_node}")
            remove_node.start()

        return remove_node

    def removenode_rejecting(self, **kwargs) -> ScyllaNode | None:
        return self.removenode(wait_stop=False)

    def decommission(self, **kwargs) -> ScyllaNode | None:
        decommission_node = self.tester.cluster.nodelist()[-1]
        logger.debug(f"Decommission {decommission_node.name}")
        decommission_node.decommission()
        decommission_node.stop()
        logger.debug(f"Decommissioned {decommission_node.name}")

        return decommission_node

    def rebuild(self, **kwargs) -> ScyllaNode:
        rebuild_node = self.tester.cluster.nodelist()[0]
        logger.debug(f"Running nodetool rebuild on {rebuild_node.name}")
        rebuild_node.nodetool("rebuild")
        logger.debug("Rebuild completed successfully")

        return rebuild_node

    @staticmethod
    def validate_repair_by_scenario(node: ScyllaNode, repair_expected: bool, search_string: str, mark_log: int = 0):
        node_name = getattr(node, "name") or str(node)
        logger.debug(f"Validate that repair based node operation on the node {node_name}")
        found_expr = node.grep_log(expr=search_string, from_mark=mark_log)
        if repair_expected:
            assert found_expr, f"Repair based node ops was not started on the node {node_name} as expected"
        else:
            assert not found_expr, f"Repair based node ops was started on the node {node_name} unexpectedly"

    def run_scenarios(self, rbno_enabled: bool, lcs: bool = False, scenarios: list | None = None, **kwargs):
        scenarios = scenarios or self.operations_flow

        for scenario in scenarios:
            search_string = rf"repair_reason={scenario.operation_name},"
            mark_all_logs = [{"name": node.name, "mark": node.mark_log()} for node in self.tester.cluster.nodelist()]

            logger.debug(f"Start {scenario.operation_name} scenario")
            dc = kwargs["dc"] if "dc" in kwargs else None
            rack = kwargs["rack"] if "rack" in kwargs else None
            rf = kwargs["rf"] if "rf" in kwargs else None
            if rf == None:
                tested_node = scenario.operation(dc=dc, rack=rack)
            else:
                tested_node = scenario.operation(dc=dc, rack=rack, rf=rf)
            logger.debug(f"Scenario {scenario.operation_name} completed. Start validation")

            for node in self.tester.cluster.nodelist():
                if node == tested_node:
                    # If test runs with --enable-repair-based-node-ops is false, default behaviour will be changed and
                    # repair by node won't be started for any operation even for default "replace" operation
                    repair_expected = rbno_enabled if not rbno_enabled else scenario.repair_on_tested_node
                    self.validate_repair_by_scenario(node=tested_node, repair_expected=repair_expected, search_string=search_string)
                else:
                    repair_expected = rbno_enabled if not rbno_enabled else scenario.repair_on_all_nodes
                    mark_log = next((mark["mark"] for mark in mark_all_logs if mark["name"] == node.name), None)
                    if mark_log is None:
                        logger.debug(f"Node {node.name} was not found in the mark_all_logs. Newly replaced node, skip validation for this node")
                        continue
                    self.validate_repair_by_scenario(node=node, repair_expected=repair_expected, search_string=search_string, mark_log=mark_log)
            if lcs and scenario.operation_name in ["bootstrap", "replace"]:
                logger.debug("Validate LCS reshaping efficiency")
                assert tested_node.grep_log(r"LeveledManifest - Reshaping \d+ disjoint sstables in level 0 into level \d+"), "Reshaping was ran in inefficient way"

            if scenario.operation_name in ["decommission"]:
                self.tester.cluster.remove(node=tested_node, wait_other_notice=True, remove_node_dir=False)


@pytest.mark.next_gating
@pytest.mark.dtest_full
class TestRepairBasedNodeOperations(Tester):
    jvm_args = None

    def prepare_cluster(self, nodes, enable_repair_based_node_ops: bool | None = None, allowed_repair_based_node_ops: str | None = None, small_table_optimization_for_rbno_max_table_size: int | None = None):
        # TODO: remove when https://github.com/scylladb/scylla/issues/10138 will be solved
        self.ignore_log_patterns += ["Could not find CDC generation"]
        logger.debug("Starting cluster...")

        jvm_args = []
        if enable_repair_based_node_ops is not None:
            jvm_args.extend([f"--enable-repair-based-node-ops", str(enable_repair_based_node_ops).lower()])

        if allowed_repair_based_node_ops:
            jvm_args.extend([f"--allowed-repair-based-node-ops", allowed_repair_based_node_ops])

        self.jvm_args = jvm_args

        self.cluster.populate(nodes)
        if small_table_optimization_for_rbno_max_table_size is not None:
            # Set this via scylla.yaml rather than as a --command-line option: a
            # scylla binary that does not yet support this option only logs a
            # warning for an unknown yaml option and still boots, whereas an
            # unrecognised --command-line option makes scylla fail to start. On
            # such older binaries the user-table small table optimization does not
            # exist anyway, so the regular ranged repair path (and the reshape it
            # validates) is exercised regardless.
            self.cluster.set_configuration_options(values={"small_table_optimization_for_rbno_max_table_size": small_table_optimization_for_rbno_max_table_size})
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=jvm_args)

    def prepare_schema(self, node: ScyllaNode, rows: int = 1000, compaction_strategy="SizeTieredCompactionStrategy", rf=3):
        with self.patient_cql_connection(node) as session:
            create_ks(session, "ks", rf)
            create_cf(
                session=session,
                name="cf",
                read_repair=0.0,
                columns={"c1": "text", "c2": "text"},
                compaction_strategy=compaction_strategy,
            )

        logger.debug(f"Insert {rows} rows ...")
        with self.patient_exclusive_cql_connection(node, "ks") as session1:
            insert_c1c2(session1, keys=range(rows), consistency=ConsistencyLevel.ONE)

        for current_node in self.cluster.nodelist():
            current_node.flush()

    @staticmethod
    def get_cluster_topology(num_nodes: int, rf: int = 3) -> dict[str, dict[str, int]]:
        assert num_nodes >= rf
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=rf, nodes_per_rack=1, dc_name_prefix="DC", rack_name_prefix="RAC")
        if num_nodes > rf:
            cluster_topology["DC1"][f"RAC{rf}"] = cluster_topology["DC1"][f"RAC{rf}"] + num_nodes - rf
        return cluster_topology

    @pytest.mark.parametrize(
        "num_nodes",
        [
            pytest.param(3, marks=pytest.mark.required_features("!tablets")),
            pytest.param(4),
        ],
    )
    def test_disable_rbno(self, num_nodes, rf=3):
        """
        This test checks that if "--enable-repair-based-node-ops" is False, repair won't be started for all
        operations, supported by RBNO: bootstrap, replace, removenode, decommission and rebuild
        """
        enable_repair_based_node_ops = False
        cluster_topology = self.get_cluster_topology(num_nodes, rf)
        self.prepare_cluster(nodes=cluster_topology, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf=rf)

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, dc="DC1", rack="RAC1")

    # With tablets removenode is rejected if there is are no nodes to rebuild its tablets on
    @pytest.mark.required_features("!tablets")
    def test_enable_rbno_for_default_operation(self):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is not
        set, repair will run in case of "replace" operation only
        """
        enable_repair_based_node_ops = True
        allowed_repair_based_node_ops = "replace,removenode,rebuild,bootstrap,decommission"
        self.prepare_cluster(nodes=3, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops=allowed_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)

        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow)

    def test_enable_rbno_extra_nodes(self):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is not
        set, repair will run in case of "replace" operation only
        """
        enable_repair_based_node_ops = True
        allowed_repair_based_node_ops = "replace,removenode,rebuild,bootstrap,decommission"
        self.prepare_cluster(nodes={"DC1": {"RAC1": 1, "RAC2": 2}}, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops=allowed_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf=2)

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow, dc="DC1", rack="RAC1")

    def test_enable_rbno_extra_nodes_multi_rack(self):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is not
        set, repair will run in case of "replace" operation only
        """
        enable_repair_based_node_ops = True
        allowed_repair_based_node_ops = "replace,removenode,rebuild,bootstrap,decommission"
        self.prepare_cluster(nodes={"DC1": {"RAC1": 2, "RAC2": 2}}, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops=allowed_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": 2})

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow, dc="DC1", rack="RAC2")

    def test_enable_rbno_extra_nodes_multi_dc(self):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is not
        set, repair will run in case of "replace" operation only
        """
        enable_repair_based_node_ops = True
        allowed_repair_based_node_ops = "replace,removenode,rebuild,bootstrap,decommission"
        self.prepare_cluster(nodes={"DC1": {"RAC1": 1, "RAC2": 2}, "DC2": {"RAC1": 1, "RAC2": 2}}, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops=allowed_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": 2, "DC2": 2})

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow, dc="DC2")

    def test_enable_rbno_extra_nodes_multi_dc_rack(self):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is not
        set, repair will run in case of "replace" operation only
        """
        enable_repair_based_node_ops = True
        allowed_repair_based_node_ops = "replace,removenode,rebuild,bootstrap,decommission"
        self.prepare_cluster(nodes={"DC1": {"RAC1": 2, "RAC2": 2}, "DC2": {"RAC1": 2, "RAC2": 2}}, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops=allowed_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": 2, "DC2": 2})

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow, dc="DC2", rack="RAC2")

    @pytest.mark.parametrize(
        "num_nodes",
        [
            pytest.param(3, marks=pytest.mark.required_features("!tablets")),
            pytest.param(4),
        ],
    )
    def test_enable_rbno_for_bootstrap(self, num_nodes, rf=3):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is set
        to "bootstrap", repair will run in case of "bootstrap" operation only
        """
        enable_repair_based_node_ops = True
        cluster_topology = self.get_cluster_topology(num_nodes, rf)
        self.prepare_cluster(nodes=cluster_topology, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops="bootstrap")
        self.prepare_schema(node=self.cluster.nodelist()[0], rf=rf)

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        # Change default expected behaviour according to enable-repair-based-node-ops and -allowed-repair-based-node-ops
        for scenario in [rbnos.rebuild_scenario, rbnos.removenode_scenario, rbnos.replace_scenario, rbnos.decommission_scenario]:
            scenario.repair_on_tested_node, scenario.repair_on_all_nodes = False, False

        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow, dc="DC1", rack="RAC1")

    @pytest.mark.parametrize(
        "num_nodes",
        [
            pytest.param(3, marks=pytest.mark.required_features("!tablets")),
            pytest.param(4),
        ],
    )
    def test_disable_rbno_for_all_operations(self, num_nodes, rf=3):
        """
        By default, --allowed-repair-based-node-ops is set for all node operations

        This test checks that if "--enable-repair-based-node-ops" is False and "--allowed-repair-based-node-ops" is set
         to all supported operations, repair will not run during all operations
        """
        enable_repair_based_node_ops = False
        cluster_topology = self.get_cluster_topology(num_nodes, rf)
        self.prepare_cluster(nodes=cluster_topology, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops="bootstrap,replace,removenode,decommission,rebuild")
        self.prepare_schema(node=self.cluster.nodelist()[0], rf=rf)

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, dc="DC1", rack="RAC1")

    # Tablets do not require offstrategy compaction post migration/rebuild
    @pytest.mark.required_features("!tablets")
    def test_lcs_reshape_efficiency(self):
        """
        For repair-based bootstrap/replace, the input disjoint run is now efficiently reshaped into an ideal level L,
        so there's no compaction backlog once reshape completes.

        This behavior will manifest in the log as this:

            LeveledManifest - Reshaping 256 disjoint sstables in level 0 into level 2

        """
        enable_repair_based_node_ops = True
        # This test validates the efficient LCS reshape done by the regular ranged
        # repair path. The small table optimization for RBNO syncs the whole ring
        # as a single range instead, which does not exercise that reshape. The test
        # table is tiny (well under the 1 GiB default), so it would be auto-detected
        # as small and routed through the optimization. Setting the max table size
        # to 0 makes any user table with data fall back to the regular ranged
        # repair, while the always-optimized small system keyspaces keep their
        # default behavior. It is applied via scylla.yaml so binaries that predate
        # this option boot normally (and use the ranged path anyway).
        self.prepare_cluster(nodes=3, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops="bootstrap,replace", small_table_optimization_for_rbno_max_table_size=0)
        self.prepare_schema(node=self.cluster.nodelist()[0], compaction_strategy="LeveledCompactionStrategy")

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, lcs=True, scenarios=[rbnos.bootstrap_scenario, rbnos.replace_scenario])

    def test_ignore_dead_nodes_for_replace_option(self):
        """
        --ignore-dead-nodes-for-replace was added by commit
        https://github.com/scylladb/scylla/commit/eba4a4fba4e6f203742307c16448034f40713711

        This option allows ignoring dead nodes for replace operation.
        If this option is not set, replace operation will fail because one node is down.
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes={"DC1": {"RAC1": 2, "RAC2": 2, "RAC3": 3}}, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.replace_with_dead_nodes_scenario])

    @pytest.mark.skip_if(issue_open("scylladb/scylladb#16826"))
    def test_ignore_dead_nodes_for_whole_dc_replace_option(self):
        """
        1. create multi-dc cluster.
        2. stop all dc2 nodes.
        3. replace all dc2 nodes one by one.
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes={"DC1": {"RAC1": 2, "RAC2": 1}, "DC2": {"RAC1": 2, "RAC2": 1}, "DC3": 1}, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": 2, "DC2": 2, "DC3": 0})

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.replace_with_whole_dc_dead_nodes_scenario])

    @pytest.mark.parametrize(
        "num_nodes",
        [
            pytest.param(3, marks=pytest.mark.required_features("!tablets")),
            pytest.param(4),
        ],
    )
    def test_replace_node_after_cluster_restart(self, num_nodes, rf=3):
        """
        This option allows ignoring dead nodes for replace operation using a multi-rack, single-dc cluster.
        If this option is not set, replace operation will fail because one node is down.
        """
        enable_repair_based_node_ops = True
        cluster_topology = self.get_cluster_topology(num_nodes, rf)
        self.prepare_cluster(nodes=cluster_topology, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf=rf)

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.replace_dead_node_after_cluster_restart_scenario])

    def test_ignore_dead_nodes_for_replace_option_multi_dc(self):
        """
        This option allows ignoring dead nodes for replace operation using a multi-rack, single-dc cluster.
        If this option is not set, replace operation will fail because one node is down.
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes=[3, 2, 2], enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"dc1": 1, "dc2": 1, "dc3": 1})

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.replace_with_dead_nodes_after_cluster_restart_scenario])

    def test_ignore_dead_nodes_for_replace_option_multi_rack(self):
        """
        This option allows ignoring dead nodes for replace operation using a multi-rack, single-dc cluster.
        If this option is not set, replace operation will fail because one node is down.
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes={"DC1": {"RACK1": 3, "RACK2": 2, "RACK3": 2}}, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": 3})

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.replace_with_dead_nodes_after_cluster_restart_scenario])

    def test_removenode_rejected_with_rnbo(self):
        """
        if node marked as alive in gossiper, remove node
        should be rejected
        """
        enable_repair_based_node_ops = True
        cluster_topology = generate_cluster_topology(rack_num=3, nodes_per_rack=1)
        self.prepare_cluster(nodes=cluster_topology, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.removenode_rejecting_scenario])

    def test_replace_error_with_different_dc_or_rack(self):
        """
        Test that replace using a node on a different dc or rack fails.
        Reproduces scylladb/scylladb#16858
        """
        self.prepare_cluster(nodes={"DC1": {"RACK1": 1, "RACK2": 1}, "DC2": {"RACK1": 1, "RACK2": 1}}, enable_repair_based_node_ops=True)
        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": 2, "DC2": 2})

        cluster = self.cluster
        node1, node2, node3, node4 = cluster.nodelist()

        startup_error_pat = rf"Cannot replace node .* with a node on a different data center or rack"
        self.ignore_log_patterns.append(startup_error_pat)

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)

        def attempt_replace(replaced_node, new_dc, new_rack):
            logger.debug(f"Attempting to replace {replaced_node.name} in {replaced_node.data_center}/{replaced_node.rack} with a node in {new_dc}/{new_rack}")
            try:
                rbnos.replace_node(replaced_node=replaced_node, dc=new_dc, rack=new_rack)
                pytest.fail("Replace node succeeded unexpectedly")
            except Exception as e:  # noqa: BLE001
                error = f"{e}"
                if "The process is dead" not in error:
                    pytest.fail(f"Replace node failed with an unexpected error: {e}")
                logger.debug(f"Replace node failed as expected: {e}")
                log_pattern = rf"Startup failed:.*{startup_error_pat}.*Current location={replaced_node.data_center}/{replaced_node.rack}.*new location={new_dc}/{new_rack}"
                new_node = cluster.nodelist()[-1]
                assert new_node.grep_log(log_pattern), f"Expected following pattern in {new_node.name} log: {log_pattern}"
                replaced_node.start(wait_other_notice=True)

        attempt_replace(node1, new_dc="DC2", new_rack="RACK1")
        attempt_replace(node2, new_dc="DC1", new_rack="RACK1")
        attempt_replace(node3, new_dc="DC1", new_rack="RACK3")
        attempt_replace(node4, new_dc="DC3", new_rack="RACK3")

    @pytest.mark.skip_if(with_feature("tablets"), reason="Support rebuild with tablets: https://github.com/scylladb/scylladb/issues/17575#issuecomment-2172751170")
    @pytest.mark.parametrize("replication_factor", [1, 2, 3])
    @pytest.mark.unmark_if("next_gating", condition=issue_open("#16826"))
    def test_safe_unsafe_rebuild(self, replication_factor):
        """
        Node rebuild from safe and unsafe source in multi-dc cluster
        https://github.com/scylladb/scylla-dtest/issues/4481
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes={"DC1": 3, "DC2": 3}, enable_repair_based_node_ops=enable_repair_based_node_ops, allowed_repair_based_node_ops="bootstrap,replace,removenode,decommission,rebuild")

        # If audit keyspace exists (when audit is enabled), alter it to have replicas in both DCs
        # to avoid RBNO safety check failures during rebuild
        node = self.cluster.nodelist()[0]
        with self.patient_cql_connection(node) as session:
            audit_keyspace = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'audit'").one()
            if audit_keyspace:
                logger.debug(f"Audit keyspace detected, altering it to have RF={replication_factor} in both DC1 and DC2")
                session.execute(f"ALTER KEYSPACE audit WITH replication = {{'class': 'NetworkTopologyStrategy', 'DC1': '{replication_factor}', 'DC2': '{replication_factor}'}}")

        self.prepare_schema(node=self.cluster.nodelist()[0], rf={"DC1": replication_factor, "DC2": replication_factor})

        self.ignore_log_patterns.extend(
            [
                "raft_topology -.*stream_ranges.*failed",
            ]
        )

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=[rbnos.safe_unsafe_rebuild_scenario], rf=replication_factor)
