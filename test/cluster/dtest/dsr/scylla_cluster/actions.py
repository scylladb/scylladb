import random
import time

from dsr.base.dsr_entity import DSREntity
from dsr.scylla_cluster.cluster import ClusterState, ScyllaClusterTest


class ClusterActionBase(DSREntity):
    chain: ScyllaClusterTest = None

    def __init__(self, chain: ScyllaClusterTest = None, **kwargs):
        if chain:
            self.chain = chain
        super().__init__(**kwargs)

    def _wait_till_nodes_are_up(self, tester, nodes):
        for node in nodes:
            for _ in range(10):
                try:
                    tester.cluster.patient_cql_connection(node).execute("USE system")
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.5)


class NodeAction(ClusterActionBase):
    node_id = None
    mark_node_untouchable = 0
    _target_node_status = None

    def randomize(self):
        state_context = {}
        for param_name, param_value in self.__dict__.items():
            if param_name[0] == "_" or param_name in ["node_id", "mark_node_untouchable"]:
                continue
            state_context[param_name] = param_value
        self.node_id = random.choice(self.get_target_nodes(self.chain.state, **state_context))
        self.change_cluster_state(self.chain.state, self.node_id, **state_context)
        if self.mark_node_untouchable == -1:
            self.chain.state.get_node_state_by_node_id(self.node_id).remove_dont_touch_mark()
        elif self.mark_node_untouchable == 1:
            self.chain.state.set_dont_touch_mark(self.node_id)

    def get_probability_coeff(self) -> int:
        if self.get_target_nodes(self.chain.state):
            return 1
        return 0

    def execute(self, tester):
        target_node = self.chain.get_node_by_id(self.node_id, tester)
        self.on_before(tester, target_node)
        self.perform_action(tester, target_node)
        self.on_after(tester, target_node)

    def on_before(self, tester, node):
        pass

    def on_after(self, test, node):
        pass

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        """
        This routing should contains logic to apply action to the cluster state
        should operate only on the ClusterState instance
        """
        raise NotImplementedError(f"Not implemented for class {cls.__name__}")

    def reapply_cluster_state(self):
        self.change_cluster_state(self.chain.state, self.node_id)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **state_context):
        """
        This routing should contains logic to apply action to the cluster state
        should operate only on the ClusterState instance
        """
        raise NotImplementedError(f"Not implemented {cls.__name__}")

    def perform_action(self, tester, target_node):
        raise NotImplementedError(f"Not implemented {self.__class__.__name__}")


class RemoveNode(NodeAction):
    def perform_action(self, tester, target_node):
        tester.cluster.remove(target_node)
        self.chain.stop_loader(target_node)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.remove_node(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_removable_nodes(decomissioned=True)


class StopNode(NodeAction):
    """
    Not tested
    """

    gently: bool = False
    wait_other_notice: bool = True
    wait: bool = True

    def perform_action(self, tester, target_node):
        target_node.stop(gently=self.gently, wait=self.wait, wait_other_notice=self.wait_other_notice)
        self.chain.stop_loader(target_node)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.stop_node(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_stoppable_nodes()


class StartNode(NodeAction):
    """
    Not tested
    """

    wait_for_binary_proto: bool = True
    wait_other_notice: bool = True

    def perform_action(self, tester, target_node):
        self.chain.start_loader(target_node, tester)
        target_node.start(
            wait_for_binary_proto=self.wait_for_binary_proto,
            wait_other_notice=self.wait_other_notice,
        )

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.start_node(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_node_ids_by_status("STARTABLE")


class RebootNode(NodeAction):
    """
    Not tested
    """

    gently: bool = False
    wait_for_binary_proto: bool = True
    wait_other_notice: bool = True
    wait: bool = True

    def perform_action(self, tester, target_node):
        target_node.stop(gently=self.gently, wait=self.wait, wait_other_notice=self.wait_other_notice)
        target_node.start(
            wait_for_binary_proto=self.wait_for_binary_proto,
            wait_other_notice=self.wait_other_notice,
        )

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.reboot_node(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_rebootable_nodes()


class RepairNode(NodeAction):
    """
    Not tested
    """

    args: list = []

    def perform_action(self, tester, target_node):
        target_node.repair(self.args)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.repair(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        if cluster_state.get_node_ids_by_status("!RUNNING"):
            return []
        return cluster_state.get_node_ids_by_status("REPAIRABLE")


class RebuildNode(NodeAction):
    """
    Not tested
    """

    args: list = []

    def perform_action(self, tester, target_node):
        target_node.nodetool("rebuild " + " ".join(self.args))

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.rebuild(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_node_ids_by_status("REBUILDABLE")


class CompactNode(NodeAction):
    """
    Not tested
    """

    args: list = []

    def perform_action(self, tester, target_node):
        target_node.compact()

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.compact(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_node_ids_by_status("UP")


class FlushNode(NodeAction):
    """
    Not tested
    """

    def perform_action(self, tester, target_node):
        target_node.flush()

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.flush(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_node_ids_by_status("FLUSHABLE")


class DecommissionNode(NodeAction):
    """
    Not tested
    """

    def perform_action(self, tester, target_node):
        target_node.decommission()

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.decommission(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_decommissionable_nodes()


class DrainNode(NodeAction):
    """
    Not tested
    """

    wait: bool = True

    def perform_action(self, test, target_node):
        target_node.drain(block_on_log=self.wait)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.drain(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_node_ids_by_status("DRAINABLE")


class DrainRestartNode(NodeAction):
    """
    Not tested
    """

    wait: bool = True
    gently: bool = True
    wait_other_notice: bool = True
    wait_for_binary_proto: bool = True

    def perform_action(self, tester, target_node):
        target_node.drain(block_on_log=self.wait)
        target_node.stop(gently=self.gently, wait=self.wait, wait_other_notice=self.wait_other_notice)
        target_node.start(
            wait_for_binary_proto=self.wait_for_binary_proto,
            wait_other_notice=self.wait_other_notice,
        )

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.decommission(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_node_ids_by_status("DRAINABLE", "REBOOTABLE")


class DecommissionRemoveNode(RemoveNode):
    def on_before(self, tester, node):
        node.decommission()

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, **extra_context):
        cluster_state.decommission(node_id)
        cluster_state.remove_node(node_id)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        return cluster_state.get_removable_nodes(decomissioned=False)


class AddNode(NodeAction):
    auto_bootstrap = False
    wait_other_notice = True
    is_seed: bool = False
    # TBD: when adding seed node it is needed to update seed node list on every node with scylla restart

    def perform_action(self, tester, target_node):
        new_node = tester.cluster.new_node(self.node_id, self.auto_bootstrap, is_seed=self.is_seed)
        new_node.start(wait_for_binary_proto=True, wait_other_notice=self.wait_other_notice)
        self._wait_till_nodes_are_up(tester, [new_node])
        self.chain.start_loader(new_node, tester)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, auto_bootstrap: bool = False, is_seed: bool = False, **extra_context):
        cluster_state.add_node(auto_bootstrap=auto_bootstrap, is_seed=is_seed)

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        if cluster_state.can_add_node():
            return [cluster_state.get_next_node_id()]
        return []

    def execute(self, tester):
        self.on_before(tester, None)
        self.perform_action(tester, None)
        self.on_after(tester, None)


class ReplaceNode(NodeAction):
    replaced_node_id = None
    wait_other_notice = True
    is_seed: bool = False
    gently: bool = False
    wait: bool = True

    @classmethod
    def get_target_nodes(cls, cluster_state: ClusterState, **extra_context):
        if cluster_state.can_add_node():
            return [cluster_state.get_next_node_id()]
        return []

    @classmethod
    def get_replace_nodes(cls, cluster_state: ClusterState, is_seed: bool):
        return cluster_state.get_removable_nodes(decomissioned=False, seed_nodes=is_seed)

    def randomize(self):
        self.node_id = random.choice(self.get_target_nodes(self.chain.state))
        self.replaced_node_id = random.choice(self.get_replace_nodes(self.chain.state, is_seed=self.is_seed))
        self.change_cluster_state(self.chain.state, node_id=self.node_id, is_seed=self.is_seed, replaced_node_id=self.replaced_node_id)
        if self.mark_node_untouchable == -1:
            self.chain.state.get_node_state_by_node_id(self.node_id).remove_dont_touch_mark()
        elif self.mark_node_untouchable == 1:
            self.chain.state.set_dont_touch_mark(self.node_id)

    def get_probability_coeff(self) -> int:
        if self.get_target_nodes(self.chain.state) and self.get_replace_nodes(self.chain.state, is_seed=self.is_seed):
            return 1
        return 0

    def execute(self, tester):
        replaced_node = self.chain.get_node_by_id(self.replaced_node_id, tester)
        replaced_node.stop(gently=self.gently, wait=self.wait, wait_other_notice=self.wait_other_notice)
        self.chain.stop_loader(replaced_node)
        new_node = tester.cluster.new_node(self.node_id, auto_bootstrap=True, is_seed=False)
        if self.is_seed:
            # TBD: Don't work, to be fixed for seed nodes
            new_node.start(wait_for_binary_proto=True, wait_other_notice=self.wait_other_notice, replace_node_host_id=replaced_node.hostid())
            self._wait_till_nodes_are_up(tester, [new_node])
            new_node.stop(gently=self.gently, wait=self.wait, wait_other_notice=self.wait_other_notice)
            new_node.set_configuration_options(values={"auto_bootstrap": ""})
            new_node.start(replace_node_host_id=replaced_node.hostid())
        else:
            new_node.start(wait_for_binary_proto=True, wait_other_notice=self.wait_other_notice, replace_node_host_id=replaced_node.hostid())
        self.chain.start_loader(new_node, tester)
        tester.cluster.remove(replaced_node)

    def reapply_cluster_state(self):
        self.change_cluster_state(self.chain.state, self.node_id)

    @classmethod
    def change_cluster_state(cls, cluster_state: ClusterState, node_id: int, is_seed: bool = False, replaced_node_id: int | None = None, **extra_context):
        cluster_state.add_node(auto_bootstrap=True, is_seed=is_seed)
        cluster_state.remove_node(replaced_node_id)
