import typing

from cassandra import ConsistencyLevel

__ALL__ = "ClusterState"

# ruff: noqa: N802


class NodeState:
    """
    This class represents scylla cluster node state
    Used for state-aware conditional randomization of actions performed on the node and cluster

    Every operation on the NodeState should be done via ClusterState, i.e. No direct use

    """

    UP: bool = True
    RUNNING: bool = True
    DRAINED: bool = False
    DECOMMISSIONED: bool = False
    HOLDS_UPTODATE_DATA: bool = False
    HOLDS_UPTODATE_SCHEMA: bool = False
    DONT_TOUCH: bool = False
    SEED: bool = False
    node_id = None
    auto_bootstrap = False

    def __init__(self, node_id, auto_bootstrap=None, is_seed=None):
        self.node_id = node_id
        if auto_bootstrap is not None:
            self.auto_bootstrap = auto_bootstrap
        if self.auto_bootstrap:
            self.HOLDS_UPTODATE_DATA = True
            self.HOLDS_UPTODATE_SCHEMA = True
        if is_seed is not None:
            self.SEED = is_seed

    @property
    def REMOVABLE(self):
        return self.UP and not self.DONT_TOUCH

    @property
    def DECOMMISSIONABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def DRAINABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def REBOOTABLE(self):
        return self.UP and not self.DONT_TOUCH

    @property
    def REPAIRABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def REBUILDABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def MOVEABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def STOPABLE(self):
        return self.UP and not self.DONT_TOUCH

    @property
    def STARTABLE(self):
        return not self.UP and not self.DONT_TOUCH

    @property
    def COMPACTABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def CLEANUPABLE(self):
        return self.UP and self.RUNNING and not self.DONT_TOUCH

    @property
    def FLUSHABLE(self):
        return self.UP and not self.DONT_TOUCH

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def drain(self):
        self.DRAINED = True
        self.RUNNING = False
        self.HOLDS_UPTODATE_DATA = False
        self.HOLDS_UPTODATE_SCHEMA = False

    def reboot(self):
        self.DRAINED = False
        self.RUNNING = True
        if self.auto_bootstrap:
            self.HOLDS_UPTODATE_DATA = True
            self.HOLDS_UPTODATE_SCHEMA = True

    def repair(self):
        self.HOLDS_UPTODATE_DATA = True
        self.HOLDS_UPTODATE_SCHEMA = True

    def rebuild(self):
        self.HOLDS_UPTODATE_DATA = True
        self.HOLDS_UPTODATE_SCHEMA = True

    def stop(self):
        self.UP = False
        self.RUNNING = False
        self.DRAINED = False
        self.HOLDS_UPTODATE_DATA = False
        self.HOLDS_UPTODATE_SCHEMA = False

    def set_dont_touch_mark(self):
        self.DONT_TOUCH = True

    def remove_dont_touch_mark(self):
        self.DONT_TOUCH = False

    def start(self):
        self.UP = True
        self.RUNNING = True
        if self.auto_bootstrap:
            self.HOLDS_UPTODATE_DATA = True
            self.HOLDS_UPTODATE_SCHEMA = True

    def cleanup(self):
        pass

    def compact(self):
        pass

    def flush(self):
        pass

    def decommission(self):
        self.RUNNING = False
        self.DECOMMISSIONED = True
        self.HOLDS_UPTODATE_DATA = False
        self.HOLDS_UPTODATE_SCHEMA = False

    def move(self):
        self.HOLDS_UPTODATE_DATA = False

    def resetlocalschema(self):
        self.HOLDS_UPTODATE_SCHEMA = True


class ClusterState:
    """
    This class represents scylla cluster state
    Used for state-aware conditional randomization of actions performed on the cluster

    The main purpose of this class if to provide a framework to avoid running into faulty states
      of the real scylla cluster before real cluster is even initiated,
      on the stage of the planning what actions are going to be performed.
    """

    _nodes: list[NodeState]
    _initial_node_count = None
    _rf = None
    _min_node_count = None
    _max_node_count = None
    _loader_consistency_level = None
    _db_configuration = {}

    def __init__(  # noqa: PLR0913
        self, initial_node_count, rf, min_node_count, max_node_count, loaders_consistency_level, db_configuration
    ):
        # TBD: Add hinted handoff logic, https://docs.scylladb.com/architecture/anti-entropy/hinted-handoff/
        self._rf = rf
        if db_configuration:
            self._db_configuration = db_configuration
        self._initial_node_count = initial_node_count
        self._min_node_count = min_node_count
        self._max_node_count = max_node_count
        # Loaders consistency_level needed to avoid states when data could be lost or
        # loader traffic is blocked due to lack of nodes
        self._loader_consistency_level = loaders_consistency_level
        # Controls how many nodes was removed without decommission/stopped, to make sure that no data lost is possible
        self._unavailable_nodes_with_lost_data = 0
        # Used to calculate global node index, so that every node could referred by the uniq number
        # Also allows silently avoid problem in ccm cluster,
        #  when it fails on adding node if node have same index/name as one that has been just removed
        self._next_node_idx = 0
        self._nodes = []
        for _ in range(self._initial_node_count):
            self.add_node(auto_bootstrap=True, is_seed=True)

    @staticmethod
    def _get_node_count_from_cl_and_rf(rf, cl):
        """
        Return amount of nodes needed to accommodate Consistency level requirements
        """
        if cl == ConsistencyLevel.QUORUM:
            return rf // 2 + 1
        if cl == ConsistencyLevel.ONE:
            return 1
        if cl == ConsistencyLevel.TWO:
            return 2
        if cl == ConsistencyLevel.THREE:
            return 3
        if cl == ConsistencyLevel.ALL:
            return rf
        if cl == ConsistencyLevel.SERIAL:
            return rf // 2 + 1

    def get_removable_nodes(self, seed_nodes: bool = False, decomissioned=False):
        """
        Returns ids of nodes that could be removed from the cluster
        """
        # Decommissioned nodes could be removed with no repercussions
        if seed_nodes is True:
            seed_status = ["SEED"]
        elif seed_nodes is False:
            seed_status = ["!SEED"]
        else:
            seed_status = []
        if decomissioned:
            result = self.get_node_ids_by_status("DECOMMISSIONED", "!DONT_TOUCH")
        else:
            result = []
        # We can't remove nodes anymore If reached limit of nodes that are not running, but could have data
        if self._check_if_data_could_be_lost_due_to_node_removal():
            return result
        if self._check_if_there_are_enough_nodes_that_holds_data():
            result.extend(self.get_node_ids_by_status("UP", "RUNNING", "!DONT_TOUCH", "HOLDS_UPTODATE_DATA", "HOLDS_UPTODATE_SCHEMA", *seed_status))
        # Add running nodes, but only if running nodes in the cluster
        #   more than needed for loaders to operate on given consistency_level level
        if not self._check_if_loaders_could_stop_working():
            result.extend(self.get_node_ids_by_status("UP", "RUNNING", "!DONT_TOUCH", "!HOLDS_UPTODATE_DATA", "!HOLDS_UPTODATE_SCHEMA", *seed_status))
        # Add not running nodes
        result.extend(self.get_node_ids_by_status("UP", "!RUNNING", "!DONT_TOUCH", *seed_status))
        return list(set(result))

    def get_decommissionable_nodes(self):
        """
        Returns ids of nodes that could be decommissioned from the cluster
        """
        # We can't decomission nodes anymore If reached limit of nodes that are not running, but could have data
        if self._check_if_data_could_be_lost_due_to_node_removal():
            return []
        # We can remove any running node, but only if running nodes in the cluster more than
        #   needed for loaders to operate on given consistency_level level
        if self._check_if_loaders_could_stop_working():
            return []
        return self.get_node_ids_by_status("DECOMMISSIONABLE", "!SEED")

    def get_rebootable_nodes(self):
        """
        Returns ids of nodes that could be rebooted
        """
        # We can reboot any node only if running nodes in the cluster more than needed for
        #   loaders to operate on given consistency_level level
        if self._check_if_loaders_could_stop_working():
            return []
        return self.get_node_ids_by_status("STOPABLE", "!SEED")

    def get_stoppable_nodes(self):
        """
        Returns ids of nodes that could be stopped
        """
        # We can't stop nodes anymore If reached limit of nodes that are not running, but could have data
        if self._check_if_data_could_be_lost_due_to_node_removal():
            return []
        result = self.get_node_ids_by_status("UP", "RUNNING", "!DONT_TOUCH")
        if len(result) - self._get_node_count_from_cl_and_rf(self._rf, self._loader_consistency_level):
            return result
        return []

    def _check_if_data_could_be_lost_due_to_node_removal(self):
        return self._unavailable_nodes_with_lost_data + 1 >= self._get_node_count_from_cl_and_rf(self._rf, self._loader_consistency_level)

    def _check_if_loaders_could_stop_working(self):
        running_nodes = self.get_node_ids_by_status("UP", "RUNNING")
        return len(running_nodes) <= self._get_node_count_from_cl_and_rf(self._rf, self._loader_consistency_level)

    def _check_if_there_are_enough_nodes_that_holds_data(self):
        nodes_that_holds_data = self.get_node_ids_by_status("UP", "RUNNING", "HOLDS_UPTODATE_DATA", "HOLDS_UPTODATE_SCHEMA")
        return len(nodes_that_holds_data) >= self._get_node_count_from_cl_and_rf(self._rf, self._loader_consistency_level)

    def can_add_node(self):
        return bool(self._max_node_count - self.count_node_states_by_status("ANY"))

    def add_node(self, auto_bootstrap=False, is_seed=False):
        self._next_node_idx += 1
        node_state = NodeState(self._next_node_idx, auto_bootstrap=auto_bootstrap, is_seed=is_seed)
        self._nodes.append(node_state)
        return node_state.node_id

    def get_next_node_id(self):
        return self._next_node_idx + 1

    def remove_node(self, node_id):
        to_be_removed = None
        for node_state in self._nodes:
            if node_state.node_id == node_id:
                to_be_removed = node_state
                break
        if not to_be_removed:
            raise RuntimeError(f"There is no such node {node_id}")
        if not to_be_removed.DECOMMISSIONED:
            self._unavailable_nodes_with_lost_data += 1
        self._nodes.remove(to_be_removed)
        return to_be_removed

    def get_nodes_count(self):
        return len(self._nodes)

    def get_node_state_by_node_id(self, node_id):
        for node in self._nodes:
            if node.node_id == node_id:
                return node
        raise RuntimeError(f"There is no such node {node_id}")

    def get_node_ids_by_status(self, *statuses):
        return self._get_node_states_by_status(statuses, return_node_ids=True)

    def _get_node_states_by_status(self, statuses, return_node_ids=True):
        if not isinstance(statuses, list | tuple):
            statuses = [statuses]
        result = []
        for node_num, node_state in enumerate(self._nodes):
            match = True
            for status in statuses:
                if status[0] == "!":
                    expected = False
                    status = status[1:]  # noqa: PLW2901
                elif status == "ANY":
                    match = True
                    break
                else:
                    expected = True
                if node_state[status] != expected:
                    match = False
                    break
            if match:
                if return_node_ids:
                    result.append(node_state.node_id)
                else:
                    result.append(node_state)
        return result

    def count_node_states_by_status(self, *statuses):
        return len(self._get_node_states_by_status(statuses, return_node_ids=True))

    def drain(self, node_id):
        self._unavailable_nodes_with_lost_data += 1
        self.get_node_state_by_node_id(node_id).drain()

    def reboot_node(self, node_id):
        self.get_node_state_by_node_id(node_id).reboot()

    def repair(self, node_id):
        self.get_node_state_by_node_id(node_id).repair()

    def rebuild(self, node_id):
        self._unavailable_nodes_with_lost_data -= 1
        self.get_node_state_by_node_id(node_id).rebuild()

    def set_dont_touch_mark(self, node_id):
        self.get_node_state_by_node_id(node_id).set_dont_touch_mark()

    def remove_dont_touch_mark(self, node_id):
        self.get_node_state_by_node_id(node_id).remove_dont_touch_mark()

    def start_node(self, node_id):
        self._unavailable_nodes_with_lost_data -= 1
        node_state = self.get_node_state_by_node_id(node_id)
        if self._db_configuration.get("hinted_handoff_enabled", False):
            node_state.HOLDS_UPTODATE_DATA = True
            node_state.HOLDS_UPTODATE_SCHEMA = True
        node_state.start()

    def stop_node(self, node_id):
        self.get_node_state_by_node_id(node_id).stop()
        self._unavailable_nodes_with_lost_data += 1

    def cleanup(self, node_id):
        self.get_node_state_by_node_id(node_id).cleanup()

    def compact(self, node_id):
        self.get_node_state_by_node_id(node_id).compact()

    def flush(self, node_id):
        self.get_node_state_by_node_id(node_id).flush()

    def decommission(self, node_id):
        self.get_node_state_by_node_id(node_id).decommission()

    def move(self, node_id):
        self.get_node_state_by_node_id(node_id).move()

    def resetlocalschema(self, node_id):
        self.get_node_state_by_node_id(node_id).resetlocalschema()
