# The idea of a randomized test is to create a random,
# yet deterministic sequence of nodetool actions and perform
# these actions in presence of LWT workload. Currently we don't
# validate the workload, simply observe there are no failures or
# crashes.
# Since some nodetool actions are not possible over an arbitrary
# cluster state, e.g. it's impossible to repair a node immediately
# after a drain, or remove more nodes than quorum/2 - 1 at the same
# time, in order to construct a random sequence of actions we
# also check the (projected) cluster and node states.
# Actions, their impact on the cluster, serialization and deserialization
# infrastructure used to make the tests repeatable is implemented
# in 'dsr' library - abbreviation from deterministic state-aware randomness.
from random import choice

import pytest
from cassandra import ConsistencyLevel

from dsr.loaders.intkeyloaders import IntKeyLoader
from dsr.scylla_cluster.actions import (
    AddNode,
    CompactNode,
    DecommissionNode,
    DecommissionRemoveNode,
    DrainNode,
    FlushNode,
    RebootNode,
    RebuildNode,
    RemoveNode,
    RepairNode,
    ReplaceNode,
    StartNode,
    StopNode,
)
from dsr.scylla_cluster.cluster import ScyllaClusterTest
from dtest_class import Tester
from dtest_setup import DTestSetup
from tools.marks import unmark


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestRandomPaxos(Tester):
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item, call):
        """
        Needed to see python code that will reproduce the test, if it has failed.
        In order to reproduce the test, take code you see in the error message,
          put in into the test file and run the test case.
        """

        outcome = yield
        report = outcome.get_result()
        test_fn = item.obj
        if hasattr(self, "test_info"):
            docstring = f""" In order to reproduce it add following code into test class body and run it:
                def test_fail_repro(self):
                    self.test_info = ScyllaClusterTest.load({self.test_info.save()!r})
                    self.test_info.randomize()
                    self.test_info.execute(tester=self)
            """
        else:
            docstring = getattr(test_fn, "__doc__")

        if docstring:
            report.nodeid = docstring

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    def test_topology_add_decommission_reboot(self, request: pytest.FixtureRequest):
        """
        Test on add, decommission and reboot node
        """
        test_info = ScyllaClusterTest(
            debug=True,
            action_variants=[DecommissionRemoveNode, AddNode(auto_bootstrap=True), RebootNode(gently=True)],
            create_keyspace_stmt="CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor': 3}",
            create_table_stmt="CREATE TABLE ks.test (k int PRIMARY KEY, v int)",
            action_count=5,
            initial_node_count=3,
            min_node_count=2,
            max_node_count=8 if self.cluster.scylla_mode != "debug" else 5,
            sleep_time=10,
            loader=IntKeyLoader(consistency_level=ConsistencyLevel.QUORUM, insert="INSERT INTO ks.test(k,v) VALUES (?, ?) IF NOT EXISTS", update="UPDATE ks.test SET v = ? WHERE k=? IF EXISTS"),
            db_configuration={"rf_rack_valid_keyspaces": False},
        )
        request.addfinalizer(test_info.cleanup)
        test_info.randomize()
        test_info.execute(tester=self)

    # Reason for unmark: this test ran more than 40 minutes on several occasions
    # this test is very heavy and might fail in debug, running in dev and release mode gives us a good picture of this scenario
    @unmark.next_gating
    @pytest.mark.scylla_mode("!debug")
    def test_topology_grow(self, request: pytest.FixtureRequest):
        """
        Test on add nodes to the cluster, covers following cases:
        1. Adding nodes when cluster has RF nodes
        2. Adding nodes in case of existing less than QUORUM nodes
        3. Adding nodes after decommissions
        4. Adding nodes after seed node decommissions
        """
        grow_after_decommission = choice([True, False])
        initial_node_count = 3
        action_count = 3
        if grow_after_decommission:
            decommission_seed = choice([True, False])
            consistency_level = ConsistencyLevel.QUORUM
            if decommission_seed:
                initial_node_count = 4
                initial_actions = [DecommissionRemoveNode(node_id=3)]
                action_count = 4
            else:
                initial_actions = [AddNode(node_id=4), DecommissionRemoveNode(node_id=4)]
                action_count = 5
        else:
            initial_actions = []
            consistency_level = choice([ConsistencyLevel.QUORUM, ConsistencyLevel.ALL])

        test_info = ScyllaClusterTest(
            debug=True,
            action_variants=[AddNode(auto_bootstrap=True)],
            create_keyspace_stmt="CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor': 3}",
            create_table_stmt="CREATE TABLE ks.test (k int PRIMARY KEY, v int)",
            action_count=action_count,
            initial_node_count=initial_node_count,
            min_node_count=3,
            max_node_count=8 if self.cluster.scylla_mode != "debug" else 5,
            sleep_time=10,
            loader=IntKeyLoader(
                # Variate consistency level in order to see if loader traffic is be processed
                #   correctly when consistency requirements is not meet
                consistency_level=consistency_level,
                insert="INSERT INTO ks.test(k,v) VALUES (?, ?) IF NOT EXISTS",
                update="UPDATE ks.test SET v = ? WHERE k=? IF EXISTS",
            ),
            actions=initial_actions,
            db_configuration={"rf_rack_valid_keyspaces": False},
        )
        request.addfinalizer(test_info.cleanup)
        test_info.randomize()
        test_info.execute(tester=self)

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    def test_topology_replace(self, request: pytest.FixtureRequest):
        """
        Test on node replacing
        """
        # TBD: To be fixed for seed nodes
        test_info = ScyllaClusterTest(
            debug=True,
            action_variants=[ReplaceNode(is_seed=False)],
            create_keyspace_stmt="CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor': 3}",
            create_table_stmt="CREATE TABLE ks.test (k int PRIMARY KEY, v int)",
            action_count=5,
            initial_node_count=3,
            min_node_count=3,
            max_node_count=8 if self.cluster.scylla_mode != "debug" else 5,
            sleep_time=10,
            loader=IntKeyLoader(consistency_level=ConsistencyLevel.QUORUM, insert="INSERT INTO ks.test(k,v) VALUES (?, ?) IF NOT EXISTS", update="UPDATE ks.test SET v = ? WHERE k=? IF EXISTS"),
            actions=[
                AddNode(node_id=4, auto_bootstrap=True),
                AddNode(node_id=5, auto_bootstrap=True),
            ],
            db_configuration={"rf_rack_valid_keyspaces": False},
        )
        request.addfinalizer(test_info.cleanup)

        test_info.randomize()
        test_info.execute(tester=self)
