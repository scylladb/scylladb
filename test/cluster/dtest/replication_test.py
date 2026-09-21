import logging
from collections import OrderedDict

import pytest
from cassandra import InvalidRequest
from ccmlib.node import Node

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.data import rows_to_list

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestRFAutoExpand(Tester):
    """
    Test for #4210 (or CASSANDRA-14303).

    This tests the UX feature of expanding replication factor when using
    NetworkTopologyStrategy, e.g. when
    {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}
    is used for the replication option, it will be expanded to
    {'class': 'NetworkTopologyStrategy', dc1: 3, dc2: 3, ... }
    where dc1, dc2, ... are the datacenters known to the node
    at the moment of creating the keyspace.
    """

    # Using `replication_factor` in ALTER KEYSPACE is rejected by design with tablets.
    # See https://github.com/scylladb/scylladb/commit/b875151405bebc785ff9465bfc0bfb4ed3e72227
    @pytest.mark.required_features("!tablets")
    def test_rf_expand(self):
        self.cluster.populate([1, 1, 1]).start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        create_ks(session, "test_simple", {"replication_factor": 1})

        # simple expansion to all dcs
        res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_simple'")
        assert rows_to_list(res) == [[mk_replication({"dc1": 1, "dc2": 1, "dc3": 1})]]

        create_ks(session, "test_manual", {"replication_factor": 1, "dc3": 3})

        # expand, but respect factors specified manually
        res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_manual'")
        assert rows_to_list(res) == [[mk_replication({"dc1": 1, "dc2": 1, "dc3": 3})]]

        # expansion doesn't change existing replication factors
        session.execute("ALTER KEYSPACE test_manual WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
        res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_manual'")
        assert rows_to_list(res) == [[mk_replication({"dc1": 1, "dc2": 1, "dc3": 3})]]

    # ALTER KEYSPACE cannot switch between vnodes and tablets, and SimpleStrategy does not support tablets.
    @pytest.mark.required_features("!tablets")
    def test_rf_expand_on_switch(self):
        cluster_topology = generate_cluster_topology(dc_num=3, rack_num=1, nodes_per_rack=1, dc_name_prefix="dc")
        self.cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        create_ks(session, "test_switch", 3, replication_class="SimpleStrategy")

        # expand when directly switching from SimpleStrategy
        # to NetworkTopologyStrategy
        session.execute("ALTER KEYSPACE test_switch WITH replication = {'class': 'NetworkTopologyStrategy'}")
        res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_switch'")
        assert rows_to_list(res) == [[mk_replication({"dc1": 3, "dc2": 3, "dc3": 3})]]

        create_ks(session, "test_switch_2", 3, replication_class="SimpleStrategy", tablets=0)

        # don't expand when switching from SimpleStrategy
        # to NTS with manually specified DCs
        session.execute("ALTER KEYSPACE test_switch_2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}")
        res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_switch_2'")
        assert rows_to_list(res) == [[mk_replication({"dc1": 2})]]

        # expand non-specified factors
        session.execute("ALTER KEYSPACE test_switch_2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
        res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_switch_2'")
        assert rows_to_list(res) == [[mk_replication({"dc1": 2, "dc2": 3, "dc3": 3})]]

        # expand non-specified factors with NetworkTopologyStrategy
        create_ks(session, "test_switch_3", {"dc1": 1})
        if "tablets" in self.scylla_features:
            with pytest.raises(InvalidRequest, match=r'Error from server: code=2200 \[Invalid query\] message="\'replication_factor\' tag is not allowed when executing ALTER KEYSPACE with tablets'):
                session.execute("ALTER KEYSPACE test_switch_3 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
        else:
            session.execute("ALTER KEYSPACE test_switch_3 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
            res = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'test_switch_3'")
            assert rows_to_list(res) == [[mk_replication({"dc1": 1, "dc2": 3, "dc3": 3})]]


def mk_replication(dcs):
    return OrderedDict([("class", "org.apache.cassandra.locator.NetworkTopologyStrategy")] + [(str(k), str(v)) for k, v in dcs.items()])


@pytest.mark.dtest_full
class TestRestrictionReplicationSimpleStrategy(Tester):
    test_keyspace_ss = "test_ks_ss"
    test_keyspace_nts = "test_ks_nts"
    simple_strategy = "SimpleStrategy"
    network_topology_strategy = "NetworkTopologyStrategy"

    def prepare_one_node_cluster(self, option_value: str) -> Node:
        logger.debug("Preparing the cluster...")
        cluster = self.cluster
        cluster.set_configuration_options(values={"restrict_replication_simplestrategy": option_value})
        cluster.populate(1).start()
        logger.debug("Cluster has been prepared...")
        return cluster.nodelist()[0]

    @staticmethod
    def run_cqlsh_on_node(node_to_run_query: Node, query: str) -> tuple[str, str]:
        logger.debug('Running query "%s" on the node %s...', query, node_to_run_query.address())
        return node_to_run_query.run_cqlsh(query, return_output=True)

    def create_test_keyspace(self, node: Node, keyspace: str, replication_strategy: str) -> tuple[str, str]:
        logger.debug("Creating a new keyspace '%s' with replication strategy '%s'...", keyspace, replication_strategy)
        query = f"create keyspace {keyspace} with replication = {{'class': '{replication_strategy}', 'replication_factor' : 1}};"
        return self.run_cqlsh_on_node(node_to_run_query=node, query=query)

    def alter_test_keyspace(self, node: Node, keyspace: str, replication_strategy: str) -> tuple[str, str]:
        logger.debug("Altering the new keyspace '%s' to use replication strategy '%s'...", keyspace, replication_strategy)
        query = f"alter keyspace {keyspace} with replication = {{'class': '{replication_strategy}', 'replication_factor' : 1}};"
        return self.run_cqlsh_on_node(node_to_run_query=node, query=query)

    def describe_keyspace(self, node: Node, keyspace: str) -> tuple[str, str]:
        query = f"describe keyspace {keyspace};"
        return self.run_cqlsh_on_node(node_to_run_query=node, query=query)
