#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from ccmlib.node import NodetoolError
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_cf, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.schema import decrease_rf, describe_rf, get_replication_options

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
class TestRebuild(Tester):
    """Rebuild tests ported from scylla-dtest's rebuild_test.py.

    Kept as a separate module from test/cluster/dtest/rebuild_test.py, which is an
    unrelated in-tree test (test_rebuild_stream_abort_repro) that happens to share the
    filename with the dtest source; the two files are not the same test suite.
    """

    def add_node(self, i, dc="dc1", rack=None):
        return self.cluster.new_node(i, debug=True, data_center=dc, rack=rack)

    def _check_data(self, session, keyspaces, tables, keys, cl=ConsistencyLevel.ALL):
        logger.debug("Checking data")
        total = 0
        errors = 0
        for ks in keyspaces:
            for cf in tables:
                for i in keys:
                    total += 1
                    try:
                        query_c1c2(session, i, ks=ks, cf=cf, consistency=cl)
                    except AssertionError:
                        errors += 1
        assert errors == 0, f"Found {errors} errors out of {total} keys"

    def test_simple_rebuild(self):
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """
        keys = 25 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10

        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})
        node1 = self.add_node(1, "dc1")

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, "ks", {"dc1": 1})
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
        logger.debug(f"Inserting {keys} keys")
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        # check data
        for i in range(keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = self.add_node(2, "dc2")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        repl = get_replication_options(session, "ks")
        # With tablets, ALTER KEYSPACE returns only after the data is streamed to dc2.
        # This took ~30s on a slow CI machine, so use a 10x margin over that.
        session.execute(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class':'NetworkTopologyStrategy', 'dc1':{repl['dc1']}, 'dc2':1 }};", timeout=300)
        session.execute("USE ks")

        self.rebuild_errors = 0
        self.unexpected_errors = 0

        # rebuild dc2 from dc1
        def rebuild():
            try:
                logger.debug("Running nodetool rebuild dc1 on node2")
                node2.nodetool("rebuild dc1")
                logger.debug("Rebuild completed successfully")
            except NodetoolError as e:
                if "rebuild is in progress" in str(e):
                    logger.debug("Rebuild is in progress")
                    self.rebuild_errors += 1
                else:
                    logger.debug(f"Unexpected rebuild failure {e!s}")
                    self.unexpected_errors += 1

        rebuild()

        # check data
        logger.debug("Verfiying data")
        for i in range(keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)

    @pytest.mark.required_features("!tablets")  # This test switches between strategies, but tablets are for NetworkTopologyStrategy only
    def test_rebuild_everywhere(self):  # noqa: PLR0915
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """
        keys = 25000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000

        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})
        node1 = self.add_node(1, "dc1")

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, "ks", {"dc1": 1})
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
        logger.debug(f"Inserting {keys} keys")
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        # check data
        for i in range(keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        logger.debug("Bootstraping node2 in dc1 with auto_bootstrap: false")
        node2 = self.add_node(2, "dc1")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Bootstraping node3 in dc2 with auto_bootstrap: false")
        node3 = self.add_node(3, "dc2")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate everywhere
        with self.patient_exclusive_cql_connection(node2, "ks") as xsession:
            xsession.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'EverywhereStrategy'};")

        self.rebuild_errors = 0
        self.unexpected_errors = 0

        # rebuild dc2 from dc1
        def rebuild():
            try:
                logger.debug("Running nodetool rebuild dc1 on node2")
                node2.nodetool("rebuild dc1")
                logger.debug("Rebuild completed successfully")
                logger.debug("Running nodetool rebuild dc1 on node3")
                node3.nodetool("rebuild dc1")
                logger.debug("Rebuild completed successfully")
            except NodetoolError as e:
                if "rebuild is in progress" in str(e):
                    logger.debug("Rebuild is in progress")
                    self.rebuild_errors += 1
                else:
                    logger.debug(f"Unexpected rebuild failure {e!s}")
                    self.unexpected_errors += 1

        executor = ThreadPoolExecutor(max_workers=1)
        rebuild_cmd = executor.submit(rebuild)

        # concurrent rebuild should not be allowed (CASSANDRA-9119)
        # (following sleep is needed to avoid conflict in 'nodetool()' method setting up env.)
        time.sleep(0.1)
        rebuild()

        rebuild_cmd.result()

        # exactly 1 of the two nodetool calls should fail
        # usually it will be the one in the main thread,
        # but occasionally it wins the race with the one in the secondary thread,
        # so we check that one succeeded and the other failed
        assert self.unexpected_errors == 0, "unexpected rebuild errors encountered."
        assert self.rebuild_errors == 1, "concurrent rebuild should not be allowed, but one rebuild command should have succeeded."

        # check data
        logger.debug("Verifying data on all nodes")
        nodes = cluster.nodelist()
        cluster.stop()
        for n in nodes:
            logger.debug(f"Starting {n.name}")
            n.start(wait_for_binary_proto=True, wait_other_notice=False)
            with self.patient_exclusive_cql_connection(n, "ks") as xsession:
                query_c1c2(xsession, i, ConsistencyLevel.ONE)
            n.stop(wait_other_notice=False)

    @pytest.mark.required_features("!tablets")
    def test_rebuild_many_tables(self):
        """
        Test rebuilding many tables in same dc works as expected.
        """
        num_keys = 100
        num_tables = 100

        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})
        node1 = self.add_node(1)

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        ks = "ks"
        dc = "dc1"
        create_ks(session, ks, {dc: 1})

        logger.debug(f"Creating {num_tables} tables")
        tables = [f"cf_{i:04d}" for i in range(num_tables)]
        for cf in tables:
            create_c1c2_table(session, cf=cf, debug_query=False)
            insert_c1c2(session, n=num_keys, ks=ks, cf=cf, consistency=ConsistencyLevel.ALL)

        keys = [i for i in range(num_keys)]
        self._check_data(session, [ks], tables, keys)
        session.shutdown()

        logger.debug("Bootstrapping node2 with {auto_bootstrap: false}")
        node2 = self.add_node(2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Adjusting replication")
        session = self.patient_exclusive_cql_connection(node2)
        session.execute(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{dc}':2 }};")

        logger.debug("Rebuilding node2")
        node2.nodetool("rebuild")

        logger.debug("Killing node1")
        node1.stop(gently=False)

        self._check_data(session, [ks], tables, keys, cl=ConsistencyLevel.ONE)

    @pytest.mark.required_features("!tablets")
    def test_rebuild_many_keyspaces(self):
        """
        Test rebuilding many keyspaces in same dc works as expected.
        """
        num_keys = 100
        num_keyspaces = 50
        num_tables = 2

        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})
        node1 = self.add_node(1)

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        dc = "dc1"
        logger.debug(f"Creating {num_keyspaces} keyspaces")
        keyspaces = [f"ks_{i:04d}" for i in range(num_keyspaces)]
        for ks in keyspaces:
            create_ks(session, ks, {dc: 1})

        logger.debug(f"Creating {num_tables} table(s) in each ks")
        tables = [f"cf_{i:04d}" for i in range(num_tables)]
        for ks in keyspaces:
            for cf in tables:
                create_c1c2_table(session, cf=f"{ks}.{cf}", debug_query=False)
                insert_c1c2(session, n=num_keys, ks=ks, cf=cf, consistency=ConsistencyLevel.ALL)

        keys = [i for i in range(num_keys)]
        self._check_data(session, keyspaces, tables, keys)
        session.shutdown()

        logger.debug("Bootstrapping node2 with {auto_bootstrap: false}")
        node2 = self.add_node(2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Adjusting replication")
        session = self.patient_exclusive_cql_connection(node2)
        for ks in keyspaces:
            session.execute(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{dc}':2 }};")

        logger.debug("Rebuilding node2")
        node2.nodetool("rebuild")

        logger.debug("Killing node1")
        node1.stop(gently=False)

        self._check_data(session, keyspaces, tables, keys, cl=ConsistencyLevel.ONE)

    def test_rebuild_keyspace_with_rf_1(self):
        self.cluster.populate(3)
        self.cluster.set_configuration_options(values={"enable_repair_based_node_ops": "false"})
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.debug("Create ks with rf 1 and insert data")
        node1 = self.cluster.nodelist()[0]
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, "ks", rf=1)
        create_c1c2_table(session)
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ONE)

        logger.debug("Check keys")
        for i in range(1000):
            query_c1c2(session, i, ConsistencyLevel.ONE)

        logger.debug("Stopping node 3.")
        node3 = self.cluster.nodelist()[2]
        node3.stop(gently=True, wait_other_notice=True)

        logger.debug("Add node4 without bootstrap")
        node4 = self.cluster.new_node(4, auto_bootstrap=False, is_seed=False)
        node4.start(replace_node_host_id=node3.hostid(), wait_for_binary_proto=True)

        # validate that warning mesasages appeared. Not error messages
        node4.watch_log_for(exprs=[r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation"])

        logger.debug("Run rebuild on node 4")
        node4.nodetool("rebuild")
        logger.debug("Rebuild done, Validate that data could lost due to rf=1")

        with pytest.raises(expected_exception=(AssertionError,), match=r"Found .* errors out of 1000 keys"):
            self._check_data(session, keyspaces=["ks"], tables=["cf"], keys=[i for i in range(1000)])

    @pytest.mark.required_features("tablets")
    def test_rebuild_add_new_dc_with_tablets(self):
        """
        Test adding new dc with RF>1 works with tablets
        """
        keys = 25000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000

        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})

        # start nodes in dc1
        logger.info("Starting nodes in dc1")
        node1 = self.add_node(1, dc="dc1", rack="r1")
        node1.start(wait_for_binary_proto=True)
        node2 = self.add_node(2, dc="dc1", rack="r2")
        node2.start(wait_for_binary_proto=True)

        # populate data in dc1
        logger.info(f"Inserting {keys} keys")
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, "ks", {"dc1": 2})
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        # check data
        logger.info("Checking data")
        for i in range(keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        # Bootstraping new nodes in dc2
        logger.info("Starting nodes in dc2")
        node3 = self.add_node(3, dc="dc2", rack="r1")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4 = self.add_node(4, dc="dc2", rack="r2")
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)

        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        logger.info("Increasing RF to 1 in dc2")
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':2, 'dc2':1};")
        logger.info("Increasing RF to 2 in dc2")
        repl = get_replication_options(session, "ks")
        dc2_rf = repl["dc2"]
        if type(dc2_rf) is list:
            dc2_rf = ["r1", "r2"]
        else:
            dc2_rf = "2"
        session.execute(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class':'NetworkTopologyStrategy', 'dc1':{repl['dc1']}, 'dc2':{describe_rf(dc2_rf)}}};")

        # check data
        session.execute("USE ks")
        logger.info("Verfiying data")
        for i in range(0, keys, 5):  # verifying all keys is too long over 4 nodes
            query_c1c2(session, i, ConsistencyLevel.ALL)

    @pytest.mark.required_features("tablets")
    def test_rebuild_del_dc_with_tablets(self):
        """
        Test removing existing dc works with tablets
        """
        keys = 25000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000

        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})

        # start nodes in dc1
        logger.info("Starting nodes in dc1")
        node1 = self.add_node(1, dc="dc1", rack="r1")
        node1.start(wait_for_binary_proto=True)
        node2 = self.add_node(2, dc="dc1", rack="r2")
        node2.start(wait_for_binary_proto=True)
        # start nodes in dc2
        logger.info("Starting nodes in dc2")
        node3 = self.add_node(3, dc="dc2", rack="r1")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4 = self.add_node(4, dc="dc2", rack="r2")
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)

        # populate data in all dcs
        logger.info(f"Inserting {keys} keys")
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, "ks", {"dc1": 2, "dc2": 2})
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        # check data
        logger.info("Checking data")
        for i in range(0, keys, 5):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        # alter keyspace to stop replicating on dc2
        session = self.patient_exclusive_cql_connection(node2)
        repl = get_replication_options(session, "ks")
        logger.info(f"rf = {repl}")
        rf = decrease_rf(repl["dc2"])
        logger.info("Decreasing RF to 1 in dc2")
        session.execute(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class':'NetworkTopologyStrategy', 'dc1':2, 'dc2':{describe_rf(rf)}}};")
        rf = decrease_rf(rf)
        logger.info("Decreasing RF to 0 in dc2")
        session.execute(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class':'NetworkTopologyStrategy', 'dc1':2, 'dc2':{describe_rf(rf)}}};")

        # removing nodes from dc2
        node3.stop()
        node4.stop()

        # check data
        session.execute("USE ks")
        logger.info("Verfiying data")
        for i in range(keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
