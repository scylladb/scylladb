#
# This test is based on a Cassandra's test with the same name.
#
import functools
import logging
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_cf, create_ks
from dtest_setup import DTestSetup
from tools.assertions import assert_none, assert_row_count_not_zero
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2_no_prepared
from tools.misc import set_trace_probability
from tools.queries import (
    enable_slow_query_tracing,
    validate_slow_query_tracing_is_enabled,
)
from tools.stress import assert_cs_success

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class PrepareClusterHelper(Tester):
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns = []

    def prepare(self, create_keyspace=True, nodes=3, rf=3, protocol_version=4, jvm_args=None):
        if jvm_args is None:
            jvm_args = []

        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=jvm_args)

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            session.execute("DROP KEYSPACE IF EXISTS ks")
            create_ks(session, "ks", rf)
        return session


@pytest.mark.next_gating
@pytest.mark.dtest_debug
@pytest.mark.dtest_full
class TestCqlTracing(PrepareClusterHelper):
    """
    Test that the default implementation for tracing works.
    """

    def trace(self, session):
        """
        * CREATE a table
        * enable TRACING
        * SELECT on a known system table and assert it ran with tracing by checking the output
        * INSERT a row into the created system table and assert it ran with tracing
        * SELECT from the table and assert it ran with tracing

        @param session The Session object to use to create a table.
        """

        node1 = self.cluster.nodelist()[0]

        # Create
        session.execute(
            """
            CREATE TABLE ks.users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """
        )

        out, _err = node1.run_cqlsh("TRACING ON", return_output=True, cqlsh_options=["--no-color"])
        assert "Tracing is enabled" in out, "Tracing has not been enabled"

        out, _err = node1.run_cqlsh("TRACING ON; SELECT * from ks.users", return_output=True, cqlsh_options=["--no-color"])
        assert "Tracing session: " in out, "SELECT query was run without tracing"
        assert "Request complete " in out, 'Expected substring "Request complete" was not found'

        # Inserts
        out, _err = node1.run_cqlsh(
            "CONSISTENCY ALL; TRACING ON; INSERT INTO ks.users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)", return_output=True, cqlsh_options=["--no-color"]
        )
        logger.debug(out)
        assert "Tracing session: " in out, "SELECT query was run without tracing"
        assert "Request complete " in out, 'Expected substring "Request complete" was not found'

        # Queries
        out, _err = node1.run_cqlsh("CONSISTENCY ALL; TRACING ON; SELECT firstname, lastname FROM ks.users WHERE userid = 550e8400-e29b-41d4-a716-446655440000", return_output=True, cqlsh_options=["--no-color"])
        logger.debug(out)
        assert "Tracing session: " in out, "SELECT query was run without tracing"
        assert f" {self.cluster.get_node_ip(1)} " in out, "Node1 IP is not in the "
        assert f" {self.cluster.get_node_ip(2)} " in out, "SELECT query was run without tracing"
        assert f" {self.cluster.get_node_ip(3)} " in out, "SELECT query was run without tracing"
        assert "Request complete " in out, 'Expected substring "Request complete" was not found'
        assert " Frodo |  Baggins" in out, 'Expected substring " Frodo |  Baggins" was not found'

    def test_tracing_simple(self):
        """
        Test tracing using the default tracing class. See trace().
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        session = self.prepare(nodes=cluster_topology, rf=3)
        self.trace(session)

    def test_tracing_shutdown(self):  # noqa: PLR0915
        """
        Check tracing functionality when Node is being shut down:
           - Check that CQL handling is stopped prior to tracing being stopped
             (otherwise there will be an assert coming from a
             cql_server::connection::process_request().
           - Check that nothing bad is going on when node is being shut done
             while a remote node requests tracing via RPC.
           - Check that tracing for all CQL requests complete prior to Node's
             shutdown are being pushed to the backend.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        self.cluster.populate(cluster_topology).start()
        node1, node2 = self.cluster.nodelist()

        # FIXME: remove when https://github.com/scylladb/scylla/issues/5697 issue is fixed
        self.ignore_log_patterns += [
            r"seastar - Timer callback failed: seastar::metrics::double_registration \
            (registering metrics twice for metrics: "
            r"storage_proxy_coordinator_background_replica_writes_failed_remote_node\)"
        ]

        logger.debug("Enable tracing for all CQL requests on node1 and node2...")
        set_trace_probability(nodes=[node1, node2], probability_value=1.0)

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        num_keys = 500
        logger.debug(f"Populating a table with {num_keys} keys...")
        insert_c1c2_no_prepared(session, keys=range(num_keys), consistency=ConsistencyLevel.ONE)
        node1.nodetool("flush")

        logger.debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        logger.debug("Checking log of node1 for assertions...")
        match = node1.grep_log("Assertion .* failed.")
        assert len(match) == 0, f"Found assertion failure: {match}"

        logger.debug("Check that all tracing session have been flushed...")
        pattern = re.compile("INSERT INTO")
        all_tracing_sessions_query = SimpleStatement("SELECT parameters FROM system_traces.sessions")
        rows = list(session.execute(all_tracing_sessions_query))
        count = functools.reduce(lambda x, y: x + y, map(lambda row: self.grep_one_line(row[0]["query"], pattern), rows))
        assert count == num_keys, "Not all tracing session have been flushed"

        logger.debug("Start node1...")
        node1.start(wait_for_binary_proto=True)

        logger.debug("Enable tracing for all CQL requests on node1...")
        set_trace_probability(nodes=[node1], probability_value=1.0)

        session = self.patient_cql_connection(node1)

        def run(name, q, additional_keys):
            try:
                q.put(True)
                logger.debug(f"Populating a table with {additional_keys} more keys...")
                insert_c1c2_no_prepared(session, keys=range(num_keys, num_keys + additional_keys), consistency=ConsistencyLevel.ONE)
                logger.debug(f"insertion of {additional_keys} keys is done")
            except:
                logger.debug("insertions was killed")

        queue = Queue()
        if node1.grep_log("WARNING: debug mode."):
            additional_keys = 2 * num_keys
        else:
            additional_keys = 30 * num_keys

        executor = ThreadPoolExecutor(max_workers=1)
        insert_thread = executor.submit(run, name="insert-thread", q=queue, additional_keys=additional_keys)
        queue.get(block=True)

        random.seed()
        wait_time = random.random()
        logger.debug(f"Wait for {wait_time} seconds")
        time.sleep(wait_time)

        logger.debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.debug("Waiting for insert-thread to complete...")
        insert_thread.result()
        executor.shutdown(wait=True)

    def test_tracing_startup(self):
        """
        Check tracing functionality when Node is started:
           - Check that CQL handling is not started before a local service is
             properly started while a remote Node sends RPC messages requesting
             tracing.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        self.cluster.populate(cluster_topology).start()
        node1, node2 = self.cluster.nodelist()

        # FIXME: remove when https://github.com/scylladb/scylla/issues/5697 issue is fixed
        self.ignore_log_patterns += [r"seastar - Timer callback failed: seastar::metrics::double_registration \(registering metrics twice for metrics: storage_proxy_coordinator_background_replica_writes_failed_remote_node\)"]

        logger.debug("Enable tracing for all CQL requests on node1...")
        set_trace_probability(nodes=[node1], probability_value=1.0)

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        logger.debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.debug("Checking log of node2 for assertions...")
        match = node2.grep_log("Assertion .* failed.")
        assert len(match) == 0, f"Found assertion errors: {match}"

        def run(name, q):
            try:
                num_keys = 15000
                q.put(True)
                logger.debug(f"Populating a table with {num_keys} keys...")
                insert_c1c2_no_prepared(session, keys=range(num_keys), consistency=ConsistencyLevel.ONE)
                logger.debug(f"insertion of {num_keys} keys is done")
            except:
                logger.debug("insertions thread was killed")

        queue = Queue()
        executor = ThreadPoolExecutor(max_workers=1)
        insert_thread = executor.submit(run, name="insert-thread", q=queue)
        queue.get(block=True)

        random.seed()
        wait_time = random.random()
        logger.debug(f"Wait for {wait_time} seconds")
        time.sleep(wait_time)

        logger.debug("Start node2...")
        node2.start(wait_for_binary_proto=True)

        insert_thread.result()
        executor.shutdown(wait=True)

    # ----------------------------------------------------------------------------------------------------------------------
    def grep_one_line(self, line, pattern):
        """
        A helper function that returns 1 if a pattern is found in a given string
        and 0 otherwise. 'pattern' is expected to be a compiled re(gular expression)
        object.
        """
        line = line.strip()
        if pattern.search(line):
            return 1
        return 0


@pytest.mark.dtest_full
class TestSlowQueryTracing(PrepareClusterHelper):
    """
    This class represents tests for Slow Query Logging tracing type.
    Tracing is a ScyllaDB tool meant to help debugging and analyzing internal flows in the server.
    One of the tracing types is Slow Query Logging - records queries with handling time above the specified threshold
    """

    @pytest.mark.use_cassandra_stress
    @pytest.mark.parametrize("fast", [True, False], ids=["enabled", "disabled"])
    def test_fast_slow_query_tracing(self, fast):
        """
        Feature: https://github.com/scylladb/scylla/pull/8314
        In fast slow query tracing mode, Scylla tracks only tracing sessions and omits all tracing events if the
        tracing context does not have a full_tracing state set. This mode tracks only CQL statement and related request
        parameters.
        This test validate that when fast slow query tracing mode is enabled, events are not reported. But sessions and
        node_slow_log are reported.
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=1)
        session = self.prepare(nodes=cluster_topology, rf=1)
        # Slow Query Logging - records queries with handling time above the specified threshold.
        # Set threshold to 500 (default is 500000) to get queries reported as slow
        threshold = 500
        enable_slow_query_tracing(node=self.cluster.nodelist()[0], fast=fast, threshold=threshold)
        validate_slow_query_tracing_is_enabled(node=self.cluster.nodelist()[0], fast=fast, threshold=threshold)
        node1 = self.cluster.nodelist()[0]

        logger.debug("Run cassandra-stress write load")
        cs_result = node1.stress(stress_options=["write", "cl=ONE", "n=10000", "-schema replication(factor=1)", "-mode cql3 native", "-rate threads=10"])
        assert_cs_success(cs_result)

        if fast:
            assert_none(session, query="select * from system_traces.events")
        else:
            assert_row_count_not_zero(session, table_name="system_traces.events")
        assert_row_count_not_zero(session, table_name="system_traces.node_slow_log")
        assert_row_count_not_zero(session, table_name="system_traces.sessions")


# ----------------------------------------------------------------------------------------------------------------------
#    @known_failure(failure_source='test',
#                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11465',
#                   flaky=True)
#    @since('3.4')
#    def test_tracing_unknown_impl(self):
#        """
#        Test that Cassandra logs an error, but keeps its default tracing
#        behavior, when a nonexistent tracing class is specified.
#
#        * set a nonexistent custom tracing class
#        * run trace()
#        * if running the test on a version with custom tracing classes
#          implemented, check that an error about the nonexistent class was
#          logged.
#
#        @jira_ticket CASSANDRA-10392
#        """
#        expected_error = 'Cannot use class junk for tracing'
#        self.ignore_log_patterns += [expected_error]
#        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=junk'])
#        self.trace(session)
#
#        errs = self.cluster.nodelist()[0].grep_log_for_errors()
#        logger.debug('Errors after attempted trace with unknown tracing class: {errs}'.format(errs=errs))
#        assert len(errs) == 1, f"Found {len(errs)} errors, expected 1"
#        assert len(errs[0]) == 1
#        err = errs[0][0]
#        assert expected_error in err, f"Expected error {expected_error} is not found"
#
#    @known_failure(failure_source='test',
#                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11465',
#                   flaky=True)
#    @since('3.4')
#    def test_tracing_default_impl(self):
#        """
#        Test that Cassandra logs an error, but keeps its default tracing
#        behavior, when the default tracing class is specified.
#
#        This doesn't work because the constructor for the default
#        implementation isn't accessible.
#
#        * set the default tracing class as a custom tracing class
#        * run trace()
#        * if running the test on a version with custom tracing classes
#          implemented, check that an error about the class was
#          logged.
#
#        @jira_ticket CASSANDRA-10392
#        """
#        expected_error = 'Cannot use class org.apache.cassandra.tracing.TracingImpl'
#        self.ignore_log_patterns += [expected_error]
#        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.TracingImpl'])
#        self.trace(session)
#
#        errs = self.cluster.nodelist()[0].grep_log_for_errors()
#        logger.debug('Errors after attempted trace with default tracing class: {errs}'.format(errs=errs))
#        assert len(errs) == 1, f"Expected 1 error, got {len(errs)}"
#        assert len(errs[0]) == 1, f"Expected 1 error, got {len(errs[0])}"
#        err = errs[0][0]
#        assert expected_error in err, f"Expected error {expected_error} is not found"
#        # make sure it logged the error for the correct reason. this isn't
#        # part of the expected error to avoid having to escape parens and
#        # periods for regexes.
#        assert "Default constructor for Tracing class "
#                      "'org.apache.cassandra.tracing.TracingImpl' is inaccessible." in err,
#                       "Expected message is not found"
# ----------------------------------------------------------------------------------------------------------------------
