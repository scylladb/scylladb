import logging
import random
import time
from threading import Event, Thread

import pytest
from cassandra import WriteFailure
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks
from tools.assertions import assert_one, assert_unavailable
from tools.cluster_topology import generate_rack_topology_based_rf

logger = logging.getLogger(__name__)


class LoadThread(Thread):
    def __init__(self, tester, node, shift):
        Thread.__init__(self)
        self.tester = tester
        self.target_node = node
        self._to_stop = Event()
        self._to_stop.clear()
        self._results = {}
        self._step = 1000
        self._base_value = -2147483648 + shift
        self._max_value = 2147483647
        self._current = 0

    def run(self):
        if not self.target_node.is_running():
            return

        while True:
            with self.tester.patient_cql_connection(self.target_node) as session:
                insert_stmt = session.prepare("INSERT INTO ks.test(k,v) VALUES (?, ?)")
                update_stmt = session.prepare("UPDATE ks.test SET v = v + 1 WHERE k=? IF EXISTS")
                for n in range(self._base_value, self._max_value, self._step):
                    if self._to_stop.is_set():
                        return
                    try:
                        self._results[(n - self._base_value) // self._step]
                    except KeyError:
                        try:
                            session.execute(insert_stmt.bind((n, 0)))
                            self._results[(n - self._base_value) // self._step] = 0
                            continue
                        except:
                            self._results[(n - self._base_value) // self._step] = None
                            continue
                    try:
                        session.execute(update_stmt.bind((n,)))
                        self._results[(n - self._base_value) // self._step] += 1
                    except:
                        pass

    def stop(self, timeout=None):
        self._to_stop.set()
        try:
            self.join(timeout)
        except:
            pass


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.lwt
class TestPaxos(Tester):
    cluster: ScyllaCluster

    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1):
        cluster = self.cluster

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        rack_layout = generate_rack_topology_based_rf(nodes, rf)
        topology_layout = {"dc1": rack_layout}
        cluster.populate(topology_layout).start(wait_for_binary_proto=True, wait_other_notice=True)

        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session

    def node_session(self, node):
        return self.patient_cql_connection(self.nodelist()[node])

    def test_replica_availability(self):
        """
        @jira_ticket CASSANDRA-8640

        Regression test for a bug (CASSANDRA-8640) that required all nodes to
        be available in order to run LWT queries, even if the query could
        complete correctly with quorum nodes available.
        """
        session = self.prepare(nodes=3, rf=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        self.cluster.nodelist()[2].stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        self.cluster.nodelist()[1].stop()
        assert_unavailable(session.execute, "INSERT INTO test (k, v) VALUES (2, 2) IF NOT EXISTS")

        self.cluster.nodelist()[1].start(wait_for_binary_proto=True, wait_other_notice=True)
        session.execute("INSERT INTO test (k, v) VALUES (3, 3) IF NOT EXISTS")

        self.cluster.nodelist()[2].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (4, 4) IF NOT EXISTS")

    def _randomly_remove_nodes(self, loaders):
        nodes = self.cluster.nodelist()
        # Reserve one node per rack so we never leave a rack empty.
        # These reserved nodes are not eligible for removal.
        reserved = set()
        seen_racks = set()
        for n in nodes:
            if n.rack not in seen_racks:
                seen_racks.add(n.rack)
                reserved.add(n)
        removable_nodes = [n for n in nodes if n not in reserved]
        if not removable_nodes:
            return
        # Randomly shuffle so removal order is non-deterministic
        random.shuffle(removable_nodes)
        logger.debug(f"_randomly_remove_nodes removing {len(removable_nodes)} nodes")

        # Collect hostids before stopping nodes
        node_to_hostid = {node: node.hostid() for node in removable_nodes}

        # Kill all selected nodes at once
        for node in removable_nodes:
            hostid = node_to_hostid[node]
            logger.debug(f"Stopping node {node.name} (hostid {hostid})")
            node.stop(wait=True, wait_other_notice=True, gently=True)
            if node.is_running():
                node.stop(wait=True, wait_other_notice=True, gently=False)
            loaders[node].stop()
            del loaders[node]

        # Pick a guaranteed-alive node to run removenode from
        alive_node = next(iter(reserved))

        # Remove nodes one-by-one; pass --ignore-dead-nodes for all except the last
        for i, node in enumerate(removable_nodes):
            hostid = node_to_hostid[node]
            # Nodes in removable_nodes that haven't been removed yet (excluding the current one)
            other_dead_hostids = [node_to_hostid[n] for n in removable_nodes[i + 1 :]]

            if other_dead_hostids:
                alive_node.nodetool(f"removenode --ignore-dead-nodes {','.join(other_dead_hostids)} {hostid}")
            else:
                alive_node.nodetool(f"removenode {hostid}")

            self.cluster.remove(node)

    def test_topology_change_in_presence_of_down_node(self):
        session = self.prepare(nodes=6, rf=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        loaders = {}
        for n, node in enumerate(self.cluster.nodelist()):
            loaders[node] = LoadThread(self, node, n)
            loaders[node].start()
        time.sleep(10)
        try:
            self._randomly_remove_nodes(loaders)
        finally:
            for loader in loaders.values():
                loader.stop()
        out, _err = self.cluster.nodelist()[0].nodetool("status", capture_output=True)
        logger.debug(f"nodetool status is: {out}")

    # Schema mismatch tests

    def _base_schema_mismatch_test_tpl(  # noqa: PLR0913
        self,
        clear_schema_cache,
        setup_test_env_action,
        insert_action,
        ddl_action,
        second_insert_action,
        verify_results_action,
    ):
        # Increase time period for which cached schema definitions will live before
        # being evicted from the node cache.
        # The tests depend on this cache being not empty, so set to some sufficiently large value, e.g. 1000 seconds.
        self.cluster.set_configuration_options(values={"schema_registry_grace_period": 1000})
        # set TRACE log level for the test node to be able
        # to catch schema_mismatch_error exceptions
        self.cluster.set_log_level("TRACE")
        session = self.prepare(nodes=1, rf=1)
        # Create test tables and configure other necessary stuff
        setup_test_env_action(session)

        node1 = self.cluster.nodelist()[0]

        # Fail at the end of "accept" stage so that we have commited a proposal but not yet completed the round
        errinj_name = "paxos_error_after_save_proposal"
        logger.debug(f"Enable {errinj_name} injection on the test node")
        self.enable_error(errinj_name, node1, one_shot=True)

        # Execute the LWT query leaving an unfinished paxos round behind
        key = 0
        with pytest.raises(WriteFailure):
            logger.debug(f"Execute the first INSERT query on key {key}")
            insert_action(session, key)

        # perform DDL action to change test table schema version
        ddl_action(session)

        logger.debug("Disable remaining injections on the node (if any)")
        self.disable_errors(node1)

        if clear_schema_cache:
            logger.debug(f"Restart the node to clear up schema_registry cache")
            node1.stop(wait=True)
            node1.start()
            # re-open the session to the node
            session = self.patient_cql_connection(node1)
            session.execute("USE ks")

        # Initiate a subsequent round on the same key so that it performs
        # repair of the previous round and it is supposed to fail
        logger.debug(f"Execute the second INSERT on key {key} (supposed to trigger repair of the previous round)")
        second_insert_action(session, key)
        # Execute additional actions to verify that the test executed successfully (check logs and data)
        verify_results_action(session, node1)

    def _schema_mismatch_tpl(self, clear_schema_cache):
        """
        Tests for the following scenario:

        1. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        2. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version.

        3. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: #6502
        """

        def create_test_table(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def add_dummy_column(session):
            session.execute("ALTER TABLE test ADD dummy int")

        def check_schema_mismatch_exc(session, node):
            exc_msg = node.grep_log("<schema_mismatch_error>")
            if exc_msg:
                raise Exception(f'Unexpected "schema_mismatch_error" exception: {exc_msg}')

        self._base_schema_mismatch_test_tpl(
            clear_schema_cache=clear_schema_cache, setup_test_env_action=create_test_table, insert_action=insert_action, ddl_action=add_dummy_column, second_insert_action=insert_action, verify_results_action=check_schema_mismatch_exc
        )

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_cache(self):
        self._schema_mismatch_tpl(clear_schema_cache=False)

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_no_cache(self):
        self._schema_mismatch_tpl(clear_schema_cache=True)

    def _schema_mismatch_mv_tpl(self, clear_schema_cache):
        """
        Tests for the following scenario:

        1. Create a table and an associated materialized view

        2. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        3. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version.

        4. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: scylladb/scylladb#6074
        """

        def create_test_table_and_mv(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
            session.execute("CREATE MATERIALIZED VIEW test_view AS SELECT * from test where k > 0 PRIMARY KEY(k)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def add_dummy_column(session):
            session.execute("ALTER TABLE test ADD dummy int")

        def check_schema_mismatch_exc(session, node):
            exc_msg = node.grep_log("<schema_mismatch_error>")
            if exc_msg:
                raise Exception(f'Unexpected "schema_mismatch_error" exception: {exc_msg}')

        self._base_schema_mismatch_test_tpl(
            clear_schema_cache=clear_schema_cache, setup_test_env_action=create_test_table_and_mv, insert_action=insert_action, ddl_action=add_dummy_column, second_insert_action=insert_action, verify_results_action=check_schema_mismatch_exc
        )

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_mv_cache(self):
        self._schema_mismatch_mv_tpl(clear_schema_cache=False)

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_mv_no_cache(self):
        self._schema_mismatch_mv_tpl(clear_schema_cache=True)

    def _schema_mismatch_drop_regular_column_tpl(self, clear_schema_cache):
        """
        Tests for the following scenario:

        1. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        2. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version (drop one of the columns participating in the query).

        3. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: #6467
        """

        def create_test_table(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def drop_column(session):
            session.execute("ALTER TABLE test DROP v")

        def second_insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k) VALUES (?) IF NOT EXISTS")
            session.execute(stmt, [key])

        def check_exc_and_table_data(session, node):
            exc_msg = r"exception during mutation write to ([0-9.]+): std::out_of_range \(regular column id 0 >= 0\)"
            exc_msg = node.grep_log(exc_msg)
            if exc_msg:
                raise Exception(f"Unexpected exception during mutation write: {exc_msg}")
            logger.debug("Selecting table contents to verify that insert was applied successfully")
            assert_one(session, "SELECT * from test", [0])

        self._base_schema_mismatch_test_tpl(
            clear_schema_cache=clear_schema_cache, setup_test_env_action=create_test_table, insert_action=insert_action, ddl_action=drop_column, second_insert_action=second_insert_action, verify_results_action=check_exc_and_table_data
        )

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_drop_regular_column_cache(self):
        self._schema_mismatch_drop_regular_column_tpl(clear_schema_cache=False)

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_drop_regular_column_no_cache(self):
        self._schema_mismatch_drop_regular_column_tpl(clear_schema_cache=True)

    def _schema_mismatch_drop_regular_column_in_the_middle_tpl(self, clear_schema_cache):
        """
        Tests for the following scenario:

        1. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        2. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version (drop one of the columns participating in the query).

        3. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: #6467
        """

        def create_test_table(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int, v2 int)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v2) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def drop_column(session):
            session.execute("ALTER TABLE test DROP v")

        def second_insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k) VALUES (?) IF NOT EXISTS")
            session.execute(stmt, [key])

        def check_exc_and_table_data(session, node):
            exc_msg = r"exception during mutation write to ([0-9.]+): std::out_of_range \(regular column id 0 >= 0\)"
            exc_msg = node.grep_log(exc_msg)
            if exc_msg:
                raise Exception(f"Unexpected exception during mutation write: {exc_msg}")
            logger.debug("Selecting table contents to verify that insert was applied successfully")
            assert_one(session, "SELECT * from test", [0, 0])

        self._base_schema_mismatch_test_tpl(
            clear_schema_cache=clear_schema_cache, setup_test_env_action=create_test_table, insert_action=insert_action, ddl_action=drop_column, second_insert_action=second_insert_action, verify_results_action=check_exc_and_table_data
        )

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_drop_regular_column_in_the_middle_cache(self):
        self._schema_mismatch_drop_regular_column_in_the_middle_tpl(clear_schema_cache=False)

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.scylla_mode("!release")
    def test_schema_mismatch_drop_regular_column_in_the_middle_no_cache(self):
        self._schema_mismatch_drop_regular_column_in_the_middle_tpl(clear_schema_cache=True)
