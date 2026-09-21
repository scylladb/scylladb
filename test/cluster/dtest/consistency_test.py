import logging
import queue
import sys
import time
import traceback
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node
from tools.assertions import assert_none, assert_unavailable
from tools.cluster_topology import generate_cluster_topology, generate_cluster_topology_based_rf
from tools.data import (
    create_c1c2_table,
    insert_c1c2,
    insert_columns,
    query_c1c2,
    rows_to_list,
)
from tools.marks import issue_open, with_feature
from tools.metrics import get_node_metrics
from tools.paging import PageFetcher

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class TestHelper(Tester):
    sessions = None
    nodes = None
    rf_value = None
    DISABLE_VNODES = False

    @classmethod
    @pytest.fixture(scope="class", autouse=True)
    def pre_setup(cls, dtest_config):
        cls.DISABLE_VNODES = not dtest_config.use_vnodes

    @staticmethod
    def _name(cl_value):
        return {
            None: "-",
            ConsistencyLevel.ANY: "ANY",
            ConsistencyLevel.ONE: "ONE",
            ConsistencyLevel.TWO: "TWO",
            ConsistencyLevel.THREE: "THREE",
            ConsistencyLevel.QUORUM: "QUORUM",
            ConsistencyLevel.ALL: "ALL",
            ConsistencyLevel.LOCAL_QUORUM: "LOCAL_QUORUM",
            ConsistencyLevel.EACH_QUORUM: "EACH_QUORUM",
            ConsistencyLevel.SERIAL: "SERIAL",
            ConsistencyLevel.LOCAL_SERIAL: "LOCAL_SERIAL",
            ConsistencyLevel.LOCAL_ONE: "LOCAL_ONE",
        }[cl_value]

    @staticmethod
    def _is_local(cl_value):
        return cl_value in (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_SERIAL)

    @staticmethod
    def _is_conditional(cl_value):
        return cl_value in (ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL)

    @staticmethod
    def _required_nodes(cl_name, rf_factors, dc_name):
        """
        Return the number of nodes required by this consistency level
        in the current data center, specified by the dc parameter,
        given a list of replication factors, one per dc.
        """
        return {
            ConsistencyLevel.ANY: 1,
            ConsistencyLevel.ONE: 1,
            ConsistencyLevel.TWO: 2,
            ConsistencyLevel.THREE: 3,
            ConsistencyLevel.QUORUM: sum(rf_factors) // 2 + 1,
            ConsistencyLevel.ALL: sum(rf_factors),
            ConsistencyLevel.LOCAL_QUORUM: rf_factors[dc_name] // 2 + 1,
            ConsistencyLevel.EACH_QUORUM: rf_factors[dc_name] // 2 + 1,
            ConsistencyLevel.SERIAL: sum(rf_factors) // 2 + 1,
            ConsistencyLevel.LOCAL_SERIAL: rf_factors[dc_name] // 2 + 1,
            ConsistencyLevel.LOCAL_ONE: 1,
        }[cl_name]

    def _should_succeed(self, cl_value, rf_factors, num_nodes_alive, current):
        """
        Return true if the read or write operation should succeed based on
        the consistency level requested, the replication factors and the
        number of nodes alive in each data center.
        """
        if self._is_local(cl_value):
            return num_nodes_alive[current] >= self._required_nodes(cl_value, rf_factors, current)
        elif cl_value == ConsistencyLevel.EACH_QUORUM:
            for i in range(len(rf_factors)):
                if num_nodes_alive[i] < self._required_nodes(cl_value, rf_factors, i):
                    return False
            return True
        else:
            return sum(num_nodes_alive) >= self._required_nodes(cl_value, rf_factors, current)

    def _start_cluster(self, save_sessions=False):
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        nodes = self.nodes
        rf = self.rf_value

        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(generate_cluster_topology_based_rf(nodes=nodes, rf=rf, dc_name_prefix="dc")).start(wait_for_binary_proto=True, wait_other_notice=True)

        self.ksname = "mytestks"
        session = self.patient_exclusive_cql_connection(cluster.nodelist()[0])

        create_ks(session, self.ksname, rf)
        self.create_tables(session)

        if save_sessions:
            self.sessions = []
            self.sessions.append(session)
            for node in cluster.nodelist()[1:]:
                self.sessions.append(self.patient_exclusive_cql_connection(node, self.ksname))

    def create_tables(self, session):
        self.create_users_table(session)
        self.create_counters_table(session)

    def truncate_tables(self, session):
        statement = SimpleStatement("TRUNCATE users", ConsistencyLevel.ALL)
        session.execute(statement)
        statement = SimpleStatement("TRUNCATE counters", ConsistencyLevel.ALL)
        session.execute(statement)

    def create_users_table(self, session):
        session.execute(
            """CREATE TABLE users (
                userid int PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            )"""
        )

    def insert_user(self, session, userid, age, consistency, serial_consistency=None):
        text = "INSERT INTO users (userid, firstname, lastname, age) VALUES (%d, 'first%d', 'last%d', %d) %s" % (userid, userid, userid, age, "IF NOT EXISTS" if serial_consistency else "")
        statement = SimpleStatement(text, consistency_level=consistency, serial_consistency_level=serial_consistency)
        session.execute(statement)

    def update_user(self, session, userid, age, consistency, serial_consistency=None, prev_age=None):  # noqa: PLR0913
        text = "UPDATE users SET age = %d WHERE userid = %d" % (age, userid)
        if serial_consistency and prev_age:
            text = text + " IF age = %d" % (prev_age)
        statement = SimpleStatement(text, consistency_level=consistency, serial_consistency_level=serial_consistency)
        session.execute(statement)

    def delete_user(self, session, userid, consistency):
        statement = SimpleStatement("DELETE FROM users where userid = %d" % (userid,), consistency_level=consistency)
        session.execute(statement)

    def query_user(self, session, userid, age, consistency, check_ret=True):
        statement = SimpleStatement("SELECT userid, age FROM users where userid = %d" % (userid,), consistency_level=consistency)
        res = session.execute(statement)
        expected = [[userid, age]] if age else []
        ret = rows_to_list(res) == expected
        if check_ret:
            assert ret, f"Got {rows_to_list(res)} from {session.cluster.contact_points}, expected {expected} at {self._name(consistency)}"
        return ret

    def create_counters_table(self, session):
        session.execute(
            """
            CREATE TABLE counters (
                id int PRIMARY KEY,
                c counter
            )
        """
        )

    def update_counter(self, session, counter_idx, consistency, serial_consistency=None):
        text = "UPDATE counters SET c = c + 1 WHERE id = %d" % (counter_idx,)
        statement = SimpleStatement(text, consistency_level=consistency, serial_consistency_level=serial_consistency)
        session.execute(statement)
        return statement

    def query_counter(self, session, id_value, val, consistency, check_ret=True):
        statement = SimpleStatement("SELECT * from counters WHERE id = %d" % (id_value,), consistency_level=consistency)
        res = session.execute(statement)
        expected = [[id_value, val]] if val else []
        res = rows_to_list(res)
        ret = res == expected
        if check_ret:
            assert ret, f"Got {res} from {session.cluster.contact_points}, expected {expected} at {self._name(consistency)}"
        return ret, "" if ret else f"Got {res} from {session.cluster.contact_points}, expected {expected} at {self._name(consistency)}"

    @staticmethod
    def read_counter(session, id_value, consistency):
        """
        Return the current counter value. If we find no value we return zero
        because after the next update the counter will become one.
        """
        statement = SimpleStatement("SELECT c from counters WHERE id = %d" % (id_value,), consistency_level=consistency)
        res = rows_to_list(session.execute(statement))
        return res[0][0] if res else 0


@pytest.mark.dtest_full
class TestAvailability(TestHelper):
    """
    Test that we can read and write depending on the number of nodes that are alive and the consistency levels.
    """

    nodes = None
    rf_value = None

    def _test_simple_strategy(self, combinations):
        """
        Helper test function for a single data center: invoke _test_insert_query_from_node() for each node
        and each combination, progressively stopping nodes.
        """
        cluster = self.cluster
        nodes = self.nodes
        rf = self.rf_value

        num_alive = nodes
        for node in range(nodes):
            logger.info("Testing node %d in single dc with %d nodes alive" % (node, num_alive))
            session = self.patient_exclusive_cql_connection(cluster.nodelist()[node], self.ksname)
            for combination in combinations:
                self._test_insert_query_from_node(session, 0, [rf], [num_alive], *combination)

            self.cluster.nodelist()[node].stop(wait=True, wait_other_notice=True)
            num_alive = num_alive - 1

    def _test_network_topology_strategy(self, combinations):
        """
        Helper test function for multiple data centers, invoke _test_insert_query_from_node() for each node
        in each dc and each combination, progressively stopping nodes.
        """
        cluster = self.cluster
        nodes = self.nodes
        rf = self.rf_value
        expected_error = r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\): std::runtime_error[ :]+\(?failed status returned from .*\)?"
        self.ignore_log_patterns.append(expected_error)

        nodes_alive = deepcopy(nodes)
        rf_factors = list(rf.values())

        for i in range(len(nodes)):  # for each dc
            logger.info("Testing dc %d with rf %d and %s nodes alive" % (i, rf_factors[i], nodes_alive))
            for node_idx in range(nodes[i]):  # for each node in this dc
                logger.info("Testing node %d in dc %d with %s nodes alive" % (node_idx, i, nodes_alive))
                node = node_idx + sum(nodes[:i])
                session = self.patient_exclusive_cql_connection(cluster.nodelist()[node], self.ksname)
                for combination in combinations:
                    self._test_insert_query_from_node(session, i, rf_factors, nodes_alive, *combination)

                self.cluster.nodelist()[node].stop(wait_other_notice=True)
                nodes_alive[i] = nodes_alive[i] - 1

    # pylint:disable=too-many-arguments
    def _test_insert_query_from_node(  # noqa: PLR0913
        self,
        session,
        dc_idx,
        rf_factors,
        num_nodes_alive,
        write_cl,
        read_cl,
        serial_cl=None,
        check_ret=True,
    ):
        """
        Test availability for read and write via the session passed in as a prameter.
        """
        logger.info(f"Connected to {session.cluster.contact_points} for {self._name(write_cl)}/{self._name(read_cl)}/{self._name(serial_cl)}")

        start = 0
        end = 100
        age = 30

        if self._should_succeed(write_cl, rf_factors, num_nodes_alive, dc_idx):
            for userid in range(start, end):
                self.insert_user(session, userid, age, write_cl, serial_cl)
        else:
            assert_unavailable(self.insert_user, session, end, age, write_cl, serial_cl)

        if self._should_succeed(read_cl, rf_factors, num_nodes_alive, dc_idx):
            for userid in range(start, end):
                self.query_user(session, userid, age, read_cl, check_ret)
        else:
            assert_unavailable(self.query_user, session, end, age, read_cl, check_ret)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_simple_strategy(self):
        """
        Test for a single datacenter, using simple replication strategy.
        """
        self.nodes = 3
        self.rf_value = 3

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.TWO),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.THREE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_ONE, None, False),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL),
        ]

        self._test_simple_strategy(combinations)

    @pytest.mark.require("#1117")
    def test_simple_strategy_each_quorum(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, using simple replication strategy, only
        the each quorum reads.
        """
        self.nodes = 3
        self.rf_value = 3

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self._test_simple_strategy(combinations)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_network_topology_strategy(self):
        """
        Test for multiple datacenters, using network topology replication strategy.
        """
        self.nodes = [3, 3, 3]
        self.rf_value = OrderedDict([("dc1", 3), ("dc2", 3), ("dc3", 3)])

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.TWO),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.THREE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_ONE, None, False),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL),
        ]

        self._test_network_topology_strategy(combinations)

    @pytest.mark.require("#1117")
    def test_network_topology_strategy_each_quorum(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, using network topology strategy, only
        the each quorum reads.
        """
        self.nodes = [3, 3, 3]
        self.rf_value = OrderedDict([("dc1", 3), ("dc2", 3), ("dc3", 3)])

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self._test_network_topology_strategy(combinations)


@pytest.mark.dtest_full
class TestAccuracy(TestHelper):
    """
    Test that we can consistently read back what we wrote depending on the write and read consitency levels.
    """

    nodes = None
    rf_value = None

    class Validation:  # pylint:disable=too-many-instance-attributes
        # pylint:disable=too-many-arguments
        def __init__(self, outer, sessions, nodes, rf_factors, start, end, write_cl, read_cl, serial_cl=None):  # noqa: PLR0913
            self.outer = outer
            self.sessions = sessions
            self.nodes = nodes
            self.rf_factors = rf_factors
            self.start = start
            self.end = end
            self.write_cl = write_cl
            self.read_cl = read_cl
            self.serial_cl = serial_cl

            self.test_name = f"Testing accuracy for {outer._name(write_cl)}/{outer._name(read_cl)}/{outer._name(serial_cl)} (keys : {start} to {end})"
            logger.info("Starting [%s]", self.test_name)

        def get_num_nodes(self, idx):
            """
            Given a node index, identify to which data center we are connecting and return
            number of nodes we write to, read from and whether R + W > N
            """
            outer = self.outer
            nodes = self.nodes
            rf_factors = list(self.rf_factors)
            write_cl = self.write_cl
            read_cl = self.read_cl

            dc_value = 0
            if isinstance(nodes, list):
                for i in range(len(nodes)):
                    if idx < sum(nodes[: i + 1]):
                        break
                    dc_value += 1

            if write_cl == ConsistencyLevel.EACH_QUORUM:
                write_nodes = sum([outer._required_nodes(write_cl, rf_factors, i) for i in range(len(nodes))])
            else:
                write_nodes = outer._required_nodes(write_cl, rf_factors, dc_value)

            read_nodes = outer._required_nodes(read_cl, rf_factors, dc_value)
            strong_consistency = read_nodes + write_nodes > sum(rf_factors)

            return write_nodes, read_nodes, strong_consistency

        def validate_users(self):
            """
            First validation function: update the users table sending different values to different sessions
            and check that when strong_consistency is true (R + W > N) we read back the latest value from all sessions.
            If strong_consistency is false we instead check that we read back the latest value from at least
            the number of nodes we wrote to.
            """
            outer = self.outer
            sessions = self.sessions
            start = self.start
            end = self.end
            write_cl = self.write_cl
            read_cl = self.read_cl
            serial_cl = self.serial_cl

            def check_all_sessions(idx: int, _userid, val):
                write_nodes, _, strong_consistency = self.get_num_nodes(idx)
                num = 0
                for _session in sessions:
                    if outer.query_user(_session, _userid, val, read_cl, check_ret=strong_consistency):
                        num = num + 1
                assert num >= write_nodes, "Failed to read value from sufficient number of nodes, required %d but  got %d - [%d, %s]" % (write_nodes, num, _userid, val)

            for userid in range(start, end):
                age = 30
                for session_idx, session in enumerate(sessions):
                    outer.insert_user(session, userid, age, write_cl, serial_cl)
                    check_all_sessions(session_idx, userid, age)
                    if serial_cl is None:
                        age = age + 1
                for session_idx, session in enumerate(sessions):
                    outer.update_user(session, userid, age, write_cl, serial_cl, age - 1)
                    check_all_sessions(session_idx, userid, age)
                    age = age + 1
                outer.delete_user(sessions[0], userid, write_cl)
                check_all_sessions(session_idx, userid, None)

        def validate_counters(self):
            """
            Second validation function: update the counters table sending different values to different sessions
            and check that when strong_consistency is true (R + W > N) we read back the latest value from all sessions.
            If strong_consistency is false we instead check that we read back the latest value from at least
            the number of nodes we wrote to.
            """
            outer = self.outer
            sessions = self.sessions
            start = self.start
            end = self.end
            write_cl = self.write_cl
            read_cl = self.read_cl
            serial_cl = self.serial_cl
            session_nodes = self.outer.cluster.nodelist()
            ips = [get_ip_from_node(node) for node in session_nodes]

            def check_all_sessions(session_idx, counter_id, val):
                write_nodes, _, strong_consistency = self.get_num_nodes(session_idx)
                num = 0
                messages = []
                for _session in sessions:
                    ok, msg = outer.query_counter(_session, counter_id, val, read_cl, check_ret=strong_consistency)
                    if ok:
                        num = num + 1
                    if msg:
                        logger.debug(f"{self.test_name}: {msg}")
                    messages.append(msg)
                assert num >= write_nodes, "Failed to read value from sufficient number of nodes, required %d but got %d - [%d, %s]\n\n%s" % (write_nodes, num, counter_id, val, "\n".join(messages))

            def wait_for_bg_writes_to_complete(node_ip, timeout=30):
                started = time.time()
                metric_name = "background_writes{"
                while get_node_metrics(node_ip, [metric_name])[metric_name] != 0.0:
                    assert (time.time() - started) < timeout, "Timed out waiting for background writes to settle."
                    time.sleep(0.1)

            for idx in range(start, end):
                counter_value = outer.read_counter(sessions[0], idx, ConsistencyLevel.ALL)
                for session_idx, session in enumerate(sessions):
                    counter_value = counter_value + 1
                    outer.update_counter(session, idx, write_cl, serial_cl)
                    check_all_sessions(session_idx, idx, counter_value)
                    # wait for background writes to complete so the new counter value
                    # propagates to all of the other nodes see: https://github.com/scylladb/scylladb/issues/10479
                    wait_for_bg_writes_to_complete(ips[session_idx])

    def _run_test_function_in_parallel(self, valid_fcn, nodes, rf_factors, combinations):
        """
        Run a test function in parallel.
        """
        self._start_cluster(save_sessions=True)

        input_queue = queue.Queue()
        exceptions_queue = queue.Queue()

        def run():
            while not input_queue.empty():
                test_accuracy_obj = None
                try:
                    test_accuracy_obj = TestAccuracy.Validation(self, self.sessions, nodes, rf_factors, *input_queue.get(block=False))
                    valid_fcn(test_accuracy_obj)
                except queue.Empty:
                    pass
                except Exception:  # noqa: BLE001
                    exceptions_queue.put((sys.exc_info(), test_accuracy_obj.test_name if test_accuracy_obj else ""))

        start = 0
        num_keys = 50
        for combination in combinations:
            input_queue.put((start, start + num_keys, *combination))
            start = start + num_keys

        threads = []
        number_of_threads = 8
        executor = ProcessPoolExecutor(max_workers=number_of_threads)
        for _ in range(number_of_threads):
            thread = executor.submit(run)
            threads.append(thread)

        logger.info("Waiting for workers to complete")
        while exceptions_queue.empty():
            time.sleep(0.1)
            if len(list(filter(lambda t: t.running(), threads))) == 0:
                break

        if not exceptions_queue.empty():
            output = ""
            while not exceptions_queue.empty():
                exc_info, test_name = exceptions_queue.get()
                output += f"Failed in {test_name}:\n\n"
                output += "\n".join(traceback.format_exception(*exc_info))
            pytest.fail(output)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_simple_strategy_users(self):
        """
        Test for a single datacenter, users table, only the each quorum reads.
        """
        self.nodes = 5
        self.rf_value = 3

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
            # These are multi-DC consitency levels that should default to
            # quorum calls
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
        ]

        logger.info("Testing single dc, users")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, [self.nodes], [self.rf_value], combinations)

    @pytest.mark.require("#1117")
    def test_simple_strategy_each_quorum_users(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, users table, only the each quorum reads.
        """
        self.nodes = 5
        self.rf_value = 3

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        logger.info("Testing single dc, users, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, [self.nodes], [self.rf_value], combinations)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_network_topology_strategy_users(self):
        """
        Test for multiple datacenters, users table.
        """
        self.nodes = [3, 3]
        self.rf_value = OrderedDict([("dc1", 3), ("dc2", 3)])

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            #            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_SERIAL),
            #            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
            #            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL),
        ]

        logger.info("Testing multiple dcs, users")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, self.nodes, self.rf_value.values(), combinations)

    @pytest.mark.require("#1117")
    def test_network_topology_strategy_each_quorum_users(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a multiple datacenters, users table, only the each quorum
        reads.
        """
        self.nodes = [3, 3]
        self.rf_value = OrderedDict([("dc1", 3), ("dc2", 3)])

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        logger.info("Testing multiple dcs, users, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, self.nodes, self.rf_value.values(), combinations)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    @pytest.mark.dtest_debug
    def test_simple_strategy_counters(self):
        """
        Test for a single datacenter, counters table.
        """
        self.nodes = 3
        self.rf_value = 3

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
            # These are multi-DC consitency levels that should default to
            # quorum calls
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
        ]

        logger.info("Testing single dc, counters")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, [self.nodes], [self.rf_value], combinations)

    @pytest.mark.require("#1117")
    def test_simple_strategy_each_quorum_counters(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, counters table, only the each quorum
        reads.
        """
        self.nodes = 3
        self.rf_value = 3

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        logger.info("Testing single dc, counters, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, [self.nodes], [self.rf_value], combinations)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_network_topology_strategy_counters(self):
        """
        Test for multiple datacenters, counters table.
        """
        self.nodes = [3, 3]
        self.rf_value = OrderedDict([("dc1", 3), ("dc2", 3)])

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
        ]

        logger.info("Testing multiple dcs, counters")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, self.nodes, self.rf_value.values(), combinations)

    @pytest.mark.require("#1117")
    def test_network_topology_strategy_each_quorum_counters(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for multiple datacenters, counters table, only the each quorum
        reads.
        """
        self.nodes = [3, 3]
        self.rf_value = OrderedDict([("dc1", 3), ("dc2", 3)])

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        logger.info("Testing multiple dcs, counters, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, self.nodes, self.rf_value.values(), combinations)


@pytest.mark.dtest_full
class TestConsistency(TestHelper):
    def test_short_read(self):
        """
        @jira_ticket CASSANDRA-9460
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=3)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session=session, name="ks", rf=3)
        create_cf(session=session, name="cf", read_repair=0.0)

        # Repeat this test 3 times to make it more easy to spot a null pointer
        # exception caused by a race, see CASSANDRA-9460
        for _ in range(3):
            # insert 9 columns in one row
            insert_columns(session, 0, 9)

            # Deleting 3 first columns with a different node dead each time
            self.stop_delete_and_restart(1, 0)
            self.stop_delete_and_restart(2, 1)
            self.stop_delete_and_restart(3, 2)

            # Query 3 firsts columns
            session = self.patient_cql_connection(node1, "ks")
            query = SimpleStatement("SELECT c, v FROM cf WHERE key='k0' LIMIT 3", consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))
            res = rows
            assert len(res) == 3, "Expecting 3 values, got %d (%s)" % (len(res), str(res))
            # value 0, 1 and 2 have been deleted
            for i in range(1, 4):
                assert res[i - 1][1] == "value%d" % (i + 2), "Expecting value%d, got %s (%s)" % (i + 2, res[i - 1][1], str(res))

            truncate_statement = SimpleStatement("TRUNCATE cf", consistency_level=ConsistencyLevel.QUORUM)
            session.execute(truncate_statement)

    def test_short_read_delete(self):
        """Test short reads ultimately leaving no columns alive [#4000]"""
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        rf = 3
        cluster_topology = generate_cluster_topology(rack_num=rf)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, *_ = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session=session, name="cf", read_repair=0.0)
        # insert 2 columns in one row
        insert_columns(session, 0, 2)

        # Delete the row while first node is dead
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, "ks")

        query = SimpleStatement("DELETE FROM cf WHERE key='k0'", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        node1.start(wait_other_notice=True)

        # Query first column
        session = self.patient_cql_connection(node1, "ks")

        query = SimpleStatement("SELECT c, v FROM cf WHERE key='k0' LIMIT 1", consistency_level=ConsistencyLevel.QUORUM)
        res = list(session.execute(query))
        assert len(res) == 0, res

    def test_short_read_quorum_delete(self):
        """
        @jira_ticket CASSANDRA-8933
        """
        cluster = self.cluster
        # Consider however 3 nodes A, B, C (RF=3), and following sequence of
        # operations (all done at QUORUM):
        cluster_topology = generate_cluster_topology(rack_num=3)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY(id, v)) WITH read_repair_chance = 0.0")
        # we write 1 and 2 in a partition: all nodes get it.
        session.execute(SimpleStatement("INSERT INTO t (id, v) VALUES (0, 1)", consistency_level=ConsistencyLevel.ALL))
        session.execute(SimpleStatement("INSERT INTO t (id, v) VALUES (0, 2)", consistency_level=ConsistencyLevel.ALL))

        # we delete 1: only A and C get it.
        node2.flush()
        node2.stop(wait_other_notice=True)
        session.execute(SimpleStatement("DELETE FROM t WHERE id = 0 AND v = 1", consistency_level=ConsistencyLevel.QUORUM))
        node2.start(wait_other_notice=True)

        # we delete 2: only B and C get it.
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, "ks")
        session.execute(SimpleStatement("DELETE FROM t WHERE id = 0 AND v = 2", consistency_level=ConsistencyLevel.QUORUM))
        node1.start(wait_other_notice=True)
        session = self.patient_cql_connection(node1, "ks")

        # we read the first row in the partition (so with a LIMIT 1) and A and
        # B answer first.
        node3.flush()
        node3.stop(wait_other_notice=True)
        assert_none(session, "SELECT * FROM t WHERE id = 0 LIMIT 1", cl=ConsistencyLevel.QUORUM)

    @pytest.mark.dtest_debug
    def test_readrepair(self):
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        if self.DISABLE_VNODES:
            cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate(cluster_topology, tokens=tokens).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_c1c2_table(session, read_repair=1.0)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ONE)

        node2.start(wait_other_notice=True)

        # query everything to cause RR
        for key in range(10000):
            query_c1c2(session=session, key=key, consistency=ConsistencyLevel.QUORUM)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been repaired
        session = self.patient_cql_connection(node2, keyspace="ks")
        for key in range(10000):
            query_c1c2(session=session, key=key, consistency=ConsistencyLevel.ONE)

    def test_short_read_reversed(self):
        """
        @jira_ticket CASSANDRA-9460
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=3)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session=session, name="cf", read_repair=0.0)

        # Repeat this test 3 times to make it more easy to spot a null pointer
        # exception caused by a race, see CASSANDRA-9460
        for _ in range(3):
            # insert 9 columns in one row
            insert_columns(session, 0, 9)

            # Deleting 3 last columns with a different node dead each time
            self.stop_delete_and_restart(1, 6)
            self.stop_delete_and_restart(2, 7)
            self.stop_delete_and_restart(3, 8)

            # Query 3 firsts columns
            session = self.patient_cql_connection(node1, "ks")
            query = SimpleStatement("SELECT c, v FROM cf WHERE key='k0' ORDER BY c DESC LIMIT 3", consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))
            res = rows
            assert len(res) == 3, "Expecting 3 values, got %d (%s)" % (len(res), str(res))
            # value 6, 7 and 8 have been deleted
            for i in range(3):
                assert res[i][1] == "value%d" % (5 - i), "Expecting value%d, got %s (%s)" % (5 - i, res[i][1], str(res))

            truncate_statement = SimpleStatement("TRUNCATE cf", consistency_level=ConsistencyLevel.QUORUM)
            session.execute(truncate_statement)

    @pytest.mark.dtest_debug  # https://github.com/scylladb/scylla/issues/4384
    def test_quorum_available_during_failure(self):
        cl_value = ConsistencyLevel.QUORUM
        rf_value = 3

        logger.info("Creating a ring")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=rf_value)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        if self.DISABLE_VNODES:
            cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        else:
            tokens = cluster.balanced_tokens(3)
            cluster.populate(cluster_topology, tokens=tokens).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()[:2]

        logger.info("Set to talk to node 2")
        session = self.patient_cql_connection(node2)
        create_ks(session, "ks", rf_value)
        create_c1c2_table(session)

        logger.info("Generating some data")
        insert_c1c2(session, n=100, consistency=cl_value)

        logger.info("Taking down node1")
        node1.stop(wait_other_notice=True)

        logger.info("Reading back data.")
        for key in range(100):
            query_c1c2(session=session, key=key, consistency=cl_value)

    def stop_delete_and_restart(self, node_number, column):
        to_stop = self.cluster.nodes["node%d" % node_number]
        next_node = self.cluster.nodes["node%d" % (((node_number + 1) % 3) + 1)]
        to_stop.flush()
        to_stop.stop(wait_other_notice=True)
        session = self.patient_cql_connection(next_node, "ks")
        query = "BEGIN BATCH "
        query = query + "DELETE FROM cf WHERE key='k0' AND c='c%06d'; " % column
        query = query + "DELETE FROM cf WHERE key='k0' AND c='c2'; "
        query = query + "APPLY BATCH;"
        simple_query = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(simple_query)

        to_stop.start(wait_other_notice=True)

    def test_data_query_digest(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, c int, r int, primary key (p, c))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (0, 1, 1)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (0, 2, 1)", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node1")
        node2.stop()
        session1.execute(SimpleStatement("delete from ks.cf1 where p = 0 and c = 1", consistency_level=ConsistencyLevel.ONE))

        logger.info("Updating node2")
        node2.start(wait_for_binary_proto=True)
        node1.stop()

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (0, 2, 2)", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True)
        logger.info("Node 1 started")

        query = SimpleStatement("select r from ks.cf1 where p = 0 limit 1", consistency_level=ConsistencyLevel.ALL)
        res = list(session2.execute(query))

        assert len(res) == 1, "Expecting 1 row, got %d (%s)" % (len(res), str(res))
        assert len(res[0]) == 1, "Expecting 1 cell, got %d (%s)" % (len(res[0]), str(res[0]))
        assert res[0][0] == 2, "Expecting value 2, got %s" % str(res[0][0])

    def test_incomplete_result_test_partition_limit(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
        cluster.set_configuration_options(values={"start_rpc": True})
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, c text, r text, primary key (p, c))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '1', '0')", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (2, '1', '1')", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node1")
        node2.stop()
        session1.execute(SimpleStatement("delete from ks.cf1 where p = 1 and c = '1'", consistency_level=ConsistencyLevel.ONE))

        logger.info("Updating node2")
        node2.start()
        node1.stop()

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (2, '1', '2')", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True)
        logger.info("Node 1 started")

        query = SimpleStatement("select r from ks.cf1 limit 1", consistency_level=ConsistencyLevel.ALL)
        res = list(session2.execute(query))

        assert len(res) == 1, "Expecting 1 row, got %d (%s)" % (len(res), str(res))
        assert len(res[0]) == 1, "Expecting 1 cell, got %d (%s)" % (len(res[0]), str(res[0]))
        assert res[0][0] == "2", "Expecting value 2, got %s" % str(res[0][0])

    def test_incomplete_result_test_per_partition_row_limit(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
        cluster.set_configuration_options(values={"start_rpc": True})
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, c text, r text, primary key (p, c))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '1', '0')", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '2', '1')", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node1")
        node2.stop()
        session1.execute(SimpleStatement("delete from ks.cf1 where p = 1 and c = '1'", consistency_level=ConsistencyLevel.ONE))

        logger.info("Updating node2")
        node2.start()
        node1.stop()

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '2', '2')", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True)
        logger.info("Node 1 started")

        query = SimpleStatement("select c from ks.cf1 limit 1", consistency_level=ConsistencyLevel.ALL)
        res = list(session2.execute(query))

        assert len(res) == 1, "Expecting 1 row, got %d (%s)" % (len(res), str(res))
        assert len(res[0]) == 1, "Expecting 1 cell, got %d (%s)" % (len(res[0]), str(res[0]))
        assert res[0][0] == "2", "Expecting value 2, got %s" % str(res[0][0])

    def test_empty_reconciled_result(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, r int, primary key (p))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (1, 1)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (2, 2)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (3, 3)", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node1")
        node2.stop(wait_other_notice=True)
        session1.execute(SimpleStatement("delete from ks.cf1 where p = 1", consistency_level=ConsistencyLevel.ONE))

        logger.info("Updating node2")
        node1.stop()
        node2.start(wait_for_binary_proto=True)

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("delete from ks.cf1 where p = 2", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.info("Node 1 started")

        query = SimpleStatement("select r from ks.cf1 limit 1", consistency_level=ConsistencyLevel.ALL, fetch_size=0)
        res = list(session2.execute(query))

        assert len(res) == 1, "Expecting 1 row, got %d (%s)" % (len(res), str(res))
        assert len(res[0]) == 1, "Expecting 1 cell, got %d (%s)" % (len(res[0]), str(res[0]))
        assert res[0][0] == 3, "Expecting value 3, got %s" % str(res[0][0])

    def test_empty_reconciled_result_with_paging(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, r int, primary key (p))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (1, 1)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (2, 2)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (3, 3)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (4, 4)", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node1")
        node2.stop(wait_other_notice=True)
        session1.execute(SimpleStatement("delete from ks.cf1 where p = 1", consistency_level=ConsistencyLevel.ONE))

        logger.info("Updating node2")
        node1.stop()
        node2.start(wait_for_binary_proto=True)

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("delete from ks.cf1 where p = 2", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.info("Node 1 started")

        future = session2.execute_async(SimpleStatement("select r from ks.cf1 limit 2", consistency_level=ConsistencyLevel.ALL, fetch_size=1))
        page_fetcher = PageFetcher(future).request_all()
        all_pages = page_fetcher.num_results_all()

        assert sum(all_pages) == 2
        assert len(all_pages) == 2

    def test_short_read_partitions(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
        cluster.set_configuration_options(values={"start_rpc": True})
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, c text, r text, primary key (p, c))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '1', '1')", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (2, '1', '2')", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (3, '1', '3')", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (4, '1', '4')", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node1")
        node2.stop(wait_other_notice=True)
        session1.execute(SimpleStatement("delete from ks.cf1 where p = 1", consistency_level=ConsistencyLevel.ONE))

        logger.info("Updating node2")
        node1.stop()
        node2.start(wait_for_binary_proto=True)

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("delete from ks.cf1 where p = 2", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.info("Node 1 started")

        query = SimpleStatement("select r from ks.cf1 limit 2", consistency_level=ConsistencyLevel.ALL)
        res = list(session2.execute(query))

        assert len(res) == 2, "Expecting 2 rows, got %d (%s)" % (len(res), str(res))

        assert len(res[0]) == 1, "Expecting 1 cell, got %d (%s)" % (len(res[0]), str(res[0]))
        assert res[0][0] == "4", "Expecting value 4, got %s" % str(res[0][0])

        assert len(res[1]) == 1, "Expecting 1 cell, got %d (%s)" % (len(res[1]), str(res[1]))
        assert res[1][0] == "3", "Expecting value 3, got %s" % str(res[1][0])

    def test_reaching_end_after_retry(self):
        logger.info("Create cluster")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        logger.info("Prepare column family")
        session1 = self.patient_cql_connection(node1)
        create_ks(session1, "ks", 2)
        session1.execute("create table ks.cf1 (p int, r int, primary key (p))")

        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (0, 0)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (1, 1)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (2, 2)", consistency_level=ConsistencyLevel.ALL))
        session1.execute(SimpleStatement("insert into ks.cf1 (p, r) values (3, 3)", consistency_level=ConsistencyLevel.ALL))

        logger.info("Updating node2")
        node1.stop()

        session2 = self.patient_cql_connection(node2)
        session2.execute(SimpleStatement("delete from ks.cf1 where p = 0", consistency_level=ConsistencyLevel.ONE))
        session2.execute(SimpleStatement("delete from ks.cf1 where p = 1", consistency_level=ConsistencyLevel.ONE))
        session2.execute(SimpleStatement("delete from ks.cf1 where p = 2", consistency_level=ConsistencyLevel.ONE))
        session2.execute(SimpleStatement("insert into ks.cf1 (p, r) values (4, 4)", consistency_level=ConsistencyLevel.ONE))
        session2.execute(SimpleStatement("insert into ks.cf1 (p, r) values (5, 5)", consistency_level=ConsistencyLevel.ONE))

        logger.info("Querying whole cluster")
        node1.start(wait_other_notice=True)
        logger.info("Node 1 started")

        query = SimpleStatement("select r from ks.cf1 limit 3", consistency_level=ConsistencyLevel.ALL)
        res = list(session2.execute(query))

        assert len(res) == 3, "Expecting 3 rows, got %d (%s)" % (len(res), str(res))

        assert res[0][0] == 5, "Expecting value 3, got %s" % str(res[0][0])
        assert res[1][0] == 4, "Expecting value 4, got %s" % str(res[0][0])
        assert res[2][0] == 3, "Expecting value 5, got %s" % str(res[0][0])
