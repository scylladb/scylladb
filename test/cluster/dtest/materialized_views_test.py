import collections
import logging
import os
import random
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import partial
from multiprocessing import Lock, Process, Queue, cpu_count

import pytest
from cassandra import ConsistencyLevel, WriteFailure, consistency_value_to_name
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from ccmlib.node import Node, NodetoolError
from ccmlib.scylla_cluster import ScyllaCluster
from packaging.version import Version

from dtest_class import Tester, create_cf, create_ks, wait_for
from tools.assertions import (
    assert_all,
    assert_all_or_none,
    assert_crc_check_chance_equal,
    assert_invalid,
    assert_none,
    assert_one,
    assert_row_count,
    assert_row_count_in_select,
    assert_two_queries_equal,
    assert_two_queries_equal_ignore_order,
    assert_unavailable,
)
from tools.cluster_topology import generate_cluster_topology_based_rf, generate_rack_topology_based_rf
from tools.data import (
    create_c1c2_table,
    insert_c1c2,
    rows_to_list,
    run_in_parallel,
    run_query_with_data_processing,
)
from tools.files import get_node_cf_dir, remove_files_in_folder
from tools.group0_and_token_ring import wait_for_token_ring_and_group0_consistency
from tools.marks import issue_closed, issue_open, unmark, with_feature
from tools.misc import flush_by_node, remove_node
from tools.retrying import retrying
from tools.session import get_supported_features
from tools.sla import Role, ServiceLevel
from tools.stress import assert_cs_success, format_cs_output, run_stress_with_tailed_output
from tools.tables_view_manager import (
    MaterializedViewManager,
    TableManager,
    sync_hinted_handoff,
    wait_for_view,
    wait_for_view_update_generation,
)

logger = logging.getLogger(__name__)

# taken out gating until sorting out:
# https://github.com/scylladb/scylladb/issues/15046
# pytestmark = pytest.mark.next_gating

# CASSANDRA-10978. Migration wait (in seconds) to use in bootstrapping tests. Needed to handle
# pathological case of flushing schema keyspace for multiple data directories. See CASSANDRA-6696
# for multiple data directory changes and CASSANDRA-10421 for compaction logging that must be
# written.
MIGRATION_WAIT = 5


class CommonUtils(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_setup_timeouts(self, fixture_dtest_setup):
        if not "debug_mode" in self.__dict__.keys():
            self.debug_mode = isinstance(fixture_dtest_setup.cluster, ScyllaCluster) and fixture_dtest_setup.cluster.scylla_mode == "debug"
            self.session_timeout = 120
            if self.debug_mode:
                self.session_timeout *= 3

    @staticmethod
    def eventually(fun, trials=64):
        """
        Runs a function until it succeeds or the trial limit is reached
        """
        assert trials > 0
        for i in range(trials - 1):
            try:
                return fun()
            except Exception as e:  # noqa: BLE001
                logger.debug(f"{fun.__name__} [{i + 1}/{trials}]: {e}: will retry in 1 second")
                time.sleep(1)
        return fun()

    def eventually_assert_one(self, *args):
        """
        Shortcut for eventually(lambda: assert_one(args))
        """
        return self.eventually(lambda: assert_one(*args))

    def eventually_assert_none(self, *args):
        """
        Shortcut for eventually(lambda: assert_one(args))
        """
        return self.eventually(lambda: assert_none(*args))

    @staticmethod
    def stress_errors_options():
        return ["-errors", "retries=10", "delay-policy=exponential", "min-delay-ms=20", "max-delay-ms=20000"]

    def stop_cluster(self):
        # Currently, (See issues #4019 and #3966), shutdown may hang for up
        # 5 minutes (the timeout set in storage_proxy::send_to_endpoint())
        # while a view build step is stuck trying to communicate with another
        # node we previously killed. So we need to increase stop()'s timeout to
        # be more than 5 minutes (=300 seconds).
        self.cluster.stop(wait_seconds=360)

    def prepare(  # noqa: PLR0913
        self,
        user_table: bool = False,
        rf: int | list | dict = 1,
        options: dict | None = None,
        nodes: int | list | dict = 3,
        fetch_size: int | None = None,
        jvm_args: list | None = None,
        tablets: int | None = None,
        **kwargs,
    ):
        cluster = self.cluster
        cluster_topology = nodes if isinstance(nodes, dict) else generate_cluster_topology_based_rf(nodes=nodes, rf=rf, dc_name_prefix="dc", rack_name_prefix="RAC")
        cluster.populate(cluster_topology)

        self.total_rf = sum([v for v in rf.values()]) if isinstance(rf, dict) else rf
        if options:
            logger.debug(f"Setting cluster configuration options: {options}")
            cluster.set_configuration_options(values=options)
        cluster.start(jvm_args=jvm_args, wait_other_notice=True, wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, **kwargs)
        if fetch_size:
            session.default_fetch_size = fetch_size
        create_ks(session, "ks", rf, tablets=tablets)

        if user_table:
            session.execute("CREATE TABLE users (username varchar, password varchar, gender varchar, session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username));")

            # create a materialized view
            session.execute("CREATE MATERIALIZED VIEW users_by_state AS SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)")

        self.fixture_dtest_setup.ignore_log_patterns += [
            r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: exceptions::mutation_write_failure_exception",
            r".*view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*seastar::gate_closed_exception",
        ]

        return session

    def update_view(self, session, query, flush, compact=False):
        session.execute(query)
        # Scylla doesn't rely on the batchlog
        # self._replay_batchlogs()
        if flush:
            self.cluster.flush()
        if compact:
            self.cluster.compact()

    @staticmethod
    def _insert_data(session):
        # insert data
        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

    def _replay_batchlogs(self):
        logger.debug("Replaying batchlog on all nodes")
        for node in self.cluster.nodelist():
            if node.is_running():
                node.nodetool("replaybatchlog")


@pytest.mark.dtest_full
@pytest.mark.use_cassandra_stress
class TestMaterializedViews(CommonUtils):
    """
    Test materialized views implementation.
    @jira_ticket CASSANDRA-6477
    """

    def test_stop_node_during_mv_insert_4_nodes(self):
        """Test stopping node during MV inserts
        Test starts with a starting size 4 and stops one node during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action="stop", exclude_errors=["mutation_write_timeout_exception"])

    def test_stop_node_during_mv_insert_3_nodes(self):
        """Test stopping node during MV inserts
        Test starts with a starting size 3 and stops one node during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=3, node_action="stop", exclude_errors=["mutation_write_timeout_exception"])

    def test_remove_node_during_mv_insert_4_nodes(self):
        """Test removing node during MV inserts
        Test starts with a starting size 4 and removes one node during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action="remove", exclude_errors=["mutation_write_timeout_exception"])

    def test_decommission_node_during_mv_insert_4_nodes(self):
        """Test removing node during MV inserts
        Test starts with a starting size 4 and removes one node during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action="decommission", exclude_errors=["mutation_write_timeout_exception"])

    # The test scenario is not supported with tablets. It starts with 3 nodes, RF=3, and then
    # removes one node. Removing the node is rejected because there are no sufficient
    # replicas to satisfy the RF.
    # Covered by test_remove_node_during_mv_insert_4_nodes when tablets are enabled
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_remove_node_during_mv_insert_3_nodes(self):
        """Test removing node during MV inserts
        Test starts with a starting size 3 and removes one node during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=3, node_action="remove", exclude_errors=["mutation_write_timeout_exception"])

    @unmark.next_gating  # stress failing with hints overload: https://github.com/scylladb/scylla-dtest/issues/3372
    def test_double_node_failure_during_mv_insert_4_nodes(self):
        """Test stopping 2 nodes during MV inserts
        Test starts with a starting size 4 and stops 2 nodes during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action="stop", duration="2m", double_failure=True, exclude_errors=["mutation_write_timeout_exception"])

    def test_double_node_failure_during_mv_insert_3_nodes(self):
        """Test stopping 2 nodes during MV inserts
        Test starts with a starting size 4 and stops 2 nodes during inserts into base table that cause to update materialized view as well
        (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=3, node_action="stop", duration="2m", double_failure=True, exclude_errors=["mutation_write_timeout_exception"])

    def _run_node_failure_during_mv_stress_insert(  # noqa: PLR0913
        self,
        rf,
        nodes,
        node_action,
        delay=30,
        duration="1m",
        double_failure=False,
        exclude_errors=None,
    ):
        configuration_options = {"range_request_timeout_in_ms": self.count_request_timeout * 1000}
        session = self.prepare(rf=rf, nodes=nodes, options=configuration_options, request_timeout=self.count_request_timeout)
        mv_profile = os.path.abspath(os.path.join("test_data", "cassandra-mv-profile", "cs_mv_profile.yaml"))

        node1 = self.cluster.nodelist()[0]
        n = 10000
        results = node1.stress(stress_options=["write", "cl=QUORUM", f"n={n}", "-schema replication(factor=3)", "-mode cql3 native", "-rate threads=10", f"-pop seq=1..{n}", *self.stress_errors_options()])
        logger.debug(format_cs_output(results))
        assert_cs_success(results)

        self.fixture_dtest_setup.ignore_log_patterns += [
            r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: seastar::broken_promise",
            r"sstable - failed reading index .* std::bad_alloc",
            r"storage_proxy - exception during mutation write",
        ]

        other_nodes = self.cluster.nodelist()
        nodes_to_start = [other_nodes.pop(1)]
        if double_failure and len(self.cluster.nodelist()) > 2:
            nodes_to_start.append(other_nodes.pop(1))

        proc_functions = [
            {
                "func": lambda: run_stress_with_tailed_output(
                    node1, ["user", f"profile={mv_profile}", "no-warmup", "cl=ONE", f"duration={duration}", "ops(insert=1,read1=1,read2=1,read3=1)", "-mode cql3  native", "-rate threads=10", *self.stress_errors_options()]
                )
            },
            {
                "func": lambda: run_stress_with_tailed_output(
                    node1, ["mixed", "cl=ONE", f"duration={duration}", "-schema replication(factor=3)", "-mode cql3 native", "-rate threads=10", f"-pop dist=UNIFORM(1..{n})", "-log interval=5", *self.stress_errors_options()]
                )
            },
            {"func": self._node_action_with_delay, "args": (node_action, nodes_to_start[0]), "kwargs": {"delay": delay, "other_nodes": other_nodes}},
        ]
        if double_failure and len(self.cluster.nodelist()) > 2:
            proc_functions.append({"func": self._node_action_with_delay, "args": (node_action, nodes_to_start[1]), "kwargs": {"delay": delay + 10, "other_nodes": other_nodes}})
        run_in_parallel(proc_functions)

        # Index will not finish building, because view building underneath is paused until updates can be sent.
        if node_action == "stop":
            self._start_nodes(nodes_to_start)

        wait_for_view(cluster=self.cluster, session=session, ks="mview", view="users_by_first_name")
        wait_for_view(cluster=self.cluster, session=session, ks="mview", view="users_by_last_name")

        self.eventually(lambda: self._validate_cs_results(node1, exclude_errors, node_action, double_failure))

    def test_multidc_dc_failure_during_mv_insert(self):
        """Test stopping all DC nodes during MV inserts
        Test starts with a starting size: two DCs with 2 nodes each, and stops 2 nodes of second DC during inserts
        into base
        table that cause to update materialized view as well (using cs_mv_profile.yaml profile).
        Validate the log has no errors.
        Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        configuration_options = {"range_request_timeout_in_ms": self.count_request_timeout * 1000}
        session = self.prepare(rf={"dc1": 2, "dc2": 1}, nodes=[3, 3], options=configuration_options, request_timeout=self.count_request_timeout)
        mv_profile = os.path.abspath(os.path.join("test_data", "cassandra-mv-profile", "cs_mv_multidc_profile.yaml"))

        node1_dc1 = next(node for node in self.cluster.nodelist() if node.data_center == "dc1")
        proc_functions = [
            {
                "func": lambda: run_stress_with_tailed_output(
                    node1_dc1, ["user", f"profile={mv_profile}", "no-warmup", "cl=QUORUM", "duration=2m", "ops(insert=3,read1=1,read2=1,read3=1)", "-mode cql3  native", "-rate threads=10", *self.stress_errors_options()]
                )
            },
            {"func": self._stop_few_nodes, "kwargs": {"delay": 30, "by_dc_name": "dc2"}},
        ]
        run_in_parallel(proc_functions)

        # Index will not finish building, because view building underneath is paused until updates can be sent.
        for node in self.cluster.nodelist():
            if node.data_center == "dc2":
                logger.debug(f"Start node {node.name}")
                node.start(wait_for_binary_proto=True)

        wait_for_view(cluster=self.cluster, session=session, ks="mview", view="users_by_first_name")
        wait_for_view(cluster=self.cluster, session=session, ks="mview", view="users_by_last_name")

        self.eventually(lambda: self._validate_cs_results(node1_dc1, exclude_errors=["mutation_write_timeout_exception"], node_action="", double_failure=True))

    def _truncate_base_during_mv_insert(self, auto_snapshot: bool):
        """Test truncating the base table during MV inserts
        Validate the log has no errors and
        that materialized views building completes.
        We can't validate the result data as we don't
        know exactly what was truncated.
        """
        session = self.prepare(nodes=3, rf=3, options={"auto_snapshot": auto_snapshot})
        mv_profile = os.path.abspath(os.path.join("test_data", "cassandra-mv-profile", "cs_mv_profile.yaml"))

        node1 = self.cluster.nodelist()[0]
        proc_functions = [
            {"func": node1.stress, "args": [["user", f"profile={mv_profile}", "no-warmup", "cl=QUORUM", "duration=1m", "ops(insert=3,read1=1,read2=1,read3=1)", "-mode cql3  native", "-rate threads=10", *self.stress_errors_options()]]},
            {"func": self._truncate_table, "kwargs": {"ks": "mview", "table": "users", "delay": 30}},
        ]
        run_in_parallel(proc_functions)

        wait_for_view(cluster=self.cluster, session=session, ks="mview", view="users_by_first_name")
        wait_for_view(cluster=self.cluster, session=session, ks="mview", view="users_by_last_name")

    def test_truncate_base_during_mv_insert_test_with_auto_snapshot(self):
        self._truncate_base_during_mv_insert(auto_snapshot=True)

    @pytest.mark.dtest_debug
    def test_truncate_base_during_mv_insert_test_without_auto_snapshot(self):
        self._truncate_base_during_mv_insert(auto_snapshot=False)

    def _node_action_with_delay(self, action, node, delay=0, wait=True, wait_other_notice=True, other_nodes=None, gently=True):  # noqa: PLR0913
        """
        :param action: expected values: stop, remove
        :param action: str
        """
        if action not in ["stop", "remove", "decommission", "restart"]:
            assert False, "Unsupported node action"

        if delay:
            logger.debug(f"Sleep for {delay} seconds")
            time.sleep(delay)

        logger.debug(f"START: {action} node {node.name}")
        if action == "stop":
            node.stop(wait=wait, wait_other_notice=wait_other_notice, other_nodes=other_nodes, gently=gently)
        elif action == "restart":
            node.stop(wait=wait, wait_other_notice=wait_other_notice, other_nodes=other_nodes, gently=gently)
            time.sleep(delay if delay else 1)
            node.start(wait_other_notice=wait_other_notice)
        elif action == "remove":
            remove_node(self.cluster, node, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
        elif action == "decommission":
            # Explicitly call node.decommission because it will change the node's status to DECOMMISSIONED,
            # unlike node.nodetool("decommission"). This affects the is_live() status, some tests depend
            # on it being correct.
            new_node_index = len(self.cluster.nodelist()) + 1
            node.decommission()
            logger.debug("START add new node")
            self._add_new_node(new_node_index=new_node_index)
            logger.debug("FINISH add new node")
        else:
            node.nodetool(action)

        logger.debug(f"FINISH: {action} node {node.name}")

    def _stop_few_nodes(  # noqa: PLR0913
        self,
        by_dc_name="",
        by_node_names=None,
        delay=0,
        wait=True,
        wait_other_notice=False,
        gently=True,
    ):
        if by_node_names is None:
            by_node_names = []
        if delay:
            logger.debug(f"Sleep for {delay} seconds")
            time.sleep(delay)

        other_nodes = self.cluster.nodelist()
        stop_nodes = []
        for node in self.cluster.nodelist():
            if (by_dc_name and node.data_center == by_dc_name) or (by_node_names and node.name in by_node_names):
                stop_nodes.append(node)
                other_nodes.remove(node)

        for node in stop_nodes:
            self._node_action_with_delay("stop", node, wait=wait, wait_other_notice=wait_other_notice, other_nodes=other_nodes, gently=gently)

    def _truncate_table(self, ks="mview", table="users", delay=0, table_creation_timeout=300):
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        # The table may be created by a concurrent workload (e.g. cassandra-stress from a profile),
        # whose startup time varies. Wait for the table to exist before counting the delay,
        # so the truncate happens while the workload is running and not before it has created the schema.
        wait_for(
            lambda: bool(list(session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s", (ks, table)))),
            text=f"Waiting for table '{ks}.{table}' to be created",
            timeout=table_creation_timeout,
        )

        if delay:
            logger.debug(f"Sleep for {delay} seconds")
            time.sleep(delay)

        logger.debug(f"Truncating table '{ks}.{table}' ...")
        start = time.time()
        session.execute(f"TRUNCATE table {ks}.{table}")
        delta = time.time() - start
        logger.debug(f"Truncating table '{ks}.{table}' done in {delta:.1f} seconds")

    def test_add_dc_during_mv_insert(self):
        """Test expand cluster - add new DC during MV inserts
        Test starts with a starting size: one DCs with 4 nodes, and add new 2 nodes of second DC during inserts into base
        table that cause to update materialized view as well.
        Verify that MV records are as it exists in the base table
        Validate the log has no errors.
        """
        self._add_dc_during_mv_change("insert", 3, 4, start_prefill=1000, more_inserts=300000)

    def _validate_cs_results(self, node, exclude_errors, node_action, double_failure, cl=None, num_attempts=1):  # noqa: PLR0913
        self.check_errors(node, exclude_errors)
        session = self.patient_exclusive_cql_connection(node)
        session.execute("USE mview")
        cl = self.set_consistency_level(node_action=node_action, double_failure=double_failure, cl=cl)
        logger.debug(f"Validate data using CL={consistency_value_to_name(cl)}")
        exp_res = run_query_with_data_processing(session, "select count(*) from mview.users", consistency_level=cl, session_timeout=self.count_request_timeout)
        try:
            exp_res = int(exp_res[0].count)
        except TypeError:
            logger.debug(f"Try to select rows count from mview.users table. Expected integer vale, received: {exp_res[0].count}")
            raise
        except Exception as e:
            logger.debug(f"Try to select rows count from mview.users table. Failed with error: {e}")
            raise

        assert_row_count(session, "users_by_first_name", exp_res, consistency_level=cl, num_attempts=num_attempts)
        assert_row_count(session, "users_by_last_name", exp_res, consistency_level=cl, num_attempts=num_attempts)

    def test_add_dc_during_mv_update(self):
        """Test expand cluster - add new DC during MV inserts
        Test starts with a starting size: one DCs with 4 nodes, and add new 2 nodes of second DC during update
        existent records of base
        table that cause to update materialized view as well.
        Verify that MV records are according to the base table
        """
        self._add_dc_during_mv_change("update", 3, 4, start_prefill=4000, more_inserts=300000)

    def _add_dc_during_mv_change(self, change, rf, nodes, start_prefill, more_inserts):
        session = self.prepare(rf=rf, nodes=nodes, fetch_size=start_prefill + more_inserts * 2)
        node1 = self.cluster.nodelist()[0]
        tm = TableManager(session, self.cluster, columns={"int": {"amount": 2, "frozen": False, "value length": {"min": 1, "max": 100}}}, cl_columns={})
        tm.create_table()

        mv = MaterializedViewManager(tm)
        mv_restrict_value = 53
        mv.create_materialized_view(mv_columns={"int": {"names": [tm.column_names_list[-1]]}}, mv_pk_column={"type": "int"}, mv_where_restriction={"position": {-2: {"operator": "=", "value": mv_restrict_value}}})

        tm.prefill_table(start_prefill, data={"int": [2, 5, 12, 45, mv_restrict_value, 78, 36, 85, 98, 100]})
        query = "select id, clmn_int0, %s from {tbl}{where}{f}" % mv.mv_columns_list[0]

        exp_query = "select id, {clmn1}, {clmn2} from {tbl}{where}{f}".format(clmn1=tm.column_names_list[-1], clmn2=next(iter(mv.mv_where_restriction.keys())), tbl=tm.table_name, where="", f="")
        act_query = query.format(tbl=mv.mv_name, where="", f="")
        assert_two_queries_equal(
            session,
            exp_query,
            session,
            act_query,
            consistency_level=ConsistencyLevel.QUORUM,
            session_timeout=self.session_timeout,
            group=True,
            groupby_column1=tm.column_names_list[-1],
            groupby_column2=tm.column_names_list[-1],
            restrict_column1=next(iter(mv.mv_where_restriction.keys())),
            restrict_value1=mv_restrict_value,
        )

        if self.debug_mode:
            more_inserts //= 10

        proc_functions = [
            {"func": self._add_few_nodes, "args": (2, "dc2")},
            {
                "func": self.add_mv_records if change == "insert" else self._multiple_int_updates,
                "args": (tm, mv_restrict_value) if change == "insert" else (session, tm, tm.column_names_list[-1], next(iter(mv.mv_where_restriction.keys())), mv_restrict_value, [100, 200]),
                "kwargs": {"delay": 5, "inserts": more_inserts} if change == "insert" else {"delay": 5, "updates": 200},
            },
        ]

        run_in_parallel(proc_functions)

        # Validate data
        for node in self.cluster.nodelist():
            if node.data_center == "dc2":
                session = self.patient_exclusive_cql_connection(node, keyspace=tm.keyspace)
                assert_two_queries_equal(
                    session,
                    exp_query,
                    session,
                    act_query,
                    consistency_level=ConsistencyLevel.ALL,
                    session_timeout=self.session_timeout,
                    group=True,
                    groupby_column1=tm.column_names_list[-1],
                    groupby_column2=tm.column_names_list[-1],
                    restrict_column1=next(iter(mv.mv_where_restriction.keys())),
                    restrict_value1=mv_restrict_value,
                )

    def add_mv_records(self, tm, mv_restrict_value=None, inserts=10, delay=0):
        if delay:
            time.sleep(delay)

        _id = tm.get_max_id()
        _id = _id if not _id else _id + 1
        data = {"int": [mv_restrict_value]} if mv_restrict_value else None
        tm.prefill_table(inserts, data=data, start_id_from=_id, flush=False)

    def _multiple_int_updates(  # noqa: PLR0913
        self,
        session,
        tm,
        updated_column,
        filter_column,
        filter_value,
        update_to_boundaries,
        updates=10,
        delay=0,
    ):
        if delay:
            time.sleep(delay)

        logger.debug("Start updates")
        res = session.execute(f"select * from {tm.table_name}").current_rows
        updated_column_index = next(i for i, clmn in enumerate(res[0]._fields) if clmn == updated_column)
        for _ in range(updates):
            time.sleep(1)
            while True:
                i = random.randint(0, len(res) - 1)
                if res[i][updated_column_index] == filter_value:
                    _id = res[i].id
                    break

            tm.update_table(
                set_clause={"by name": {updated_column: random.randint(update_to_boundaries[0], update_to_boundaries[1])}},
                where_filter={"by name": {filter_column: {"operator": "=", "value": filter_value}, "id": {"operator": "=", "value": _id}}},
            )
        logger.debug("Updates were finished")

    def _add_few_nodes(self, nodes, data_center, delay=0):
        if delay:
            logger.debug(f"Sleep for {delay} seconds")
            time.sleep(delay)

        for i in range(nodes):
            logger.debug(f"Bootstrapping {i + 1} node in {data_center}")
            self._add_new_node(data_center=data_center)

    @pytest.mark.timeout(4500)
    def test_small_concurrent(self):
        """
        - Create 10 materialized views on the same base table.
        - Pre-fill the table with 2000 records. Expected same records amount in the all views
        - Validate the records count in the base table and all MVs
        - In the parallel threads run: insert 2000 new records / updates / reads
        - Validate the records count in the base table and all MVs
        - If previous validation passed - validated the data in the MVs is as in the base table
        """
        self._parallel_updates_inserts(records=2000, nodes=3, rf=3, mvs_amount=10)

    # TODO: update non-key column
    def _parallel_updates_inserts(self, records, nodes, rf, mvs_amount):
        def _assert_rows_count(expected_rows=None):
            names_list = [tm.table_name, *tm.materialized_views.keys()]
            for name in names_list:
                if expected_rows:
                    assert_row_count_in_select(session=session, query=f"SELECT * FROM {name}", num_rows_expected=expected_rows, consistency_level=ConsistencyLevel.QUORUM)
                else:
                    assert_two_queries_equal(session, f"select count(*) from {tm.table_name}", session, f"select count(*) from {name}")

        options = {"authenticator": "PasswordAuthenticator", "authorizer": "CassandraAuthorizer"}
        session = self.prepare(rf=rf, nodes=nodes, fetch_size=records * 3, options=options, user="cassandra", password="cassandra")
        tm = TableManager(session, self.cluster, columns={"int": {"amount": mvs_amount, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={})
        tm.create_table()

        for i in range(1, len(tm.column_names_list)):
            ctype = tm.columns_list[i].split(" ")[1]
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(
                mv_columns={ctype: {"names": [tm.column_names_list[i]]}},
                mv_pk_column={"names": [tm.column_names_list[i]]},
            )

        start_data = [2, 5, 12, 45, 63, 78, 36, 85, 98, 100]
        tm.prefill_table(records, data={"int": start_data})
        _assert_rows_count(records)

        def _prepare_table_manager_and_role(session, name):
            sl = ServiceLevel(session=session, name=f"{name}_sl", shares=100).create()
            role = Role(session=session, name=f"{name}_role", password="password", login=True, superuser=True).create()
            role.attach_service_level(service_level=sl)
            role_session = self.patient_cql_connection(self.cluster.nodelist()[0], keyspace="ks", user=f"{name}_role", password="password")
            role_tm = TableManager(role_session, self.cluster, columns={"int": {"amount": mvs_amount, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={})
            role_tm.create_table()  # the create table statement uses IF NOT EXISTS so it's safe to call it repeatedly. It needs to be called to finish initialization of the TableManager object.
            role_tm.materialized_views = tm.materialized_views
            return role_tm

        prefill_tm = _prepare_table_manager_and_role(session, "prefill")
        update_tm = _prepare_table_manager_and_role(session, "update")
        select_tm = _prepare_table_manager_and_role(session, "select")
        new_data = [-2, -5, -12, -45, -63, -78, -36, -85, -98, -100]
        num_updates = records // 10
        proc_functions = [
            {"func": prefill_tm.prefill_table, "args": (records,), "kwargs": {"data": {"int": new_data}, "start_id_from": records + 1}},
            {"func": update_tm.multiple_int_updates_by_id, "args": ([200, 300],), "kwargs": {"filter_values": start_data + new_data, "updates": num_updates // 2, "same_id": False}},
            {"func": update_tm.multiple_int_updates_by_id, "args": ([300, 400],), "kwargs": {"filter_values": start_data, "updates": num_updates // 2}},
            {"func": select_tm.select_all_mvs, "kwargs": {"reads": 2000, "by_id": True}},
        ]

        run_in_parallel(proc_functions)

        # Validate count on every node
        self.eventually(lambda: _assert_rows_count(records * 2))

        # Validate data
        query_template = "select {clmn} from {tbl}"
        for mv_name, mv in tm.materialized_views.items():
            exp_query = query_template.format(clmn=mv.mv_columns_list[-1], tbl=tm.table_name)
            act_query = query_template.format(clmn=mv.mv_columns_list[-1], tbl=mv_name)
            logger.debug(f"Compare: {exp_query} AND {act_query}")
            self.eventually(
                lambda: assert_two_queries_equal(
                    session, exp_query, session, act_query, consistency_level=ConsistencyLevel.QUORUM, session_timeout=self.session_timeout, group=True, groupby_column1=mv.mv_columns_list[-1], groupby_column2=mv.mv_columns_list[-1]
                )
            )

    @staticmethod
    def _create_mvs_by_one_column(tm, mvs_amount, wait_for_view_built=False):
        for i in range(1, mvs_amount + 1):
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(mv_columns={tm.columns_list[i].split(" ")[1]: {"names": [tm.column_names_list[i]]}}, mv_pk_column={"names": [tm.column_names_list[i]]}, wait_for_view_built=wait_for_view_built)

    def test_mvs_populating_from_existing_data(self):
        """Create 10 materialized view on the populated base table"""
        self._mv_populating_from_existing_data(nodes=4, rf=3, mvs=10, prefill=1000)

    def _mv_populating_from_existing_data(self, nodes, rf, mvs, prefill):
        session = self.prepare(rf=rf, nodes=nodes)
        tm = TableManager(session, self.cluster, columns={"int": {"amount": mvs, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={})
        tm.create_table()
        tm.prefill_table(prefill)

        self._create_mvs_by_one_column(tm, mvs, wait_for_view_built=True)

        self._validate_data_in_mvs(tm=tm, session=session, table_expected_rows=prefill, mv_expected_rows=prefill, consistency_level=ConsistencyLevel.ALL)
        self.fixture_dtest_setup.ignore_log_patterns += [r"(\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: data_dictionary::no_such_column_family"]

    def test_mv_populating_from_existing_data_with_restriction(self):
        session = self.prepare(rf=3, nodes=4)
        mvs = 10
        tm = TableManager(session, self.cluster, columns={"int": {"amount": mvs + 1, "frozen": False, "value length": {"min": 1, "max": 100}}}, cl_columns={})
        tm.create_table()
        data = [2, 5, 12, 45, 53, 78, 36, 85, 98, 100]
        tm.prefill_table(10000, data={"int": data})

        self.fixture_dtest_setup.ignore_log_patterns += [
            r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: exceptions::mutation_write_failure_exception "
            r"\(Operation failed for ks.tm_table_mv_\d+ - received 0 responses and 1 failures from 1 CL=ONE\.\)"
        ]

        for i in range(2, mvs + 1):
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(mv_columns={"int": {"names": [tm.column_names_list[i]]}}, mv_pk_column={"names": [tm.column_names_list[i]]}, mv_where_restriction={"names": {tm.pk_list[1]: {"operator": "=", "value": data[i - 1]}}})

        query = "select id, clmn_int0, {clmn} from {tbl}"
        for mv_name, mv in tm.materialized_views.items():
            act_query = query.format(clmn=mv.mv_columns_list[0], tbl=mv.mv_name)
            exp_query = query.format(clmn=mv.mv_columns_list[0], tbl=tm.table_name)
            self.eventually(
                lambda: assert_two_queries_equal(
                    session,
                    exp_query,
                    session,
                    act_query,
                    consistency_level=ConsistencyLevel.QUORUM,
                    session_timeout=self.session_timeout,
                    group=True,
                    groupby_column1=mv.mv_columns_list[0],
                    groupby_column2=mv.mv_columns_list[0],
                    restrict_column1=next(iter(mv.mv_where_restriction.keys())),
                    restrict_value1=mv.mv_where_restriction[next(iter(mv.mv_where_restriction.keys()))]["value"],
                )
            )

    def test_mv_populating_from_existing_data_during_inserts(self):
        """Create 10 materialized views in parallel with base table prefill"""
        self._mv_populating_from_existing_data_during_changes_test("insert")

    def test_mv_populating_from_existing_data_during_updates(self):
        """Create 10 materialized views in parallel with base table updates"""
        self._mv_populating_from_existing_data_during_changes_test("update")

    def test_mv_populating_from_existing_data_during_deletes(self):
        """Create 10 materialized views in parallel with base table deletes"""
        self._mv_populating_from_existing_data_during_changes_test("delete")

    @pytest.mark.skip_if(
        issue_open("scylladb/scylladb#17543")  # scylla asserts when truncating
        | issue_open("scylladb/scylladb#17635")  # view table is not truncated when base table is
    )
    def test_mv_populating_from_existing_data_during_truncate(self):
        """Create 10 materialized views in parallel with base table truncation"""
        self._mv_populating_from_existing_data_during_changes_test("truncate")

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18826"))
    def test_mv_populating_from_existing_data_during_extend(self):
        """Create 10 materialized views in parallel with adding a node"""
        self._mv_populating_from_existing_data_during_changes_test("add node")

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18826"))
    def test_mv_populating_from_existing_data_during_node_remove(self):
        """Create 10 materialized views in parallel with removing a node"""
        self._mv_populating_from_existing_data_during_changes_test("remove node")

    @pytest.mark.dtest_heavy
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_mv_populating_from_existing_data_during_node_stop(self):
        """Create 10 materialized views in parallel with stopping a node"""
        self._mv_populating_from_existing_data_during_changes_test("stop node")

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18826"))
    def test_mv_populating_from_existing_data_during_node_decommission(self):
        """Create 10 materialized views in parallel with a node decommission"""
        self._mv_populating_from_existing_data_during_changes_test("decommission")

    @pytest.mark.dtest_heavy
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_mv_populating_from_existing_data_during_node_restart(self):
        """Create 10 materialized views in parallel with a node restart"""
        self._mv_populating_from_existing_data_during_changes_test("restart node")

    def _mv_populating_from_existing_data_during_changes_test(self, change_type, nodes=4, rf=3, mvs=None, prefill=None):  # noqa: PLR0912
        session = self.prepare(rf=rf, nodes=nodes, options={"prometheus_port": 0})

        node_action = change_type.split(" ")[0]
        if node_action in ["decommission", "restart", "remove", "stop"]:
            session.cluster.shutdown()
            cs = self.patient_cql_cluster_session(self.cluster.nodelist()[0], "ks", exclusive=True, consistency_level=ConsistencyLevel.QUORUM)
            session = cs.session

        cluster = self.cluster
        if prefill is None:
            prefill = 40000
            max_delete = 6000
            mvs = 10
            if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
                prefill = 10000
                max_delete = 2000
                mvs = 2

        tm = TableManager(session, self.cluster, columns={"int": {"amount": mvs, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={})

        rows_after_test = prefill

        if change_type == "insert":
            change_func = {"func": tm.prefill_table, "args": (prefill // 2,), "kwargs": {"start_id_from": prefill + 1, "delay": 1}}
            rows_after_test = prefill * 1.5
        elif change_type == "update":
            change_func = {"func": tm.multiple_int_updates_by_id, "args": ([-100, -1],), "kwargs": {"same_id": False, "delay": 1}}
        elif change_type == "delete":
            change_func = {"func": tm.multiple_deletes, "args": ({"id": [i for i in range(1000, max_delete)]},), "kwargs": {"delay": 1}}
            rows_after_test = max(0, prefill - (max_delete - 1000))
        elif change_type == "truncate":
            change_func = {"func": tm.truncate_table}
            rows_after_test = 0
        elif change_type == "add node":
            change_func = {"func": self._add_new_node, "kwargs": {"delay": 1}}
        elif change_type == "decommission":
            change_func = {"func": self._node_action_with_delay, "args": ("decommission", self.cluster.nodes["node2"]), "kwargs": {"delay": 2}}
        elif change_type == "restart node":
            change_func = {"func": self._node_action_with_delay, "args": (node_action, self.cluster.nodelist()[1]), "kwargs": {"delay": 1}}
        elif change_type in ["remove node", "stop node"]:
            change_func = {"func": self._node_action_with_delay, "args": (node_action, self.cluster.nodelist()[1]), "kwargs": {"delay": 1}}
        else:
            assert False, f'Unexpected parameter "change_type": {change_type}. Expected values: insert / update / delete / add node / remove node / stop node / decommissionrestart node'

        tm.create_table()
        tm.prefill_table(prefill)

        logger.debug("Disabling schema agreement")
        session.cluster.max_schema_agreement_wait = 0

        proc_functions = [change_func, {"func": self._create_mvs_by_one_column, "args": (tm, mvs)}]
        run_in_parallel(proc_functions)

        if node_action == "stop":
            self.cluster.nodelist()[1].start()

        for mv_name in tm.materialized_views.keys():
            wait_for_view(cluster=self.cluster, session=session, ks=tm.keyspace, view=mv_name)

        self._validate_data_in_mvs(tm=tm, session=session, table_expected_rows=rows_after_test, mv_expected_rows=rows_after_test, node_action=node_action)

        exclude_errors = [
            "migration_task - Cant send migration request",
            "mutation_write_timeout_exception",
            r"(\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to",
            "view - Failed to update materialized view bookkeeping.*seastar::no_sharded_instance_exception.*continuing anyway",
            # See https://github.com/scylladb/scylladb/issues/17290 for why
            # we ignore these error messages, that happen when a node is
            # brought down in the middle of migrating a tablet.
            "Request is aborted by a caller",
            "drain rpc failed.*connection is closed",
            "Failed to handle STREAM_MUTATION_FRAGMENTS.*rpc stream was closed by peer",
            "Failed to handle STREAM_MUTATION_FRAGMENTS.*unavailable_exception",
        ]
        self.fixture_dtest_setup.ignore_log_patterns += exclude_errors

    def _restart_node(self, node, delay=0):
        time.sleep(delay)
        logger.debug(f"Start {node.name} restart")
        node.stop()
        time.sleep(5)
        node.start()
        logger.debug(f"Finish node {node.name} restart")

    def set_consistency_level(self, node_action, double_failure=None, cl=None):
        # Set CL as:
        #      - for double_failure - ALL
        #      - for stop/restart/decommission node action - QUORUM
        #      - if RF more then active nodes amount - QUORUM
        #      - for remove node action - ALL
        cl = cl or (ConsistencyLevel.ALL if double_failure else ConsistencyLevel.QUORUM if node_action in ["stop", "restart", "decommission"] or self.total_rf > len(self.cluster.nodelist()) else ConsistencyLevel.ALL)
        logger.debug(f"Query will run with consistency level {cl}")
        return cl

    def create_few_mv(  # noqa: PLR0913
        self,
        mvs_count,
        session,
        keyspace_name,
        table_name,
        synchronous_updates,
        rows,
        mv_name_prefix="mv_cf_view",
        wait_for_mv_built=True,
    ):
        self.fixture_dtest_setup.ignore_log_patterns += [r"view - Error applying view update to .*: exceptions::mutation_write_failure_exception"]

        for i in range(mvs_count):
            query = f"CREATE MATERIALIZED VIEW {mv_name_prefix}_{i} AS SELECT * FROM {table_name} WHERE c1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (c1, key) {' WITH synchronous_updates = true' if synchronous_updates else ''}"
            logger.info(f"Create MV {mv_name_prefix}_{i} as: {query}")
            session.execute(query)

        if wait_for_mv_built:
            for i in range(mvs_count):
                mv_name = f"{mv_name_prefix}_{i}"
                logger.info(f"Wait for view {mv_name}...")
                wait_for_view(self.cluster, session, keyspace_name, mv_name)
                logger.info(f"View {mv_name} is built")
                assert_row_count(session, table_name=mv_name, expected=rows, consistency_level=ConsistencyLevel.QUORUM)

    def test_mv_create_with_synchronous_updates(self):
        """
        Commit: https://github.com/scylladb/scylladb/commit/cb8a67dc98b60919ac9d5bbb6618e17f7d1602c7
        Allow materialized views to run updates in synchronous mode.
        In this mode, all view updates are applied synchronously as if the view was local.
        Test scenario:
        - prepare cluster with 4 nodes
        - create keyspace with RF = 3
        - create base table
        - insert data
        - create 50 MVs with synchronous_updates = True
        - wait for all views are built
        - validate rows count
        - run update on non-PK column (update view)
        - immediately validate that old data is not found in the view (select a view randomly)
        """
        keyspace_name = "ks"
        table_name = "cf"
        session = self.prepare(rf=3, nodes=4, consistency_level=ConsistencyLevel.QUORUM)

        create_cf(session, table_name, columns={"c1": "text", "c2": "text"})

        logger.info("Inserting data...")
        rows = 1000
        insert_c1c2(session, n=rows, c1_values=[f"c1 value {i}" for i in range(rows)], c2_values=[f"c2 value {i}" for i in range(rows)])
        assert_row_count(session, table_name=table_name, expected=rows, consistency_level=ConsistencyLevel.QUORUM)

        mv_name_pref = "mv_cf_view"
        mvs_count = 50
        self.create_few_mv(mvs_count=mvs_count, session=session, keyspace_name=keyspace_name, table_name=table_name, synchronous_updates=True, rows=rows)

        logger.info("Run updates on MVs in synchronous node. Not expected to find rows with not updated (old) values.")
        for _ in range(20000):
            row_index = random.randint(0, rows)
            failed = self.update_one_row_and_assert_view(row_index=row_index, session=session, keyspace_name=keyspace_name, table_name=table_name, mv_name_pref=mv_name_pref, mvs_count=mvs_count)
            assert not failed, f"Unexpectedly found not updated rows in views: {failed}"

    def test_mv_alter_with_synchronous_updates(self):
        """
        Commit: https://github.com/scylladb/scylladb/commit/cb8a67dc98b60919ac9d5bbb6618e17f7d1602c7
        Allow materialized views to run updates in synchronous mode.
        In this mode, all view updates are applied synchronously as if the view was local.
        Test scenario:
        - prepare cluster with 4 nodes
        - create keyspace with RF = 3
        - create base table
        - create 50 MVs with synchronous_updates = False
        - insert data
        - wait for a view is built and validate rows count
        - run update on non-PK column (update view)
         - immediately validate updated column value (select a view randomly) - expected to get old value as MV is
           in asynchronous mode
        - Alter all MVs with synchronous_updates to TRUE
        - run update on non-PK column (update view) and in it time alter synchronous_updates to TRUE
        - immediately validate that old data is not found in the view (select a view randomly)
        """
        keyspace_name = "ks"
        table_name = "cf"
        session = self.prepare(rf=3, nodes=4, consistency_level=ConsistencyLevel.QUORUM)

        create_cf(session, table_name, columns={"c1": "text", "c2": "text"})

        rows = 10000 if self.debug_mode else 100000
        logger.info("Inserting data...")
        insert_c1c2(session, n=rows, c1_values=[f"c1 value {i}" for i in range(rows)], c2_values=[f"c2 value {i}" for i in range(rows)])
        assert_row_count(session, table_name=table_name, expected=rows, consistency_level=ConsistencyLevel.QUORUM)
        logger.info("Finish inserting data...")

        mv_name_pref = "mv_cf_view"
        mvs_count = 10 if self.debug_mode else 50
        self.create_few_mv(mvs_count=mvs_count, session=session, keyspace_name=keyspace_name, table_name=table_name, synchronous_updates=False, rows=rows)

        logger.info("Run updates on MVs in asynchronous node. Rows with not updated (old) values may be found.")
        asynch_failed_assertion = []
        for row_index in range(rows):
            if failed := self.update_one_row_and_assert_view(row_index=row_index, session=session, keyspace_name=keyspace_name, table_name=table_name, mv_name_pref=mv_name_pref, mvs_count=mvs_count):
                asynch_failed_assertion.append(failed)
                break

        if not asynch_failed_assertion:
            logger.error("Not updated rows are not found in views in asynchronous mode")

        logger.info(f"Alter materialized views: set synchronous_updates = true")
        for i in range(mvs_count):
            mv_name = f"{mv_name_pref}_{i}"
            session.execute(f"ALTER MATERIALIZED VIEW {mv_name} WITH synchronous_updates = true")

        logger.info("Run updates on MVs in synchronous node. Not expected to find rows with not updated (old) values.")
        for row_index in range(rows):
            failed = self.update_one_row_and_assert_view(row_index=row_index, session=session, keyspace_name=keyspace_name, table_name=table_name, mv_name_pref=mv_name_pref, mvs_count=mvs_count)
            assert not failed, f"Unexpectedly found not updated rows in views: {failed}"

        if asynch_failed_assertion:
            logger.info("Not updated rows were found in views in ASYNCHRONOUS mode. Not updated rows were not found in views in SYNCHRONOUS mode.'synchronous_updates' feature works as expected.")
        else:
            logger.warning("Not updated rows are NOT found in views in ASYNCHRONOUS mode. We can not be sure that 'synchronous_updates' feature works as expected")

    @staticmethod
    def update_one_row_and_assert_view(row_index, session, keyspace_name, table_name, mv_name_pref, mvs_count):  # noqa: PLR0913
        failed_assertion = {}
        c2_new_value = f"c2 value {row_index * random.randint(10, 100000)}"
        session.execute(f"INSERT INTO {keyspace_name}.{table_name} (key, c1, c2) VALUES ('k{row_index}', 'c1 value {row_index}', '{c2_new_value}')")
        mv_name_for_assert = f"{mv_name_pref}_{random.randint(0, mvs_count - 1)}"
        try:
            assert_one(session=session, query=f"select c2 from {keyspace_name}.{mv_name_for_assert} where key='k{row_index}' and c1 = 'c1 value {row_index}'", expected=[c2_new_value], cl=ConsistencyLevel.QUORUM)
        except AssertionError as err:
            logger.error(err)
            failed_assertion = {"mv name": mv_name_for_assert, "error": err}
        return failed_assertion

    def _validate_data_in_mvs(  # noqa: PLR0913
        self,
        tm,
        session,
        table_expected_rows,
        mv_expected_rows,
        node_action=None,
        grouby_column_index=-1,
        consistency_level=None,
    ):
        query = "select * from {}"
        consistency_level = self.set_consistency_level(node_action=node_action, cl=consistency_level)
        for mv_name, mv in tm.materialized_views.items():
            self._assert_count_table_mv(session, tm.table_name, table_expected_rows, mv_name, mv_expected_rows, cl=consistency_level)

            self.eventually(
                lambda: assert_two_queries_equal(
                    session,
                    query.format(tm.table_name),
                    session,
                    query.format(mv_name),
                    consistency_level=consistency_level,
                    session_timeout=self.session_timeout,
                    group=True,
                    groupby_column1=mv.mv_columns_list[grouby_column_index],
                    groupby_column2=mv.mv_columns_list[grouby_column_index],
                )
            )

    def test_concurrent_updates_deletes(self):
        prefill = 2
        session = self.prepare(rf=3, nodes=4, fetch_size=prefill * 2)
        mvs = 1
        tm = TableManager(session, self.cluster, columns={"int": {"amount": mvs, "frozen": False, "value length": {"min": 1, "max": 1000}}}, pk_columns={}, cl_columns={})

        tm.create_table()
        self._create_mvs_by_one_column(tm, mvs, wait_for_view_built=True)

        start_data = [2, 5, 12, 45, 63, 78, 36, 85, 98, 100]
        tm.prefill_table(prefill, data={"int": start_data})

        proc_functions = [
            {"func": tm.multiple_int_updates_by_id, "args": ([-100, -1],), "kwargs": {"same_id": False, "ids": [0 for _ in range(1001)], "updates": 1000}},
            {"func": tm.multiple_deletes, "args": ({"id": [0 for _ in range(1001)]},)},
        ]
        run_in_parallel(proc_functions)

        mv_name, mv = next(iter(tm.materialized_views.items()))
        self.eventually(
            lambda: assert_two_queries_equal(
                session,
                f"select * from {tm.table_name}",
                session,
                f"select * from {mv_name}",
                consistency_level=ConsistencyLevel.QUORUM,
                session_timeout=self.session_timeout,
                group=True,
                groupby_column1=mv.mv_columns_list[-1],
                groupby_column2=mv.mv_columns_list[-1],
            )
        )

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    def test_multi_mvs_on_different_base_tables(self):
        """Few keyspaces and every keyspace has a few tables and every table has a few MVs.
        MVs are created on the empty base tables
        """
        self._multi_mvs_on_different_base_tables_multi_ks(rf=3, tables=3, mvs=4, prefill_start=10000, increase_rows=10, populated_table=False)

    def test_multi_mvs_on_different_populated_base_tables(self):
        """Few keyspaces and every keyspace has a few tables and every table has a few MVs.
        MVs are created on the populated base tables
        """
        """ Test when keyspace has a few tables and every table has a few MVs. MVs are created on the populated base tables """
        self._multi_mvs_on_different_base_tables_multi_ks(rf=3, tables=3, mvs=4, prefill_start=10000, increase_rows=10, populated_table=True)

    def _multi_mvs_on_different_base_tables_multi_ks(  # noqa: PLR0913
        self,
        rf,
        tables,
        mvs,
        prefill_start,
        increase_rows,
        populated_table,
    ):
        session = self.prepare(rf=rf, nodes=4, options={"hinted_handoff_enabled": False, "read_repair_chance": 0.0})
        session2, session3 = map(self.patient_cql_connection, [self.cluster.nodelist()[1], self.cluster.nodelist()[2]])
        list(map(create_ks, [session2, session3], ["multi1", "multi2"], [rf, rf]))
        proc_functions = []
        for s in [session, session2, session3]:
            proc_functions.append(
                {"func": self._multi_mvs_on_different_base_tables, "args": (s,), "kwargs": {"tables": tables, "mvs": mvs, "prefill_start": prefill_start, "increase_rows": increase_rows, "populated_table": populated_table}}
            )
        run_in_parallel(proc_functions)

    def _multi_mvs_on_different_base_tables(self, session, tables, mvs, prefill_start, increase_rows, populated_table):  # noqa: PLR0913
        def _prefill_base_tables():
            prefill = prefill_start
            for base_table in base_tables:
                base_table.prefill_table(prefill, flush=False)
                prefill = prefill + increase_rows

        def _create_mvs():
            for base_table in base_tables:
                self._create_mvs_by_one_column(base_table, mvs_amount=mvs, wait_for_view_built=True)

        base_tables = []
        for i in range(tables):
            tm = TableManager(
                session,
                self.cluster,
                table_name=f"tm_table{i}",
                columns={"int": {"amount": mvs // 2, "frozen": False, "value length": {"min": 1, "max": 100}}, "text": {"amount": mvs // 2, "frozen": False, "value length": {"min": 1, "max": 10}}},
                pk_columns={},
                cl_columns={},
                keyspace=session.keyspace,
            )
            tm.create_table()
            base_tables.append(tm)

        order = [_prefill_base_tables, _create_mvs] if populated_table else [_create_mvs, _prefill_base_tables]
        for func in order:
            func()

        prefill = prefill_start
        for base_table in base_tables:
            self._validate_data_in_mvs(tm=base_table, session=session, table_expected_rows=prefill, mv_expected_rows=prefill, consistency_level=ConsistencyLevel.ALL)
            prefill = prefill + increase_rows

    def _prepare_cluster_for_drop(self, columns: dict) -> Session:
        rf = 3
        session = self.prepare(rf=rf, nodes=3)

        logger.debug("Create table cf")
        create_cf(session=session, name="cf", key_type="int", columns=columns)

        logger.debug("Create materialized view mv_v1_view")
        session.execute("CREATE MATERIALIZED VIEW mv_v1_view AS SELECT v1, key FROM cf WHERE v1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (v1, key)")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="mv_v1_view")
        session.execute("INSERT INTO cf (key, v0, v1) VALUES(0, 0, 0)")
        return session

    def _search_for_apply_mutation_error(self, mark_logs: dict | None = None, update_mark_logs: bool = True):
        if mark_logs is None:
            mark_logs = {}
        for node in self.cluster.nodelist():
            try:
                wait_for(func=node.grep_log, step=1, timeout=5, throw_exc=True, expr="Failed to apply mutation", from_mark=mark_logs[node.name])
                assert False, f"'Failed to apply mutation' error found in the {node.name} log unexpectedly"
            except:
                pass

            if update_mark_logs:
                mark_logs[node.name] = node.mark_log()

    def run_insert(self, session: Session, columns: str, range_start: int, range_end: int, repeat: int):
        logger.debug("Insert some data")
        for _ in range(repeat):
            try:
                for i in range(range_start, range_end):
                    cmd = f"INSERT INTO cf ({columns}) VALUES({','.join([str(i) for _ in columns.split(',')])})"
                    logger.debug(f"Run {cmd}")
                    session.execute(cmd)
            except WriteFailure as wf:
                assert False, f"Insert request failed unexpectedly with exception {wf}"

    @staticmethod
    @retrying(num_attempts=3)
    def rows_validation(session, rows):
        logger.debug("Validation")
        assert_row_count(session, "cf", rows, consistency_level=ConsistencyLevel.QUORUM)
        assert_row_count(session, "mv_v1_view", rows, consistency_level=ConsistencyLevel.QUORUM)

    def test_add_drop_column(self):
        """
        Cover https://github.com/scylladb/scylla-enterprise/issues/1467 and
        https://github.com/scylladb/scylla/issues/7061
        Drop the column that was added after MV creation
        - Create base table and materialized view
        - Add 2 new columns
        - Drop one added column
        - Insert data
        All data is inserted, no failures
        """
        session = self._prepare_cluster_for_drop(columns={"v0": "int", "v1": "int"})

        mark_logs = {}
        for node in self.cluster.nodelist():
            mark_logs[node.name] = node.mark_log()

        with ThreadPoolExecutor(max_workers=1) as tp:
            thread = tp.submit(self.run_insert, session=session, columns="key, v0, v1", range_start=0, range_end=20, repeat=20)

            for i in range(2, 7):
                logger.debug(f"Add regular column v{i}")
                session.execute(f"ALTER TABLE cf ADD v{i} int")

            thread.result(timeout=60)

        logger.debug("Search for mutation error in nodes' logs")
        self._search_for_apply_mutation_error(mark_logs)

        self.rows_validation(session=session, rows=20)

        with ThreadPoolExecutor(max_workers=1) as tp:
            thread = tp.submit(self.run_insert, session=session, columns="key, v0, v1", range_start=20, range_end=40, repeat=20)

            for i in range(2, 7):
                logger.debug(f"Drop regular column v{i}")
                session.execute(f"ALTER TABLE cf DROP v{i}")

            thread.result(timeout=60)

        logger.debug("Search for mutation error in nodes' logs")
        self._search_for_apply_mutation_error(mark_logs, update_mark_logs=False)

        self.rows_validation(session=session, rows=40)

    def test_drop_existing_column(self):
        """
        Cover https://github.com/scylladb/scylla-enterprise/issues/1467 and
        https://github.com/scylladb/scylla/issues/7061
        Drop the regular column that was added before MV creation.
        - Create base table and materialized view
        - Drop regular column
        - Insert data
        All data is inserted, no failures
        """
        session = self._prepare_cluster_for_drop(columns={"v0": "int", "v1": "int"})

        mark_logs = {}
        for node in self.cluster.nodelist():
            mark_logs[node.name] = node.mark_log()

        with ThreadPoolExecutor(max_workers=1) as tp:
            thread = tp.submit(self.run_insert, session=session, columns="key,v1", range_start=0, range_end=20, repeat=20)

            logger.debug(f"Drop regular column v0")
            session.execute(f"ALTER TABLE cf DROP v0")

            thread.result(timeout=60)

        logger.debug("Search for mutation error in nodes' logs")
        self._search_for_apply_mutation_error(mark_logs, update_mark_logs=False)

        self.rows_validation(session=session, rows=20)

    @pytest.mark.dtest_heavy
    def test_drop_mv_during_base_table_writes(self):
        """Test drop a view during base table writes: the view is created on empty base table and dropped during table prefill
        Test scenario:
        - Create base table
        - Create materialized view
        - Start table prefill with 1000000 records
        - After 40 seconds (before the table prefill is finished) drop the MV
        - Test that the view does not exist in the system schema and base table has 1000000 rows
        """
        prefill = 1000000
        if hasattr(self.cluster, "scylla_mode") and self.cluster.scylla_mode == "debug":
            prefill //= 10

        def _create_mvs(delay=0):
            time.sleep(delay)
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(mv_columns={tm.columns_list[1].split(" ")[1]: {"names": [tm.column_names_list[1]]}}, mv_pk_column={"names": [tm.column_names_list[1]]})

        def _drop_mv(delay=0):
            time.sleep(delay)
            next(iter(tm.materialized_views.values())).drop_mv()

        timeout = self.cql_timeout(300)
        session = self.prepare(
            rf=3,
            nodes=4,
            options={
                "range_request_timeout_in_ms": timeout * 1000,
            },
        )
        tm = TableManager(session, self.cluster, columns={"int": {"amount": 1, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={})
        tm.create_table()

        _create_mvs()
        proc_functions = [{"func": self.add_mv_records, "args": (tm,), "kwargs": {"inserts": prefill, "delay": 0}}, {"func": _drop_mv, "kwargs": {"delay": 40}}]
        run_in_parallel(proc_functions)

        logger.debug("Verifying that system_schema.views is empty")
        assert_none(session, "select * from system_schema.views", cl=ConsistencyLevel.ALL)
        logger.debug(f"Verifying that {tm.table_name} has {prefill} rows")
        assert_row_count(session, tm.table_name, prefill, consistency_level=ConsistencyLevel.QUORUM, timeout=timeout)

        self.check_errors(node=self.cluster.nodelist()[0], exclude_errors=["mutation_write_timeout_exception", "no_such_column_family"])

    def test_fetch_mv_after_recreate(self):
        """Validate it's allowed to fetch from MV after it is dropped and recreated"""
        session = self.prepare(rf=3, nodes=3)
        tm = TableManager(session, self.cluster, columns={"int": {"amount": 2, "frozen": False, "value length": {"min": 1, "max": 100}}}, cl_columns={})
        tm.create_table()

        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_pk_column={"type": "int"})

        tm.prefill_table(100)

        query = "select * from {}"
        assert_two_queries_equal_ignore_order(session, query.format(tm.table_name), session, query.format(mv.mv_name), consistency_level=ConsistencyLevel.ALL, session_timeout=self.session_timeout)
        logger.debug("Drop materialized view and create another with the same name")
        mv.drop_mv()
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_pk_column={"type": "int"})

        assert_two_queries_equal_ignore_order(session, query.format(tm.table_name), session, query.format(mv.mv_name), consistency_level=ConsistencyLevel.ALL, session_timeout=self.session_timeout)

    @pytest.mark.dtest_debug
    def test_create(self):
        """Test the materialized view creation"""

        session = self.prepare(user_table=True)

        result = list(session.execute("SELECT * FROM system_schema.views WHERE keyspace_name='ks' ALLOW FILTERING"))
        assert len(result) == 1, "Expecting 1 materialized view, got" + str(result)

    def test_gcgs_validation(self):
        """Verify that it's not possible to create or set a too low gc_grace_seconds on MVs"""
        session = self.prepare(user_table=True)

        # Shouldn't be able to alter the gc_grace_seconds of the base table to 0
        assert_invalid(
            session,
            "ALTER TABLE users WITH gc_grace_seconds = 0",
            "Cannot alter gc_grace_seconds of the base table of a materialized view "
            "to 0, since this value is used to TTL undelivered updates. Setting "
            "gc_grace_seconds too low might cause undelivered updates to expire "
            "before being replayed.",
        )

        # But can alter the gc_grace_seconds of the bease table to a value != 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 10")

        # Shouldn't be able to alter the gc_grace_seconds of the MV to 0
        assert_invalid(
            session,
            "ALTER MATERIALIZED VIEW users_by_state WITH gc_grace_seconds = 0",
            "Cannot alter gc_grace_seconds of a materialized view to 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.",
        )

        # Now let's drop MV
        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        # Now we should be able to set the gc_grace_seconds of the base table to 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")

        # Now we shouldn't be able to create a new MV on this table
        assert_invalid(
            session,
            "CREATE MATERIALIZED VIEW users_by_state AS SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)",
            "Cannot create materialized view 'users_by_state' for base table 'users' "
            "with gc_grace_seconds of 0, since this value is used to TTL undelivered "
            "updates. Setting gc_grace_seconds too low might cause undelivered updates"
            " to expire before being replayed.",
        )

    @pytest.mark.require("2431")
    def test_crc_check_chance(self):
        """Test that crc_check_chance parameter is properly populated after mv creation and update"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id) WITH crc_check_chance = 0.5")

        assert_crc_check_chance_equal(session, "t_by_v", 0.5, view=True)

        session.execute("ALTER MATERIALIZED VIEW t_by_v WITH crc_check_chance = 0.3")

        assert_crc_check_chance_equal(session, "t_by_v", 0.3, view=True)

    def test_prepared_statement(self):
        """Test basic insertions with prepared statement"""

        session = self.prepare(user_table=True)

        insertPrepared = session.prepare("INSERT INTO users (username, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);")
        selectPrepared = session.prepare("SELECT state, password, session_token FROM users_by_state WHERE state=?;")

        # insert data
        session.execute(insertPrepared.bind(("user1", "ch@ngem3a", "f", "TX", 1968)))
        session.execute(insertPrepared.bind(("user2", "ch@ngem3b", "m", "CA", 1971)))
        session.execute(insertPrepared.bind(("user3", "ch@ngem3c", "f", "FL", 1978)))
        session.execute(insertPrepared.bind(("user4", "ch@ngem3d", "m", "TX", 1974)))

        result = list(session.execute("SELECT * FROM users;"))
        assert len(result) == 4, f"Expecting {4} users, got {len(result)}"

        result = list(session.execute(selectPrepared.bind(["TX"])))
        assert len(result) == 2, f"Expecting {2} users, got {len(result)}"

        result = list(session.execute(selectPrepared.bind(["CA"])))
        assert len(result) == 1, f"Expecting {1} users, got {len(result)}"

        result = list(session.execute(selectPrepared.bind(["MA"])))
        assert len(result) == 0, f"Expecting {0} users, got {len(result)}"

    def test_truncate_base(self):
        """Test truncate base table and as result - materialized view"""
        session = self.prepare(user_table=True)
        session.execute("INSERT INTO users (state, username) VALUES ('TX', 'user1')")

        # ensure sstables are created and will be dropped
        self.cluster.flush()

        # ensure data is loaded into cache and the cache will be cleared
        assert_one(session, "SELECT * FROM users_by_state", ["TX", "user1", None, None, None, None])

        session.execute("TRUNCATE table users")

        self._assert_count_table_mv(session, "users", 0, "users_by_state", 0)

    def test_drop_mv(self):
        """Test that we can drop a view properly"""

        session = self.prepare(user_table=True)

        # create another materialized view
        session.execute("CREATE MATERIALIZED VIEW users_by_birth_year AS SELECT * FROM users WHERE birth_year IS NOT NULL AND username IS NOT NULL PRIMARY KEY (birth_year, username)")

        result = list(session.execute("SELECT * FROM system_schema.views WHERE keyspace_name='ks'"))
        assert len(result) == 2, f"Expecting {2} materialized view, got {len(result)}"

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        result = list(session.execute("SELECT * FROM system_schema.views WHERE keyspace_name='ks'"))
        assert len(result) == 1, f"Expecting {1} materialized view, got {len(result)}"

    def _add_dc_after_mv_test(self, rf, nodes=3):
        """
        @jira_ticket CASSANDRA-10978
        Add datacenter with configurable replication.
        """

        # Limit RF-change migrations to avoid the debug-mode CQL timeout (SCYLLADB-3909).
        tablets = 4 if "tablets" in self.scylla_features else None
        session = self.prepare(nodes=nodes, rf=rf, tablets=tablets)

        logger.debug("Creating schema")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        logger.debug("Writing 1k to base")
        for i in range(1000):
            session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")

        logger.debug("Reading 1k from view")
        for i in range(1000):
            self.eventually_assert_one(session, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i])

        logger.debug("Reading 1k from base")
        for i in range(1000):
            assert_one(session, f"SELECT * FROM t WHERE id = {i}", [i, -i])

        logger.debug("Bootstrapping new node in another dc")
        # We are adding a new dc, to follow the add dc procedure, we should
        # bootstrap the node, then modify the rf to use the new dc, then rebuild
        # https://docs.scylladb.com/operating-scylla/procedures/cluster-management/add_dc_to_exist_dc/
        node4 = self.cluster.new_node(4, data_center="dc2")
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        # Adding a DC waits for tablet replica rebuilds, including the materialized view.
        # after https://github.com/scylladb/scylladb/issues/20665 will be fixed this 200 second timeout can be revisited
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};", timeout=200)
        node4.nodetool("rebuild -- dc1")

        logger.debug("Bootstrapping new node in another dc")
        node5 = self.cluster.new_node(5, data_center="dc2")
        node5.start()

        session2 = self.patient_exclusive_cql_connection(node4, "ks")

        logger.debug("Verifying data from new node in view")
        for i in range(1000):
            self.eventually_assert_one(session2, f"SELECT * FROM ks.t_by_v WHERE v = {-i}", [-i, i], ConsistencyLevel.LOCAL_ONE)

        logger.debug("Inserting 100 into base")
        for i in range(1000, 1100):
            session2.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")

        logger.debug("Verify 100 in view")
        for i in range(1000, 1100):
            self.eventually_assert_one(session, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i], ConsistencyLevel.LOCAL_ONE)

        self.check_errors(node=self.cluster.nodelist()[0], exclude_errors="migration_task - Cant send migration request")

    @pytest.mark.parametrize("rf", [pytest.param(1, marks=pytest.mark.skip_if(issue_open("scylladb/scylladb#26981") | issue_open("scylladb/scylladb#26984"))), pytest.param(3)])
    def test_add_node_during_base_table_update(self, rf):
        """Test expand cluster - add one node during MV updates
        Test starts with a starting size: one DCs with 4 nodes, and add new node to the same DC during update existent records of base
        table that cause to update materialized view as well.
        Verify that MV records are according to the base table
        """
        session = self.prepare(rf=rf)
        # Create table
        tm = TableManager(
            session, self.cluster, columns={"int": {"amount": 1, "frozen": False, "value length": {"min": 1, "max": 10000}}, "text": {"amount": 1, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={}
        )
        tm.create_table()

        # Create materialized view
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_columns=None, mv_pk_column={"type": "int"})

        # Pre-fill table
        prefill = 5000
        tm.prefill_table(prefill)

        # Check if all records were saved
        self.eventually(lambda: self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill))

        # Run update base table and add new node in the parallel
        update_to = 5000
        proc_functions = [{"func": self._add_new_node, "args": (), "kwargs": {}}] + [
            {"func": tm.update_table, "args": ({"by type": {"int": update_to}}, {"by name": {"id": {"operator": "in", "value": [i for i in range(start, start + 100)]}}}), "kwargs": {"delay": 5}} for start in range(100, 4900, 100)
        ]
        results = run_in_parallel(proc_functions)

        # Receive the results
        new_node_session = set_clause = None
        for result in results:
            if isinstance(result, tuple):
                set_clause, _ = result
            else:
                new_node_session = result

        # Validate the results
        assert new_node_session and set_clause, "Can't run the test. Reason: {}".format("; ".join(msg[1] for msg in [[new_node_session, "new session has been not created"], [set_clause, "SET clause is empty"]] if not msg[0]))

        select = "{}".format(", ".join(clmn for clmn in set_clause))
        statement_template = f"select {select} from {tm.keyspace}.{{0}}"

        def base_and_view_match():
            assert_two_queries_equal(
                session, statement_template.format(tm.table_name), new_node_session, statement_template.format(mv.mv_name), consistency_level=ConsistencyLevel.QUORUM, group=True, groupby_column1=select, groupby_column2=select
            )

        self.eventually(base_and_view_match)

    def _add_new_node(  # noqa: PLR0913
        self,
        data_center="dc1",
        wait_for_binary_proto=True,
        wait_other_notice=False,
        jvm_args=None,
        configuration_options=None,
        delay=0,
        new_node_index=None,
        rack=None,
    ):
        time.sleep(delay)
        i = len(self.cluster.nodes) + 1 if not new_node_index else new_node_index
        node = self.cluster.new_node(i=i, data_center=data_center, rack=rack)
        if configuration_options:
            node.set_configuration_options(values=configuration_options)  # CASSANDRA-11670
        logger.debug("Start join at {}".format(time.strftime("%H:%M:%S")))
        node.start(wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice, jvm_args=jvm_args)
        session = self.patient_exclusive_cql_connection(node)
        logger.debug("Finish join at {}".format(time.strftime("%H:%M:%S")))
        return session

    # This case is redundant when tablets are enabled since SimpleStrategy
    # is not supported with tablets so NetworkTopologyStrategy is used under the covers,
    # like test_add_dc_after_mv_network_replication below.
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_add_dc_after_mv_simple_replication(self):
        """
        @jira_ticket CASSANDRA-10634

        Test that materialized views work as expected when adding a datacenter with SimpleStrategy.
        """

        self._add_dc_after_mv_test(1)

    def test_add_dc_after_mv_network_replication(self):
        """
        @jira_ticket CASSANDRA-10634

        Test that materialized views work as expected when adding a datacenter with NetworkTopologyStrategy.
        """

        self._add_dc_after_mv_test({"dc1": 1}, nodes=[3])

    def test_add_node_after_mv(self):
        """
        @jira_ticket CASSANDRA-10978
        Test that materialized views work as expected when adding a node.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        for i in range(1000):
            session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")

        num_attempts = 75 if self.cluster.scylla_mode != "debug" else 300

        for i in range(1000):
            # mv updates are asynchronous. need to check the condition repeatedly until it becomes true.
            retrying(num_attempts=num_attempts, sleep_time=1)(assert_one)(session, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i])

        session2 = self._add_new_node(data_center="dc1")
        # After each topology change, we need to wait until
        # token ring converges on all nodes to mace sure that date streaming/repair finished
        for node in self.cluster.nodelist():
            wait_for_token_ring_and_group0_consistency(node, 4)

        for i in range(1000):
            assert_one(session2, f"SELECT * FROM ks.t_by_v WHERE v = {-i}", [-i, i])

        for i in range(1000, 1100):
            session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")

        for i in range(1000, 1100):
            # mv updates are asynchronous. need to check the condition repeatedly until it becomes true.
            retrying(num_attempts=num_attempts, sleep_time=1)(assert_one)(session, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i])

    @pytest.mark.resource_intensive
    def test_add_node_after_wide_mv_with_range_deletions(self):  # noqa: PLR0912
        """
        Taken from Cassandra
        @jira_ticket CASSANDRA-11670
        Test that materialized views work with wide materialized views as expected when adding a node.
        commitlog_segment_size_in_mb=32 and no defined max_mutation_size_in_kb
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v)) WITH compaction = { 'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' }")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        for i in range(10):
            for j in range(100):
                session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {j})")

        for i in range(10):
            for j in range(100):
                assert_one(session, f"SELECT * FROM t WHERE id = {i} and v = {j}", [i, j])
                assert_one(session, f"SELECT * FROM t_by_v WHERE id = {i} and v = {j}", [j, i])

        for i in range(10):
            for j in range(100):
                if j % 10 == 0:
                    session.execute(f"DELETE FROM t WHERE id = {i} AND v >= {j} and v < {j + 2}")

        for i in range(10):
            for j in range(100):
                if j % 10 == 0 or (j - 1) % 10 == 0:
                    assert_none(session, f"SELECT * FROM t WHERE id = {i} and v = {j}")
                    assert_none(session, f"SELECT * FROM t_by_v WHERE id = {i} and v = {j}")
                else:
                    assert_one(session, f"SELECT * FROM t WHERE id = {i} and v = {j}", [i, j])
                    assert_one(session, f"SELECT * FROM t_by_v WHERE id = {i} and v = {j}", [j, i])

        # Scylla does not support migration_task_wait_in_seconds and max_mutation_size_in_kb parameters
        session2 = self._add_new_node(
            wait_for_binary_proto=True
            # , jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)],
            # configuration_options={'max_mutation_size_in_kb': 20}
        )
        for i in range(10):
            for j in range(100):
                if j % 10 == 0 or (j - 1) % 10 == 0:
                    assert_none(session2, f"SELECT * FROM ks.t WHERE id = {i} and v = {j}")
                    assert_none(session2, f"SELECT * FROM ks.t_by_v WHERE id = {i} and v = {j}")
                else:
                    assert_one(session2, f"SELECT * FROM ks.t WHERE id = {i} and v = {j}", [i, j])
                    assert_one(session2, f"SELECT * FROM ks.t_by_v WHERE id = {i} and v = {j}", [j, i])

        for i in range(10):
            for j in range(100, 110):
                session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {j})")

        for i in range(10):
            for j in range(110):
                if j < 100 and (j % 10 == 0 or (j - 1) % 10 == 0):
                    assert_none(session2, f"SELECT * FROM ks.t WHERE id = {i} and v = {j}")
                    assert_none(session2, f"SELECT * FROM ks.t_by_v WHERE id = {i} and v = {j}")
                else:
                    assert_one(session2, f"SELECT * FROM ks.t WHERE id = {i} and v = {j}", [i, j])
                    assert_one(session2, f"SELECT * FROM ks.t_by_v WHERE id = {i} and v = {j}", [j, i])

    # @pytest.mark.skip('unrecognised option \'-Dcassandra.migration_task_wait_in_second\'')
    @pytest.mark.resource_intensive
    def test_add_node_after_very_wide_mv(self):
        """
        Taken from Cassandra
        @jira_ticket CASSANDRA-11670
        Test that materialized views work with very wide materialized views as expected when adding a node.
        commitlog_segment_size_in_mb=32 and no defined max_mutation_size_in_kb
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v))")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        for i in range(5):
            for j in range(5000):
                session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {j})")

        for i in range(5):
            for j in range(5000):
                assert_one(session, f"SELECT * FROM t_by_v WHERE id = {i} and v = {j}", [j, i])

        # Scylla does not support migration_task_wait_in_seconds and max_mutation_size_in_kb parameters
        session2 = self._add_new_node(
            wait_for_binary_proto=True
            # , jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)],
            # configuration_options={'max_mutation_size_in_kb': 20}
        )
        for i in range(5):
            for j in range(5000):
                assert_one(session2, f"SELECT * FROM ks.t_by_v WHERE id = {i} and v = {j}", [j, i])

        for i in range(5):
            for j in range(5100):
                session.execute(f"INSERT INTO t (id, v) VALUES ({i}, {j})")

        for i in range(5):
            for j in range(5100):
                assert_one(session, f"SELECT * FROM t_by_v WHERE id = {i} and v = {j}", [j, i])

    def test_allow_filtering(self):
        """Test that allow filtering works as usual for a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)")

        for i in range(1000):
            session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")

        num_attempts = 75 if self.cluster.scylla_mode != "debug" else 300

        for i in range(1000):
            # mv updates are asynchronous. need to check the condition repeatedly until it becomes true.
            retrying(num_attempts=num_attempts, sleep_time=1)(assert_one)(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

        @retrying(num_attempts=num_attempts, sleep_time=1)
        def check_t_by_v2():
            rows = list(session.execute("SELECT * FROM t_by_v2 WHERE v2 = 'a'"))
            assert len(rows) == 1000, f"Expected 1000 rows but got {len(rows)}"

        check_t_by_v2()

        assert_invalid(session, "SELECT * FROM t_by_v WHERE v = 1 AND v2 = 'a'", expected=Exception)
        assert_invalid(session, "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = 1", expected=Exception)

        for i in range(1000):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i} AND v3 = 3.0 ALLOW FILTERING", [i, i, "a", 3.0])
            assert_one(session, f"SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = {i} ALLOW FILTERING", ["a", i, i, 3.0])

    @staticmethod
    def _assert_count_table_mv(  # noqa: PLR0913
        session,
        table_name,
        table_expected_count,
        mv_name,
        mv_expected_count,
        cl=ConsistencyLevel.QUORUM,
    ):
        assert_row_count(session, table_name, table_expected_count, consistency_level=cl)
        assert_row_count(session, mv_name, mv_expected_count, consistency_level=cl)

    def test_query_new_column(self):
        """
        Test that a materialized view created with 'SELECT <col1, ...>' works as expected when adding a new column
        @expected_result The new column is not present in the view.
        """

        session = self.prepare(user_table=True)

        session.execute("CREATE MATERIALIZED VIEW users_by_state2 AS SELECT username FROM users WHERE STATE IS NOT NULL AND USERNAME IS NOT NULL PRIMARY KEY (state, username)")

        self._insert_data(session)

        assert_one(session, "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'", ["TX", "user1"])

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = list(session.execute("SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'"))
        assert len(results) == 1
        assert not hasattr(results[0], "first_name"), 'Column "first_name" found in view'
        assert_one(session, "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'", ["TX", "user1"])

    @pytest.mark.lwt
    def test_lwt(self):
        """Test that lightweight transaction behave properly with a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        logger.debug("Inserting initial data using IF NOT EXISTS")
        for i in range(1000):
            session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0) IF NOT EXISTS")
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()

        logger.debug("All rows should have been inserted")
        for i in range(1000):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

        logger.debug("Tyring to UpInsert data with a different value using IF NOT EXISTS")
        for i in range(1000):
            v = i * 2
            session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {v}, 'a', 3.0) IF NOT EXISTS")
        # self._replay_batchlogs()

        logger.debug("No rows should have changed")
        for i in range(1000):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

        logger.debug("Update the 10 first rows with a different value")
        for i in range(1000):
            v = i + 2000
            session.execute(f"UPDATE t SET v={v} WHERE id = {i} IF v < 10")
        # self._replay_batchlogs()

        logger.debug("Verify that only the 10 first rows changed.")
        results = list(session.execute("SELECT * FROM t_by_v;"))
        assert len(results) == 1000
        for i in range(1000):
            v = i + 2000 if i < 10 else i
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {v}", [v, i, "a", 3.0])

        logger.debug("Deleting the first 10 rows")
        for i in range(1000):
            v = i + 2000
            session.execute(f"DELETE FROM t WHERE id = {i} IF v = {v} ")
        # self._replay_batchlogs()

        logger.debug("Verify that only the 10 first rows have been deleted.")
        results = list(session.execute("SELECT * FROM t_by_v;"))
        assert len(results) == 990
        for i in range(10, 1000):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

    def test_mv_with_default_ttl_with_flush(self):
        self._test_mv_with_default_ttl(True)

    def test_mv_with_default_ttl_without_flush(self):
        self._test_mv_with_default_ttl(False)

    def _test_mv_with_default_ttl(self, flush):  # noqa: PLR0915
        """
        Verify mv with default_time_to_live can be deleted properly using expired livenessInfo
        @jira_ticket CASSANDRA-14071
        """
        session = self.prepare(rf=3, nodes=3, options={"hinted_handoff_enabled": False}, consistency_level=ConsistencyLevel.QUORUM)
        session.execute("USE ks")

        logger.debug("MV with same key and unselected columns")
        session.execute("CREATE TABLE t2 (k int, a int, b int, c int, primary key(k, a)) with default_time_to_live=600")
        session.execute("CREATE MATERIALIZED VIEW mv2 AS SELECT k,a,b FROM t2 WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)")
        session.cluster.control_connection.wait_for_schema_agreement()

        self.update_view(session, "UPDATE t2 SET c=1 WHERE k=1 AND a=1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,c FROM t2", [1, 1, None, 1])
        self.eventually_assert_one(session, "SELECT k,a,b FROM mv2", [1, 1, None])

        self.update_view(session, "UPDATE t2 SET c=null WHERE k=1 AND a=1;", flush)
        self.eventually_assert_none(session, "SELECT k,a,b,c FROM t2")
        self.eventually_assert_none(session, "SELECT k,a,b FROM mv2")

        self.update_view(session, "UPDATE t2 SET c=2 WHERE k=1 AND a=1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,c FROM t2", [1, 1, None, 2])
        self.eventually_assert_one(session, "SELECT k,a,b FROM mv2", [1, 1, None])

        self.update_view(session, "DELETE c FROM t2 WHERE k=1 AND a=1;", flush)
        self.eventually_assert_none(session, "SELECT k,a,b,c FROM t2")
        self.eventually_assert_none(session, "SELECT k,a,b FROM mv2")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t2")
            assert_none(session, "SELECT * FROM mv2")

        # test with user-provided ttl
        self.update_view(session, "INSERT INTO t2(k,a,b,c) VALUES(2,2,2,2) USING TTL 5", flush)
        self.update_view(session, "UPDATE t2 USING TTL 100 SET c=1 WHERE k=2 AND a=2;", flush)
        self.update_view(session, "UPDATE t2 USING TTL 50 SET c=2 WHERE k=2 AND a=2;", flush)
        self.update_view(session, "DELETE c FROM t2 WHERE k=2 AND a=2;", flush)

        time.sleep(6)

        self.eventually_assert_none(session, "SELECT k,a,b,c FROM t2")
        self.eventually_assert_none(session, "SELECT k,a,b FROM mv2")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t2")
            assert_none(session, "SELECT * FROM mv2")

        logger.debug("MV with extra key")
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int) with default_time_to_live=600")
        session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)")
        session.cluster.control_connection.wait_for_schema_agreement()

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1);", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1])

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 2, 1);", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 2, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 2, 1])

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 3, 1);", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 3, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 3, 1])

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t", [1, 3, 1])
            assert_one(session, "SELECT * FROM mv", [1, 3, 1])

        # user provided ttl
        self.update_view(session, "UPDATE t USING TTL 30 SET a = 4 WHERE k = 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 4, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 4, 1])

        self.update_view(session, "UPDATE t USING TTL 20 SET a = 5 WHERE k = 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 5, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 5, 1])

        last_update = time.time()
        self.update_view(session, "UPDATE t USING TTL 10 SET a = 6 WHERE k = 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 6, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 6, 1])

        if flush:
            self.cluster.compact()
            now = time.time()
            if now - last_update < 10:
                time.sleep(10 - (now - last_update))
            self.eventually_assert_one(session, "SELECT * FROM t", [1, None, 1])
            self.eventually_assert_none(session, "SELECT * FROM mv")

    @pytest.mark.parametrize("flush", [True, False], ids=["with_flush", "without_flush"])
    def test_base_column_in_view_pk_complex_timestamp(self, flush):  # noqa: PLR0915
        """
        Able to shadow old view row with column ts greater than pk's ts and re-insert the view row
        Able to shadow old view row with column ts smaller than pk's ts and re-insert the view row

        @jira_ticket CASSANDRA-11500
        """
        session = self.prepare(rf=3, nodes=3, options={"hinted_handoff_enabled": False}, consistency_level=ConsistencyLevel.QUORUM)
        _node1, node2, node3 = self.cluster.nodelist()

        session.execute("USE ks")
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int)")
        session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)")
        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=1
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1])

        # increase b ts to 10
        self.update_view(session, "UPDATE t USING TIMESTAMP 10 SET b = 2 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET a = 2 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

        # switch entries. shadow a = 2, insert a = 1
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET a = 1 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET a = 2 WHERE k = 1;", flush, compact=True)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

        # able to shadow view row even if base-column in view pk's ts is smaller than row timestamp
        # set row TS = 20, a@6, b@20
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 5 where k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, None, 2, 10])
        self.eventually_assert_none(session, "SELECT k,a,b,writetime(b) FROM mv")
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 6;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])
        self.update_view(session, "INSERT INTO t (k, b) VALUES (1, 1) USING TIMESTAMP 20;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 1, 20])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET a = 2 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 2, 1, 7, 20])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 1, 20])

        # switch entries. shadow a = 2, insert a = 1
        self.update_view(session, "UPDATE t USING TIMESTAMP 8 SET a = 1 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 1, 1, 8, 20])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

        # create another view row
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (2, 2, 2);", flush)
        self.eventually_assert_one(session, "SELECT k,a,b FROM t WHERE k = 2", [2, 2, 2])
        self.eventually_assert_one(session, "SELECT k,a,b FROM mv WHERE k = 2", [2, 2, 2])

        # stop node2, node3
        logger.debug("Shutdown [node2, node3]")
        self.cluster.stop_nodes([node2, node3], wait_other_notice=True)
        # shadow a = 1, create a = 2
        query = SimpleStatement("UPDATE t USING TIMESTAMP 9 SET a = 2 WHERE k = 1", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)
        # shadow (a=2, k=2) after 3 second
        query = SimpleStatement("UPDATE t USING TTL 3 SET a = 2 WHERE k = 2", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)

        logger.debug("Starting [node2, node3]")
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        # For k = 1 & a = 1, We should get a digest mismatch of tombstones and repaired
        # We don't have check_trace_events
        query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 1", consistency_level=ConsistencyLevel.ALL)
        # result = session.execute(query, trace=True)
        # self.check_trace_events(result.get_query_trace(), True)
        # assert 0 == len(result.current_rows)

        # For k = 1 & a = 1, second time no digest mismatch
        # self.check_trace_events(result.get_query_trace(), False)
        # assert_none(session, "SELECT * FROM mv WHERE k = 1 AND a = 1")

        def check_query_rows_size(_query, expected_rows_size):
            assert len(session.execute(_query, trace=True).current_rows) == expected_rows_size

        self.eventually(lambda: check_query_rows_size(query, 0))
        # For k = 1 & a = 2, We should get a digest mismatch of data and repaired for a = 2
        query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 2", consistency_level=ConsistencyLevel.ALL)
        # result = session.execute(query, trace=True)
        # self.check_trace_events(result.get_query_trace(), True)
        # assert 1 == len(result.current_rows)

        # For k = 1 & a = 2, second time no digest mismatch
        # self.check_trace_events(result.get_query_trace(), False)
        self.eventually(lambda: check_query_rows_size(query, 1))
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv WHERE k = 1", [1, 2, 1, 20])

        time.sleep(3)
        # For k = 2 & a = 2, We should get a digest mismatch of expired and repaired
        query = SimpleStatement("SELECT * FROM mv WHERE k = 2 AND a = 2", consistency_level=ConsistencyLevel.ALL)
        # self.check_trace_events(result.get_query_trace(), True)
        # logger.debug(result.current_rows)
        # assert 0 == len(result.current_rows)

        # For k = 2 & a = 2, second time no digest mismatch
        # result = session.execute(query, trace=True)
        # self.check_trace_events(result.get_query_trace(), False)
        self.eventually(lambda: check_query_rows_size(query, 0))

    def test_view_tombstone(self):
        """
        Test that a materialized views properly tombstone
        @jira_ticket CASSANDRA-10261
        @jira_ticket CASSANDRA-10910
        """

        self.prepare(rf=3, options={"hinted_handoff_enabled": False, "cache_hit_rate_read_balancing": False})
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("USE ks")

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v,id) WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.0 AND speculative_retry = 'none'")

        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=0, verify
        session.execute(SimpleStatement("INSERT INTO t (id, v, v2, v3) VALUES (1, 2, 'a', 3.0) USING TIMESTAMP 0", consistency_level=ConsistencyLevel.ALL))

        assert_one(session, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "a", 3.0])
        session.execute(SimpleStatement("INSERT INTO t (id, v2) VALUES (1, 'b') USING TIMESTAMP 1", consistency_level=ConsistencyLevel.ALL))

        assert_one(session, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "b", 3.0])

        # change v's value and TS=3, tombstones v=1 and adds v=0 record
        session.execute(SimpleStatement("UPDATE t USING TIMESTAMP 3 SET v = 0 WHERE id = 1", consistency_level=ConsistencyLevel.ALL))
        assert_none(session, "SELECT * FROM t_by_v WHERE v = 2")

        logger.debug("Shutdown nodes 2 and 3")
        self.cluster.stop_nodes([node2, node3], wait_other_notice=True)

        logger.debug("Update base table")
        session.execute(SimpleStatement("UPDATE t USING TIMESTAMP 4 SET v = 2 WHERE id = 1", consistency_level=ConsistencyLevel.ONE))

        logger.debug("Starting nodes 2 and 3")
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        session2 = self.patient_exclusive_cql_connection(node2)
        session2.execute("USE ks")

        sync_hinted_handoff(node1)

        # We should get a digest mismatch, and data should be repaired.
        # We may need to wait for the MV update to be applied. With tablets, self-pairing of base-view replicas
        # is not guaranteed, so the MV update may be targeted to a remote view replica which is down. In that case
        # we need to wait for the hint to be sent and applied.
        # See https://github.com/scylladb/scylla-dtest/issues/4628
        logger.debug("Verifying view data")
        assert_one(session, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "b", 3.0], cl=ConsistencyLevel.ALL)
        assert_none(session, "SELECT * FROM t_by_v WHERE v = 0", cl=ConsistencyLevel.ALL)
        assert_none(session, "SELECT * FROM t_by_v WHERE v = 1", cl=ConsistencyLevel.ALL)

        assert_one(session2, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "b", 3.0], cl=ConsistencyLevel.ONE)

    def _setup_for_viewbuildstatus(self, num_of_rows=None):
        """this function creates a materialized view for viewbildstatus nodetool command tests
        Returns a list of [TableManager, MaterializedViewManager] objects
        """
        if not num_of_rows:
            num_of_rows = 10000 if self.debug_mode else 100000
        session = self.prepare(rf=3, nodes=3, fetch_size=num_of_rows * 2)
        table_manager = TableManager(session, self.cluster, columns={"int": {"amount": 20, "frozen": False, "value length": {"min": 1, "max": 100}}}, cl_columns={}, pk_columns={})
        table_manager.create_table()
        table_manager.prefill_table(num_of_rows)

        mv_pk_column = table_manager.column_names_list[1]
        mv = MaterializedViewManager(table_manager)
        mv.create_materialized_view(mv_pk_column={"names": [mv_pk_column]}, wait_for_view_built=False)
        return [table_manager, mv]

    def test_viewbuildstatus_progress_success_flow(self):
        """ " test viewbuildstatus nodetool command output correctness during the creation of a materialized view"""

        table_manager, mv = self._setup_for_viewbuildstatus()
        number_of_nodes = len(self.cluster.nodelist())

        in_progress_str = output = "has not finished building; node status is below."
        success_str = "has finished building"
        max_retries = 60 if self.debug_mode else 20
        current_retry = 0

        """
        viewbuildstatus command output for example:

           keyspace1.m_view has not finished building; node status is below.

           Host      Info
           127.0.0.2 STARTED
           127.0.0.3 STARTED
           127.0.0.1 SUCCESS

        """

        while current_retry < max_retries and in_progress_str in output:
            current_retry += 1
            node_to_run = random.choice(self.cluster.nodelist())
            logger.debug(f"Waiting for viewbuildstatus command to finish. Current retry = {current_retry},command is running from {node_to_run.name}\n output = {output}")
            try:
                output = node_to_run.nodetool(f"viewbuildstatus {table_manager.keyspace} {mv.mv_name}")
                assert success_str in output[0], f"viewbuildstatus command finished with unexpected output: {output}, Terminating test"
                logger.debug("viewbuildstatus command finished successfully")
            except NodetoolError as e:
                # viewbuildstatus command returns exit(1) during the materialized view build process duration
                # TODO remove the try-except when https://github.com/scylladb/scylla-tools-java/issues/289 is resolved
                output = e.stdout
                logger.debug(f"e.stdout = {e.stdout}")
                cluster_info = output.splitlines()[3:]
                assert len(cluster_info) == number_of_nodes, f"Wrong output of viewbuildstatus command:number of lines is wrong"
                for line in cluster_info:
                    host_ip, host_status = line.split()
                    assert host_status in ("SUCCESS", "STARTED"), f'Wrong output of viewbuildstatus command: host {host_ip} state is not "STARTED" or "SUCCESS" '
            time.sleep(1)

        assert success_str in output[0], f"viewbuildstatus command exceeded {max_retries} retries without receiving {success_str} string in output"

    def test_viewbuildstatus_progress_unknown_flow(self):
        """ " test viewbuildstatus nodetool command output correctness,
        testing UNKNOWN host state by giving a wrong materialized view parameter to viewbuildstatus command
        """
        """
           viewbildstatus command output for example:

               ks.tm_table has not finished building; node status is below.

               Host       Info
               127.0.41.3 UNKNOWN
               127.0.41.1 UNKNOWN
               127.0.41.2 UNKNOWN
        """
        table_manager, _mv = self._setup_for_viewbuildstatus(num_of_rows=10)
        number_of_nodes = len(self.cluster.nodelist())
        try:
            node_to_run = random.choice(self.cluster.nodelist())
            logger.debug(f"Testing viewbuilstatus nodetool command with wrong materialized view name.\n command is running from {node_to_run.name}")
            node_to_run.nodetool(f"viewbuildstatus {table_manager.keyspace} {table_manager.table_name}")
        except NodetoolError as e:
            logger.debug(f"e.stdout = {e.stdout}")
            assert e.stdout.count("UNKNOWN") == number_of_nodes, "wrong number of UNKNOWN host states in viewbuildstatus command output"

    @pytest.mark.parametrize(
        "colocated_view_replicas",
        [
            pytest.param(True, marks=pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#24816")), id="colocated_view_replicas"),
            pytest.param(False, id="non_colocated_view_replicas"),
        ],
    )
    def test_repair_mv(self, colocated_view_replicas: bool):
        """Test repair of materialized view"""
        configuration_options = {
            "hinted_handoff_enabled": False,
            "range_request_timeout_in_ms": self.count_request_timeout * 1000,
        }
        session = self.prepare(rf=3, nodes=3, options=configuration_options, fetch_size=100, request_timeout=self.count_request_timeout)
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int, v1 int, v2 int, PRIMARY KEY (id))")

        if colocated_view_replicas:
            session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE v1 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (id, v1)")
        else:
            session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE v1 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v1, id)")

        prefill = 100
        for i in range(prefill):
            # insert symmetric values to (id, v1) so when it's transformed to (v1, id) in mv the table and view will be identical.
            # we use this property to verify the view consistency
            if i % 2 == 0:
                v1 = i + 1
            else:
                v1 = i - 1
            session.execute(f"INSERT INTO t (id, v1, v2) VALUES ({i}, {v1}, {2})")

        table_statement = "SELECT * FROM t"
        mv_statement = "SELECT * FROM mv"
        self.eventually(lambda: assert_two_queries_equal(session, table_statement, session, mv_statement, session_timeout=self.session_timeout))

        node2.stop(wait_other_notice=True)

        for i in range(prefill // 2):
            session.execute(f"UPDATE t SET v2=3 WHERE id={i}")

        # We need to read from the view with TWO because when one node is
        # down, it's possible that two base replicas are updated but only
        # a single view replica.
        self.eventually(lambda: assert_two_queries_equal(session, table_statement, session, mv_statement, session_timeout=self.session_timeout, consistency_level=ConsistencyLevel.TWO))

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Repair the mv replica")
        node1.repair(keyspace="ks", tables=["mv"])

        logger.debug("Stop [node1, node3]")
        self.cluster.stop_nodes([node1, node3], wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node2)
        session.execute("USE ks")

        # Validate data
        logger.debug("Verify the MV data for updated rows in the MV with CL=ONE")
        assert_one(session, f"select count(*) from mv where v2=2 ALLOW FILTERING", [prefill - prefill // 2])

        logger.debug("Verify the MV data for not updated rows in the MV with CL=ONE")
        assert_one(session, f"select count(*) from mv where v2=3 ALLOW FILTERING", [prefill // 2])

        logger.debug("Verify the base table data with CL=ONE - all rows shouldn't be updated")
        for i in range(prefill):
            assert_one(session, f"select v2 from t where id={i}", [2])

    def test_mv_sync_after_remove_add_node(self):
        """
        Test that a materialized view is consistent after removing a node, then adding a new node.
        The data is eventually verified to be correct after going through build-view and off-strategy compaction.
        Test scenario:
        - insert data (rf 3, 4 nodes).
        - Decommission node4
        - Add and repair node5
        - Stop nodes 2,3
        - Verify all data exists, querying nodes 1,5 with CL one.
        """

        session = self.prepare(rf=3, options={"hinted_handoff_enabled": False}, nodes={"dc1": {"RAC1": 1, "RAC2": 1, "RAC3": 2}})
        node4 = self.cluster.nodelist()[-1]

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH read_repair_chance = 0.0")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id) WITH read_repair_chance = 0.0")

        session.cluster.control_connection.wait_for_schema_agreement()

        partition_num = 4444
        for i in range(partition_num):
            session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")

        logger.debug(f"Decommission {node4.name}")
        node4.decommission()

        logger.debug("Verify the data in the MV with CL=ONE")
        for i in range(partition_num):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.ONE)

        new_node_session = self._add_new_node(wait_other_notice=True, rack="RAC3")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v", cl=ConsistencyLevel.ALL)
        logger.info("Stopping nodes 2,3")
        for node in self.cluster.nodelist()[1:3]:
            node.stop(gently=True, wait_other_notice=True)
        logger.info("Verify the data in the MV with CL=ONE.")
        for i in range(partition_num):
            assert_one(new_node_session, f"SELECT * FROM ks.t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.ONE)

    def test_simple_repair(self):
        """
        Test that a materialized view are consistent after a simple repair.
        """

        session = self.prepare(rf=3, options={"hinted_handoff_enabled": False})
        node1, node2, _node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH read_repair_chance = 0.0")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id) WITH read_repair_chance = 0.0")

        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("Shutdown node2")
        node2.stop(wait_other_notice=True)

        for i in range(1000):
            session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")

        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()

        # We need to read from the view with TWO because when one node is
        # down, it's possible that two base replicas are updated but only
        # view replica. See https://github.com/scylladb/scylladb/issues/17043.
        logger.debug("Verify the data in the MV with CL=TWO")

        def check():
            for i in range(1000):
                assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.TWO)

        self.eventually(check)

        logger.debug("Verify the data in the MV with CL=ALL. All should be unavailable.")
        for i in range(1000):
            statement = SimpleStatement(f"SELECT * FROM t_by_v WHERE v = {i}", consistency_level=ConsistencyLevel.ALL)

            assert_unavailable(session.execute, statement)

        logger.debug("Start node2, and repair")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.repair()

        logger.debug("Verify the data in the MV with CL=ONE. All should be available now.")
        for i in range(1000):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.ONE)

    def test_base_replica_repair(self):
        self._base_replica_repair_test()

    # This test is taken from Cassandra. Not relevant for us
    # def test_base_replica_repair_with_contention(self):
    #     """
    #     Test repair does not fail when there is MV lock contention
    #     @jira_ticket CASSANDRA-12905
    #     """

    #     self._base_replica_repair_test(fail_mv_lock=True)

    def _base_replica_repair_test(self, fail_mv_lock=False):
        """
        Test that a materialized view are consistent after the repair of the base replica.
        """

        session = self.prepare(rf=3)
        node1, node2, node3 = self.cluster.nodelist()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")
        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("Write initial data")
        num_keys = 10000
        for i in range(num_keys):
            session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")

        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()

        logger.debug("Verify the data in the MV with CL=ALL")
        for i in range(num_keys):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.ALL)

        logger.debug("Shutdown node1")
        node1.stop(wait_other_notice=True)

        logger.debug("Delete node1 data")
        for tname in ["t_by_v", "t"]:
            table_folder = get_node_cf_dir(node=node1, ks_name="ks", cf_name=tname)
            logger.info(f"Removing SSTables from folder '{table_folder}'")
            remove_files_in_folder(table_folder)

        # This code is taken from Cassandra. Not relevant for us
        # jvm_args = []
        # if fail_mv_lock:
        #     if self.cluster.version() >= LooseVersion('3.10'):  # CASSANDRA-10134
        #         jvm_args = ['-Dcassandra.allow_unsafe_replace=true', '-Dcassandra.replace_address={}'.format(node1.address())]
        #     jvm_args.append("-Dcassandra.test.fail_mv_locks_count=1000")
        #     # this should not make Keyspace.apply throw WTE on failure to acquire lock
        #     node1.set_configuration_options(values={'write_request_timeout_in_ms': 100})
        # logger.debug('Restarting node1 with jvm_args={}'.format(jvm_args))
        # node1.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=jvm_args)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.debug("Shutdown node2 and node3")
        self.cluster.stop_nodes([node2, node3], wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("USE ks")

        logger.debug("Verify that there is no data on node1")
        for i in range(num_keys):
            assert_none(session, f"SELECT * FROM t_by_v WHERE v = {i}")

        logger.debug("Restarting node2 and node3")
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        # Just repair the base replica
        logger.debug("Starting repair on node1")
        node1.repair(keyspace="ks", tables=["t"])

        # Until https://github.com/scylladb/scylladb/issues/19727 is fixed
        logger.info(f"Waiting for view update generation")
        wait_for_view_update_generation(node1)

        # repair the view on all node since base/view self-pairing
        # is not guaranteed with tablets
        for node in self.cluster.nodelist():
            node.repair(keyspace="ks", tables=["t_by_v"], partitioner_range=True)

        logger.debug("Verify base table data with cl=ONE")
        for i in range(num_keys):
            assert_one(session, f"SELECT * FROM t WHERE id = {i}", [i, i, "a", 3.0])

        logger.debug("Verify materialize view data with cl=ONE")
        for i in range(num_keys):
            assert_one(session, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

    def _stop_nodes(self, nodes):
        logger.debug(f"Stopping {[node.name for node in nodes]}")
        self.cluster.stop_nodes(nodes, wait_other_notice=True)

    def _start_nodes(self, nodes):
        logger.debug(f"Starting {[node.name for node in nodes]}")
        self.cluster.start_nodes(nodes, wait_other_notice=True, wait_for_binary_proto=True)

    # This test's setup assumes it can start a 5-node cluster with RF=5,
    # kill 3 nodes, and writes will go to the two live view replicas.
    # This is no longer true when tablets are enabled, because we disabled
    # so called "self-pairing", and although two base replicas are alive,
    # they may be paired by two dead nodes and no view update will succeed.
    # https://github.com/scylladb/scylladb/issues/17043 can be done to
    # make this test work again.
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#17043"))
    def test_complex_repair(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        def _verify_data_by_one(session, rows, cl, multiply, debug_message, none_data=False):  # noqa: PLR0913
            logger.debug(debug_message)
            statement_template = "SELECT * FROM ks.t_by_v WHERE v = {}"
            for i in range(rows):
                v = i * 2 if multiply else i
                statement = statement_template.format(v)
                if not none_data:
                    expected_row = [v, v, "a", 6.0] if multiply else [v, v, "a", 3.0]
                    assert_one(session, statement, expected_row, cl=cl)
                else:
                    assert_none(session2, statement, cl=cl)

        session = self.prepare(rf=5, options={"hinted_handoff_enabled": False, "read_repair_chance": 0.0}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)WITH gc_grace_seconds = 5")
        session.execute("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")
        session.cluster.control_connection.wait_for_schema_agreement()

        self._stop_nodes([node2, node3])
        rows = 1000

        logger.debug("Write initial data to node1 (will be replicated to node4 and node5)")
        for i in range(rows):
            session.execute(SimpleStatement(f"INSERT INTO ks.t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)", consistency_level=ConsistencyLevel.THREE))

        _verify_data_by_one(session, rows, ConsistencyLevel.ONE, False, "Verify the data in the MV on node1 with CL=ONE")

        self._stop_nodes([node1, node4, node5])

        self._start_nodes([node2, node3])

        session2 = self.patient_cql_connection(node2)

        _verify_data_by_one(session2, rows, ConsistencyLevel.ONE, False, "Verify the data in the MV on node2 with CL=ONE. No rows should be found.", none_data=True)

        logger.debug("Write new data in node2 and node3 that overlap those in node1, node4 and node5")
        for i in range(rows):
            # we write i*2 as value, instead of i
            session2.execute(SimpleStatement(f"INSERT INTO ks.t (id, v, v2, v3) VALUES ({i * 2}, {i * 2}, 'a', 6.0)", consistency_level=ConsistencyLevel.TWO))

        _verify_data_by_one(session2, rows, ConsistencyLevel.ONE, True, "Verify the new data in the MV on node2 with CL=ONE")

        # Scylla doesn't leverage the batchlog for MVs
        # logger.debug('Wait for batchlogs to expire from node2 and node3')
        # time.sleep(5)

        self._start_nodes([node1, node4, node5])
        self._stop_nodes([node2, node3])
        session = self.patient_cql_connection(node1)

        _verify_data_by_one(session, rows, ConsistencyLevel.QUORUM, False, "Verify the new data in the MV on node2 with CL=ONE")

        self._start_nodes([node2, node3])

        logger.debug("Run global repair on node1")
        node1.repair()

        self._stop_nodes([node2, node3])

        table_statement = "SELECT * FROM ks.t"
        mv_statement = "SELECT * FROM ks.t_by_v"
        logger.debug("Read data from MV at quorum (new data should be returned after repair)")
        assert_two_queries_equal(session, table_statement, session, mv_statement, consistency_level=ConsistencyLevel.QUORUM, session_timeout=self.session_timeout)

        self._start_nodes([node2, node3])
        self._stop_nodes([node1, node4, node5])

        session2 = self.patient_cql_connection(node2)

        logger.debug("Read data from MV at quorum (new data should be returned after repair)")
        assert_two_queries_equal(session2, table_statement, session2, mv_statement, consistency_level=ConsistencyLevel.ONE, session_timeout=self.session_timeout)

    # This test's setup assumes it can start a 5-node cluster with RF=5,
    # kill 3 nodes, and writes will go to the two live view replicas.
    # This is no longer true when tablets are enabled, because we disabled
    # so called "self-pairing", and although two base replicas are alive,
    # they may be paired by two dead nodes and no view update will succeed.
    # https://github.com/scylladb/scylladb/issues/17043 can be done to
    # make this test work again.
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#17043"))
    def test_really_complex_repair(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        session = self.prepare(rf=5, options={"hinted_handoff_enabled": False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int, v int, v2 text, v3 decimal, PRIMARY KEY(id, v, v2))WITH gc_grace_seconds = 1")
        session.execute("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL AND v IS NOT NULL AND v2 IS NOT NULL PRIMARY KEY (v2, v, id)")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")
        session.cluster.control_connection.wait_for_schema_agreement()

        self._stop_nodes([node2, node3])

        # We have a cluster of 5 nodes and a keyspace of RF=5. Only 3 of
        # the nodes are currently alive, so a write will *eventually* succeed
        # to these 3 nodes. To avoid this test becoming flaky (see
        # https://github.com/scylladb/scylladb/issues/15314) let's convert
        # this eventuality to something synchronous, by using CL=THREE
        # we'll wait for all three copies to be written. Because N=RF, the
        # view writes are also synchronous, so the view too will be up-to-date
        # as soon as these inserts finish.
        session.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0)", consistency_level=ConsistencyLevel.THREE))
        session.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'a', 3.0)", consistency_level=ConsistencyLevel.THREE))
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        logger.debug("Verify the data in the MV on node1 with CL=ONE")
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [["a", 1, 1, 3.0], ["a", 2, 2, 3.0]])

        session.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'b', 3.0)", consistency_level=ConsistencyLevel.THREE))
        session.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'b', 3.0)", consistency_level=ConsistencyLevel.THREE))
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        logger.debug("Verify the data in the MV on node1 with CL=ONE")
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'b'", [["b", 1, 1, 3.0], ["b", 2, 2, 3.0]])

        session.shutdown()

        self._stop_nodes([node1, node4, node5])
        self._start_nodes([node2, node3])

        session2 = self.patient_exclusive_cql_connection(node2)
        session2.execute("USE ks")

        logger.debug("Verify the data in the MV on node2 with CL=ONE. No rows should be found.")
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'")

        logger.debug("Write new data in node2 that overlap those in node1")
        session2.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'c', 3.0)", consistency_level=ConsistencyLevel.TWO))
        session2.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'c', 3.0)", consistency_level=ConsistencyLevel.TWO))
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'", [["c", 1, 1, 3.0], ["c", 2, 2, 3.0]])

        session2.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'd', 3.0)", consistency_level=ConsistencyLevel.TWO))
        session2.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'd', 3.0)", consistency_level=ConsistencyLevel.TWO))
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'", [["d", 1, 1, 3.0], ["d", 2, 2, 3.0]])

        logger.debug("Composite delete of everything")
        session2.execute(SimpleStatement("DELETE FROM ks.t WHERE id = 1 and v = 1", consistency_level=ConsistencyLevel.TWO))
        session2.execute(SimpleStatement("DELETE FROM ks.t WHERE id = 2 and v = 2", consistency_level=ConsistencyLevel.TWO))
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'")
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'")

        # Scylla doesn't leverage the batchlog for MVs
        # logger.debug('Wait for batchlogs to expire from node2 and node3')
        # time.sleep(5)

        logger.debug("Start remaining nodes")
        self._start_nodes([node1, node4, node5])

        # at this point the data may not be repaired yet so we may have an inconsistency.
        # this value should return either the expected data or None
        assert_all_or_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [["a", 1, 1, 3.0], ["a", 2, 2, 3.0]], cl=ConsistencyLevel.QUORUM)

        logger.debug("Run global repair on node1")
        node1.repair()

        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", cl=ConsistencyLevel.QUORUM)

    def test_complex_mv_select_statements(self):
        """
        Test complex MV select statements
        @jira_ticket CASSANDRA-9664
        """

        self.prepare(rf=3)
        node1, _, _ = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        logger.debug("Creating keyspace")
        session.execute("CREATE KEYSPACE mvtest WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}")
        session.execute("USE mvtest")

        mv_primary_keys = ["((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"]

        for mv_primary_key in mv_primary_keys:
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c))")

            insert_stmt = session.prepare("INSERT INTO test (a, b, c, d) VALUES (?, ?, ?, ?)")
            update_stmt = session.prepare("UPDATE test SET d = ? WHERE a = ? AND b = ? AND c = ?")
            delete_stmt1 = session.prepare("DELETE FROM test WHERE a = ? AND b = ? AND c = ?")
            delete_stmt2 = session.prepare("DELETE FROM test WHERE a = ?")

            session.cluster.control_connection.wait_for_schema_agreement()

            rows = [(0, 0, 0, 0), (0, 0, 1, 0), (0, 1, 0, 0), (0, 1, 1, 0), (1, 0, 0, 0), (1, 0, 1, 0), (1, 1, -1, 0), (1, 1, 0, 0), (1, 1, 1, 0)]

            for row in rows:
                session.execute(insert_stmt, row)

            logger.debug(f"Testing MV primary key: {mv_primary_key}")

            session.execute(f"CREATE MATERIALIZED VIEW mv AS SELECT * FROM test WHERE a = 1 AND b IS NOT NULL AND c = 1 PRIMARY KEY {mv_primary_key}")

            wait_for_view(cluster=self.cluster, session=session, ks="mvtest", view="mv")

            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 1, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)

            # insert new rows that does not match the filter
            session.execute(insert_stmt, (0, 0, 1, 0))
            session.execute(insert_stmt, (1, 1, 0, 0))
            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 1, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # insert new row that does match the filter
            session.execute(insert_stmt, (1, 2, 1, 0))
            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # update rows that does not match the filter
            session.execute(update_stmt, (1, 1, -1, 0))
            session.execute(update_stmt, (0, 1, 1, 0))
            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # update a row that does match the filter
            session.execute(update_stmt, (2, 1, 1, 1))
            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # delete rows that does not match the filter
            session.execute(delete_stmt1, (1, 1, -1))
            session.execute(delete_stmt1, (2, 0, 1))
            session.execute(delete_stmt2, (0,))
            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # delete a row that does match the filter
            session.execute(delete_stmt1, (1, 1, 1))
            assert_all(session, "SELECT a, b, c, d FROM mv", [[1, 0, 1, 0], [1, 2, 1, 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # delete a partition that matches the filter
            session.execute(delete_stmt2, (1,))
            assert_all(session, "SELECT a, b, c, d FROM mv", [], cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # Cleanup
            session.execute("DROP MATERIALIZED VIEW mv")
            session.execute("DROP TABLE test")

    def _test_base_view_consistency_on_crash(self, fail_phase):
        """
        * Fails base table write before or after applying views
        * Restart node and replay commit and batchlog
        * Check that base and views are present

        @jira_ticket CASSANDRA-13069
        """

        self.cluster.set_batch_commitlog(enabled=True)
        self.fixture_dtest_setup.ignore_log_patterns += [r"Dummy failure", r"Failed to force-recycle all segments"]
        self.prepare(rf=1, install_byteman=True)
        node1, _node2, _node3 = self.cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        session.execute("USE ks")

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug(f"Make node1 fail {fail_phase} view writes")
        node1.byteman_submit([f"./byteman/fail_{fail_phase}_view_write.btm"])

        logger.debug("Write 1000 rows - all node1 writes should fail")

        failed = False
        for i in range(1, 1000):
            try:
                session.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0) USING TIMESTAMP {i}")
            except WriteFailure:
                failed = True

        assert failed, "Should fail at least once."
        assert node1.grep_log("Dummy failure"), "Should throw Dummy failure"

        missing_entries = 0
        session = self.patient_exclusive_cql_connection(node1)
        session.execute("USE ks")
        for i in range(1, 1000):
            view_entry = rows_to_list(session.execute(SimpleStatement(f"SELECT * FROM t_by_v WHERE id = {i} AND v = {i}", consistency_level=ConsistencyLevel.ONE)))
            base_entry = rows_to_list(session.execute(SimpleStatement(f"SELECT * FROM t WHERE id = {i}", consistency_level=ConsistencyLevel.ONE)))

            if not base_entry:
                missing_entries += 1
            if not view_entry:
                missing_entries += 1

        logger.debug(f"Missing entries {missing_entries}")
        assert missing_entries > 0

        logger.debug("Restarting node1 to ensure commit log is replayed")
        node1.stop(wait_other_notice=True)
        # Set batchlog.replay_timeout_seconds=1 so we can ensure batchlog will be replayed below
        node1.start(jvm_args=["-Dcassandra.batchlog.replay_timeout_in_ms=1"], no_wait=True)

        logger.debug("Replay batchlogs")
        time.sleep(0.001)  # Wait batchlog.replay_timeout_in_ms=1 (ms)
        self._replay_batchlogs()

        logger.debug("Verify that both the base table entry and view are present after commit and batchlog replay")
        session = self.patient_exclusive_cql_connection(node1)
        session.execute("USE ks")
        for i in range(1, 1000):
            view_entry = rows_to_list(session.execute(SimpleStatement(f"SELECT * FROM t_by_v WHERE id = {i} AND v = {i}", consistency_level=ConsistencyLevel.ONE)))
            base_entry = rows_to_list(session.execute(SimpleStatement(f"SELECT * FROM t WHERE id = {i}", consistency_level=ConsistencyLevel.ONE)))

            assert base_entry, f"Both base {base_entry} and view entry {view_entry} should exist."
            assert view_entry, f"Both base {base_entry} and view entry {view_entry} should exist."

    def _write_to_hinted_handoff_for_views(self, double_failure):
        """
        Test that view updates are stored as hints in data/view_pending_updates directory
        and that reading data from a view is consistent after updates stored as hints.
        """
        session = self.prepare(user_table=True, rf=3, nodes=3, options={"hinted_handoff_enabled": True})
        _node1, node2, node3 = self.cluster.nodelist()
        ks = "ks"
        session.execute(f"USE {ks}")

        for i in range(500):
            session.execute(SimpleStatement(f"INSERT INTO users (username, password, gender, birth_year) VALUES('Jane{i}', 'Doe', 'F', 1980)", consistency_level=ConsistencyLevel.ALL))
        self.cluster.flush()

        stopped = [node2]
        if double_failure:
            stopped.append(node3)
        self.cluster.stop_nodes(stopped, wait_other_notice=True)

        num_updates = 500
        for i in range(num_updates):
            session.execute(SimpleStatement(f"UPDATE users SET state = 'CA{i}' WHERE username = 'Jane{1500 - 2 * i}'", consistency_level=ConsistencyLevel.ANY))
        self.cluster.start_nodes(stopped, wait_for_binary_proto=True, wait_other_notice=True)
        view = "users_by_state"
        # Wait until the view is built.
        # Note that it won't wait until all the data is propagated from hinted handoff
        wait_for_view(cluster=self.cluster, session=session, ks=ks, view=view)
        total_wait = 60
        sleep_time = 5
        returned_rows = []
        for _ in range(total_wait // sleep_time):
            returned_rows = [row for row in session.execute(SimpleStatement(f"SELECT * FROM {view}", consistency_level=ConsistencyLevel.ALL))]
            if len(returned_rows) == num_updates:
                break
            else:
                logger.debug(f"Expected {num_updates} rows, got {len(returned_rows)}. Will retry in {sleep_time} second(s)")
                time.sleep(sleep_time)

        assert len(returned_rows) == num_updates
        for row in returned_rows:
            assert int(row.username[4:]) == 1500 - 2 * int(row.state[2:])

    def test_write_to_hinted_handoff_for_views(self):
        self._write_to_hinted_handoff_for_views(double_failure=False)

    def test_write_to_hinted_handoff_for_views_double_failure(self):
        self._write_to_hinted_handoff_for_views(double_failure=True)

    def test_virtual_columns_schema(self):
        """
        Test that virtual columns in materialized views are correctly
        propagated between nodes as part of the schema. Reproduces issue #4339.
        """
        # Create a cluster of three nodes.
        cluster = self.cluster
        cluster.populate({"dc1": [1, 1, 1]})
        cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
        [node1, node2, _node3] = self.cluster.nodelist()
        # Create a keyspace and base table, while the three nodes are alive
        session = self.patient_cql_connection(node1)
        self.total_rf = 3
        create_ks(session, "ks", self.total_rf)
        session.execute("CREATE TABLE tab (a INT, b INT, c INT,PRIMARY KEY (a));")
        # Wait for all nodes to know about the base table
        session.cluster.control_connection.wait_for_schema_agreement()
        # stop the second node, and create a materialized view which only
        # the first and third node will know about:
        node2.stop(wait_other_notice=True)
        session.execute("CREATE MATERIALIZED VIEW mv AS SELECT a,b FROM tab WHERE a IS NOT NULL PRIMARY KEY (a)")
        # Because the above materialized views has the same key columns
        # as the base table and an unselected column (c), it will have c
        # as a "virtual column", and should be listed in the
        # "view_virtual_columns" system table.
        result1 = list(session.execute("SELECT * FROM system_schema.view_virtual_columns WHERE keyspace_name='ks' ALLOW FILTERING"))
        logger.debug(result1)
        assert len(result1) == 1, "expecting one virtual column"
        # Start the dead node. It should copy the missing schema tables
        # from the live node, including the view_virtual_columns table.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(node2)
        result2 = list(session2.execute("SELECT * FROM system_schema.view_virtual_columns WHERE keyspace_name='ks' ALLOW FILTERING"))
        logger.debug(result2)
        assert len(result2) == 1, "expecting one virtual column"
        assert result1 == result2, "expecting same results on both nodes"

    @pytest.mark.dtest_debug
    @pytest.mark.scylla_mode("!release")
    def test_injected_noncritical_errors(self):
        self.fixture_dtest_setup.ignore_log_patterns += [r".*std::runtime_error.*view.*"]
        cluster = self.cluster
        cluster.populate({"dc1": generate_rack_topology_based_rf(nodes=2, rf=2), "dc2": 0})
        cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
        nodes = self.cluster.nodelist()
        [node1, node2] = nodes
        session = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        self.total_rf = 2
        create_ks(session, "ks", self.total_rf)
        session.execute("CREATE TABLE tab (a INT, b INT, c INT,PRIMARY KEY (a));")
        # Wait for both nodes to know about the base table
        session.cluster.control_connection.wait_for_schema_agreement()
        for node in nodes:
            self.disable_errors(node)
        # Arm the injection points
        injection_points = [
            "table_push_view_replica_updates_stale_time_point",
            "table_push_view_replica_updates_timeout",
            "view_builder_load_views",
            "view_builder_check_for_built_views",
            "view_builder_consume_new_partition",
            "view_builder_consume_tombstone",
            "view_builder_consume_static_row",
            "view_builder_consume_clustering_row",
            "view_builder_consume_range_tombstone",
            "view_builder_flush_fragments",
            "view_builder_consume_end_of_partition",
            "view_builder_consume_end_of_stream",
            "view_builder_mark_view_as_built",
            "view_update_generator_consume_staging_sstable",
            "view_update_generator_collect_consumed_sstables",
            "view_update_generator_move_staging_sstable",
            "view_update_generator_registering_staging_sstable",
        ]
        for i, injection_point in enumerate(injection_points):
            self.enable_error(injection_point, i % 2, one_shot=True)

        for i in range(10):
            session.execute(SimpleStatement(f"INSERT INTO tab (a, b, c) VALUES({i}, {2 * i}, {-i})", consistency_level=ConsistencyLevel.ALL))

        # Create a view and wait until it's built
        session.execute("CREATE MATERIALIZED VIEW mv AS SELECT a,b FROM tab WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)")

        for i in range(5, 20):
            session2.execute(SimpleStatement(f"INSERT INTO ks.tab (a, b, c) VALUES({i}, {2 * i}, {-i})", consistency_level=ConsistencyLevel.ALL))

        get_all = "SELECT * FROM ks.mv"

        self.eventually(lambda: assert_row_count_in_select(session, get_all, 20, ConsistencyLevel.ALL))
        self.eventually(lambda: assert_row_count_in_select(session2, get_all, 20, ConsistencyLevel.ALL))

    def prepare_schema_for_range_tombstone_tests(self, session, keyspace_name, table_name, mv_name):
        session.execute(f"CREATE TABLE {table_name} (id int, ck int, v2 int, v3 text, PRIMARY KEY(id, ck))")
        session.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS SELECT * FROM {table_name} WHERE ck IS NOT NULL  AND v2 is not null PRIMARY KEY (v2, id, ck)")
        wait_for_view(cluster=self.cluster, session=session, ks=keyspace_name, view=mv_name)
        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("Write initial data 300 rows")
        for k in range(4):
            for i in range(300):
                session.execute(f"INSERT INTO {table_name} (id, ck, v2, v3) VALUES ({k}, {i}, {i}, '{10000 * ' '}')")
        self.cluster.flush()

    @staticmethod
    def delete_range(where_clauses, session, table_name):
        for where_clause in where_clauses:
            logger.info(f"Delete range data with were clause {where_clause}")
            session.execute(f"DELETE FROM {table_name} where {where_clause}")

    @staticmethod
    def delete_keyspace_sstables(node, keyspace_name, mv_name, table_name):
        logger.debug(f'Delete {keyspace_name} sstables on node "{node.name}"')
        for tname in [mv_name, table_name]:
            table_folder = get_node_cf_dir(node=node, ks_name=keyspace_name, cf_name=tname)
            logger.info(f"Removing SSTables from folder '{table_folder}'")
            remove_files_in_folder(table_folder)

    @staticmethod
    def find_view_update_generator_error(node, table_name, mark):
        errors = node.grep_log_for_errors(from_mark=mark)

        expected_error = f"permit ks.{table_name}:view_update_generator: was not closed before destruction"
        return [err for err in ["\n".join(err) for err in errors] if expected_error in err]

    @pytest.mark.parametrize(
        "where_clauses",
        [["id = 0 and ck >= 0"], ["id = 0 and ck > 0 and ck < 300"], ["id in (0, 1) and ck > 50 and ck < 290"], ["id in (0, 1) and ck > 150"], ["id = 0 and ck > 250", "id in (0, 1) and ck > 250", "id in (0, 1) and ck > 200 and ck < 250"]],
        ids=["delete_open_range", "delete_close_range", "delete_close_range_in_few_partitions", "delete_open_range_in_few_partitions", "run_few_delete_queries"],
    )
    def test_range_tombstone_and_repair_test(self, where_clauses):
        """
        https://github.com/scylladb/scylladb/commit/c25201c1a311cdb23056404947af00c3237fc876

        Reproducer for issue https://github.com/scylladb/scylla-enterprise/issues/3072#issuecomment-1605647790:

        When a base table of a materialized view is updated, the affected rows are also changed in the materialized view.
        For DELETE statements, many rows can be affected, and so the view update code splits the work into batches.
        However, this split was not performed correctly when range tombstones were involved.
        When the view_updating_consumer exceeds its buffer size limit, it flushes the mutation fragment stream in the middle of a partition.
        But it doesn't take care to maintain range tombstones properly while doing this, and if the buffer limit is exceeded in the middle
        of a range tombstone, the mutation fragment stream will end with an unclosed range tombstone, which is illegal.

        This test will create range of tombstones by using different where clauses, every time on the new cluster
        """
        self.allow_log_errors = True
        session = self.prepare(rf=2, nodes=2)
        node1, node2 = self.cluster.nodelist()

        keyspace_name = "ks"
        table_name = "tombstone_table"
        mv_name = "tombstone_mv_by_v2"
        self.prepare_schema_for_range_tombstone_tests(session=session, keyspace_name=keyspace_name, table_name=table_name, mv_name=mv_name)

        self.delete_range(where_clauses=where_clauses, session=session, table_name=table_name)

        logger.debug("Shutdown node2")
        node2.stop(wait_other_notice=True)

        self.delete_keyspace_sstables(node=node2, keyspace_name=keyspace_name, mv_name=mv_name, table_name=table_name)

        logger.debug("Start node2")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Starting repair on node2")
        mark = node2.mark_log()
        try:
            node2.repair(keyspace=f"{keyspace_name}", timeout=60)
        except subprocess.TimeoutExpired:
            pass

        found_error = self.find_view_update_generator_error(node=node2, table_name=table_name, mark=mark)
        assert not found_error, f"Found error during repair: {found_error}"

        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, keyspace_name)
        logger.debug(f"Validate data in {mv_name} - expected same data as in {table_name}")
        assert_two_queries_equal_ignore_order(session1=session, query1=f"select id, ck, v2, v3 from {table_name}", session2=session, query2=f"select id, ck, v2, v3 from {mv_name}")

        # Validate that tombstones were repaired and rows were deleted
        for where_clause in where_clauses:
            assert_none(query=f"select * from {mv_name} where {where_clause} ALLOW FILTERING", session=session)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_range_tombstone_and_repair_multiple_cycles(self):
        """
        https://github.com/scylladb/scylladb/commit/c25201c1a311cdb23056404947af00c3237fc876

        Reproducer for issue https://github.com/scylladb/scylla-enterprise/issues/3072#issuecomment-1605647790:

        When a base table of a materialized view is updated, the affected rows are also changed in the materialized view.
        For DELETE statements, many rows can be affected, and so the view update code splits the work into batches.
        However, this split was not performed correctly when range tombstones were involved.
        When the view_updating_consumer exceeds its buffer size limit, it flushes the mutation fragment stream in the middle of a partition.
        But it doesn't take care to maintain range tombstones properly while doing this, and if the buffer limit is exceeded in the middle
        of a range tombstone, the mutation fragment stream will end with an unclosed range tombstone, which is illegal.

        This test will run create range of tombstones, delete sstables and repair the node a few times, the same node without
        recreate a cluster
        """
        self.allow_log_errors = True
        session = self.prepare(rf=2, nodes=2)
        node1, node2 = self.cluster.nodelist()

        keyspace_name = "ks"
        table_name = "tombstone_table"
        mv_name = "tombstone_mv_by_v2"
        self.prepare_schema_for_range_tombstone_tests(session=session, keyspace_name=keyspace_name, table_name=table_name, mv_name=mv_name)

        for where_clause in ["id = 0 and ck >= 0", "id = 1 and ck > 0 and ck < 300", "id in (2, 3) and ck > 50 and ck < 150", "id in (2, 3) and ck > 150"]:
            self.delete_range(where_clauses=[where_clause], session=session, table_name=table_name)

            logger.debug("Shutdown node2")
            node2.stop(wait_other_notice=True)

            self.delete_keyspace_sstables(node=node2, keyspace_name=keyspace_name, mv_name=mv_name, table_name=table_name)

            logger.debug("Start node2")
            node2.start(wait_other_notice=True, wait_for_binary_proto=True)

            logger.debug("Starting repair on node2")
            mark = node2.mark_log()
            try:
                node2.nodetool(f"repair {keyspace_name}", timeout=60)
            except subprocess.TimeoutExpired:
                pass

            found_error = self.find_view_update_generator_error(node=node2, table_name=table_name, mark=mark)
            assert not found_error, f"Found error during repair: {found_error}"

            node1.stop(wait_other_notice=True)
            session = self.patient_cql_connection(node2, keyspace_name)
            logger.debug(f"Validate data in {mv_name} - expected same data as in {table_name}")
            assert_two_queries_equal_ignore_order(session1=session, query1=f"select id, ck, v2, v3 from {table_name}", session2=session, query2=f"select id, ck, v2, v3 from {mv_name}")

            # Validate that tombstones were repaired and rows were deleted
            assert_none(query=f"select * from {mv_name} where {where_clause} ALLOW FILTERING", session=session)
            node1.start(wait_other_notice=True, wait_for_binary_proto=True)

    @pytest.mark.next_gating
    def test_mv_consistency_after_truncate_and_insert(self, cache):
        """
        1. Create base table and create multiple materialized views.
        2. Populate base table and verify against all materialized views
        3. Truncate base table and assert all mv are empty.
        4. Populate base table again and verify.
        """
        session = self.prepare(rf=3, nodes=3, consistency_level=ConsistencyLevel.QUORUM)
        keyspace_name = "ks"
        base_table = "truncate_test"
        create_c1c2_table(session=session, cf=base_table)

        logger.info("Inserting data...")
        rows = 10000
        insert_c1c2(session, n=rows, cf=base_table, c1_values=[f"c1 value {i}" for i in range(rows)], c2_values=[f"c2 value {i}" for i in range(rows)])
        assert_row_count(session, table_name=base_table, expected=rows)

        mv_name_prefix = "mv_cf_view"
        mvs_count = 10
        self.create_few_mv(mvs_count=mvs_count, session=session, keyspace_name=keyspace_name, table_name=base_table, mv_name_prefix=mv_name_prefix, synchronous_updates=True, rows=rows)
        for i in range(mvs_count):
            assert_row_count(session, table_name=f"{mv_name_prefix}_{i}", expected=rows)
        logger.info("Truncate base table")
        session.execute(f"TRUNCATE {keyspace_name}.{base_table}")
        for i in range(mvs_count):
            assert_row_count(session, table_name=f"{mv_name_prefix}_{i}", expected=0)
        logger.info("Inserting more data...")
        insert_c1c2(session, n=rows, cf=base_table, c1_values=[f"c1 value {i}" for i in range(rows)], c2_values=[f"c2 value {i}" for i in range(rows)])
        assert_row_count(session, table_name=base_table, expected=rows)
        for i in range(mvs_count):
            assert_row_count(session, table_name=f"{mv_name_prefix}_{i}", expected=rows)


# For read verification


class MutationPresence(Enum):
    __order__ = "match extra missing excluded unknown"
    match = 1
    extra = 2
    missing = 3
    excluded = 4
    unknown = 5


class MM:
    mp = None

    def out(self):
        pass


class Match(MM):
    def __init__(self):
        self.mp = MutationPresence.match

    def out(self):
        return None


class Extra(MM):
    expecting = None
    value = None
    row = None

    def __init__(self, expecting, value, row):
        self.mp = MutationPresence.extra
        self.expecting = expecting
        self.value = value
        self.row = row

    def out(self):
        return f"Extra. Expected {self.expecting} instead of {self.value}; row: {self.row}"


class Missing(MM):
    value = None
    row = None

    def __init__(self, value, row):
        self.mp = MutationPresence.missing
        self.value = value
        self.row = row

    def out(self):
        return f"Missing. At {self.row}"


class Excluded(MM):
    def __init__(self):
        self.mp = MutationPresence.excluded

    def out(self):
        return None


class Unknown(MM):
    def __init__(self):
        self.mp = MutationPresence.unknown

    def out(self):
        return None


read_consistency = ConsistencyLevel.QUORUM
write_consistency = ConsistencyLevel.QUORUM
SimpleRow = collections.namedtuple("SimpleRow", "a b c d")


def row_generate(i, num_partitions):
    return SimpleRow(a=i % num_partitions, b=(i % 400) / num_partitions, c=i, d=i)


# Create a threaded session and execute queries from a Queue
def thread_session(ip, queue, start, end, rows, num_partitions):  # noqa: PLR0913
    def execute_query(session, select_gi, i):
        row = row_generate(i, num_partitions)
        if (row.a, row.b) in rows:
            base = rows[(row.a, row.b)]
        else:
            base = -1
        gi = list(session.execute(select_gi, [row.c, row.a]))
        if base == i and len(gi) == 1:
            return Match()
        elif base != i and len(gi) == 1:
            return Extra(base, i, (gi[0][0], gi[0][1], gi[0][2], gi[0][3]))
        elif base == i and len(gi) == 0:
            return Missing(base, i)
        elif base != i and len(gi) == 0:
            return Excluded()
        else:
            return Unknown()

    try:
        cluster = Cluster([ip])
        session = cluster.connect()
        select_gi = session.prepare("SELECT * FROM mvtest.mv1 WHERE c = ? AND a = ?")
        select_gi.consistency_level = read_consistency

        for i in range(start, end):
            ret = execute_query(session, select_gi, i)
            queue.put_nowait(ret)
    except Exception as e:  # noqa: BLE001
        print(str(e))
        queue.close()


@pytest.mark.skipif(sys.platform == "win32", reason="Bug in python on Windows: https://bugs.python.org/issue10128")
@pytest.mark.dtest_full
class TestMaterializedViewsConsistency(Tester):
    def prepare(self, user_table=False, options=None):
        if options is None:
            options = {}
        cluster = self.cluster
        if options:
            logger.debug(f"Setting cluster configuration options: {options}")
            cluster.set_configuration_options(values=options)
        cluster.populate(3).start()
        node2 = cluster.nodelist()[1]

        # Keep the status of async requests
        self.exception_type = collections.Counter()
        self.num_request_done = 0
        self.counts = {}
        for mp in MutationPresence:
            self.counts[mp] = 0
        self.rows = {}
        self.update_stats_every = 100

        logger.debug("Set to talk to node 2")
        self.session = self.patient_cql_connection(node2)

        return self.session

    def _print_write_status(self, row):
        output = f"\r{row}"
        for key in self.exception_type.keys():
            output = f"{output} ({key}: {self.exception_type[key]})"
        sys.stdout.write(output)
        sys.stdout.flush()

    def _print_read_status(self, row):
        if self.counts[MutationPresence.unknown] == 0:
            sys.stdout.write(f"\rOn {row}; match: {self.counts[MutationPresence.match]}; extra: {self.counts[MutationPresence.extra]}; missing: {self.counts[MutationPresence.missing]}")
        else:
            sys.stdout.write(f"\rOn {row}; match: {self.counts[MutationPresence.match]}; extra: {self.counts[MutationPresence.extra]}; missing: {self.counts[MutationPresence.missing]}; WTF: {self.counts[MutationPresence.unknown]}")
        sys.stdout.flush()

    def _do_row(self, insert_stmt, i, num_partitions):
        # Error callback for async requests
        def handle_errors(row, exc):
            self.num_request_done += 1
            try:
                name = type(exc).__name__
                self.exception_type[name] += 1
            except Exception as e:  # noqa: BLE001
                print(traceback.format_exception_only(type(e), e))

        # Success callback for async requests
        def success_callback(row):
            self.num_request_done += 1

        if i % self.update_stats_every == 0:
            self._print_write_status(i)

        row = row_generate(i, num_partitions)
        async_exec = self.session.execute_async(insert_stmt, row)
        errors = partial(handle_errors, row)
        async_exec.add_callbacks(success_callback, errors)

    def _populate_rows(self):
        statement = SimpleStatement("SELECT a, b, c FROM mvtest.test1", consistency_level=read_consistency)
        data = self.session.execute(statement)
        for row in data:
            self.rows[(row.a, row.b)] = row.c

    @pytest.mark.require("2210")
    def test_single_partition_consistent_reads_after_write(self):
        """
        Tests consistency of multiple writes to a single partition
        @jira_ticket CASSANDRA-10981
        """
        self._consistent_reads_after_write_test(1)

    # nodetool: Found unexpected parameters: [replaybatchlog]
    @pytest.mark.require("2210")
    def test_multi_partition_consistent_reads_after_write(self):
        """
        Taken from Cassandra
        Tests consistency of multiple writes to a multiple partitions
        @jira_ticket CASSANDRA-10981
        """
        self._consistent_reads_after_write_test(20)

    # Scylla doesn't rely on the batchlog, but running this
    # test in debug mode fails because some writes timeout.
    # To enable this, we would need to store failed updates;
    # we plan to leverage hinted handoff for this.

    def _consistent_reads_after_write_test(self, num_partitions):
        session = self.prepare()
        [node1, node2, node3] = self.cluster.nodelist()

        # Test config
        lower = 0
        upper = 100000
        processes = 4
        queues = [None] * processes
        eachProcess = (upper - lower) / processes

        logger.debug("Creating schema")
        session.execute("CREATE KEYSPACE IF NOT EXISTS mvtest WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}")
        session.execute("CREATE TABLE mvtest.test1 (a int, b int, c int, d int, PRIMARY KEY (a,b))")
        session.cluster.control_connection.wait_for_schema_agreement()

        insert1 = session.prepare("INSERT INTO mvtest.test1 (a,b,c,d) VALUES (?,?,?,?)")
        insert1.consistency_level = write_consistency

        logger.debug("Writing data to base table")
        for i in range(upper // 10):
            self._do_row(insert1, i, num_partitions)

        logger.debug("Creating materialized view")
        session.execute("CREATE MATERIALIZED VIEW mvtest.mv1 AS SELECT a,b,c,d FROM mvtest.test1 WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c,a,b)")
        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("Writing more data to base table")
        for i in range(upper // 10, upper):
            self._do_row(insert1, i, num_partitions)

        # Wait that all requests are done
        while self.num_request_done < upper:
            time.sleep(1)

        logger.debug("Making sure all batchlogs are replayed on node1")
        node1.nodetool("replaybatchlog")
        logger.debug("Making sure all batchlogs are replayed on node2")
        node2.nodetool("replaybatchlog")
        logger.debug("Making sure all batchlogs are replayed on node3")
        node3.nodetool("replaybatchlog")

        logger.debug("Finished writes, now verifying reads")
        self._populate_rows()

        for i in range(processes):
            start = lower + (eachProcess * i)
            if i == processes - 1:
                end = upper
            else:
                end = lower + (eachProcess * (i + 1))
            q = Queue()
            node_ip = self.get_ip_from_node(node2)
            p = Process(target=thread_session, args=(node_ip, q, start, end, self.rows, num_partitions))
            p.start()
            queues[i] = q

        for i in range(lower, upper):
            if i % 100 == 0:
                self._print_read_status(i)
            mm = queues[i % processes].get()
            if not mm.out() is None:
                sys.stdout.write(f"\r{mm.out()}\n")
            self.counts[mm.mp] += 1

        self._print_read_status(upper)
        sys.stdout.write("\n")
        sys.stdout.flush()
