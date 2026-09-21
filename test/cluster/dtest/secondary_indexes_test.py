import logging
import os
import random
import re
import time
import uuid
from collections import defaultdict
from collections.abc import Callable

import pytest
from cassandra import (
    ConfigurationException,
    ConsistencyLevel,
    InvalidRequest,
    OperationTimedOut,
    WriteFailure,
)
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.query import BatchStatement, SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_cf, create_ks, retry_till_success
from dtest_setup import DTestSetup
from tools.assertions import PytestRegex as regexp_matches
from tools.assertions import (
    assert_all,
    assert_expected_error,
    assert_invalid,
    assert_none,
    assert_one,
    assert_row_count,
    assert_row_count_in_select,
)
from tools.cluster_topology import generate_cluster_topology_based_rf
from tools.data import (
    create_index,
    create_local_index,
    get_entity_id,
    get_truncated_time_from_system_local,
    get_truncated_time_from_system_truncated,
    get_view_id,
    rows_to_list,
    wait_for_schema_agreement,
)
from tools.marks import issue_open, unmark, unmark_if, with_feature
from tools.misc import generate_random_text, remove_node
from tools.retrying import retrying
from tools.tables_view_manager import (
    get_index_view_name,
    index_is_built,
    view_built_status_query,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating

LONG_TEXT_LENGTH = 8193
OVERSIZE_LENGTH = 66536


class SecondaryIndexesHelpers(Tester):
    compaction_strategy = None
    INDEX_TYPE: str
    cluster: ScyllaCluster
    patient_exclusive_cql_connection: Callable
    patient_cql_connection: Callable
    patient_cql_cluster_session: Callable

    @pytest.fixture(scope="function", autouse=True)
    def fixture_setup_timeouts(self, fixture_dtest_setup):
        if not "debug_mode" in self.__dict__.keys():
            self.debug_mode = isinstance(fixture_dtest_setup.cluster, ScyllaCluster) and fixture_dtest_setup.cluster.scylla_mode == "debug"
            self.session_timeout = 120
            if self.debug_mode:
                self.session_timeout *= 3

    @pytest.fixture(autouse=True)
    def random_compaction_strategy(self, dtest_config):
        if not SecondaryIndexesHelpers.compaction_strategy:
            # NOTE: DateTieredCompactionStrategy is forbidden since https://github.com/scylladb/scylladb/pull/11458, so removed from list below
            strategies = ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"]

            SecondaryIndexesHelpers.compaction_strategy = strategies[random.randint(0, len(strategies) - 1)]
            logger.debug("Randomly selected %s as compaction strategy for base table", SecondaryIndexesHelpers.compaction_strategy)

    def assert_bootstrap_state(self, node, expected_bootstrap_state):
        """
        Assert that a node is on a given bootstrap state
        @param tester The dtest.Tester object to fetch the exclusive connection to the node
        @param node The node to check bootstrap state
        @param expected_bootstrap_state Bootstrap state to expect
        Examples:
        assert_bootstrap_state(node3, 'COMPLETED')
        """
        session = self.patient_exclusive_cql_connection(node)
        assert_all(session, "SELECT bootstrapped FROM system.local WHERE key='local'", [expected_bootstrap_state])

    @staticmethod
    def create_and_build_index(  # noqa: PLR0913
        create_index_func,
        cluster,
        session,
        ks_name,
        table_name,
        index_column,
        index_name,
        pk_name=None,
        compaction=None,
    ):
        if not pk_name:
            create_index_func(session=session, table_name=table_name, index_column=index_column, index_name=index_name, compaction=compaction)
        else:
            create_index_func(session=session, table_name=table_name, index_column=index_column, index_name=index_name, compaction=compaction, pk_name=pk_name)

        return index_is_built(cluster, session, ks_name, table_name, index_name)

    def prepare(  # noqa: PLR0913
        self,
        user_table=False,
        rf=3,
        options=None,
        keyspace_name="ks",
        nodes=3,
        use_vnodes=False,
        fetch_size=None,
        jvm_args=None,
        session_rack=1,
        session_node=1,
        consistency_level=None,
        **kwargs,
    ):
        """
        Prepare environment for test
        """
        if jvm_args is None:
            jvm_args = []
        if options is None:
            options = {}
        cluster = self.cluster
        cluster_topology = nodes if isinstance(nodes, dict) else generate_cluster_topology_based_rf(nodes=nodes, rf=rf, dc_name_prefix="dc", rack_name_prefix="rack")
        logger.info("Populate cluster with %s", cluster_topology)

        cluster.populate(cluster_topology, use_vnodes=use_vnodes)
        if options:
            cluster.set_configuration_options(values=options)
        if not jvm_args:
            jvm_args = ["--smp", "2", "--memory", "1G"]
        cluster.start(jvm_args=jvm_args)
        if "--smp" in jvm_args:
            logger.debug("The cluster has been started with SMP {}".format(jvm_args[jvm_args.index("--smp") + 1]))
        nodes_in_rack1 = [node for node in self.cluster.nodelist() if node.rack == f"rack{session_rack}"]
        node1 = nodes_in_rack1[session_node - 1]
        if consistency_level is None:
            consistency_level = ConsistencyLevel.QUORUM if nodes > 1 else ConsistencyLevel.ONE

        session = self.patient_cql_connection(node1, consistency_level=consistency_level, **kwargs)

        if fetch_size:
            session.default_fetch_size = fetch_size
        create_ks(session, keyspace_name, rf)

        if user_table:
            columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
            create_cf(session, "users", columns=columns, compaction={"class": self.compaction_strategy})

        self.fixture_dtest_setup.ignore_log_patterns += [
            r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: exceptions::mutation_write_failure_exception",
        ]

        return session

    def insert_row_with_long_value(  # noqa: PLR0913
        self,
        create_table_cql,
        create_index_cql,
        insert_cql,
        session,
        column_name,
        value_length,
        expect_message,
    ):
        """Validate two variations of the supplied insert statement, first
        as it is and then again transformed into a conditional statement
        """
        table_name = "table_" + str(round(time.time() * 1000, None))
        session.execute(create_table_cql % (table_name, {"class": self.compaction_strategy}))
        session.execute(create_index_cql % table_name)
        value = "X" * value_length
        self.assert_request(session, insert_cql % table_name, value, table_name, column_name, value_length, expect_message)

    def assert_request(self, session, insert_cql, value, table_name, column_name, value_length, expect_message):  # noqa: PLR0913
        """Perform two executions of the supplied statement, as a
        single statement and again as part of a batch
        """
        prepared = session.prepare(insert_cql)
        self.execute_and_assert(lambda: session.execute(prepared, [value]), insert_cql, table_name, column_name, session, value_length, expect_message)
        batch = BatchStatement()
        batch.add(prepared, [value])
        self.execute_and_assert(lambda: session.execute(batch), insert_cql, table_name, column_name, session, value_length, expect_message)

    def execute_and_assert(self, operation, cql_string, table_name, column_name, session, value_length, expect_message):  # noqa: PLR0912, PLR0913
        try:
            operation()
            res_length = 0
            if self.INDEX_TYPE == "local":
                stmt = "select {0} from {1} where a=0 and {0}='{2}'".format(column_name, table_name, "X" * value_length)
            elif self.INDEX_TYPE == "global":
                stmt = f"select {column_name} from {table_name}"
            else:
                raise ValueError("Unknown index type: %s", self.INDEX_TYPE)

            result = list(session.execute(stmt))
            if result:
                res_length = len(next(iter(result[0])))
            if value_length == OVERSIZE_LENGTH:
                assert_success = False
                assert_fail = "Expecting query %s to be invalid" % cql_string
            else:
                assert_success = value_length == res_length
                assert_fail = f"Expecting value length is {value_length}, received {res_length}. Result: {result}"
            assert assert_success, assert_fail
        except AssertionError as e:
            raise e
        except WriteFailure:
            if not expect_message:
                raise

            error_found = False
            for node in self.cluster.nodelist():
                if node.grep_log(expr=expect_message):
                    error_found = True
                    break

            assert error_found, f'Expected that failure reason is "{expect_message}", but the message wasn\'t found in the log'

        except Exception as e:
            if (expect_message and not re.search(expect_message, str(e))) or not expect_message:
                raise e

    def drain_and_restart_node(self, node, keyspace_name):
        node.nodetool("drain")
        node.stop()
        self.cluster.start()
        return self.patient_cql_connection(node, keyspace=keyspace_name)

    def node_action_with_delay(self, action, node=None, delay=0, wait=True, wait_other_notice=False, gently=True):  # noqa: PLR0913
        """
        :param action: expected values: stop, remove
        :param action: str
        """
        if action not in ["stop", "remove", "decommission", "add"]:
            assert False, "Unsupported node action"

        if delay:
            logger.debug(f"Sleep for {delay} seconds")
            time.sleep(delay)

        logger.debug(f"START: {action} node {node.name}")
        if action == "stop":
            node.stop(wait=wait, wait_other_notice=wait_other_notice, gently=gently)
        elif action == "remove":
            remove_node(cluster=self.cluster, node=node)
        elif action == "add":
            self.add_new_node()
        else:
            node.nodetool(action)
            if action == "decommission":
                self.ignore_log_patterns += [r"raft_topology - raft_topology_cmd stream_ranges failed with: (?:seastar::abort_requested_exception[ :]+\(?abort requested\)?|abort requested)"]
                node.stop(wait=wait, wait_other_notice=wait_other_notice, gently=gently)
        logger.debug(f"FINISH: {action} node {node.name}")

    def add_new_node(  # noqa: PLR0913
        self,
        data_center="dc1",
        rack="rack1",
        wait_for_binary_proto=True,
        jvm_args=None,
        configuration_options=None,
        queue=None,
        delay=0,
        node_index=None,
    ):
        time.sleep(delay)
        i = len(self.cluster.nodes) + 1 if not node_index else node_index
        node = self.cluster.new_node(i=i, data_center=data_center, rack=rack)
        if configuration_options:
            node.set_configuration_options(values=configuration_options)  # CASSANDRA-11670
        logger.debug("Start join at {}".format(time.strftime("%H:%M:%S")))
        node.start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        session = self.patient_exclusive_cql_connection(node)
        logger.debug("Finish join at {}".format(time.strftime("%H:%M:%S")))
        if queue:
            queue.put_nowait(session)
        return session

    def validate_index_data(self, session, cl, num_rows, table_name, index_column, num_attempts=60):  # noqa: PLR0913
        if self.INDEX_TYPE == "local":
            stmt = "SELECT key FROM {table_name} WHERE key = {key_value} AND {index_column} = {index_value}"
        elif self.INDEX_TYPE == "global":
            stmt = "SELECT key FROM {table_name} WHERE {index_column} = {index_value}"
        else:
            raise ValueError("Unknown index type: %s", self.INDEX_TYPE)

        logger.debug(f"Verify data with {ConsistencyLevel.value_to_name[cl]} consistency level")

        @retrying(num_attempts=num_attempts, sleep_time=1, message="Validating index data")
        def read_all_index():
            for i in range(num_rows):
                query = stmt.format(table_name=table_name, index_column=index_column, key_value=i, index_value=i + num_rows)
                logger.debug("CQL: %s", query)
                assert_all(session, query, expected=[[i]], cl=cl)

        read_all_index()


@pytest.mark.dtest_full
class TestStaticSecondaryIndexes(SecondaryIndexesHelpers):
    INDEX_TYPE = "global"

    @unmark.next_gating  # this is failing as xfailed
    def test_query_data_with_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        Test also various queries with index
        """
        keyspace_name = "ks"
        table_name = "statics"
        session = self.prepare(user_table=False, nodes=4, rf=3, keyspace_name=keyspace_name)
        create_cf(session, f"{keyspace_name}.{table_name}", key_type="text", columns={"ck": "text", "cs1": "text static"}, primary_key="key, ck", compaction={"class": self.compaction_strategy})
        # insert data
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc1', 'val1', 'stat1');")
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc2', 'val2', 'stat2');")

        # create index on static column
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name=table_name, index_column="cs1", index_name="cs1_key", compaction=self.compaction_strategy), "Index {} is not built".format("cs1_key")

        # insert data
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc3', 'val3', 'stat3');")
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc4', 'val4', 'stat3');")
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc5', 'val5', 'stat2');")

        assert_all(session, "select count(*) from statics", expected=[[5]], cl=ConsistencyLevel.QUORUM)

        # verify can query by index on static column
        assert_all(session, "select count(*) from statics where cs1='stat3'", expected=[[2]], cl=ConsistencyLevel.QUORUM, num_attempts=10)

        # verify data created before creating index can be queried
        assert_all(session, "select count(*) from statics where cs1='stat2'", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from statics where cs1='stat1'", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # verify can query by pk and static column index
        assert_all(session, "select count(*) from statics where key='abc1' and cs1='stat1'", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # verify can query by ck and static column index requires ALLOW FILTERING
        assert_invalid(session, "select count(*) from statics where ck='val1' and cs1='stat1'", matching="use ALLOW FILTERING")
        assert_all(session, "select count(*) from statics where ck='val1' and cs1='stat1' ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # verify SELECT by indexed key with WHERE like "key = X AND key = Y" returns no results
        assert_all(session, "select count(*) from statics where cs1='stat2' and cs1='stat1' and key='abc2'", expected=[[0]], cl=ConsistencyLevel.QUORUM)

        # delete of a static column modifies the index
        session.execute("DELETE FROM statics WHERE KEY='abc3';")
        assert_all(session, "select count(*) from statics where cs1='stat3'", expected=[[1]], cl=ConsistencyLevel.QUORUM, num_attempts=10)

        # verify ttl on static column expires also its index (by update and insert)
        session.execute("UPDATE statics USING TTL 60 SET cs1='I will expire' WHERE key='abc5';")
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc6', 'val6', 'stat4') USING TTL 60;")

        assert_all(session, "select count(*) from statics where cs1='I will expire'", expected=[[1]], cl=ConsistencyLevel.QUORUM, num_attempts=10)
        assert_all(session, "select count(*) from statics where cs1='stat4'", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        time.sleep(62)
        assert_all(session, "select count(*) from statics where cs1='stat4'", expected=[[0]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from statics where cs1='I will expire'", expected=[[0]], cl=ConsistencyLevel.QUORUM)
        # make sure index data is also deleted
        assert_all(session, "select count(*) from statics where cs1='stat4'", expected=[[0]], cl=ConsistencyLevel.QUORUM)

        # verify index is truncated when base table is truncated
        self.cluster.flush()
        session.execute("TRUNCATE table statics")
        assert_all(session, "select count(*) from statics", expected=[[0]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from statics where cs1='stat3'", num_attempts=10, expected=[[0]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from statics where cs1='stat2'", expected=[[0]], cl=ConsistencyLevel.QUORUM)

        # verify count(*) when using static column index counts properly when there's multiple rows in partition
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc1', 'val1', 'stat1');")
        session.execute("INSERT INTO statics (KEY, ck, cs1) VALUES ('abc2', 'val1', 'stat1');")
        session.execute("INSERT INTO statics (KEY, ck) VALUES ('abc1', 'val2');")

        assert_all(session, "select count(*) from statics where cs1='stat1'", expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=10)

        # verify can query by pk, ck and static column index
        pytest.xfail(reason="#12829")
        assert_all(session, "select count(*) from statics where key='abc1' and ck='val1' and cs1='stat1'", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()


@unmark.next_gating
@pytest.mark.dtest_heavy
@pytest.mark.dtest_full
class TestSecondaryIndexes(SecondaryIndexesHelpers):
    INDEX_TYPE = "global"

    @staticmethod
    def _index_sstables_files(node, keyspace, table, index):
        files = []
        for data_dir in node.data_directories():
            keyspace_dir = os.path.join(data_dir, keyspace)
            base_tbl_dir = os.path.join(keyspace_dir, next(s for s in os.listdir(keyspace_dir) if s.startswith(table)))
            index_sstables_dir = os.path.join(base_tbl_dir, "." + index)
            files.extend(os.listdir(index_sstables_dir))
        return set(files)

    def config_keyspace(self, session, ks_name, table_name, index, ks_create=True):
        if ks_create:
            create_ks(session, ks_name, 1)
        create_cf(session, f"{ks_name}.{table_name}", key_type="text", columns={"col1": "text"}, compaction={"class": self.compaction_strategy})

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name, table_name, index["index_column"], index["index_name"], compaction=self.compaction_strategy), "Index %s is not built" % index["index_name"]

    def test_agg_query_by_pk(self):
        """
        Filter data by first primary key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute("CREATE TABLE ks.t3(pk1 int, pk2 int, ck int, PRIMARY KEY((pk1, pk2), ck))")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 1)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk2) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(ck) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t3", index_column="pk1", index_name="ks_t3", compaction=self.compaction_strategy), "Index ks_t3 is not built"

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1", expected=[[3]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk2) FROM ks.t3 WHERE pk1 = 1", expected=[[3]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck) FROM ks.t3 WHERE pk1 = 1", expected=[[3]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_second_pk(self):
        """
        Filter data by second primary key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute("CREATE TABLE ks.t3(pk1 int, pk2 int, ck int, PRIMARY KEY((pk1, pk2), ck))")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 1)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 4)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(ck) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t3", index_column="pk2", index_name="ks_t3", compaction=self.compaction_strategy), "Index ks_t3 is not built"

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1", expected=[[2]], cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE pk2 = 1", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck) FROM ks.t3 WHERE pk2 = 1", expected=[[4]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_ck(self):
        """
        Filter data by clustering key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute("CREATE TABLE ks.t3(pk1 int, ck int, v int, PRIMARY KEY(pk1, ck))")
        session.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 1, 1)")
        session.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 4, 1)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(v) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t3", index_column="ck", index_name="ks_t3", compaction=self.compaction_strategy), "Index ks_t3 is not built"

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 2, 2)")
        session.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 3, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck = 1", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(v) FROM ks.t3 WHERE ck = 3", expected=[[3]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_second_ck(self):
        """
        Filter data by second clustering key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute("CREATE TABLE ks.t3(pk1 int, ck1 int, ck2 int, PRIMARY KEY(pk1, ck1, ck2))")
        session.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 1)")
        session.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(ck1) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t3", index_column="ck2", index_name="ks_t3", compaction=self.compaction_strategy), "Index ks_t3 is not built"

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 2, 1)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck2 = 1", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck1) FROM ks.t3 WHERE ck2 = 1", expected=[[2]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_two_ck(self):
        """
        Filter data by two secondary keys columns that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)
        session.default_fetch_size = 1

        session.execute("CREATE TABLE ks.t3(pk1 int, pk2 int, ck1 int, ck2 int, PRIMARY KEY((pk1, pk2), ck1, ck2))")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 1, 1, 1)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 2, 1, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT AVG(pk2) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck2) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", expected=[[3]], cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t3", index_column="ck1", index_name="ck1_index", compaction=self.compaction_strategy), 'Index on "ck1" column is not built'
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t3", index_column="ck2", index_name="ks_t3", compaction=self.compaction_strategy), 'Index "ck2" column is not built'

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING", expected=[[2]], cl=ConsistencyLevel.QUORUM)

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 >= 1 ALLOW FILTERING", expected=[[3]], cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (2, 1, 2, 1)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (2, 2, 1, 4)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 2 and ck2 in (1, 3) ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk2) FROM ks.t3 WHERE ck1 = 1 and ck2 > 1 ALLOW FILTERING", expected=[[5]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT AVG(pk1) FROM ks.t3 WHERE ck1 = 2 and ck2 >= 1 ALLOW FILTERING", expected=[[2]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_group_by_pk_filter_by_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        """
        session = self.prepare(user_table=False, nodes=4, rf=3)

        session.execute("CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck))")
        session.execute("INSERT INTO ks.t(pk, ck, v) VALUES (1, 2, 3)")
        session.execute("INSERT INTO ks.t(pk, ck, v) VALUES (1, 4, 3)")
        session.execute("INSERT INTO ks.t(pk, ck, v) VALUES (2, 4, 3)")
        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk ALLOW FILTERING", expected=[[1], [2]], cl=ConsistencyLevel.QUORUM)

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t", index_column="v", index_name="v_key", compaction=self.compaction_strategy), "Index state_key is not built"

        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk", expected=[[1], [2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT pk, count(pk) FROM ks.t WHERE v=3 GROUP BY pk", expected=[[1, 2], [2, 1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    @pytest.mark.require("#7432")
    def test_filter_by_index_with_paging(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        Read with pagination
        """
        session = self.prepare(user_table=False, nodes=4, rf=3)
        session.default_fetch_size = 1

        session.execute("CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck))")
        session.execute("INSERT INTO ks.t(pk, ck, v) VALUES (1, 2, 3)")
        session.execute("INSERT INTO ks.t(pk, ck, v) VALUES (1, 4, 3)")
        session.execute("INSERT INTO ks.t(pk, ck, v) VALUES (2, 4, 3)")
        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk ALLOW FILTERING", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="t", index_column="v", index_name="v_key", compaction=self.compaction_strategy), "Index state_key is not built"

        session.default_fetch_size = 3

        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk", expected=[[1], [2]], cl=ConsistencyLevel.QUORUM)

        session.default_fetch_size = 1
        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_query_data_created_before_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="users", index_column="gender", index_name="gender_key", compaction=self.compaction_strategy), "Index {} is not built".format(
            "gender_key"
        )

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="users", index_column="state", index_name="state_key", compaction=self.compaction_strategy), "Index {} is not built".format(
            "state_key"
        )

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="users", index_column="birth_year", index_name="birth_year_key", compaction=self.compaction_strategy), "Index {} is not built".format(
            "birth_year_key"
        )

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        assert_all(session, "select count(*) from users", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where state='TX'", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where state='CA'", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_query_data_by_pk_and_index(self):
        """
        Filter data by primary key and secondary index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name="ks", table_name="users", index_column="gender", index_name="gender_key", compaction=self.compaction_strategy), "Index {} is not built".format(
            "gender_key"
        )

        assert_all(session, "select count(*) from users", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where gender='f'", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select KEY, password, gender, state, birth_year from users where KEY='user2' and gender='m'", expected=[["user2", "ch@ngem3b", "m", "CA", 1971]], cl=ConsistencyLevel.ALL)
        assert_all(session, "select count(*) from users where KEY='user1' and gender='m'", expected=[[0]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_query_data_by_ck_and_index(self):
        """
        Filter data by primary and clustering keys and secondary index
        """
        ks_name = "ks"
        table_name = "cf"
        index_column = "v"
        index_name = "v_inx"

        session = self.prepare(nodes=4, rf=3)
        create_cf(session, f"{ks_name}.{table_name}", key_type="text", compaction={"class": self.compaction_strategy})

        # insert data
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user1', 'ch@ngem3a', 'f')")
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user2', 'ch@ngem3b', 'm')")
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user3', 'ch@ngem3c', 'f')")
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user4', 'ch@ngem3d', 'm')")

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name=ks_name, table_name=table_name, index_column=index_column, index_name=index_name, compaction=self.compaction_strategy), (
            "Index %s is not built" % index_name
        )

        assert_all(session, f"select count(*) from {table_name}", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, f"select count(*) from {table_name} where v='f'", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, f"select count(*) from {table_name} where key='user2' and c='ch@ngem3b' and v='m'", expected=[[1]], cl=ConsistencyLevel.ALL)

        assert_all(session, f"select count(*) from {table_name} where KEY='user1' and c='ch@ngem3a' and v='m'", expected=[[0]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_low_cardinality_indexes(self):
        """
        Checks that low-cardinality secondary index subqueries are executed concurrently
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = "ks"
        table_name = "cf"
        index = {"index_name": "col1_index", "index_column": "col1"}

        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows // 3)
            # use the same indexed value three times
            session.execute(f"INSERT INTO {ks_name}.{table_name} (key, col1) VALUES ('{i}', '{indexed_value}');")

        assert_all(session, "SELECT count(*) FROM {}.{} WHERE {}='1'".format(ks_name, table_name, index["index_column"]), expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=20)
        assert_all(session, "SELECT count(*) FROM {}.{} WHERE {}='1' LIMIT 100".format(ks_name, table_name, index["index_column"]), expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=20)
        assert_all(session, "SELECT count(*) FROM {}.{} WHERE {}='1' LIMIT 3".format(ks_name, table_name, index["index_column"]), expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=20)

        for limit in (1, 2):
            assert_row_count_in_select(
                session, query="select * from {}.{} WHERE {}='1' LIMIT {}".format(ks_name, table_name, index["index_column"], limit), num_rows_expected=limit, consistency_level=ConsistencyLevel.QUORUM, num_attempts=20
            )

    def test_insert_data_after_recreating_ks(self):
        """
        Data inserted immediately after dropping and recreating a keyspace with an indexed column family is not included
        in the index.
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = "ks"
        table_name = "cf"
        index = {"index_name": "col1_key", "index_column": "col1"}
        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        for i in range(10):
            logger.debug(f"round {i}")
            try:
                session.execute(f"DROP KEYSPACE {ks_name}")
            except ConfigurationException:
                pass

            self.config_keyspace(session, ks_name, table_name, index)

            for r in range(10):
                session.execute(f"INSERT INTO {ks_name}.{table_name} (key, col1) VALUES ('{r}','asdf');")

            wait_for_schema_agreement(session)
            assert_all(session, f"select count(*) from {ks_name}.{table_name} WHERE col1='asdf'", expected=[[10]], cl=ConsistencyLevel.QUORUM, num_attempts=60, sleep_time=1)

    def test_insert_data_after_recreating_cf(self):
        """
        Data inserted immediately after dropping and recreating an indexed column family is not included in the index.
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = "ks"
        table_name = "cf"
        index = {"index_name": "col1_key", "index_column": "col1"}
        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)
        for r in range(10):
            session.execute(f"INSERT INTO {ks_name}.{table_name} (key, col1) VALUES ('{r}','asdf');")

        for i in range(10):
            logger.debug("round %s" % i)
            drop_stmt = f"DROP COLUMNFAMILY {ks_name}.{table_name}"

            logger.debug(drop_stmt)
            try:
                retry_till_success(session.execute, query=drop_stmt, timeout=self.session_timeout, bypassed_exception=OperationTimedOut)
            except InvalidRequest:
                pass
            wait_for_schema_agreement(session)

            self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

            for r in range(10):
                session.execute("INSERT INTO {}.{} (key, {}) VALUES ('{}','asdf');".format(ks_name, table_name, index["index_column"], r))

            wait_for_schema_agreement(session)
            assert_all(session, "select count(*) from {}.{} WHERE {}='asdf'".format(ks_name, table_name, index["index_column"]), expected=[[10]], cl=ConsistencyLevel.QUORUM, num_attempts=60, sleep_time=1)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    @pytest.mark.skip_if(issue_open("#8627"))
    def test_oversize_indexed_values(self):
        """
        Reject inserts & updates where values of any indexed column is > 64k
        """
        # Before fixing issue #10366, we had a internal server error with
        # the text "Key size too large". After fixing it, it became an
        # InvalidRequest error, with the text "is longer than maximum".
        expect_message = "Key size too large|is longer than maximum"
        self._validate_long_indexed_values(OVERSIZE_LENGTH, expect_message)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_long_indexed_values(self):
        """
        Correct inserts & updates where values of any indexed column is long and up to 64k
        """
        self._validate_long_indexed_values(LONG_TEXT_LENGTH, expect_message=None)

    def _validate_long_indexed_values(self, value_length, expect_message):
        session = self.prepare(nodes=4, rf=3)
        test = "oversize" if value_length == OVERSIZE_LENGTH else "long"

        if expect_message:
            self.ignore_log_patterns += [expect_message]

        logger.debug(f"Insert {test} value into non-PK column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b int, c varchar, PRIMARY KEY (a)) WITH compaction = %s",
            "CREATE INDEX ON %s(c)",
            "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
            session,
            column_name="c",
            value_length=value_length,
            expect_message=expect_message,
        )

        logger.debug(f"Insert {test} value into clustering key column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b)) WITH compaction = %s",
            "CREATE INDEX ON %s(b)",
            "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)",
            session,
            column_name="b",
            value_length=value_length,
            expect_message=expect_message,
        )

        logger.debug(f"Insert {test} value into partition key column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a text, b int, c int, PRIMARY KEY ((a, b))) WITH compaction = %s",
            "CREATE INDEX ON %s(a)",
            "INSERT INTO %s (a, b, c) VALUES (?, 0, 0)",
            session,
            column_name="a",
            value_length=value_length,
            expect_message=expect_message,
        )

        logger.debug(f"Table with compact storage. Insert {test} value into non-PK column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE and compaction = %s",
            "CREATE INDEX ON %s(b)",
            "INSERT INTO %s (a, b) VALUES (0, ?)",
            session,
            column_name="b",
            value_length=value_length,
            expect_message=expect_message,
        )

        logger.debug(f"Insert {test} value into indexed static key column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b int, c text static, PRIMARY KEY (a, b)) WITH compaction = %s",
            "CREATE INDEX ON %s(c)",
            "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
            session,
            column_name="c",
            value_length=value_length,
            expect_message=expect_message,
        )

    def test_multi_index_filtering_query(self):
        """
        asserts that having multiple indexes that cover all predicates still requires ALLOW FILTERING to also be present
        """
        keyspace_name = "ks"
        table_name = "tbl"
        index_names = {"ix_tbl_c0": "c0", "ix_tbl_c1": "c1"}

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="uuid", columns={"c0": "text", "c1": "text", "c2": "text"}, compaction={"class": self.compaction_strategy})

        for name, column in index_names.items():
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name, column, name, compaction=self.compaction_strategy), f"Index {name} is not built"

        smt = "INSERT INTO {0} (key, c0, c1, c2) values (uuid(), '{1}', '{2}', '{3}')"
        session.execute(smt.format(table_name, "a", "b", "c"))
        session.execute(smt.format(table_name, "a", "b", "c"))
        session.execute(smt.format(table_name, "q", "b", "c"))
        session.execute(smt.format(table_name, "a", "e", "f"))
        session.execute(smt.format(table_name, "a", "e", "f"))

        @retrying(num_attempts=5, sleep_time=1, message="Running try_assert_all")
        def try_assert_all(session, stmt, expected, cl):
            assert_all(session, stmt, expected, cl)

        try_assert_all(session, "SELECT count(*) FROM {} WHERE {} = 'a';".format(table_name, index_names["ix_tbl_c0"]), expected=[[4]], cl=ConsistencyLevel.QUORUM)

        # Filter query by multi index without using ALLOW FILTERING option expected fail
        smt = "SELECT count(*) FROM {} WHERE {} = 'a' AND {} = 'b'".format(table_name, index_names["ix_tbl_c0"], index_names["ix_tbl_c1"])
        assert_invalid(session, smt, matching="use ALLOW FILTERING")

        try_assert_all(session, f"{smt} ALLOW FILTERING", expected=[[2]], cl=ConsistencyLevel.QUORUM)

    def test_index_same_key_twice(self):
        """SELECT by indexed key with WHERE like "key = X AND key = Y".

        See scylladb/scylladb#7772 for details.
        """
        keyspace_name = "ks"
        table_name = "tbl"

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="uuid", columns={"c0": "text", "c1": "text", "c2": "text"}, compaction={"class": self.compaction_strategy})

        assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name, "c0", "ix_tbl_c0", compaction=self.compaction_strategy), "Index ix_tbl_c0 is not built"

        smt = "INSERT INTO {0} (key, c0, c1, c2) values (uuid(), '{1}', '{2}', '{3}')"
        session.execute(smt.format(table_name, "a", "b", "c"))
        session.execute(smt.format(table_name, "a", "b", "c"))
        session.execute(smt.format(table_name, "q", "b", "c"))
        session.execute(smt.format(table_name, "a", "e", "f"))
        session.execute(smt.format(table_name, "a", "e", "f"))

        smt = "SELECT count(*) FROM {0} WHERE {1} = 'a' AND {1} = 'b'".format(table_name, "c0")
        assert_all(session, smt, expected=[[0]], cl=ConsistencyLevel.QUORUM)

    def test_truncate_base(self):
        """
        asserts that truncating base table will result in truncating secondary index as well
        """

        def create_data():
            smt = "INSERT INTO {0} (key, c0, c1) values (uuid(), '{1}', '{2}')"
            session.execute(smt.format(table_name, "a", "b"))
            session.execute(smt.format(table_name, "a", "b"))
            session.execute(smt.format(table_name, "q", "b"))
            session.execute(smt.format(table_name, "a", "e"))
            session.execute(smt.format(table_name, "a", "e"))

        def validate_truncated_entries_for_table_and_views():
            for node in self.cluster.nodelist():
                node_session = self.patient_exclusive_cql_connection(node=node)
                for name in [table_name] + [index_name + "_index" for index_name in index_names.keys()]:
                    table_or_view = "table" if name == table_name else "view"
                    _id = get_entity_id(session=node_session, table_or_view=table_or_view, keyspace_name=keyspace_name, entity_name=name)

                    # validate truncation entries in the system.truncated table - expected entry
                    truncated_time = get_truncated_time_from_system_truncated(session=node_session, table_id=_id)

                    assert truncated_time, "Expected truncated entry in the system.truncated table, but it's not found"

                    # validate truncation entries in the system.local table - not expected entry
                    truncated_time = get_truncated_time_from_system_local(session=node_session)
                    assert truncated_time == [[None]], "Not expected truncated entry in the system.local table, but it's found"

        keyspace_name = "ks"
        table_name = "tbl"
        index_names = {"ix_tbl_c0": "c0", "ix_tbl_c1": "c1"}

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="uuid", columns={"c0": "text", "c1": "text", "c2": "text"}, compaction={"class": self.compaction_strategy})

        for name, column in index_names.items():
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name, column, name, compaction=self.compaction_strategy), "Index %s is not built" % name

        create_data()

        # ensure sstables are created and will be dropped
        self.cluster.flush()

        smt = "SELECT count(*) FROM {0} WHERE {1} = '{2}'"

        # ensure data is loaded into cache and the cache will be cleared
        assert_all(session, smt.format(table_name, index_names["ix_tbl_c0"], "a"), expected=[[4]], cl=ConsistencyLevel.QUORUM)

        assert_row_count(session, "tbl", 5)

        session.execute("TRUNCATE table tbl")
        assert_row_count(session, "tbl", 0)

        # check that index queries are also truncated
        assert_all(session, smt.format(table_name, index_names["ix_tbl_c0"], "a"), expected=[[0]], cl=ConsistencyLevel.QUORUM)

        assert_all(session, smt.format(table_name, index_names["ix_tbl_c1"], "b"), expected=[[0]], cl=ConsistencyLevel.QUORUM)

        validate_truncated_entries_for_table_and_views()

        logger.debug("Insert data after truncate")
        create_data()
        validate_truncated_entries_for_table_and_views()

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_query_indexes_with_vnodes(self):
        """
        Verifies correct query behaviour in the presence of vnodes
        @jira_ticket CASSANDRA-11104
        """
        keyspace_name = "ks"
        # True/False: create table with/without compact storage
        tables = {"compact_table": True, "regular_table": False}
        index_column = "b"

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name, use_vnodes=True)

        for table_name, compact_storage in tables.items():
            create_cf(session, table_name, key_type="int", columns={"b": "int"}, compact_storage=compact_storage, compaction={"class": self.compaction_strategy})
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name, index_column, get_index_view_name(table_name), compaction=self.compaction_strategy), (
                "Index %s is not built" % get_index_view_name(table_name)
            )

        insert_args = [(i, i % 2) for i in range(100)]
        for table in tables:
            logger.debug(f"Perform the test for {table} table")
            execute_concurrent_with_args(session, session.prepare(f"INSERT INTO {keyspace_name}.{table} (key, {index_column}) VALUES (?, ?)"), insert_args)
            res = session.execute(f"SELECT * FROM {keyspace_name}.{table} WHERE {index_column} = 0")
            assert len(rows_to_list(res)) == 50, f"Expected: {50}, got {len(rows_to_list(res))}"

    @pytest.mark.single_node
    def test_multi_column_index(self):
        """
        Test that impossible to create secondary index on the few columns and valid error message is received
        """
        keyspace_name = "ks"
        table_name = "cf"
        index_columns = {"b": "int", "c": "int"}

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        # try to create index on 2 columns
        create_cf(session, table_name, key_type="int", columns=index_columns, compaction={"class": self.compaction_strategy})
        assert_expected_error(func=create_index, expected_error="Only CUSTOM indexes support multiple columns", args=(session, table_name, index_columns), kwargs={"index_name": "two_columns_index", "compaction": self.compaction_strategy})

        # try to create index on 6 columns
        table_name = "cf_6columns"
        index_columns = {"b": "int", "c": "int", "d": "int", "e": "int", "f": "int", "g": "int"}
        create_cf(session, table_name, key_type="int", columns=index_columns, compaction={"class": self.compaction_strategy})
        assert_expected_error(func=create_index, expected_error="Only CUSTOM indexes support multiple columns", args=(session, table_name, index_columns), kwargs={"index_name": "six_columns_index", "compaction": self.compaction_strategy})

    def _prepare_for_ttl(self):
        keyspace_name = "ks"
        table_name = "cf"
        index_column = "b"
        index_name = f"{index_column}_inx"
        select_query = "select * from {} where {} = {}"
        mv_query = f"select key, {index_column} from {get_index_view_name(index_name)}"

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="int", columns={"b": "int", "c": "int"}, compaction={"class": self.compaction_strategy})
        session.execute(f"INSERT INTO {table_name} (key, b, c) VALUES (0, 1, 2)")
        assert_all(session, select_query.format(table_name, "key", 0), [[0, 1, 2]], cl=ConsistencyLevel.ALL)

        assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name, index_column, index_name, compaction=self.compaction_strategy), "Index %s is not built" % index_name
        assert_all(session, select_query.format(table_name, index_column, 1), [[0, 1, 2]], cl=ConsistencyLevel.ALL)
        return session, keyspace_name, table_name, index_column, index_name, select_query, mv_query

    @pytest.mark.single_node
    def test_ttl_index_column(self):
        """
        Verify SI with default_time_to_live can be deleted properly using expired livenessInfo
        """
        session, _keyspace_name, table_name, index_column, _index_name, select_query, mv_query = self._prepare_for_ttl()

        ttl = 60
        logger.debug(f"Update index column with TTL {ttl}")
        session.execute(f"UPDATE {table_name} USING TTL {ttl} SET {index_column}=3 WHERE key=0")
        assert_all(session, select_query.format(table_name, "key", 0), [[0, 3, 2]], cl=ConsistencyLevel.ALL)
        assert_all(session, select_query.format(table_name, index_column, 3), [[0, 3, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, index_column, 1), cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 3]], cl=ConsistencyLevel.ALL)

        time.sleep(ttl + 5)
        # Validate that no record is returned when filtered by index
        assert_all(session, select_query.format(table_name, "key", 0), [[0, None, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, index_column, 3), cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, index_column, 1), cl=ConsistencyLevel.ALL)
        assert_none(session, mv_query, cl=ConsistencyLevel.ALL)

    @pytest.mark.single_node
    def test_ttl_non_index_column(self):
        """
        Verify SI is not impact from TTL on non-imdex column
        """
        session, _keyspace_name, table_name, index_column, _index_name, select_query, mv_query = self._prepare_for_ttl()

        ttl = 60
        logger.debug(f"Update non-index column with TTL {ttl}")
        session.execute("UPDATE {} USING TTL {} SET {}=3 WHERE key=0".format(table_name, ttl, "c"))
        assert_all(session, select_query.format(table_name, "key", 0), [[0, 1, 3]], cl=ConsistencyLevel.ALL)
        assert_all(session, select_query.format(table_name, index_column, 1), [[0, 1, 3]], cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 1]], cl=ConsistencyLevel.ALL)

        time.sleep(ttl + 5)
        # Validate that record is returned when filtered by index
        assert_all(session, select_query.format(table_name, "key", 0), [[0, 1, None]], cl=ConsistencyLevel.ALL)
        assert_all(session, select_query.format(table_name, index_column, 1), [[0, 1, None]], cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 1]], cl=ConsistencyLevel.ALL)

    def test_delete_indexed_rows(self):
        """
        Delete rows from indexed table and read data by index
        """
        keyspace_name = "ks"
        table_name = "cf"
        index_name = "b_index"
        index_column = "b"

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name, session_rack=2)

        create_cf(session, table_name, key_type="int", columns={"b": "int"}, compaction={"class": self.compaction_strategy})
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name=keyspace_name, table_name=table_name, index_column=index_column, index_name=index_name, compaction=self.compaction_strategy), (
            "Index %s is not built" % index_name
        )

        num_rows = 100
        for i in range(num_rows):
            indexed_value = i + 100
            session.execute(f"INSERT INTO {keyspace_name}.{table_name} (key, b) VALUES ({i}, {indexed_value})")

        # Delete 10 rows by index
        logger.debug("Delete 10 rows by index")
        start_key, delete_num = 30, 10
        rows_for_delete = list(range(start_key, start_key + delete_num))
        for i in rows_for_delete:
            session.execute(f"DELETE FROM {table_name} WHERE key = {i}")

        # Validate the data is not in table
        assert_row_count(session, table_name=table_name, expected=num_rows - delete_num, consistency_level=ConsistencyLevel.ALL, num_attempts=60, sleep_time=1)

        query = "select key, b from {} where {}={}"
        for i in list(range(num_rows)):
            if i in rows_for_delete:
                assert_none(session, query=query.format(table_name, "key", i), cl=ConsistencyLevel.ALL)
                assert_none(session, query=query.format(table_name, index_column, i + 100), cl=ConsistencyLevel.ALL)
            else:
                res = [[i, i + 100]]
                assert_all(session, query=query.format(table_name, "key", i), expected=res, cl=ConsistencyLevel.ALL)
                assert_all(session, query=query.format(table_name, index_column, i + 100), expected=res, cl=ConsistencyLevel.ALL)

        # Validate the data is not in table and SI materialized view
        assert_row_count(session, table_name=get_index_view_name(index_name), expected=num_rows - delete_num, consistency_level=ConsistencyLevel.ALL)

    def test_stop_node_after_index_build(self):
        """
        Stop one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="stop", nodes=4, rf=3, num_rows=1000)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#22394"))
    def test_remove_node_after_index_build(self):
        """
        Remove one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="remove", nodes=4, rf=3, num_rows=1000)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#22394"))
    def test_decommission_node_after_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="decommission", nodes=4, rf=3, num_rows=1000)

    def test_add_node_after_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="add", nodes=3, rf=3, num_rows=1000)

    def _node_action_after_index_build(self, node_action, nodes, rf, num_rows):
        keyspace_name = "ks"
        table_name = "cf"
        index_name = "b_index"
        index_column = "b"
        view_name = get_index_view_name(index_name)

        session = self.prepare(nodes=nodes, rf=rf, keyspace_name=keyspace_name, session_rack=2)
        nodes_in_rack1 = [node for node in self.cluster.nodelist() if node.rack == "rack1"]
        node2 = nodes_in_rack1[-1]
        node2_ip = next(iter(node2.network_interfaces["binary"]))

        create_cf(session, table_name, key_type="int", columns={"b": "int"}, compaction={"class": self.compaction_strategy})

        statement = session.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, b) VALUES (?, ?)")
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement, map(lambda k: [k, k + num_rows], [k for k in range(num_rows)]))

        # Create index and wait while the index is built
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name=keyspace_name, table_name=table_name, index_name=index_name, index_column=index_column, compaction=self.compaction_strategy), (
            "Index %s is not built" % index_name
        )

        exclude_errors = [
            f"Can't send migration request: node {node2_ip} is down",
            rf"(\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: exceptions::mutation_write_failure_exception (Operation "
            rf"failed for {keyspace_name}\.{index_name}_index - received 0 responses and 1 failures from "
            f"1 CL=ONE)",
        ]
        self.ignore_log_patterns += exclude_errors

        # Perform action on second node
        self.node_action_with_delay(node_action, node=node2)

        # Validate the data using filtering by index
        self.validate_index_data(session, cl=ConsistencyLevel.QUORUM, num_rows=num_rows, table_name=table_name, index_column=index_column)

        # Validate view rows
        assert_row_count_in_select(session=session, query=f"SELECT * FROM {view_name}", num_rows_expected=num_rows, consistency_level=ConsistencyLevel.QUORUM)

        self.check_errors(self.cluster.nodelist()[0], exclude_errors)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSecondaryIndexesOnCollections(SecondaryIndexesHelpers):
    INDEX_TYPE = "global"

    def test_tuple_indexes(self):
        """
        Checks that secondary indexes on tuples work for querying
        """
        keyspace_name = "tuple_index_test"
        table_name = "simple_with_tuple"
        index_columns = {"single_tuple": "({0})", "double_tuple": "({0},{0})", "triple_tuple": "({0},{0},{0})", "nested_one": "({0},({0},{0}))"}
        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(
            session,
            table_name,
            key_type="uuid",
            columns={"normal_col": "int", "single_tuple": "tuple<int>", "double_tuple": "tuple<int, int>", "triple_tuple": "tuple<int, int, int>", "nested_one": "tuple<int, tuple<int, int>>"},
            compaction={"class": self.compaction_strategy},
        )

        cmds = [
            (
                f"""insert into {table_name}
                        (key, normal_col, single_tuple, double_tuple, triple_tuple, nested_one)
                    values
                        (uuid(), {n}, ({n}), ({n},{n}), ({n},{n},{n}), ({n},({n},{n})))""",
                (),
            )
            for n in range(50)
        ]

        results = execute_concurrent(session, cmds * 5, raise_on_first_error=True, concurrency=200)

        for success, result in results:
            assert success, f"didn't get success on insert: {result}"

        # no index present yet, make sure there's an error trying to query column
        stmt = f"SELECT * from {table_name} where single_tuple = (1)"

        assert_invalid(session, stmt, matching="use ALLOW FILTERING", expected=Exception)

        for index_column in index_columns.keys():
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name, index_column, "idx_" + index_column, compaction=self.compaction_strategy), f"Index idx_{index_column} is not built"

        select_cmd = "select * from {} where {} = {}"
        # check if indexes work on existing data
        for n in range(50):
            for index_column, template in index_columns.items():
                assert 5 == len(list(session.execute(select_cmd.format(table_name, index_column, template.format(n)))))

                assert 0 == len(list(session.execute(select_cmd.format(table_name, index_column, template.format(-1)))))

        # check if indexes work on new data inserted after index creation
        results = execute_concurrent(session, cmds * 3, raise_on_first_error=True, concurrency=200)
        for success, result in results:
            assert success, f"didn't get success on insert: {result}"
        time.sleep(5)

        def _validate_data(expected_rows, format_value):
            for index_column, template in index_columns.items():
                assert expected_rows == len(list(session.execute(select_cmd.format(table_name, index_column, template.format(format_value)))))

        for n in range(50):
            _validate_data(expected_rows=8, format_value=n)

        # check if indexes work on mutated data
        for n in range(5):
            for index_column, template in index_columns.items():
                rows = session.execute(select_cmd.format(table_name, index_column, template.format(n)))
                for row in rows:
                    session.execute(f"update {table_name} set {index_column} = {template.format(-999)} where key = {row.key}")

        for n in range(5):
            _validate_data(expected_rows=0, format_value=n)

        for n in range(50):
            _validate_data(expected_rows=40, format_value=-999)

    def test_frozen_list_indexes(self):
        """
        Checks that secondary indexes can't be created on frozen list column
        """
        self.frozen_collection_indexes_run(_type="frozen list")

    def test_frozen_set_indexes(self):
        """
        Checks that secondary indexes can't be created on frozen set column
        """
        self.frozen_collection_indexes_run(_type="frozen set")

    def test_frozen_map_indexes(self):
        """
        Checks that secondary indexes can't be created on frozen map column
        """
        self.frozen_collection_indexes_run(_type="frozen map")

    def frozen_collection_indexes_run(self, _type):
        keyspace_name = "index_search"
        table_name = "users"
        index_name = "user_uuids"
        index_column = "uuids"
        index_column_type = {"frozen list": "frozen<list<uuid>>", "frozen map": "frozen<map<uuid, uuid>>", "frozen set": "frozen<set<uuid>>"}
        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="uuid", columns={"email": "text", "uuids": index_column_type[_type]}, compaction={"class": self.compaction_strategy})

        # try to create global index
        with pytest.raises(expected_exception=InvalidRequest) as err:
            create_index(session, table_name, index_column, index_name, compaction=self.compaction_strategy)

        assert str(err) == regexp_matches(r".*Cannot create index on (index_values|value) of frozen.*"), "Not expected error"

        # try to create local index
        with pytest.raises(expected_exception=InvalidRequest) as err:
            create_local_index(session, table_name, "key", index_column, index_name, compaction=self.compaction_strategy)

        assert str(err) == regexp_matches(r".*Cannot create index on (index_values|value) of frozen.*"), "Not expected error"


@unmark.next_gating
@pytest.mark.dtest_heavy
@pytest.mark.dtest_full
class TestLocalIndexes(SecondaryIndexesHelpers):
    INDEX_TYPE = "local"

    def config_keyspace(  # noqa: PLR0913
        self,
        session,
        ks_name,
        table_name,
        index,
        columns=None,
        ks_create=True,
        global_index_name=None,
    ):
        if ks_create:
            create_ks(session, ks_name, 1)
        session.execute(f"USE {ks_name}")
        create_cf(session, f"{ks_name}.{table_name}", key_type="text", columns=columns)
        assert self.create_and_build_index(
            create_index_func=create_local_index, cluster=self.cluster, session=session, ks_name=ks_name, table_name=table_name, index_column=index["index_column"], index_name=index["index_name"], pk_name=index["pk_name"]
        ), "Index %s is not built" % index["index_name"]

        if global_index_name:
            assert self.create_and_build_index(create_index_func=create_index, cluster=self.cluster, session=session, ks_name=ks_name, table_name=table_name, index_column=index["index_column"], index_name=global_index_name), (
                "Index %s is not built" % global_index_name
            )

    def test_simple_local_index(self):
        """
        - Create table with 3 columns
        - Create local index on "v" column and partition key "key"
        - Filter data by key and local index and validate result
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = "ks"
        table_name = "cf"
        index = {"index_name": "v_local_key", "index_column": "v", "pk_name": "key"}

        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        data = {
            "Tel Aviv": [{"c": generate_random_text(), "v": "Dizzengof"}, {"c": generate_random_text(), "v": "Arlozorov"}],
            "Washington": [{"c": generate_random_text(), "v": "Southgate"}, {"c": generate_random_text(), "v": "11th"}, {"c": generate_random_text(), "v": "10th"}],
            "London": [{"c": generate_random_text(), "v": "Geneva"}, {"c": generate_random_text(), "v": "Moorland"}],
            "Vancouver": [{"c": generate_random_text(), "v": "12th Ave"}, {"c": generate_random_text(), "v": "16th Ave"}],
        }

        for key, row_columns in data.items():
            for columns_data in row_columns:
                query = "INSERT INTO {table_name} (key, c, v) VALUES ('{key}', '{c}', '{v}')".format(table_name=table_name, key=key, c=columns_data["c"], v=columns_data["v"])
                session.execute(query)

        for key, indexes in data.items():
            for columns_data in indexes:
                ck = columns_data["c"]
                index_value = columns_data["v"]
                query = f"SELECT key, c, v FROM {table_name} WHERE key='{key}' AND v='{index_value}'"
                assert_all(session=session, query=query, expected=[[key, ck, index_value]], cl=ConsistencyLevel.QUORUM, num_attempts=30)

    def test_global_local_index_on_same_column(self):
        """
        - Create table with 3 columns
        - Create local index on "v" column and partition key "key"
        - Create global index on "v" column
        - Filter data by key and local index and validate result
        - Filter data by global index and validate result
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = "ks"
        table_name = "cf"
        index = {"index_name": "v_local_key", "index_column": "v", "pk_name": "key"}

        self.config_keyspace(session, ks_name, table_name, index, ks_create=False, global_index_name="v_global_key")

        local_data = {
            "Tel Aviv": [{"c": generate_random_text(), "v": "Dizzengof"}, {"c": generate_random_text(), "v": "Geneva"}],
            "Washington": [{"c": generate_random_text(), "v": "Southgate"}, {"c": generate_random_text(), "v": "Dizzengof"}, {"c": generate_random_text(), "v": "10th"}],
            "London": [{"c": generate_random_text(), "v": "Geneva"}, {"c": generate_random_text(), "v": "Moorland"}],
            "Vancouver": [{"c": generate_random_text(), "v": "12th Ave"}, {"c": generate_random_text(), "v": "Geneva"}],
        }

        # Dictionary for filter by global index
        global_data = defaultdict(list)
        for key, indexes in local_data.items():
            for columns_data in indexes:
                global_data[columns_data["v"]].append({"pk": key, "c": columns_data["c"]})

        # Insert data
        for key, columns in local_data.items():
            for columns_data in columns:
                query = "INSERT INTO {table_name} (key, c, v) VALUES ('{key}', '{c}', '{v}')".format(table_name=table_name, key=key, c=columns_data["c"], v=columns_data["v"])
                session.execute(query)

        # Filter by local index
        for key, columns in local_data.items():
            for columns_data in columns:
                ck = columns_data["c"]
                index_value = columns_data["v"]
                query = f"SELECT key, c, v FROM {table_name} WHERE key='{key}' AND v='{index_value}'"
                assert_all(session=session, query=query, expected=[[key, ck, index_value]], cl=ConsistencyLevel.QUORUM, ignore_order=True, num_attempts=30)

        # Filter by global index
        for index_value, columns in global_data.items():
            expected_result = [[row["pk"], row["c"]] for row in columns]
            query = f"SELECT key, c FROM {table_name} WHERE v='{index_value}'"
            assert_all(session=session, query=query, expected=expected_result, cl=ConsistencyLevel.QUORUM, ignore_order=True, num_attempts=30)

    def test_query_data_created_before_local_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name="ks", table_name="users", index_column="gender", index_name="gender_key", pk_name="key", compaction=self.compaction_strategy), (
            'Index "gender_key" is not built'
        )
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name="ks", table_name="users", index_column="state", index_name="state_key", pk_name="key", compaction=self.compaction_strategy), (
            'Index "state_key" is not built'
        )
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name="ks", table_name="users", index_column="birth_year", index_name="birth_year_key", pk_name="key", compaction=self.compaction_strategy), (
            'Index "birth_year_key" is not built'
        )

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        assert_all(session, "select count(*) from users", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where key='user4' and state='TX'", expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where key='user2' and state='CA'", expected=[[1]], cl=ConsistencyLevel.QUORUM)

    def test_query_data_by_ck_and_local_index(self):
        """
        Filter data by primary and clustering keys and secondary index
        """
        ks_name = "ks"
        table_name = "cf"
        index_column = "v"
        index_name = "v_inx"

        session = self.prepare(nodes=4, rf=3)
        create_cf(session, f"{ks_name}.{table_name}", key_type="text", compaction={"class": self.compaction_strategy})

        # insert data
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user1', 'ch@ngem3a', 'f')")
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user2', 'ch@ngem3b', 'm')")
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user3', 'ch@ngem3c', 'f')")
        session.execute(f"INSERT INTO {table_name} (key, c, v) VALUES ('user4', 'ch@ngem3d', 'm')")

        # create index
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name=ks_name, table_name=table_name, index_column=index_column, index_name=index_name, pk_name="key", compaction=self.compaction_strategy), (
            f"Index {index_name} is not built"
        )

        assert_all(session, f"select count(*) from {table_name}", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, f"select count(*) from {table_name} where key='user2' and c='ch@ngem3b' and v='m'", expected=[[1]], cl=ConsistencyLevel.ALL)
        assert_all(session, f"select count(*) from {table_name} where KEY='user1' and c='ch@ngem3a' and v='m'", expected=[[0]], cl=ConsistencyLevel.QUORUM)

    def test_insert_data_after_recreating_ks_with_local_index(self):
        """
        Data inserted immediately after dropping and recreating a keyspace with an indexed column familiy is not
        included in the index.
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = "ks"
        table_name = "cf"
        index = {"index_name": "v_ind", "index_column": "v", "pk_name": "key"}
        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        for i in range(10):
            logger.debug(f"round {i}")
            try:
                session.execute(f"DROP KEYSPACE {ks_name}")
            except ConfigurationException:
                pass

            self.config_keyspace(session, ks_name, table_name, index)

            for r in range(10):
                session.execute(f"INSERT INTO {ks_name}.{table_name} (key, c, v) VALUES ('{r}', '{generate_random_text()}','asdf')")

            wait_for_schema_agreement(session)
            for r in range(10):
                assert_all(session, f"select count(*) from {ks_name}.{table_name} WHERE key='{r}' and v='asdf'", expected=[[1]], cl=ConsistencyLevel.QUORUM, num_attempts=60, sleep_time=1)

    @pytest.mark.parametrize(
        "value_length,expect_message",
        [
            (OVERSIZE_LENGTH, "Key size too large|is longer than maximum"),
            (LONG_TEXT_LENGTH, None),
        ],
        ids=["oversize", "long"],
    )
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_local_indexed_values(self, value_length, expect_message):
        """
        First test:
         Reject inserts & updates where values of any indexed column is > 64k

        Second test:
          Correct inserts & updates where values of any indexed column is long and up to 64k
        """
        # This test is negative, and there are errors in the cluster' logs.
        # The teardown fails because it expects a cluster doesn't contain errors if the test is passed.
        # Before fixing issue #10366, we had a internal server error with
        # the text "Key size too large". After fixing it, it became an
        # InvalidRequest error, with the text "is longer than maximum".
        if OVERSIZE_LENGTH == value_length:
            self.ignore_log_patterns += [
                r".*std::runtime_error[ :]+\(?Key size too large: .*? > 65535\)?.*",
                r".*is longer than maximum.*",
            ]
        if expect_message:
            self.ignore_log_patterns += [expect_message]

        session = self.prepare(nodes=4, rf=3)
        test = "oversize" if value_length == OVERSIZE_LENGTH else "long"

        logger.debug(f"Insert {test} value into non-PK column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b int, c varchar, PRIMARY KEY (a)) WITH compaction = %s",
            "CREATE INDEX ON %s ((a), c)",
            "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
            session,
            column_name="c",
            value_length=value_length,
            expect_message=expect_message,
        )

        logger.debug(f"Insert {test} value into clustering key column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b)) WITH compaction = %s",
            "CREATE INDEX ON %s ((a), b)",
            "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)",
            session,
            column_name="b",
            value_length=value_length,
            expect_message=expect_message,
        )

        logger.debug(f"Table with compact storage. Insert {test} value into non-PK column")
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE and compaction = %s",
            "CREATE INDEX ON %s ((a), b)",
            "INSERT INTO %s (a, b) VALUES (0, ?)",
            session,
            column_name="b",
            value_length=value_length,
            expect_message=expect_message,
        )

    def test_truncate_base_with_local_index(self):
        """
        asserts that truncating base table will result in truncating secondary index as well
        """

        def select_by_index(expected_count):
            smt = "SELECT count(*) FROM {0} WHERE {1} = '{2}' and key = {3}"
            for data in data_set:
                assert_all(session, smt.format(table_name, index_column, data[1], data[0]), expected=[[expected_count]], cl=ConsistencyLevel.QUORUM)

        keyspace_name = "ks"
        table_name = "tbl"
        index_name = "ix_tbl_c0"
        index_column = "c0"
        pk_name = "key"

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="int", columns={"c0": "text", "c1": "text", "c2": "text"}, compaction={"class": self.compaction_strategy})

        assert self.create_and_build_index(create_local_index, self.cluster, session, keyspace_name, table_name, index_column, index_name, pk_name=pk_name, compaction=self.compaction_strategy), "Index %s is not built" % index_name

        data_set = [[0, "a", "b"], [1, "a", "b"], [2, "q", "b"], [3, "a", "e"], [4, "a", "e"]]
        smt = "INSERT INTO {table_name} (key, c0, c1) values ({pk}, '{c0}', '{c1}')"
        for data in data_set:
            session.execute(smt.format(table_name=table_name, pk=data[0], c0=data[1], c1=data[2]))

        # ensure sstables are created and will be dropped
        self.cluster.flush()

        # ensure data is loaded into cache and the cache will be cleared
        select_by_index(1)
        assert_row_count(session, "tbl", 5)

        session.execute("TRUNCATE table tbl")
        assert_row_count(session, "tbl", 0)

        # check that index queries are also truncated
        select_by_index(0)

    @pytest.mark.single_node
    def test_multi_column_local_index(self):
        """
        Test that impossible to create secondary index on the few columns and valid error message is received
        """
        keyspace_name = "ks"
        table_name = "cf"
        index_columns = {"b": "int", "c": "int"}

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        # try to create index on 2 columns
        create_cf(session, table_name, key_type="int", columns=index_columns, compaction={"class": self.compaction_strategy})
        assert_expected_error(
            func=create_local_index, expected_error="Only CUSTOM indexes support multiple columns", args=(session, table_name, "key", index_columns.keys()), kwargs={"index_name": "two_columns_index", "compaction": self.compaction_strategy}
        )

        # try to create index on 6 columns
        table_name = "cf_6columns"
        index_columns = {"b": "int", "c": "int", "d": "int", "e": "int", "f": "int", "g": "int"}
        create_cf(session, table_name, key_type="int", columns=index_columns, compaction={"class": self.compaction_strategy})
        assert_expected_error(
            func=create_local_index, expected_error="Only CUSTOM indexes support multiple columns", args=(session, table_name, "key", index_columns), kwargs={"index_name": "six_columns_index", "compaction": self.compaction_strategy}
        )

    def _prepare_for_ttl(self):
        keyspace_name = "ks"
        table_name = "cf"
        index_column = "b"
        index_name = f"{index_column}_inx"
        select_query = "select * from {} where {} key = 0"
        mv_query = f"select key, {index_column} from {get_index_view_name(index_name)}"

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type="int", columns={"b": "int", "c": "int"}, compaction={"class": self.compaction_strategy})
        session.execute(f"INSERT INTO {table_name} (key, b, c) VALUES (0, 1, 2)")
        assert_all(session, select_query.format(table_name, ""), [[0, 1, 2]], cl=ConsistencyLevel.ALL)

        assert self.create_and_build_index(create_local_index, self.cluster, session, keyspace_name, table_name, index_column, index_name, pk_name="key", compaction=self.compaction_strategy), "Index %s is not built" % index_name
        assert_all(session, select_query.format(table_name, "%s = %d and" % (index_column, 1)), [[0, 1, 2]], cl=ConsistencyLevel.ALL)
        return session, keyspace_name, table_name, index_column, index_name, select_query, mv_query

    @pytest.mark.single_node
    def test_ttl_local_index_column(self):
        """
        Verify SI with default_time_to_live can be deleted properly using expired livenessInfo
        """
        session, _keyspace_name, table_name, index_column, _index_name, select_query, mv_query = self._prepare_for_ttl()

        ttl = 60
        logger.debug(f"Update index column with TTL {ttl}")
        session.execute(f"UPDATE {table_name} USING TTL {ttl} SET {index_column}=3 WHERE key=0")
        assert_all(session, select_query.format(table_name, "%s = %d and" % (index_column, 3)), [[0, 3, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, "%s = %d and" % (index_column, 1)), cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 3]], cl=ConsistencyLevel.ALL)

        time.sleep(ttl + 5)
        # Validate that no record is returned when filtered by index
        assert_all(session, select_query.format(table_name, ""), [[0, None, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, "%s = %d and" % (index_column, 3)), cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, "%s = %d and" % (index_column, 1)), cl=ConsistencyLevel.ALL)
        assert_none(session, mv_query, cl=ConsistencyLevel.ALL)

    def test_delete_local_indexed_rows(self):
        """
        Delete rows from indexed table and read data by index
        """
        keyspace_name = "ks"
        table_name = "cf"
        index_name = "b_index"
        index_column = "b"

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name, session_rack=2)

        create_cf(session, table_name, key_type="int", columns={"b": "int"}, compaction={"class": self.compaction_strategy})
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name=keyspace_name, table_name=table_name, index_column=index_column, index_name=index_name, pk_name="key", compaction=self.compaction_strategy), (
            "Index %s is not built" % index_name
        )

        num_rows = 100
        for i in range(num_rows):
            indexed_value = i + 100
            session.execute(f"INSERT INTO {keyspace_name}.{table_name} (key, b) VALUES ({i}, {indexed_value})")

        # Delete 10 rows by index
        logger.debug("Delete 10 rows by index")
        start_key, delete_num = 30, 10
        rows_for_delete = list(range(start_key, start_key + delete_num))
        for i in rows_for_delete:
            session.execute(f"DELETE FROM {table_name} WHERE key = {i}")

        # Validate the data is not in table
        assert_row_count(session, table_name=table_name, expected=num_rows - delete_num, consistency_level=ConsistencyLevel.ALL, num_attempts=60, sleep_time=1)

        query = "select key, b from {} where key={} and {}={}"
        for i in list(range(num_rows)):
            if i in rows_for_delete:
                assert_none(session, query=query.format(table_name, i, index_column, i + 100), cl=ConsistencyLevel.ALL)
            else:
                res = [[i, i + 100]]
                assert_all(session, query=query.format(table_name, i, index_column, i + 100), expected=res, cl=ConsistencyLevel.ALL)

        # Validate the data is not in table and SI materialized view
        assert_row_count(session, table_name=get_index_view_name(index_name), expected=num_rows - delete_num, consistency_level=ConsistencyLevel.ALL)

    def test_stop_node_after_local_index_build(self):
        """
        Stop one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="stop", nodes=4, rf=3, num_rows=1000)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#22394"))
    def test_remove_node_after_local_index_build(self):
        """
        Remove one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="remove", nodes=4, rf=3, num_rows=1000)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#22394"))
    def test_decommission_node_after_local_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="decommission", nodes=4, rf=3, num_rows=1000)

    def test_add_node_after_local_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action="add", nodes=3, rf=3, num_rows=1000)

    def _node_action_after_index_build(self, node_action, nodes, rf, num_rows):
        keyspace_name = "ks"
        table_name = "cf"
        index_name = "b_index"
        index_column = "b"
        view_name = get_index_view_name(index_name)

        session = self.prepare(nodes=nodes, rf=rf, keyspace_name=keyspace_name, session_rack=2)
        nodes_in_rack1 = [node for node in self.cluster.nodelist() if node.rack == "rack1"]
        node2 = nodes_in_rack1[-1]
        node2_ip = next(iter(node2.network_interfaces["binary"]))

        create_cf(session, table_name, key_type="int", columns={"b": "int"}, compaction={"class": self.compaction_strategy})

        statement = session.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, b) VALUES (?, ?)")
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement, map(lambda k: [k, k + num_rows], [k for k in range(num_rows)]))

        # Create index and wait while the index is built
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name=keyspace_name, table_name=table_name, index_name=index_name, index_column=index_column, pk_name="key", compaction=self.compaction_strategy), (
            "Index %s is not built" % index_name
        )

        exclude_errors = [f"Can't send migration request: node {node2_ip} is down"]
        self.ignore_log_patterns += exclude_errors

        # Perform action on second node
        self.node_action_with_delay(node_action, node=node2)

        # Validate the data using filtering by index
        self.validate_index_data(session, cl=ConsistencyLevel.QUORUM, num_rows=num_rows, table_name=table_name, index_column=index_column)

        # Validate view rows
        assert_row_count_in_select(session=session, query=f"SELECT * FROM {view_name}", num_rows_expected=num_rows, consistency_level=ConsistencyLevel.QUORUM)

        self.check_errors(self.cluster.nodelist()[0], exclude_errors)


@pytest.mark.dtest_full
class TestMultipleSecondaryIndexes(SecondaryIndexesHelpers):
    def _prepare_for_multi_index_test(self):
        session = self.prepare(user_table=False, nodes=4, rf=3, keyspace_name="ks")
        session.consistency_level = ConsistencyLevel.QUORUM
        session.execute("CREATE TABLE test_table (row varchar PRIMARY KEY, name varchar, value int);")
        assert self.create_and_build_index(create_index, self.cluster, session, "ks", "test_table", "name", "name_idx"), "Index name_idx is not built"
        assert self.create_and_build_index(create_index, self.cluster, session, "ks", "test_table", "value", "value_idx"), "Index value_idx is not built"
        stmt_insert = session.prepare("INSERT INTO test_table (row, name, value) VALUES (?, ?, ?)")
        for rec in [
            ["AAA1", "AAAA", 0],
            ["AAA2", "AAAA", 100],
            ["AAA3", "XXXX", 0],
            ["AAA4", "XXXX", 100],
            ["AAA5", "AAAA", 0],
            ["AAA6", "AAAA", 100],
            ["AAA7", "XXXX", 0],
            ["AAA8", "XXXX", 100],
        ]:
            session.execute(stmt_insert, rec)
        self._use_filtering_error_message = (
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
        )
        return session

    def test_multy_secondary_query_with_no_pk(self):
        """
        Test against table with multiple secondary indexes, queries have no primary index field in WHERE clause
        """
        session = self._prepare_for_multi_index_test()
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='AAAA'",
            expected=[["AAA2", "AAAA", 100], ["AAA1", "AAAA", 0], ["AAA6", "AAAA", 100], ["AAA5", "AAAA", 0]],
            ignore_order=True,
            cl=ConsistencyLevel.QUORUM,
        )
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='XXXX'",
            expected=[["AAA7", "XXXX", 0], ["AAA8", "XXXX", 100], ["AAA4", "XXXX", 100], ["AAA3", "XXXX", 0]],
            ignore_order=True,
            cl=ConsistencyLevel.QUORUM,
        )
        assert_all(session, "SELECT * FROM test_table WHERE value=0", expected=[["AAA7", "XXXX", 0], ["AAA1", "AAAA", 0], ["AAA3", "XXXX", 0], ["AAA5", "AAAA", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE value=100",
            expected=[["AAA2", "AAAA", 100], ["AAA8", "XXXX", 100], ["AAA4", "XXXX", 100], ["AAA6", "AAAA", 100]],
            ignore_order=True,
            cl=ConsistencyLevel.QUORUM,
        )
        assert_invalid(session, "SELECT * FROM test_table WHERE name='AAAA' and value=0", self._use_filtering_error_message)
        assert_all(session, "SELECT * FROM test_table WHERE name='AAAA' and value=0 ALLOW FILTERING", expected=[["AAA1", "AAAA", 0], ["AAA5", "AAAA", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_invalid(session, "SELECT * FROM test_table WHERE name='AAAA' and value=100", self._use_filtering_error_message)
        assert_all(session, "SELECT * FROM test_table WHERE name='AAAA' and value=100 ALLOW FILTERING", expected=[["AAA2", "AAAA", 100], ["AAA6", "AAAA", 100]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_invalid(session, "SELECT * FROM test_table WHERE name='XXXX' and value=0", self._use_filtering_error_message)
        assert_all(session, "SELECT * FROM test_table WHERE name='XXXX' and value=0 ALLOW FILTERING", expected=[["AAA7", "XXXX", 0], ["AAA3", "XXXX", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_invalid(session, "SELECT * FROM test_table WHERE name='XXXX' and value=100", self._use_filtering_error_message)
        assert_all(session, "SELECT * FROM test_table WHERE name='XXXX' and value=100 ALLOW FILTERING", expected=[["AAA8", "XXXX", 100], ["AAA4", "XXXX", 100]], ignore_order=True, cl=ConsistencyLevel.QUORUM)

    def test_multy_secondary_query_with_pk(self):
        """
        Test against table with multiple secondary indexes, queries have primary index field in WHERE clause
        """
        session = self._prepare_for_multi_index_test()
        assert_invalid(session, "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA' and value=0", self._use_filtering_error_message)
        assert_all(session, "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA' and value=0 ALLOW FILTERING", expected=[["AAA1", "AAAA", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA'", expected=[["AAA1", "AAAA", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT * FROM test_table WHERE row='AAA1' and value=0", expected=[["AAA1", "AAAA", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT * FROM test_table WHERE row='AAA1'", expected=[["AAA1", "AAAA", 0]], ignore_order=True, cl=ConsistencyLevel.QUORUM)


class DtestTimeoutError(Exception):
    pass
