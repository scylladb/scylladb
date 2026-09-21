import logging
import string
import struct
import time
from datetime import timedelta
from random import randint, sample

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, ReadTimeout, Unavailable
from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionShutdown
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import FlakyRetryPolicy, Tester, create_ks
from tools.assertions import assert_all, assert_invalid, assert_one, assert_unavailable
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology
from tools.data import (
    get_entity_id,
    get_truncated_time_from_system_local,
    get_truncated_time_from_system_truncated,
    rows_to_list,
)
from tools.session import wait_reconnection

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class CQLTester(Tester):
    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None, user=None, password=None, configuration_options=None, **kwargs):  # noqa: PLR0913
        cluster = self.cluster

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        start_rpc = kwargs.pop("start_rpc", False)
        if start_rpc:
            cluster.set_configuration_options(values={"start_rpc": True})

        if user:
            config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0}
            cluster.set_configuration_options(values=config)

        if configuration_options:
            logger.debug("Setting cluster configuration_options: %s", configuration_options)
            cluster.set_configuration_options(values=configuration_options)

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestStorageProxyCQL(CQLTester):
    """
    Each CQL statement is exercised at least once in order to
    ensure we execute the code path in StorageProxy.
    Note that in depth CQL validation is done in Java unit tests,
    see CASSANDRA-9160.
    """

    # ALTER KEYSPACE cannot switch between vnodes and tablets, and SimpleStrategy does not support tablets.
    @pytest.mark.required_features("!tablets")
    def test_keyspace(self):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})
        cluster.new_node(1, data_center="dc0")
        cluster.new_node(2, data_center="dc1")
        cluster.start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        logger.debug("Creating keyspace")
        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        start = time.time()
        while True:
            try:
                logger.debug("Alter keyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 }")
                session.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")
                break
            except ConfigurationException:
                # wait a while for gossiping snitch
                if time.time() - start < 60:
                    time.sleep(1)

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "USE ks", expected=InvalidRequest)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_table(self):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")

        session.execute("ALTER TABLE test1 ADD v2 int")

        for i in range(10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
            session.execute("INSERT INTO test2 (k, c1, v1) VALUES (%d, %d, %d)" % (i, i, i))

        res = sorted(session.execute("SELECT * FROM test1"))
        assert rows_to_list(res) == [[i, i, i] for i in range(10)], res

        res = sorted(session.execute("SELECT * FROM test2"))
        assert rows_to_list(res) == [[i, i, i] for i in range(10)], res

        session.execute("TRUNCATE test1")
        session.execute("TRUNCATE test2")

        res = session.execute("SELECT * FROM test1")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [], res

        session.execute("DROP TABLE test1")
        session.execute("DROP TABLE test2")

        assert_invalid(session, "SELECT * FROM test1", expected=InvalidRequest)
        assert_invalid(session, "SELECT * FROM test2", expected=InvalidRequest)

    def test_index(self):
        """
        CREATE INDEX, DROP INDEX statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int)")
        session.execute("CREATE INDEX testidx ON test3 (v1)")

        for i in range(10):
            session.execute("INSERT INTO test3 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT * FROM test3 WHERE v1 = 0")
        assert rows_to_list(res) == [[0, 0, 0]], res

        session.execute("DROP INDEX testidx")

        assert_invalid(session, "SELECT * FROM test3 where v1 = 0", expected=InvalidRequest)

    def test_type(self):
        """
        CREATE TYPE, ALTER TYPE, DROP TYPE statements
        """
        session = self.prepare()

        session.execute("CREATE TYPE address_t (street text, city text, zip_code int)")
        session.execute("CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("ALTER TYPE address_t ADD phones set<text>")
        session.execute("CREATE TABLE test5 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("DROP TABLE test4")
        session.execute("DROP TABLE test5")
        session.execute("DROP TYPE address_t")
        assert_invalid(session, "CREATE TABLE test6 (id int PRIMARY KEY, address frozen<address_t>)", expected=InvalidRequest)

    def test_user(self):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        session = self.prepare(user="cassandra", password="cassandra")

        session.execute("CREATE USER user1 WITH PASSWORD 'secret'")

        session.execute("ALTER USER user1 WITH PASSWORD 'secret^2'")

        session.execute("DROP USER user1")

    @pytest.mark.dtest_smoke
    @pytest.mark.dtest_debug
    def test_statements(self):
        """
        INSERT, UPDATE, SELECT, SELECT COUNT, DELETE statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in range(10):
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', %d, %d, %d)" % (i, i, i))
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', %d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[10]], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2')")
        assert rows_to_list(res) == [[20]], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0")
        assert rows_to_list(res) == [[2]], res

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [["ev1", i, i, i] for i in range(10)], res

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev2'")
        assert rows_to_list(res) == [["ev2", i, i, i] for i in range(10)], res

        for i in range(10):
            session.execute("UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=%d" % (i,))

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [["ev1", i, 0, 0] for i in range(10)], res

        res = session.execute("DELETE FROM test7 WHERE kind = 'ev1'")
        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[0]], res

    def test_batch(self):
        """
        BATCH statement
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test8 (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """
        )

        query = SimpleStatement(
            """
            BEGIN BATCH
                INSERT INTO test8 (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE test8 SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO test8 (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM test8 WHERE userid = 'user1';
            APPLY BATCH;
        """,
            consistency_level=ConsistencyLevel.QUORUM,
        )
        session.execute(query)


@pytest.mark.dtest_full
class TestMiscellaneousCQL(CQLTester):
    """
    CQL tests that cannot be performed as Java unit tests, see CASSANDRA-9160. Please consider
    writing java unit tests for CQL validation, add a new test here only if there is a reason for it,
    e.g. something related to the client protocol or examine the log files, or multiple nodes
    required.
    """

    @pytest.mark.single_node
    def test_large_collection(self):
        """
        For large collections, make sure that we are printing warnings.
        """

        threshold = 1000
        configuration_options = {"compaction_collection_elements_count_warning_threshold": threshold}
        session = self.prepare(configuration_options=configuration_options)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        session.execute(
            """
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """
        )

        logger.debug("Populating map up to warning threshold")
        for i in range(threshold):
            session.execute("UPDATE maps SET properties[%i] = 'x' WHERE userid = 'user'" % i)

        node1.flush()
        node1.compact()
        node1.wait_for_compactions()

        msg = "Writing large collection"
        res = node1.grep_log(msg)
        assert not res, f"Found unexpected warnings: {res}"
        mark = node1.mark_log()

        query = "SELECT * from system.large_cells WHERE keyspace_name='ks' AND table_name='maps' ALLOW FILTERING"
        res = list(session.execute(query))
        assert not res, f"Found unexpected row in system.large_cells: {res}"

        self.ignore_log_patterns.append(msg)
        logger.debug("Adding entry to map to trigger warning")
        session.execute(f"UPDATE maps SET properties[{threshold}] = 'x' WHERE userid = 'user'")

        node1.flush()
        node1.compact()
        node1.wait_for_compactions()

        res = node1.grep_log(msg, from_mark=mark)
        assert res, f"Did not find expected log message: {msg}"

        query = "SELECT * from system.large_cells WHERE keyspace_name='ks' AND table_name='maps' ALLOW FILTERING"
        res = list(session.execute(query))
        logger.debug(res)
        assert res, f"Did not find expected row in system.large_cells"
        assert len(res) == 1, f"Found too many rows in system.large_cells: {res}"

    @pytest.mark.single_node
    def test_prepared_statement_invalidation(self):
        """
        @jira_ticket CASSANDRA-7910
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int, b int, c int)")
        session.execute("INSERT INTO test (k, a, b, c) VALUES (0, 0, 0, 0)")

        wildcard_prepared = session.prepare("SELECT * FROM test")
        explicit_prepared = session.prepare("SELECT k, a, b, c FROM test")
        result = list(session.execute(wildcard_prepared.bind(None)))
        assert result == [(0, 0, 0, 0)]

        session.execute("ALTER TABLE test DROP c")
        result = list(session.execute(wildcard_prepared.bind(None)))
        # wildcard select can be automatically re-prepared by the driver
        assert result == [(0, 0, 0)]
        # but re-preparing the statement with explicit columns should fail
        # (see PYTHON-207 for why we expect InvalidRequestException instead of the normal exc)
        assert_invalid(session, explicit_prepared.bind(None), expected=InvalidRequest)

        session.execute("ALTER TABLE test ADD d int")
        result = list(session.execute(wildcard_prepared.bind(None)))
        assert result == [(0, 0, 0, None)]

        explicit_prepared = session.prepare("SELECT k, a, b, d FROM test")

        # when the type is altered, both statements will need to be re-prepared
        # by the driver, but the re-preparation should succeed
        session.execute("ALTER TABLE test ALTER d TYPE blob")
        result = list(session.execute(wildcard_prepared.bind(None)))
        assert result == [(0, 0, 0, None)]

        result = list(session.execute(explicit_prepared.bind(None)))
        assert result == [(0, 0, 0, None)]

    def test_reverse_query(self, subtests):
        """
        Issue: https://github.com/scylladb/scylla/issues/6171
        Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
        Create table with 2 clustering keys
        Read with filter using "in" restriction on clustering keys and ordered by clustering keys DESC with and without
        BYPASS CACHE
        """
        session = self.prepare(nodes={"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}}, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1))")

        test_value = string.ascii_lowercase * 40
        for i in range(10000):
            session.execute(f"INSERT INTO cf(pk, ck, ck1, v) VALUES (0, {i}, {i}, '{test_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [10000], cl=ConsistencyLevel.QUORUM)

        in_list = [i for i in range(0, 10000, 1000)]
        in_str = ", ".join(str(i) for i in in_list)
        expected_results = [[test_value] for _ in reversed(in_list)]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) and ck1 in ({in_str}) ORDER BY ck DESC, ck1 DESC"

        with subtests.test("Read without BYPASS CACHE"):
            logger.debug(f"Read without BYPASS CACHE with query: {read_stmt}")
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with subtests.test("Read with BYPASS CACHE"):
            logger.debug(f"Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE")
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def test_normal_query(self, subtests):
        """
        Issue: https://github.com/scylladb/scylla/issues/6171
        Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
        Create table with 2 clustering keys
        Read with filter using "in" restriction on clustering keys and ordered by clustering keys ASC with and without
        BYPASS CACHE
        """
        session = self.prepare(nodes={"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}}, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1))")

        test_value = string.ascii_lowercase * 40
        for i in range(10000):
            session.execute(f"INSERT INTO cf(pk, ck, ck1, v) VALUES (0, {i}, {i}, '{test_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [10000], cl=ConsistencyLevel.QUORUM)

        in_list = [i for i in range(0, 10000, 1000)]
        in_str = ", ".join(str(i) for i in in_list)
        expected_results = [[test_value] for _ in in_list]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) and ck1 in ({in_str})"

        with subtests.test("Read without BYPASS CACHE"):
            logger.debug(f"Read without BYPASS CACHE with query: {read_stmt}")
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with subtests.test("Read with BYPASS CACHE"):
            logger.debug(f"Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE")
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def test_reverse_query_ck_collect(self, subtests):
        """
        Issue: https://github.com/scylladb/scylla/issues/6171
        Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
        Create table where clustering key is frozen collection
        Read with filter using "in" restriction on clustering key and ordered by clustering key DESC with and without
        BYPASS CACHE
        """
        session = self.prepare(nodes={"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}}, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck frozen<list<text>>, v text, PRIMARY KEY (pk, ck))")

        all_ascii = list(string.ascii_lowercase)
        text_value = string.ascii_lowercase * 40
        for i in all_ascii:
            session.execute(f"INSERT INTO cf(pk, ck, v) VALUES (0, ['{i}'], '{text_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [len(all_ascii)], cl=ConsistencyLevel.QUORUM)

        in_list = [all_ascii[i] for i in range(0, 26, 10)]
        in_str = ", ".join(f"['{i}']" for i in in_list)
        expected_results = [[f"{text_value}"] for _ in reversed(in_list)]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) ORDER BY ck DESC"

        with subtests.test("Read without BYPASS CACHE"):
            logger.debug(f"Read without BYPASS CACHE with query: {read_stmt}")
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with subtests.test("Read with BYPASS CACHE"):
            logger.debug(f"Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE")
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def test_reverse_query_table_desc(self, subtests):
        """
        Issue: https://github.com/scylladb/scylla/issues/6171
        Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
        Create table with 2 clustering keys and ordered by both clustering keys DESC
        Read with filter using "in" restriction on clustering keys and ordered by clustering keys DESC with and without
        BYPASS CACHE
        """
        session = self.prepare(nodes={"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}}, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1)) WITH CLUSTERING ORDER BY (ck DESC, ck1 DESC)")

        text_value = string.ascii_lowercase * 40
        for i in range(10000):
            session.execute(f"INSERT INTO cf(pk, ck, ck1, v) VALUES (0, {i}, {i}, '{text_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [10000], cl=ConsistencyLevel.QUORUM)

        in_list = [i for i in range(10000, 0, 1000)]
        in_str = ", ".join(str(i) for i in in_list)
        expected_results = [[text_value] for _ in reversed(in_list)]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) and ck1 in ({in_str}) ORDER BY ck DESC, ck1 DESC"

        with subtests.test("Read without BYPASS CACHE"):
            logger.debug(f"Read without BYPASS CACHE with query: {read_stmt}")
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with subtests.test("Read with BYPASS CACHE"):
            logger.debug(f"Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE")
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def test_range_slice(self):
        """Test a regression from #1337"""

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """
        )
        time.sleep(1)

        session.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        session.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 2, res

    @pytest.mark.parametrize("stop_gently, query_type", ((gently, query) for gently in [True, False] for query in ["point", "count"]))
    def test_query_failed_when_node_is_down(self, stop_gently: bool, query_type: str):
        """
        Test that a select query with consistency_level=QUOROM
        returns an Unavailable error properly when one out of two nodes is DOWN.
        """
        cluster = self.cluster
        # reduce read/range_request_timeout to make the first try
        # timeout faster when node2 is killed.
        request_timeout_in_ms = randint(1, 10) * 1000
        if query_type == "point":
            config = {"read_request_timeout_in_ms": f"{request_timeout_in_ms}"}
        elif query_type == "count":
            config = {"range_request_timeout_in_ms": f"{request_timeout_in_ms}"}

        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        session = self.prepare(nodes=cluster_topology, rf=2, configuration_options=config)
        node1, node2 = self.cluster.nodelist()

        ks = "ks"
        cf = "cf"
        session.execute(f"CREATE TABLE {cf} (pk int, ck int, v text, PRIMARY KEY (pk, ck))")

        logger.debug("Inserting data...")
        for pk in range(10):
            for ck in range(100):
                q = SimpleStatement(f"INSERT INTO {ks}.{cf} (pk, ck, v) VALUES ({pk}, {ck}, 'foo')", consistency_level=ConsistencyLevel.ALL)
                session.execute(q)
        cluster.flush()

        logger.debug(f"Stopping node (gently={stop_gently})")
        node2.stop(gently=stop_gently, wait_other_notice=True)

        with self.patient_cql_cluster_session(node1, ks, exclusive=True) as session:
            logger.debug("Selecting with CL=ONE")

            if query_type == "point":
                assert_one(session, f"SELECT v from {ks}.{cf} WHERE pk = 1 AND ck = 1 BYPASS CACHE", ["foo"], cl=ConsistencyLevel.ONE)
            elif query_type == "count":
                assert_one(session, f"SELECT count(*) from {ks}.{cf} BYPASS CACHE", [1000], cl=ConsistencyLevel.ONE)

            logger.debug("Selecting with CL=QUORUM (expected to fail)")
            t0 = time.time()

            if query_type == "point":
                q = SimpleStatement(f"SELECT v from {ks}.{cf} WHERE pk = 1 AND ck = 1 BYPASS CACHE", consistency_level=ConsistencyLevel.QUORUM)
                assert_unavailable(lambda t: session.execute(q, timeout=t), self.cql_timeout(60))
            elif query_type == "count":
                q = SimpleStatement(f"SELECT count(*) from {ks}.{cf} BYPASS CACHE", consistency_level=ConsistencyLevel.QUORUM)
                assert_unavailable(lambda t: session.execute(q, timeout=t), self.cql_timeout(60), additional=(NoHostAvailable,))

            dt = time.time() - t0
            allowed_timeout = 1
            if not stop_gently:
                if query_type == "point":
                    config_option = "read_request_timeout_in_ms"
                elif query_type == "count":
                    config_option = "range_request_timeout_in_ms"

                request_timeout = int(cluster._config_options[config_option]) / 1000
                # The request should fail up to request_timeout + 1 seconds
                # after max_retries or after the node is marked
                # as DN by gossip, the earlier of the two.
                allowed_timeout += 1 + request_timeout + min(request_timeout * FlakyRetryPolicy().max_retries, 20)
            assert dt <= allowed_timeout, f"Query took too long to timeout: {dt} > {allowed_timeout}"


@pytest.mark.dtest_full
class TestTruncate(CQLTester):
    @staticmethod
    def create_schema(session, rf=1):
        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor':%d} AND DURABLE_WRITES = true" % rf)
        session.execute("CREATE TABLE ks.test1 (k int PRIMARY KEY, v1 int)")

    @staticmethod
    def insert_data(conn, data=None):
        if not data:
            data = list([i, i] for i in range(30))

        for x, y in data:
            conn.execute("INSERT INTO ks.test1 (k, v1) VALUES (%d, %d)" % (x, y))
        return data

    def validate_truncated_entries_for_table(self, keyspace_name, table_name, prev_truncated_time=None):
        truncated_time_per_node = []
        for node in self.cluster.nodelist():
            if node.status == "DOWN":
                continue
            session = self.patient_exclusive_cql_connection(node=node)
            _id = get_entity_id(session=session, table_or_view="table", keyspace_name=keyspace_name, entity_name=table_name)

            # validate truncation entries in the system.truncated table - expected entry
            truncated_time = get_truncated_time_from_system_truncated(session=session, table_id=_id)
            assert truncated_time, "Expected truncated entry in the system.truncated table, but it's not found"
            truncated_time_per_node.append({node.name: truncated_time})

            # validate truncation entries in the system.local table - not expected entry
            truncated_time = get_truncated_time_from_system_local(session=session)
            assert truncated_time == [[None]], "Not expected truncated entry in the system.local table, but it's found"

        if prev_truncated_time:
            assert prev_truncated_time == truncated_time_per_node

        return truncated_time_per_node

    def test_truncate_before_restart(self):
        """
        Truncate table and then restart the node. Validate that truncated entries were removed.
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        session = self.prepare(nodes=cluster_topology, create_keyspace=False)

        self.create_schema(session=session, rf=3)

        data = self.insert_data(conn=session)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        session.execute("TRUNCATE ks.test1")
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        truncated_time_per_node = self.validate_truncated_entries_for_table(keyspace_name="ks", table_name="test1")

        node2 = self.cluster.nodelist()[1]
        node2.stop(wait_other_notice=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node2)
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        self.validate_truncated_entries_for_table(keyspace_name="ks", table_name="test1", prev_truncated_time=truncated_time_per_node)

    def test_truncate_twice(self):
        """
        Truncate table and then truncate it again. Validate that truncate ended and no data left after both truncate calls.
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        session = self.prepare(nodes=cluster_topology, create_keyspace=False)

        self.create_schema(session=session, rf=3)

        data = self.insert_data(conn=session)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        logger.debug("Truncate first time")
        session.execute("TRUNCATE ks.test1")
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        truncated_time_per_node = self.validate_truncated_entries_for_table(keyspace_name="ks", table_name="test1")

        time.sleep(1)
        logger.debug("Truncate second time")
        session.execute("TRUNCATE ks.test1")
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        sec_truncated_time_per_node = self.validate_truncated_entries_for_table(keyspace_name="ks", table_name="test1")

        assert len(truncated_time_per_node) <= len(sec_truncated_time_per_node)

    @pytest.mark.dtest_debug
    def test_truncate_after_restart(self):
        session = self.prepare(nodes=1, create_keyspace=False)

        self.create_schema(session=session, rf=1)

        node2 = new_node(self.cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True)

        data = self.insert_data(conn=session)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        node2.stop(wait_other_notice=True)
        node2.start(wait_for_binary_proto=True)

        # Many connections to exercise many shards
        conns = [self.patient_exclusive_cql_connection(node2) for i in range(3)]
        for conn in conns:
            self.insert_data(conn=conn, data=data)
            conn.execute("TRUNCATE ks.test1")
            assert_all(session=conn, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

    def test_cql_query_filtering_without_indexes(self):
        """
        https://github.com/scylladb/scylla/issues/2025
        Testing cql query filtering without the use of indexes
        use cases:
        # general use-case
        # CQL statement with relational operations( =, !=, >, < ).
        # CQL statement with IN
        # CQL statement with Limit
        """
        loop_size = 500
        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, "ks", 1)
        session.execute(
            """
                    CREATE TABLE t1 (
                        p int,
                        c int,
                        v int,
                        PRIMARY KEY (p, c)
                    );
                """
        )

        for i in range(loop_size):
            session.execute(f"INSERT INTO t1 (p, c, v) VALUES ({i},{i},{i + 1}) ")

        rand_num = randint(0, loop_size - 1)
        q1_ls = [
            f"select * from ks.t1 where c = {rand_num} and v = {rand_num + 1} allow filtering;",
            f"select * from ks.t1 where p = {rand_num} and v = {rand_num + 1} allow filtering;",
            f"select * from ks.t1 where p = {rand_num} and c = {rand_num} and v = {rand_num + 1} allow filtering;",
        ]

        for query in q1_ls:
            result = rows_to_list(session.execute(query))
            logger.debug(f"Query: {query} Result: {result}")
            assert result == [[rand_num, rand_num, rand_num + 1]], f"Query {query}: failed on assertion, Result: {result}"

        session.execute(
            """
                    CREATE TABLE t2 (
                        item_id int,
                        item_name text,
                        insert_time time,
                        PRIMARY KEY (item_id,item_name)
                    );
                """
        )

        inserted_times = []
        selected_items_q3 = None
        second_samples = sample(range(int(timedelta(days=1).total_seconds())), loop_size)
        for i in range(loop_size):
            # timedelta is formatted like "1:12:13"
            rand_time_str = str(timedelta(seconds=second_samples[i]))
            rand_time = time.strptime(rand_time_str, "%H:%M:%S")
            inserted_times.append(rand_time)
            item_name = "name_" + str(i)
            if randint(1, 10) == 1:
                selected_items_q3 = f"'{item_name}'" if selected_items_q3 is None else selected_items_q3 + ", " + f"'{item_name}'"
            session.execute(f"INSERT INTO t2 (item_id, item_name, insert_time) VALUES ({i},'{item_name}','{rand_time_str}')")

        selected_index = 6
        inserted_times.sort()
        count_above_selected_time = loop_size - selected_index - 1
        selected_time = inserted_times[selected_index]
        selected_time_str = time.strftime("%H:%M:%S", selected_time)

        # CQL statement with relational operations( =, !=, >, < ).
        q2 = f"Select item_id from t2 where insert_time > '{selected_time_str}' allow filtering;"
        q2_result = rows_to_list(session.execute(q2))
        logger.debug(f"Query: {q2}, Len_Result: {len(q2_result)}, Result: {q2_result}")
        assert len(q2_result) == count_above_selected_time, f"The returned list count doesnt match the calculated count"

        # CQL statement with IN
        q3 = f"Select * from t2 where item_name IN ({selected_items_q3})  allow filtering;"
        q3_result = rows_to_list(session.execute(q3))
        logger.debug(f"Query: {q3}, Len_Result: {len(q3_result)}, Result: {q3_result}")
        assert len(q3_result) == len(selected_items_q3.split(",")), f"The returned list count does not match the calculated count"

        # CQL statement with Limit
        rand_limit = randint(1, count_above_selected_time)
        q4 = f"Select item_id from t2 where insert_time >'{selected_time_str}' limit {rand_limit} allow filtering;"
        q4_result = rows_to_list(session.execute(q4))
        logger.debug(f"Query: {q4}, Len_Result: {len(q4_result)} Result: {q4_result}")
        assert len(q4_result) == min(count_above_selected_time, rand_limit), f"The returned rows count doesnt match min(count_above_selected_time,rand_limit) [{min(count_above_selected_time, rand_limit)}]"


@pytest.mark.dtest_full
class TestAbortedQueries(CQLTester):
    """
    @jira_ticket CASSANDRA-7392
    Test that read-queries that take longer than read_request_timeout_in_ms time out
    """

    @pytest.mark.single_node
    def test_local_query(self):
        """
        Check that a query running on the local coordinator node times out
        """
        cluster = self.cluster
        if not isinstance(cluster, ScyllaCluster):
            cluster.set_configuration_options(values={"read_request_timeout_in_ms": 1000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds during each
        # iteration of non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        jvm_args = None if isinstance(cluster, ScyllaCluster) else ["-Dcassandra.monitoring_check_interval_ms=50", "-Dcassandra.test.read_iteration_delay_ms=1500"]
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE test1 (
                id int PRIMARY KEY,
                val text
            );
        """
        )

        for i in range(500):
            session.execute(f"INSERT INTO test1 (id, val) VALUES ({i}, 'foo')")

        if isinstance(cluster, ScyllaCluster):
            node.stop()
            node.start(wait_for_binary_proto=True, jvm_args=["--read-request-timeout-in-ms=0", "--range-request-timeout-in-ms=0"])

        mark = node.mark_log()
        statement = SimpleStatement("SELECT * from test1", consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session, additional=(NoHostAvailable, ConnectionShutdown))
        if not isinstance(cluster, ScyllaCluster):
            node.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)

    def test_remote_query(self):
        """
        Check that a query running on a node other than the coordinator times out
        """
        cluster = self.cluster
        if not isinstance(cluster, ScyllaCluster):
            cluster.set_configuration_options(values={"read_request_timeout_in_ms": 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        jvm_args = None if isinstance(cluster, ScyllaCluster) else ["-Dcassandra.monitoring_check_interval_ms=50", "-Dcassandra.test.read_iteration_delay_ms=1500"]
        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True, jvm_args=jvm_args)  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE test2 (
                id int,
                col int,
                val text,
                PRIMARY KEY(id, col)
            );
        """
        )

        num_keys = 500
        num_rows = 10

        for i in range(num_keys):
            for j in range(num_rows):
                session.execute(f"INSERT INTO test2 (id, col, val) VALUES ({i}, {j}, 'foo')")

        bypass_cache = ""
        node2.stop()

        bypass_cache = " BYPASS CACHE" if isinstance(cluster, ScyllaCluster) else ""
        ids = []
        statement = session.prepare(f"SELECT * from test2 where id = ?{bypass_cache}")
        statement.consistency_level = ConsistencyLevel.ONE
        statement.retry_policy = FallthroughRetryPolicy()

        # Look for keys that exist now only on node2
        # by querying on node1 while node2 is down
        for i in range(num_keys):
            try:
                session.execute(statement, (i,))
            except Unavailable:
                ids.append(i)
                if len(ids) >= 3:
                    break

        if isinstance(cluster, ScyllaCluster):
            cluster.stop()
            cluster.set_configuration_options(values={"read_request_timeout_in_ms": 0, "range_request_timeout_in_ms": 0})
            cluster.start()
            # The local query may timeout (especially in debug mode)
            wait_reconnection(session, allowed_exceptions=(ReadTimeout,))
        else:
            node2.start(jvm_args=jvm_args)

        mark = node2.mark_log()

        assert_unavailable(lambda c: logger.debug(c.execute(statement, ids[0:1])), session)

        statement = session.prepare(f"SELECT * from test2 where id IN (?, ?, ?) AND col < 10{bypass_cache}")
        statement.consistency_level = ConsistencyLevel.ONE
        statement.retry_policy = FallthroughRetryPolicy()
        assert_unavailable(lambda c: logger.debug(c.execute(statement, ids)), session)

        statement = SimpleStatement(f"SELECT * from test2 where col > 5 ALLOW FILTERING{bypass_cache}", consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)

        statement = SimpleStatement(f"SELECT * from test2{bypass_cache}", consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)

        if not isinstance(cluster, ScyllaCluster):
            node2.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)

    @pytest.mark.single_node
    def test_index_query(self):
        """
        Check that a secondary index query times out
        """
        cluster = self.cluster
        if not isinstance(cluster, ScyllaCluster):
            cluster.set_configuration_options(values={"read_request_timeout_in_ms": 1000})

        jvm_args = None if isinstance(cluster, ScyllaCluster) else ["-Dcassandra.monitoring_check_interval_ms=50", "-Dcassandra.test.read_iteration_delay_ms=1500"]
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE test3 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """
        )

        session.execute("CREATE INDEX ON test3 (col)")

        for i in range(500):
            session.execute(f"INSERT INTO test3 (id, col, val) VALUES ({i}, {i // 10}, 'foo')")

        mark = node.mark_log()
        statement = session.prepare("SELECT * from test3 WHERE col < ? ALLOW FILTERING")
        statement.consistency_level = ConsistencyLevel.ONE
        statement.retry_policy = FallthroughRetryPolicy()

        if isinstance(cluster, ScyllaCluster):
            node.stop()
            node.start(wait_for_binary_proto=True, jvm_args=["--read-request-timeout-in-ms=0", "--range-request-timeout-in-ms=0"])

        assert_unavailable(lambda c: logger.debug(c.execute(statement, [50])), session, additional=(NoHostAvailable, ConnectionShutdown))
        if not isinstance(cluster, ScyllaCluster):
            node.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)

    def test_materialized_view(self):
        """
        Check that a materialized view query times out
        """
        cluster = self.cluster
        if not isinstance(cluster, ScyllaCluster):
            cluster.set_configuration_options(values={"read_request_timeout_in_ms": 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        jvm_args = None if isinstance(cluster, ScyllaCluster) else ["-Dcassandra.monitoring_check_interval_ms=50", "-Dcassandra.test.read_iteration_delay_ms=1500"]
        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True, jvm_args=jvm_args)  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE test4 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """
        )

        session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM test4 WHERE col IS NOT NULL AND id IS NOT NULL PRIMARY KEY (col, id)")

        for i in range(50):
            session.execute(f"INSERT INTO test4 (id, col, val) VALUES ({i}, {i // 10}, 'foo')")

        if isinstance(cluster, ScyllaCluster):
            node1.stop()
            node1.start(wait_for_binary_proto=True, jvm_args=["--read-request-timeout-in-ms=0", "--range-request-timeout-in-ms=0"])

        mark = node2.mark_log()
        statement = SimpleStatement("SELECT * FROM mv WHERE col = 50", consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session, additional=(NoHostAvailable, ConnectionShutdown))
        if not isinstance(cluster, ScyllaCluster):
            node2.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)
