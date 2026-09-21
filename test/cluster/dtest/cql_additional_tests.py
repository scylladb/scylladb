import logging
import math
import os
import random
import re
import time
import timeit
from collections import OrderedDict, defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest
from _pytest.outcomes import Failed
from cassandra import (
    AlreadyExists,
    ConsistencyLevel,
    InvalidRequest,
    ReadTimeout,
    WriteTimeout,
)
from cassandra.cluster import NoHostAvailable, ResultSet
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import ConfigurationException, SyntaxException
from cassandra.query import UNSET_VALUE, SimpleStatement, dict_factory
from cassandra.util import sortedset
from packaging.version import Version

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node, wait_for
from tools.assertions import (
    assert_all,
    assert_invalid,
    assert_none,
    assert_one,
    assert_row_count,
)
from tools.cassandra_helpers import CassandraCluster, java_version_exist
from tools.cluster_topology import generate_cluster_topology, generate_rack_topology_based_rf
from tools.data import (
    create_index,
    create_local_index,
    get_rows_set_from_res,
    rows_to_list,
)
from tools.marks import issue_open, unmark, with_feature
from tools.metrics import get_node_metrics
from tools.retrying import retrying
from tools.stress import format_cs_output
from tools.tables_view_manager import index_is_built, wait_for_view

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestCQL(Tester):
    @pytest.fixture(scope="class")
    def compaction_strategy_for_migration(self):
        return random.choice(["SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "LeveledCompactionStrategy"])

    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None, options=None, jvm_args=None, **kwargs):  # noqa: PLR0913
        if jvm_args is None:
            jvm_args = []
        if options is None:
            options = {}
        cluster = self.cluster

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        start_rpc = kwargs.pop("start_rpc", False)
        if start_rpc:
            cluster.set_configuration_options(values={"start_rpc": True})

        if options:
            cluster.set_configuration_options(values=options)

        if not cluster.nodelist():
            rack_layout = generate_rack_topology_based_rf(nodes, rf)
            topology_layout = {"dc1": rack_layout}
            cluster.populate(topology_layout).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session

    @pytest.mark.single_node
    def test_static_cf(self):
        """
        Test static CF syntax.
        """
        session = self.prepare()

        # Create
        session.execute(
            """
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """
        )

        # Inserts
        session.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        session.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        res = session.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [["Frodo", "Baggins"]], list(res)

        res = session.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [[UUID("550e8400-e29b-41d4-a716-446655440000"), 32, "Frodo", "Baggins"]], list(res)

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), 33, "Samwise", "Gamgee"],
            [UUID("550e8400-e29b-41d4-a716-446655440000"), 32, "Frodo", "Baggins"],
        ], list(res)

        # Test batch inserts
        session.execute(
            """
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """
        )

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), 37, None, None],
            [UUID("550e8400-e29b-41d4-a716-446655440000"), 36, None, None],
        ], list(res)

    @pytest.mark.single_node
    def test_prepared_statement_cache_unprivileged_eviction(self):
        """
        Test that prepared statements cache is evicting unprivileged entries and updates the corresponding metrics.

        """
        session = self.prepare(jvm_args=["--smp", "1"])
        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int)")
        node = self.cluster.nodelist()[0]

        i = 0
        # Let's simulate a pollution: query is prepared and executed exactly once
        while True:
            explicit_prepared = session.prepare(f"SELECT k, a FROM test where k = {i}")
            result = session.execute(explicit_prepared)
            i = i + 1
            res = get_node_metrics(get_ip_from_node(node), metrics=["prepared_cache_evictions", "unprivileged_entries_evictions_on_size"])
            assert res["prepared_cache_evictions"] == res["unprivileged_entries_evictions_on_size"]

            if res["prepared_cache_evictions"] > 100:
                logger.debug("number of prepared: {} prepared_cache_evictions: {} unprivileged_entries_evictions_on_size: {}".format(i, res["prepared_cache_evictions"], res["unprivileged_entries_evictions_on_size"]))
                break

    @pytest.mark.single_node
    def test_prepared_statement_cache_privileged_eviction(self):
        """
        Test that prepared statements cache is evicting privileged entries when appropriate and that the unprivileged
        section eviction metrics remains 0.

        """
        session = self.prepare(jvm_args=["--smp", "1"])
        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int)")
        node = self.cluster.nodelist()[0]

        i = 0
        # Let's verify that if query is executed more than once it moves to a privileged cache section.
        # In such a case eviction is NOT going to be from unprivileged cache section.
        while True:
            explicit_prepared = session.prepare(f"SELECT k, a FROM test where k = {i}")
            session.execute(explicit_prepared)
            session.execute(explicit_prepared)

            i = i + 1
            res = get_node_metrics(get_ip_from_node(node), metrics=["prepared_cache_evictions", "unprivileged_entries_evictions_on_size"])

            logger.debug("number of prepared: {} prepared_cache_evictions: {} unprivileged_entries_evictions_on_size: {}".format(i, res["prepared_cache_evictions"], res["unprivileged_entries_evictions_on_size"]))

            assert res["unprivileged_entries_evictions_on_size"] == 0

            if res["prepared_cache_evictions"] > 100:
                logger.debug("number of prepared: {} prepared_cache_evictions: {} unprivileged_entries_evictions_on_size: {}".format(i, res["prepared_cache_evictions"], res["unprivileged_entries_evictions_on_size"]))
                break

    def get_prep_id(self, node, pattern, mark):
        """
        Prepared statement IDs are printed in the log when prepared_statements_cache logger is set to a 'trace' verbosity.
        This function fetches those IDs.

        IDs are printed when they are inserted into the cache and when they are evicted.
        """
        node.watch_log_for([pattern], from_mark=mark)
        lines = node.grep_log(pattern, from_mark=mark)
        ids = []

        for match in lines:
            m = re.search(r"cql_id: ([0-9a-f]+)", match[0])
            assert m, f"Bad format in a prepared_statements_cache log line: {match[0]}"
            ids.append(m.group(1))
        return ids

    @pytest.mark.single_node
    def test_prepared_statement_cache_lru_eviction_privileged(self):
        """
        Test that prepared statements cache evicts LRU entry first from the privileged section:
        Let's create a workload where a single statement is always going to be MRU while we keep on pushing new
        distinct prepared statements and use them twice to force them into the privileged cache section.

        We expect the MRU statement to never be evicted and the LRU entry to be evicted first.

        """

        session = self.prepare(jvm_args=["--logger-log-level", "prepared_statements_cache=trace", "--smp", "1"])
        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int)")
        node = self.cluster.nodelist()[0]

        mark = node.mark_log()

        # This is going to be our "MRU entry" - execute it twice to push it into the privileged cache section.
        explicit_prepared0 = session.prepare("SELECT k, a FROM test where k = 0")
        session.execute(explicit_prepared0)
        session.execute(explicit_prepared0)
        first_statement_id = self.get_prep_id(node, "storing the value for the first time", mark)[0]
        logger.debug(f"first_id: {first_statement_id}")

        i = 1
        # Let's populate the prepared cache till it starts evicting: let's execute each statement twice to push them
        # into the privileged section.
        # We will also remember their IDs. We will use them to verify that entries are evicted in an LRU order.
        prep_statements_ids = []
        while True:
            mark = node.mark_log()
            prep = session.prepare(f"SELECT k, a FROM test where k = {i}")
            prep_id = self.get_prep_id(node, "storing the value for the first time", mark)[0]
            session.execute(prep)
            session.execute(prep)
            session.execute(explicit_prepared0)
            session.execute(explicit_prepared0)
            i = i + 1

            prep_statements_ids.append(prep_id)

            res = get_node_metrics(get_ip_from_node(node), metrics=["prepared_cache_evictions"])
            if res["prepared_cache_evictions"] > 0:
                break

        # Now let's populate it again with new entries while the cache is full and let's check that the MRU is not evicted
        # and LRU entries are evicted first.
        # No need to execute explicit_prepared0 twice - it's already in a privileged section.
        last_evicted = res["prepared_cache_evictions"]
        lru_id_idx = 0
        for j in range(i, 2 * i - 1):
            statement_ids = self.get_prep_id(node, r"prepared_statements_cache - shrink()", mark)

            for statement_id in statement_ids:
                logger.debug(f"{i}-{j}: evicted_id: {statement_id}")
                assert statement_id != first_statement_id, "MRU entry got evicted!"
                assert statement_id == prep_statements_ids[lru_id_idx], "LRU entry haven't got evicted!"
                lru_id_idx = lru_id_idx + 1

            mark = node.mark_log()
            prep = session.prepare(f"SELECT k, a FROM test where k = {j}")
            session.execute(prep)
            session.execute(prep)
            session.execute(explicit_prepared0)
            res = get_node_metrics(get_ip_from_node(node), metrics=["prepared_cache_evictions"])
            logger.debug("{}-{}: last_evicted {}-{}".format(i, j, last_evicted, res["prepared_cache_evictions"]))
            assert last_evicted + 1 == res["prepared_cache_evictions"], "No eviction! Must have been!"
            last_evicted = res["prepared_cache_evictions"]

    @pytest.mark.single_node
    def test_prepared_statement_cache_lru_eviction_unprivileged(self):
        """
        Test that prepared statements cache is evicted LRU entry first from the unprivileged section:
        Let's create a workload where a single statement is always going to be MRU and therefore will be in the privileged section
        while we keep on pushing new distinct prepared statements which will only be used once.

        We expect the MRU statement to never be evicted and the LRU entry to be evicted first.

        """

        # Start nodes with a single shard to keep the filtering simple
        session = self.prepare(jvm_args=["--logger-log-level", "prepared_statements_cache=trace", "--smp", "1"])
        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int)")
        node = self.cluster.nodelist()[0]

        mark = node.mark_log()

        # This is going to be our MRU entry
        explicit_prepared0 = session.prepare("SELECT k, a FROM test where k = 0")
        session.execute(explicit_prepared0)
        first_statement_id = self.get_prep_id(node, "storing the value for the first time", mark)[0]
        logger.debug(f"first_id: {first_statement_id}")

        i = 1
        # Let's populate the prepared cache till it starts evicting while executing the explicit_prepared0 on each iteration.
        # We will also remember their IDs. We will use them to verify that entries are evicted in an LRU order.
        prep_statements_ids = []
        while True:
            mark = node.mark_log()
            prep = session.prepare(f"SELECT k, a FROM test where k = {i}")
            prep_id = self.get_prep_id(node, "storing the value for the first time", mark)[0]
            session.execute(prep)
            session.execute(explicit_prepared0)
            i = i + 1

            prep_statements_ids.append(prep_id)

            res = get_node_metrics(get_ip_from_node(node), metrics=["prepared_cache_evictions", "unprivileged_entries_evictions_on_size"])
            if res["prepared_cache_evictions"] > 0:
                assert res["prepared_cache_evictions"] == res["unprivileged_entries_evictions_on_size"], "Privileged entry got evicted!"
                break

        # Now let's populate it again with new entries while the cache is full and let's check that the MRU is not evicted
        # and LRU entries are evicted first.
        last_evicted = res["prepared_cache_evictions"]
        lru_id_idx = 0
        for j in range(i, 2 * i - 1):
            statement_ids = self.get_prep_id(node, r"prepared_statements_cache - shrink()", mark)

            for statement_id in statement_ids:
                logger.debug(f"{i}-{j}: evicted_id: {statement_id}")
                assert statement_id != first_statement_id, "MRU entry got evicted!"
                assert statement_id == prep_statements_ids[lru_id_idx], "LRU entry haven't got evicted!"
                lru_id_idx = lru_id_idx + 1

            mark = node.mark_log()
            prep = session.prepare(f"SELECT k, a FROM test where k = {j}")
            session.execute(prep)
            session.execute(explicit_prepared0)
            res = get_node_metrics(get_ip_from_node(node), metrics=["prepared_cache_evictions", "unprivileged_entries_evictions_on_size"])
            logger.debug("{}-{}: last_evicted {}-{}".format(i, j, last_evicted, res["prepared_cache_evictions"]))
            assert last_evicted + 1 == res["prepared_cache_evictions"], "No eviction! Must have been!"
            assert res["prepared_cache_evictions"] == res["unprivileged_entries_evictions_on_size"], "Privileged entry got evicted!"
            last_evicted = res["prepared_cache_evictions"]

    @pytest.mark.single_node
    def test_noncomposite_static_cf(self):
        """
        Test non-composite static CF syntax.
        """
        session = self.prepare()

        # Create
        session.execute(
            """
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """
        )

        # Inserts
        session.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        session.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        res = session.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [["Frodo", "Baggins"]], list(res)

        res = session.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [[UUID("550e8400-e29b-41d4-a716-446655440000"), 32, "Frodo", "Baggins"]], list(res)

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), 33, "Samwise", "Gamgee"],
            [UUID("550e8400-e29b-41d4-a716-446655440000"), 32, "Frodo", "Baggins"],
        ], list(res)

        # Test batch inserts
        session.execute(
            """
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """
        )

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), 37, None, None],
            [UUID("550e8400-e29b-41d4-a716-446655440000"), 36, None, None],
        ], list(res)

    @pytest.mark.single_node
    def test_select_duplicate_column(self):
        """
        Regression test for https://github.com/scylladb/scylla/issues/1367

        Verify that we can select an arbitrary amount of a single column, as in
        SELECT [identifier], [identifier] FROM [table]
        """
        session = self.prepare()
        session.execute(
            """
            CREATE TABLE clicks (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            );
        """
        )
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
        # In case #1367 reproduces, we'll get
        # NoHostAvailable: ('Unable to complete the operation against any hosts', {})
        # And in the scylla log, we'll get the assertion failure
        # scylla: cql3/result_set.cc:145: void cql3::result_set::add_row(std::vector<std::experimental::fundamentals_v1::optional<basic_sstring<signed char, unsigned int, 31u> > >): Assertion `row.size() == _metadata->value_count()' failed.
        session.execute("SELECT userid, userid FROM clicks")

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_dynamic_cf(self):
        """
        Test non-composite dynamic CF syntax.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE clicks (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """
        )

        # Inserts
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)")
        session.execute("UPDATE clicks SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'")
        session.execute("UPDATE clicks SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'")

        # Queries
        res = session.execute("SELECT url, time FROM clicks WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [["http://bar.bar", 128], ["http://foo-2.bar", 24], ["http://foo-3", 12], ["http://foo.bar", 42]], list(res)

        res = session.execute("SELECT * FROM clicks WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")
        assert rows_to_list(res) == [[UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), "http://bar.foo", 24], [UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), "http://foo-3", 12]], list(res)

        res = session.execute("SELECT time FROM clicks")
        # Result from 'f47ac10b-58cc-4372-a567-0e02b2c3d479' are first
        assert rows_to_list(res) == [[24], [12], [128], [24], [12], [42]], list(res)

        # Check we don't allow empty values for url since this is the full underlying cell name (#6152)
        assert_invalid(session, "INSERT INTO clicks (userid, url, time) VALUES (810e8500-e29b-41d4-a716-446655440000, '', 42)")

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_dense_cf(self):
        """
        Test composite 'dense' CF syntax.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE connections (
                userid uuid,
                ip text,
                port int,
                time bigint,
                PRIMARY KEY (userid, ip, port)
            ) WITH COMPACT STORAGE;
        """
        )

        # Inserts
        session.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)")
        session.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)")
        session.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)")
        session.execute("UPDATE connections SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80")

        # we don't have to include all of the clustering columns (see CASSANDRA-7990)
        session.execute("INSERT INTO connections (userid, ip, time) VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, '192.168.0.3', 42)")
        session.execute("UPDATE connections SET time = 42 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'")

        # Queries
        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [["192.168.0.1", 80, 42], ["192.168.0.2", 80, 24], ["192.168.0.2", 90, 42]], list(res)

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip >= '192.168.0.2'")
        assert rows_to_list(res) == [["192.168.0.2", 80, 24], ["192.168.0.2", 90, 42]], list(res)

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip = '192.168.0.2'")
        assert rows_to_list(res) == [["192.168.0.2", 80, 24], ["192.168.0.2", 90, 42]], list(res)

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip > '192.168.0.2'")
        assert rows_to_list(res) == [], list(res)

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")
        assert [["192.168.0.3", None, 42]] == rows_to_list(res)

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'")
        assert [["192.168.0.4", None, 42]] == rows_to_list(res)

        # Deletion
        session.execute("DELETE time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND ip = '192.168.0.2' AND port = 80")
        res = list(session.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000"))
        assert len(res) == 2, res

        session.execute("DELETE FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = list(session.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000"))
        assert len(res) == 0, res

        session.execute("DELETE FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")
        res = list(session.execute("SELECT * FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'"))
        assert [] == res

    @pytest.mark.single_node
    def test_sparse_cf(self):
        """
        Test composite 'sparse' CF syntax.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE timeline (
                userid uuid,
                posted_month int,
                posted_day int,
                body text,
                posted_by text,
                PRIMARY KEY (userid, posted_month, posted_day)
            );
        """
        )

        # Inserts
        session.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 12, 'Something else', 'Frodo Baggins')")
        session.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 24, 'Something something', 'Frodo Baggins')")
        session.execute("UPDATE timeline SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND posted_month = 1 AND posted_day = 3")
        session.execute("UPDATE timeline SET body = 'Yet one more message' WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 and posted_day = 30")

        # Queries
        res = session.execute("SELECT body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day = 24")
        assert rows_to_list(res) == [["Something something", "Frodo Baggins"]], list(res)

        res = session.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day > 12")
        assert rows_to_list(res) == [[24, "Something something", "Frodo Baggins"], [30, "Yet one more message", None]], list(res)

        res = session.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1")
        assert rows_to_list(res) == [[12, "Something else", "Frodo Baggins"], [24, "Something something", "Frodo Baggins"], [30, "Yet one more message", None]], list(res)

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_create_invalid(self):
        """
        Check invalid CREATE TABLE requests.
        """

        session = self.prepare()

        assert_invalid(session, "CREATE TABLE test ()", expected=SyntaxException)

        if Version(self.cluster.version()) < Version("1.2"):
            assert_invalid(session, "CREATE TABLE test (key text PRIMARY KEY)")

        assert_invalid(session, "CREATE TABLE test (c1 text, c2 text, c3 text)")
        assert_invalid(session, "CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)")

        assert_invalid(session, "CREATE TABLE test (key text PRIMARY KEY, key int)")
        assert_invalid(session, "CREATE TABLE test (key text PRIMARY KEY, c int, c text)")

        assert_invalid(session, "CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE")

    @pytest.mark.single_node
    def test_limit_ranges(self):
        """
        Validate LIMIT option for 'range queries' in SELECT statements.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            );
        """
        )

        # Inserts
        for _id in range(100):
            for tld in ["com", "org", "net"]:
                session.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (_id, tld))

        # Queries
        res = session.execute("SELECT * FROM clicks WHERE token(userid) >= token(2) LIMIT 1")
        assert rows_to_list(res) == [[2, "http://foo.com", 42]], list(res)

        res = session.execute("SELECT * FROM clicks WHERE token(userid) > token(2) LIMIT 1")
        assert rows_to_list(res) == [[45, "http://foo.com", 42]], list(res)

    @pytest.mark.single_node
    def test_limit_multiget(self):
        """
        Validate LIMIT option for 'multiget' in SELECT statements.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            );
        """
        )

        # Inserts
        for _id in range(100):
            for tld in ["com", "org", "net"]:
                session.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (_id, tld))

        # Check that we do limit the output to 1 *and* that we respect query
        # order of keys (even though 48 is after 2)
        res = session.execute("SELECT * FROM clicks WHERE userid IN (48, 2) LIMIT 1")
        if Version(self.cluster.version()) >= Version("2.2"):  # Scylla reports 3.0, but has <2.2 behavior.
            assert rows_to_list(res) == [[2, "http://foo.com", 42]], list(res)
        else:
            assert rows_to_list(res) == [[48, "http://foo.com", 42]], list(res)

    def tuple_query_mixed_order_columns_prepare(self, session, *col_order):
        session.execute(
            f"""
            create table foo (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e) )
            WITH CLUSTERING ORDER BY (b {col_order[0]}, c {col_order[1]}, d {col_order[2]}, e {col_order[3]});
        """
        )

        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 2, 0, 0, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 1, 0, 0, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 0, 0, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 2, -1);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 1, -1);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 1, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 0, 2);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 2, 1, -3);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 2, 0, 3);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, -1, 2, 2, 2);""")

    @pytest.mark.single_node
    def test_tuple_query_mixed_order_columns(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "DESC", "ASC", "DESC", "ASC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 1, 2, -1], [0, 0, 1, 1, 1], [0, 0, 2, 1, -3], [0, 0, 2, 0, 3]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test2(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "DESC", "DESC", "DESC", "ASC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 2, 1, -3], [0, 0, 2, 0, 3], [0, 0, 1, 2, -1], [0, 0, 1, 1, 1]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test3(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "ASC", "DESC", "DESC", "ASC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 0, 2, 1, -3], [0, 0, 2, 0, 3], [0, 0, 1, 2, -1], [0, 0, 1, 1, 1], [0, 1, 0, 0, 0], [0, 2, 0, 0, 0]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test4(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "DESC", "ASC", "ASC", "DESC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 1, 1, 1], [0, 0, 1, 2, -1], [0, 0, 2, 0, 3], [0, 0, 2, 1, -3]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test5(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "DESC", "DESC", "DESC", "DESC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 2, 1, -3], [0, 0, 2, 0, 3], [0, 0, 1, 2, -1], [0, 0, 1, 1, 1]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test6(self):
        """CASSANDRA-7281: SELECT on tuple relations are broken for mixed ASC/DESC clustering order
        Test that non mixed columns are still working.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "ASC", "ASC", "ASC", "ASC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 0, 1, 1, 1], [0, 0, 1, 2, -1], [0, 0, 2, 0, 3], [0, 0, 2, 1, -3], [0, 1, 0, 0, 0], [0, 2, 0, 0, 0]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test7(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "DESC", "ASC", "DESC", "ASC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) <= (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 0, 0, 0, 0], [0, 0, 1, 1, -1], [0, 0, 1, 1, 0], [0, 0, 1, 0, 2], [0, -1, 2, 2, 2]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test8(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "ASC", "DESC", "DESC", "ASC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) <= (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, -1, 2, 2, 2], [0, 0, 1, 1, -1], [0, 0, 1, 1, 0], [0, 0, 1, 0, 2], [0, 0, 0, 0, 0]], rows_list

    @pytest.mark.single_node
    def tuple_query_mixed_order_columns_test9(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, "DESC", "ASC", "DESC", "DESC")
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) <= (0, 1, 1, 0);")
        rows_list = rows_to_list(res)
        assert rows_list == [[0, 0, 0, 0, 0], [0, 0, 1, 1, 0], [0, 0, 1, 1, -1], [0, 0, 1, 0, 2], [0, -1, 2, 2, 2]], rows_list

    @pytest.mark.require("#64")
    @pytest.mark.single_node
    def test_simple_tuple_query(self):
        """
        @jira_ticket CASSANDRA-8613
        [Invalid query] message="Clustering columns may not be skipped in multi-column relations.
        They should appear in the PRIMARY KEY order. Got (c, d, e) > (1, 1, 1)"
        """
        session = self.prepare()

        session.execute("create table bard (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e))")

        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 2, 0, 0, 0);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 1, 0, 0, 0);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 0, 0, 0);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 2, 2, 2);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 3, 3, 3);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")

        res = session.execute("SELECT * FROM bard WHERE b=0 AND (c, d, e) > (1, 1, 1) ALLOW FILTERING;")
        assert rows_to_list(res) == [[0, 0, 2, 2, 2], [0, 0, 3, 3, 3]]

    def create_insert_table(self, session):
        session.execute(
            """
            CREATE TABLE clicks (
                userid int,
                url text,
                day int,
                month text,
                year int,
                PRIMARY KEY (userid, url)
            );
        """
        )

        # Inserts
        for _id in range(100):
            for tld in ["com", "org", "net"]:
                session.execute("INSERT INTO clicks (userid, url, day, month, year) VALUES (%i, 'http://foo.%s', 1, 'jan', 2012)" % (_id, tld))

    def test_writetime_functions_query(self, subtests):
        """Test time functions combination and invalid time values issue #5552"""
        rack_num = 3
        nodes_per_rack = 1
        session = self.prepare(nodes=3, rf=3)
        self.create_insert_table(session)
        # run multiple times to check if node is still UP after serving the query
        # see https://github.com/scylladb/scylla/issues/5552
        expected_exceptions = (NoHostAvailable, InvalidRequest)
        matching = "timestamp is out of range|A WRITETIME.*doesn't match the type"
        for tries in range(rack_num * nodes_per_rack):
            with subtests.test("Query qith writetime function non primary key coloumn", i=tries):
                assert_invalid(session=session, query=f"select toDate(max(mintimeuuid(writetime(day)))) from clicks ;", matching=matching, expected=expected_exceptions)

            with subtests.test("Query qith writetime function non primary key text coloumn", i=tries):
                assert_invalid(session=session, query=f"select toDate(max(mintimeuuid(writetime(month)))) from clicks ;", matching=matching, expected=expected_exceptions)

    def test_query_coloumn_timeuuid_with_invalid_values(self, subtests):
        """Test time functions combination and invalid time values issue #5552"""
        invalid_values = (160616626311127, 16061662631112228)
        self.query_coloumn_timeuuid(invalid_values, subtests)

    @pytest.mark.require("#7691")
    def test_query_coloumn_timeuuid_with_invalid_values_issue7691(self, subtests):
        """Test time functions combination and invalid time values issue #7691"""
        invalid_values = (16061662631112223339,)
        self.query_coloumn_timeuuid(invalid_values, subtests)

    def query_coloumn_timeuuid(self, values, subtests):
        def create_insert_table_timeuuid(session):
            session.execute(
                """
                  CREATE TABLE test (
                      k int,
                      t timeuuid,
                      PRIMARY KEY (k, t)
                  )
              """
            )

            for i in range(4):
                session.execute("INSERT INTO test (k, t) VALUES (0, now())")

        session = self.prepare(nodes=3, rf=3)
        create_insert_table_timeuuid(session)

        for value in values:
            with subtests.test("Query mintimeuuid conversion ", v=value):
                query = f"SELECT t FROM test WHERE t > mintimeuuid({value}) ALLOW FILTERING;"
                assert_invalid(session=session, query=query, matching="timestamp is out of range", expected=NoHostAvailable)

    @pytest.mark.single_node
    def test_limit_sparse(self):
        """
        Validate LIMIT option for sparse table in SELECT statements.
        """
        session = self.prepare()
        self.create_insert_table(session)

        # Queries
        # Check we do get as many rows as requested
        res = list(session.execute("SELECT * FROM clicks LIMIT 4"))
        assert len(res) == 4, list(res)

    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_filter_by_counter(self, subtests):
        session = self.prepare()

        session.execute(
            """
                    CREATE TABLE clicks (
                        pk int,
                        ck int,
                        c1 counter,
                        c2 counter,
                        PRIMARY KEY (pk, ck)
                    ) ;
                """
        )

        for i in range(5):
            session.execute(f"UPDATE clicks SET c1 = c1+{i} WHERE pk=0 and ck={i}")

        for i in range(5):
            session.execute(f"UPDATE clicks SET c1 = c1+{i}, c2 = c2+{i} WHERE pk=1 and ck={i}")

        assert_row_count(session=session, table_name="clicks", expected=10)

        with subtests.test("Filter by counter column with equal condition", i=1):
            assert_all(session=session, query="select * from clicks where c1 = 2 ALLOW FILTERING", expected=[[1, 2, 2, 2], [0, 2, 2, None]])

        with subtests.test("Filter by counter column with more condition", i=2):
            assert_all(session=session, query="select * from clicks where c1 > 2 ALLOW FILTERING", expected=[[1, 3, 3, 3], [1, 4, 4, 4], [0, 3, 3, None], [0, 4, 4, None]])

        with subtests.test("Filter by counter column with more and equal condition", i=3):
            assert_all(session=session, query="select * from clicks where c1 > 3 and c2 = 4 ALLOW FILTERING", expected=[[1, 4, 4, 4]])

        with subtests.test('Filter by counter column with "in" and >= condition'):
            assert_all(session=session, query="select * from clicks where c1 in (0, 2) and c2 >= 2 ALLOW FILTERING", expected=[[1, 2, 2, 2]])

        with subtests.test("Filter by all columns including counter column"):
            assert_all(session=session, query="select * from clicks where pk = 1 and ck = 4 and c1 < 3 and c2 = 0 ALLOW FILTERING", expected=[])

        with subtests.test("Verify ALLOW FILTERING error message"):
            assert_invalid(
                session=session,
                query=f"select * from clicks where pk = 1 and ck = 4 and c1 > 3 and c2 = 0",
                matching="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING",
            )

        with subtests.test("Add new counter column"):
            session.execute("ALTER TABLE clicks ADD c3 counter")
            assert_all(session=session, query=f"select * from clicks where pk=1 and ck=1", expected=[[1, 1, 1, 1, None]])
            assert_all(session=session, query="select count(*) from clicks where c3 = 0 ALLOW FILTERING", expected=[[0]])  # c3 is null, which cannot be selected with filtering.

            session.execute(f"UPDATE clicks SET c3 = c3-1 WHERE pk=0 and ck=1")
            assert_all(session=session, query="select * from clicks where c3 = -1 ALLOW FILTERING", expected=[[0, 1, 1, None, -1]])

        with subtests.test("Delete counter column"):
            session.execute("DELETE c1 FROM clicks WHERE pk = 1 and ck = 1")
            assert_all(session=session, query="select * from clicks where c1 = 1 ALLOW FILTERING", expected=[[0, 1, 1, None, -1]])

            assert_all(session=session, query="select * from clicks where c1 = 0 ALLOW FILTERING", expected=[[1, 0, 0, 0, None], [0, 0, 0, None, None]])

    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_counters(self):
        """
        Validate counter support.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
            );
        """
        )

        session.execute("UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[1]], list(res)

        session.execute("UPDATE clicks SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[-3]], list(res)

        session.execute("UPDATE clicks SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[-2]], list(res)

        session.execute("UPDATE clicks SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[-4]], list(res)

    @pytest.mark.single_node
    def test_indexed_with_eq(self):
        """Check that you can query for an indexed column even with a key EQ clause"""
        session = self.prepare()

        # Create
        session.execute(
            """
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """
        )

        session.execute("CREATE INDEX byAge ON users(age)")

        # Inserts
        session.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        session.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        res = session.execute("SELECT firstname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33")
        assert rows_to_list(res) == [], list(res)

        res = session.execute("SELECT firstname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33")
        assert rows_to_list(res) == [["Samwise"]], list(res)

    @pytest.mark.single_node
    def test_select_key_in(self):
        """
        Query for KEY IN (...).
        """
        session = self.prepare()

        # Create
        session.execute(
            """
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """
        )

        # Inserts
        session.execute(
            """
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)
        """
        )
        session.execute(
            """
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, 'Samwise', 'Gamgee', 33)
        """
        )

        # Select
        res = list(
            session.execute(
                """
                SELECT firstname, lastname FROM users
                WHERE userid IN (550e8400-e29b-41d4-a716-446655440000, f47ac10b-58cc-4372-a567-0e02b2c3d479)
        """
            )
        )

        assert len(res) == 2, res

    @pytest.mark.single_node
    def test_exclusive_slice(self):
        """
        Test SELECT respects inclusive and exclusive bounds.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            );
        """
        )

        # Inserts
        for x in range(10):
            session.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        # Queries
        res = list(session.execute("SELECT v FROM test WHERE k = 0"))
        assert len(res) == 10, list(res)

        res = list(session.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c <= 6"))
        assert len(res) == 5 and res[0][0] == 2 and res[len(res) - 1][0] == 6, list(res)

        res = list(session.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6"))
        assert len(res) == 4 and res[0][0] == 3 and res[len(res) - 1][0] == 6, list(res)

        res = list(session.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6"))
        assert len(res) == 4 and res[0][0] == 2 and res[len(res) - 1][0] == 5, list(res)

        res = list(session.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c < 6"))
        assert len(res) == 3 and res[0][0] == 3 and res[len(res) - 1][0] == 5, list(res)

        # With LIMIT
        res = list(session.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2"))
        assert len(res) == 2 and res[0][0] == 3 and res[len(res) - 1][0] == 4, list(res)

        res = list(session.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"))
        assert len(res) == 2 and res[0][0] == 5 and res[len(res) - 1][0] == 4, list(res)

    @pytest.mark.single_node
    def test_in_clause_wide_rows(self):
        """Check IN support for 'wide rows' in SELECT statement"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            );
        """
        )

        # Inserts
        for x in range(10):
            session.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        res = session.execute("SELECT v FROM test1 WHERE k = 0 AND c IN (5, 2, 8)")
        if Version(self.cluster.version()) <= Version("1.2"):
            assert rows_to_list(res) == [[5], [2], [8]], list(res)
        else:
            assert rows_to_list(res) == [[2], [5], [8]], list(res)

        # composites
        session.execute(
            """
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            );
        """
        )

        # Inserts
        for x in range(10):
            session.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, 0, %i, %i)" % (x, x))

        # Check first we don't allow IN everywhere
        if Version(self.cluster.version()) >= Version("2.2"):
            assert_none(session, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")
        else:
            assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")

        res = session.execute("SELECT v FROM test2 WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)")
        assert rows_to_list(res) == [[2], [5], [8]], list(res)

    @pytest.mark.single_node
    def test_order_by(self):
        """Check ORDER BY support in SELECT statement"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            );
        """
        )

        # Inserts
        for x in range(10):
            session.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        res = session.execute("SELECT v FROM test1 WHERE k = 0 ORDER BY c DESC")
        assert rows_to_list(res) == [[x] for x in range(9, -1, -1)], list(res)

        # composites
        session.execute(
            """
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            );
        """
        )

        # Inserts
        for x in range(4):
            for y in range(2):
                session.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, %i)" % (x, y, x * 2 + y))

        # Check first we don't always ORDER BY
        assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 ORDER BY c DESC")
        assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC")
        assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 ORDER BY k DESC")

        res = session.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC")
        assert rows_to_list(res) == [[x] for x in range(7, -1, -1)], list(res)

        res = session.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1")
        assert rows_to_list(res) == [[x] for x in range(8)], list(res)

    @pytest.mark.single_node
    def test_more_order_by(self):
        """More ORDER BY checks (#4160)"""
        session = self.prepare()

        session.execute(
            """
            CREATE COLUMNFAMILY Test (
                row text,
                number int,
                string text,
                PRIMARY KEY (row, number)
            )
        """
        )

        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 1, 'one');")
        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 2, 'two');")
        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 3, 'three');")
        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 4, 'four');")

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number ASC;")
        assert rows_to_list(res) == [[1], [2]], list(res)

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number ASC;")
        assert rows_to_list(res) == [[3], [4]], list(res)

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[2], [1]], list(res)

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[4], [3]], list(res)

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number > 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[4]], list(res)

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number <= 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[3], [2], [1]], list(res)

    @pytest.mark.single_node
    def test_order_by_validation(self):
        """Check we don't allow order by on row key (#4246)"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY (k1, k2)
            )
        """
        )

        q = "INSERT INTO test (k1, k2, v) VALUES (%d, %d, %d)"
        session.execute(q % (0, 0, 0))
        session.execute(q % (1, 1, 1))
        session.execute(q % (2, 2, 2))

        assert_invalid(session, "SELECT * FROM test ORDER BY k2")

    @pytest.mark.single_node
    def test_order_by_with_in(self):
        """Check that order-by works with IN (#4327)"""
        session = self.prepare()
        session.default_fetch_size = None
        session.execute(
            """
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                value varchar,
                PRIMARY KEY (my_id, col1)
            )
        """
        )
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key1', 1, 'a')")
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key2', 3, 'c')")
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key3', 2, 'b')")
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key4', 4, 'd')")

        query = SimpleStatement("SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")
        res = session.execute(query)
        assert rows_to_list(res) == [[1], [2], [3]], list(res)

        query = SimpleStatement("SELECT col1, my_id FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")
        res = session.execute(query)
        assert rows_to_list(res) == [[1, "key1"], [2, "key3"], [3, "key2"]], list(res)

        query = SimpleStatement("SELECT my_id, col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")
        res = session.execute(query)
        assert rows_to_list(res) == [["key1", 1], ["key3", 2], ["key2", 3]], list(res)

    @pytest.mark.single_node
    def test_reversed_comparator(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC);
        """
        )

        # Inserts
        for x in range(10):
            session.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        res = session.execute("SELECT c, v FROM test WHERE k = 0 ORDER BY c ASC")
        assert rows_to_list(res) == [[x, x] for x in range(10)], list(res)

        res = session.execute("SELECT c, v FROM test WHERE k = 0 ORDER BY c DESC")
        assert rows_to_list(res) == [[x, x] for x in range(9, -1, -1)], list(res)

        session.execute(
            """
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v text,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """
        )

        # Inserts
        for x in range(10):
            for y in range(10):
                session.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, '%i%i')" % (x, y, x, y))

        assert_invalid(session, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
        assert_invalid(session, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

        res = session.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC")
        assert rows_to_list(res) == [[x, y, "%i%i" % (x, y)] for x in range(10) for y in range(9, -1, -1)], list(res)

        res = session.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 DESC")
        assert rows_to_list(res) == [[x, y, "%i%i" % (x, y)] for x in range(10) for y in range(9, -1, -1)], list(res)

        res = session.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 ASC")
        assert rows_to_list(res) == [[x, y, "%i%i" % (x, y)] for x in range(9, -1, -1) for y in range(10)], list(res)

        assert_invalid(session, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

    @pytest.mark.single_node
    def test_invalid_old_property(self):
        """Check obsolete properties from CQL2 are rejected"""
        session = self.prepare()

        assert_invalid(session, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp", expected=SyntaxException)

        session.execute("CREATE TABLE test (foo text PRIMARY KEY, c int)")
        assert_invalid(session, "ALTER TABLE test WITH default_validation=int;", expected=SyntaxException)

    @pytest.mark.single_node
    def test_null_support_index(self):
        """Test support for nulls, INDEX is created"""
        self.test_null_support(create_index=True)

    @pytest.mark.single_node
    def test_null_support(self, create_index=False):
        """Test support for nulls"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 set<text>,
                PRIMARY KEY (k, c)
            );
        """
        )

        if create_index:
            session.execute("CREATE INDEX on test(v1)")
            ALLOW_FILTERING = ""
        else:
            ALLOW_FILTERING = "ALLOW FILTERING"

        # Inserts
        session.execute("INSERT INTO test (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})")
        session.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, 1)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 0, None, set(["1", "2"])], [0, 1, 1, None]], list(res)

        session.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, null)")
        session.execute("INSERT INTO test (k, c, v2) VALUES (0, 0, null)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 0, None, None], [0, 1, None, None]], list(res)
        logger.debug(list(res))

        res = session.execute("SELECT * FROM test WHERE k = 0")
        assert rows_to_list(res) == [[0, 0, None, None], [0, 1, None, None]], list(res)

        res = session.execute("SELECT * FROM test WHERE k = null")
        assert rows_to_list(res) == [], list(res)

        # all RHSs are the same
        res = session.execute("SELECT * FROM test WHERE k = 0 AND k = 0")
        assert rows_to_list(res) == [[0, 0, None, None], [0, 1, None, None]], list(res)

        # all RHSs are the same (with multiple pks)
        res = session.execute("SELECT * FROM test WHERE k = 0 AND k = 0 AND c = 1")
        assert rows_to_list(res) == [[0, 1, None, None]], list(res)

        # at least two RHSs are different
        res = session.execute("SELECT * FROM test WHERE k = 0 AND k = 1")
        assert rows_to_list(res) == [], list(res)

        # expect to get empty result for null filtering, CQL is different with SQL
        # Ref: https://github.com/scylladb/scylla/pull/5763#discussion_r405455092
        res = session.execute(f"SELECT * FROM test WHERE k = 0 AND v1 = null {ALLOW_FILTERING}")
        assert rows_to_list(res) == [], list(res)

        res = session.execute(f"SELECT * FROM test WHERE v1 = null {ALLOW_FILTERING}")
        assert rows_to_list(res) == [], list(res)

        assert_invalid(session, "INSERT INTO test (k, c, v2) VALUES (0, 2, {1, null})")
        assert_invalid(session, "INSERT INTO test (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })")

    @pytest.mark.single_node
    def test_unset_value_support(self):
        """Test support for unset value"""
        session = self.prepare(protocol_version=4)

        session.execute(
            """
            CREATE TYPE simple_type (
            number int
            )
        """
        )

        simple_type = namedtuple("simple_type", ("number"))

        session.execute(
            """
            CREATE TABLE test (
                key int,
                i int,
                l list<int>,
                s set<int>,
                m map<int,int>,
                t tuple<int,int>,
                u frozen<simple_type>,
                PRIMARY KEY (key)
            );
        """
        )

        # Insert and verify test data:
        session.execute("INSERT INTO test (key, i, l, s, m, t, u) VALUES (0, 1, [1, 2, 3], {1, 2, 3}, {1: 2}, (1, 2), {number: 1})")
        res = session.execute("SELECT key, i, l, s, m, t, u FROM test")
        assert rows_to_list(res) == [[0, 1, list([1, 2, 3]), set([1, 2, 3]), dict({1: 2}), (1, 2), simple_type(1)]]

        # Make sure unset works with all the types:
        stmt = session.prepare("UPDATE test SET i = ?, l = ?, s = ?, m = ?, t = ?, u = ? WHERE key = ?")
        session.execute(stmt.bind((UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, 0)))
        res = session.execute("SELECT key, i, l, s, m, t, u FROM test")
        assert rows_to_list(res) == [[0, 1, list([1, 2, 3]), set([1, 2, 3]), dict({1: 2}), (1, 2), simple_type(1)]]

        # Mix values and unset values together:
        stmt = session.prepare("UPDATE test SET i = ?, l = ?, s = ?, m = ?, t = ?, u = ? WHERE key = ?")
        session.execute(stmt.bind((2, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, 0)))
        res = session.execute("SELECT key, i, l, s, m, t, u FROM test")
        assert rows_to_list(res) == [[0, 2, list([1, 2, 3]), set([1, 2, 3]), dict({1: 2}), (1, 2), simple_type(1)]]

    def test_nameless_index(self):
        """Test CREATE INDEX without name and validate the index can be dropped"""
        session = self.prepare(nodes=4, rf=1)

        session.execute(
            """
            CREATE TABLE users (
                id text PRIMARY KEY,
                birth_year int,
            )
        """
        )

        session.execute("CREATE INDEX on users(birth_year)")

        session.execute("INSERT INTO users (id, birth_year) VALUES ('Tom', 42)")
        session.execute("INSERT INTO users (id, birth_year) VALUES ('Paul', 24)")
        session.execute("INSERT INTO users (id, birth_year) VALUES ('Bob', 42)")

        # Because in Scylla writes to the index are asynchronous, if we want
        # to verify the result of SELECT in this test (but why do we? why is
        # this check even part of this test?), we need to allow retry.
        assert_all(session, "SELECT id FROM users WHERE birth_year = 42", [["Tom"], ["Bob"]], num_attempts=3)

        session.execute("DROP INDEX users_birth_year_idx")

        assert_invalid(session, "SELECT id FROM users WHERE birth_year = 42")

    @pytest.mark.single_node
    def test_deletion(self):
        """Test simple deletion and in particular check for #4193 bug"""

        session = self.prepare()

        session.execute(
            """
            CREATE TABLE testcf (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id)
            );
        """
        )

        q = "INSERT INTO testcf (username, id, name, stuff) VALUES ('%s', %d, '%s', '%s');"
        row1 = ("abc", 2, "rst", "some value")
        row2 = ("abc", 4, "xyz", "some other value")
        session.execute(q % row1)
        session.execute(q % row2)

        res = session.execute("SELECT * FROM testcf")
        assert rows_to_list(res) == [list(row1), list(row2)], list(res)

        session.execute("DELETE FROM testcf WHERE username='abc' AND id=2")

        res = session.execute("SELECT * FROM testcf")
        assert rows_to_list(res) == [list(row2)], list(res)

        # Compact case
        session.execute(
            """
            CREATE TABLE testcf2 (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id, name)
            );
        """
        )

        q = "INSERT INTO testcf2 (username, id, name, stuff) VALUES ('%s', %d, '%s', '%s');"
        row1 = ("abc", 2, "rst", "some value")
        row2 = ("abc", 4, "xyz", "some other value")
        session.execute(q % row1)
        session.execute(q % row2)

        res = session.execute("SELECT * FROM testcf2")
        assert rows_to_list(res) == [list(row1), list(row2)], list(res)

        # Won't be allowed until #3708 is in
        if Version(self.cluster.version()) < Version("1.2"):
            assert_invalid(session, "DELETE FROM testcf2 WHERE username='abc' AND id=2")

    @pytest.mark.single_node
    def test_count(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE events (
                kind text,
                time int,
                value1 int,
                value2 int,
                PRIMARY KEY(kind, time)
            )
        """
        )

        full = "INSERT INTO events (kind, time, value1, value2) VALUES ('ev1', %d, %d, %d)"
        no_v2 = "INSERT INTO events (kind, time, value1) VALUES ('ev1', %d, %d)"

        session.execute(full % (0, 0, 0))
        session.execute(full % (1, 1, 1))
        session.execute(no_v2 % (2, 2))
        session.execute(full % (3, 3, 3))
        session.execute(no_v2 % (4, 4))
        session.execute("INSERT INTO events (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")

        res = session.execute("SELECT COUNT(*) FROM events WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[5]], list(res)

        res = session.execute("SELECT COUNT(1) FROM events WHERE kind IN ('ev1', 'ev2') AND time=0")
        assert rows_to_list(res) == [[2]], list(res)

    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_reserved_keyword(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test1 (
                key text PRIMARY KEY,
                count counter,
            )
        """
        )

        assert_invalid(session, "CREATE TABLE test2 ( select text PRIMARY KEY, x int)", expected=SyntaxException)

    @pytest.mark.single_node
    def test_identifier(self):
        session = self.prepare()

        # Test case insensitivity
        session.execute("CREATE TABLE test1 (key_23 int PRIMARY KEY, CoLuMn int)")

        # Should work
        session.execute("INSERT INTO test1 (Key_23, Column) VALUES (0, 0)")
        session.execute("INSERT INTO test1 (KEY_23, COLUMN) VALUES (0, 0)")

        # invalid due to repeated identifiers
        assert_invalid(session, "INSERT INTO test1 (key_23, column, column) VALUES (0, 0, 0)")
        assert_invalid(session, "INSERT INTO test1 (key_23, column, COLUMN) VALUES (0, 0, 0)")
        assert_invalid(session, "INSERT INTO test1 (key_23, key_23, column) VALUES (0, 0, 0)")
        assert_invalid(session, "INSERT INTO test1 (key_23, KEY_23, column) VALUES (0, 0, 0)")

        # Reserved keywords
        assert_invalid(session, "CREATE TABLE test1 (select int PRIMARY KEY, column int)", expected=SyntaxException)

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_table(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """
        )

        session.execute(
            """
            CREATE TABLE test2 (
                k int,
                name int,
                value int,
                PRIMARY KEY(k, name)
            ) WITH COMPACT STORAGE
        """
        )

        session.execute(
            """
            CREATE TABLE test3 (
                k int,
                c int,
                PRIMARY KEY (k),
            )
        """
        )

        # existing table
        assert_invalid(session, "CREATE TABLE test3 (k int PRIMARY KEY, c int)", expected=AlreadyExists, matching=re.escape("ks.test3"))
        # repeated column
        assert_invalid(session, "CREATE TABLE test4 (k int PRIMARY KEY, c int, k text)", matching="code=2200")

        # compact storage limitations
        assert_invalid(session, "CREATE TABLE test4 (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE", expected=SyntaxException)

        session.execute("DROP TABLE test1")
        session.execute("TRUNCATE test2")

        session.execute(
            """
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c1 int,
                c2 int,
            )
        """
        )

    @pytest.mark.single_node
    def test_batch(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE users (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """
        )

        query = SimpleStatement(
            """
            BEGIN BATCH
                INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM users WHERE userid = 'user1';
            APPLY BATCH;
        """,
            consistency_level=ConsistencyLevel.QUORUM,
        )
        session.execute(query)

    @pytest.mark.single_node
    def test_token_range(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int,
                v int
            )
        """
        )

        c = 100
        for i in range(c):
            session.execute("INSERT INTO test (k, c, v) VALUES (%d, %d, %d)" % (i, i, i))

        rows = session.execute("SELECT k FROM test")
        inOrder = [x[0] for x in rows]
        assert len(inOrder) == c, "Expecting %d elements, got %d" % (c, len(inOrder))

        if Version(self.cluster.version()) < Version("1.2"):
            session.execute("SELECT k FROM test WHERE token(k) >= 0")
        else:
            min_token = -(2**63)
        res = list(session.execute("SELECT k FROM test WHERE token(k) >= %d" % min_token))
        assert len(res) == c, f"{res!s} [all: {inOrder!s}]"

        # make sure comparing tokens to int literals doesn't fall down
        session.execute("SELECT k FROM test WHERE token(k) >= 0")

        res = session.execute("SELECT k FROM test WHERE token(k) >= token(%d) AND token(k) < token(%d)" % (inOrder[32], inOrder[65]))
        assert rows_to_list(res) == [[inOrder[x]] for x in range(32, 65)], f"{res!s} [all: {inOrder!s}]"

    @pytest.mark.single_node
    def test_table_options(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int
            ) WITH comment = 'My comment'
               AND read_repair_chance = 0.5
               AND dclocal_read_repair_chance = 0.5
               AND gc_grace_seconds = 4
               AND bloom_filter_fp_chance = 0.01
               AND compaction = { 'class' : 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb' : 10 }
               AND compression = { 'sstable_compression' : '' }
               AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
        """
        )

        session.execute(
            """
            ALTER TABLE test
            WITH comment = 'other comment'
             AND read_repair_chance = 0.3
             AND dclocal_read_repair_chance = 0.3
             AND gc_grace_seconds = 100
             AND bloom_filter_fp_chance = 0.1
             AND compaction = { 'class' : 'SizeTieredCompactionStrategy',
                                'min_sstable_size' : 42 }
             AND compression = { 'sstable_compression' : 'SnappyCompressor' }
             AND caching = '{"keys":"NONE","rows_per_partition":"ALL"}'
        """
        )

    @pytest.mark.single_node
    def test_timestamp_and_ttl(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                c text,
                d text
            )
        """
        )

        session.execute("INSERT INTO test (k, c) VALUES (1, 'test')")
        session.execute("INSERT INTO test (k, c) VALUES (2, 'test') USING TTL 400")

        res = list(session.execute("SELECT k, c, writetime(c), ttl(c) FROM test"))
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], int)
            if r[0] == 1:
                assert r[3] is None, res
            else:
                assert isinstance(r[3], int), res

        # wrap writetime(), ttl() in other functions (test for CASSANDRA-8451)
        res = list(session.execute("SELECT k, c, blobAsBigint(bigintAsBlob(writetime(c))), ttl(c) FROM test"))
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], int)
            if r[0] == 1:
                assert r[3] is None, res
            else:
                assert isinstance(r[3], int), res

        res = list(session.execute("SELECT k, c, writetime(c), blobAsInt(intAsBlob(ttl(c))) FROM test"))
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], int)
            if r[0] == 1:
                assert r[3] is None, res
            else:
                assert isinstance(r[3], int), res

        assert_invalid(session, "SELECT k, c, writetime(k) FROM test")

        res = list(session.execute("SELECT k, d, writetime(d) FROM test WHERE k = 1"))
        assert rows_to_list(res) == [[1, None, None]]

    @pytest.mark.single_node
    def test_no_range_ghost(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """
        )

        for k in range(5):
            session.execute("INSERT INTO test (k, v) VALUES (%d, 0)" % k)

        unsorted_res = list(session.execute("SELECT k FROM test"))
        res = sorted(unsorted_res)
        assert rows_to_list(res) == [[k] for k in range(5)], res

        session.execute("DELETE FROM test WHERE k=2")

        unsorted_res = list(session.execute("SELECT k FROM test"))
        res = sorted(unsorted_res)
        assert rows_to_list(res) == [[k] for k in range(5) if k != 2], list(res)

        # Example from #3505
        session.execute("CREATE KEYSPACE ks1 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };")
        session.execute("USE ks1")
        session.execute(
            """
            CREATE COLUMNFAMILY users (
                KEY varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                birth_year bigint)
        """
        )

        session.execute("INSERT INTO users (KEY, password) VALUES ('user1', 'ch@ngem3a')")
        session.execute("UPDATE users SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'")
        res = session.execute("SELECT * FROM users WHERE KEY='user1'")
        assert rows_to_list(res) == [["user1", 1980, "m", "ch@ngem3a"]], list(res)

        session.execute("TRUNCATE users")

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [], list(res)

        res = session.execute("SELECT * FROM users WHERE KEY='user1'")
        assert rows_to_list(res) == [], list(res)

    @pytest.mark.single_node
    def test_undefined_column_handling(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            )
        """
        )

        session.execute("INSERT INTO test (k, v1, v2) VALUES (0, 0, 0)")
        session.execute("INSERT INTO test (k, v1) VALUES (1, 1)")
        session.execute("INSERT INTO test (k, v1, v2) VALUES (2, 2, 2)")

        res = session.execute("SELECT v2 FROM test")
        assert rows_to_list(res) == [[None], [0], [2]], list(res)

        res = session.execute("SELECT v2 FROM test WHERE k = 1")
        assert rows_to_list(res) == [[None]], list(res)

    def test_quorum_with_null(self):
        """Test Quorum with null:
        creating table  where
        node1 - row looks like: (pk1, 123, null, null, 456)
        node2 - row looks like: (pk1, 123 null, null, 789)
        node3 - row looks like: (pk1, 123 null, null, 789)
        query and cause read_reapir and make sure that row1 on node1 (stop the 2 others) is fixed correctly.
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        time.sleep(0.2)
        node1 = self.cluster.nodelist()[1]
        session = self.patient_cql_connection(node1, row_factory=dict_factory)
        with session:
            create_ks(session, "ks", rf=3)
            session.execute(
                """
                CREATE TABLE test1 (
                    k int,
                    c1 int,
                    v1 int,
                    v2 int,
                    v3 int,
                    PRIMARY KEY (k, c1)
                );
            """
            )
            time.sleep(1)
            session.execute("INSERT INTO test1 (k, c1,v3) VALUES (1, 123,456)")
            time.sleep(0.5)
            cluster.flush()
            node_to_stop = self.cluster.nodelist()[0]
            node_to_stop.stop(wait_other_notice=True)
            session.execute("INSERT INTO test1 (k, c1,v3) VALUES (1, 123,768)")
            time.sleep(0.5)
            updated_nodes = [self.cluster.nodelist()[1], self.cluster.nodelist()[2]]
        for node in updated_nodes:
            node.stop(wait_other_notice=True)
        query = "SELECT * FROM ks.test1 WHERE k = 1 and c1=123"
        node_to_stop.start(wait_other_notice=True)
        session = self.patient_cql_connection(node_to_stop, row_factory=dict_factory)
        simple_query = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)
        res = session.execute(simple_query).current_rows[0]
        assert res["v3"] == 456, f"before repair expected v3=456, actual {res['v3']}"
        for node in updated_nodes:
            node.start(wait_other_notice=True)
        node_to_stop.repair()
        for node in updated_nodes:
            node.stop(wait_other_notice=True)
        res = session.execute(simple_query).current_rows[0]
        assert res["v3"] == 768, f"after repair expected v3=768, actual {res['v3']}"

    def test_range_tombstones(self):
        """Test deletion by 'composite prefix' (range tombstones)"""
        cluster = self.cluster

        # Uses 3 nodes just to make sure RowMutation are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
                CREATE TABLE test1 (
                    k int,
                    c1 int,
                    c2 int,
                    v1 int,
                    v2 int,
                    PRIMARY KEY (k, c1, c2)
                );
            """
        )
        time.sleep(1)

        rows = 5
        col1 = 2
        col2 = 2
        cpr = col1 * col2
        for i in range(rows):
            for j in range(col1):
                for k in range(col2):
                    n = (i * cpr) + (j * col2) + k
                    session.execute("INSERT INTO test1 (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)" % (i, j, k, n, n))

        for i in range(rows):
            res = session.execute("SELECT v1, v2 FROM test1 where k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in range(i * cpr, (i + 1) * cpr)], list(res)

        for i in range(rows):
            session.execute("DELETE FROM test1 WHERE k = %d AND c1 = 0" % i)

        for i in range(rows):
            res = session.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in range(i * cpr + col1, (i + 1) * cpr)], list(res)

        self.cluster.flush()
        time.sleep(0.2)

        for i in range(rows):
            res = session.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in range(i * cpr + col1, (i + 1) * cpr)], list(res)

    @pytest.mark.single_node
    def test_range_tombstones_compaction(self):
        """Test deletion by 'composite prefix' (range tombstones) with compaction"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 text,
                PRIMARY KEY (k, c1, c2)
            );
        """
        )

        for c1 in range(4):
            for c2 in range(2):
                session.execute("INSERT INTO test1 (k, c1, c2, v1) VALUES (0, %d, %d, '%s')" % (c1, c2, "%i%i" % (c1, c2)))

        self.cluster.flush()

        session.execute("DELETE FROM test1 WHERE k = 0 AND c1 = 1")

        self.cluster.flush()
        self.cluster.compact()

        res = session.execute("SELECT v1 FROM test1 WHERE k = 0")
        assert rows_to_list(res) == [["%i%i" % (c1, c2)] for c1 in range(4) for c2 in range(2) if c1 != 1], list(res)

    @pytest.mark.single_node
    def test_delete_row(self):
        """Test deletion of rows"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                 k int,
                 c1 int,
                 c2 int,
                 v1 int,
                 v2 int,
                 PRIMARY KEY (k, c1, c2)
            );
        """
        )

        q = "INSERT INTO test (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)"
        session.execute(q % (0, 0, 0, 0, 0))
        session.execute(q % (0, 0, 1, 1, 1))
        session.execute(q % (0, 0, 2, 2, 2))
        session.execute(q % (0, 1, 0, 3, 3))

        session.execute("DELETE FROM test WHERE k = 0 AND c1 = 0 AND c2 = 0")
        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 3, res

    @pytest.mark.single_node
    def test_range_query_2ndary(self):
        """Test range queries with 2ndary indexes (#4257)"""
        session = self.prepare()

        session.execute("CREATE TABLE indextest (id int primary key, row int, setid int);")
        session.execute("CREATE INDEX indextest_setid_idx ON indextest (setid)")

        q = "INSERT INTO indextest (id, row, setid) VALUES (%d, %d, %d);"
        session.execute(q % (0, 0, 0))
        session.execute(q % (1, 1, 0))
        session.execute(q % (2, 2, 0))
        session.execute(q % (3, 3, 0))

        assert_invalid(session, "SELECT * FROM indextest WHERE setid = 0 AND row < 1;")
        res = session.execute("SELECT * FROM indextest WHERE setid = 0 AND row < 1 ALLOW FILTERING;")
        assert rows_to_list(res) == [[0, 0, 0]], list(res)

    @pytest.mark.single_node
    def test_compression_option_validation(self):
        """Check for unknown compression parameters options (#4266)"""
        session = self.prepare()

        assert_invalid(
            session,
            """
          CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
          WITH compression_parameters:sstable_compressor = 'DeflateCompressor';
        """,
            expected=SyntaxException,
        )

        if Version(self.cluster.version()) >= Version("1.2"):
            assert_invalid(
                session,
                """
              CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
              WITH compression = { 'sstable_compressor' : 'DeflateCompressor' };
            """,
                expected=ConfigurationException,
            )

    def test_keyspace_creation_options(self):
        """Check one can use arbitrary name for datacenter when creating keyspace (#4278)"""
        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch"})
        cluster.new_node(1, data_center="us-east")
        cluster.new_node(2, data_center="us-west")
        cluster.start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        # we just want to make sure the following is valid
        if Version(self.cluster.version()) >= Version("1.2"):
            session.execute(
                """
                CREATE KEYSPACE Foo
                    WITH replication = { 'class' : 'NetworkTopologyStrategy',
                                         'us-east' : 1,
                                         'us-west' : 1 };
            """
            )
        else:
            session.execute(
                """
                CREATE KEYSPACE Foo
                    WITH strategy_class='NetworkTopologyStrategy'
                     AND strategy_options:"us-east"=1
                     AND strategy_options:"us-west"=1;
            """
            )

    @pytest.mark.single_node
    def test_set(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE user (
                fn text,
                ln text,
                tags set<text>,
                PRIMARY KEY (fn, ln)
            )
        """
        )

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(q % "tags = tags + { 'foo' }")
        session.execute(q % "tags = tags + { 'bar' }")
        session.execute(q % "tags = tags + { 'foo' }")
        session.execute(q % "tags = tags + { 'foobar' }")
        session.execute(q % "tags = tags - { 'bar' }")

        res = session.execute("SELECT tags FROM user")
        assert rows_to_list(res) == [[set(["foo", "foobar"])]], list(res)

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        session.execute(q % "tags = { 'a', 'c', 'b' }")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[set(["a", "b", "c"])]], list(res)

        time.sleep(0.01)

        session.execute(q % "tags = { 'm', 'n' }")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[set(["m", "n"])]], list(res)

        session.execute("DELETE tags['m'] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[set(["n"])]], list(res)

        session.execute("DELETE tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        if Version(self.cluster.version()) <= Version("1.2"):
            assert rows_to_list(res) == [None], list(res)
        else:
            assert rows_to_list(res) == [], list(res)

    @pytest.mark.single_node
    def test_map(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE user (
                fn text,
                ln text,
                m map<text, int>,
                PRIMARY KEY (fn, ln)
            )
        """
        )

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(q % "m['foo'] = 3")
        session.execute(q % "m['bar'] = 4")
        session.execute(q % "m['woot'] = 5")
        session.execute(q % "m['bar'] = 6")
        session.execute("DELETE m['foo'] FROM user WHERE fn='Tom' AND ln='Bombadil'")

        res = session.execute("SELECT m FROM user")
        assert rows_to_list(res) == [[{"woot": 5, "bar": 6}]], list(res)

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        session.execute(q % "m = { 'a' : 4 , 'c' : 3, 'b' : 2 }")
        res = session.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[{"a": 4, "b": 2, "c": 3}]], list(res)

        time.sleep(0.01)

        # Check we correctly overwrite
        session.execute(q % "m = { 'm' : 4 , 'n' : 1, 'o' : 2 }")
        res = session.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[{"m": 4, "n": 1, "o": 2}]], list(res)

        session.execute(q % "m = {}")
        res = session.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        if Version(self.cluster.version()) <= Version("1.2"):
            assert rows_to_list(res) == [None], list(res)
        else:
            assert rows_to_list(res) == [], list(res)

    @pytest.mark.single_node
    def test_list(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE user (
                fn text,
                ln text,
                tags list<text>,
                PRIMARY KEY (fn, ln)
            )
        """
        )

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(q % "tags = tags + [ 'foo' ]")
        session.execute(q % "tags = tags + [ 'bar' ]")
        session.execute(q % "tags = tags + [ 'foo' ]")
        session.execute(q % "tags = tags + [ 'foobar' ]")

        res = session.execute("SELECT tags FROM user")
        assert rows_to_list(res) == [[["foo", "bar", "foo", "foobar"]]]

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        session.execute(q % "tags = [ 'a', 'c', 'b', 'c' ]")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[["a", "c", "b", "c"]]]

        session.execute(q % "tags = [ 'm', 'n' ] + tags")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[["m", "n", "a", "c", "b", "c"]]]

        session.execute(q % "tags[2] = 'foo', tags[4] = 'bar'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[["m", "n", "foo", "c", "bar", "c"]]]

        session.execute("DELETE tags[2] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[["m", "n", "c", "bar", "c"]]]

        session.execute(q % "tags = tags - [ 'bar' ]")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[["m", "n", "c", "c"]]]

    @pytest.mark.single_node
    def test_list_prefetch_with_static_column(self):
        # Explits https://github.com/scylladb/scylla/issues/903
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE user (
                fn text,
                ln text,
                static_tags list<text> static,
                tags list<text>,
                PRIMARY KEY (fn, ln)
            )
        """
        )

        update_q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        select_q = "SELECT %s FROM user WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(update_q % "tags = tags + [ 'a', 'b', 'c', 'b' ]")
        session.execute("update user set static_tags = static_tags + [ 'a', 'b', 'c', 'b' ] where fn='Tom'")

        session.execute(update_q % "tags = tags - [ 'b' ]")
        res = session.execute(select_q % "tags")
        assert rows_to_list(res) == [[["a", "c"]]]
        res = session.execute("select static_tags from user where fn='Tom'")
        assert rows_to_list(res) == [[["a", "b", "c", "b"]]]

        session.execute("update user set static_tags = static_tags - [ 'b' ] where fn='Tom'")
        res = session.execute("select static_tags from user where fn='Tom'")
        assert rows_to_list(res) == [[["a", "c"]]]

        session.execute("update user set static_tags[1] = 'b' where fn='Tom'")
        res = session.execute("select static_tags from user where fn='Tom'")
        assert rows_to_list(res) == [[["a", "b"]]]

    @pytest.mark.single_node
    def test_multi_collection(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE foo(
                k uuid PRIMARY KEY,
                L list<int>,
                M map<text, int>,
                S set<int>
            );
        """
        )

        # since in shard-aware scylla-driver we have connection to each shard,
        # two statements appending to L can happen at the same time, hitting
        # https://github.com/scylladb/scylla/issues/6846
        # hence the ordering of L isn't determined for now
        # for solving this, we changing those two into prepare statements,
        # so they would both land on the same shard
        statement1 = session.prepare("UPDATE ks.foo SET L = [1, 3, 5] WHERE k = ?")
        statement2 = session.prepare("UPDATE ks.foo SET L = L + [7, 11, 13] WHERE k =?")

        session.execute(statement1, parameters=(UUID("b017f48f-ae67-11e1-9096-005056c00008"),))
        session.execute(statement2, parameters=(UUID("b017f48f-ae67-11e1-9096-005056c00008"),))
        session.execute("UPDATE ks.foo SET S = {1, 3, 5} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET S = S + {7, 11, 13} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET M = {'foo': 1, 'bar' : 3} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET M = M + {'foobar' : 4} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")

        res = session.execute("SELECT L, M, S FROM foo WHERE k = b017f48f-ae67-11e1-9096-005056c00008")
        assert rows_to_list(res) == [[[1, 3, 5, 7, 11, 13], OrderedDict([("bar", 3), ("foo", 1), ("foobar", 4)]), sortedset([1, 3, 5, 7, 11, 13])]]

    @pytest.mark.single_node
    def test_range_query(self):
        """Range test query from #4372"""
        session = self.prepare()

        session.execute("CREATE TABLE test (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )")

        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5');")

        res = session.execute("SELECT a, b, c, d, e, f FROM test WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2;")
        assert rows_to_list(res) == [[1, 1, 1, 1, 2, "2"], [1, 1, 1, 1, 3, "3"], [1, 1, 1, 1, 5, "5"]], list(res)

    @pytest.mark.single_node
    def test_update_type(self):
        """Test altering the type of a column, including the one in the primary key (#4041)"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k text,
                c text,
                s set<text>,
                v text,
                PRIMARY KEY (k, c)
            )
        """
        )

        req = "INSERT INTO test (k, c, v, s) VALUES ('%s', '%s', '%s', {'%s'})"
        # using utf8 character so that we can see the transition to BytesType
        session.execute(req % ("ɸ", "ɸ", "ɸ", "ɸ"))

        session.execute("SELECT * FROM test")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["ɸ", "ɸ", set(["ɸ"]), "ɸ"]], list(res)

        session.execute("ALTER TABLE test ALTER v TYPE blob")
        res = session.execute("SELECT * FROM test")
        # the last should not be utf8 but a raw string
        assert rows_to_list(res) == [["ɸ", "ɸ", set(["ɸ"]), "ɸ".encode()]], list(res)

        session.execute("ALTER TABLE test ALTER k TYPE blob")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["ɸ".encode(), "ɸ", set(["ɸ"]), "ɸ".encode()]], list(res)

        session.execute("ALTER TABLE test ALTER c TYPE blob")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["ɸ".encode(), "ɸ".encode(), set(["ɸ"]), "ɸ".encode()]], list(res)

        if Version(self.cluster.version()) < Version("2.1"):
            assert_invalid(session, "ALTER TABLE test ALTER s TYPE set<blob>", expected=ConfigurationException)
        else:
            session.execute("ALTER TABLE test ALTER s TYPE set<blob>")
            res = session.execute("SELECT * FROM test")
            assert rows_to_list(res) == [["ɸ".encode(), "ɸ".encode(), set(["ɸ".encode()]), "ɸ".encode()]], list(res)

    @pytest.mark.single_node
    def test_composite_row_key(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k1 int,
                k2 int,
                c int,
                v int,
                PRIMARY KEY ((k1, k2), c)
            )
        """
        )

        req = "INSERT INTO test (k1, k2, c, v) VALUES (%d, %d, %d, %d)"
        for i in range(4):
            session.execute(req % (0, i, i, i))

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], list(res)

        res = session.execute("SELECT * FROM test WHERE k1 = 0 and k2 IN (1, 3)")
        assert rows_to_list(res) == [[0, 1, 1, 1], [0, 3, 3, 3]], list(res)

        assert_invalid(session, "SELECT * FROM test WHERE k2 = 3")

        v = Version(self.cluster.version())
        if v < Version("2.2.0"):
            # still failed in 3.0: https://github.com/scylladb/scylla/issues/1735
            assert_invalid(session, "SELECT * FROM test WHERE k1 IN (0, 1) and k2 = 3")

        res = session.execute("SELECT * FROM test WHERE token(k1, k2) = token(0, 1)")
        assert rows_to_list(res) == [[0, 1, 1, 1]], list(res)

        res = session.execute("SELECT * FROM test WHERE token(k1, k2) > " + str(-((2**63) - 1)))
        assert rows_to_list(res) == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], list(res)

    @pytest.mark.single_node
    def test_row_existence(self):
        """Check the semantic of CQL row existence (part of #4361)"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c)
            )
        """
        )

        session.execute("INSERT INTO test (k, c, v1, v2) VALUES (1, 1, 1, 1)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[1, 1, 1, 1]], list(res)

        assert_invalid(session, "DELETE c FROM test WHERE k = 1 AND c = 1")

        session.execute("DELETE v2 FROM test WHERE k = 1 AND c = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[1, 1, 1, None]], list(res)

        session.execute("DELETE v1 FROM test WHERE k = 1 AND c = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[1, 1, None, None]], list(res)

        session.execute("DELETE FROM test WHERE k = 1 AND c = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [], list(res)

        session.execute("INSERT INTO test (k, c) VALUES (2, 2)")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[2, 2, None, None]], list(res)

    @pytest.mark.single_node
    def test_only_pk(self):
        """Check table with only a PK (#4361)"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                PRIMARY KEY (k, c)
            )
        """
        )

        q = "INSERT INTO test (k, c) VALUES (%d, %d)"
        for k in range(2):
            for c in range(2):
                session.execute(q % (k, c))

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[x, y] for x in range(1, -1, -1) for y in range(2)], list(res)

        # Check for dense tables too
        session.execute(
            """
            CREATE TABLE test2 (
                k int,
                c int,
                PRIMARY KEY (k, c)
            )
        """
        )

        q = "INSERT INTO test2 (k, c) VALUES (%d, %d)"
        for k in range(2):
            for c in range(2):
                session.execute(q % (k, c))

        res = session.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [[x, y] for x in range(1, -1, -1) for y in range(2)], list(res)

    @pytest.mark.single_node
    def test_date(self):
        """Check dates are correctly recognized and validated"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timestamp
            )
        """
        )

        session.execute("INSERT INTO test (k, t) VALUES (0, '2011-02-03')")
        assert_invalid(session, "INSERT INTO test (k, t) VALUES (0, '2011-42-42')")

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

    def test_min_and_max_on_sets_and_udt(self):
        """
        Test min() max() on collections with various element types.
        Covers testing of PR: https://github.com/scylladb/scylla/pull/6801/
        that fixes: https://github.com/scylladb/scylla/issues/6768
        see explanation of comparing sets and UDT in a lexicographical way here:
        https://github.com/scylladb/scylla/blob/cb4c874bf944a2133226261e925ec32895b54fe5/types.hh#L49
        """

        session = self.prepare(ordered=True)
        session.execute("CREATE TYPE udt (first text, second int, third int)")
        session.execute(
            """
                CREATE TABLE sets (
                    id int PRIMARY KEY,
                    s1 set<int>,
                    s2 set<blob>,
                    my_asc set<ascii>,
                    my_type udt
                )
            """
        )

        session.execute("INSERT INTO sets (id, my_asc) VALUES (1, {'hi', 'im', 'ascii'})")
        session.execute("INSERT INTO sets (id, my_asc) VALUES (2, {'im', 'ascii', 'too'})")
        # test min and max on set of ascii type
        res_max = session.execute("select max(my_asc) FROM sets")
        res_max_rows = rows_to_list(res_max)
        res_min = session.execute("select min(my_asc) FROM sets")
        res_min_rows = rows_to_list(res_min)
        assert res_max_rows[0][0] == {"ascii", "im", "too"}, res_max_rows
        assert res_min_rows[0][0] == {"ascii", "hi", "im"}, res_min_rows

        session.execute("INSERT INTO sets (id, s1, s2, my_type) VALUES (1, {-1, 1}, {0xff, 0x01}, {first:'a', second: 2, third: 3})")
        session.execute("INSERT INTO sets (id, s1, s2, my_type) VALUES (2, {-2, 2}, {0xfe, 0x02}, {first:'b', second: 5, third: 6})")

        # test min and max on UDT
        res_max = session.execute("select max(my_type) FROM sets")
        res_max_rows = rows_to_list(res_max)
        res_min = session.execute("select min(my_type) FROM sets")
        res_min_rows = rows_to_list(res_min)
        assert str(res_max_rows[0][0]) == "udt(first='b', second=5, third=6)", res_max_rows
        assert str(res_min_rows[0][0]) == "udt(first='a', second=2, third=3)", res_min_rows

        # test min and max on int and blob sets
        res = session.execute("select max(s1), max(s2) FROM sets")
        res_rows = rows_to_list(res)
        logger.debug(f"res_rows: {res_rows}")
        assert res_rows == [[{-1, 1}, {b"\x02", b"\xfe"}]], res_rows

    def test_aggregate_and_simple_selection_together(self):
        session = self.prepare(ordered=True)
        session.execute(
            """
                CREATE TABLE together (
                    a int,
                    b int,
                    c int,
                    PRIMARY KEY ((a), c)
                )
            """
        )
        session.execute("INSERT INTO together (a, b, c) VALUES (1, 2, 3)")
        session.execute("INSERT INTO together (a, b, c) VALUES (2, 4, 6)")
        session.execute("INSERT INTO together (a, b, c) VALUES (3, 6, 9)")
        session.execute("INSERT INTO together (a, b, c) VALUES (3, 8, 10)")
        res = session.execute("SELECT sum(c), avg(b), min(c), a  FROM together WHERE b>2 ALLOW FILTERING")
        assert rows_to_list(res) == [[25, 6, 6, 2]], list(res)
        res = session.execute("SELECT count(c), max(b)  FROM together WHERE a = 3 ")
        assert rows_to_list(res) == [[2, 8]], list(res)

    def test_partition_key_as_secondary_index(self):
        session = self.prepare(ordered=True)
        session.execute(
            """
                CREATE TABLE test_index (
                    a BIGINT,
                    b BIGINT,
                    c BIGINT,
                    PRIMARY KEY ((a, b))
                )
            """
        )
        session.execute("CREATE INDEX ON test_index(a)")
        session.execute("INSERT INTO test_index (a, b, c) VALUES (0, 2, 1)")
        session.execute("INSERT INTO test_index (a, b, c) VALUES (1, 2, 3)")
        session.execute("INSERT INTO test_index (a, b, c) VALUES (2, 2, 4)")

        res = session.execute("SELECT * FROM test_index WHERE a>0 AND b=2 ALLOW FILTERING")
        rows_set = get_rows_set_from_res(res)
        assert rows_set == {(2, 2, 4), (1, 2, 3)}, rows_set
        res = session.execute("SELECT b,c FROM test_index WHERE a>=2 ALLOW FILTERING")
        rows_set = get_rows_set_from_res(res)
        assert rows_set == {(2, 4)}, rows_set

    def test_restricted_column_not_in_select_clause(self):
        session = self.prepare(ordered=True)
        session.execute(
            """
                    CREATE TABLE test_index (
                        a BIGINT,
                        b BIGINT,
                        c BIGINT,
                        d INT,
                        e INT,
                        PRIMARY KEY ((a, b),c)
                    )
                """
        )
        session.execute("CREATE INDEX ON test_index(d)")
        session.execute("INSERT INTO test_index (a, b, c, d, e) VALUES (1, 2, 3, 4, 5)")
        session.execute("INSERT INTO test_index (a, b, c, d, e) VALUES (11, 22, 33, 44, 55)")

        res = session.execute("select c,e from ks.test_index where d = 44 ALLOW FILTERING")
        rows_list = rows_to_list(res)
        assert rows_list == [[33, 55]], rows_list
        res = session.execute("select a from ks.test_index where d > 43 ALLOW FILTERING")
        rows_list = rows_to_list(res)
        assert rows_list == [[11]], rows_list

    @pytest.mark.single_node
    def test_composite_index_with_pk(self):
        session = self.prepare()
        session.execute(
            """
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content text,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """
        )

        session.execute("CREATE INDEX ON blogs(author)")

        req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', '%s')"
        session.execute(req % (1, 0, 0, "foo", "bar1"))
        session.execute(req % (1, 0, 1, "foo", "bar2"))
        session.execute(req % (2, 1, 0, "foo", "baz"))
        session.execute(req % (3, 0, 1, "gux", "qux"))

        res = session.execute("SELECT blog_id, content FROM blogs WHERE author='foo'")
        assert rows_to_list(res) == [[1, "bar1"], [1, "bar2"], [2, "baz"]], list(res)

        res = session.execute("SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [[2, "baz"]], list(res)

        res = session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [[2, "baz"]], list(res)

        res = session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [[2, "baz"]], list(res)

        res = session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [], list(res)

        res = session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [], list(res)

        assert_invalid(session, "SELECT content FROM blogs WHERE time2 >= 0 AND author='foo'")

        # as discussed in CASSANDRA-8148, some queries that should have required ALLOW FILTERING
        # in 2.0 have been fixed for 2.2
        v = Version(self.cluster.version())
        if v < Version("2.2.0"):
            session.execute("SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
            session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
            session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
            session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
            session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")
        else:
            assert_invalid(session, "SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
            assert_invalid(session, "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
            assert_invalid(session, "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
            assert_invalid(session, "SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
            assert_invalid(session, "SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_limit_bugs(self):
        """Test for LIMIT bugs from 4579"""

        session = self.prepare()
        session.execute(
            """
            CREATE TABLE testcf (
                a int,
                b int,
                c int,
                d int,
                e int,
                PRIMARY KEY (a, b)
            );
        """
        )

        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (1, 1, 1, 1, 1);")
        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (2, 2, 2, 2, 2);")
        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (3, 3, 3, 3, 3);")
        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (4, 4, 4, 4, 4);")

        res = session.execute("SELECT * FROM testcf;")
        assert rows_to_list(res) == [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2], [4, 4, 4, 4, 4], [3, 3, 3, 3, 3]], list(res)

        res = session.execute("SELECT * FROM testcf LIMIT 1;")  # columns d and e in result row are null
        assert rows_to_list(res) == [[1, 1, 1, 1, 1]], list(res)

        res = session.execute("SELECT * FROM testcf LIMIT 2;")  # columns d and e in last result row are null
        assert rows_to_list(res) == [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2]], list(res)

        session.execute(
            """
            CREATE TABLE testcf2 (
                a int primary key,
                b int,
                c int,
            );
        """
        )

        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (1, 1, 1);")
        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (2, 2, 2);")
        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (3, 3, 3);")
        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (4, 4, 4);")

        res = session.execute("SELECT * FROM testcf2;")
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [4, 4, 4], [3, 3, 3]], list(res)

        res = session.execute("SELECT * FROM testcf2 LIMIT 1;")  # gives 1 row
        assert rows_to_list(res) == [[1, 1, 1]], list(res)

        res = session.execute("SELECT * FROM testcf2 LIMIT 2;")  # gives 1 row
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2]], list(res)

        res = session.execute("SELECT * FROM testcf2 LIMIT 3;")  # gives 2 rows
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [4, 4, 4]], list(res)

        res = session.execute("SELECT * FROM testcf2 LIMIT 4;")  # gives 2 rows
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [4, 4, 4], [3, 3, 3]], list(res)

        res = session.execute("SELECT * FROM testcf2 LIMIT 5;")  # gives 3 rows
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [4, 4, 4], [3, 3, 3]], list(res)

    @pytest.mark.single_node
    def test_bug_4532(self):
        session = self.prepare()
        session.execute(
            """
            CREATE TABLE compositetest(
                status ascii,
                ctime bigint,
                key ascii,
                nil ascii,
                PRIMARY KEY (status, ctime, key)
            )
        """
        )

        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key1','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key2','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key3','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key4','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key5','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345680,'key6','')")

        assert_invalid(session, "SELECT * FROM compositetest WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;")
        assert_invalid(session, "SELECT * FROM compositetest WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3")

    @pytest.mark.single_node
    def test_order_by_multikey(self):
        """Test for #4612 bug and more generaly order by when multiple C* rows are queried"""

        session = self.prepare()
        session.default_fetch_size = None
        session.execute(
            """
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                col2 int,
                value varchar,
                PRIMARY KEY (my_id, col1, col2)
            );
        """
        )

        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key1', 1, 1, 'a');")
        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key2', 3, 3, 'a');")
        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key3', 2, 2, 'b');")
        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key4', 2, 1, 'b');")

        res = session.execute("SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1;")
        assert rows_to_list(res) == [[1], [2], [3]], list(res)

        res = session.execute("SELECT col1, value, my_id, col2 FROM test WHERE my_id in('key3', 'key4') ORDER BY col1, col2;")
        assert rows_to_list(res) == [[2, "b", "key4", 1], [2, "b", "key3", 2]], list(res)

        assert_invalid(session, "SELECT col1 FROM test ORDER BY col1;")
        assert_invalid(session, "SELECT col1 FROM test WHERE my_id > 'key1' ORDER BY col1;")

    @pytest.mark.single_node
    def test_remove_range_slice(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """
        )

        for i in range(3):
            session.execute("INSERT INTO test (k, v) VALUES (%d, %d)" % (i, i))

        session.execute("DELETE FROM test WHERE k = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 0], [2, 2]], list(res)

    @pytest.mark.single_node
    def test_indexes_composite(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                blog_id int,
                timestamp int,
                author text,
                content text,
                PRIMARY KEY (blog_id, timestamp)
            )
        """
        )

        req = "INSERT INTO test (blog_id, timestamp, author, content) VALUES (%d, %d, '%s', '%s')"
        session.execute(req % (0, 0, "bob", "1st post"))
        session.execute(req % (0, 1, "tom", "2nd post"))
        session.execute(req % (0, 2, "bob", "3rd post"))
        session.execute(req % (0, 3, "tom", "4nd post"))
        session.execute(req % (1, 0, "bob", "5th post"))

        session.execute("CREATE INDEX ON test(author)")
        # The rows above predate the index, so they only become visible through
        # it once the backing view has been built. Reads are not blocked on an
        # unbuilt index, so wait for the build instead of sleeping.
        assert index_is_built(self.cluster, session, "ks", "test", "test_author_idx")

        res = session.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        assert rows_to_list(res) == [[1, 0], [0, 0], [0, 2]], list(res)

        session.execute(req % (1, 1, "tom", "6th post"))
        session.execute(req % (1, 2, "tom", "7th post"))
        session.execute(req % (1, 3, "bob", "8th post"))

        res = session.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        assert rows_to_list(res) == [[1, 0], [1, 3], [0, 0], [0, 2]], list(res)

        session.execute("DELETE FROM test WHERE blog_id = 0 AND timestamp = 2")

        res = session.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        assert rows_to_list(res) == [[1, 0], [1, 3], [0, 0]], list(res)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    @pytest.mark.single_node
    def test_validate_counter_regular(self):
        """
        @jira_ticket CASSANDRA-4706

        Regression test for a validation bug.
        """

        session = self.prepare()
        assert_invalid(session, "CREATE TABLE test (id bigint PRIMARY KEY, count counter, things set<text>)", matching=r"Cannot add a( non)? counter column", expected=ConfigurationException)

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_reversed_compact(self):
        """
        @jira_ticket CASSANDRA-4716

        Regression test for #4716 bug and more generally for good behavior of ordering.
        """

        session = self.prepare()
        session.execute(
            """
            CREATE TABLE test1 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
              AND CLUSTERING ORDER BY (c DESC);
        """
        )

        for i in range(10):
            session.execute("INSERT INTO test1(k, c, v) VALUES ('foo', %i, %i)" % (i, i))

        res = session.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo'")
        assert rows_to_list(res) == [[5], [4], [3]], list(res)

        res = session.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo'")
        assert rows_to_list(res) == [[6], [5], [4], [3], [2]], list(res)

        res = session.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[3], [4], [5]], list(res)

        res = session.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[2], [3], [4], [5], [6]], list(res)

        res = session.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[5], [4], [3]], list(res)

        res = session.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[6], [5], [4], [3], [2]], list(res)

        session.execute(
            """
            CREATE TABLE test2 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """
        )

        for i in range(10):
            session.execute("INSERT INTO test2(k, c, v) VALUES ('foo', %i, %i)" % (i, i))

        res = session.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo'")
        assert rows_to_list(res) == [[3], [4], [5]], list(res)

        res = session.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo'")
        assert rows_to_list(res) == [[2], [3], [4], [5], [6]], list(res)

        res = session.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[3], [4], [5]], list(res)

        res = session.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[2], [3], [4], [5], [6]], list(res)

        res = session.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[5], [4], [3]], list(res)

        res = session.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[6], [5], [4], [3], [2]], list(res)

    @pytest.mark.single_node
    def test_unescaped_string(self):
        """
        Test that unescaped strings in CQL statements raise syntax exceptions.
        """

        session = self.prepare()
        session.execute(
            """
            CREATE TABLE test (
                k text PRIMARY KEY,
                c text,
            )
        """
        )

        # The \ in this query string is not forwarded to cassandra.
        # The ' is being escaped in python, but only ' is forwarded
        # over the wire instead of \'.
        assert_invalid(session, "INSERT INTO test (k, c) VALUES ('foo', 'CQL is cassandra's best friend')", expected=SyntaxException)

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_reversed_compact_multikey(self):
        """
        @jira_ticket CASSANDRA-4760
        @jira_ticket CASSANDRA-4759

        Regression test for two related tickets.
        """

        session = self.prepare()
        session.execute(
            """
            CREATE TABLE test (
                key text,
                c1 int,
                c2 int,
                value text,
                PRIMARY KEY(key, c1, c2)
                ) WITH COMPACT STORAGE
                  AND CLUSTERING ORDER BY(c1 DESC, c2 DESC);
        """
        )

        for i in range(3):
            for j in range(3):
                session.execute("INSERT INTO test(key, c1, c2, value) VALUES ('foo', %i, %i, 'bar');" % (i, j))

        # Equalities

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [1, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0]], list(res)

        # GT

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[2, 0], [2, 1], [2, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]], list(res)

        # LT

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1")
        assert rows_to_list(res) == [[0, 2], [0, 1], [0, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[0, 0], [0, 1], [0, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[0, 2], [0, 1], [0, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC")
        assert rows_to_list(res) == [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]], list(res)

    @pytest.mark.single_node
    def test_collection_and_regular(self):
        session = self.prepare()

        session.execute(
            """
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>,
            c int
          )
        """
        )

        session.execute("INSERT INTO test(k, l, c) VALUES(3, [0, 1, 2], 4)")
        session.execute("UPDATE test SET l[0] = 1, c = 42 WHERE k = 3")
        res = session.execute("SELECT l, c FROM test WHERE k = 3")
        assert rows_to_list(res) == [[[1, 1, 2], 42]]

    @pytest.mark.single_node
    def test_batch_and_list(self):
        session = self.prepare()

        session.execute(
            """
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>
          )
        """
        )

        session.execute(
            """
          BEGIN BATCH
            UPDATE test SET l = l + [ 1 ] WHERE k = 0;
            UPDATE test SET l = l + [ 2 ] WHERE k = 0;
            UPDATE test SET l = l + [ 3 ] WHERE k = 0;
          APPLY BATCH
        """
        )

        res = session.execute("SELECT l FROM test WHERE k = 0")
        assert rows_to_list(res[0]) == [[1, 2, 3]]

        session.execute(
            """
          BEGIN BATCH
            UPDATE test SET l = [ 1 ] + l WHERE k = 1;
            UPDATE test SET l = [ 2 ] + l WHERE k = 1;
            UPDATE test SET l = [ 3 ] + l WHERE k = 1;
          APPLY BATCH
        """
        )

        res = session.execute("SELECT l FROM test WHERE k = 1")
        assert rows_to_list(res[0]) == [[3, 2, 1]]

    @pytest.mark.single_node
    def test_boolean(self):
        session = self.prepare()

        session.execute(
            """
          CREATE TABLE test (
            k boolean PRIMARY KEY,
            b boolean
          )
        """
        )

        session.execute("INSERT INTO test (k, b) VALUES (true, false)")
        res = session.execute("SELECT * FROM test WHERE k = true")
        assert rows_to_list(res) == [[True, False]], list(res)

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_multiordering(self):
        session = self.prepare()
        session.execute(
            """
            CREATE TABLE test (
                k text,
                c1 int,
                c2 int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """
        )

        for i in range(2):
            for j in range(2):
                session.execute("INSERT INTO test(k, c1, c2) VALUES ('foo', %i, %i)" % (i, j))

        res = session.execute("SELECT c1, c2 FROM test WHERE k = 'foo'")
        assert rows_to_list(res) == [[0, 1], [0, 0], [1, 1], [1, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC")
        assert rows_to_list(res) == [[0, 1], [0, 0], [1, 1], [1, 0]], list(res)

        res = session.execute("SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [0, 0], [0, 1]], list(res)

        assert_invalid(session, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 DESC")
        assert_invalid(session, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 ASC")
        assert_invalid(session, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC")

    @pytest.mark.single_node
    def test_multiordering_validation(self):
        session = self.prepare()

        assert_invalid(session, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)")
        assert_invalid(session, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)")
        assert_invalid(session, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)")

        session.execute("CREATE TABLE test1 (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)")
        session.execute("CREATE TABLE test2 (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)")

    @pytest.mark.single_node
    def test_bug_4882(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """
        )

        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 0, 0);")
        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 1, 1);")
        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 2, 2);")
        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 3, 3);")

        res = session.execute("select * from test where k = 0 limit 1;")
        assert rows_to_list(res) == [[0, 0, 2, 2]], list(res)

    @pytest.mark.single_node
    def test_multi_list_set(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                l1 list<int>,
                l2 list<int>
            )
        """
        )

        session.execute("INSERT INTO test (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])")
        session.execute("UPDATE test SET l2[1] = 42, l1[1] = 24  WHERE k = 0")

        res = session.execute("SELECT l1, l2 FROM test WHERE k = 0")
        assert rows_to_list(res) == [[[1, 24, 3], [4, 42, 6]]]

    @pytest.mark.single_node
    def test_composite_index_collections(self):
        session = self.prepare()
        session.execute(
            """
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content set<text>,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """
        )

        session.execute("CREATE INDEX ON blogs(author)")

        req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', %s)"
        session.execute(req % (1, 0, 0, "foo", "{ 'bar1', 'bar2' }"))
        session.execute(req % (1, 0, 1, "foo", "{ 'bar2', 'bar3' }"))
        session.execute(req % (2, 1, 0, "foo", "{ 'baz' }"))
        session.execute(req % (3, 0, 1, "gux", "{ 'qux' }"))

        res = session.execute("SELECT blog_id, content FROM blogs WHERE author='foo'")
        assert rows_to_list(res) == [[1, set(["bar1", "bar2"])], [1, set(["bar2", "bar3"])], [2, set(["baz"])]], list(res)

    @pytest.mark.single_node
    def test_truncate_clean_cache(self):
        session = self.prepare(use_cache=True)

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            ) WITH CACHING = '{"keys":"ALL","rows_per_partition":"ALL"}';
        """
        )

        for i in range(3):
            session.execute("INSERT INTO test(k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i * 2))

        res = session.execute("SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)")
        assert rows_to_list(res) == [[0, 0], [1, 2], [2, 4]], list(res)

        session.execute("TRUNCATE test")

        res = session.execute("SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)")
        assert rows_to_list(res) == [], list(res)

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_allow_filtering(self):
        """
        test queries with multiple restrictions.
        see where 'allow_filtering' is optional and when it is required.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """
        )

        for i in range(3):
            for j in range(3):
                session.execute("INSERT INTO test(k, c, v) VALUES(%d, %d, %d)" % (i, j, j))

        # Don't require filtering, always allowed
        queries = ["SELECT * FROM test WHERE k = 1", "SELECT * FROM test WHERE k = 1 AND c > 2", "SELECT * FROM test WHERE k = 1 AND c = 2"]
        for q in queries:
            self._assert_valid_query(session=session, query=q)
            self._assert_valid_query(session=session, query=q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = [
            "SELECT * FROM test WHERE v = 2",
            "SELECT * FROM test WHERE v > 2 AND v <= 4",
            # Uncomment when scylla#7608 is fixed: "SELECT * FROM test WHERE c > 2",
        ]
        for q in queries:
            self._assert_valid_query(session=session, query=q + " ALLOW FILTERING")
            self._assert_invalid_filtering(session=session, query=q)

    @pytest.mark.single_node
    def test_allow_filtering_secondary_indexes(self):
        """
        test queries with multiple restrictions + secondary indexes.
        see where 'allow_filtering' is optional and when it is required.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE indexed (
                k int PRIMARY KEY,
                a int,
                b int,
            )
        """
        )

        session.execute("CREATE INDEX ON indexed(a)")

        for i in range(5):
            session.execute("INSERT INTO indexed(k, a, b) VALUES(%d, %d, %d)" % (i, i * 10, i * 100))

        # Don't require filtering, always allowed
        queries = ["SELECT * FROM indexed WHERE k = 1", "SELECT * FROM indexed WHERE a = 20"]
        for q in queries:
            self._assert_valid_query(session=session, query=q)
            self._assert_valid_query(session=session, query=q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = ["SELECT * FROM indexed WHERE a = 20 AND b = 200"]
        for q in queries:
            self._assert_invalid_filtering(session=session, query=q)
            self._assert_valid_query(session=session, query=q + " ALLOW FILTERING")

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_range_with_deletes(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """
        )

        nb_keys = 30
        nb_deletes = 5

        for i in range(nb_keys):
            session.execute("INSERT INTO test(k, v) VALUES (%d, %d)" % (i, i))

        for i in random.sample(range(nb_keys), nb_deletes):
            # failing cause of https://github.com/PyCQA/astroid/pull/1189

            session.execute("DELETE FROM test WHERE k = %d" % i)

        res = list(session.execute("SELECT * FROM test LIMIT %d" % (nb_keys / 2)))
        assert len(res) == nb_keys / 2, "Expected %d but got %d" % (nb_keys / 2, len(res))

    @pytest.mark.single_node
    def test_alter_with_collections(self):
        """
        @jira_ticket CASSANDRA-4982

        Test you can add columns in a table with collections. Regression test
        for CASSANDRA-4982.
        """
        session = self.prepare()

        session.execute("CREATE TABLE collections (key int PRIMARY KEY, aset set<text>)")
        session.execute("ALTER TABLE collections ADD c text")
        session.execute("ALTER TABLE collections ADD alist list<text>")

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_collection_compact(self):
        session = self.prepare()

        assert_invalid(
            session,
            """
            CREATE TABLE test (
                user ascii PRIMARY KEY,
                mails list<text>
            ) WITH COMPACT STORAGE;
        """,
        )

    @pytest.mark.single_node
    def test_collection_function(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                l set<int>
            )
        """
        )

        assert_invalid(session, "SELECT ttl(l) FROM test WHERE k = 0")
        assert_invalid(session, "SELECT writetime(l) FROM test WHERE k = 0")

    @pytest.mark.single_node
    def test_collection_counter(self):
        session = self.prepare()

        assert_invalid(
            session,
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                l list<counter>
            )
        """,
            expected=(InvalidRequest, SyntaxException),
        )

        assert_invalid(
            session,
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                s set<counter>
            )
        """,
            expected=(InvalidRequest, SyntaxException),
        )

        assert_invalid(
            session,
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                m map<text, counter>
            )
        """,
            expected=(InvalidRequest, SyntaxException),
        )

    @pytest.mark.single_node
    def test_composite_partition_key_validation(self):
        """
        @jira_ticket CASSANDRA-5122

        Regression test for CASSANDRA-5122.
        """
        session = self.prepare()

        session.execute("CREATE TABLE foo (a int, b text, c uuid, PRIMARY KEY ((a, b)));")

        session.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)")
        session.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)")
        session.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)")

        res = list(session.execute("SELECT * FROM foo"))
        assert len(res) == 3, res

        assert_invalid(session, "SELECT * FROM foo WHERE a=1")

    @pytest.mark.single_node
    def test_large_clustering_in(self):
        """
        @jira_ticket CASSANDRA-8410
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """
        )

        insert_statement = session.prepare("INSERT INTO test (k, c, v) VALUES (?, ?, ?)")
        session.execute(insert_statement, (0, 0, 0))

        select_statement = session.prepare("SELECT * FROM test WHERE k=? AND c IN ?")
        in_values = list(range(100))

        # try to fetch one existing row and 9999 non-existing rows
        rows = list(session.execute(select_statement, [0, in_values]))
        assert 1 == len(rows)
        assert (0, 0, 0) == rows[0]

        # insert approximately 1000 random rows between 0 and 10k
        clustering_values = set([random.randint(0, 9999) for _ in range(1000)])
        clustering_values.add(0)
        args = [(0, i, i) for i in clustering_values]
        execute_concurrent_with_args(session, insert_statement, args)

        rows = list(session.execute(select_statement, [0, in_values]))
        expected_rows = [v for v in clustering_values if v in in_values]
        assert len(expected_rows) == len(rows), f"expected_rows={expected_rows} rows={rows}"

    @pytest.mark.single_node
    def test_timeuuid(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            )
        """
        )

        assert_invalid(session, "INSERT INTO test (k, t) VALUES (0, 2012-11-07 18:18:22-0800)", expected=SyntaxException)

        for i in range(4):
            session.execute("INSERT INTO test (k, t) VALUES (0, now())")
            time.sleep(1)

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 4, res
        dates = [d[1] for d in res]

        res = list(session.execute("SELECT * FROM test WHERE k = 0 AND t >= %s" % dates[0]))
        assert len(res) == 4, res

        res = list(session.execute("SELECT * FROM test WHERE k = 0 AND t < %s" % dates[0]))
        assert len(res) == 0, res

        res = list(session.execute(f"SELECT * FROM test WHERE k = 0 AND t > {dates[0]} AND t <= {dates[2]}"))
        assert len(res) == 2, res

        res = list(session.execute("SELECT * FROM test WHERE k = 0 AND t = %s" % dates[0]))
        assert len(res) == 1, res

        assert_invalid(session, "SELECT dateOf(k) FROM test WHERE k = 0 AND t = %s" % dates[0])

        session.execute("SELECT dateOf(t), unixTimestampOf(t) FROM test WHERE k = 0 AND t = %s" % dates[0])
        session.execute("SELECT t FROM test WHERE k = 0 AND t > maxTimeuuid(1234567) AND t < minTimeuuid('2012-11-07 18:18:22-0800')")
        # not sure what to check exactly so just checking the query returns

    @pytest.mark.single_node
    def test_cql_tinyint_type(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                t tinyint,
                PRIMARY KEY (t)
            )
        """
        )

        assert_invalid(session, "INSERT INTO test (t) VALUES (-129)", expected=InvalidRequest)
        assert_invalid(session, "INSERT INTO test (t) VALUES (128)", expected=InvalidRequest)

        session.execute("INSERT INTO test (t) VALUES (-128);")
        session.execute("INSERT INTO test (t) VALUES (127);")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 2, res

        assert -128 == res[0].t
        assert 127 == res[1].t

    @pytest.mark.single_node
    def test_cql_smallint_type(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                t smallint,
                PRIMARY KEY (t)
            )
        """
        )

        assert_invalid(session, "INSERT INTO test (t) VALUES (-32769)", expected=InvalidRequest)
        assert_invalid(session, "INSERT INTO test (t) VALUES (32768)", expected=InvalidRequest)

        session.execute("INSERT INTO test (t) VALUES (-32768);")
        session.execute("INSERT INTO test (t) VALUES (32767);")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 2, res

        assert -32768 == res[0].t
        assert 32767 == res[1].t

    @pytest.mark.single_node
    def test_cql_date_type(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                t date,
                PRIMARY KEY (t)
            )
        """
        )

        assert_invalid(session, "INSERT INTO test (t) VALUES ('-5877641-06-22')", expected=InvalidRequest)
        assert_invalid(session, "INSERT INTO test (t) VALUES ('5881580-07-12')", expected=InvalidRequest)

        session.execute("INSERT INTO test (t) VALUES ('-5877641-06-23')")
        session.execute("INSERT INTO test (t) VALUES ('1970-01-01')")
        session.execute("INSERT INTO test (t) VALUES ('5881580-07-11')")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 3, res

        assert "-2147483648" == str(res[0].t)
        assert "1970-01-01" == str(res[1].t)
        assert "2147483647" == str(res[2].t)

    @pytest.mark.single_node
    def test_cql_time_type(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                t time,
                PRIMARY KEY (t)
            )
        """
        )

        assert_invalid(session, "INSERT INTO test (t) VALUES ('14:53')", expected=InvalidRequest)
        assert_invalid(session, "INSERT INTO test (t) VALUES ('14:53:12.1234567890')", expected=InvalidRequest)

        session.execute("INSERT INTO test (t) VALUES ('14:53:12')")
        session.execute("INSERT INTO test (t) VALUES ('14:53:12.1234')")
        session.execute("INSERT INTO test (t) VALUES ('14:53:12.123456789')")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 3, res

        assert "14:53:12.123400000" == str(res[0].t)
        assert "14:53:12.000000000" == str(res[1].t)
        assert "14:53:12.123456789" == str(res[2].t)

    @pytest.mark.single_node
    def test_float_with_exponent(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                d double,
                f float
            )
        """
        )

        session.execute("INSERT INTO test(k, d, f) VALUES (0, 3E+10, 3.4E3)")
        session.execute("INSERT INTO test(k, d, f) VALUES (1, 3.E10, -23.44E-3)")
        session.execute("INSERT INTO test(k, d, f) VALUES (2, 3, -2)")

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_compact_metadata(self):
        """
        @jira_ticket CASSANDRA-5189

        Regression test for CASSANDRA-5189.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE bar (
                id int primary key,
                i int
            ) WITH COMPACT STORAGE;
        """
        )

        session.execute("INSERT INTO bar (id, i) VALUES (1, 2);")
        res = session.execute("SELECT * FROM bar")
        assert rows_to_list(res) == [[1, 2]], list(res)

    @pytest.mark.single_node
    def test_clustering_indexing(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE posts (
                id1 int,
                id2 int,
                author text,
                time bigint,
                v1 text,
                v2 text,
                PRIMARY KEY ((id1, id2), author, time)
            )
        """
        )

        session.execute("CREATE INDEX ON posts(time)")
        session.execute("CREATE INDEX ON posts(id2)")

        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')")

        res = session.execute("SELECT v1 FROM posts WHERE time = 1")
        assert rows_to_list(res) == [["B"], ["E"]], list(res)

        res = session.execute("SELECT v1 FROM posts WHERE id2 = 1")
        assert rows_to_list(res) == [["C"], ["E"]], list(res)

        res = session.execute("SELECT v1 FROM posts WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0")
        assert rows_to_list(res) == [["A"]], list(res)

        # Test for CASSANDRA-8206
        session.execute("UPDATE posts SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1")

        res = session.execute("SELECT v1 FROM posts WHERE id2 = 0")
        assert rows_to_list(res) == [["A"], ["B"], ["D"]], list(res)

        res = session.execute("SELECT v1 FROM posts WHERE time = 1")
        assert rows_to_list(res) == [["B"], ["E"]], list(res)

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_invalid_clustering_indexing(self):
        session = self.prepare()

        session.execute("CREATE TABLE test1 (a int, b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE")
        assert_invalid(session, "CREATE INDEX ON test1(a)")
        assert_invalid(session, "CREATE INDEX ON test1(b)")

        session.execute("CREATE TABLE test2 (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE")
        assert_invalid(session, "CREATE INDEX ON test2(a)")
        assert_invalid(session, "CREATE INDEX ON test2(b)")
        assert_invalid(session, "CREATE INDEX ON test2(c)")

    @pytest.mark.single_node
    def test_indexing_of_static_column(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (a int, b int, c int static , PRIMARY KEY (a, b))")
        session.execute("CREATE INDEX ON test(c)")

    @pytest.mark.single_node
    def test_edge_2i_on_complex_pk(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE indexed (
                pk0 int,
                pk1 int,
                ck0 int,
                ck1 int,
                ck2 int,
                value int,
                PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2)
            )
        """
        )

        session.execute("CREATE INDEX ON indexed(pk0)")
        session.execute("CREATE INDEX ON indexed(ck0)")
        session.execute("CREATE INDEX ON indexed(ck1)")
        session.execute("CREATE INDEX ON indexed(ck2)")

        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (0, 1, 2, 3, 4, 5)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (1, 2, 3, 4, 5, 0)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (2, 3, 4, 5, 0, 1)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (3, 4, 5, 0, 1, 2)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (4, 5, 0, 1, 2, 3)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (5, 0, 1, 2, 3, 4)")

        res = session.execute("SELECT value FROM indexed WHERE pk0 = 2")
        assert [[1]] == rows_to_list(res)

        res = session.execute("SELECT value FROM indexed WHERE ck0 = 0")
        assert [[3]] == rows_to_list(res)

        res = session.execute("SELECT value FROM indexed WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0")
        assert [[2]] == rows_to_list(res)

        res = session.execute("SELECT value FROM indexed WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING")
        assert [[4]] == rows_to_list(res)

    @pytest.mark.single_node
    def test_bug_5240(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test(
                interval text,
                seq int,
                id int,
                severity int,
                PRIMARY KEY ((interval, seq), id)
            ) WITH CLUSTERING ORDER BY (id DESC);
        """
        )

        session.execute("CREATE INDEX ON test(severity);")

        session.execute("insert into test(interval, seq, id , severity) values('t',1, 1, 1);")
        session.execute("insert into test(interval, seq, id , severity) values('t',1, 2, 1);")
        session.execute("insert into test(interval, seq, id , severity) values('t',1, 3, 2);")
        session.execute("insert into test(interval, seq, id , severity) values('t',1, 4, 3);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 1, 3);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 2, 3);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 3, 1);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 4, 2);")

        res = session.execute("select * from test where severity = 3 and interval = 't' and seq =1;")
        assert rows_to_list(res) == [["t", 1, 4, 3]], list(res)

    @pytest.mark.single_node
    def test_ticket_5230(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE foo (
                key text,
                c text,
                v text,
                PRIMARY KEY (key, c)
            )
        """
        )

        session.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '1', '1')")
        session.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '2', '2')")
        session.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '3', '3')")

        res = session.execute("SELECT c FROM foo WHERE key = 'foo' AND c IN ('1', '2');")
        assert rows_to_list(res) == [["1"], ["2"]], list(res)

    @pytest.mark.single_node
    def test_conversion_functions(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                i varint,
                b blob
            )
        """
        )

        session.execute("INSERT INTO test(k, i, b) VALUES (0, blobAsVarint(bigintAsBlob(3)), textAsBlob('foobar'))")
        res = session.execute("SELECT i, blobAsText(b) FROM test WHERE k = 0")
        assert rows_to_list(res) == [[3, "foobar"]], list(res)

    @pytest.mark.single_node
    def test_alter_bug(self):
        """
        @jira_ticket CASSANDRA-5232
        """
        session = self.prepare()

        session.execute("CREATE TABLE t1 (id int PRIMARY KEY, t text);")

        session.execute("UPDATE t1 SET t = '111' WHERE id = 1;")
        session.execute("ALTER TABLE t1 ADD l list<text>;")

        time.sleep(0.5)

        res = session.execute("SELECT * FROM t1;")
        assert rows_to_list(res) == [[1, None, "111"]], list(res)

        session.execute("ALTER TABLE t1 ADD m map<int, text>;")
        time.sleep(0.5)
        res = session.execute("SELECT * FROM t1;")
        assert rows_to_list(res) == [[1, None, None, "111"]], list(res)

    @pytest.mark.single_node
    def bug_5376(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                key text,
                c bigint,
                v text,
                x set<text>,
                PRIMARY KEY (key, c)
            );
        """
        )

        assert_invalid(session, "select * from test where key = 'foo' and c in (1,3,4);")

    @pytest.mark.single_node
    def test_function_and_reverse_type(self):
        """
        @jira_ticket CASSANDRA-5386
        """

        session = self.prepare()
        session.execute(
            """
            CREATE TABLE test (
                k int,
                c timeuuid,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC)
        """
        )

        session.execute("INSERT INTO test (k, c, v) VALUES (0, now(), 0);")

    @pytest.mark.single_node
    def bug_5404(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (key text PRIMARY KEY)")
        # We just want to make sure this doesn't NPE server side
        assert_invalid(session, "select * from test where token(key) > token(int(3030343330393233)) limit 1;")

    @pytest.mark.single_node
    def test_empty_blob(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, b blob)")
        session.execute("INSERT INTO test (k, b) VALUES (0, 0x)")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, b""]], list(rows_to_list(res))

    @pytest.mark.single_node
    def test_clustering_order_and_functions(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            ) WITH CLUSTERING ORDER BY (t DESC)
        """
        )

        for i in range(5):
            session.execute("INSERT INTO test (k, t) VALUES (%d, now())" % i)

        session.execute("SELECT dateOf(t) FROM test")

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_conditional_update(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 text,
                v3 int
            )
        """
        )

        # Shouldn't apply
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [False, None])
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS", [False, None, None, None, None])

        # Should apply
        assert_one(session, "INSERT INTO test (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS", [True, None, None, None, None])

        # Shouldn't apply
        assert_one(session, "INSERT INTO test (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS", [False, 0, 2, "foo", None])
        assert_one(session, "SELECT * FROM test", [0, 2, "foo", None])

        # Should not apply
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [False, 2])
        assert_one(session, "SELECT * FROM test", [0, 2, "foo", None])

        # Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
        assert_one(session, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = 2", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS", [True, 0, 3, "bar", None])
        assert_one(session, "SELECT * FROM test", [0, 3, "bar", None])

        # Shouldn't apply, only one condition is ok
        assert_one(session, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'", [False, 3, "bar"])
        assert_one(session, "SELECT * FROM test", [0, 3, "bar", None])

        # Should apply
        assert_one(session, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'bar'", [True, 3, "bar"])
        assert_one(session, "SELECT * FROM test", [0, 5, "foobar", None])

        # Shouldn't apply
        assert_one(session, "DELETE v2 FROM test WHERE k = 0 IF v1 = 3", [False, 5])
        assert_one(session, "SELECT * FROM test", [0, 5, "foobar", None])

        # Shouldn't apply
        assert_one(session, "DELETE v2 FROM test WHERE k = 0 IF v1 = null", [False, 5])
        assert_one(session, "SELECT * FROM test", [0, 5, "foobar", None])

        # Should apply
        assert_one(session, "DELETE v2 FROM test WHERE k = 0 IF v1 = 5", [True, 5])
        assert_one(session, "SELECT * FROM test", [0, 5, None, None])

        # Shouln't apply
        assert_one(session, "DELETE v1 FROM test WHERE k = 0 IF v3 = 4", [False, None])

        # Should apply
        assert_one(session, "DELETE v1 FROM test WHERE k = 0 IF v3 = null", [True, None])
        assert_one(session, "SELECT * FROM test", [0, None, None, None])

        # Should apply
        assert_one(session, "DELETE FROM test WHERE k = 0 IF v1 = null", [True, None])
        assert_none(session, "SELECT * FROM test")

        # Shouldn't apply
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS", [False, None, None, None, None])

        if Version(self.cluster.version()) > Version("2.1.1"):
            # Should apply
            assert_one(session, "DELETE FROM test WHERE k = 0 IF v1 IN (null)", [True, None])

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_non_eq_conditional_update(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 text,
                v3 int
            )
        """
        )

        # non-EQ conditions
        session.execute("INSERT INTO test (k, v1, v2) VALUES (0, 2, 'foo')")
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 < 3", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 <= 3", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 > 1", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 >= 1", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 != 1", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 != 2", [False, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 IN (0, 1, 2)", [True, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 IN (142, 276)", [False, 2])
        assert_one(session, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 IN ()", [False, 2])

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_conditional_delete(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
            )
        """
        )

        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [False, None, None])

        session.execute("INSERT INTO test (k, v1) VALUES (1, 2)")
        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [True, 1, 2])
        assert_none(session, "SELECT * FROM test WHERE k=1")
        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [False, None, None])

        session.execute("UPDATE test USING TTL 1 SET v1=2 WHERE k=1")
        time.sleep(1.5)
        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [False, None, None])
        assert_none(session, "SELECT * FROM test WHERE k=1")

        session.execute("INSERT INTO test (k, v1) VALUES (2, 2) USING TTL 1")
        time.sleep(1.5)
        assert_one(session, "DELETE FROM test WHERE k=2 IF EXISTS", [False, None, None])
        assert_none(session, "SELECT * FROM test WHERE k=2")

        session.execute("INSERT INTO test (k, v1) VALUES (3, 2)")
        assert_one(session, "DELETE v1 FROM test WHERE k=3 IF EXISTS", [True, 3, 2])
        assert_one(session, "SELECT * FROM test WHERE k=3", [3, None])
        assert_one(session, "DELETE v1 FROM test WHERE k=3 IF EXISTS", [True, 3, None])
        assert_one(session, "DELETE FROM test WHERE k=3 IF EXISTS", [True, 3, None])

        # static columns
        session.execute(
            """
            CREATE TABLE test2 (
                k text,
                s text static,
                i int,
                v text,
                PRIMARY KEY (k, i)
            )"""
        )

        session.execute("INSERT INTO test2 (k, s, i, v) VALUES ('k', 's', 0, 'v')")
        assert_one(session, "DELETE v FROM test2 WHERE k='k' AND i=0 IF EXISTS", [True, "k", 0, "s", "v"])
        assert_one(session, "DELETE FROM test2 WHERE k='k' AND i=0 IF EXISTS", [True, "k", 0, "s", None])
        assert_one(session, "DELETE v FROM test2 WHERE k='k' AND i=0 IF EXISTS", [False, None, None, None, None])
        assert_one(session, "DELETE FROM test2 WHERE k='k' AND i=0 IF EXISTS", [False, None, None, None, None])

        # CASSANDRA-6430
        v = Version(self.cluster.version())
        if v >= Version("2.1.1") or (v < Version("2.1") and v >= Version("2.0.11")):
            assert_invalid(session, "DELETE FROM test2 WHERE k = 'k' IF EXISTS")
            assert_invalid(session, "DELETE FROM test2 WHERE k = 'k' IF v = 'foo'")
            assert_invalid(session, "DELETE FROM test2 WHERE i = 0 IF EXISTS")
            assert_invalid(session, "DELETE FROM test2 WHERE k = 0 AND i > 0 IF EXISTS")
            assert_invalid(session, "DELETE FROM test2 WHERE k = 0 AND i > 0 IF v = 'foo'")

    @pytest.mark.single_node
    def test_range_key_ordered(self):
        session = self.prepare()

        session.execute("CREATE TABLE test ( k int PRIMARY KEY)")

        session.execute("INSERT INTO test(k) VALUES (-1)")
        session.execute("INSERT INTO test(k) VALUES ( 0)")
        session.execute("INSERT INTO test(k) VALUES ( 1)")

        assert_all(session, "SELECT * FROM test", [[1], [0], [-1]])
        assert_invalid(session, "SELECT * FROM test WHERE k >= -1 AND k < 1;")

    @pytest.mark.single_node
    def test_select_with_alias(self):
        session = self.prepare()
        session.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")

        ttl = 100
        num_keys = 5

        logger.debug(f"Inserting {num_keys} using ttl={ttl}")
        pre_insert = int(time.time())
        for _id in range(num_keys):
            session.execute(f"INSERT INTO users (id, name) VALUES ({_id}, 'name{_id}') USING TTL {ttl} AND TIMESTAMP 0")
        post_insert = int(time.time())

        logger.debug("Test aliasing count(*)")
        res = list(session.execute("SELECT count(*) AS user_count FROM users"))
        assert "user_count" == res[0]._fields[0]
        assert num_keys == res[0].user_count

        logger.debug("Test aliasing of regular value")
        res = list(session.execute("SELECT name AS user_name FROM users WHERE id = 0"))
        assert "user_name" == res[0]._fields[0]
        assert "name0" == res[0].user_name

        logger.debug("Test aliasing writetime")
        res = list(session.execute("SELECT writeTime(name) AS name_writetime FROM users WHERE id = 0"))
        assert "name_writetime" == res[0]._fields[0]
        assert 0 == res[0].name_writetime

        logger.debug("Test aliasing ttl")
        pre_select = int(time.time())
        res = list(session.execute("SELECT ttl(name) AS name_ttl FROM users WHERE id = 0"))
        post_select = int(time.time())
        assert "name_ttl" == res[0]._fields[0]
        name_ttl = res[0].name_ttl
        allowed_min = ttl - (post_select - pre_insert + 1)
        allowed_max = ttl - (pre_select - post_insert - 1)
        logger.debug(f"Verifying name_ttl={name_ttl} is in range=[{allowed_min}, {allowed_max}]")
        assert allowed_min <= name_ttl <= allowed_max

        logger.debug("Test aliasing a regular function")
        res = list(session.execute("SELECT intAsBlob(id) AS id_blob FROM users WHERE id = 0"))
        assert "id_blob" == res[0]._fields[0]
        assert b"\x00\x00\x00\x00" == res[0].id_blob

        logger.debug("Test that select throws a meaningful exception for aliases in where clause")
        assert_invalid(session, "SELECT id AS user_id, name AS user_name FROM users WHERE user_id = 0", matching="Aliases aren't allowed in the WHERE clause")

        logger.debug("Test that select throws a meaningful exception for aliases in order by clause")
        assert_invalid(session, "SELECT id AS user_id, name AS user_name FROM users WHERE id IN (0) ORDER BY user_name", matching="Aliases are not allowed in order by clause")

    @pytest.mark.single_node
    def test_nonpure_function_collection(self):
        """
        @jira_ticket CASSANDRA-5795
        """

        session = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v list<timeuuid>)")

        # we just want to make sure this doesn't throw
        session.execute("INSERT INTO test(k, v) VALUES (0, [now()])")

    @pytest.mark.single_node
    def test_empty_in(self):
        session = self.prepare()
        session.execute("CREATE TABLE test (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))")

        def fill(table):
            for i in range(2):
                for j in range(2):
                    session.execute("INSERT INTO %s (k1, k2, v) VALUES (%d, %d, %d)" % (table, i, j, i + j))

        def assert_nothing_changed(table):
            res = session.execute("SELECT * FROM %s" % table)  # make sure nothing got removed
            assert [[0, 0, 0], [0, 1, 1], [1, 0, 1], [1, 1, 2]] == rows_to_list(sorted(res))

        # Inserts a few rows to make sure we don't actually query something
        fill("test")

        # Test empty IN () in SELECT
        assert_none(session, "SELECT v FROM test WHERE k1 IN ()")
        assert_none(session, "SELECT v FROM test WHERE k1 = 0 AND k2 IN ()")

        # Test empty IN () in DELETE
        session.execute("DELETE FROM test WHERE k1 IN ()")
        assert_nothing_changed("test")

        # Test empty IN () in UPDATE
        session.execute("UPDATE test SET v = 3 WHERE k1 IN () AND k2 = 2")
        assert_nothing_changed("test")

        # Same test, but for compact
        session.execute("CREATE TABLE test_compact (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))")

        fill("test_compact")

        assert_none(session, "SELECT v FROM test_compact WHERE k1 IN ()")
        assert_none(session, "SELECT v FROM test_compact WHERE k1 = 0 AND k2 IN ()")

        # Test empty IN () in DELETE
        session.execute("DELETE FROM test_compact WHERE k1 IN ()")
        assert_nothing_changed("test_compact")

        # Test empty IN () in UPDATE
        session.execute("UPDATE test_compact SET v = 3 WHERE k1 IN () AND k2 = 2")
        assert_nothing_changed("test_compact")

    @pytest.mark.single_node
    def test_collection_flush(self):
        """
        @jira_ticket CASSANDRA-5805
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, s set<int>)")

        session.execute("INSERT INTO test(k, s) VALUES (1, {1})")
        self.cluster.flush()
        session.execute("INSERT INTO test(k, s) VALUES (1, {2})")
        self.cluster.flush()

        assert_one(session, "SELECT * FROM test", [1, set([2])])

    @pytest.mark.single_node
    def test_select_distinct(self):
        session = self.prepare()

        # Test a regular (CQL3) table.
        session.execute("CREATE TABLE regular (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))")

        for i in range(3):
            session.execute("INSERT INTO regular (pk0, pk1, ck0, val) VALUES (%d, %d, 0, 0)" % (i, i))
            session.execute("INSERT INTO regular (pk0, pk1, ck0, val) VALUES (%d, %d, 1, 1)" % (i, i))

        res = session.execute("SELECT DISTINCT pk0, pk1 FROM regular LIMIT 1")
        assert [[0, 0]] == rows_to_list(res)

        res = session.execute("SELECT DISTINCT pk0, pk1 FROM regular LIMIT 3")
        assert [[0, 0], [1, 1], [2, 2]] == rows_to_list(sorted(res))

        session.execute("CREATE TABLE compact (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1)))")

        for i in range(3):
            session.execute("INSERT INTO compact (pk0, pk1, val) VALUES (%d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT DISTINCT pk0, pk1 FROM compact LIMIT 1")
        assert [[0, 0]] == rows_to_list(res)

        res = list(session.execute("SELECT DISTINCT pk0, pk1 FROM compact LIMIT 3"))
        assert [[0, 0], [1, 1], [2, 2]] == rows_to_list(sorted(res))

        # Test a 'wide row' thrift table.
        session.execute("CREATE TABLE wide (pk int, name text, val int, PRIMARY KEY(pk, name))")

        for i in range(3):
            session.execute("INSERT INTO wide (pk, name, val) VALUES (%d, 'name0', 0)" % i)
            session.execute("INSERT INTO wide (pk, name, val) VALUES (%d, 'name1', 1)" % i)

        res = session.execute("SELECT DISTINCT pk FROM wide LIMIT 1")
        assert [[1]] == rows_to_list(res)

        res = list(session.execute("SELECT DISTINCT pk FROM wide LIMIT 3"))
        assert [[0], [1], [2]] == rows_to_list(sorted(res))

        # Test selection validation.
        assert_invalid(session, "SELECT DISTINCT pk0 FROM regular", matching="queries must request all the partition key columns")
        assert_invalid(session, "SELECT DISTINCT pk0, pk1, ck0 FROM regular", matching="queries must only request partition key columns")

    @pytest.mark.single_node
    def test_select_distinct_with_deletions(self):
        session = self.prepare()
        session.execute("CREATE TABLE t1 (k int PRIMARY KEY, c int, v int)")
        for i in range(10):
            session.execute("INSERT INTO t1 (k, c, v) VALUES (%d, %d, %d)" % (i, i, i))

        rows = list(session.execute("SELECT DISTINCT k FROM t1"))
        assert 10 == len(rows)
        key_to_delete = rows[3].k

        session.execute("DELETE FROM t1 WHERE k=%d" % (key_to_delete,))
        rows = list(session.execute("SELECT DISTINCT k FROM t1"))
        assert 9 == len(rows)

        rows = list(session.execute("SELECT DISTINCT k FROM t1 LIMIT 5"))
        assert 5 == len(rows)

        session.default_fetch_size = 5
        rows = list(session.execute("SELECT DISTINCT k FROM t1"))
        assert 9 == len(rows)

    @pytest.mark.single_node
    def test_function_with_null(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timeuuid,
            )
        """
        )

        session.execute("INSERT INTO test(k) VALUES (0)")
        assert_one(session, "SELECT dateOf(t) FROM test WHERE k=0", [None])

    @pytest.mark.lwt
    def test_cas_simple(self):
        session = self.prepare(nodes=3, rf=3)

        session.execute("CREATE TABLE tkns (tkn int, consumed boolean, PRIMARY KEY (tkn));")

        for i in range(1, 10):
            query = SimpleStatement("INSERT INTO tkns (tkn, consumed) VALUES (%i,FALSE);" % i, consistency_level=ConsistencyLevel.QUORUM)
            session.execute(query)
            assert_one(session, "UPDATE tkns SET consumed = TRUE WHERE tkn = %i IF consumed = FALSE;" % i, [True, False], cl=ConsistencyLevel.QUORUM)
            assert_one(session, "UPDATE tkns SET consumed = TRUE WHERE tkn = %i IF consumed = FALSE;" % i, [False, True], cl=ConsistencyLevel.QUORUM)

    @pytest.mark.single_node
    def test_bug_6050(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                a int,
                b int
            )
        """
        )

        session.execute("CREATE INDEX ON test(a)")
        assert_invalid(session, "SELECT * FROM test WHERE a = 3 AND b IN (1, 3)")

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_bug_6069(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                s set<int>
            )
        """
        )

        assert_one(session, "INSERT INTO test(k, s) VALUES (0, {1, 2, 3}) IF NOT EXISTS", [True, None, None])
        assert_one(session, "SELECT * FROM test", [0, {1, 2, 3}])

    @pytest.mark.single_node
    def test_bug_6115(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, v int, PRIMARY KEY (k, v))")

        session.execute("INSERT INTO test (k, v) VALUES (0, 1)")
        session.execute("BEGIN BATCH DELETE FROM test WHERE k=0 AND v=1; INSERT INTO test (k, v) VALUES (0, 2); APPLY BATCH")

        assert_one(session, "SELECT * FROM test", [0, 2])

    @pytest.mark.single_node
    def secondary_index_counters(self):
        session = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, c counter)")
        assert_invalid(session, "CREATE INDEX ON test(c)")

    @pytest.mark.single_node
    def test_column_name_validation(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k text,
                c int,
                v timeuuid,
                PRIMARY KEY (k, c)
            )
        """
        )

        assert_invalid(session, "INSERT INTO test(k, c) VALUES ('', 0)")

        # Insert a value that don't fit 'int'
        assert_invalid(session, "INSERT INTO test(k, c) VALUES (0, 10000000000)")

        # Insert a non-version 1 uuid
        assert_invalid(session, "INSERT INTO test(k, c, v) VALUES (0, 0, 550e8400-e29b-41d4-a716-446655440000)")

    @pytest.mark.single_node
    def test_bug_6327(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k, v)
            )
        """
        )

        session.execute("INSERT INTO test (k, v) VALUES (0, 0)")
        self.cluster.flush()
        assert_one(session, "SELECT v FROM test WHERE k=0 AND v IN (1, 0)", [0])

    @pytest.mark.single_node
    def test_large_count(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k)
            )
        """
        )

        session.default_fetch_size = 10000
        # We know we page at 10K, so test counting just before, at 10K, just after and
        # a bit after that.
        for k in range(1, 10000):
            session.execute("INSERT INTO test(k) VALUES (%d)" % k)

        assert_one(session, "SELECT COUNT(*) FROM test", [9999])

        session.execute("INSERT INTO test(k) VALUES (%d)" % 10000)

        assert_one(session, "SELECT COUNT(*) FROM test", [10000])

        session.execute("INSERT INTO test(k) VALUES (%d)" % 10001)

        assert_one(session, "SELECT COUNT(*) FROM test", [10001])

        for k in range(10002, 15001):
            session.execute("INSERT INTO test(k) VALUES (%d)" % k)

        assert_one(session, "SELECT COUNT(*) FROM test", [15000])

    @pytest.mark.single_node
    def test_nan_infinity(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (f float PRIMARY KEY)")

        session.execute("INSERT INTO test(f) VALUES (NaN)")
        session.execute("INSERT INTO test(f) VALUES (-NaN)")
        session.execute("INSERT INTO test(f) VALUES (Infinity)")
        session.execute("INSERT INTO test(f) VALUES (-Infinity)")

        selected = rows_to_list(session.execute("SELECT * FROM test"))

        # selected should be [[nan], [inf], [-inf]],
        # but assert element-wise because NaN != NaN
        assert len(selected) == 3
        assert len(selected[0]) == 1
        assert math.isnan(selected[0][0])
        assert selected[1] == [float("inf")]
        assert selected[2] == [float("-inf")]

    @pytest.mark.single_node
    def test_static_columns(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                v int,
                PRIMARY KEY (k, p)
            )
        """
        )

        session.execute("INSERT INTO test(k, s) VALUES (0, 42)")

        assert_one(session, "SELECT * FROM test", [0, None, 42, None])

        # Check that writetime works (#7081) -- we can't predict the exact value easily so
        # we just check that it's non zero
        row = session.execute("SELECT s, writetime(s) FROM test WHERE k=0")
        assert next(iter(row[0])) == 42 and list(row[0])[1] > 0, row

        session.execute("INSERT INTO test(k, p, s, v) VALUES (0, 0, 12, 0)")
        session.execute("INSERT INTO test(k, p, s, v) VALUES (0, 1, 24, 1)")

        # Check the static columns in indeed "static"
        assert_all(session, "SELECT * FROM test", [[0, 0, 24, 0], [0, 1, 24, 1]])

        # Check we do correctly get the static column value with a SELECT *, even
        # if we're only slicing part of the partition
        assert_one(session, "SELECT * FROM test WHERE k=0 AND p=0", [0, 0, 24, 0])
        assert_one(session, "SELECT * FROM test WHERE k=0 AND p=1", [0, 1, 24, 1])

        # Test for IN on the clustering key (#6769)
        assert_all(session, "SELECT * FROM test WHERE k=0 AND p IN (0, 1)", [[0, 0, 24, 0], [0, 1, 24, 1]])

        # Check things still work if we don't select the static column. We also want
        # this to not request the static columns internally at all, though that part
        # require debugging to assert
        assert_one(session, "SELECT p, v FROM test WHERE k=0 AND p=1", [1, 1])

        # Check selecting only a static column with distinct only yield one value
        # (as we only query the static columns)
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=0", [24])
        # But without DISTINCT, we still get one result per row
        assert_all(session, "SELECT s FROM test WHERE k=0", [[24], [24]])
        # but that querying other columns does correctly yield the full partition
        assert_all(session, "SELECT s, v FROM test WHERE k=0", [[24, 0], [24, 1]])
        assert_one(session, "SELECT s, v FROM test WHERE k=0 AND p=1", [24, 1])
        assert_one(session, "SELECT p, s FROM test WHERE k=0 AND p=1", [1, 24])
        assert_one(session, "SELECT k, p, s FROM test WHERE k=0 AND p=1", [0, 1, 24])

        # Check that deleting a row don't implicitely deletes statics
        session.execute("DELETE FROM test WHERE k=0 AND p=0")
        assert_all(session, "SELECT * FROM test", [[0, 1, 24, 1]])

        # But that explicitely deleting the static column does remove it
        session.execute("DELETE s FROM test WHERE k=0")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, 1]])

        # Check we can add a static column ...
        session.execute("ALTER TABLE test ADD s2 int static")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, None, 1]])
        session.execute("INSERT INTO TEST (k, p, s2, v) VALUES(0, 2, 42, 2)")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, 42, 1], [0, 2, None, 42, 2]])
        # ... and that we can drop it
        session.execute("ALTER TABLE test DROP s2")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, 1], [0, 2, None, 2]])

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_static_columns_cas(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                id int,
                k text,
                version int static,
                v text,
                PRIMARY KEY (id, k)
            )
        """
        )

        # Test that INSERT IF NOT EXISTS concerns only the static column if no clustering nor regular columns
        # is provided, but concerns the CQL3 row targetted by the clustering columns otherwise
        session.execute("INSERT INTO test(id, k, v) VALUES (1, 'foo', 'foo')")
        assert_one(session, "INSERT INTO test(id, k, version) VALUES (1, 'foo', 1) IF NOT EXISTS", [False, 1, "foo", None, "foo"])
        assert_one(session, "INSERT INTO test(id, version) VALUES (1, 1) IF NOT EXISTS", [True, 1, None, None, None])
        assert_one(session, "SELECT * FROM test", [1, "foo", 1, "foo"])
        session.execute("DELETE FROM test WHERE id = 1")

        session.execute("INSERT INTO test(id, version) VALUES (0, 0)")

        assert_one(session, "UPDATE test SET v='foo', version=1 WHERE id=0 AND k='k1' IF version = 0", [True, 0])
        assert_all(session, "SELECT * FROM test", [[0, "k1", 1, "foo"]])

        assert_one(session, "UPDATE test SET v='bar', version=1 WHERE id=0 AND k='k2' IF version = 0", [False, 1])
        assert_all(session, "SELECT * FROM test", [[0, "k1", 1, "foo"]])

        assert_one(session, "UPDATE test SET v='bar', version=2 WHERE id=0 AND k='k2' IF version = 1", [True, 1])
        assert_all(session, "SELECT * FROM test", [[0, "k1", 2, "foo"], [0, "k2", 2, "bar"]])

        # Testing batches
        assert_all(
            session,
            """
                     BEGIN BATCH
                       UPDATE test SET v='foobar' WHERE id=0 AND k='k1';
                       UPDATE test SET v='barfoo' WHERE id=0 AND k='k2';
                       UPDATE test SET version=3 WHERE id=0 IF version=1;
                     APPLY BATCH
                   """,
            [[False, 0, "k1", 2], [False, 0, None, 2], [False, 0, None, 2]],
        )

        assert_all(
            session,
            """
                     BEGIN BATCH
                       UPDATE test SET v='foobar' WHERE id=0 AND k='k1';
                       UPDATE test SET v='barfoo' WHERE id=0 AND k='k2';
                       UPDATE test SET version=3 WHERE id=0 IF version=2;
                     APPLY BATCH
                   """,
            [[True, 0, "k1", 2], [True, 0, None, 2], [True, 0, None, 2]],
        )
        assert_all(session, "SELECT * FROM test", [[0, "k1", 3, "foobar"], [0, "k2", 3, "barfoo"]])

        assert_all(
            session,
            """
                   BEGIN BATCH
                       UPDATE test SET version=4 WHERE id=0 IF version=3;
                       UPDATE test SET v='row1' WHERE id=0 AND k='k1' IF v='foo';
                       UPDATE test SET v='row2' WHERE id=0 AND k='k2' IF v='bar';
                   APPLY BATCH
                   """,
            [[False, 0, None, 3, None], [False, 0, "k1", 3, "foobar"], [False, 0, "k2", 3, "barfoo"]],
        )

        assert_all(
            session,
            """
                     BEGIN BATCH
                       UPDATE test SET version=4 WHERE id=0 IF version=3;
                       UPDATE test SET v='row1' WHERE id=0 AND k='k1' IF v='foobar';
                       UPDATE test SET v='row2' WHERE id=0 AND k='k2' IF v='barfoo';
                     APPLY BATCH
                   """,
            [[True, 0, None, 3, None], [True, 0, "k1", 3, "foobar"], [True, 0, "k2", 3, "barfoo"]],
        )
        assert_all(session, "SELECT * FROM test", [[0, "k1", 4, "row1"], [0, "k2", 4, "row2"]])

        assert_invalid(
            session,
            """
                         BEGIN BATCH
                           UPDATE test SET version=5 WHERE id=0 IF version=4;
                           UPDATE test SET v='row1' WHERE id=0 AND k='k1';
                           UPDATE test SET v='row2' WHERE id=1 AND k='k2';
                         APPLY BATCH
                       """,
        )

        assert_all(
            session,
            """
                     BEGIN BATCH
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k1', 'val1') IF NOT EXISTS;
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k2', 'val2') IF NOT EXISTS;
                     APPLY BATCH
                   """,
            [[True, None, None, None, None], [True, None, None, None, None]],
        )
        assert_all(session, "SELECT * FROM test WHERE id=1", [[1, "k1", None, "val1"], [1, "k2", None, "val2"]])

        assert_all(
            session,
            """
                     BEGIN BATCH
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k2', 'val2') IF NOT EXISTS;
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k3', 'val3') IF NOT EXISTS;
                     APPLY BATCH
                   """,
            [[False, 1, "k2", None, "val2"], [False, None, None, None, None]],
        )

        assert_all(
            session,
            """
                     BEGIN BATCH
                       UPDATE test SET v='newVal' WHERE id=1 AND k='k2' IF v='val0';
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k3', 'val3') IF NOT EXISTS;
                     APPLY BATCH
                   """,
            [[False, 1, "k2", None, "val2"], [False, None, None, None, None]],
        )
        assert_all(session, "SELECT * FROM test WHERE id=1", [[1, "k1", None, "val1"], [1, "k2", None, "val2"]])

        assert_all(
            session,
            """
                     BEGIN BATCH
                       UPDATE test SET v='newVal' WHERE id=1 AND k='k2' IF v='val2';
                       INSERT INTO TEST (id, k, v, version) VALUES(1, 'k3', 'val3', 1) IF NOT EXISTS;
                     APPLY BATCH
                   """,
            [[True, 1, "k2", None, "val2"], [True, None, None, None, None]],
        )
        assert_all(session, "SELECT * FROM test WHERE id=1", [[1, "k1", 1, "val1"], [1, "k2", 1, "newVal"], [1, "k3", 1, "val3"]])

        if Version(self.cluster.version()) >= Version("2.1"):
            assert_all(
                session,
                """
                         BEGIN BATCH
                           UPDATE test SET v='newVal1' WHERE id=1 AND k='k2' IF v='val2';
                           UPDATE test SET v='newVal2' WHERE id=1 AND k='k2' IF v='val3';
                         APPLY BATCH
                       """,
                [[False, 1, "k2", "newVal"], [False, 1, "k2", "newVal"]],
            )
        else:
            assert_invalid(
                session,
                """
                             BEGIN BATCH
                               UPDATE test SET v='newVal1' WHERE id=1 AND k='k2' IF v='val2';
                               UPDATE test SET v='newVal2' WHERE id=1 AND k='k2' IF v='val3';
                             APPLY BATCH
                           """,
            )

    @pytest.mark.single_node
    def test_static_columns_with_2i(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                v int,
                PRIMARY KEY (k, p)
            )
        """
        )

        session.execute("CREATE INDEX ON test(v)")

        session.execute("INSERT INTO test(k, p, s, v) VALUES (0, 0, 42, 1)")
        session.execute("INSERT INTO test(k, p, v) VALUES (0, 1, 1)")
        session.execute("INSERT INTO test(k, p, v) VALUES (0, 2, 2)")

        assert_all(session, "SELECT * FROM test WHERE v = 1", [[0, 0, 42, 1], [0, 1, 42, 1]])
        assert_all(session, "SELECT p, s FROM test WHERE v = 1", [[0, 42], [1, 42]])
        assert_all(session, "SELECT p FROM test WHERE v = 1", [[0], [1]])
        assert_all(session, "SELECT s FROM test WHERE v = 1", [[42], [42]])

    @pytest.mark.single_node
    def test_static_columns_with_distinct(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                PRIMARY KEY (k, p)
            )
        """
        )

        session.execute("INSERT INTO test (k, p) VALUES (1, 1)")
        session.execute("INSERT INTO test (k, p) VALUES (1, 2)")

        assert_all(session, "SELECT k, s FROM test", [[1, None], [1, None]])
        assert_one(session, "SELECT DISTINCT k, s FROM test", [1, None])
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=1", [None])
        assert_none(session, "SELECT DISTINCT s FROM test WHERE k=2")

        session.execute("INSERT INTO test (k, p, s) VALUES (2, 1, 3)")
        session.execute("INSERT INTO test (k, p) VALUES (2, 2)")

        assert_all(session, "SELECT k, s FROM test", [[1, None], [1, None], [2, 3], [2, 3]])
        assert_all(session, "SELECT DISTINCT k, s FROM test", [[1, None], [2, 3]])
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=1", [None])
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=2", [3])

        assert_invalid(session, "SELECT DISTINCT s FROM test")

        # paging to test for CASSANDRA-8108
        session.execute("TRUNCATE test")
        for i in range(10):
            for j in range(10):
                session.execute("INSERT INTO test (k, p, s) VALUES (%s, %s, %s)", (i, j, i))

        session.default_fetch_size = 7
        rows = list(session.execute("SELECT DISTINCT k, s FROM test"))
        assert list(range(10)) == sorted([r[0] for r in rows])
        assert list(range(10)) == sorted([r[1] for r in rows])

        keys = ",".join(map(str, range(10)))
        rows = list(session.execute(f"SELECT DISTINCT k, s FROM test WHERE k IN ({keys})"))
        assert list(range(10)) == [r[0] for r in rows]
        assert list(range(10)) == [r[1] for r in rows]

        # additional testing for CASSANRA-8087
        session.execute(
            """
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                s1 int static,
                s2 int static,
                PRIMARY KEY (k, c1, c2)
            )
        """
        )

        for i in range(10):
            for j in range(5):
                for k in range(5):
                    session.execute("INSERT INTO test2 (k, c1, c2, s1, s2) VALUES (%s, %s, %s, %s, %s)", (i, j, k, i, i + 1))

        for fetch_size in (None, 2, 5, 7, 10, 24, 25, 26, 1000):
            session.default_fetch_size = fetch_size
            rows = list(session.execute("SELECT DISTINCT k, s1 FROM test2"))
            assert list(range(10)) == sorted([r[0] for r in rows])
            assert list(range(10)) == sorted([r[1] for r in rows])

            rows = list(session.execute("SELECT DISTINCT k, s2 FROM test2"))
            assert list(range(10)) == sorted([r[0] for r in rows])
            assert list(range(1, 11)) == sorted([r[1] for r in rows])

            print("page size: ", fetch_size)
            rows = list(session.execute("SELECT DISTINCT k, s1 FROM test2 LIMIT 10"))
            assert list(range(10)) == sorted([r[0] for r in rows])
            assert list(range(10)) == sorted([r[1] for r in rows])

            keys = ",".join(map(str, range(10)))
            rows = list(session.execute(f"SELECT DISTINCT k, s1 FROM test2 WHERE k IN ({keys})"))
            assert list(range(10)) == [r[0] for r in rows]
            assert list(range(10)) == [r[1] for r in rows]

            keys = ",".join(map(str, range(10)))
            rows = list(session.execute(f"SELECT DISTINCT k, s2 FROM test2 WHERE k IN ({keys})"))
            assert list(range(10)) == [r[0] for r in rows]
            assert list(range(1, 11)) == [r[1] for r in rows]

            keys = ",".join(map(str, range(10)))
            rows = list(session.execute(f"SELECT DISTINCT k, s1 FROM test2 WHERE k IN ({keys}) LIMIT 10"))
            assert list(range(10)) == sorted([r[0] for r in rows])
            assert list(range(10)) == sorted([r[1] for r in rows])

    @pytest.mark.single_node
    def test_select_count_paging(self):
        """
        @jira_ticket CASSANDRA-6579
        Regression test for 'select count' paging bug.
        """

        session = self.prepare()
        session.execute("create table test(field1 text, field2 timeuuid, field3 boolean, primary key(field1, field2));")
        session.execute("create index test_index on test(field3);")

        session.execute("insert into test(field1, field2, field3) values ('hola', now(), false);")
        session.execute("insert into test(field1, field2, field3) values ('hola', now(), false);")

        if Version(self.cluster.version()) > Version("2.2"):
            assert_one(session, "select count(*) from test where field3 = false limit 1;", [2])
        else:
            assert_one(session, "select count(*) from test where field3 = false limit 1;", [1])

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_cas_and_ttl(self):
        session = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int, lock boolean)")

        session.execute("INSERT INTO test (k, v, lock) VALUES (0, 0, false)")
        session.execute("UPDATE test USING TTL 1 SET lock=true WHERE k=0")
        time.sleep(2)
        assert_one(session, "UPDATE test SET v = 1 WHERE k = 0 IF lock = null", [True, None])

    @pytest.mark.single_node
    def test_in_order_by_without_selecting(self):
        """Test that columns don't need to be selected for ORDER BY when there is a IN (#4911)"""

        cursor = self.prepare()
        cursor.default_fetch_size = None
        cursor.execute("CREATE TABLE test (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))")

        cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, 0, 0, 0)")
        cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, 0, 1, 1)")
        cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, 0, 2, 2)")
        cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (1, 1, 0, 3)")
        cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (1, 1, 1, 4)")
        cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (1, 1, 2, 5)")

        assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)", [[0, 0, 0, 0], [0, 0, 2, 2]])
        assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC, c2 ASC", [[0, 0, 0, 0], [0, 0, 2, 2]])

        # check that we don't need to select the column on which we order
        assert_all(cursor, "SELECT v FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)", [[0], [2]])
        assert_all(cursor, "SELECT v FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC", [[0], [2]])
        assert_all(cursor, "SELECT v FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC", [[2], [0]])
        if Version(self.cluster.version()) >= Version("2.2"):  # Scylla reports 2.2, but has 2.1 behavior.
            assert_all(cursor, "SELECT v FROM test WHERE k IN (1, 0)", [[0], [1], [2], [3], [4], [5]])
        else:
            assert_all(cursor, "SELECT v FROM test WHERE k IN (1, 0)", [[3], [4], [5], [0], [1], [2]])
        assert_all(cursor, "SELECT v FROM test WHERE k IN (1, 0) ORDER BY c1 ASC", [[0], [1], [2], [3], [4], [5]])

        # we should also be able to use functions in the select clause (additional test for CASSANDRA-8286)
        results = list(cursor.execute("SELECT writetime(v) FROM test WHERE k IN (1, 0) ORDER BY c1 ASC"))
        # since we don't know the write times, just assert that the order matches the order we expect
        assert results == list(sorted(results))

    @pytest.mark.single_node
    def test_tuple_notation(self):
        """
        @jira_ticket CASSANDRA-4851

        Test for new tuple syntax introduced in CASSANDRA-4851.
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3))")
        for i in range(2):
            for j in range(2):
                for k in range(2):
                    session.execute("INSERT INTO test(k, v1, v2, v3) VALUES (0, %d, %d, %d)" % (i, j, k))

        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0", [[0, 0, 0], [0, 0, 1], [0, 1, 0], [0, 1, 1], [1, 0, 0], [1, 0, 1], [1, 1, 0], [1, 1, 1]])

        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)", [[1, 0, 1], [1, 1, 0], [1, 1, 1]])
        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2) >= (1, 1)", [[1, 1, 0], [1, 1, 1]])
        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2) > (0, 1) AND (v1, v2, v3) <= (1, 1, 0)", [[1, 0, 0], [1, 0, 1], [1, 1, 0]])

        assert_invalid(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v3) > (1, 0)")

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_slicing(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) with compact storage")

        for i in range(3):
            session.execute("INSERT INTO test(k, c1, v) VALUES (0, %d, 1)" % (i))
            for j in range(3):
                session.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, %d, %d, 0)" % (i, j))

        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0", [[0, None, 1], [0, 0, 0], [0, 1, 0], [0, 2, 0], [1, None, 1], [1, 0, 0], [1, 1, 0], [1, 2, 0], [2, None, 1], [2, 0, 0], [2, 1, 0], [2, 2, 0]])

        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 >= 1 and c2 < 2", [[0, 1, 0]])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 > 1", [[0, 2, 0]])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 >= 1", [[0, 1, 0], [0, 2, 0]])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 < 2", [[0, None, 1], [0, 0, 0], [0, 1, 0]])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 <= 2", [[0, None, 1], [0, 0, 0], [0, 1, 0], [0, 2, 0]])

        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 < 1 and c2 > 2", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 < 1 and c2 >= 2", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 < 2 and c2 > 1", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 = 0 and c2 < 3 and c2 > 1", [[0, 2, 0]])

        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND (c1, c2) < (0, 1) and (c1, c2) > (0, 2)", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND (c1, c2) >= (0, 1) and (c1) < (0)", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND (c1, c2) >= (0, 1) and (c1) <= (0)", [[0, 1, 0], [0, 2, 0]])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND (c1, c2) < (0, 1) and (c1, c2) >= (0, 2)", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND (c1, c2) < (0, 2) and (c1, c2) > (0, 1)", [])
        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND (c1, c2) < (0, 3) and (c1, c2) > (0, 1)", [[0, 2, 0]])

        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 >= 0", [[0, None, 1], [0, 0, 0], [0, 1, 0], [0, 2, 0], [1, None, 1], [1, 0, 0], [1, 1, 0], [1, 2, 0], [2, None, 1], [2, 0, 0], [2, 1, 0], [2, 2, 0]])

        assert_all(
            session,
            "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 < 1",
            [
                [0, None, 1],
                [0, 0, 0],
                [0, 1, 0],
                [0, 2, 0],
            ],
        )

        assert_all(session, "SELECT c1, c2, v FROM test WHERE k = 0 AND c1 < 1 and c1 > 1", [])

    @pytest.mark.single_node
    def test_in_with_desc_order(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))")
        session.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 0)")
        session.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 1)")
        session.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 2)")

        assert_all(session, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC", [[0, 0, 2], [0, 0, 0]])

    @pytest.mark.single_node
    @pytest.mark.lwt
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_cas_and_compact(self):
        """
        @jira_ticket CASSANDRA-6813

        Test for CAS with compact storage table, and #6813 in particular.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE lock (
                partition text,
                key text,
                owner text,
                PRIMARY KEY (partition, key)
            ) WITH COMPACT STORAGE
        """
        )

        session.execute("INSERT INTO lock(partition, key, owner) VALUES ('a', 'b', null)")
        assert_one(session, "UPDATE lock SET owner='z' WHERE partition='a' AND key='b' IF owner=null", [True, None])

        assert_one(session, "UPDATE lock SET owner='b' WHERE partition='a' AND key='b' IF owner='a'", [False, "z"])
        assert_one(session, "UPDATE lock SET owner='b' WHERE partition='a' AND key='b' IF owner='z'", [True, "z"])

        assert_one(session, "INSERT INTO lock(partition, key, owner) VALUES ('a', 'c', 'x') IF NOT EXISTS", [True, None, None, None])

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_whole_list_conditional(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE tlist (
                k int PRIMARY KEY,
                l list<text>
            )"""
        )

        session.execute(
            """
            CREATE TABLE frozentlist (
                k int PRIMARY KEY,
                l frozen<list<text>>
            )"""
        )

        for frozen in (False, True):
            table = "frozentlist" if frozen else "tlist"
            session.execute(f"INSERT INTO {table}(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

            def check_applies(condition):
                assert_one(session, f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {condition}", [True, ["foo", "bar", "foobar"]])
                assert_one(session, f"SELECT * FROM {table}", [0, ["foo", "bar", "foobar"]])  # read back at default cl.one

            check_applies("l = ['foo', 'bar', 'foobar']")
            check_applies("l != ['baz']")
            check_applies("l > ['a']")
            check_applies("l >= ['a']")
            check_applies("l < ['z']")
            check_applies("l <= ['z']")
            check_applies("l IN (null, ['foo', 'bar', 'foobar'], ['a'])")
            # multiple conditions
            check_applies("l > ['aaa', 'bbb'] AND l > ['aaa']")
            check_applies("l != null AND l IN (['foo', 'bar', 'foobar'])")

            def check_does_not_apply(condition):
                assert_one(session, f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {condition}", [False, ["foo", "bar", "foobar"]])
                assert_one(session, f"SELECT * FROM {table}", [0, ["foo", "bar", "foobar"]])  # read back at default cl.one

            # should not apply
            check_does_not_apply("l = ['baz']")
            check_does_not_apply("l != ['foo', 'bar', 'foobar']")
            check_does_not_apply("l > ['z']")
            check_does_not_apply("l >= ['z']")
            check_does_not_apply("l < ['a']")
            check_does_not_apply("l <= ['a']")
            check_does_not_apply("l IN (['a'], null)")
            check_does_not_apply("l IN ()")
            # multiple conditions
            check_does_not_apply("l IN () AND l IN (['foo', 'bar', 'foobar'])")
            check_does_not_apply("l > ['zzz'] AND l < ['zzz']")

            def check_invalid(condition, expected=InvalidRequest):
                assert_invalid(session, f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {condition}", expected=expected)
                assert_one(session, f"SELECT * FROM {table}", [0, ["foo", "bar", "foobar"]])

            check_does_not_apply("l = [null]")
            check_invalid("l < null")
            check_invalid("l <= null")
            check_invalid("l > null")
            check_invalid("l >= null")
            check_invalid("l IN null", expected=SyntaxException)
            check_invalid("l IN 367", expected=SyntaxException)
            check_invalid("l CONTAINS KEY 123", expected=SyntaxException)
            # not supported yet
            check_invalid("m CONTAINS 'bar'", expected=SyntaxException)

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_list_item_conditional(self):
        # Lists
        session = self.prepare()

        frozen_values = (False, True) if Version(self.cluster.version()) >= Version("2.1.3") else (False,)
        for frozen in frozen_values:
            session.execute("DROP TABLE IF EXISTS tlist")

            session.execute(
                """
                CREATE TABLE tlist (
                    k int PRIMARY KEY,
                    l {},
                )""".format("frozen<list<text>>" if frozen else "list<text>")
            )

            session.execute("INSERT INTO tlist(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[null] = 'foobar'", [False, ["foo", "bar", "foobar"]])
            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[-2] = 'foobar'", [False, ["foo", "bar", "foobar"]])
            if Version(self.cluster.version()) < Version("2.1"):
                # no longer invalid after CASSANDRA-6839
                assert_invalid(session, "DELETE FROM tlist WHERE k=0 IF l[3] = 'foobar'")
            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[1] = null", [False, ["foo", "bar", "foobar"]])
            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[1] = 'foobar'", [False, ["foo", "bar", "foobar"]])
            assert_one(session, "SELECT * FROM tlist", [0, ["foo", "bar", "foobar"]])

            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[1] = 'bar'", [True, ["foo", "bar", "foobar"]])
            assert_none(session, "SELECT * FROM tlist")

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_expanded_list_item_conditional(self):
        """
        expanded functionality from CASSANDRA-6839
        @jira_ticket CASSANDRA-6839
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE tlist (
                k int PRIMARY KEY,
                l list<text>
            )"""
        )

        session.execute(
            """
            CREATE TABLE frozentlist (
                k int PRIMARY KEY,
                l frozen<list<text>>
            )"""
        )

        for frozen in (False, True):
            table = "frozentlist" if frozen else "tlist"
            session.execute(f"INSERT INTO {table}(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

            def check_applies(condition):
                assert_one(session, f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {condition}", [True, ["foo", "bar", "foobar"]])
                assert_one(session, f"SELECT * FROM {table}", [0, ["foo", "bar", "foobar"]])

            check_applies("l[1] < 'zzz'")
            check_applies("l[1] <= 'bar'")
            check_applies("l[1] > 'aaa'")
            check_applies("l[1] >= 'bar'")
            check_applies("l[1] != 'xxx'")
            check_applies("l[1] != null")
            check_applies("l[1] IN (null, 'xxx', 'bar')")
            check_applies("l[1] > 'aaa' AND l[1] < 'zzz'")
            # check beyond end of list
            check_applies("l[3] = null")
            check_applies("l[3] IN (null, 'xxx', 'bar')")

            def check_does_not_apply(condition):
                assert_one(session, f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {condition}", [False, ["foo", "bar", "foobar"]])
                assert_one(session, f"SELECT * FROM {table}", [0, ["foo", "bar", "foobar"]])

            check_does_not_apply("l[1] < 'aaa'")
            check_does_not_apply("l[1] <= 'aaa'")
            check_does_not_apply("l[1] > 'zzz'")
            check_does_not_apply("l[1] >= 'zzz'")
            check_does_not_apply("l[1] != 'bar'")
            check_does_not_apply("l[1] IN (null, 'xxx')")
            check_does_not_apply("l[1] IN ()")
            check_does_not_apply("l[1] != null AND l[1] IN ()")
            # check beyond end of list
            check_does_not_apply("l[3] != null")
            check_does_not_apply("l[3] = 'xxx'")

            def check_invalid(condition, expected=InvalidRequest):
                assert_invalid(session, f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {condition}", expected=expected)
                assert_one(session, f"SELECT * FROM {table}", [0, ["foo", "bar", "foobar"]])

            check_invalid("l[1] < null")
            check_invalid("l[1] <= null")
            check_invalid("l[1] > null")
            check_invalid("l[1] >= null")
            check_invalid("l[1] IN null", expected=SyntaxException)
            check_invalid("l[1] IN 367", expected=SyntaxException)
            check_invalid("l[1] IN (1, 2, 3)")
            check_invalid("l[1] CONTAINS 367", expected=SyntaxException)
            check_invalid("l[1] CONTAINS KEY 367", expected=SyntaxException)
            check_applies("l[null] = null")

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_whole_set_conditional(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE tset (
                k int PRIMARY KEY,
                s set<text>
            )"""
        )

        session.execute(
            """
            CREATE TABLE frozentset (
                k int PRIMARY KEY,
                s frozen<set<text>>
            )"""
        )

        for frozen in (False, True):
            table = "frozentset" if frozen else "tset"
            assert_one(session, f"INSERT INTO {table}(k, s) VALUES (0, {{'bar', 'foo'}}) IF NOT EXISTS", [True, None, None])

            def check_applies(condition):
                assert_one(session, f"UPDATE {table} SET s = {{'bar', 'foo'}} WHERE k=0 IF {condition}", [True, set({"bar", "foo"})])
                assert_one(session, f"SELECT * FROM {table}", [0, set(["bar", "foo"])], cl=ConsistencyLevel.QUORUM)

            check_applies("s = {'bar', 'foo'}")
            check_applies("s = {'foo', 'bar'}")
            check_applies("s != {'baz'}")
            check_applies("s > {'a'}")
            check_applies("s >= {'a'}")
            check_applies("s < {'z'}")
            check_applies("s <= {'z'}")
            check_applies("s IN (null, {'bar', 'foo'}, {'a'})")
            # multiple conditions
            check_applies("s > {'a'} AND s < {'z'}")
            check_applies("s IN (null, {'bar', 'foo'}, {'a'}) AND s IN ({'a'}, {'bar', 'foo'}, null)")

            def check_does_not_apply(condition):
                assert_one(session, f"UPDATE {table} SET s = {{'bar', 'foo'}} WHERE k=0 IF {condition}", [False, set({"bar", "foo"})])
                assert_one(session, f"SELECT * FROM {table}", [0, {"bar", "foo"}], cl=ConsistencyLevel.QUORUM)

            # should not apply
            check_does_not_apply("s = {'baz'}")
            check_does_not_apply("s != {'bar', 'foo'}")
            check_does_not_apply("s > {'z'}")
            check_does_not_apply("s >= {'z'}")
            check_does_not_apply("s < {'a'}")
            check_does_not_apply("s <= {'a'}")
            check_does_not_apply("s IN ({'a'}, null)")
            check_does_not_apply("s IN ()")
            check_does_not_apply("s != null AND s IN ()")

            def check_invalid(condition, expected=InvalidRequest):
                assert_invalid(session, f"UPDATE {table} SET s = {{'bar', 'foo'}} WHERE k=0 IF {condition}", expected=expected)
                assert_one(session, f"SELECT * FROM {table}", [0, {"bar", "foo"}], cl=ConsistencyLevel.QUORUM)

            check_invalid("s = {null}")
            check_invalid("s < null")
            check_invalid("s <= null")
            check_invalid("s > null")
            check_invalid("s >= null")
            check_invalid("s IN null", expected=SyntaxException)
            check_invalid("s IN 367", expected=SyntaxException)
            check_invalid("s CONTAINS KEY 123", expected=SyntaxException)
            # not supported yet
            check_invalid("m CONTAINS 'bar'", expected=SyntaxException)

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_whole_map_conditional(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE tmap (
                k int PRIMARY KEY,
                m map<text, text>
            )"""
        )

        session.execute(
            """
            CREATE TABLE frozentmap (
                k int PRIMARY KEY,
                m frozen<map<text, text>>
            )"""
        )

        for frozen in (False, True):
            logger.debug("Testing {} maps".format("frozen" if frozen else "normal"))
            table = "frozentmap" if frozen else "tmap"
            session.execute(f"INSERT INTO {table}(k, m) VALUES (0, {{'foo' : 'bar'}})")

            def check_applies(condition):
                assert_one(session, f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k=0 IF {condition}", [True, {"foo": "bar"}])
                assert_one(session, f"SELECT * FROM {table}", [0, {"foo": "bar"}], cl=ConsistencyLevel.QUORUM)

            check_applies("m = {'foo': 'bar'}")
            check_applies("m > {'a': 'a'}")
            check_applies("m >= {'a': 'a'}")
            check_applies("m < {'z': 'z'}")
            check_applies("m <= {'z': 'z'}")
            check_applies("m != {'a': 'a'}")
            check_applies("m IN (null, {'a': 'a'}, {'foo': 'bar'})")
            # multiple conditions
            check_applies("m > {'a': 'a'} AND m < {'z': 'z'}")
            check_applies("m != null AND m IN (null, {'a': 'a'}, {'foo': 'bar'})")

            def check_does_not_apply(condition):
                assert_one(session, f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k=0 IF {condition}", [False, {"foo": "bar"}])
                assert_one(session, f"SELECT * FROM {table}", [0, {"foo": "bar"}], cl=ConsistencyLevel.QUORUM)

            # should not apply
            check_does_not_apply("m = {'a': 'a'}")
            check_does_not_apply("m > {'z': 'z'}")
            check_does_not_apply("m >= {'z': 'z'}")
            check_does_not_apply("m < {'a': 'a'}")
            check_does_not_apply("m <= {'a': 'a'}")
            check_does_not_apply("m != {'foo': 'bar'}")
            check_does_not_apply("m IN ({'a': 'a'}, null)")
            check_does_not_apply("m IN ()")
            check_does_not_apply("m = null AND m != null")

            def check_invalid(condition, expected=InvalidRequest):
                assert_invalid(session, f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k=0 IF {condition}", expected=expected)
                assert_one(session, f"SELECT * FROM {table}", [0, {"foo": "bar"}], cl=ConsistencyLevel.QUORUM)

            check_invalid("m = {null: null}")
            check_invalid("m = {'a': null}")
            check_invalid("m = {null: 'a'}")
            check_invalid("m < null")
            check_invalid("m IN null", expected=SyntaxException)
            # not supported yet
            check_invalid("m CONTAINS 'bar'", expected=SyntaxException)
            check_invalid("m CONTAINS KEY 'foo'", expected=SyntaxException)
            check_invalid("m CONTAINS null", expected=SyntaxException)
            check_invalid("m CONTAINS KEY null", expected=SyntaxException)

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_map_item_conditional(self):
        session = self.prepare()

        frozen_values = (False, True) if Version(self.cluster.version()) >= Version("2.1.3") else (False,)
        for frozen in frozen_values:
            session.execute("DROP TABLE IF EXISTS tmap")

            session.execute(
                """
                CREATE TABLE tmap (
                    k int PRIMARY KEY,
                    m {}
                )""".format("frozen<map<text, text>>" if frozen else "map<text, text>")
            )

            session.execute("INSERT INTO tmap(k, m) VALUES (0, {'foo' : 'bar'})")
            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m[null] = 'foo'", [False, {"foo": "bar"}])
            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m['foo'] = 'foo'", [False, {"foo": "bar"}])
            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m['foo'] = null", [False, {"foo": "bar"}])
            assert_one(session, "SELECT * FROM tmap", [0, {"foo": "bar"}])

            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m['foo'] = 'bar'", [True, {"foo": "bar"}])
            assert_none(session, "SELECT * FROM tmap")

            if Version(self.cluster.version()) > Version("2.1.1"):
                session.execute("INSERT INTO tmap(k, m) VALUES (1, null)")
                if frozen:
                    assert_invalid(session, "UPDATE tmap set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)")
                else:
                    assert_one(session, "UPDATE tmap set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)", [True, None])

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_expanded_map_item_conditional(self):
        """
        Expanded functionality from CASSANDRA-6839
        @jira_ticket CASSANDRA-6839
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE tmap (
                k int PRIMARY KEY,
                m map<text, text>
            )"""
        )

        session.execute(
            """
            CREATE TABLE frozentmap (
                k int PRIMARY KEY,
                m frozen<map<text, text>>
            )"""
        )

        for frozen in (False, True):
            logger.debug("Testing {} maps".format("frozen" if frozen else "normal"))
            table = "frozentmap" if frozen else "tmap"
            session.execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})" % table)

            def check_applies(condition):
                assert_one(session, f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k=0 IF {condition}", [True, {"foo": "bar"}])
                assert_one(session, f"SELECT * FROM {table}", [0, {"foo": "bar"}], cl=ConsistencyLevel.QUORUM)

            check_applies("m['xxx'] = null")
            check_applies("m['foo'] < 'zzz'")
            check_applies("m['foo'] <= 'bar'")
            check_applies("m['foo'] > 'aaa'")
            check_applies("m['foo'] >= 'bar'")
            check_applies("m['foo'] != 'xxx'")
            check_applies("m['foo'] != null")
            check_applies("m['foo'] IN (null, 'xxx', 'bar')")
            check_applies("m['xxx'] IN (null, 'xxx', 'bar')")  # m['xxx'] is not set
            # multiple conditions
            check_applies("m['foo'] < 'zzz' AND m['foo'] > 'aaa'")

            def check_does_not_apply(condition):
                assert_one(session, f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k=0 IF {condition}", [False, {"foo": "bar"}])
                assert_one(session, f"SELECT * FROM {table}", [0, {"foo": "bar"}], cl=ConsistencyLevel.QUORUM)

            check_does_not_apply("m['foo'] < 'aaa'")
            check_does_not_apply("m['foo'] <= 'aaa'")
            check_does_not_apply("m['foo'] > 'zzz'")
            check_does_not_apply("m['foo'] >= 'zzz'")
            check_does_not_apply("m['foo'] != 'bar'")
            check_does_not_apply("m['xxx'] != null")  # m['xxx'] is not set
            check_does_not_apply("m['foo'] IN (null, 'xxx')")
            check_does_not_apply("m['foo'] IN ()")
            check_does_not_apply("m['foo'] != null AND m['foo'] = null")

            def check_invalid(condition, expected=InvalidRequest):
                assert_invalid(session, f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k=0 IF {condition}", expected=expected)
                assert_one(session, f"SELECT * FROM {table}", [0, {"foo": "bar"}])

            check_invalid("m['foo'] < null")
            check_invalid("m['foo'] <= null")
            check_invalid("m['foo'] > null")
            check_invalid("m['foo'] >= null")
            check_invalid("m['foo'] IN null", expected=SyntaxException)
            check_invalid("m['foo'] IN 367", expected=SyntaxException)
            check_invalid("m['foo'] IN (1, 2, 3)")
            check_invalid("m['foo'] CONTAINS 367", expected=SyntaxException)
            check_invalid("m['foo'] CONTAINS KEY 367", expected=SyntaxException)
            check_applies("m[null] = null")

    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_cas_and_list_index(self):
        """
        @jira_ticket CASSANDRA-7499
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v text,
                l list<text>
            )
        """
        )

        session.execute("INSERT INTO test(k, v, l) VALUES(0, 'foobar', ['foi', 'bar'])")

        assert_one(session, "UPDATE test SET l[0] = 'foo' WHERE k = 0 IF v = 'barfoo'", [False, "foobar"])
        assert_one(session, "UPDATE test SET l[0] = 'foo' WHERE k = 0 IF v = 'foobar'", [True, "foobar"])

        # since we write at all, and LWT update (serial), we need to read back at serial (or higher)
        assert_one(session, "SELECT * FROM test", [0, ["foo", "bar"], "foobar"], cl=ConsistencyLevel.QUORUM)

    @pytest.mark.single_node
    def test_static_with_limit(self):
        """
        @jira_ticket CASSANDRA-6956

        Test LIMIT when static columns are present.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                s int static,
                v int,
                PRIMARY KEY (k, v)
            )
        """
        )

        session.execute("INSERT INTO test(k, s) VALUES(0, 42)")
        for i in range(4):
            session.execute("INSERT INTO test(k, v) VALUES(0, %d)" % i)

        assert_one(session, "SELECT * FROM test WHERE k = 0 LIMIT 1", [0, 0, 42])
        assert_all(session, "SELECT * FROM test WHERE k = 0 LIMIT 2", [[0, 0, 42], [0, 1, 42]])
        assert_all(session, "SELECT * FROM test WHERE k = 0 LIMIT 3", [[0, 0, 42], [0, 1, 42], [0, 2, 42]])

    @pytest.mark.single_node
    def test_static_with_empty_clustering(self):
        """
        @jira_ticket CASSANDRA-7455

        Regression test for CASSANDRA-7455.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test(
                pkey text,
                ckey text,
                value text,
                static_value text static,
                PRIMARY KEY(pkey, ckey)
            )
        """
        )

        session.execute("INSERT INTO test(pkey, static_value) VALUES ('partition1', 'static value')")
        session.execute("INSERT INTO test(pkey, ckey, value) VALUES('partition1', '', 'value')")

        assert_one(session, "SELECT * FROM test", ["partition1", "", "static value", "value"])

    @pytest.mark.single_node
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def limit_compact_table(self):
        """
        @jira_ticket CASSANDRA-7052
        @jira_ticket CASSANDRA-7059

        Regression test for CASSANDRA-7052.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k, v)
            ) WITH COMPACT STORAGE
        """
        )

        for i in range(4):
            for j in range(4):
                session.execute("INSERT INTO test(k, v) VALUES (%d, %d)" % (i, j))

        assert_all(session, "SELECT v FROM test WHERE k=0 AND v > 0 AND v <= 4 LIMIT 2", [[1], [2]])
        assert_all(session, "SELECT v FROM test WHERE k=0 AND v > -1 AND v <= 4 LIMIT 2", [[0], [1]])

        assert_all(session, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2", [[0, 1], [0, 2]])
        assert_all(session, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2", [[0, 0], [0, 1]])
        assert_all(session, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6", [[0, 1], [0, 2], [0, 3], [1, 1], [1, 2], [1, 3]])

        # Introduced in CASSANDRA-7059
        assert_invalid(session, "SELECT * FROM test WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING")

    @pytest.mark.single_node
    def key_index_with_reverse_clustering(self):
        """
        @jira_ticket CASSANDRA-6950

        Regression test for CASSANDRA-6950.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY ((k1, k2), v)
            ) WITH CLUSTERING ORDER BY (v DESC)
        """
        )

        session.execute("CREATE INDEX ON test(k2)")

        session.execute("INSERT INTO test(k1, k2, v) VALUES (0, 0, 1)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (0, 1, 2)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (0, 0, 3)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (1, 0, 4)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (1, 1, 5)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (2, 0, 7)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (2, 1, 8)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (3, 0, 1)")

        assert_all(session, "SELECT * FROM test WHERE k2 = 0 AND v >= 2 ALLOW FILTERING", [[2, 0, 7], [0, 0, 3], [1, 0, 4]])

    @pytest.mark.single_node
    def test_clustering_order_in(self):
        """
        @jira_ticket CASSANDRA-7105

        Regression test for CASSANDRA-7105.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                a int,
                b int,
                c int,
                PRIMARY KEY ((a, b), c)
            ) with clustering order by (c desc)
        """
        )

        session.execute("INSERT INTO test (a, b, c) VALUES (1, 2, 3)")
        session.execute("INSERT INTO test (a, b, c) VALUES (4, 5, 6)")

        assert_one(session, "SELECT * FROM test WHERE a=1 AND b=2 AND c IN (3)", [1, 2, 3])
        assert_one(session, "SELECT * FROM test WHERE a=1 AND b=2 AND c IN (3, 4)", [1, 2, 3])

    @pytest.mark.single_node
    def test_bug7105(self):
        """
        @jira_ticket CASSANDRA-7105

        Regression test for CASSANDRA-7105.
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                a int,
                b int,
                c int,
                d int,
                PRIMARY KEY (a, b)
            )
        """
        )

        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 3, 3)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 6, 5)")

        assert_one(session, "SELECT * FROM test WHERE a=1 AND b=2 ORDER BY b DESC", [1, 2, 3, 3])

    @pytest.mark.single_node
    def test_conditional_ddl_index(self):
        session = self.prepare(create_keyspace=False)

        create_ks(session, "my_test_ks", 1)

        session.execute(
            """
            CREATE TABLE my_test_table (
            id text PRIMARY KEY,
            value1 blob,
            value2 blob) with comment = 'foo';
            """
        )

        # try dropping when doesn't exist
        session.execute("DROP INDEX IF EXISTS myindex")

        # create and confirm
        session.execute("CREATE INDEX IF NOT EXISTS myindex ON my_test_table (value1)")

        # index building is asynch, wait for it to finish
        for i in range(10):
            results = session.execute("""select index_name from system."IndexInfo" where table_name = 'my_test_ks'""")

            if results:
                assert "myindex" == results.one()[0], results
                break

            time.sleep(0.5)
        else:
            # this is executed when 'break' is never called
            self.fail("Didn't see my_test_table.myindex after polling for 5 seconds")

        # unsuccessful create since it's already there
        session.execute("CREATE INDEX IF NOT EXISTS myindex ON my_test_table (value1)")

        # drop and confirm
        session.execute("DROP INDEX IF EXISTS myindex")
        assert_none(session, """select index_name from system."IndexInfo" where table_name = 'my_test_ks'""")

    @pytest.mark.single_node
    def test_bug_6612(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE session_data (
                username text,
                session_id text,
                app_name text,
                account text,
                last_access timestamp,
                created_on timestamp,
                PRIMARY KEY (username, session_id, app_name, account)
            );
        """
        )

        session.execute("create index sessionIndex ON session_data (session_id)")
        session.execute("create index sessionAppName ON session_data (app_name)")
        session.execute("create index lastAccessIndex ON session_data (last_access)")

        assert_one(session, "select count(*) from session_data where app_name='foo' and account='bar' and last_access > 4 allow filtering", [0])

        session.execute("insert into session_data (username, session_id, app_name, account, last_access, created_on) values ('toto', 'foo', 'foo', 'bar', 12, 13)")

        assert_one(session, "select count(*) from session_data where app_name='foo' and account='bar' and last_access > 4 allow filtering", [1])

    @pytest.mark.single_node
    def test_blobAs_functions(self):  # noqa: N802
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            );
        """
        )

        # A blob that is not 4 bytes should be rejected
        assert_invalid(session, "INSERT INTO test(k, v) VALUES (0, blobAsInt(0x01))")

    @pytest.mark.single_node
    def test_alter_clustering_and_static(self):
        session = self.prepare()

        session.execute("CREATE TABLE foo (bar int, PRIMARY KEY (bar))")

        # We shouldn't allow static when there is not clustering columns
        assert_invalid(session, "ALTER TABLE foo ADD bar2 text static")

    @pytest.mark.single_node
    def test_alter_with_multiple_columns(self):
        session = self.prepare()

        session.execute("CREATE TABLE foo (bar int, PRIMARY KEY (bar))")
        session.execute("ALTER TABLE foo ADD (c text, d int)")
        session.execute("INSERT INTO foo (bar, c, d) VALUES (1, 'hello', 100)")

    @pytest.mark.single_node
    def test_drop_and_readd_collection(self):
        """
        @jira_ticket CASSANDRA-6276
        """
        session = self.prepare()

        session.execute("create table test (k int primary key, v set<text>, x int)")
        session.execute("insert into test (k, v) VALUES (0, {'fffffffff'})")
        self.cluster.flush()
        session.execute("alter table test drop v")
        assert_invalid(session, "alter table test add v set<int>")

    @pytest.mark.single_node
    def test_downgrade_to_compact_bug(self):
        """
        @jira_ticket CASSANDRA-7744
        """
        session = self.prepare()

        session.execute("create table test (k int primary key, v set<text>)")
        session.execute("insert into test (k, v) VALUES (0, {'f'})")
        self.cluster.flush()
        session.execute("alter table test drop v")
        session.execute("alter table test add v int")

    @pytest.mark.single_node
    def test_invalid_string_literals(self):
        """
        @jira_ticket CASSANDRA-8101

        - assert INSERTing into a nonexistent table fails normally, with an InvalidRequest exception
        - create a table with ascii and text columns
        - assert that trying to execute an insert statement with non-UTF8 contents raises a ProtocolException
            - tries to insert into a nonexistent column to make sure the ProtocolException is raised over other errors
        """
        session = self.prepare()
        # this should fail as normal, not with a ProtocolException
        assert_invalid(session, "insert into invalid_string_literals (k, a) VALUES (0, '\u038e\u0394\u03b4\u03e0')")

        session = self.patient_cql_connection(self.cluster.nodelist()[0], keyspace="ks")
        session.execute("create table invalid_string_literals (k int primary key, a ascii, b text)")

        # this should still fail with an InvalidRequest
        assert_invalid(session, "insert into invalid_string_literals (k, c) VALUES (0, '\u038e\u0394\u03b4\u03e0')")

        # try to insert utf-8 characters into an ascii column and make sure it fails
        assert_invalid(session, "insert into invalid_string_literals (k, a) VALUES (0, '\xe0\x80\x80')", expected=InvalidRequest, matching="(Invalid ASCII character in string literal|Value not compatible)")

    @pytest.mark.single_node
    def test_negative_timestamp(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) USING TIMESTAMP -42")

        assert_one(session, "SELECT writetime(v) FROM TEST WHERE k = 1", [-42])

    @pytest.mark.single_node
    def test_bug_8558(self):
        session = self.prepare()
        node1 = self.cluster.nodelist()[0]

        session.execute("CREATE  KEYSPACE space1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute("CREATE  TABLE space1.table1(a int, b int, c text,primary key(a,b))")
        session.execute("INSERT INTO space1.table1(a,b,c) VALUES(1,1,'1')")
        node1.nodetool("flush")
        session.execute("DELETE FROM space1.table1 where a=1 and b=1")
        node1.nodetool("flush")

        assert_none(session, "select * from space1.table1 where a=1 and b=1")

    @pytest.mark.single_node
    def test_bug_5732(self, request):
        session = self.prepare(use_cache=True)

        session.execute(
            """
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """
        )

        session.execute("ALTER TABLE test WITH CACHING={'keys':'ALL','rows_per_partition':'ALL'}")
        session.execute("INSERT INTO test (k,v) VALUES (0,0)")
        session.execute("INSERT INTO test (k,v) VALUES (1,1)")
        session.execute("CREATE INDEX testindex on test(v)")

        # wait for the index to be fully built
        start = time.time()
        while True:
            results = session.execute("""SELECT index_name FROM system."IndexInfo" WHERE table_name = 'ks'""")
            if results:
                assert "testindex" == results.one()[0], results
                break

            if time.time() - start > 10.0:
                results = list(session.execute('SELECT * FROM system."IndexInfo"'))
                raise Exception(f"Failed to build secondary index within ten seconds: {results}")
            time.sleep(0.1)

        assert_all(session, "SELECT k FROM test WHERE v = 0", [[0]])

        self.cluster.stop()
        time.sleep(0.5)
        self.cluster.start()
        time.sleep(0.5)

        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        assert_all(session, "SELECT k FROM ks.test WHERE v = 0", [[0]])

    @pytest.mark.single_node
    def test_double_with_npe(self):
        """
        @jira_ticket CASSANDRA-9565

        Regression test for a null pointer exception that occurred in the CQL
        parser when parsing a statement that erroneously used 'WITH WITH'.
        """
        session = self.prepare()
        statements = ["ALTER KEYSPACE WITH WITH DURABLE_WRITES = true", "ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true", "CREATE KEYSPACE WITH WITH DURABLE_WRITES = true", "CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true"]

        for s in statements:
            session.execute("DROP KEYSPACE IF EXISTS ks")
            try:
                session.execute(s)
            except Exception as e:  # noqa: BLE001
                assert isinstance(e, SyntaxException)
                assert "NullPointerException" not in str(e)

    @pytest.mark.single_node
    def test_cql_versions_collections(self):
        for p in range(4, 5):
            session = self.prepare(protocol_version=p)

            session.execute(
                """
            CREATE TABLE cql2ct ( a int PRIMARY KEY, b list<int>, c map<int, int>, d set<int> );
            """
            )
            # Note: would use bind, but having issues with it and sets/maps...
            for i in range(4):
                session.execute(
                    f"""
                INSERT INTO cql2ct (a, b, c, d) values ({i}, [{i},{i + 1}], {{{i}:{i + 1}}}, {{{i},{i + 1}}});
                """
                )

            unsorted_res = list(
                session.execute(
                    """
            SELECT * FROM cql2ct
            """
                )
            )
            res = sorted(unsorted_res)
            assert len(res) == 4, res
            sres = rows_to_list(res)
            for i in range(4):
                assert sres[i][0] == i, sres[i]
                assert sres[i][1] == [i, i + 1], sres[i]
                assert sres[i][2] == {i: i + 1}
                assert sres[i][3] == {i, i + 1}

            session.execute("DROP KEYSPACE IF EXISTS ks")

    @pytest.mark.single_node
    def test_cql_versions_batch(self):
        for p in range(4, 4):
            session = self.prepare(protocol_version=p)
            session.execute(
                """
            CREATE TABLE dogs (
            dogid int PRIMARY KEY,
            dogname text,
            );
            """
            )
            session.execute(
                """
            CREATE TABLE users (
            id int,
            firstname text,
            lastname text,
            PRIMARY KEY (id)
            );
            """
            )

            session.execute(
                """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO dogs (dogid, dogname) VALUES (0, 'Pluto')
            APPLY BATCH
            """
            )

            assert_one(session, "SELECT * FROM users", [0, "Jack", "Sparrow"])
            assert_one(session, "SELECT * FROM dogs", [0, "Pluto"])
            session.execute("DROP KEYSPACE IF EXISTS ks")

    @pytest.mark.single_node
    def test_bop_order(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int,
            )
        """
        )

        session.execute("INSERT INTO test (k, v) VALUES ('c1', 0)")
        session.execute("INSERT INTO test (k, v) VALUES ('a1', 1)")
        session.execute("INSERT INTO test (k, v) VALUES ('b1', 2)")
        session.execute("INSERT INTO test (k, v) VALUES ('z', 3)")
        session.execute("INSERT INTO test (k, v) VALUES ('g1', 4)")
        session.execute("INSERT INTO test (k, v) VALUES ('1', 5)")
        session.execute("INSERT INTO test (k, v) VALUES ('1000', 6)")
        session.execute("INSERT INTO test (k, v) VALUES ('2', 7)")

        res = session.execute("SELECT v FROM test")
        assert rows_to_list(res) == [[3], [0], [2], [6], [4], [7], [1], [5]], list(res)

    @pytest.mark.single_node
    def collection_column_can_replace_dropped_non_collection_column(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int,
            )
        """
        )

        session.execute("INSERT INTO test (k, v) VALUES ('k', 10)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["k", 10]], list(res)

        session.execute("""ALTER TABLE test DROP v""")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["k"]], list(res)

        session.execute("""ALTER TABLE test ADD v list<int>""")

        session.execute("INSERT INTO test (k, v) VALUES ('k', [8])")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["k", [8]]], list(res)

        session.execute("""ALTER TABLE test DROP v""")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [["k"]], list(res)

        assert_invalid(session, "ALTER TABLE test ADD v list<text>", expected=InvalidRequest)

    def mc_prepare_table(  # noqa: PLR0913
        self,
        nodes,
        keyspace_name,
        table_name,
        dataset,
        data_amount,
        columns=None,
        keys_amount=3,
        rf=4,
        compaction_options=None,
    ):
        if columns is None:
            columns = ['"ID"', '"Ck1"', '"cK2"', '"Columnfamily_for_mc_sstables_column1"']
        session = self.prepare(create_keyspace=False, nodes=nodes, rf=4)
        session.consistency_level = "QUORUM"
        create_ks(session=session, name=keyspace_name, rf=rf)
        session.execute(f"USE {keyspace_name}")
        columns_desc = " int, ".join(columns)
        keys_desc = ", ".join(columns[:keys_amount])
        query = f"CREATE TABLE {table_name} ({columns_desc} int, PRIMARY KEY ({keys_desc}))"
        if compaction_options:
            if isinstance(compaction_options, dict):
                query += f" WITH compaction = {compaction_options}"
            else:
                query += f" WITH compaction = {{'class': '{compaction_options}'}}"
        logger.debug(f'Create table: "{query}"')
        session.execute(query=query)

        query = session.prepare("INSERT INTO {} ({}) VALUES ({})".format(table_name, ", ".join(columns), ", ".join(["?" for _ in columns])))
        logger.debug(f"Insert data into {keyspace_name}.{table_name}")
        execute_concurrent_with_args(session, query, dataset)

        self.mc_validate_data(session=session, table_name=table_name, data_amount=data_amount, dataset=dataset, columns=columns, keys_columns_amount=2)
        return session

    def mc_migrate_scylla_to_cassandra(  # noqa: PLR0913
        self,
        keyspace_name,
        table_name,
        dataset,
        data_amount,
        columns=None,
        keys_columns_amount=3,
        request=None,
    ):
        if columns is None:
            columns = ['"ID"', '"Ck1"', '"cK2"', '"Columnfamily_for_mc_sstables_column1"']
        cc = CassandraCluster(cassandra_version="3.11.16", request=request, test_instance=self)
        cassandra_node1 = cc.run_migration(scylla_cluster=self.cluster, scylla_test_path=self.test_path, keyspace_names_list=[keyspace_name])
        cassandra_session = self.patient_cql_connection(cassandra_node1, keyspace=keyspace_name.replace('"', ""))

        self.mc_validate_data(session=cassandra_session, table_name=table_name, data_amount=data_amount, dataset=dataset, columns=columns, keys_columns_amount=keys_columns_amount)

    @pytest.mark.skipif(condition=not java_version_exist(8), reason="test depends on cassandra 3.x, and needs java 8 to run")
    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_mc_sstables_case_sensitive_insert(self, request, compaction_strategy_for_migration):
        """
        Test how the mc SSTAbles files format works when the column names are case sensitive
        1. Create the table with case sensitive column names
        2. Insert data
        3. Validate data
        4. Migrate the data to Cassandra cluster and validate
        """
        keyspace_name = '"Keyspace_for_mc_sstables"'
        table_name = '"Columnfamily_For_Mc_Sstables"'
        data_amount = 10
        dataset = [(i, i, i, random.randint(124571, 236283618)) for i in range(data_amount)]

        self.mc_prepare_table(nodes=4, keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, compaction_options=compaction_strategy_for_migration)

        # Create Cassandra cluster, migrate the Scylla data and validate the migrated data
        self.mc_migrate_scylla_to_cassandra(keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, request=request)

    @pytest.mark.skipif(condition=not java_version_exist(8), reason="test depends on cassandra 3.x, and needs java 8 to run")
    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_mc_sstables_case_sensitive_update_value(self, request, compaction_strategy_for_migration):
        """
        Test how the mc SSTAbles files format works when the column names are case sensitive
        1. Create the table with case sensitive column names
        2. Insert data
        3. Validate data
        4. Update data in the non-PK column
        5. Validate data
        6. Migrate the data to Cassandra cluster and validate
        """
        keyspace_name = '"Keyspace_for_mc_sstables"'
        table_name = '"Columnfamily_For_Mc_Sstables"'
        data_amount = 10
        dataset = [(i, i, i, random.randint(124571, 23628361)) for i in range(data_amount)]

        session = self.mc_prepare_table(nodes=4, keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, compaction_options=compaction_strategy_for_migration)

        logger.debug("Run update")
        for i, row_data in enumerate(dataset):
            new_value = random.randint(23628361, 456283616)
            dataset[i] = (row_data[0], row_data[1], row_data[2], new_value)
            session.execute(query=f'UPDATE {table_name} SET "Columnfamily_for_mc_sstables_column1"={new_value} WHERE "ID"={row_data[0]} AND "Ck1"={row_data[1]} AND "cK2"={row_data[2]}')

        self.mc_validate_data(session=session, table_name=table_name, data_amount=data_amount, dataset=dataset)

        # Update non-PK column to NULL with TTL
        for i, row_data in enumerate(dataset[:2]):
            new_value = "NULL"
            dataset[i] = (row_data[0], row_data[1], row_data[2], None)
            session.execute(query=f'UPDATE {table_name} USING TTL 5 SET "Columnfamily_for_mc_sstables_column1"={new_value} WHERE "ID"={row_data[0]} AND "Ck1"={row_data[1]} AND "cK2"={row_data[2]}')
        time.sleep(5)
        self.mc_validate_data(session=session, table_name=table_name, data_amount=data_amount, dataset=dataset)

        # Create Cassandra cluster, migrate the Scylla data and validate the migrated data
        self.mc_migrate_scylla_to_cassandra(keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, request=request)

    @pytest.mark.skipif(condition=not java_version_exist(8), reason="test depends on cassandra 3.x, and needs java 8 to run")
    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_mc_sstables_case_sensitive_delete_value(self, request, compaction_strategy_for_migration):
        """
        Test how the mc SSTAbles files format works when the column names are case sensitive
        1. Create the table with case sensitive column names
        2. Insert data
        3. Validate data
        4. Delete part of rows
        5. Validate data
        6. Migrate the data to Cassandra cluster and validate
        """
        keyspace_name = '"Keyspace_for_mc_sstables"'
        table_name = '"Columnfamily_For_Mc_Sstables"'
        data_amount = 10
        dataset = [(i, i, i, random.randint(124571, 236283618)) for i in range(data_amount)]

        session = self.mc_prepare_table(nodes=4, keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, compaction_options=compaction_strategy_for_migration)

        logger.debug("Run delete")
        for i in range(2, 5):
            row = dataset[i]
            session.execute(query=f'DELETE FROM {table_name} WHERE "ID"={row[0]} AND "Ck1"={row[1]} AND "cK2"={row[2]}')
            del dataset[i]
        data_amount = data_amount - 3

        self.mc_validate_data(session=session, table_name=table_name, data_amount=data_amount, dataset=dataset)

        # Create Cassandra cluster, migrate the Scylla data and validate the migrated data
        self.mc_migrate_scylla_to_cassandra(keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, request=request)

    @pytest.mark.skipif(condition=not java_version_exist(8), reason="test depends on cassandra 3.x, and needs java 8 to run")
    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_mc_sstables_case_sensitive_add_column(self, request, compaction_strategy_for_migration):
        """
        Test how the mc SSTAbles files format works when the column names are case sensitive
        1. Create the table with case sensitive column names
        2. Insert data
        3. Validate data
        4. Add new column with case sensitive name
        5. Validate data
        6. Migrate the data to Cassandra cluster and validate
        """
        keyspace_name = "keyspace_for_mc_sstables"
        table_name = "columnfamily_for_mc_sstables"
        data_amount = 10
        dataset = [(i, i + 1, i + 2) for i in range(data_amount)]
        columns = ["id", "ck1", "ck2"]
        keys_columns_amount = 2

        session = self.mc_prepare_table(
            nodes=4, keyspace_name=keyspace_name, table_name=table_name, columns=columns, keys_amount=keys_columns_amount, dataset=dataset, data_amount=data_amount, compaction_options=compaction_strategy_for_migration
        )

        # Add new columns with case sensitive name
        new_column_name = '"Columnfamily_for_mc_sstables_column1"'
        columns.append(new_column_name)
        logger.debug(f"Add {new_column_name} column")
        session.execute(query=f"ALTER TABLE {table_name} ADD {new_column_name} int")

        for i, _ in enumerate(dataset):
            dataset[i] += (random.randint(10, 50),)
        logger.debug("Insert data in the new column")
        query = session.prepare("INSERT INTO {} ({}) VALUES (?, ?, ?, ?)".format(table_name, ", ".join(columns)))
        execute_concurrent_with_args(session, query, dataset)

        self.mc_validate_data(session=session, table_name=table_name, data_amount=data_amount, dataset=dataset, columns=columns, keys_columns_amount=keys_columns_amount)

        # Create Cassandra cluster, migrate the Scylla data and validate the migrated data
        self.mc_migrate_scylla_to_cassandra(keyspace_name=keyspace_name, table_name=table_name, dataset=dataset, data_amount=data_amount, columns=columns, keys_columns_amount=keys_columns_amount, request=request)

    def mc_validate_data(  # noqa: PLR0913
        self,
        session,
        table_name,
        data_amount,
        dataset,
        columns=None,
        keys_columns_amount=3,
    ):
        if columns is None:
            columns = ['"ID"', '"Ck1"', '"cK2"', '"Columnfamily_for_mc_sstables_column1"']
        res = list(session.execute(f"select count(*) from {table_name}"))
        assert res[0].count == data_amount

        assert_all(session=session, query="select {} from {}".format(", ".join(columns), table_name), expected=[list(dc) for dc in dataset], cl=ConsistencyLevel.QUORUM, ignore_order=True)

        for row in dataset:
            where_clause = " and ".join(f"{column}={row[i]}" for i, column in enumerate(columns[:keys_columns_amount]))
            assert_one(session=session, query=f"select {columns[-1]} from {table_name} where {where_clause}", expected=[row[-1]])

    @pytest.mark.single_node
    def test_filtering_with_mv(self):
        """
        test queries with multiple restrictions + materialized view.
        """
        session = self.prepare()

        session.execute("CREATE TABLE users (username varchar, password varchar, gender varchar, session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username));")

        # create a materialized view
        session.execute("CREATE MATERIALIZED VIEW users_by_state AS SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)")

        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        assert_all(session, "SELECT count(*) FROM users WHERE username = 'user1'", [[1]])

        assert_all(session, "SELECT count(*) FROM users_by_state WHERE username = 'user1' ALLOW FILTERING", [[1]])

        assert_all(session, "SELECT count(*) FROM users_by_state WHERE state = 'TX' AND username = 'user1'", [[1]])

    @pytest.mark.single_node
    def test_partition_key_allow_filtering(self):
        """
        Filtering with unrestricted parts of partition keys
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_filter (
                k1 int,
                k2 int,
                ck1 int,
                v int,
                PRIMARY KEY ((k1, k2), ck1)
            )
        """
        )
        for k1 in [0, 1]:
            for k2 in [0, 1]:
                for ck1 in range(4):
                    session.execute(f"INSERT INTO test_filter (k1, k2, ck1, v) VALUES ({k1}, {k2}, {ck1}, 0)")

        # (0, 0, 0, 0)
        # (0, 0, 1, 0)
        # (0, 0, 2, 0)
        # (0, 0, 3, 0)
        # (0, 1, 0, 0)
        # (0, 1, 1, 0)
        # (0, 1, 2, 0)
        # (0, 1, 3, 0)
        # (1, 0, 0, 0)
        # (1, 0, 1, 0)
        # (1, 0, 2, 0)
        # (1, 0, 3, 0)
        # (1, 1, 0, 0)
        # (1, 1, 1, 0)
        # (1, 1, 2, 0)
        # (1, 1, 3, 0)

        # select test
        assert_all(session, "SELECT * FROM test_filter WHERE k1 = 0 ALLOW FILTERING", [[0, 0, 0, 0], [0, 0, 1, 0], [0, 0, 2, 0], [0, 0, 3, 0], [0, 1, 0, 0], [0, 1, 1, 0], [0, 1, 2, 0], [0, 1, 3, 0]], ignore_order=True)

        assert_all(session, "SELECT * FROM test_filter WHERE k1 <= 1 AND k2 >= 1 ALLOW FILTERING", [[0, 1, 0, 0], [0, 1, 1, 0], [0, 1, 2, 0], [0, 1, 3, 0], [1, 1, 0, 0], [1, 1, 1, 0], [1, 1, 2, 0], [1, 1, 3, 0]], ignore_order=True)

        assert_none(session, "SELECT * FROM test_filter WHERE k1 = 2 ALLOW FILTERING")
        assert_none(session, "SELECT * FROM test_filter WHERE k1 <=0 AND k2 > 1 ALLOW FILTERING")

        assert_all(session, "SELECT * FROM test_filter WHERE k2 <= 0 ALLOW FILTERING", [[0, 0, 0, 0], [0, 0, 1, 0], [0, 0, 2, 0], [0, 0, 3, 0], [1, 0, 0, 0], [1, 0, 1, 0], [1, 0, 2, 0], [1, 0, 3, 0]], ignore_order=True)

        assert_all(session, "SELECT * FROM test_filter WHERE k1 <= 0 AND k2 = 0 ALLOW FILTERING", [[0, 0, 0, 0], [0, 0, 1, 0], [0, 0, 2, 0], [0, 0, 3, 0]])

        assert_all(session, "SELECT * FROM test_filter WHERE k2 = 1 ALLOW FILTERING", [[0, 1, 0, 0], [0, 1, 1, 0], [0, 1, 2, 0], [0, 1, 3, 0], [1, 1, 0, 0], [1, 1, 1, 0], [1, 1, 2, 0], [1, 1, 3, 0]], ignore_order=True)

        assert_none(session, "SELECT * FROM test_filter WHERE k2 = 2 ALLOW FILTERING")

        # filtering on both Partition Key and Clustering key
        assert_all(session, "SELECT * FROM test_filter WHERE k1 = 0 AND ck1=0 ALLOW FILTERING", [[0, 0, 0, 0], [0, 1, 0, 0]], ignore_order=True)

        assert_all(session, "SELECT * FROM test_filter WHERE k1 = 0 AND k2=1 AND ck1=0", [[0, 1, 0, 0]])

        # count(*) test
        assert_all(session, "SELECT count(*) FROM test_filter WHERE k2 = 0 ALLOW FILTERING", [[8]])

        assert_all(session, "SELECT count(*) FROM test_filter WHERE k2 = 1 ALLOW FILTERING", [[8]])

        assert_all(session, "SELECT count(*) FROM test_filter WHERE k2 = 2 ALLOW FILTERING", [[0]])

        # test invalid query
        self._assert_invalid_filtering(session, "SELECT * FROM test_filter WHERE k1 = 0")

        self._assert_invalid_filtering(session, "SELECT * FROM test_filter WHERE k1 = 0 AND k2 > 0")

        self._assert_invalid_filtering(session, "SELECT * FROM test_filter WHERE k1 >= 0 AND k2 in (0,1,2)")

        self._assert_invalid_filtering(session, "SELECT * FROM test_filter WHERE k2 > 0")

    def test_cql_timeout_parameter(self):
        """
        A new CQL timeout parameter is introduced in: https://github.com/scylladb/scylla/issues/7777
        """
        session = self.prepare(nodes=3, rf=3)

        session.execute(
            """
            CREATE TABLE test (
                k int,
                p int,
                s int,
                v int,
                PRIMARY KEY (k, p)
            )
        """
        )

        # Fill in some data in table should succeed with large enough timeouts.
        for value in range(10):
            session.execute(f"INSERT INTO test(k, p) VALUES ({value}, {value}) USING TIMEOUT 60m")
            session.execute(f"UPDATE test USING TIMEOUT 60m SET v = {value} WHERE p = {value} AND k = {value}")
        node_to_stop = self.cluster.nodelist()[2]
        timeout_msg = "Coordinator node timed out waiting for replica nodes"

        # Performing operations with a small enough timeout is guaranteed to fail
        with pytest.raises(WriteTimeout, match=timeout_msg):
            session.execute("INSERT INTO test(k, p) VALUES (3, 4) USING TIMEOUT 0ms")
        with pytest.raises(ReadTimeout, match=timeout_msg):
            session.execute("SELECT * FROM test USING TIMEOUT 0ms")

        # Stopping one node to test interactions with query timeouts.
        node_to_stop.stop(wait_other_notice=True)
        with pytest.raises(WriteTimeout, match=timeout_msg):
            session.execute("UPDATE test USING TIMEOUT 0ms SET v = 1 WHERE p = 1 AND k = 1")
        with pytest.raises(ReadTimeout, match=timeout_msg):
            session.execute("SELECT * FROM test USING TIMEOUT 0ms")

        res = session.execute("SELECT * FROM test USING TIMEOUT 60m")
        assert len(res.current_rows) == 10, "Unexpected number of table rows."

    def _assert_invalid_filtering(self, session, query):
        msg_upper = "ALLOW FILTERING"
        msg_lower = msg_upper.lower()
        matching = f"{msg_upper}|{msg_lower}"
        assert_invalid(session=session, query=query, matching=matching)

    def _assert_valid_query(self, session, query):
        try:
            res = session.execute(query)
            assert type(res) == ResultSet
        except AssertionError as e:
            logger.debug(f"CQL query validation failed: {query} - {e}")
            raise e

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    def test_cql_timeout_non_zero_value(self):
        """
        Verifies that a positive, non-zero value for CQL TIMEOUT parameter works.
        That means getting an expected cassandra read-timeout for a long-duration 'select' query,
        where the TIMEOUT value is small enough but yet positive.
        """

        def run_stress(node):
            logger.debug("Start stress command")
            result = node.stress(["write", "duration=15s", "-mode", "cql3", "native", "-rate", "threads=50", "-pop", "seq=1..100000000", "-log", "interval=5"])
            logger.debug("Stress results:\n" + format_cs_output(result))

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # Start stress in thread
        executor = ThreadPoolExecutor(max_workers=1)
        stress_run_thread = executor.submit(run_stress, node1)

        timeout_duration_ms = 2
        cql_timeout_duration_param = f"{timeout_duration_ms}ms"
        timeout_msg = "Coordinator node timed out waiting for replica nodes"
        session = self.patient_cql_connection(node1)

        # Wait for stress to create the keyspace and table before querying
        def check_keyspace_table_created():
            try:
                rows = list(session.execute("SELECT * from keyspace1.standard1 LIMIT 1"))
                return len(rows) > 0
            except InvalidRequest:
                return False

        wait_for(check_keyspace_table_created, timeout=60, text="Waiting until keyspace1.standard1 created")

        # The below query is expected to take longer than 10 millisecond.
        # that is why using a timeout of only 10 millisecond is expected to fail.

        def full_scan():
            session.execute("SELECT * FROM keyspace1.standard1 BYPASS CACHE;")

        @retrying(num_attempts=8, sleep_time=2, allowed_exceptions=(AssertionError, InvalidRequest))
        def verify_full_scan_minimal_duration():
            full_scan_duration = timeit.timeit(full_scan, number=1)
            logger.debug(f"Full-scan duration is: {full_scan_duration}")
            assert full_scan_duration > timeout_duration_ms / 1000, f"The full-scan 'select' command took unexpectedly shorter time than {cql_timeout_duration_param}"

        verify_full_scan_minimal_duration()

        @retrying(num_attempts=8, sleep_time=2, allowed_exceptions=(Failed,))
        def verify_full_scan_timeout_failure():
            with pytest.raises(ReadTimeout, match=timeout_msg):
                execution_start = time.time()
                session.execute(f"SELECT * FROM keyspace1.standard1 BYPASS CACHE USING TIMEOUT {cql_timeout_duration_param};")
                execution_duration = time.time() - execution_start
                logger.debug(f"Full-scan duration is: {execution_duration}")

        verify_full_scan_timeout_failure()
        stress_run_thread.result()

    def range_tombstones_test(self):
        """Test deletion by 'composite prefix' (range tombstones)"""
        cluster = self.cluster

        # Uses 3 nodes just to make sure RowMutation are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c1, c2)
            );
        """
        )
        time.sleep(1)

        rows = 5
        col1 = 2
        col2 = 2
        cpr = col1 * col2
        for i in range(rows):
            for j in range(col1):
                for k in range(col2):
                    n = (i * cpr) + (j * col2) + k
                    session.execute("INSERT INTO test1 (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)" % (i, j, k, n, n))

        for i in range(rows):
            res = session.execute("SELECT v1, v2 FROM test1 where k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in range(i * cpr, (i + 1) * cpr)], list(res)

        for i in range(rows):
            session.execute("DELETE FROM test1 WHERE k = %d AND c1 = 0" % i)

        for i in range(rows):
            res = session.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in range(i * cpr + col1, (i + 1) * cpr)], list(res)

        cluster.flush()
        time.sleep(0.2)

        for i in range(rows):
            res = session.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in range(i * cpr + col1, (i + 1) * cpr)], list(res)

    def test_cql_warning_when_filtering_potentially_infinite_partitions(self):
        """
        Test queries which contain a clustering key filter but does not contain a primary key constraint,
        resulting in a potentially unlimited partition slice.
        pre 4.6.rc1: No warning
        post 4.6.rc1: Warning
        future: Exception
        """
        expected_message = "This query should use ALLOW FILTERING and will be rejected in future versions."
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test_infinite_partition_filtering (
                pk int,
                ck int,
                v1 int,
                v2 int,
                PRIMARY KEY(pk, ck)
            );
        """
        )

        for pk_mult in range(10):
            session.execute(f"INSERT INTO test_infinite_partition_filtering(pk, ck, v1, v2) VALUES ({pk_mult * 1000}, 2, 100, 50)")

        result = session.execute("SELECT pk, ck FROM test_infinite_partition_filtering WHERE ck = 2")
        assert result.response_future.warnings and expected_message in result.response_future.warnings, "Starting with 4.6 a warning should be generated for query which can potentially contain infinite partitions"


@pytest.mark.dtest_full
class TestsCQLAdditional(Tester):
    def prepare(self, options=None):
        """
        Sets up cluster to test against.
        """
        if options is None:
            options = {}
        cluster = self.cluster

        if options:
            cluster.set_configuration_options(values=options)

        cluster.populate(1).start()
        return cluster

    @pytest.mark.single_node
    def test_simple_null_value(self):
        cluster = self.prepare()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
             CREATE TABLE foobar ( key text PRIMARY KEY , val1 text , val2 float );
        """
        )

        update = session.prepare("UPDATE foobar SET val1 = ?, val2 = ? WHERE key = ?;")
        session.execute(update.bind(("ccc", 1.0, "java1")))
        session.execute(update.bind((None, 1.0, "java2")))
        session.execute(update.bind((None, None, "java3")))
        session.execute(update.bind(("ddd", None, "java4")))

        res = list(
            session.execute(
                """
                SELECT * FROM foobar
        """
            )
        )
        assert len(res) == 3, res

    @pytest.mark.single_node
    def test_create_secondary_indexes(self):
        cluster = self.prepare()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "racing", 1)

        c = """CREATE TABLE racing.rank_by_year_and_name (
              race_year int,
              race_name text,
              racer_name text,
              rank int,
              PRIMARY KEY ((race_year, race_name), rank)
            )"""
        session.execute(c)

        c = """CREATE INDEX ryear ON racing.rank_by_year_and_name (race_year)"""
        try:
            session.execute(c)
        except Exception as err:  # noqa: BLE001
            assert str(err) == "Indexes are not supported yet"
            assert getattr(err, "code") == 0000

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.lwt
    def test_lightweight_transaction(self):
        cluster = self.prepare()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        c = """CREATE TABLE ks.users (
              login text,
              email text,
              name text,
              PRIMARY KEY (login)
            )"""
        session.execute(c)

        row = ["bcanet", "benoit@scylladb.com", "Benoit Canet"]

        c = f"""INSERT INTO ks.users (login, email, name)
            values ('{row[0]}', '{row[1]}', '{row[2]}')
            IF NOT EXISTS"""
        session.execute(c)

        logger.debug("Make sure the row is not updated if it exists...")
        c = """INSERT INTO ks.users (login, email, name)
            values ('bcanet', 'disabled@scylladb.com', 'disabled')
            IF NOT EXISTS"""
        session.execute(c)

        logger.debug("Verify content...")
        res = rows_to_list(session.execute("SELECT * FROM ks.users"))
        assert len(res) == 1, res
        assert res[0] == row, res[0]

    @pytest.mark.single_node
    def test_grant(self):
        cluster = self.prepare(options={"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"})
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node, user="cassandra", password="cassandra")
        session.execute("CREATE ROLE benoit")
        create_ks(session, "ks", 1)
        c = """GRANT SELECT ON ALL KEYSPACES TO benoit"""
        try:
            session.execute(c)
        except Exception as err:  # noqa: BLE001
            assert str(err) == "Not implemented: GRANT"
            assert getattr(err, "code") == 0000

    @pytest.mark.single_node
    def test_revoke(self):
        cluster = self.prepare(options={"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"})
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node, user="cassandra", password="cassandra")
        create_ks(session, "ks", 1)
        create_cf(session, "user")
        session.execute("CREATE ROLE benoit")

        c = """GRANT SELECT ON ALL KEYSPACES TO benoit"""
        session.execute(c)

        c = """REVOKE SELECT ON ks.user FROM benoit"""
        try:
            session.execute(c)
        except Exception as err:  # noqa: BLE001
            assert str(err) == "Not implemented: REVOKE"
            assert getattr(err, "code") == 0000

    @pytest.mark.single_node
    def test_list(self):
        cluster = self.prepare(options={"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"})
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node, user="cassandra", password="cassandra")
        create_ks(session, "ks", 1)
        create_cf(session, "boo")

        c = """LIST ALL PERMISSIONS ON ks.boo"""
        try:
            session.execute(c)
        except Exception as err:  # noqa: BLE001
            assert str(err) == "Not implemented: LIST"
            assert getattr(err, "code") == 0000

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_limit_date_value_out_of_range(self):
        # positive case for scylladb/scylladb#1694
        cluster = self.prepare()
        node = cluster.nodelist()[0]
        query_template = "select * from raw_data %s;"
        regex = r"([0-9]+) (rows)"

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE ks.raw_data (
                              test_id int,
                              partition_key text,
                              time timestamp,
                              value double,
                              PRIMARY KEY ((test_id, partition_key), time)
                              ) WITH CLUSTERING ORDER BY (time ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                                AND comment = ''
                                AND compaction = {'class': 'SizeTieredCompactionStrategy'}
                                AND compression = {'sstable_compression': 'LZ4Compressor'}
                                AND dclocal_read_repair_chance = 0.1
                                AND default_time_to_live = 0
                                AND gc_grace_seconds = 864000
                                AND max_index_interval = 2048
                                AND memtable_flush_period_in_ms = 0
                                AND min_index_interval = 128
                                AND read_repair_chance = 0.0
                                AND speculative_retry = '99.0PERCENTILE';"""
        )

        res = session.execute(query_template % "limit 1")
        assert len(list(res)) == 0
        out, _err = node.run_cqlsh("use ks; " + query_template % "limit 1", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 0

        for i in range(100):
            session.execute(f"insert into ks.raw_data (test_id, partition_key, time, value) values ({i}, '{i}', '{2000 - i}-02-03 04:05+0000', {i * 1.0});")

        res = session.execute(query_template % "limit 1")
        assert len(list(res)) == 1
        out, _err = node.run_cqlsh("use ks; " + query_template % "limit 1", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 1

        res = session.execute(query_template % "")
        assert len(list(res)) == 100
        out, _err = node.run_cqlsh("use ks; " + query_template % "", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 100

    @pytest.mark.require("2251")
    @pytest.mark.single_node
    def test_limit_date_value_out_of_range_lower_limit(self):
        cluster = self.prepare()
        node = cluster.nodelist()[0]
        query_template = "select * from raw_data %s;"
        regex = r"([0-9]+) (rows)"

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE ks.raw_data (
                              test_id int,
                              partition_key text,
                              time timestamp,
                              value double,
                              PRIMARY KEY ((test_id, partition_key), time)
                              ) WITH CLUSTERING ORDER BY (time ASC);"""
        )

        for i in range(2000):
            session.execute(f"insert into ks.raw_data (test_id, partition_key, time, value) values ({i}, '{i}', '{2000 - i}-02-03 04:05+0000', {i * 1.0});")

        res = session.execute(query_template % "limit 1")
        assert len(list(res)) == 1
        out, _err = node.run_cqlsh("use ks; " + query_template % "limit 1", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 1

        res = session.execute(query_template % "")
        assert len(list(res)) == 2000
        out, _err = node.run_cqlsh("use ks; " + query_template % "limit 10", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 10

    @pytest.mark.single_node
    def test_limit_date_value_out_of_range_upper_limit(self):
        cluster = self.prepare()
        node = cluster.nodelist()[0]
        query_template = "select * from raw_data %s;"
        regex = r"([0-9]+) (rows)"

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE ks.raw_data (
                              test_id int,
                              partition_key text,
                              time timestamp,
                              value double,
                              PRIMARY KEY ((test_id, partition_key), time)
                              ) WITH CLUSTERING ORDER BY (time DESC);"""
        )

        for i in range(8000):
            session.execute(f"insert into ks.raw_data (test_id, partition_key, time, value) values ({i}, '{i}', '{2000 + i}-02-03 04:05+0000', {i * 1.0});")

        res = session.execute(query_template % "limit 1")
        assert len(list(res)) == 1
        out, _err = node.run_cqlsh("use ks; " + query_template % "limit 1", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 1

        res = session.execute(query_template % "")
        assert len(list(res)) == 8000
        out, _err = node.run_cqlsh("use ks; " + query_template % "limit 10", show_output=True, return_output=True)
        num_rows = int(re.search(regex, out).group(1))
        assert num_rows == 10

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_select_all_data_and_filter_explicitly(self):
        # https://github.com/scylladb/scylla/issues/2272
        cluster = self.prepare()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)
        session.execute(
            """
                  CREATE TABLE ks.hour_data (
                  bucket text,
                  hour_ts int,
                  ug int,
                  user bigint,
                  PRIMARY KEY (bucket, hour_ts, ug, user)
              ) WITH CLUSTERING ORDER BY (hour_ts ASC, ug ASC, user ASC)
                  AND bloom_filter_fp_chance = 0.01
                  AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                  AND comment = ''
                  AND compaction = {'class': 'LeveledCompactionStrategy'}
                  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                  AND dclocal_read_repair_chance = 0.1
                  AND default_time_to_live = 0
                  AND gc_grace_seconds = 864000
                  AND max_index_interval = 2048
                  AND memtable_flush_period_in_ms = 0
                  AND min_index_interval = 128
                  AND read_repair_chance = 0.0
                  AND speculative_retry = '99.0PERCENTILE';"""
        )
        # insert 10K random data
        for i in range(10000):
            session.execute(f"insert into ks.hour_data (bucket, hour_ts, ug, user) values ('2017-29-03', {random.randint(0, 23)}, {random.randint(1, 17)}, {random.randint(0, 9999999999)});")
        # A little more data from another bucket
        for i in range(100):
            session.execute(f"insert into ks.hour_data (bucket, hour_ts, ug, user) values ('2017-29-04', {random.randint(0, 23)}, {random.randint(1, 17)}, {random.randint(0, 9999999999)});")
        # filter only by bucket
        sql = """
        SELECT hour_ts, ug, user
        FROM hour_data
        WHERE bucket = '2017-29-03'
        """
        counted = defaultdict(int)
        res = session.execute(sql)
        for num, row in enumerate(res):
            counted[(row.hour_ts, row.ug)] += 1

        r_implicitly = {}
        i = 0
        for (hour, ug), count in sorted(counted.items()):
            r_implicitly[i] = (hour, ug, count)
            i += 1
            logger.debug(f"{hour} {ug} {count} ")

        # filter explicitly by hour_ts and ug
        sql = """
        SELECT hour_ts, ug, user
        FROM hour_data
        WHERE bucket = '2017-29-03' AND hour_ts = %s AND ug = %s
        """

        r_explicitly = {}
        i = 0

        for hour_ts in range(24):
            counted = defaultdict(int)
            for ug in range(1, 18):
                res = session.execute(sql, (hour_ts, ug))
                for num, row in enumerate(res):
                    counted[(row.hour_ts, row.ug)] += 1
            for (hour, ug), count in sorted(counted.items()):
                r_explicitly[i] = (hour, ug, count)
                i += 1
            logger.debug(f"{hour} {ug} {count} ")
        # expect the same data
        assert r_explicitly == r_implicitly

    @unmark.next_gating  # https://github.com/scylladb/scylladb/issues/14806
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_create_100tables(self):
        """
        The scenario referenced https://github.com/scylladb/scylla/issues/2923

        Create 100 tables, and try to restart the scylla-server of two nodes
        """
        jvm_args = [
            "--twcs-max-window-count",
            "100",
            "--request-timeout-in-ms",
            "60000",
            "--write-request-timeout-in-ms",
            "60000",
        ]
        self.cluster.populate(3).start(jvm_args=jvm_args)
        nodes = self.cluster.nodelist()
        schema_file = "test_data/c-s-profiles/create_100tables.cql"
        assert os.path.exists(schema_file), "schema file doesn't exist"

        logger.debug("Create 100+ tables by simple_test_100tables.cql")

        out, _err = nodes[0].run_cqlsh(
            cmds="SOURCE '%s'" % schema_file,
            cqlsh_options=["--request-timeout=300", "--connect-timeout=60"],
            show_output=True,
            return_output=True,
        )

        logger.debug("Check created tables in KEYSPACE `veraminetest`")
        out, _err = nodes[0].run_cqlsh(cmds="USE veraminetest; DESCRIBE TABLES", show_output=True, return_output=True)
        assert len(out.split()) == 112, out

        logger.debug("Drain node1")
        resp = nodes[0].drain()
        logger.debug("Restart node1")
        nodes[0].stop(wait_other_notice=False, gently=False)
        nodes[0].start(wait_other_notice=True)

        logger.debug("Drain node2")
        resp = nodes[1].drain()
        logger.debug("Restart node2")
        nodes[1].stop(wait_other_notice=False, gently=False)
        nodes[1].start(wait_other_notice=True)

        session = self.patient_cql_connection(nodes[0])

        logger.debug("Check created tables in KEYSPACE `veraminetest` after restart")
        out, _err = nodes[0].run_cqlsh(cmds="USE veraminetest; DESCRIBE TABLES", show_output=True, return_output=True)
        assert len(out.split()) == 112, "created 100+ tables"

    def _create_100_keyspaces(self, nodes, rf=1):
        """
        Create 100 keyspaces in a multi-dc cluster
        """
        cluster = self.cluster
        cluster.populate(nodes).start()
        node = cluster.nodelist()[0]
        node_ip = node.address()
        session = self.patient_cql_connection(node)

        metrics = get_node_metrics(node_ip, metrics=["memory"])
        mem_before = int(metrics["memory"])

        logger.debug(f"Create 100 keyspaces on {nodes} node{'' if nodes == 1 else 's'}")
        for i in range(100):
            session.execute(f"CREATE KEYSPACE test_keyspace_{i} WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf} }}")

        metrics = get_node_metrics(node_ip, metrics=["memory"])
        mem_after = int(metrics["memory"])

        logger.debug(f"Consumed {mem_after - mem_before} bytes")

    @pytest.mark.single_node
    def test_create_100_keyspaces_single_node(self):
        """
        Create 100 keyspaces on a single node
        """
        self._create_100_keyspaces(nodes=1, rf=1)

    def test_create_100_keyspaces(self):
        """
        Create 100 keyspaces in a multi-dc cluster
        """
        self._create_100_keyspaces(nodes=[2, 2, 2], rf=1)

    # Regression test for scylladb/scylladb#8447
    @pytest.mark.single_node
    def test_twcs_ck_filtering_with_cache(self):
        logger.debug("creating single-node cluster")
        cluster = self.prepare({"ring_delay_ms": 1000})
        node = cluster.nodelist()[0]

        logger.debug("waiting for connection")
        session = self.patient_cql_connection(node)

        logger.debug("creating table")
        session.execute("create keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute("create table ks.t (pk int, ck int, primary key (pk, ck)) with compaction = {'class': 'TimeWindowCompactionStrategy'} and bloom_filter_fp_chance = 1;")

        logger.debug("creating sstables")
        session.execute("insert into ks.t (pk, ck) values (1, 0)")
        node.nodetool("flush")
        session.execute("insert into ks.t (pk, ck) values (0, 1)")
        node.nodetool("flush")

        logger.debug("stopping node")
        node.stop()
        logger.debug("restarting node")
        node.start(wait_for_binary_proto=True)

        logger.debug("waiting for connection")
        session = self.patient_cql_connection(node)

        logger.debug("performing selects")
        # Both sstables pass through the pk filter (thanks to bloom_filter_fp_chance = 1),
        # but only the second one - which does not contain the queried partition - passes through the ck filter.
        # If the created reader does not return `partition_start` (as in #8447)
        # this will cause the cache to rememeber that there is no partition 1.
        assert_none(session, "select * from ks.t where pk = 1 and ck = 1")
        # If the cache remembered that there is no partition 1 in the above query,
        # this would return no row.
        assert_one(session, "select * from ks.t where pk = 1 and ck = 0", [1, 0])

    def test_indexed_statement_concurrency_limit(self):
        """
        Verify that in case of indexed paged query the first page returned would contain
        around 1MiB of data, as dictated by internal scylla concurrency limit when fetching data from
        indexed SELECT statements
        Pre 4.6.rc1: Test fails, the current_rows would be equal to page_size
        4.6.rc1: Test passes, current rows will be less than page_size, at 511 rows as of 4.6.rc1
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        logger.debug("Preparing keyspace and table")
        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
        table_stmt = SimpleStatement("CREATE TABLE IF NOT EXISTS ks.tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))", consistency_level=ConsistencyLevel.ALL)
        session.execute(table_stmt)
        session.execute("CREATE INDEX ON ks.tab (v)")

        blob = "d" * 4096
        logger.debug("Inserting data...")
        total_rows = 3 * 1024
        """
          This page size should trigger internal scylla limit and
          the query using this page size should return less rows than requested
        """
        page_size = 1024
        for i in range(total_rows):
            session.execute(f"INSERT INTO ks.tab (pk, ck, v, v2, v3) VALUES ({i % 3}, 'hello{i}', 1, {i}, '{blob}')")
            if i % 1024 == 0:
                logger.debug(f"Inserted {i}/{total_rows} rows.")

        logger.debug("Rows inserted.")
        indexed_select_paged = SimpleStatement("SELECT * FROM ks.tab WHERE v = 1", fetch_size=page_size, consistency_level=ConsistencyLevel.LOCAL_ONE)
        res = session.execute(indexed_select_paged)
        rows_received = len(res.current_rows)
        logger.debug(f"Indexed select fetched {rows_received} rows out of {page_size}")
        assert rows_received < page_size, f"Expected to get less rows than requested, got {rows_received} with page size of {page_size}."

    @pytest.mark.single_node
    @pytest.mark.parametrize("restricted_future_state", [False, True])
    def test_restricted_future_timestamp(self, restricted_future_state):
        cluster = self.prepare({"restrict_future_timestamp": restricted_future_state})
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        session.execute(
            """
             CREATE TABLE foobar ( key bigint PRIMARY KEY , val1 text , val2 float );
        """
        )
        logger.debug("Check that timestamp less than 3 days in future always written")
        for day in [0, 1, 2]:
            timestamp = int((time.time() + day * 24 * 3600) * 1000000)
            session.execute(f"INSERT INTO foobar (key, val1, val2) VALUES (1, 'java1', 1.0) USING TIMESTAMP {timestamp}")
            result = list(session.execute("""SELECT * FROM foobar"""))
            assert len(result) == 1, "Invalid number of inserted rows"
            result = session.execute(f"SELECT writetime(val1) FROM foobar WHERE key = 1")
            assert [(timestamp,)] == result
            session.execute(f"UPDATE foobar USING TIMESTAMP {timestamp + 100} SET val1 = 'test' WHERE key = 1")
            result = session.execute(f"SELECT writetime(val1) FROM foobar WHERE key = 1")
            assert [(timestamp + 100,)] == result
            session.execute(f"DELETE val1 FROM foobar USING TIMESTAMP {timestamp + 1000} WHERE key = 1")
            result = list(session.execute(f"SELECT val1 FROM foobar WHERE key = 1"))

            assert result[0].val1 is None

        logger.debug("Check that timestamp far in future than 3 days, written only if restrict_future_timestamp is disabled")
        for day in [5, 10, 100, 1000]:
            timestamp = int((time.time() + day * 24 * 3600) * 1000000)

            if restricted_future_state:
                with pytest.raises(InvalidRequest, match="into the future"):
                    session.execute(f"INSERT INTO foobar (key, val1, val2) VALUES (1, 'java1', 1.0) USING TIMESTAMP {timestamp}")
                session.execute(f"INSERT INTO foobar (key, val1, val2) VALUES (1, 'java1', 1.0)")
                with pytest.raises(InvalidRequest, match="into the future"):
                    session.execute(f"UPDATE foobar USING TIMESTAMP {timestamp} SET val1 = 'test' WHERE key = 1")
                with pytest.raises(InvalidRequest, match="into the future"):
                    session.execute(f"DELETE val1 FROM foobar USING TIMESTAMP {timestamp} WHERE key = 1")

            else:
                session.execute(f"INSERT INTO foobar (key, val1, val2) VALUES (1, 'java1', 1.0) USING TIMESTAMP {timestamp}")
                result = list(session.execute("""SELECT * FROM foobar"""))
                assert len(result) == 1, "Invalid number of inserted rows"
                result = session.execute(f"SELECT writetime(val1) FROM foobar WHERE key = 1")
                assert [(timestamp,)] == result
                session.execute(f"UPDATE foobar USING TIMESTAMP {timestamp + 100} SET val1 = 'test' WHERE key = 1")
                result = session.execute(f"SELECT writetime(val1) FROM foobar WHERE key = 1")
                assert [(timestamp + 100,)] == result
                session.execute(f"DELETE val1 FROM foobar USING TIMESTAMP {timestamp + 1000} WHERE key = 1")
                result = list(session.execute(f"SELECT val1 FROM foobar WHERE key = 1"))

                assert result[0].val1 is None


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.single_node
class TestsMultiColumnRestrictionSimple(Tester):
    INSERT_COLUMNS = "key,clmn_int,clmn_text,clmn_timestamp,clmn_bool,clmn_ascii,clmn_uuid,clmn_blob"
    INSERT_2COLUMNS = "key,clmn_int"
    SELECT_COLUMNS = INSERT_COLUMNS.replace("clmn_timestamp", "cast(clmn_timestamp as text)").replace("clmn_uuid", "cast(clmn_uuid as text)")

    TEST_DATA = [
        [0, 0, "text1", 12345674987, True, "045asciitext", "de5cba0d-41a2-4f39-8834-35130d8b5d86", "c" * 10],
        [1, 0, "text2", 63873478378, False, "abcdefj", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "b" * 10],
        [2, 2, "text3", 398793781719, True, "354dsfsd", "de5cba0d-41a2-4f39-8834-35130d8b5d86", "b" * 10],
        [3, 3, "text4", 398793781719, False, "897dfjka9", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "a" * 10],
        [4, 4],
    ]

    EXPECTED_DATA = [
        [0, 0, "text1", "1970-05-23T21:21:14.987Z", True, "045asciitext", "de5cba0d-41a2-4f39-8834-35130d8b5d86", b"c" * 10],
        [1, 0, "text2", "1972-01-10T06:37:58.378Z", False, "abcdefj", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", b"b" * 10],
        [2, 2, "text3", "1982-08-21T16:03:01.719Z", True, "354dsfsd", "de5cba0d-41a2-4f39-8834-35130d8b5d86", b"b" * 10],
        [3, 3, "text4", "1982-08-21T16:03:01.719Z", False, "897dfjka9", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", b"a" * 10],
        [4, 4, None, None, None, None, None, None],
    ]
    TABLE_NAME = "cf"
    MV_NAME = "cf_mv"

    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None):
        cluster = self.cluster

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        if not cluster.nodelist():
            cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session

    def create_8_columns_table(self, session, table_name=TABLE_NAME, add_ck=False):
        query = "CREATE COLUMNFAMILY {table_name} (key int, clmn_int int, clmn_text varchar, clmn_timestamp timestamp, clmn_bool boolean, clmn_ascii ascii, clmn_uuid uuid, clmn_blob blob, PRIMARY KEY(key{ck}))".format(
            table_name=table_name, ck=", clmn_int" if add_ck else ""
        )

        logger.debug(query)
        session.execute(query)

    def create_materialized_view(
        self,
        session,
        view_column,
        view_name=MV_NAME,
        table_name=TABLE_NAME,
        table_with_ck=True,
    ):
        query = "CREATE MATERIALIZED VIEW {view_name} as SELECT * FROM {table_name} WHERE key IS NOT NULL {ck}and {view_column} IS NOT NULL PRIMARY KEY (key{ckey}, {view_column})".format(
            view_name=view_name, table_name=table_name, ck="AND clmn_int IS NOT NULL " if table_with_ck else "", ckey=", clmn_int" if table_with_ck else "", view_column=view_column
        )
        logger.debug(query)
        session.execute(query)
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view=view_name)

    def insert_data_in_8_columns_table(self, session, insert_data=TEST_DATA, table_name=TABLE_NAME):
        logger.debug("Insert data")
        for data in insert_data:
            if len(data) == 2:
                data_str = f"{data[0]},{data[1]}"
                insert_columns = self.INSERT_2COLUMNS
            elif len(data) == 8:
                data_str = f"{data[0]},{data[1]},'{data[2]}','{data[3]}',{data[4]},'{data[5]}',{data[6]},textAsBlob('{data[7]}')"
                insert_columns = self.INSERT_COLUMNS
            else:
                assert False, f"Expected data set with 2 or 8, but received {len(data)}"

            stmt = f"INSERT INTO {table_name}({insert_columns}) VALUES({data_str})"
            session.execute(stmt)

    def test_filter_by_one_non_indexed_columns(self):
        session = self.prepare()
        self.create_8_columns_table(session=session)

        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by integer non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_int = 0 ALLOW FILTERING", expected=self.EXPECTED_DATA[:2], ignore_order=True)

        logger.debug("Filter by text non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_text = 'text3' ALLOW FILTERING", expected=[self.EXPECTED_DATA[2]], ignore_order=True)

        logger.debug("Filter by timestamp non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_timestamp = 398793781719 ALLOW FILTERING", expected=self.EXPECTED_DATA[2:4], ignore_order=True)

        logger.debug("Filter by boolean non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_bool = True ALLOW FILTERING", expected=[self.EXPECTED_DATA[0], self.EXPECTED_DATA[2]], ignore_order=True, cl=ConsistencyLevel.QUORUM)

        logger.debug("Filter by ascii non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_ascii = 'abcdefj' ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True, cl=ConsistencyLevel.QUORUM)

        logger.debug("Filter by uuid non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_uuid = de5cba0d-41a2-4f39-8834-35130d8b5d86 ALLOW FILTERING", expected=[self.EXPECTED_DATA[0], self.EXPECTED_DATA[2]], ignore_order=True, cl=ConsistencyLevel.QUORUM)

        logger.debug("Filter by blob non-indexed column")
        assert_all(session=session, query=select_stmt + " where clmn_blob = textAsBlob('{}') ALLOW FILTERING".format("b" * 10), expected=self.EXPECTED_DATA[1:3], ignore_order=True, cl=ConsistencyLevel.QUORUM)

    def test_filter_by_three_non_indexed_columns(self):
        session = self.prepare()
        self.create_8_columns_table(session=session)

        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by integer & uuid & timestamp non-indexed columns")
        assert_all(session=session, query=select_stmt + "where clmn_int = 2 and clmn_uuid = de5cba0d-41a2-4f39-8834-35130d8b5d86 and clmn_timestamp = 398793781719 ALLOW FILTERING", expected=[self.EXPECTED_DATA[2]], ignore_order=True)

        logger.debug("Filter by ascii & text & blob non-indexed columns")
        assert_all(session=session, query=select_stmt + "where clmn_ascii = '897dfjka9' and clmn_text = 'text4' and clmn_blob = textAsBlob('{}') ALLOW FILTERING".format("a" * 10), expected=[self.EXPECTED_DATA[3]], ignore_order=True)

    def test_filter_by_pk_ck_and_non_indexed_columns(self):
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)

        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by PK and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 0 and clmn_timestamp = 12345674987 ALLOW FILTERING", expected=[self.EXPECTED_DATA[0]], ignore_order=True)

        logger.debug("Filter by PK, CK and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 0 and clmn_int = 0 and clmn_timestamp = 12345674987 ALLOW FILTERING", expected=[self.EXPECTED_DATA[0]], ignore_order=True)

        logger.debug("Filter by PK and two non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 0 and clmn_timestamp = 12345674987 and clmn_bool = True ALLOW FILTERING", expected=[self.EXPECTED_DATA[0]], ignore_order=True)

        logger.debug("Filter by PK, CK and two non-indexed column")
        assert_all(
            session=session, query=select_stmt + "where key = 0 and clmn_int = 0 and clmn_timestamp = 12345674987 and clmn_uuid=de5cba0d-41a2-4f39-8834-35130d8b5d86 ALLOW FILTERING", expected=[self.EXPECTED_DATA[0]], ignore_order=True
        )

    def test_filter_by_pk_ck_globalSI_and_non_indexed_columns(self):  # noqa: N802
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)

        create_index(session=session, table_name=self.TABLE_NAME, index_column="clmn_text", index_name="global_idx")

        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by PK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 1 and clmn_text = 'text2' and clmn_timestamp = 63873478378 ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True)

        logger.debug("Filter by PK, CK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 1 and clmn_int = 0 and clmn_text = 'text2' and clmn_timestamp = 63873478378 ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True)

        logger.debug("Filter by PK, SI and two non-indexed column")
        assert_all(
            session=session,
            query=select_stmt + "where key = 1 and clmn_text = 'text2' and clmn_uuid = fa80080c-a4c5-46d6-afe4-5e184fec35ae and clmn_timestamp = 63873478378 ALLOW FILTERING",
            expected=[self.EXPECTED_DATA[1]],
            ignore_order=True,
        )

        logger.debug("Filter by PK, CK, SI and two non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 1 and clmn_int = 0 and clmn_text = 'text2' and clmn_bool = False and clmn_timestamp = 63873478378 ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True)

    def test_filter_by_pk_ck_localSI_and_non_indexed_columns(self):  # noqa: N802
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)

        create_local_index(session=session, table_name=self.TABLE_NAME, pk_name="key", index_column="clmn_text", index_name="global_idx")

        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by PK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 1 and clmn_text = 'text2' and clmn_timestamp = 63873478378 ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True)

        logger.debug("Filter by PK, CK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 1 and clmn_int = 0 and clmn_text = 'text2' and clmn_timestamp = 63873478378 ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True)

        logger.debug("Filter by PK, SI and two non-indexed column")
        assert_all(
            session=session,
            query=select_stmt + "where key = 1 and clmn_text = 'text2' and clmn_uuid = fa80080c-a4c5-46d6-afe4-5e184fec35ae and clmn_timestamp = 63873478378 ALLOW FILTERING",
            expected=[self.EXPECTED_DATA[1]],
            ignore_order=True,
        )

        logger.debug("Filter by PK, CK, SI and two non-indexed column")
        assert_all(session=session, query=select_stmt + "where key = 1 and clmn_int = 0 and clmn_text = 'text2' and clmn_bool = False and clmn_timestamp = 63873478378 ALLOW FILTERING", expected=[self.EXPECTED_DATA[1]], ignore_order=True)

    def test_filter_by_two_non_indexed_columns_with_operator(self):
        session = self.prepare()
        self.create_8_columns_table(session=session)

        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug('Filter by integer & uuid non-indexed columns with "=<" operator')
        assert_all(session=session, query=select_stmt + "where clmn_int < 2 and clmn_uuid <= fa80080c-a4c5-46d6-afe4-5e184fec35ae ALLOW FILTERING", expected=self.EXPECTED_DATA[:2], ignore_order=True)

    def test_filter_by_pk_ck_globalSI_and_non_indexed_columns_with_operator(self):  # noqa: N802
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)

        create_index(session=session, table_name=self.TABLE_NAME, index_column="clmn_text", index_name="global_idx")

        self.insert_data_in_8_columns_table(session=session, insert_data=self.TEST_DATA[:4])

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by PK, CK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key > 1 and clmn_int < 5 and clmn_text >= 'text2' and clmn_timestamp > 63873478378 ALLOW FILTERING", expected=self.EXPECTED_DATA[2:4], ignore_order=True)

    def test_filter_by_pk_ck_localSI_and_non_indexed_columns_with_operator(self):  # noqa: N802
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)

        create_local_index(session=session, table_name=self.TABLE_NAME, pk_name="key", index_column="clmn_text", index_name="local_idx")

        self.insert_data_in_8_columns_table(session=session, insert_data=self.TEST_DATA[:4])

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by PK, CK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key > 1 and clmn_int < 5 and clmn_text >= 'text2' and clmn_timestamp > 63873478378 ALLOW FILTERING", expected=self.EXPECTED_DATA[2:4], ignore_order=True)

    def test_filter_by_non_indexed_columns_from_mv(self):
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)
        self.create_materialized_view(session=session, view_column="clmn_text")
        self.insert_data_in_8_columns_table(session=session)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.MV_NAME} "

        # Issue #4776
        # logger.debug('Filter by one empty non-indexed column')
        # assert_all(session=session, query=select_stmt + 'where clmn_text = \'\' ALLOW FILTERING',
        #            expected=[self.EXPECTED_DATA[4]], ignore_order=True)

        logger.debug("Filter by one non-indexed column")
        assert_all(session=session, query=select_stmt + "where clmn_text = 'text1' ALLOW FILTERING", expected=[self.EXPECTED_DATA[0]], ignore_order=True)

        logger.debug('Filter by PK, CK, SI and two non-indexed column with "less-more" operator')
        assert_all(
            session=session,
            query=select_stmt + "where key > 1 and clmn_int < 5 and clmn_uuid = fa80080c-a4c5-46d6-afe4-5e184fec35ae and clmn_blob = textAsBlob('{}') ALLOW FILTERING".format("a" * 10),
            expected=[self.EXPECTED_DATA[3]],
            ignore_order=True,
        )

    def test_empty_data_set_result(self):
        session = self.prepare()
        self.create_8_columns_table(session=session, add_ck=True)
        self.create_materialized_view(session=session, view_column="clmn_text")
        self.insert_data_in_8_columns_table(session=session, insert_data=self.TEST_DATA[:4])

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.TABLE_NAME} "

        logger.debug("Filter by PK, CK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key > 1 and clmn_int < 5 and clmn_text <= 'text2' and clmn_timestamp < 63873478378 ALLOW FILTERING", expected=[], ignore_order=True)

        logger.debug("Filter by PK, CK, SI and two non-indexed column")
        assert_all(session=session, query=select_stmt + "where key > 1 and clmn_int < 5 and clmn_uuid = fa80080c-a4c5-46d6-afe4-5e184fec35ae and clmn_blob = textAsBlob('bbbbbbbbbb') ALLOW FILTERING", expected=[], ignore_order=True)

        select_stmt = f"select {self.SELECT_COLUMNS} from {self.MV_NAME} "
        logger.debug("Filter by PK, CK, SI and one non-indexed column")
        assert_all(session=session, query=select_stmt + "where key > 1 and clmn_int < 5 and clmn_text <= 'text2' and clmn_timestamp < 63873478378 ALLOW FILTERING", expected=[], ignore_order=True)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsMultiColumnRestrictionCollection(Tester):
    TABLE_NAME = "cf"
    TEST_DATA = [
        [
            0,
            "[0, 1, 2]",
            "[textAsBlob('t1'), textAsBlob('t2')]",
            "{de5cba0d-41a2-4f39-8834-35130d8b5d86, fa80080c-a4c5-46d6-afe4-5e184fec35ae}",
            "{'t3', 't4', 't5'}",
            "{'a': True, 'b': True, 'c': False}",
            "{'a': f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4, 'c': f34f6cec-b383-11e9-a2a3-2a2ae2dbcce4}",
            "[5, 6]",
            "['f1', 'f2']",
            "{7, 9}",
            "{'f3', 'f4', 'f5'}",
            "{'fa': 'b', 'fc': 'd'}",
            "{'fa': 1, 'fb': 2, 'fc':3}",
        ],
        [
            1,
            "[3, 4, 5]",
            "[textAsBlob('t3'), textAsBlob('t4')]",
            "{8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4, 8e4fea9c-b383-11e9-a2a3-2a2ae2dbcce4}",
            "{'t5', 't6', 't7'}",
            "{'a1': False, 'c1': False}",
            "{'a1': 26a8e352-b384-11e9-a2a3-2a2ae2dbcce4, 'b1': 26a8e5be-b384-11e9-a2a3-2a2ae2dbcce4, 'c1': 26a8e712-b384-11e9-a2a3-2a2ae2dbcce4}",
            "[7, 8]",
            "['f3', 'f4']",
            "{9, 10}",
            "{'f6', 'f7', 'f8'}",
            "{'f1': 'c', 'f2': 'e'}",
            "{'f1': 1, 'f2': 2, 'f3': 3}",
        ],
    ]
    INSERT_COLUMNS = "id, list_int, list_blob, set_uuid, set_text, map_bool, map_uuid, f_list_int, f_list_text, f_set_int, f_set_text, f_map_text, f_map_int"

    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None):
        cluster = self.cluster

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        if not cluster.nodelist():
            cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session

    def create_all_collections_table(self, session):
        stmt = (
            f"CREATE TABLE {self.TABLE_NAME} (id int PRIMARY KEY, list_int list<int>, list_blob list<blob>, "
            "set_uuid set<uuid>, set_text set<text>, map_bool map<text, boolean>, "
            "map_uuid map<text, uuid>, f_list_int frozen<list<int>>, f_list_text frozen<list<text>>, "
            "f_set_int frozen<set<int>>, f_set_text frozen<set<text>>,f_map_text frozen<map<text, text>>, "
            "f_map_int frozen<map<text, int>>)"
        )
        logger.debug(stmt)
        session.execute(stmt)

    def insert_data_in_all_collections_columns_table(self, session, insert_data=TEST_DATA):
        logger.debug("Insert data")
        for data in insert_data:
            data_str = f"{data[0]},{data[1]},{data[2]},{data[3]},{data[4]},{data[5]},{data[6]},{data[7]},{data[8]},{data[9]},{data[10]},{data[11]},{data[12]}"

            stmt = f"INSERT INTO {self.TABLE_NAME}({self.INSERT_COLUMNS}) VALUES({data_str})"

            session.execute(stmt)

    def test_filter_by_one_non_indexed_collection_column(self):
        session = self.prepare()
        self.create_all_collections_table(session=session)

        self.insert_data_in_all_collections_columns_table(session=session)

        select_stmt = f"select id from {self.TABLE_NAME} "

        logger.debug("Filter by list of integer non-indexed column")
        assert_all(session=session, query=select_stmt + "where list_int CONTAINS 4 ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by list of blob non-indexed column")
        assert_all(session=session, query=select_stmt + "where list_blob CONTAINS textAsBlob('t1') ALLOW FILTERING", expected=[[0]], ignore_order=True)

        logger.debug("Filter by set of uuid non-indexed column")
        assert_all(session=session, query=select_stmt + "where set_uuid CONTAINS 8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4 ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by set of text non-indexed column")
        assert_all(session=session, query=select_stmt + "where set_text CONTAINS 't5' ALLOW FILTERING", expected=[[0], [1]], ignore_order=True)

        logger.debug("Filter by map <text, boolean> non-indexed column")
        assert_all(session=session, query=select_stmt + "where map_bool CONTAINS False and map_bool CONTAINS KEY 'a1' ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by map <text, uuid> non-indexed column")
        assert_all(session=session, query=select_stmt + "where map_uuid CONTAINS f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4  ALLOW FILTERING", expected=[[0]], ignore_order=True)

        logger.debug("Filter by map <text, uuid> non-indexed column")
        assert_all(session=session, query=select_stmt + "where map_uuid CONTAINS KEY 'a1' ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen list of integer non-indexed column")
        assert_all(session=session, query=select_stmt + "where f_list_int CONTAINS 8 ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen list of text non-indexed column")
        assert_all(session=session, query=select_stmt + "where f_list_text CONTAINS 'f4' ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen set of int non-indexed column (EQUAL)")
        assert_all(session=session, query=select_stmt + "where f_set_int = {9, 7}  ALLOW FILTERING", expected=[[0]], ignore_order=True)

        logger.debug("Filter by frozen set of int non-indexed column (CONTAINS)")
        assert_all(session=session, query=select_stmt + "where f_set_int CONTAINS 9 and f_set_int CONTAINS 10  ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen set of text non-indexed column")
        assert_all(session=session, query=select_stmt + "where f_set_text CONTAINS 'f6' ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen map of text non-indexed column (EQUAL)")
        assert_all(session=session, query=select_stmt + "where f_map_text = {'f2': 'e', 'f1': 'c'} ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen map of text non-indexed column (CONTAINS)")
        assert_all(session=session, query=select_stmt + "where f_map_text CONTAINS 'c' ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen map of text non-indexed column (CONTAINS KEY)")
        assert_all(session=session, query=select_stmt + "where f_map_text CONTAINS KEY 'f2' ALLOW FILTERING", expected=[[1]], ignore_order=True)

        logger.debug("Filter by frozen map of int non-indexed column (CONTAINS)")
        assert_all(session=session, query=select_stmt + "where f_map_int CONTAINS 2 ALLOW FILTERING", expected=[[0], [1]], ignore_order=True)

    def test_filter_by_pk_and_two_non_indexed_collection_column(self):
        session = self.prepare()
        self.create_all_collections_table(session=session)

        self.insert_data_in_all_collections_columns_table(session=session)

        select_stmt = f"select id from {self.TABLE_NAME} "

        logger.debug("Filter by PK, map of uusi and frozen set of integer non-indexed column")
        assert_all(session=session, query=select_stmt + "where id = 0 and map_uuid CONTAINS f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4 and f_set_int CONTAINS 9 ALLOW FILTERING", expected=[[0]], ignore_order=True)

        logger.debug("Filter by PK, map of uuid and frozen set of integer non-indexed column")
        assert_all(session=session, query=select_stmt + "where id = 1 and set_uuid CONTAINS 8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4 and f_map_int = {'f1': 1, 'f2': 2, 'f3': 3} ALLOW FILTERING", expected=[[1]], ignore_order=True)

    def test_empty_data_set_result(self):
        session = self.prepare()
        self.create_all_collections_table(session=session)

        self.insert_data_in_all_collections_columns_table(session=session)

        select_stmt = f"select id from {self.TABLE_NAME} "

        logger.debug("Filter by PK, map of uuid and frozen set of integer non-indexed column")
        assert_all(session=session, query=select_stmt + "where id = 0 and map_uuid CONTAINS f54f6a76-b383-11e9-a2a3-2a2ae2dbcce4 and f_set_int CONTAINS 9 ALLOW FILTERING", expected=[], ignore_order=True)


@pytest.mark.dtest_full
@pytest.mark.lwt
class TestLWTWithCQL(Tester):
    """
    Validate CQL queries for LWTs for static columns for null and non-existing rows
    @jira_ticket CASSANDRA-9842
    """

    def get_lwttester_session(self):
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute(
            """CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION={'class':'NetworkTopologyStrategy',
            'replication_factor':1}"""
        )
        session.execute("USE ks")
        return session

    def prepare(self):
        cluster = self.cluster

        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        return self.get_lwttester_session()

    def test_lwt_with_static_columns(self):
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE lwt_with_static (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """
        )

        assert_one(session, "UPDATE lwt_with_static SET s = 1 WHERE a = 1 IF s = NULL", [True, None])

        assert_one(session, "SELECT * FROM lwt_with_static", [1, None, 1, None])

        assert_one(session, "UPDATE lwt_with_static SET s = 2 WHERE a = 2 IF EXISTS", [False, None, None, None, None])

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 1", [1, None, 1, None])

        assert_one(session, "INSERT INTO lwt_with_static (a, s) VALUES (2, 2) IF NOT EXISTS", [True, None, None, None, None])

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 2", [2, None, 2, None])

        assert_all(
            session, "BEGIN BATCH\n" + "INSERT INTO lwt_with_static (a, b, d) values (3, 3, 'a');\n" + "UPDATE lwt_with_static SET s = 3 WHERE a = 3 IF s = null;\n" + "APPLY BATCH;", [[True, None, None, None], [True, None, None, None]]
        )

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 3", [3, 3, 3, "a"])

        # LWT applies before INSERT
        assert_all(
            session, "BEGIN BATCH\n" + "INSERT INTO lwt_with_static (a, b, d) values (4, 4, 'a');\n" + "UPDATE lwt_with_static SET s = 4 WHERE a = 4 IF s = null;\n" + "APPLY BATCH;", [[True, None, None, None], [True, None, None, None]]
        )

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 4", [4, 4, 4, "a"])

    def _validate_non_existing_or_null_values(self, table_name, session):
        assert_one(session, f"UPDATE {table_name} SET s = 1 WHERE a = 1 IF s = NULL", [True, None])

        assert_one(session, f"SELECT a, s, d FROM {table_name} WHERE a = 1", [1, 1, None])

        assert_one(session, f"UPDATE {table_name} SET s = 2 WHERE a = 2 IF s IN (10,20,NULL)", [True, None])

        assert_one(session, f"SELECT a, s, d FROM {table_name} WHERE a = 2", [2, 2, None])

        assert_one(session, f"UPDATE {table_name} SET s = 4 WHERE a = 4 IF s != 4", [True, None])

        assert_one(session, f"SELECT a, s, d FROM {table_name} WHERE a = 4", [4, 4, None])

    def test_conditional_updates_on_static_columns_with_null_values(self):
        session = self.prepare()

        table_name = "conditional_updates_on_static_columns_with_null"
        session.execute(
            f"""
            CREATE TABLE {table_name} (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """
        )

        for i in range(1, 6):
            session.execute(f"INSERT INTO {table_name} (a, b) VALUES ({i}, {i})")

        self._validate_non_existing_or_null_values(table_name, session)

        assert_one(session, f"UPDATE {table_name} SET s = 30 WHERE a = 3 IF s IN (10,20,30)", [False, None])

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 3", [3, 3, None, None])

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, f"UPDATE {table_name} SET s = 50 WHERE a = 5 IF s {operator} 3", [False, None])

            assert_one(session, f"SELECT * FROM {table_name} WHERE a = 5", [5, 5, None, None])

    def test_conditional_updates_on_static_columns_with_non_existing_values(self):
        session = self.prepare()

        table_name = "conditional_updates_on_static_columns_with_ne"
        session.execute(
            f"""
            CREATE TABLE {table_name} (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """
        )

        self._validate_non_existing_or_null_values(table_name, session)

        assert_one(session, f"UPDATE {table_name} SET s = 30 WHERE a = 3 IF s IN (10,20,30)", [False, None])

        assert_none(session, f"SELECT * FROM {table_name} WHERE a = 3")

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, f"UPDATE {table_name} SET s = 50 WHERE a = 5 IF s {operator} 3", [False, None])

            assert_none(session, f"SELECT * FROM {table_name} WHERE a = 5")

    def _validate_non_existing_or_null_values_batch(self, table_name, session):
        assert_all(
            session,
            f"""
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, d) values (2, 2, 'a');
                UPDATE {table_name} SET s = 2 WHERE a = 2 IF s = null;
            APPLY BATCH""",
            [[True, 2, 2, None], [True, 2, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 2", [2, 2, 2, "a"])

        assert_all(
            session,
            f"""
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s, d) values (4, 4, 4, 'a')
                UPDATE {table_name} SET s = 5 WHERE a = 4 IF s = null;
            APPLY BATCH""",
            [[True, 4, 4, None], [True, 4, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 4", [4, 4, 5, "a"])

        assert_all(
            session,
            f"""
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s, d) values (5, 5, 5, 'a')
                UPDATE {table_name} SET s = 6 WHERE a = 5 IF s IN (1,2,null)
            APPLY BATCH""",
            [[True, 5, 5, None], [True, 5, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 5", [5, 5, 6, "a"])

        assert_all(
            session,
            f"""
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s, d) values (7, 7, 7, 'a')
                UPDATE {table_name} SET s = 8 WHERE a = 7 IF s != 7;
            APPLY BATCH""",
            [[True, None, None, None], [True, None, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 7", [7, 7, 8, "a"])

    def test_conditional_updates_on_static_columns_with_null_values_batch(self):
        session = self.prepare()

        table_name = "lwt_on_static_columns_with_null_batch"
        session.execute(
            f"""
            CREATE TABLE {table_name} (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """
        )

        for i in range(1, 7):
            session.execute(f"INSERT INTO {table_name} (a, b) VALUES ({i}, {i})")

        self._validate_non_existing_or_null_values_batch(table_name, session)

        for operator in [">", "<", ">=", "<=", "="]:
            assert_all(
                session,
                f"""
                BEGIN BATCH
                    INSERT INTO {table_name} (a, b, s, d) values (3, 3, 40, 'a')
                    UPDATE {table_name} SET s = 30 WHERE a = 3 IF s {operator} 5;
                APPLY BATCH""",
                [[False, 3, 3, None], [False, 3, None, None]],
            )

            assert_one(session, f"SELECT * FROM {table_name} WHERE a = 3", [3, 3, None, None])

        assert_all(
            session,
            f"""
                BEGIN BATCH
                    INSERT INTO {table_name} (a, b, s, d) values (6, 6, 70, 'a')
                    UPDATE {table_name} SET s = 60 WHERE a = 6 IF s IN (1,2,3)
                APPLY BATCH""",
            [[False, 6, 6, None], [False, 6, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 6", [6, 6, None, None])

    def test_conditional_deletes_on_static_columns_with_null_values(self):
        session = self.prepare()

        table_name = "conditional_deletes_on_static_with_null"
        session.execute(
            f"""
            CREATE TABLE {table_name} (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))
        """
        )

        for i in range(1, 6):
            session.execute(f"INSERT INTO {table_name} (a, b, s1, s2, v) VALUES ({i}, {i}, {i}, null, {i})")

        assert_one(session, f"DELETE s1 FROM {table_name} WHERE a = 1 IF s2 = null", [True, None])

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 1", [1, 1, None, None, 1])

        assert_one(session, f"DELETE s1 FROM {table_name} WHERE a = 2 IF s2 IN (10,20,30)", [False, None])

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 2", [2, 2, 2, None, 2])

        assert_one(session, f"DELETE s1 FROM {table_name} WHERE a = 3 IF s2 IN (null,20,30)", [True, None])

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 3", [3, 3, None, None, 3])

        assert_one(session, f"DELETE s1 FROM {table_name} WHERE a = 4 IF s2 != 4", [True, None])

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 4", [4, 4, None, None, 4])

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, f"DELETE s1 FROM {table_name} WHERE a = 5 IF s2 {operator} 3", [False, None])
            assert_one(session, f"SELECT * FROM {table_name} WHERE a = 5", [5, 5, 5, None, 5])

    def test_conditional_deletes_on_static_columns_with_null_values_batch(self):
        session = self.prepare()

        table_name = "conditional_deletes_on_static_with_null_batch"
        session.execute(
            f"""
            CREATE TABLE {table_name} (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))
        """
        )

        assert_all(
            session,
            f"""
             BEGIN BATCH
                 INSERT INTO {table_name} (a, b, s1, v) values (2, 2, 2, 2);
                 DELETE s1 FROM {table_name} WHERE a = 2 IF s2 = null;
             APPLY BATCH""",
            [[True, None, None, None], [True, None, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 2", [2, 2, None, None, 2])

        for operator in [">", "<", ">=", "<=", "="]:
            assert_all(
                session,
                f"""
                BEGIN BATCH
                    INSERT INTO {table_name} (a, b, s1, v) values (3, 3, 3, 3);
                    DELETE s1 FROM {table_name} WHERE a = 3 IF s2 {operator} 5;
                APPLY BATCH""",
                [[False, None, None, None], [False, None, None, None]],
            )

            assert_none(session, f"SELECT * FROM {table_name} WHERE a = 3")

        assert_all(
            session,
            f"""
             BEGIN BATCH
                 INSERT INTO {table_name} (a, b, s1, v) values (6, 6, 6, 6);
                 DELETE s1 FROM {table_name} WHERE a = 6 IF s2 IN (1,2,3);
             APPLY BATCH""",
            [[False, None, None, None], [False, None, None, None]],
        )

        assert_none(session, f"SELECT * FROM {table_name} WHERE a = 6")

        assert_all(
            session,
            f"""
             BEGIN BATCH
                 INSERT INTO {table_name} (a, b, s1, v) values (4, 4, 4, 4);
                 DELETE s1 FROM {table_name} WHERE a = 4 IF s2 = null;
             APPLY BATCH""",
            [[True, None, None, None], [True, None, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 4", [4, 4, None, None, 4])

        assert_all(
            session,
            f"""
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s1, v) VALUES (5, 5, 5, 5);
                DELETE s1 FROM {table_name} WHERE a = 5 IF s1 IN (1,2,null);
            APPLY BATCH""",
            [[True, None, None, None], [True, None, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 5", [5, 5, None, None, 5])

        assert_all(
            session,
            f"""
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s1, v) values (7, 7, 7, 7);
                DELETE s1 FROM {table_name} WHERE a = 7 IF s2 != 7;
            APPLY BATCH""",
            [[True, None, None, None], [True, None, None, None]],
        )

        assert_one(session, f"SELECT * FROM {table_name} WHERE a = 7", [7, 7, None, None, 7])

    def test_lwt_with_empty_resultset(self):
        """
        LWT with unset row.
        @jira_ticket CASSANDRA-12694
        """
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE test (pk text, v1 int, v2 text, PRIMARY KEY (pk));
        """
        )
        session.execute("update test set v1 = 100 where pk = 'test1';")
        node1 = self.cluster.nodelist()[0]
        self.cluster.flush()
        assert_one(session, "UPDATE test SET v1 = 100 WHERE pk = 'test1' IF v2 = null;", [True, None])

    def test_batch_delete_insert_same_row(self):
        """
        Issue: 6273

        Delete have priority above Insert.
        """
        session = self.prepare()
        table_name = "cf"
        session.execute(
            f"""
                        CREATE COLUMNFAMILY {table_name} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))
                        """
        )
        assert_one(session, f"""INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;""", [True, None, None, None])

        assert_all(
            session,
            f"""BEGIN BATCH
                        DELETE FROM {table_name} WHERE key=1 and ck=0 if exists;
                        INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'b', 'c'}});
                      APPLY BATCH;""",
            [[True, 1, 0, {"a", "b"}], [True, 1, 0, {"a", "b"}]],
        )

        assert_none(session, f"SELECT * FROM {table_name}")

    def test_batch_insert_delete_same_row(self):
        """
        Issue: 6273

        Delete have priority above Insert.
        """
        session = self.prepare()
        table_name = "cf"
        session.execute(
            f"""
                        CREATE COLUMNFAMILY {table_name} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))
                        """
        )
        assert_one(session, f"""INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;""", [True, None, None, None])

        assert_all(
            session,
            f"""BEGIN BATCH
                        INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'b', 'c'}});
                        DELETE FROM {table_name} WHERE key=1 and ck=0 if exists;
                      APPLY BATCH;""",
            [[True, 1, 0, {"a", "b"}], [True, 1, 0, {"a", "b"}]],
        )

        assert_none(session, f"SELECT * FROM {table_name}")

    def test_batch_insert_new_delete_old_row(self):
        """ """
        session = self.prepare()
        table_name = "cf"
        session.execute(
            f"""
                        CREATE COLUMNFAMILY {table_name} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))
                        """
        )
        assert_one(session, f"""INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;""", [True, None, None, None])

        assert_all(
            session,
            f"""BEGIN BATCH
                        INSERT INTO {table_name} (key, ck, cv) VALUES (1, 1, {{'b', 'c'}}) IF NOT EXISTS;
                        DELETE FROM {table_name} WHERE key=1 and ck=0 if exists;
                      APPLY BATCH;""",
            [[True, None, None, None], [True, 1, 0, {"a", "b"}]],
        )

        assert_one(session, f"SELECT * FROM {table_name}", expected=[1, 1, {"b", "c"}])

    def test_batch_update_insert_same_row(self):
        """
        workaround for #6273
        """
        session = self.prepare()

        table_name = "cf"

        session.execute(
            f"""
                        CREATE COLUMNFAMILY {table_name} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))
                        """
        )

        assert_one(session, f"""INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;""", [True, None, None, None])

        assert_all(
            session,
            f"""BEGIN BATCH
                        UPDATE {table_name} SET cv=null WHERE key=1 and ck=0 if exists;
                        INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'b', 'c'}});
                      APPLY BATCH;""",
            [[True, 1, 0, {"a", "b"}], [True, 1, 0, {"a", "b"}]],
        )

        assert_one(session, f"SELECT * FROM {table_name}", [1, 0, {"b", "c"}])
