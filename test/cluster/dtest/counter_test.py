#
# Copyright (C) 2011-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import glob
import logging
import os
import random
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unauthorized
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_cf, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_invalid, assert_one
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology, generate_cluster_topology_based_rf
from tools.data import rows_to_list
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)


class TestCounters(Tester):
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns += [
            r"raft_topology - raft_topology_cmd wait_for_ip failed with: seastar::sleep_aborted",
        ]

    def test_simple_increment(self):
        """Simple incrementation test (Created for #3465, that wasn't a bug)"""
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        create_ks(session, "ks", 3)
        create_cf(session, "cf", validation="CounterColumnType", columns={"c": "counter"})

        sessions = [self.patient_cql_connection(node, "ks") for node in nodes]
        nb_increment = 50
        nb_counter = 10

        for i in range(nb_increment):
            for c in range(nb_counter):
                session = sessions[(i + c) % len(nodes)]
                query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

            session = sessions[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in range(nb_counter)])
            query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.QUORUM)
            res = list(session.execute(query))

            assert len(res) == nb_counter
            for c in range(nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][1])

    def test_upgrade(self):
        """Test for bug of #4436"""

        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(generate_cluster_topology(rack_num=2)).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        create_ks(session, "ks", 2)

        query = """
            CREATE TABLE counterTable (
                k int PRIMARY KEY,
                c counter
            )
        """
        query = query + "WITH compression = { 'sstable_compression' : 'SnappyCompressor' }"

        session.execute(query)
        time.sleep(2)

        keys = range(4)
        updates = 50

        def make_updates():
            session = self.patient_cql_connection(nodes[0], keyspace="ks")
            upd = "UPDATE counterTable SET c = c + 1 WHERE k = %d;"
            batch = " ".join(["BEGIN COUNTER BATCH"] + [upd % x for x in keys] + ["APPLY BATCH;"])

            for i in range(updates):
                query = SimpleStatement(batch, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

        def check(i):
            session = self.patient_cql_connection(nodes[0], keyspace="ks")
            query = SimpleStatement("SELECT * FROM counterTable", consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))

            assert len(rows) == len(keys), "Expected %d rows, got %d: %s" % (len(keys), len(rows), str(rows))
            for row in rows:
                assert row[1] == i * updates, "Unexpected value %s" % str(row)

        def rolling_restart():
            # Rolling restart
            for i in range(2):
                time.sleep(0.2)
                nodes[i].nodetool("drain")
                nodes[i].stop(wait_other_notice=False)
                nodes[i].start(wait_other_notice=True, wait_for_binary_proto=True)
                time.sleep(0.2)

        make_updates()
        check(1)
        rolling_restart()

        make_updates()
        check(2)
        rolling_restart()

        make_updates()
        check(3)
        rolling_restart()

        check(3)

    def test_counter_consistency(self):
        """
        Do a bunch of writes with ONE, read back with ALL and check results.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)
        node1, _node2, _node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 3)

        stmt = """
              CREATE TABLE counter_table (
              id uuid PRIMARY KEY,
              counter_one COUNTER,
              counter_two COUNTER,
              )
           """
        session.execute(stmt)

        counters = []
        # establish 50 counters (2x25 rows)
        for i in range(25):
            _id = str(uuid.uuid4())
            counters.append({_id: {"counter_one": 1, "counter_two": 1}})

            query = SimpleStatement(
                f"""
                UPDATE counter_table
                SET counter_one = counter_one + 1, counter_two = counter_two + 1
                where id = {_id}""",
                consistency_level=ConsistencyLevel.ONE,
            )
            session.execute(query)

        # increment a bunch of counters with CL.ONE
        for i in range(10000):
            counter = counters[random.randint(0, len(counters) - 1)]
            counter_id = next(iter(counter.keys()))

            query = SimpleStatement(
                f"""
                UPDATE counter_table
                SET counter_one = counter_one + 2
                where id = {counter_id}""",
                consistency_level=ConsistencyLevel.ONE,
            )
            session.execute(query)

            query = SimpleStatement(
                f"""
                UPDATE counter_table
                SET counter_two = counter_two + 10
                where id = {counter_id}""",
                consistency_level=ConsistencyLevel.ONE,
            )
            session.execute(query)

            query = SimpleStatement(
                f"""
                UPDATE counter_table
                SET counter_one = counter_one - 1
                where id = {counter_id}""",
                consistency_level=ConsistencyLevel.ONE,
            )
            session.execute(query)

            query = SimpleStatement(
                f"""
                UPDATE counter_table
                SET counter_two = counter_two - 5
                where id = {counter_id}""",
                consistency_level=ConsistencyLevel.ONE,
            )
            session.execute(query)

            # update expectations to match (assumed) db state
            counter[counter_id]["counter_one"] += 1
            counter[counter_id]["counter_two"] += 5

        # let's verify the counts are correct, using CL.ALL
        for counter_dict in counters:
            counter_id = next(iter(counter_dict.keys()))

            query = SimpleStatement(
                f"""
                SELECT counter_one, counter_two
                FROM counter_table WHERE id = {counter_id}
                """,
                consistency_level=ConsistencyLevel.ALL,
            )
            rows = list(session.execute(query))

            counter_one_actual, counter_two_actual = rows[0]

            assert counter_one_actual == counter_dict[counter_id]["counter_one"]
            assert counter_two_actual == counter_dict[counter_id]["counter_two"]

    def test_multi_counter_update(self):
        """
        Test for singlular update statements that will affect multiple counters.
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)
        node1, _node2, _node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 3)

        session.execute(
            """
            CREATE TABLE counter_table (
            id text,
            myuuid uuid,
            counter_one COUNTER,
            PRIMARY KEY (id, myuuid))
            """
        )

        expected_counts = {}

        # set up expectations
        for i in range(1, 6):
            _id = uuid.uuid4()

            expected_counts[_id] = i

        for k, v in expected_counts.items():
            session.execute(
                f"""
                UPDATE counter_table set counter_one = counter_one + {v}
                WHERE id='foo' and myuuid = {k}
                """
            )

        for k, v in expected_counts.items():
            count = list(
                session.execute(
                    f"""
                SELECT counter_one FROM counter_table
                WHERE id = 'foo' and myuuid = {k}
                """
                )
            )

            assert count and len(count[0]), f"Expected counter_one={v} for myuuid={k}, got: {count}"
            assert v == count[0][0]

    @pytest.mark.single_node
    def test_validate_empty_column_name(self):
        cluster = self.cluster
        cluster.set_configuration_options(
            values={
                "cache_hit_rate_read_balancing": False,
                "enable_create_table_with_compact_storage": True,
            }
        )

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 1)

        session.execute(
            """
            CREATE TABLE compact_counter_table (
                pk int,
                ck text,
                value counter,
                PRIMARY KEY (pk, ck))
            WITH COMPACT STORAGE
            """
        )

        assert_invalid(session, "UPDATE compact_counter_table SET value = value + 1 WHERE pk = 0 AND ck = ''")
        assert_invalid(session, "UPDATE compact_counter_table SET value = value - 1 WHERE pk = 0 AND ck = ''")

        session.execute("UPDATE compact_counter_table SET value = value + 5 WHERE pk = 0 AND ck = 'ck'")
        session.execute("UPDATE compact_counter_table SET value = value - 2 WHERE pk = 0 AND ck = 'ck'")

        assert_one(session, "SELECT pk, ck, value FROM compact_counter_table", [0, "ck", 3])

    @pytest.mark.single_node
    def test_drop_counter_column(self):
        """Test for CASSANDRA-7831"""
        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        session.execute("UPDATE counter_bug SET c = c + 1 where t = 1")
        row = list(session.execute("SELECT * from counter_bug"))

        assert rows_to_list(row)[0] == [1, 1]
        assert len(row) == 1

        session.execute("ALTER TABLE counter_bug drop c")

        assert_invalid(session, "ALTER TABLE counter_bug add c counter", "Cannot re-add previously dropped counter column c")

    def test_increment_counters_in_threads(self):
        """
        3 nodes in test
        increment 2 counters * 200 threads * 500 times
        expected result: counters equal 100000(500*200)
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        create_ks(session, "ks", 3)
        create_cf(session, "cf", validation="CounterColumnType", columns={"c": "counter"})

        sessions = [self.patient_cql_connection(node, "ks") for node in nodes]
        nb_increment = 500
        if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
            nb_increment //= 10
        nb_counter = 2

        def run(connection):
            for i in range(nb_increment):
                for c in range(nb_counter):
                    query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.QUORUM)
                    connection.execute(query)

        threads = []
        num_threads = 200
        if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
            num_threads //= 10
        executor = ThreadPoolExecutor(max_workers=num_threads)
        for x in range(num_threads):
            conn = sessions[x % len(nodes)]
            threads.append(executor.submit(run, conn))
        for t in threads:
            t.result()

        conn = sessions[1 % len(nodes)]
        keys = ",".join(["'counter%i'" % c for c in range(nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.QUORUM)
        res = list(conn.execute(query))
        expected_counters = nb_increment * num_threads

        assert len(res) == nb_counter
        for c in range(nb_counter):
            assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
            assert res[c][1] == expected_counters, "Expecting counter%i = %i, got %i" % (c, expected_counters, res[c][1])

    def test_increment_decrement_counters_in_threads(self):
        """
        3 nodes in test
        2 counters:
        increment 500 times * 400 threads and
        decrement 500 times * 200 threads in parallel
        expected result: counters equal 100000(500*200)
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        create_ks(session, "ks", 3)
        create_cf(session, "cf", validation="CounterColumnType", columns={"c": "counter"})

        sessions = [self.patient_cql_connection(node, "ks") for node in nodes]
        nb_increment = 500
        if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
            nb_increment //= 10
        nb_counter = 2

        def run(connection, decrement):
            for i in range(nb_increment):
                for c in range(nb_counter):
                    if decrement:
                        query = SimpleStatement("UPDATE cf SET c = c - 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.QUORUM)
                    else:
                        query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.QUORUM)
                    connection.execute(query)

        threads = []
        num_threads = 600
        if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
            num_threads //= 10
        executor = ThreadPoolExecutor(max_workers=num_threads)
        for x in range(num_threads):
            conn = sessions[x % len(nodes)]
            decrement = (x % len(nodes)) == 0
            threads.append(executor.submit(run, conn, decrement))
        for t in threads:
            t.result()

        conn = sessions[1 % len(nodes)]
        keys = ",".join(["'counter%i'" % c for c in range(nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.QUORUM)
        res = list(conn.execute(query))
        expected_counters = nb_increment * num_threads // 3

        assert len(res) == nb_counter
        for c in range(nb_counter):
            assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
            assert res[c][1] == expected_counters, "Expecting counter%i = %i, got %i" % (c, expected_counters, res[c][1])

    @pytest.mark.single_node
    def test_update_counter_with_ttl_and_timestamp_negative(self):
        """
        Try to update counter column using TTL/TIMESTAMP option
        Result: should be rejected
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.populate(1).start()
        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, "Test", 1)
        session.execute("CREATE TABLE counters (t int PRIMARY KEY, c counter)")

        for option in ("TTL 5", "TIMESTAMP 11223344"):
            with pytest.raises(InvalidRequest):
                session.execute(f"UPDATE counters USING {option} SET c = c + 1 where t = 1")

    @pytest.mark.single_node
    def test_prepare_statement(self):
        """
        update counters with prepare statement, and verify the data
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        logger.debug("Created counter table, try to update one counter")
        session.execute("UPDATE counter_bug SET c = c + 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        assert rows == [[0, 1]]
        # reset the counter (key=0) to 0
        session.execute("UPDATE counter_bug SET c = c - 1 where t = 0")

        keys_num = 1000

        counter_list = []
        logger.debug("Update %s counters with random int by prepare statement" % keys_num)
        for key in range(keys_num):
            statement = session.prepare("update counter_tests.counter_bug set c = c + ? where t = ?")
            # int is from `-sys.maxsize - 1` to `sys.maxsize`, we will reupdate
            # counters with random int, so sys.maxsize // 2 is safe to avoid rollover
            rand_c = random.randint(-sys.maxsize // 2, sys.maxsize // 2)
            counter_list.append([key, rand_c])
            session.execute(statement.bind((rand_c, key)))
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        for row in rows:
            assert row in counter_list, "Counter isn't updated correctly"
        assert len(rows) == keys_num
        logger.debug("Verified that all counters are updated correctly")

        logger.debug("Reupdate all counters")
        for key in range(keys_num):
            statement = session.prepare("update counter_tests.counter_bug set c = c + ? where t = ?")
            rand_c = random.randint(-sys.maxsize // 2, sys.maxsize // 2)
            session.execute(statement.bind((rand_c, key)))
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == keys_num
        logger.debug("Verified that counters number is correct: %s" % keys_num)

        logger.debug("drop all counters")
        for key in range(keys_num):
            session.execute("DELETE c FROM counter_tests.counter_bug where t = %s" % key)
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 0

    def assert_unauthorized(self, message, session, query):
        with pytest.raises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, str(cm)), f"Expected '{message}', but got '{cm.typename}'"

    @pytest.mark.single_node
    def test_static_counter_column(self):
        """
        Test of static counter column
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "Test", 1)
        session.execute("CREATE TABLE Test.cf (pk int, ck int, s counter static, v counter, primary key (pk, ck))")

        logger.debug("Update counters")
        pk = 10
        incr = 1
        for i in range(1, 101):
            session.execute(f"UPDATE Test.cf SET s=s+{incr}, v=v+{i + 1} where pk = {pk} and ck = {i}")

        logger.debug("Verify counter data")
        res = session.execute("SELECT * FROM Test.cf;")
        assert len(rows_to_list(res)) == 100
        res = session.execute("SELECT s,v FROM Test.cf;")
        rows = sorted(rows_to_list(res), key=lambda x: (x and x[0] is not None, x))
        for i in range(1, 101):
            assert rows[i - 1][0] == 100
            assert rows[i - 1][1] == i + 1

        logger.debug("Update static counter column")
        incr = 10
        for i in range(1, 11):
            # update static counter with two methods, they have same effect
            if i % 2 == 0:
                # update all items of same pk
                session.execute(f"UPDATE Test.cf SET s=s+{incr} where pk = {pk}")
            else:
                # only update one item that is assigned by pk + ck
                session.execute(f"UPDATE Test.cf SET s=s+{incr}, v=v+{0} where pk = {pk} and ck = {i}")

        logger.debug("Verify counter data")
        res = session.execute("SELECT s,v FROM Test.cf;")
        rows = sorted(rows_to_list(res), key=lambda x: (x and x[0] is not None, x))
        assert len(rows) == 100
        for i in range(1, 101):
            assert rows[i - 1][0] == 200
            assert rows[i - 1][1] == i + 1

    def test_compact_counter_cluster(self):
        """
        @jira_ticket CASSANDRA-12219
        """
        cluster = self.cluster

        cluster.set_configuration_options(
            values={
                "cache_hit_rate_read_balancing": False,
                "enable_create_table_with_compact_storage": True,
            }
        )
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 1)

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS counter_cs (
                key bigint PRIMARY KEY,
                data counter
            ) WITH COMPACT STORAGE
            """
        )

        for outer in range(5):
            for idx in range(5):
                session.execute(f"UPDATE counter_cs SET data = data + 1 WHERE key = {idx}")

        for idx in range(5):
            row = list(session.execute(f"SELECT data from counter_cs where key = {idx}"))
            assert rows_to_list(row)[0][0] == 5


class TestCountersOnMultipleNodes(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"start_rpc": "true"})
        self._start_row = 2
        self._row_cnt = 1000
        self._extra_row_cnt = 0
        return dtest_setup_overrides

    @pytest.fixture(scope="function", autouse=True)
    def setup_and_teardown(self):
        logger.debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        yield

        row_cnt = self._row_cnt - self._start_row + self._extra_row_cnt
        self._verify_data(row_cnt)
        logger.debug("Update counter data")
        session = self.patient_cql_connection(self.node1)
        for i in range(self._start_row, self._row_cnt):
            session.execute(f"UPDATE Test.cf SET cnt = cnt - 1 WHERE pk = {i};")
            session.execute(f"UPDATE Test.cf SET cnt = cnt + 1 WHERE pk = {i};")

        self._verify_data(row_cnt)

    def _populate_data(self, rf=2):
        session = self.patient_cql_connection(self.node1)
        create_ks(session, "Test", rf)

        session.execute(
            """
                    CREATE TABLE cf (
                        pk INT PRIMARY KEY,
                        cnt COUNTER
                    ) WITH read_repair_chance=0.0;
                """
        )

        logger.debug("Update counter data")
        for i in range(self._start_row, self._row_cnt):
            session.execute(SimpleStatement(f"UPDATE Test.cf SET cnt = cnt + {i} WHERE pk = {i};", consistency_level=ConsistencyLevel.ALL))
            session.execute(f"UPDATE Test.cf SET cnt = cnt - 1 WHERE pk = {i};")
            session.execute(f"UPDATE Test.cf SET cnt = cnt + 1 WHERE pk = {i};")

        self._verify_data(self._row_cnt - self._start_row)

    def _verify_data(self, expected_row_count):
        logger.debug("Verify counter data")
        session = self.patient_cql_connection(self.node1)
        res = session.execute("SELECT * FROM Test.cf;")
        rows = rows_to_list(res)
        assert len(rows) == expected_row_count
        for row in rows:
            assert row[0] == row[1]

    def _verify_data_repair(self, expected_row_count):
        for node in (self.node1, self.node2):
            node.stop(wait_other_notice=True)

        session = self.patient_cql_connection(self.node3)
        pk_list = ",".join([str(i) for i in range(self._row_cnt, self._row_cnt + self._extra_row_cnt)])
        query = SimpleStatement(f"SELECT * FROM Test.cf WHERE pk IN ({pk_list});", consistency_level=ConsistencyLevel.ONE)
        res = session.execute(query)
        rows = rows_to_list(res)
        assert len(rows) == expected_row_count

        for node in (self.node1, self.node2):
            node.start(wait_other_notice=True, wait_for_binary_proto=True)

    def _verify_data_rebuild(self):
        for node in (self.node1, self.node2):
            node.stop(wait_other_notice=True)

        logger.debug("Verify counter data on node3")
        session = self.patient_cql_connection(self.node3)
        query = SimpleStatement("SELECT * FROM Test.cf;", consistency_level=ConsistencyLevel.ONE)
        res = session.execute(query)
        rows = rows_to_list(res)
        assert len(rows) == self._row_cnt - self._start_row
        for row in rows:
            assert row[0] == row[1]

        for node in (self.node1, self.node2):
            node.start(wait_other_notice=True)

    def test_counter_consistency_node_replace(self):
        """
        Cluster: 3 nodes, keyspace RF=2
        Populate counters data, replace one of the nodes by a new one
        Result: counters data stays consistent
        """
        self.cluster.populate(generate_cluster_topology_based_rf(nodes=3, rf=2)).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = self.cluster.nodelist()
        self._populate_data(rf=2)
        logger.debug("Stop node3 and create new node to replace it")
        self.node3.stop(gently=True, wait_other_notice=True)
        node4 = new_node(self.cluster, bootstrap=True, data_center=self.node3.data_center, rack=self.node3.rack)
        logger.debug("Start the new node")
        node4.start(replace_node_host_id=self.node3.hostid(), wait_for_binary_proto=True)

    def test_counter_consistency_node_remove(self):
        """
        Cluster: 3 nodes, keyspace RF=2
        Populate counters data, remove one of the nodes
        Result: counters data stays consistent
        """
        self.cluster.populate({"dc1": {"rack1": 2, "rack2": 1}}).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = self.cluster.nodelist()
        self._populate_data(rf=2)
        logger.debug("Stop and remove node2")
        node2_hostid = self.node2.hostid()
        self.node2.stop(wait_other_notice=True)
        self.node1.nodetool("removenode %s" % node2_hostid)

    def test_counter_consistency_node_add(self):
        """
        Cluster: 3 nodes, keyspace RF=2
        Populate counters data, add a new node
        Result: counters data stays consistent
        """
        self.cluster.populate(generate_cluster_topology_based_rf(nodes=3, rf=2)).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = self.cluster.nodelist()
        self._populate_data(rf=2)
        logger.debug("Add a new node")
        node4 = new_node(self.cluster, bootstrap=True, data_center=self.node3.data_center, rack=self.node3.rack)
        node4.start(wait_for_binary_proto=True)

    def test_counter_consistency_node_decommission(self):
        """
        Cluster: 3 nodes, keyspace RF=1
        Populate counters data, decommission one of the nodes
        Result: counters data stays consistent
        """
        self.cluster.populate(3).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = self.cluster.nodelist()
        self._populate_data(rf=1)
        logger.debug("Decommission node2")
        self.node2.decommission()
        self.node2.stop()

    def test_counter_consistency_node_repair(self):
        """
        Cluster: 3 nodes, keyspace RF=3
        Populate counters data, stop one of the nodes, change counter data
        Then start and repair the stopped node
        Result: counters data stays consistent
        """
        self.cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = self.cluster.nodelist()
        self._extra_row_cnt = 10
        self._populate_data(rf=3)
        logger.debug("Stop node3")
        self.node3.flush()
        self.node3.stop(wait_other_notice=True)

        logger.debug("Update counter data")
        session = self.patient_cql_connection(self.node1)
        for i in range(self._row_cnt, self._row_cnt + self._extra_row_cnt):
            query = SimpleStatement(f"UPDATE Test.cf SET cnt = cnt + {i} WHERE pk = {i};", consistency_level=ConsistencyLevel.TWO)
            session.execute(query)

        logger.debug("Start node3 and verify new data is not present")
        self.node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self._verify_data_repair(0)
        logger.debug("Repair node3")
        self.node3.repair()
        logger.debug("Verify new data is present on node3")
        self._verify_data_repair(self._extra_row_cnt)

    def test_counter_consistency_node_rebuild(self):
        """
        Cluster: 3 nodes, keyspace RF=3
        Populate counters data, stop one of the nodes and remove sstables and commit log for it
        Then start the node and rebuild it
        Result: counters data stays consistent
        """
        self.cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = self.cluster.nodelist()
        self._populate_data(rf=3)
        logger.debug("Stop node3")
        self.node3.flush()
        self.node3.stop(wait_other_notice=True)

        logger.debug("Remove sstables and commit log for node3")
        # We should keep the system tables and delete user tables
        for dir_name in ("commitlogs", "data/test"):
            data_dir = os.path.join(self.node3.get_path(), dir_name)
            logger.debug(f"Removing {data_dir}")
            files = glob.glob(os.path.join(self.node3.get_path(), dir_name, "*"))
            for f in files:
                self.cluster.remove_dir_with_retry(f)

        logger.debug("Start node3 and rebuild it")
        self.node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        if "tablets" not in self.scylla_features:
            self.node3.nodetool("rebuild")
        else:
            self.node3.nodetool("cluster repair")
        self._verify_data_rebuild()
