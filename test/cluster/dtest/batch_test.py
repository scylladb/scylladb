#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.assertions import assert_invalid, assert_one
from tools.cluster_topology import generate_cluster_topology

logger = logging.getLogger(__name__)


class TestBatch(Tester):
    @pytest.mark.skip_mode(mode='release', reason='Test should run in debug mode')
    @pytest.mark.skip_mode(mode='dev', reason='Test should run in debug mode')
    def test_replay_after_schema_change(self):
        """Test that logged batch is replayed after schema was changed on the node"""
        self.ignore_log_patterns += [
            r"raft - .* Transferring snapshot .* failed with: raft::transport_error",
        ]
        ring_delay_sec = 5
        self.cluster.set_configuration_options(values={"ring_delay_ms": ring_delay_sec * 1000})
        cmdline_args = ["--logger-log-level", "batchlog_manager=debug"]
        self.cluster.populate(3).start(jvm_args=cmdline_args)

        self.cluster.start(wait_other_notice=True)
        nodes = self.cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])

        logger.debug("Creating schema...")
        create_ks(session, "ks", 1)  # RF=1 so that we're sensitive for node2 missing updates
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

        names = ["k%d" % (i) for i in range(100)]

        st = SimpleStatement(
            """
            BEGIN BATCH
            %s
            APPLY BATCH
            """
            % ("\n".join(f"INSERT INTO users (id, firstname, lastname) VALUES ({i}, '{name}', '{name}')" for i, name in enumerate(names))),
            consistency_level=ConsistencyLevel.ALL,
        )

        logger.debug("Killing node2 so that batch fails")
        nodes[1].stop(gently=False, wait_other_notice=True)
        with pytest.raises(Unavailable):
            session.execute(st, timeout=60)

        logger.debug("Altering schema")
        session.execute("ALTER TABLE users add aa int;")

        logger.debug("Killing all other nodes so that they won't remember old schema during replay")
        for i in [0, 2]:
            nodes[i].flush()
            nodes[i].stop(gently=False)

        marks = []
        for node in nodes:
            marks.append(node.mark_log())

        logger.debug("Starting all nodes")
        self.cluster.start_nodes(wait_for_binary_proto=True, jvm_args=cmdline_args)

        logger.debug("Waiting for batch replay")
        for i, node in enumerate(nodes):
            for shard in range(node.smp()):
                node.watch_log_for(f"Batchlog replay on shard {shard}: done", from_mark=marks[i])

        rows = session.execute("SELECT * FROM users")
        res = sorted(rows)
        assert len(res) == len(names), f"expected length=len(names), got {res}"
        for i, name in enumerate(names):
            expected = [i, None, name, name]
            assert list(res[i]) == expected, f"Expected {expected}, got {res[i]}"

    def test_logged_batch_doesnt_throw_uae(self):
        """Test that logged batch DOES NOT throw UAE if there are at least 2 live nodes"""
        session = self.prepare(nodes=3)
        self.cluster.nodelist()[-1].stop(wait_other_notice=True)
        query = SimpleStatement(
            """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """,
            consistency_level=ConsistencyLevel.ANY,
        )
        session.execute(query)

    @pytest.mark.single_node
    def test_batch_uses_proper_timestamp(self):
        """Test that each statement will be executed with provided BATCH timestamp"""
        session = self.prepare()
        session.execute(
            """
            BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """
        )
        rows = session.execute("SELECT id, writetime(firstname), writetime(lastname) FROM users")
        res = sorted(rows)
        expected = [[0, 1111111111111111, 1111111111111111], [1, 1111111111111111, 1111111111111111]]
        assert [list(res[0]), list(res[1])] == expected, f"expected={expected}, got {res}"

    @pytest.mark.single_node
    def test_only_one_timestamp_is_valid(self):
        """Test that TIMESTAMP must not be used in the statements within the batch."""
        session = self.prepare()
        assert_invalid(
            session,
            """
            BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 2
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """,
            matching="Timestamp must be set either on BATCH or individual statements",
        )

    @pytest.mark.single_node
    def test_each_statement_in_batch_uses_proper_timestamp(self):
        """Test that each statement will be executed with its own timestamp"""
        session = self.prepare()
        session.execute(
            """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner') USING TIMESTAMP 1111111111111112
            APPLY BATCH
        """
        )
        rows = session.execute("SELECT id, writetime(firstname), writetime(lastname) FROM users")
        res = sorted(rows)
        expected = [[0, 1111111111111111, 1111111111111111], [1, 1111111111111112, 1111111111111112]]
        assert [list(res[0]), list(res[1])] == expected, f"expected={expected}, got {res}"

    @pytest.mark.single_node
    def test_multi_table_batch_for_10554(self):
        """Test a batch on 2 tables having different columns, restarting the node afterwards, to reproduce CASSANDRA-10554"""

        session = self.prepare()

        # prepare() adds users and clicks but clicks is a counter table, so
        # adding a random other table for this test.
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
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO dogs (dogid, dogname) VALUES (0, 'Pluto')
            APPLY BATCH
        """
        )

        assert_one(session, "SELECT * FROM users", [0, "Jack", "Sparrow"])
        assert_one(session, "SELECT * FROM dogs", [0, "Pluto"])

        # Flush and restart the node as it's how 10554 reproduces
        node1 = self.cluster.nodelist()[0]
        node1.flush()
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1, keyspace="ks")

        assert_one(session, "SELECT * FROM users", [0, "Jack", "Sparrow"])
        assert_one(session, "SELECT * FROM dogs", [0, "Pluto"])

    @pytest.mark.single_node
    def test_unlogged_batch_gcgs_below_threshold_should_not_print_warning(self):
        """Test that unlogged batch does not print gc_grace_seconds warning"""
        session = self.prepare()
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")
        session.execute(
            """
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """
        )
        node1 = self.cluster.nodelist()[0]
        warning = node1.grep_log("setting a too low gc_grace_seconds on tables involved in an atomic batch")
        logger.debug(warning)
        assert 0 == len(warning), "Cannot find the gc_grace_seconds warning message."

    def prepare(self, nodes=1, compression=True):
        if not self.cluster.nodelist():
            self.cluster.populate(generate_cluster_topology(rack_num=nodes))

            self.cluster.start(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_schema(session, nodes)
        return session

    def create_schema(self, session, rf):
        logger.debug("Creating schema...")
        create_ks(session, "ks", rf)

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

        time.sleep(0.5)
