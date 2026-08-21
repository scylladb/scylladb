#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.assertions import assert_invalid, assert_one
from tools.cluster_topology import generate_cluster_topology

logger = logging.getLogger(__name__)


class BatchTester:
    """Helper class that contains batch test logic, allowing cluster reuse across tests."""

    def __init__(self, tester: Tester):
        self.tester = tester

    @property
    def cluster(self):
        return self.tester.cluster

    def patient_cql_connection(self, *args, **kwargs):
        return self.tester.patient_cql_connection(*args, **kwargs)

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

    def prepare_single_node(self):
        """Prepare a single-node cluster, reusing if already running."""
        if not self.cluster.nodelist():
            self.cluster.populate(generate_cluster_topology(rack_num=1))
            self.cluster.start(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        # Drop and recreate schema for test isolation
        session.execute("DROP KEYSPACE IF EXISTS ks")
        self.create_schema(session, 1)
        return session

    def check_batch_uses_proper_timestamp(self, session):
        """Test that each statement will be executed with provided BATCH timestamp"""
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

    def check_only_one_timestamp_is_valid(self, session):
        """Test that TIMESTAMP must not be used in the statements within the batch."""
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

    def check_each_statement_in_batch_uses_proper_timestamp(self, session):
        """Test that each statement will be executed with its own timestamp"""
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

    def check_multi_table_batch_for_10554(self, session):
        """Test a batch on 2 tables having different columns, restarting the node afterwards, to reproduce CASSANDRA-10554"""
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

    def check_unlogged_batch_gcgs_below_threshold_should_not_print_warning(self, session):
        """Test that unlogged batch does not print gc_grace_seconds warning"""
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

    def check_logged_batch_doesnt_throw_uae(self, session):
        """Test that logged batch DOES NOT throw UAE if there are at least 2 live nodes"""
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


class TestBatch(Tester):
    """Batch tests grouped by cluster topology to enable cluster reuse."""

    @pytest.mark.single_node
    def test_single_node_batch_group(self):
        """Group of tests that reuse a single-node cluster.

        Runs multiple batch sub-tests sequentially, reusing the same cluster
        to significantly improve execution time. The schema is dropped and
        recreated between sub-tests for isolation.
        """
        bt = BatchTester(self)

        session = bt.prepare_single_node()
        logger.info("Running: check_batch_uses_proper_timestamp")
        bt.check_batch_uses_proper_timestamp(session)

        session = bt.prepare_single_node()
        logger.info("Running: check_only_one_timestamp_is_valid")
        bt.check_only_one_timestamp_is_valid(session)

        session = bt.prepare_single_node()
        logger.info("Running: check_each_statement_in_batch_uses_proper_timestamp")
        bt.check_each_statement_in_batch_uses_proper_timestamp(session)

        session = bt.prepare_single_node()
        logger.info("Running: check_multi_table_batch_for_10554")
        bt.check_multi_table_batch_for_10554(session)

        session = bt.prepare_single_node()
        logger.info("Running: check_unlogged_batch_gcgs_below_threshold_should_not_print_warning")
        bt.check_unlogged_batch_gcgs_below_threshold_should_not_print_warning(session)

    def test_multi_node_batch_group(self):
        """Group of tests that reuse a 3-node cluster."""
        bt = BatchTester(self)

        if not self.cluster.nodelist():
            self.cluster.populate(generate_cluster_topology(rack_num=3))
            self.cluster.start(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
        session = bt.patient_cql_connection(node1)
        session.execute("DROP KEYSPACE IF EXISTS ks")
        bt.create_schema(session, 3)

        logger.info("Running: check_logged_batch_doesnt_throw_uae")
        bt.check_logged_batch_doesnt_throw_uae(session)
