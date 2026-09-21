#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, DriverException, Unavailable

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.log_utils import wait_for_any_log

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestLwtDestructiveDDL(Tester):
    """
    Destructive DDL in presence of LWT: execute destructive DDL
    instructions (i.e. statements which cause the executed LWT
    query to become invalid) in a loop in one connection,
    while running LWT queries against test table in another.

    Currently the following DDL nemesis operations are implemented:
    1. DROP + CREATE TABLE
    2. DROP + CREATE KEYSPACE/TABLE
    3. ALTER TABLE RENAME COLUMN TO (only pk columns are allowed)
    4. DROP + ADD COLUMN
    5. ALTER KEYSPACE (use NetworkTopologyStrategy, set to non-existing
       data center)
    6. REVOKE + GRANT PERMISSION ON TABLE
    """

    def prepare(self, ks_dc_mapping=None, setup_auth=False):
        is_multi_dc = ks_dc_mapping is not None
        if is_multi_dc:
            assert isinstance(ks_dc_mapping, dict)

        cluster = self.cluster

        configuration = {}

        if setup_auth:
            configuration.update(
                {
                    "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
                    "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
                    "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
                    # FIXME: this should be 0 to completely disable permissions cache altogether.
                    # But in such case we are running into https://github.com/scylladb/scylla/issues/6164.
                    #
                    # So make it least possible positive number to have "almost disabled"
                    # cache which will expire soon enough
                    # for testing purposes (rapid changes in permissions for a role).
                    "permissions_validity_in_ms": 1,
                }
            )

        cluster.set_configuration_options(values=configuration)
        # always generate cluster with {datacenter1: {rack1: 1, rack2: 1, rack3: 1}}
        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)

        self.nodes = cluster.nodelist()

        user = None
        password = None
        if setup_auth:
            found = wait_for_any_log(self.cluster.nodelist(), ["Created default superuser role", "Created default superuser authentication record"], 30, dispersed=True)
            if not found:
                raise Exception("Failed to create default superuser role during cluster startup")
            user = "cassandra"
            password = "cassandra"

        session = self.patient_cql_connection(self.nodes[0], user=user, password=password)

        if not is_multi_dc:
            create_ks(session, "ks", 3)
        else:
            create_ks(session, "ks", ks_dc_mapping)

        return session

    def _lwt_load_run(self, node, need_to_stop, tolerate_unavailable=False, user=None, password=None):
        thread_name = threading.current_thread().name

        session = self.patient_cql_connection(node, user=user, password=password)
        session.execute("USE ks")

        raw_dml_statements = ["INSERT INTO test (pk, v) VALUES (:pk, :v) IF NOT EXISTS", "UPDATE test SET v = 1001 WHERE pk = :pk IF v > -100 AND v < 100", "DELETE FROM test WHERE pk = :pk IF EXISTS"]
        dml_statements = [session.prepare(raw_stmt) for raw_stmt in raw_dml_statements]
        # Also include conditional BATCH statements in LWT workload:
        # wrap every kind of query into a batch.
        # Settle now only on single-statement batches for simplicity.
        dml_statements.extend(
            [
                session.prepare(
                    f"""
                BEGIN BATCH
                {raw_stmt}
                APPLY BATCH
                """
                )
                for raw_stmt in raw_dml_statements
            ]
        )

        for stmt in dml_statements:
            stmt.serial_consistency_level = ConsistencyLevel.SERIAL

        logger.info('Producing LWT load on the cluster (thread "%s")', thread_name)
        while True:
            if need_to_stop.is_set():
                break
            try:
                session.execute(random.choice(dml_statements), {"pk": random.randint(0, 10000), "v": random.randint(-1000, 1000)})
            except Unavailable as exc:
                logger.info('Failed to execute LWT statement (thread "%s"). Unavailable error: %s', thread_name, exc)
                if not tolerate_unavailable:
                    # If there is not enough replicas to properly execute the query
                    # it means that some node is DOWN for some reason, which we don't tolerate
                    logger.info('Aborting LWT worker thread "%s"', thread_name)
                    raise
            except DriverException as exc:
                logger.info('Failed to execute LWT statement (thread "%s"). Driver error: %s', thread_name, exc)
        logger.info('Finished LWT stress workload (thread "%s")', thread_name)

    def _ddl_run(self, node, need_to_stop, fn, user=None, password=None):
        thread_name = threading.current_thread().name

        ddl_session = self.patient_cql_connection(node, user=user, password=password)
        ddl_session.execute("USE ks")

        logger.info('Producing destructive DDL load on the cluster (thread "%s")', thread_name)
        while True:
            if need_to_stop.is_set():
                break
            try:
                fn_desc = fn.__doc__
                logger.info('Executing DDL statement: %s (thread "%s")', fn_desc, thread_name)
                fn(ddl_session)
                logger.info('Successfully executed DDL statement: %s (thread "%s")', fn_desc, thread_name)
            except Unavailable as exc:
                # That means that somebody is DOWN, bubble this exception up
                # to the main thread
                raise
            except DriverException as exc:
                logger.info('Failure during disruption thread operation (thread "%s"). Driver error: %s', thread_name, exc)
        logger.info('Finished DDL stress workload (thread "%s")', thread_name)

    def _case_template(self, session, ddl_fn, test_duration_sec=60, tolerate_unavailable=False, ddl_user=None, ddl_pass=None, lwt_user=None, lwt_pass=None):  # noqa: PLR0913
        # Replica-side LWT/paxos write timeouts under this load are expected and are
        # logged at ERROR by storage_proxy; ignore only that exception class.
        self.ignore_log_patterns.extend([r"exception during mutation write to .*: exceptions::mutation_write_timeout_exception"])

        create_test_table_stmt = session.prepare(
            """
            CREATE TABLE IF NOT EXISTS test (
                pk int PRIMARY KEY,
                v int
            )
        """
        )
        session.execute(create_test_table_stmt)

        if lwt_user:
            session.execute(f"GRANT ALL ON TABLE test TO {lwt_user}")

        need_to_stop = threading.Event()

        # Use 4 worker threads to emulate lwt load with dml statements and 4 threads for DDL workload
        lwt_load_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lwt_wrk_thr")
        ddl_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ddl_thr")
        for i in range(4):
            node_idx = (len(self.nodes) % (i + 1)) - 1
            lwt_load_executor.submit(self._lwt_load_run, self.nodes[node_idx], need_to_stop, tolerate_unavailable, lwt_user, lwt_pass)
            ddl_executor.submit(self._ddl_run, self.nodes[node_idx], need_to_stop, ddl_fn, ddl_user, ddl_pass)

        # Test duration is restricted to 60 seconds by default
        time.sleep(float(test_duration_sec))
        need_to_stop.set()

        # Wait for worker threads to complete
        lwt_load_executor.shutdown()
        ddl_executor.shutdown()

        # Assert that all nodes in the cluster are alive
        for node in self.nodes:
            assert node.is_live()

    def test_drop_table(self):
        """
        Tests that Scylla doesn't crash when there is concurrent LWT load in one
        thread and destructive DDL (DROP TABLE, CREATE TABLE) in another.
        """

        # These errors are expected to happen, ignore them
        self.ignore_log_patterns.extend(["Can't find a column family", "exception during mutation write", "mutation_write_failure_exception", "prune: failed"])

        session = self.prepare()
        create_test_table_stmt = session.prepare(
            """
            CREATE TABLE IF NOT EXISTS test (
                pk int PRIMARY KEY,
                v int
            )"""
        )
        drop_test_table_stmt = session.prepare("DROP TABLE IF EXISTS test")

        def drop_create_table_fn(session):
            """DROP + CREATE TABLE"""
            session.execute(drop_test_table_stmt)
            session.execute(create_test_table_stmt)

        self._case_template(session, drop_create_table_fn)

    def test_drop_keyspace(self, fixture_dtest_setup):
        """
        Tests that Scylla doesn't crash when there is concurrent LWT load in one
        thread and destructive DDL (DROP KEYSPACE, CREATE KEYSPACE + TABLE) in another.
        """

        # These errors are expected to happen, ignore them
        fixture_dtest_setup.ignore_log_patterns.extend(["Can't find a column family", "exception during mutation write", "Can't find a keyspace", "Column family test exists", "mutation_write_failure_exception", "prune: failed"])

        session = self.prepare()
        create_test_table_stmt = session.prepare(
            """
            CREATE TABLE IF NOT EXISTS test (
                pk int PRIMARY KEY,
                v int
            )"""
        )
        drop_test_ks_stmt = session.prepare("DROP KEYSPACE IF EXISTS ks")

        def drop_create_ks_fn(session):
            """DROP + CREATE KEYSPACE/TABLE"""
            session.execute(drop_test_ks_stmt)
            create_ks(session, "ks", 3)
            session.execute(create_test_table_stmt)

        self._case_template(session, drop_create_ks_fn)

    def test_rename_column(self):
        """
        Tests that Scylla doesn't crash when there is concurrent LWT load in one
        thread and destructive DDL (rename column participating in lwt statements) in another.

        Note that only columns that are part of primary key (partitioning and clustering key columns)
        can be renamed. The same restriction also applies to cassandra.
        """

        session = self.prepare()
        alter_column_stmt = session.prepare("ALTER TABLE test RENAME pk TO pk1")
        revert_alter_column_stmt = session.prepare("ALTER TABLE test RENAME pk1 TO pk")

        def rename_column_fn(session):
            """RENAME COLUMN TO"""
            session.execute(alter_column_stmt)
            session.execute(revert_alter_column_stmt)

        self._case_template(session, rename_column_fn)

    def test_drop_column(self):
        """
        Tests that Scylla doesn't crash when there is concurrent LWT load in one
        thread and destructive DDL (drop and recreate column participating in lwt statements) in another.
        """

        session = self.prepare()
        alter_column_stmt = session.prepare("ALTER TABLE test DROP v")
        revert_alter_column_stmt = session.prepare("ALTER TABLE test ADD v int")

        def drop_create_column_fn(session):
            """DROP + CREATE COLUMN"""
            session.execute(alter_column_stmt)
            session.execute(revert_alter_column_stmt)

        self._case_template(session, drop_create_column_fn)

    def test_ks_netw_topology_set_nonexistent_dc(self):
        """
        Tests that Scylla doesn't crash when there is concurrent LWT load in one
        thread and destructive DDL (drop and recreate column participating in lwt statements) in another.
        """

        session = self.prepare({"datacenter1": 3})
        set_nonexistent_dc_stmt = session.prepare(
            """
            ALTER KEYSPACE ks
            WITH replication={'class': 'NetworkTopologyStrategy', 'dc2': '3'}"""
        )
        revert_set_nonexistent_dc_stmt = session.prepare(
            """
            ALTER KEYSPACE ks
            WITH replication={'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}"""
        )

        def set_nonexistent_dc_fn(session):
            """ALTER KEYSPACE WITH replication={nonexistent-dc}"""
            session.execute(set_nonexistent_dc_stmt)
            session.execute(revert_set_nonexistent_dc_stmt)

        self._case_template(session, set_nonexistent_dc_fn, tolerate_unavailable=True)

    def test_revoke_permissions(self):
        """
        Tests that Scylla doesn't crash when there is concurrent LWT load in one
        thread and destructive DDL (revoke and grant modify permissions on the test table) in another.
        """

        session = self.prepare(setup_auth=True)

        lwt_user = "jon"
        lwt_pass = "snow"

        # user default superuser for ddl statements
        ddl_user = "cassandra"
        ddl_pass = "cassandra"

        session.execute(
            f"""
            CREATE ROLE {lwt_user}
            WITH password='{lwt_pass}'
                AND superuser=false AND login=true"""
        )

        revoke_permissions_stmt = session.prepare(f"REVOKE ALL ON TABLE test FROM {lwt_user}")
        grant_permissions_stmt = session.prepare(f"GRANT ALL ON TABLE test TO {lwt_user}")

        def revoke_permissions_fn(session):
            """REVOKE + GRANT PERMISSION ON TABLE"""
            session.execute(revoke_permissions_stmt)
            session.execute(grant_permissions_stmt)

        self._case_template(session, revoke_permissions_fn, ddl_user=ddl_user, ddl_pass=ddl_pass, lwt_user=lwt_user, lwt_pass=lwt_pass)
