import logging
import os
import random
import re
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from pathlib import Path
from threading import Event
from typing import Any

import pytest
import requests
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodetoolError
from ccmlib.scylla_node import ScyllaNode
from packaging.version import Version

from dtest_class import Tester, create_cf, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools import commitlog
from tools.data import create_index, create_local_index
from tools.files import replace_in_file, safe_mkdtemp
from tools.marks import require, unmark
from tools.misc import ImmutableMapping
from tools.snapshots import (
    get_cf_snapshot_saved_dir,
    get_table_description,
    make_snapshot,
    restore_snapshot_with_refresh,
)
from tools.stress import assert_cs_success, format_cs_output

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class SnapshotOperations:
    """Base snapshot operations for parallel executing

    Have operations for creating keyspaces, tables,
    populating the tables, run operations in parallel
    """

    def verify_stderr_empty(self, results):
        """check that snapshot commands don't have stderr

        Arguments:
            results {list} -- list of list with future results
        """
        for future_result in results:
            for result in future_result:
                _stdout, stderr = result
                assert not stderr

    def init_cluster(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        return node, session

    def prepare_schemas_and_data(self, session, num_ks=1, num_cf=1, num_rows=1, column_length=10):
        for j in range(num_ks):
            create_ks(session, name=f"ks{j}", rf=1)

            for k in range(num_cf):
                create_cf(session, f"table_cf{k}", key_type="varchar")
                st = session.prepare(f"INSERT INTO table_cf{k} (key, c, v) VALUES (?, ?, ?)")
                execute_concurrent_with_args(session, st, map(lambda x, y, z: [str(x), str(y), str(z)], list(range(num_rows)), list(range(num_rows)), [f"{i}" * column_length for i in range(num_rows)]))

    def create_snapshot_for_all_keyspaces(self, node, start_process=None) -> list[tuple[str, str]]:
        if start_process:
            start_process.wait()

        stdout, stderr = node.nodetool(f"snapshot -t {uuid.uuid4()}")
        return [(stdout, stderr)]

    def create_snapshots_per_keyspace_table(self, node, start_process=None, num_ks=1, num_cf=1):
        if start_process:
            start_process.wait()
        results = []
        for i in range(num_ks):
            for j in range(num_cf):
                stdout, stderr = node.nodetool(f"snapshot ks{i}.table_cf{j} -t {uuid.uuid4()}")

            results.append((stdout, stderr))
        return results

    def clear_all_snapshots(self, node, start_process=None):
        """Clear all snapshots on nde

        :param node: Node were run clear snapshots command
        :type node: ScyllaNode
        :param start_process: Flag to start threads at same time, default None
        :type start_process: Barrier, optional
        """
        if start_process:
            start_process.wait()
        stdout, stderr = node.nodetool("clearsnapshot")
        return [(stdout, stderr)]

    def clear_snapshots_per_keyspace(self, node, start_process=None, num_ks=1):
        if start_process:
            start_process.wait()
        results = []
        for i in range(num_ks):
            stdout, stderr = node.nodetool(f"clearsnapshot ks{i}")
            results.append((stdout, stderr))
        return results

    def list_snapshots(self, node):
        stdout, stderr = node.nodetool("listsnapshots")
        return [(stdout, stderr)]


class SnapshotTester(Tester):
    """
    Object with utility functions to perform snapshot operations.
    """

    def init_cluster(self) -> tuple[ScyllaNode, Session]:
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        return node, session

    def create_tables(self, session, tables_number):
        tables = []
        for i in range(tables_number):
            name = f"cf{i}"
            create_cf(session=session, name=name, key_type="int", columns={"val": "text"})
            tables.append(name)
        return tables

    def insert_rows(self, session, start, end, cf=None):
        cfs = ["cf"] if cf == None else [cf] if isinstance(cf, str) else cf
        for cf_name in cfs:
            insert_statement = session.prepare(f"INSERT INTO ks.{cf_name} (key, val) VALUES (?, 'asdf')")
            args = [(r,) for r in range(start, end)]
            execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def validate_rows_count_in_all_tables(self, session, tables, expected_rows_count):
        for table in tables:
            rows = session.execute(f"SELECT count(*) from ks.{table}")
            assert rows.one()[0] == expected_rows_count

    def clear_snapshot_per_keyspace_per_table(self, ip, tag, ks, cf):
        requests.delete(f"http://{ip}:10000/storage_service/snapshots?tag={tag}&kn={ks}&cf={cf}")


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSnapshot(SnapshotTester):
    """
    Test snapshot operations.
    """

    @pytest.mark.dtest_debug
    def test_basic_snapshot_and_restore_with_refresh(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(tables_number=1, cf_param_name="")

    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_and_restore_with_refresh(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(tables_number=5, cf_param_name="-cf")

    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_using_column_family(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(tables_number=5, cf_param_name="--column-family")

    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_using_table(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(tables_number=5, cf_param_name="--table")

    def basic_snapshot_and_restore(self, tables_number, cf_param_name):
        """
        Base testing method:

        1. Create a keyspace
        2. Create a column family
        3. Insert 100 rows into the column family
        4. Take a snapshot
        5. Insert more rows after the snapshot
        6. Drop the keyspace, assure we have no data after the deletion
        7. Restore the snapshot
        8. Verify we have the same num of rows inserted prior to the snapshot.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        tables = self.create_tables(session=session, tables_number=tables_number)

        self.insert_rows(session, 0, 100, cf=tables)

        if cf_param_name:
            tables_for_snapshot = ",".join(table for table in tables)
            snapshot_dir = make_snapshot(node1, ks="ks", cf=tables_for_snapshot, cf_param_name=cf_param_name, name="basic")
        else:
            snapshot_dir = make_snapshot(node1, ks="ks", name="basic")

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200, cf=tables)
        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=200)

        # Drop the keyspace, make sure we have no data:
        session.execute("DROP KEYSPACE ks")
        shutil.rmtree(os.path.join(node1.get_path(), "data", "ks"))
        create_ks(session, "ks", 1)
        self.create_tables(session=session, tables_number=tables_number)
        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=0)

        # Restore data from snapshot:
        for table in tables:
            restore_snapshot_with_refresh(snapshot_dir, node1, "ks", table)

            node1.nodetool(f"refresh ks {table}")

        # clean up
        logger.info("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=100)

    def test_snapshot_for_2kc_and_cf_failure(self):
        """
        Base testing method:

        1. Create a 2 keyspaces
        2. Create a column family in every ks
        3. Insert 100 rows into the column family
        4. Try take a snapshot
        5. Verify error message: Only one keyspace allowed when specifying a column family
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session1 = self.patient_cql_connection(node1)

        create_ks(session, "ks", 1)
        create_ks(session1, "ks1", 1)
        tables = self.create_tables(session=session, tables_number=1)
        tables1 = self.create_tables(session=session1, tables_number=1)

        self.insert_rows(session, 0, 100, cf=tables)
        self.insert_rows(session1, 0, 100, cf=tables1)

        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=100)
        self.validate_rows_count_in_all_tables(session=session1, tables=tables1, expected_rows_count=100)

        tables_for_snapshot = ",".join(table for table in tables)

        expected_error = "Only one keyspace allowed when specifying a column family"
        self.ignore_log_patterns += [expected_error]

        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks="ks,ks1", cf=tables_for_snapshot, name="basic")

        assert expected_error in ne.value.stdout or expected_error in ne.value.stderr, ne.tb

    @pytest.mark.use_cassandra_stress
    @pytest.mark.high_memory
    def test_nodetool_snapshot_race_condition_with_compaction_under_stress(self):
        # Cover Issue #4051 https://github.com/scylladb/scylla/issues/4051
        def run_stress(node):
            logger.info("Start stress command")
            results = node.stress(["write", "duration=1m", "-mode", "cql3", "native", "-rate", "threads=100", "-pop", "seq=1..100000000", "-log", "interval=5"])
            logger.info("Stress results:\n" + format_cs_output(results))
            assert_cs_success(results)

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        executor = ThreadPoolExecutor(max_workers=1)
        stress_run_th = executor.submit(run_stress, node1)

        while not stress_run_th.done():
            results, errors = node1.nodetool(f"snapshot -t {uuid.uuid4()}")
            logger.info(results + errors)
            assert "failed: filesystem error: link failed: No such file or directory" not in " ".join(results + errors)
            assert not errors, "Some errors in creating snapshot: %s" % errors

        stress_run_th.result()

    @pytest.mark.use_cassandra_stress
    def test_nodetool_snapshot_race_condition_with_compaction_after_node_start(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.info("Run stress command")
        results = node1.stress(["write", "n=10000", "-rate", "threads=10"])
        logger.info("Stress results:\n" + format_cs_output(results))
        assert_cs_success(results)

        logger.info("Stoping node..")
        node1.stop()
        logger.info("Node has been stopped")

        logger.info("Starting node...")
        node1.start(wait_for_binary_proto=True)
        logger.info("Node has been started")

        logger.info("Create snapshot right after start")
        result, errors = node1.nodetool(f"snapshot -t {uuid.uuid4()}")
        logger.info(result + errors)
        assert "failed: filesystem error: link failed: No such file or directory" not in " ".join(result + errors)
        # Check that no other errors occured during snapshot command
        assert not errors, "Some errors in creating snapshot: %s" % errors

    @pytest.mark.use_cassandra_stress
    def test_nodetool_snapshot_during_major_compaction(self):
        def run_compaction(node):
            logger.info("Start compaction by command")
            node.compact()
            logger.info("Compaction done")

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        ks_name = "keyspace1"
        table_name = "standard1"

        logger.info("Run stress command")
        num_keys = 1000000 if self.cluster.scylla_mode != "debug" else 10000
        results = node1.stress(["write", f"n={num_keys}", "-rate", "threads=10", "-schema", "compaction(strategy=SizeTieredCompactionStrategy,enabled=false)"])
        logger.info("Stress results:\n" + format_cs_output(results))
        assert_cs_success(results)
        assert node1.is_live()

        executor = ThreadPoolExecutor(max_workers=1)
        compaction_thread = executor.submit(run_compaction, node1)

        logger.debug("Waiting for compaction to start")
        node1.watch_log_for(f"User initiated compaction started on behalf of {ks_name}.{table_name}")
        time.sleep(0.1)

        logger.info("Create snapshot right after start")
        result, errors = node1.nodetool("snapshot")
        logger.info(result + errors)
        assert "failed: filesystem error: link failed: No such file or directory" not in " ".join(result + errors)
        # Check that no other errors occured during snapshot command
        assert not errors, "Some errors in creating snapshot: %s" % errors

        logger.debug("Waiting for compaction to complete")
        compaction_thread.result()
        node1.wait_for_compactions()

    def test_cleaning_snapshot_created_by_ks(self):
        self.cleaning_snapshot_by_cf(snapshot_by_multiple_cf=False)

    def test_cleaning_snapshot_created_by_multiple_cf(self):
        self.cleaning_snapshot_by_cf(snapshot_by_multiple_cf=True)

    def cleaning_snapshot_by_cf(self, snapshot_by_multiple_cf):
        """Test deleting specific table from snapshot
        The test create a keyspace and two tables
        it take a snapshot, make sure that both tables are part of the backup
        It then delete on table and make sure that it is deleted but the other one is not.
        """

        def search_cf_in_snapshot(node, cf, tag):
            snapshot_dir = os.path.join(node.get_path(), "data", "ks")
            cf_id = next(s for s in os.listdir(snapshot_dir) if s.startswith(cf + "-"))

            if not os.path.exists(os.path.join(snapshot_dir, cf_id)):
                return False
            if not os.path.exists(os.path.join(snapshot_dir, cf_id, "snapshots", tag)):
                return False
            return True

        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);")
        session.execute("CREATE TABLE ks.cf1 ( key int PRIMARY KEY, val text);")

        self.insert_rows(session, 0, 100)
        self.insert_rows(session, 0, 100, "cf1")

        logger.info("all KSes and CFes are created")
        node.flush()
        if snapshot_by_multiple_cf:
            # Take snapshot by multiple tables
            node.nodetool("snapshot ks -cf cf,cf1 -t per_cf")
        else:
            # Take snapshot for all tables in keyspace
            node.nodetool("snapshot ks -t per_cf")

        assert search_cf_in_snapshot(node, "cf", "per_cf"), "cf {} is not found in snapshot".format("cf")
        assert search_cf_in_snapshot(node, "cf1", "per_cf"), "cf {} is not found in snapshot".format("cf1")
        logger.info("all KSes and CFes are part of the snapshot")

        self.clear_snapshot_per_keyspace_per_table(self.cluster.get_node_ip(1), "per_cf", "ks", "cf")

        assert not search_cf_in_snapshot(node, "cf", "per_cf"), "cf {} is found in snapshot but should be deleted".format("cf")
        assert search_cf_in_snapshot(node, "cf1", "per_cf"), "cf {} is not found in snapshot but should be remain".format("cf1")


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestParallelSnapshotOperations(Tester, SnapshotOperations):
    log = logging.getLogger()

    def test_parallel_creating_cleaning_one_ks(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=1, num_cf=1, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshots_per_keyspace_table(node, num_ks=1, num_cf=1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=1, num_cf=1))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=1))
            logger.info("Start processes")
            starter.set()
            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_for_10_ks_1_table_per_ks(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=1, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshots_per_keyspace_table(node, num_ks=10, num_cf=1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=10, num_cf=1))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=10))
            logger.info("Start processes")
            starter.set()

            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_for_10ks_and_10tables_and_clearallsnapshots(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshots_per_keyspace_table(node, num_ks=10, num_cf=10)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=10, num_cf=10))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=10))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            logger.info("Start processes")
            starter.set()

            for f in futures:
                results.append(f.result())
        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operation_create_clear_for_all_ks(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=10, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            logger.info("Start processes")
            starter.set()

            # run operations in parallel without syncinc start operations
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))

            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_create_clear_per_ks_and_all(self):
        """Test create snapshots per keyspae and clear all

        Run create snpahosts for each keyspaces and run clearing all snapshots
        in parallel
        """
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=10, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=10, num_cf=10))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=10))
            starter.set()
            # run operations in parallel without syncinc start operations
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))

            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_create_list_clear_for_all_ks(self):
        """Test create/list/clear for all keyspaces

        Verify that parallel operations for snapshots for all
        keyspaces run without errors
        """
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=30, num_cf=10, num_rows=10, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            futures.append(pool.submit(self.list_snapshots, node))
            starter.set()
            # run operations in parallel without syncinc start operations
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))
            futures.append(pool.submit(self.list_snapshots, node))

            for f in futures:
                results.append(f.result())
        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_with_large_data_size(self):
        """Test create/list/clear in parallel, which start not at same time

        Validate that if operations started at same time and
        another operations started in parallel, doesn't cause
        any crtitical issues.
        """
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=15, num_cf=15, num_rows=1000, column_length=1000)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=15, num_cf=15))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=15))
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.list_snapshots, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))
            starter.set()

            for f in futures:
                results.append(f.result())
        self.verify_stderr_empty(results)

    def test_snapshot_parallel_in_complex_mode_creating_listing_clearing(self):
        """Test varios snapshot operations in parallel

        Verify that snapshot operations (create, list, clear)
        which running in parallel at same time, are not crashed
        and not return stderr
        Additionally run periodically the listsnapsots and clearsnapshot
        operations

        this test has very long time to run
        """

        def monitor_lists_snapshots(node, kill):
            while not kill.is_set():
                result = self.list_snapshots(node)
                self.verify_stderr_empty([result])
                kill.wait(1)

        def clear_snapshots_periodically(node, kill):
            while not kill.is_set():
                result = self.clear_all_snapshots(node)
                self.verify_stderr_empty([result])
                kill.wait(2)

        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=15, num_cf=15, num_rows=1000, column_length=1000)
        logger.info("all KSes and CFes are created")

        kill = Event()
        futures = []
        results = []
        starter = Event()
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=15, num_cf=15))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            starter.set()
            monitor = pool.submit(monitor_lists_snapshots, node, kill)
            clearing_snapshots = pool.submit(clear_snapshots_periodically, node, kill)
            for f in futures:
                results.append(f.result())
            kill.set()
            monitor.result()
            clearing_snapshots.result()

        self.verify_stderr_empty(results)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSchemaFileInSnapshot(SnapshotTester):
    native_column_types_and_values = {
        "bigint": ("10000", "1", "2"),
        "boolean": ("true", "true", "false"),
        "blob": ("textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", "bigintAsBlob(1)"),
        "date": ("currentDate()", "currentDate()", "currentDate()"),
        "decimal": ("10.1", "11.1", "12.2"),
        "double": ("10.1000001", "11.111111", "22.22222"),
        "duration": ("89h1m48s", "11h11m11s", "22h22m22s"),
        "float": ("10.10001", "33.33", "44.44"),
        "inet": ("'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"),
        "int": ("100001", "1", "2"),
        "smallint": ("1", "1", "2"),
        "time": ("currentTime()", "currentTime()", "currentTime()"),
        "timestamp": ("currentTimestamp()", "currentTimestamp()", "currentTimestamp()"),
        "timeuuid": ("currentTimeUUID()", "currentTimeUUID()", "currentTimeUUID()"),
        "tinyint": ("1", "2", "5"),
        "uuid": ("uuid()", "uuid()", "uuid()"),
        "varint": ("1", "4", "5"),
        "text": ("'a'", "'A'", "'b'"),
        "varchar": ("'c'", "'d'", "'E'"),
        "ascii": ("'1'", "'c'", "'$'"),
    }

    def test_schema_file_created(self):
        """Check that schema.cql file is in snapshot"""
        node1, session = self.init_cluster()
        create_ks(session, "ks", 1)
        create_cf(session, name="cf", key_type="int", columns={"val": "text"})
        self.insert_rows(session, 0, 100)

        base_snapshot_dir = make_snapshot(node1, ks="ks", cf="cf", name="basic")
        schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "cf", "basic")

        table_desc = get_table_description(node1, "ks", "cf")

        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", 1)
        self.restore_table_by_schema_file(session, schema_file)
        restored_table_desc = get_table_description(node1, "ks", "cf")

        assert table_desc == restored_table_desc

    def test_schema_file_created_by_multiple_tables(self):
        """Check that schema.cql file is in snapshot"""
        node1, session = self.init_cluster()
        create_ks(session, "ks", 1)
        tables = self.create_tables(session=session, tables_number=5)
        self.insert_rows(session, 0, 100, cf=tables)

        tables_for_snapshot = ",".join(t for t in tables)
        base_snapshot_dir = make_snapshot(node1, ks="ks", cf=tables_for_snapshot, name="basic")
        schema_files = []
        tables_desc = []
        for table in tables:
            schema_files.append(self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", table, "basic"))
            tables_desc.append(get_table_description(node1, "ks", table))

        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", 1)

        for table, schema_file, desc in zip(tables, schema_files, tables_desc):
            self.restore_table_by_schema_file(session, schema_file)
            restored_table_desc = get_table_description(node1, "ks", table)

            assert desc == restored_table_desc

    def test_restoring_by_schema_file_with_refresh(self):
        self.create_restore_data_with_snapshot()

    def test_schema_file_contains_altering_table_changes(self):
        node1, session = self.init_cluster_and_create_schema("ks", "cf")

        self.insert_rows(session, 0, 100)
        base_snapshot_dir = make_snapshot(node1, ks="ks", cf="cf", name="basic")
        schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "cf", "basic")
        table_desc = get_table_description(node1, "ks", "cf")

        session.execute("ALTER TABLE ks.cf ADD val1 text")
        base_snapshot_dir = make_snapshot(node1, ks="ks", cf="cf", name="basic1")
        new_schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "cf", "basic1")
        altered_table_desc = get_table_description(node1, "ks", "cf")

        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", rf=1)

        self.restore_table_by_schema_file(session, new_schema_file)
        restored_altered_table_desc = get_table_description(node1, "ks", "cf")

        assert altered_table_desc == restored_altered_table_desc
        assert restored_altered_table_desc != table_desc
        assert self.read_schema_from_file(schema_file) != self.read_schema_from_file(new_schema_file)

    def test_restore_data_for_all_native_data_types_from_snapshot_with_refresh(self):
        self.create_and_restore_data_all_native_datatypes()

    def test_restore_data_from_snapshot_with_udt_with_refresh(self):
        self.create_and_restore_udt_from_snapshot()

    def test_restore_data_from_snapshot_with_frozen_udt_with_refresh(self):
        self.create_and_restore_udt_from_snapshot(use_frozen=True)

    def test_upper_case_of_table_name_is_saved(self):
        node1, session = self.init_cluster()
        create_ks(session, "ks", 1)
        session.execute('CREATE TABLE "UPPER_CASE_CF" ( "KEY" int PRIMARY KEY, "VAL" text);')
        session.execute('INSERT INTO "UPPER_CASE_CF" ("KEY", "VAL") VALUES (1, \'ASDFG\');')

        base_snapshot_dir = make_snapshot(node1, ks="ks", cf="UPPER_CASE_CF", name="basic")
        schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "UPPER_CASE_CF", "basic")
        table_desc = get_table_description(node1, "ks", '"UPPER_CASE_CF"')
        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", 1)
        self.restore_table_by_schema_file(session, schema_file)
        restored_table_desc = get_table_description(node1, "ks", '"UPPER_CASE_CF"')

        assert table_desc == restored_table_desc

    def test_upper_case_of_table_name_is_saved_mixed_case(self):
        node1, session = self.init_cluster()
        create_ks(session, "ks", 1)
        session.execute('CREATE TABLE "UPPER_CASE_CF" ( "KEY" int PRIMARY KEY, "VAL" text);')
        session.execute('INSERT INTO "UPPER_CASE_CF" ("KEY", "VAL") VALUES (1, \'ASDFG\');')
        create_cf(session=session, name="cf", key_type="int", columns={"val": "text"})
        self.insert_rows(session, 0, 10)

        base_snapshot_dir = make_snapshot(node1, ks="ks", cf="UPPER_CASE_CF,cf", name="basic")

        upper_schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "UPPER_CASE_CF", "basic")
        upper_table_desc = get_table_description(node1, "ks", '"UPPER_CASE_CF"')

        lower_schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "cf", "basic")
        lower_table_desc = get_table_description(node1, "ks", "cf")

        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", 1)
        self.restore_table_by_schema_file(session, upper_schema_file)
        upper_restored_table_desc = get_table_description(node1, "ks", '"UPPER_CASE_CF"')
        self.restore_table_by_schema_file(session, lower_schema_file)
        lower_restored_table_desc = get_table_description(node1, "ks", "cf")

        assert upper_table_desc == upper_restored_table_desc
        assert lower_table_desc == lower_restored_table_desc

    def create_restore_data_with_snapshot(self):
        node1, session = self.init_cluster_and_create_schema("ks", "cf")
        self.insert_rows(session, 0, 100)
        self.check_rows_number_in_table(session, "ks", "cf", 100)

        snapshot_dir = make_snapshot(node1, ks="ks", cf="cf", name="basic")

        # get table schema from schema file saved in snapshot
        schema_cql_file = self.get_schema_file_from_snapshot(snapshot_dir, "ks", "cf", "basic")
        table_schema = get_table_description(node1, "ks", "cf")

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        self.check_rows_number_in_table(session, "ks", "cf", 200)

        # Drop the keyspace, make sure we have no data:
        self.drop_keyspaces_and_clear_files(session, "ks", node1)

        # Restore keyspace
        create_ks(session, "ks", 1)

        self.restore_table_by_schema_file(session, schema_cql_file)

        restored_table_schema = get_table_description(node1, "ks", "cf")

        assert table_schema == restored_table_schema

        # check that data is not restored yet
        self.check_rows_number_in_table(session, "ks", "cf", 0)

        restore_snapshot_with_refresh(snapshot_dir, node1, "ks", "cf", "basic")
        node1.nodetool("refresh ks cf")

        # check data correctly restored and updated
        self.check_rows_number_in_table(session, "ks", "cf", 100)

    def create_and_restore_data_all_native_datatypes(self):
        node1, session = self.init_cluster()
        create_ks(session, "ks", rf=1)
        cl_types = list(self.native_column_types_and_values.keys())
        columns = ""
        for cl_type in cl_types:
            columns += f"cl_{cl_type} {cl_type}, "
        session.execute(f"CREATE TABLE native_types_table ({columns} PRIMARY KEY (cl_{cl_types[0]}))")

        for i in range(3):
            columns = [f"cl_{cl_type}" for cl_type in cl_types]
            values = [f"{values[i]}" for _, values in self.native_column_types_and_values.items()]

            session.execute(f"INSERT INTO native_types_table ({', '.join(columns)}) VALUES ({', '.join(values)})")

        self.check_rows_number_in_table(session, "ks", "native_types_table", 3)

        snapshots_dir = make_snapshot(node1, ks="ks", name="basic")
        table_desc = get_table_description(node1, "ks", "native_types_table")
        schema_file = self.get_schema_file_from_snapshot(snapshots_dir, "ks", "native_types_table", "basic")

        self.drop_keyspaces_and_clear_files(session, "ks", node1)

        create_ks(session, "ks", rf=1)

        self.restore_table_by_schema_file(session, schema_file)

        self.check_rows_number_in_table(session, "ks", "native_types_table", 0)

        restored_table_desc = get_table_description(node1, "ks", "native_types_table")
        restore_snapshot_with_refresh(snapshots_dir, node1, "ks", "native_types_table", "basic")

        self.check_rows_number_in_table(session, "ks", "native_types_table", 3)

        assert table_desc == restored_table_desc

    def create_and_restore_udt_from_snapshot(self, use_frozen=False):
        node1, session = self.init_cluster()

        create_ks(session, "ks", rf=1)

        cl_types = list(self.native_column_types_and_values.keys())
        columns = [f"cl_{cl_type} {cl_type}" for cl_type in cl_types]
        session.execute(f"CREATE TYPE all_native_types ({', '.join(columns)})")
        udt_type = "frozen<all_native_types>" if use_frozen else "all_native_types"
        session.execute(f"CREATE TABLE table_with_udt (cl_{cl_types[0]} {cl_types[0]}, data {udt_type}, PRIMARY KEY (cl_{cl_types[0]}))")

        for i in range(3):
            columns = [f"cl_{cl_type}" for cl_type in cl_types]
            udt_values = [f"cl_{cl_type}: {values[i]}" for cl_type, values in self.native_column_types_and_values.items()]
            session.execute(
                f"INSERT INTO table_with_udt (cl_{cl_types[0]}, data) VALUES ({self.native_column_types_and_values[cl_types[0]][i]}, \
                            {{{', '.join(udt_values)}}})"
            )

        self.check_rows_number_in_table(session, "ks", "table_with_udt", 3)

        snapshots_dir = make_snapshot(node1, ks="ks", name="basic")
        table_desc = get_table_description(node1, "ks", "table_with_udt")
        schema_file = self.get_schema_file_from_snapshot(snapshots_dir, "ks", "table_with_udt", "basic")

        self.drop_keyspaces_and_clear_files(session, "ks", node1)

        create_ks(session, "ks", rf=1)
        columns = [f"cl_{cl_type} {cl_type}" for cl_type in cl_types]
        session.execute(f"CREATE TYPE all_native_types ({', '.join(columns)})")

        self.restore_table_by_schema_file(session, schema_file)

        self.check_rows_number_in_table(session, "ks", "table_with_udt", 0)

        restored_table_desc = get_table_description(node1, "ks", "table_with_udt")
        restore_snapshot_with_refresh(snapshots_dir, node1, "ks", "table_with_udt", "basic")

        self.check_rows_number_in_table(session, "ks", "table_with_udt", 3)

        assert table_desc == restored_table_desc

    def drop_keyspaces_and_clear_files(self, session, ks, node):
        session.execute(f"DROP KEYSPACE {ks}")
        node.rmtree(os.path.join(node.get_path(), "data", ks))

    def restore_table_by_schema_file(self, session, schema_file):
        schema = self.read_schema_from_file(schema_file)
        session.execute(schema)

    def read_schema_from_file(self, schema_file):
        with open(schema_file) as fp:
            content = fp.read()
        return content

    def get_mv_description(self, node, ks, mv):
        mv_desc = node.run_cqlsh(f"DESCRIBE MATERIALIZED VIEW {ks}.{mv}", return_output=True)
        return mv_desc[0]

    def get_index_description(self, node, ks, index):
        index_desc = node.run_cqlsh(f"describe index {ks}.{index}", return_output=True)
        return index_desc[0]

    def get_schema_file_from_snapshot(self, base_snapshot_dir: str, ks: str, cf: str, name: str | None = None) -> str:
        snapshot_dir = get_cf_snapshot_saved_dir(base_snapshot_dir, ks, cf, name)
        schema_file = os.path.join(snapshot_dir, "schema.cql")
        assert os.path.exists(schema_file)
        return schema_file

    def check_schema_file(self, schema_file: str, *attributes: list[str]) -> None:
        """Check that schema file is exists and contains provided attributes

        Validate that schema file was created and is in snapshot dir.
        Check that all attributes provided in attributes list are present
        in schema.cql file
        :param schema_file: path to schema.cql file
        :type schema_file: str
        :param *attributes: list of attributes to check in schema
        :type *attributes: list[str]
        """
        assert "schema.cql" in schema_file
        with open(schema_file) as fp:
            content = fp.read()

        assert content
        for attribute in attributes:
            assert attribute in content

    def check_rows_number_in_table(self, session, ks, cf, number):
        rows = session.execute(f"SELECT count(*) from {ks}.{cf}")
        assert rows.one()[0] == number

    def check_rows_number_in_index(self, session, ks, cf, number, index_column, value):  # noqa: PLR0913
        rows = session.execute(f"SELECT count(*) from {ks}.{cf} WHERE {index_column} = {value}")
        assert rows.one()[0] == number

    def init_cluster_and_create_schema(self, ks, cf, mv=False, si=False, lsi=False):
        node, session = self.init_cluster()
        create_ks(session, ks, 1)
        create_cf(session, name=cf, key_type="int", columns={"val": "text"})
        if mv:
            session.execute(
                f"CREATE MATERIALIZED VIEW {cf}_mv AS SELECT val, key FROM ks.cf \
                              WHERE val IS NOT NULL PRIMARY KEY (val, key)"
            )
        if si:
            create_index(session, cf, "val", f"{cf}_ind")
        if lsi:
            create_local_index(session, cf, "key", "val", index_name="cf_val")

        return node, session


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSnapshotOptions(SnapshotTester):
    SNAP_OPS = SnapshotOperations
    NODE_COUNT = 1
    KEYSPACE_COUNT = 3
    TABLE_COUNT = 2
    ROWS_PER_TABLE_COUNT = 1
    COLUMN_LENGTH = 10

    @cache
    def prepare(self):
        self.node, self.session = self.init_cluster()
        self.system_keyspaces = self._get_system_keyspace_names()
        self.keyspaces = [f"ks{i}" for i in range(self.KEYSPACE_COUNT)]
        self.table_names = [f"table_cf{j}" for j in range(self.TABLE_COUNT)]
        self.SNAP_OPS.prepare_schemas_and_data(self, session=self.session, num_ks=self.KEYSPACE_COUNT, num_cf=self.TABLE_COUNT, num_rows=self.ROWS_PER_TABLE_COUNT, column_length=self.COLUMN_LENGTH)

    def test_snapshot_defaults_to_all_keyspaces(self):
        """
        Assert that using the nodetool snapshot command without
        specifying any keyspaces or tables defaults to a making
        a snapshot of all the keyspaces.
        1. Create a snapshot using 'nodetool snapshot'.
        2. Extract keyspace names from snapshot directory paths.
        3. Assert that extracted names contain all of the system keyspaces
        plus created keyspaces and that the number of names equals the sum
        of system_keyspace_count + created_keyspaces_count.
        """
        self.prepare()
        keyspaces = self.keyspaces + self.system_keyspaces
        keyspace_dir_names = self.base_case_make_snapshot_and_extract_ks_cf_names(make_snapshot_kwargs={"node": self.node})[0]

        assert set(keyspace_dir_names) == set(keyspaces)

    def test_snapshot_of_specified_keyspaces_only(self):
        """
        Assert that using nodetool snapshot command with specifying
        keyspaces creates a snapshot of the specified keyspaces only,
        i.e. no other keyspaces are present in the snapshot
        directory.
        1. Create a snapshot using 'nodetool snapshot <keyspaces>'.
        Use one keyspace less than the full created keyspaces list.
        2. Extract keyspace names from snapshot directory paths.
        3. Assert that extracted names contain only the keyspace
        names provided to the 'nodetool snapshot <keyspcaes>'
        command.
        """
        self.prepare()
        keyspaces_to_snap = self._get_random_keyspaces_to_snap()
        keyspace_dir_names = self.base_case_make_snapshot_and_extract_ks_cf_names(make_snapshot_kwargs={"node": self.node, "ks": ",".join(keyspaces_to_snap)})[0]

        assert set(keyspace_dir_names) == set(keyspaces_to_snap)

    def test_snapshot_of_specified_keyspaces_only_with_kc_list_option(self):
        """
        Assert that using nodetool snapshot command with specifying
        keyspaces  with the '-kc' option creates a snapshot of the
        specified keyspaces only, i.e. no other keyspaces are present
        in the snapshot directory.
        1. Create a snapshot using 'nodetool
        snapshot -kc <keyspaces>'.
        Use one keyspace less than the full created keyspaces list.
        2. Extract keyspace names from snapshot directory paths.
        3. Assert that extracted names contain only the keyspace
        names provided to the 'nodetool snapshot <keyspcaes>'
        command.
        """
        self.prepare()
        keyspaces_to_snap = self._get_random_keyspaces_to_snap()
        keyspace_dir_names = self.base_case_make_snapshot_and_extract_ks_cf_names(make_snapshot_kwargs={"node": self.node, "additional_options": [f"-kc {','.join(keyspaces_to_snap)}"]})[0]

        assert set(keyspace_dir_names) == set(keyspaces_to_snap)

    def test_snapshot_tagging(self):
        """
        Assert that when using the '-t' option for specifying a
        snapshot tag, the snapshot is named according to the
        parameter provided for that option.
        1. Execute the nodetool snapshot command with the '-t' option.
        2. Check the stdout for the name of the snapshot.
        """
        self.prepare()
        tag = "charybdis"

        assert f"Snapshot directory: {tag}" in self.node.nodetool(cmd=f"snapshot -t {tag}")[0]

    def test_snapshot_skip_flush(self):
        """
        Assert that using the '-sf'/'--skip-flush' forces nodetool
        to make a snapshot of the data without flushing memtables.
        1. Insert a few rows of data to populate the memtable.
        2. Query 'nodetool tablestats' for memtable data size and
        number of memtable switches.
        3. Use 'nodetool snapshot --skip-flush' to trigger the
        snapshot without flushing the memtable.
        4. Query 'nodetool tablestats' again for memtable data size
        and number of memtable switches.
        5. Assert that memtable data size is greater or equal to
        before the snapshot.
        6. Assert that the number of memtable switches is equal to
        the number before the snapshot.
        """
        self.prepare()
        table = self.table_names[0]
        ks = self.keyspaces[0]
        self._insert_rows_into_ks_cf(insert_row_count=2, ks=ks, cf=table)
        stdout_pre, _stderr_pre = self._get_tabestats_for_table(keyspace=ks, table=table)

        memtable_data_size_pre, memtable_switch_count_pre = self._get_memtable_stats_from_tablestats(stdout_pre)
        logger.info("memtable_data_size_pre=%s memtable_switch_count_pre=%s", memtable_data_size_pre, memtable_switch_count_pre)
        self.base_case_make_snapshot_and_extract_ks_cf_names(make_snapshot_kwargs={"node": self.node, "ks": ks, "cf": table, "additional_options": ["--skip-flush"]})

        stdout_post, _stderr_post = self._get_tabestats_for_table(keyspace=ks, table=table)
        memtable_data_size_post, memtable_switch_count_post = self._get_memtable_stats_from_tablestats(stdout_post)
        logger.info("memtable_data_size_post=%s memtable_switch_count_post=%s", memtable_data_size_post, memtable_switch_count_post)

        assert memtable_data_size_post >= memtable_data_size_pre, (
            f"Expected memtable data size after the skip-flush snapshot to be greater or equal to size before snapshot, but was not.\nSize pre: {memtable_data_size_pre}\nSize post: {memtable_data_size_post}"
        )
        assert memtable_switch_count_pre == memtable_switch_count_post, (
            f"Expected memtable switch count after the skip-flush snapshot to be equal to the switch count before the snapshot, but was not.\nSwitch count pre: {memtable_switch_count_pre}\nSwitch count post: {memtable_switch_count_post}"
        )

    def base_case_make_snapshot_and_extract_ks_cf_names(self, make_snapshot_kwargs: dict[str, Any]) -> tuple[list[str], list[str], str]:
        """
        Base case collecting common steps for other TestSnapshotOptions
        test cases.
        1. Make a snapshot with the given keyword args.
        2. Extract keyspace and table dir names from the temporary
        dir housing the snapshot dir copy.
        3. Return a tuple of keyspace dir names list, table subdir
        names list and the snapshot root dir name.
        """
        snapshot_root_dir = make_snapshot(**make_snapshot_kwargs)
        snapshot_dir_paths = self._get_snapshot_dir_paths(snapshot_root_dir)
        keyspace_dir_names = self._parse_names_from_paths(snapshot_dir_paths["keyspace_dirs"])
        table_subdirs_names = self._parse_names_from_paths(snapshot_dir_paths["table_subdirs"])

        return keyspace_dir_names, table_subdirs_names, snapshot_root_dir

    def _insert_rows_into_ks_cf(self, insert_row_count: int = 1, ks: str = "ks0", cf: str = "table_cf0"):
        insert_statement = self.session.prepare(f"INSERT INTO {ks}.{cf} (key, c, v) VALUES (?, 'sometext', 'someothertext')")
        logger.info("Inserting %d rows into keyspace %s, column family: %s", insert_row_count, ks, cf)
        for i in range(self.ROWS_PER_TABLE_COUNT, self.ROWS_PER_TABLE_COUNT + insert_row_count):
            args = [(str(i),)]
            execute_concurrent_with_args(self.session, insert_statement, args, concurrency=20)

    def _get_random_keyspaces_to_snap(self) -> list[str]:
        keyspace_to_omit = random.choice(self.keyspaces)
        keyspaces = self.keyspaces.copy()
        keyspaces.remove(keyspace_to_omit)

        return keyspaces

    def _get_tabestats_for_table(self, keyspace: str, table: str) -> tuple[str, str]:
        nodetool_cmd = f"tablestats {keyspace}.{table}"
        stdout, stderr = self.node.nodetool(nodetool_cmd)

        return stdout, stderr

    def _get_system_keyspace_names(self):
        query = "select keyspace_name from system_schema.keyspaces"
        keyspace_list = [item.keyspace_name for item in self.session.execute(query=query).all()]

        if "consistent-topology-changes" in self.scylla_features:
            # with consistent topology auth-v2 gets enabled and it stores
            # data in system_auth_v2 keyspace, but it leaves empty legacy system_auth
            # for compatibility with cqlsh, as nodetool does snapshots on per table
            # basis empty keyspace causes that no snapshot is generated which would
            # fail our snapshot test later so we pretend that system_auth doesn't exists
            keyspace_list.remove("system_auth")

        return keyspace_list

    @staticmethod
    def _get_memtable_stats_from_tablestats(tablestats_stdout: str) -> tuple[int, int]:
        memtable_data_size_pattern = re.compile(r"(?:Memtable data size:\s*)(\d+)")
        memtable_switch_count_pattern = re.compile(r"(?:Memtable switch count:\s*)(\d+)")
        memtable_data_szie = int(memtable_data_size_pattern.search(tablestats_stdout).group(1))
        memtable_switch_count = int(memtable_switch_count_pattern.search(tablestats_stdout).group(1))

        return memtable_data_szie, memtable_switch_count

    @staticmethod
    def _get_snapshot_dir_paths(snapshot_root_path: str) -> dict[str, list[Path]]:
        root = Path(snapshot_root_path)
        keyspace_dirs = [path for path in root.glob("*") if path.is_dir]
        table_subdirs = [path for subdir in keyspace_dirs for path in subdir.glob("*") if path.is_dir]

        return {"keyspace_dirs": keyspace_dirs, "table_subdirs": table_subdirs}

    @staticmethod
    def _parse_names_from_paths(path_list: list[Path]) -> list[str]:
        return [path.stem for path in path_list]
