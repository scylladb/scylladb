#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import glob
import logging
import os
import stat
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra.cluster import NoHostAvailable, Session
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from tools.assertions import (
    assert_all,
    assert_almost_equal,
    assert_lists_equal_ignoring_order,
    assert_one,
    assert_row_count,
    assert_row_count_in_select_less,
)
from tools.data import insert_c1c2, rows_to_list
from tools.files import corrupt_file


logger = logging.getLogger(__name__)


@pytest.mark.single_node
class TestCommitLog(Tester):
    """CommitLog Tests"""

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns += [
            r"commitlog - Exception in segment reservation: storage_io_error \(Storage I/O error: 13: filesystem error: open failed",
            "Shutting down communications due to I/O errors until operator intervention",
            r"commitlog - Failed to (flush|persist) commits to disk.*storage_io_error \(Storage I/O error: 28: No space left on device\)",
            r"storage_proxy - .*\(Could not write mutation .* to commitlog\): storage_io_error \(Storage I/O error: 28: No space left on device\)",
        ]

    @pytest.fixture(autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.set_configuration_options({"start_rpc": "true"})
        fixture_dtest_setup.cluster.populate(1)
        [self.node1] = fixture_dtest_setup.cluster.nodelist()

    def prepare(self, configuration=None, create_test_keyspace=True, **kwargs):
        if configuration is None:
            configuration = {}
        conf = {"commitlog_sync_period_in_ms": 1000}

        conf.update(configuration)
        self.cluster.set_configuration_options(values=conf, **kwargs)
        self.cluster.start()
        unknown_options = self.cluster.nodelist()[0].grep_log("config - Unknown option")
        if unknown_options:
            pytest.fail(f"Unknown option found! Please check the test! {unknown_options}")
        self.session1 = self.patient_cql_connection(self.node1)
        if create_test_keyspace:
            self.session1.execute("DROP KEYSPACE IF EXISTS ks;")
            create_ks(self.session1, "ks", 1)
            self.session1.execute("DROP TABLE IF EXISTS test;")
            query = """
              CREATE TABLE test (
                key int primary key,
                col1 int
              )
            """
            self.session1.execute(query)

    def _get_commitlog_path(self):
        """Returns the commitlog path"""

        return os.path.join(self.node1.get_path(), "commitlog")

    def _get_commitlog_files(self):
        """Returns the commitlog files in the directory"""

        path = self._get_commitlog_path()
        return glob.glob(path + "/CommitLog-*.log")

    def _get_commitlog_size(self, include_stdout=False, allow_errors=False):
        """Returns the commitlog directory size in MB"""

        path = self._get_commitlog_path()
        files = glob.glob(f"{path}/*CommitLog-*.log")
        if not files:
            return 0, "" if include_stdout else 0
        cmd_args = ["du", "-m"]
        cmd_args.extend(files)
        p = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.returncode

        # allow_error is needed for running du
        # while the node is up and running
        # and commitlog files may be created and deleted
        # while du runs.
        if exit_status:
            if allow_errors:
                return -1, stderr.decode()
            pytest.fail("du exited with a non-zero status: %d" % exit_status + "\n%s" % stderr.decode())
        # du will print a line for file
        size = 0
        for l in stdout.decode().split("\n"):
            if not l:
                continue
            a = l.split()
            if len(a) == 2:
                size += int(a[0])
            else:
                logger.warning(f"Unrecognized du output line: {l}")
        return size, stdout.decode() if include_stdout else size

    def _segment_size_test(self, segment_size_in_mb, compressed=False):
        """Execute a basic commitlog test and validate the commitlog files"""

        conf = {"commitlog_segment_size_in_mb": segment_size_in_mb}
        if compressed:
            conf["commitlog_compression"] = [{"class_name": "LZ4Compressor"}]
        conf["memtable_heap_space_in_mb"] = 512
        self.prepare(configuration=conf, create_test_keyspace=False)

        segment_size = segment_size_in_mb * 1024 * 1024
        self.node1.stress(["write", "n=150000", "-rate", "threads=25"])
        time.sleep(1)

        commitlogs = self._get_commitlog_files()
        assert len(commitlogs) > 0, "No commit log files were created"

        # the most recently-written segment of the commitlog may be smaller
        # than the expected size, so we allow exactly one segment to be smaller
        smaller_found = False
        for i, f in enumerate(commitlogs):
            size = os.path.getsize(f)
            size_in_mb = int(size / 1024 / 1024)
            logger.debug(f"segment file {f} {size_in_mb}; smaller already found: {smaller_found}")
            if size_in_mb < 1 or size < (segment_size * 0.1):
                continue  # commitlog not yet used

            try:
                if compressed:
                    # if compression is used, we assume there will be at most a 50% compression ratio
                    assert size <= segment_size, f"expect {size} < {segment_size}"
                    assert size > segment_size / 2, f"expect {size} > {segment_size / 2}"
                else:
                    # if no compression is used, the size will be close to what we expect
                    assert_almost_equal(size, segment_size, error=0.05)
            except AssertionError as e:
                #  the last segment may be smaller
                if not smaller_found:
                    assert size <= segment_size, f"expects size <= segment_size, actual are {size} {segment_size}"
                    smaller_found = True
                else:
                    raise e

    def test_commitlog_replay_on_startup(self):
        """Test commit log replay"""
        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start(wait_for_binary_proto=True)

        logger.debug("Insert data")
        session = self.patient_cql_connection(node1)
        create_ks(session, "Test", 1)
        session.execute(
            """
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """
        )
        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('gandalf', 'p@$$', 'male', 'WA', 1955);")

        logger.debug("Verify data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        assert rows_to_list(res) == [["gandalf", 1955, "male", "p@$$", "WA"]]

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Verify commitlog was written before abrupt stop")
        commitlog_files = self._get_commitlog_files()
        assert len(commitlog_files) > 0, f"expecting positive, len(commitlog_files)={len(commitlog_files)}"

        logger.debug("Verify no SSTables were flushed before abrupt stop")
        data_dir = os.path.join(node1.get_path(), "data")
        cf_id = next(s for s in os.listdir(os.path.join(data_dir, "test")) if s.startswith("users"))
        cf_data_dir = glob.glob(f"{data_dir}/test/{cf_id}")[0]
        cf_data_dir_files = os.listdir(cf_data_dir)
        for special_dir in ["backups", "upload", "staging"]:
            if special_dir in cf_data_dir_files:
                cf_data_dir_files.remove(special_dir)
        assert len(cf_data_dir_files) == 0, f"expecting 0, len(cf_data_dir_files)={len(cf_data_dir_files)}"

        logger.debug("Verify commit log was replayed on startup")
        node1.start(wait_for_binary_proto=False)
        assert node1.is_running(), "node is not running"
        node1.watch_log_for("Log replay complete")
        # Here we verify there was more than 0 replayed mutations
        zero_replays = node1.grep_log(" 0 replayed mutations", filter_expr="DEBUG")
        assert len(zero_replays) == 0, f"expect 0, len(zero_replays)={len(zero_replays)}"

        logger.debug("Make query and ensure data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        assert_lists_equal_ignoring_order(rows_to_list(res), [["gandalf", 1955, "male", "p@$$", "WA"]])

    def test_commitlog_replay_with_alter_table(self):
        """
        Test commit log replay with alter table
        The goal of the test is to verify that commitlog replay works correctly even if the commitlog contains
        mutations written using old versions of the schema.
        Based on test_commitlog_replay_on_startup
        """

        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start(wait_for_binary_proto=True)

        logger.debug("Create table")
        session = self.patient_cql_connection(node1)
        create_ks(session, "Test", 1)
        session.execute(
            """
            CREATE TABLE cf (
                pk1 int,
                ck1 int,
                r2 int,
                r3 text,
                r5 set<int>,
                PRIMARY KEY(pk1, ck1)
            );
        """
        )

        logger.debug("Insert some data")
        n_partitions = 5
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r2, r3, r5) VALUES(%d, 9, 8, 'seven', {6, 5});" % (key))
            session.execute("INSERT INTO Test.cf (pk1, ck1, r3) VALUES(%d, 8, 'eight');" % (key))

        logger.debug("Flush")
        self.cluster.flush()

        logger.debug("Insert more data and alter table")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r2, r3, r5) VALUES(%d, 0, 1, 'two', {3, 4});" % (key))
        session.execute("ALTER TABLE Test.cf ADD r1 int;")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r1, r2, r3, r5) VALUES(%d, 1, 2, 3, 'four', {5, 6, 7});" % (key))
        session.execute("ALTER TABLE Test.cf DROP r2;")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r1, r3) VALUES(%d, 2, 3, 'four');" % (key))
        session.execute("ALTER TABLE Test.cf DROP r5;")
        session.execute("ALTER TABLE Test.cf ADD r2 varint;")
        session.execute("ALTER TABLE Test.cf ADD r4 int;")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r2, r4) VALUES(%d, 0, 99, 999);" % (key))

        logger.debug("Verify data is present")
        session = self.patient_cql_connection(node1)
        for key in range(n_partitions):
            assert_all(
                session,
                f"SELECT * FROM Test.cf where pk1 = {key}",
                [
                    [key, 0, None, 99, "two", 999],
                    [key, 1, 2, None, "four", None],
                    [key, 2, 3, None, "four", None],
                    [key, 8, None, None, "eight", None],
                    [key, 9, None, None, "seven", None],
                ],
                ignore_order=True,
            )
        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Verify commitlog was written before abrupt stop")
        commitlog_files = self._get_commitlog_files()
        assert len(commitlog_files) > 0, f"expect >0, len(commitlog_files)={len(commitlog_files)}"

        logger.debug("Verify commitlog was replayed on startup")
        node1.start(wait_for_binary_proto=False)
        node1.watch_log_for("Log replay complete")
        replays = node1.grep_log(r" (\d+) replayed mutations", filter_expr="DEBUG")
        assert len(replays) > 0, f"expect >0, len(replays)={len(replays)}"
        replayed_mutations = 0
        for line, m in replays:
            replayed_mutations += int(m.group(1))
        assert replayed_mutations >= 4, f"expect >=4, replayed_mutations={replayed_mutations}"

        logger.debug("Make query and ensure data is present")
        session = self.patient_cql_connection(node1)
        for key in range(n_partitions):
            assert_all(
                session,
                f"SELECT * FROM Test.cf where pk1 = {key}",
                [
                    [key, 0, None, 99, "two", 999],
                    [key, 1, 2, None, "four", None],
                    [key, 2, 3, None, "four", None],
                    [key, 8, None, None, "eight", None],
                    [key, 9, None, None, "seven", None],
                ],
                ignore_order=True,
            )

    def test_default_segment_size(self):
        """Test default commitlog_segment_size_in_mb (32MB)"""

        self._segment_size_test(32)

    def test_small_segment_size(self):
        """Test a small commitlog_segment_size_in_mb (5MB)"""

        self._segment_size_test(5)

    def test_default_compressed_segment_size(self):
        """Test default compressed commitlog_segment_size_in_mb (32MB)"""
        # Scylla: Unknown option commitlog_compression
        self._segment_size_test(32, compressed=True)

    def test_small_compressed_segment_size(self):
        """Test a small compressed commitlog_segment_size_in_mb (5MB)"""
        # Scylla: Unknown option commitlog_compression
        self._segment_size_test(5, compressed=True)

    def prepare_cluster_with_ks_cf(self, jvm_args=None):
        node1 = self.node1
        jvm_args = jvm_args or []
        if not "--commitlog-sync-period-in-ms" in jvm_args:
            commit_log_sync_period = 10000
            if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
                commit_log_sync_period *= 3
            jvm_args.extend(["--commitlog-sync-period-in-ms", str(commit_log_sync_period)])
        self.cluster.start(jvm_args=jvm_args)

        logger.debug("Create table")
        session = self.patient_cql_connection(node1)
        create_ks(session, "Test", 1)
        session.execute(
            """
                    CREATE TABLE cf (
                        pk1 INT,
                        ck1 INT,
                        v1 int,
                        PRIMARY KEY(pk1, ck1)
                    );
                """
        )
        return session, node1

    def test_periodic_commitlog(self):
        """
        Test periodic mode of commitlog flushing
        'periodic' mode is where all commitlog writes are ready the moment they are stored in
        a memory buffer and the memory buffer is flushed to a storage periodically.
        """

        session, node1 = self.prepare_cluster_with_ks_cf()
        logger.debug("Insert 100 rows")
        for i in range(100):
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({i}, {i}, {i})")

        assert_row_count(session=session, table_name="Test.cf", expected=100)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        assert_row_count_in_select_less(session=session, query="select * from Test.cf", max_rows_expected=100)

    def _test_total_space_limit_of_commitlog(  # noqa: PLR0915
        self,
        commitlog_segment_size_in_mb,
        commitlog_total_space_in_mb,
    ):
        """
        `commitlog_reuse_segments` is enabled by default, reusing commitlog
        segments without deleting and recreating will improve the performance.
        `commitlog_segment_size_in_mb` is the size limit of single commitlog.

        The space usage of commitlog is limited by `commitlog_total_space_in_mb`,
        after flusing segment buffers or sgement is recycled, nothing is filled
        but the diskspace isn't released.

        This case testes with different size of `commitlog_total_space_in_mb`
        and `commitlog_segment_size_in_mb`, fills data by cs workload for
        reaching the `commitlog_disk_usage_threshold`, then flushing will be
        triggered automatically.

        When the cs workload writes data, commitlog disk usage will be limited
        by `commitlog_total_space_in_mb`. Currently scylla allows to have one
        more segment, so the real limit is increased to be a multiple of
        segment size.

        Related Scylla PR: https://github.com/scylladb/scylla/pull/6368
        """
        node1 = self.node1
        log_level = "trace" if commitlog_total_space_in_mb == -1 else "debug"

        total_space_limit = commitlog_total_space_in_mb

        if commitlog_segment_size_in_mb == -1:
            commitlog_segment_size_in_mb = 32  # default

        logger.debug(f"commitlog_segment_size_in_mb={commitlog_segment_size_in_mb}")
        logger.debug(f"commitlog_total_space_in_mb={commitlog_total_space_in_mb}")
        logger.debug(f"total_space_limit={total_space_limit}")

        # Calculate a reasonably low commitlog_disk_usage_threshold, flushing will be triggered
        # at this point. We want something that is within reachable range
        commitlog_disk_usage_threshold = int(total_space_limit / 2)

        opts = {"commitlog_segment_size_in_mb": commitlog_segment_size_in_mb, "commitlog_total_space_in_mb": commitlog_total_space_in_mb, "commitlog_reuse_segments": True, "commitlog_use_hard_size_limit": True}

        if total_space_limit != -1:
            opts.update({"commitlog_flush_threshold_in_mb": commitlog_disk_usage_threshold})

        # By default periodic commitlog_sync mode will be used, so we have chance
        # to accumulate more commitlogs.
        commit_log_sync_period = 10000
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            commit_log_sync_period *= 3
        node1.set_configuration_options(values=opts)

        logger.debug(f"Commitlog size before start: {self._get_commitlog_size()}M")
        memory = max(0, commitlog_total_space_in_mb) + 512
        logger.debug(f"Start cluster with '--smp 1' '--memory {memory}M' ...")
        args = ["--smp", "1", "--memory", f"{memory}M", "--logger-log-level", f"commitlog={log_level}"]
        self.cluster.start(jvm_args=args, wait_for_binary_proto=True)

        if commitlog_total_space_in_mb == -1:
            matches = node1.grep_log(r"Commitlog .* maximum disk size: (\d+) MB / cpu \(\d+ cpus\)")
            total_space_limit = int(matches[0][1].group(1))
            commitlog_disk_usage_threshold = int(total_space_limit / 2)
            logger.debug(f"total_space_limit={total_space_limit}")
            logger.debug(f"commitlog_disk_usage_threshold={commitlog_disk_usage_threshold}")

        logger.debug("Create test keyspace and table")
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})

        # Fill data by CS workload, a set of commitlogs will be generated for testing the space limit
        cs_n = max(int(total_space_limit * 200), 20000)
        cs_cmd = ["write", f"n={cs_n}", "-rate", "threads=25", "-col", "size=FIXED(1000)", "-log", "interval=100"]
        logger.debug(f"Starting CS workload: {cs_cmd}")
        executor = ThreadPoolExecutor(max_workers=1)
        start = time.time()
        cs_task = executor.submit(lambda: node1.stress(cs_cmd))

        # Insert a few data by insert_c1c2(), the data will be verifed in the end
        total_size = 0
        unit_size = commitlog_segment_size_in_mb
        reach_threshold_cases = []
        # Scylla allows to create one more commitlog file out of the space limit
        # and commitlog segments may be go over commitlog_segment_size_in_mb in 1MB as well.
        actual_space_limit = (total_space_limit // commitlog_segment_size_in_mb + 1) * (commitlog_segment_size_in_mb + 1)

        def check_commitlog_size(allow_errors: bool):
            dir_size, stdout = self._get_commitlog_size(include_stdout=True, allow_errors=allow_errors)
            if dir_size > actual_space_limit and not allow_errors:
                logger.debug(f"Commitlog file sizes in MB:\n{stdout}")
                assert dir_size <= actual_space_limit, f"Out of total space limit\n"
            return dir_size

        while not cs_task.done() and not reach_threshold_cases:
            # Insert a few data
            insert_c1c2(session, keys=range(total_size, total_size + unit_size))
            total_size += unit_size
            dir_size = check_commitlog_size(allow_errors=True)
            if dir_size > commitlog_disk_usage_threshold:
                reach_threshold_cases.append(dir_size)
            if len(reach_threshold_cases) > 0:
                logger.debug(
                    f"Current commitlog size: {dir_size}M "
                    f"({round(100 * dir_size / total_space_limit, 2)})%, "
                    f"Well-used cases: {len(reach_threshold_cases)} / 5, "
                    f"Time passed: {round(time.time() - start, 2)}s, "
                    f"commitlog_disk_usage_threshold: {commitlog_disk_usage_threshold}M"
                )
            # Have enough commitlog to trigger flushing
            if len(reach_threshold_cases) >= 5:
                logger.debug("Waiting the cs workload to be done ...")
                logger.debug(cs_task.result())
                break
        logger.debug(cs_task.result())
        node1.stop(gently=False)
        dir_size = check_commitlog_size(allow_errors=False)
        logger.debug(f"Final commitlog size: [{self._get_commitlog_path()}] {dir_size}M")

        # set commitlog config back to default
        node1.set_configuration_options(values={"commitlog_segment_size_in_mb": 32, "commitlog_total_space_in_mb": -1, "commitlog_reuse_segments": True, "commitlog_use_hard_size_limit": True})
        logger.debug("Restart node1 to enable default commitlog configure")
        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        assert_row_count_in_select_less(session=session, query="select * from ks.cf", max_rows_expected=total_size + 1)

        logger.debug("Test with more data after rollback to default config")
        insert_c1c2(session, n=int(total_size * 1.5))
        assert_row_count(session=session, table_name="ks.cf", expected=int(total_size * 1.5))

    def test_total_space_limit_of_commitlog_with_memory_based_limit(self):
        """
        Test with auto-sized commitlog files, and total space limit (based on available memory)
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=-1, commitlog_total_space_in_mb=-1)

    def test_total_space_limit_of_commitlog_with_large_limit(self):
        """
        Test with 512M commitlog files, total space limit is 3096M
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=512, commitlog_total_space_in_mb=3096)

    def test_total_space_limit_of_commitlog_with_medium_limit(self):
        """
        Test with 100M commitlog files, total space limit is 1024M
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=100, commitlog_total_space_in_mb=1024)

    def test_total_space_limit_of_commitlog_with_small_limit(self):
        """
        Test with 5M commitlog files, total space limit is 30M
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=5, commitlog_total_space_in_mb=30)

    def test_alter_keyspace_durable_writes_false(self):
        """
        Test 'CREATE KEYSPACE ... WITH durable_writes = false;'
        The commitlog should not be used when durable_writes is false.
        """
        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start()
        session = self.patient_cql_connection(node1)

        logger.debug("Create keyspace")
        create_ks(session, "dw", 1)

        logger.debug("Create table")
        session.execute("CREATE TABLE dw.cf(id int PRIMARY KEY)")

        logger.debug("Set durable_writes")
        session.execute("ALTER KEYSPACE dw WITH durable_writes = true")

        logger.debug("Insert data")
        session.execute("INSERT INTO dw.cf(id) VALUES (1);")
        session.execute("INSERT INTO dw.cf(id) VALUES (2);")

        logger.debug("Unset durable_writes")
        session.execute("ALTER KEYSPACE dw WITH durable_writes = false")

        logger.debug("Insert data")
        session.execute("INSERT INTO dw.cf(id) VALUES (3);")
        session.execute("INSERT INTO dw.cf(id) VALUES (4);")

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node again")
        node1.start()

        session = self.patient_cql_connection(node1)
        assert_all(session=session, query="SELECT * FROM dw.cf", expected=[[1], [2]], ignore_order=True)

    def test_alter_keyspace_durable_writes_true(self):
        """
        Test 'ALTER KEYSPACE ... WITH durable_writes = true;'

        After changing durable_writes from false to true, following inserts should
        become persistent immediately.
        """
        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start()
        session = self.patient_cql_connection(node1)

        logger.debug("Create keyspace")
        create_ks(session, "dw", 1)

        logger.debug("Create table")
        session.execute("CREATE TABLE dw.cf(id int PRIMARY KEY);")

        logger.debug("Unset durable_writes")
        session.execute("ALTER KEYSPACE dw WITH durable_writes = false;")

        logger.debug("Insert data")
        session.execute("INSERT INTO dw.cf(id) VALUES (1);")
        session.execute("INSERT INTO dw.cf(id) VALUES (2);")

        logger.debug("Set durable_writes")
        session.execute("ALTER KEYSPACE dw WITH durable_writes = true;")

        logger.debug("Insert data")
        session.execute("INSERT INTO dw.cf(id) VALUES (3);")
        session.execute("INSERT INTO dw.cf(id) VALUES (4);")

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node again")
        node1.start()

        session = self.patient_cql_connection(node1)
        assert_all(session=session, query="SELECT * FROM dw.cf;", expected=[[3], [4]], ignore_order=True)

    def prepare_big_mutation(self):
        self.prepare(batch_commitlog=True)
        self.node1: ScyllaNode
        session: Session = self.patient_cql_connection(self.node1)

        # Create keyspace
        self.ks = "test_ks"
        create_ks(session, self.ks, rf=1)
        self.node1.nodetool(f"disableautocompaction {self.ks}")  # so data is not written to disk

        # the mutation limit is about half of the segment size (default 32MB)
        # so 20 columns of 1MB each is guaranteed to be above it
        self.big_columns = 20
        # add one more collumn as the primary key is not allowed to be this big
        columns = self.big_columns + 1
        self.values = [1, *[f"value_{i:05d}_{'x' * 1024 * 1024}" for i in range(1, columns)]]

        # Create table
        self.cf = "big_commitlog"
        create_statement = f"CREATE TABLE {self.cf} (c00000 int PRIMARY KEY, {', '.join(f'c{i:05d} varchar' for i in range(1, columns))});"
        session.execute(create_statement)
        self.insert_statement = session.prepare(f"INSERT INTO {self.ks}.{self.cf} ({', '.join(f'c{i:05d}' for i in range(columns))}) VALUES({', '.join(f'?' for i in range(columns))});")

    def test_one_big_mutation_replay_on_startup(self):
        """
        Test commit log replay with a single big (larger than mutation limit) commitlog mutation
        The node is forcefully shut down after the transaction is commited
        After restarting the node, check for relevant logs and whether the data exists
        """
        self.prepare_big_mutation()
        node1 = self.node1
        session: Session = self.patient_cql_connection(node1)

        session.execute(self.insert_statement, self.values)
        # check data
        in_table = rows_to_list(session.execute(f"SELECT * FROM {self.ks}.{self.cf};"))
        assert in_table == [self.values]

        # stop node
        # because the mutation is large, it uses a code path that puts the commilog to disk before acknowledging the insert
        node1.stop(gently=False)

        # restart the node
        node1.start(wait_for_binary_proto=True)
        session: Session = self.patient_cql_connection(node1)
        # check the data and the logs
        in_table = rows_to_list(session.execute(f"SELECT * FROM {self.ks}.{self.cf};"))
        assert node1.grep_log(f"large_data - Writing large row {self.ks}/{self.cf}:")
        assert in_table == [self.values]

    def test_one_big_mutation_corrupted_on_startup(self):
        """
        Test commit log replay with a single big (larger than mutation limit) commitlog mutation
        While the node is shut down, corrupt the commitlog
        After restarting the node, check for relevant logs and whether the data exists
        """
        self.prepare_big_mutation()
        node1 = self.node1
        session: Session = self.patient_cql_connection(node1)

        session.execute(self.insert_statement, self.values)
        # check data
        in_table = rows_to_list(session.execute(f"SELECT * FROM {self.ks}.{self.cf};"))
        assert in_table == [self.values]

        # stop node
        # because the mutation is large, it uses a code path that puts the commilog to disk before acknowledging the insert
        node1.stop(gently=False)

        # corrupt the commitlogs
        for commitlog in self._get_commitlog_files():
            corrupt_file(commitlog)

        # restart the node
        node1.start(wait_for_binary_proto=True)
        session: Session = self.patient_cql_connection(node1)
        # check the data and the logs
        in_table = rows_to_list(session.execute(f"SELECT * FROM {self.ks}.{self.cf};"))
        assert not node1.grep_log(f"large_data - Writing large row {self.ks}/{self.cf}")
        assert node1.grep_log("commitlog_replayer - Corrupted file:")
        assert in_table == []

    def test_one_big_mutation_rollback_on_startup(self):
        """
        Test commit log replay with a single big (larger than mutation limit) commitlog mutation
        The node is shut down while the transaction is in progress
        After restarting the node, check for relevant logs and whether the data exists
        """
        self.prepare_big_mutation()
        node1 = self.node1
        session: Session = self.patient_cql_connection(node1)

        # execute_async so it can be interrupted
        session.execute_async(self.insert_statement, self.values)

        # stop node
        node1.stop(gently=False)

        if self._get_commitlog_size()[0] > self.big_columns:
            pytest.skip("Skipping rollback scenario as data was written to commitlog before stopping node")

        # restart the node
        node1.start(wait_for_binary_proto=True)
        session: Session = self.patient_cql_connection(node1)
        # check the data and the logs
        in_table = rows_to_list(session.execute(f"SELECT * FROM {self.ks}.{self.cf};"))
        assert not node1.grep_log(f"large_data - Writing large row {self.ks}/{self.cf}")
        assert in_table == []
