#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import binascii
import glob
import logging
import os
import stat
import struct
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import WriteTimeout
from cassandra.cluster import NoHostAvailable, OperationTimedOut, Session
from ccmlib.common import is_win
from ccmlib.node import Node, TimeoutError
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from tools import commitlog
from tools.assertions import (
    assert_all,
    assert_almost_equal,
    assert_lists_equal_ignoring_order,
    assert_none,
    assert_one,
    assert_row_count,
    assert_row_count_in_select,
    assert_row_count_in_select_less,
)
from tools.data import insert_c1c2, rows_to_list
from tools.docker_utils import running_in_podman
from tools.files import copy_files_to, corrupt_file
from tools.marks import issue_open, with_feature
from tools.metrics import get_node_metrics

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
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

    @pytest.fixture(scope="function", autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.set_configuration_options({"start_rpc": "true"})
        fixture_dtest_setup.cluster.populate(1)
        [self.node1] = fixture_dtest_setup.cluster.nodelist()
        yield

        # Some of the tests change commitlog permissions to provoke failure
        # so this changes them back so we can delete them.
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)

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

    def _change_commitlog_perms(self, mod):
        path = self._get_commitlog_path()
        os.chmod(path, mod)
        commitlogs = glob.glob(path + "/*")
        for commitlog in commitlogs:
            os.chmod(commitlog, mod)

    def _get_commitlog_path(self):
        """Returns the commitlog path"""

        return os.path.join(self.node1.get_path(), "commitlogs")

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
                logger.warn(f"Unrecognized du output line: {l}")
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

    def _provoke_commitlog_failure(self):
        """Provoke the commitlog failure"""

        # Test things are ok at this point
        self.session1.execute(
            """
            INSERT INTO test (key, col1) VALUES (1, 1);
        """
        )
        assert_one(self.session1, "SELECT * FROM test where key=1;", [1, 1])

        self._change_commitlog_perms(0)

        try:
            self.node1.stress(["write", "n=10K", "-col", "size=FIXED(1000)", "-rate", "threads=25"])
        except:
            logger.debug("Stress failed as expected")

    @pytest.mark.dtest_debug
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
        commitlog_dir = os.path.join(node1.get_path(), "commitlogs")
        commitlog_files = commitlog.list_files(commitlog_dir)
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

    @pytest.mark.dtest_debug
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
        commitlog_dir = os.path.join(node1.get_path(), "commitlogs")
        commitlog_files = commitlog.list_files(commitlog_dir)
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

    @pytest.mark.use_cassandra_stress
    def test_default_segment_size(self):
        """Test default commitlog_segment_size_in_mb (32MB)"""

        self._segment_size_test(32)

    @pytest.mark.use_cassandra_stress
    def test_small_segment_size(self):
        """Test a small commitlog_segment_size_in_mb (5MB)"""

        self._segment_size_test(5)

    @pytest.mark.use_cassandra_stress
    def test_default_compressed_segment_size(self):
        """Test default compressed commitlog_segment_size_in_mb (32MB)"""
        # Scylla: Unknown option commitlog_compression
        self._segment_size_test(32, compressed=True)

    @pytest.mark.use_cassandra_stress
    def test_small_compressed_segment_size(self):
        """Test a small compressed commitlog_segment_size_in_mb (5MB)"""
        # Scylla: Unknown option commitlog_compression
        self._segment_size_test(5, compressed=True)

    @pytest.mark.dtest_debug
    @pytest.mark.use_cassandra_stress
    def test_stop_failure_policy(self):
        """Test the stop commitlog failure policy (default one)"""
        self.prepare()
        # #9343 - CL will attempt to re-delete files it fails to create/open.
        # The way we do things, this will cause more exceptions
        self.ignore_log_patterns.append("commitlog - Could not (delete|recycle) segment")

        self._provoke_commitlog_failure()
        self.expected_log_message = "storage_service - Shutting down communications due to I/O errors until operator intervention"
        failure = self.node1.grep_log(self.expected_log_message)
        logger.debug(failure)
        assert failure, f"Cannot find the commitlog failure message in logs, searched for {self.expected_log_message}"
        assert self.node1.is_running(), "Node1 should still be running"

        # Cannot write anymore after the failure
        with pytest.raises(NoHostAvailable):
            self.session1.execute(
                """
              INSERT INTO test (key, col1) VALUES (2, 2);
            """
            )

        # Should not be able to read neither
        with pytest.raises(NoHostAvailable):
            self.session1.execute(
                """
              "SELECT * FROM test;"
            """
            )

    @pytest.mark.skip("scylladb/scylla-dtest#2190")
    # Scylla reports the failed commitlog entries, but they will be ignored, it won't break
    # the startup like Cassandra. And Scylla doesn't have `commit_failure_policy` option.
    def test_bad_crc(self):
        """
        if the commit log header crc (checksum) doesn't match the actual crc of the header data,
        and the commit_failure_policy is stop, C* shouldn't startup
        @jira_ticket CASSANDRA-9749
        """
        self.ignore_log_patterns.append("cdc - Could not retrieve CDC streams with timestamp")
        expected_error = "Exiting due to error while processing commit log during initialization."
        self.ignore_log_patterns.append(expected_error)
        node = self.node1
        assert isinstance(node, Node), f"expect node instance of Node, node is {type(node)}"
        node.set_configuration_options({"commit_failure_policy": "stop", "commitlog_sync_period_in_ms": 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(cursor, "ks", 1)
        cursor.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute(f"INSERT INTO ks.tbl (k, v) VALUES ({i}, {i})")

        results = list(cursor.execute("SELECT * FROM ks.tbl"))
        assert len(results) == 10, f"expexted 10, actual len(results)={len(results)}"

        # with the commitlog_sync_period_in_ms set to 1000,
        # this sleep guarantees that the commitlog data is
        # actually flushed to disk before we kill -9 it
        time.sleep(1)

        node.stop(gently=False)

        # check that ks.tbl hasn't been flushed
        path = node.get_path()
        ks_dir = os.path.join(path, "data", "ks")
        db_dir = os.listdir(ks_dir)[0]
        sstables = len([f for f in os.listdir(os.path.join(ks_dir, db_dir)) if f.endswith(".db")])
        assert sstables == 0, f"expected 0 , actual sstables={sstables}"

        # modify the commit log crc values
        cl_dir = os.path.join(path, "commitlogs")
        assert len(os.listdir(cl_dir)) > 0, "expected >0, actual len(os.listdir(cl_dir))={len(os.listdir(cl_dir))}"
        for cl in os.listdir(cl_dir):
            if cl == ".lock":
                continue
            # locate the CRC location
            with open(os.path.join(cl_dir, cl), "rb") as f:
                f.seek(0)
                out = f.read(4)
                if len(out) != 4:
                    logger.debug(f"{cl}, {out}")
                    continue
                version = struct.unpack(">i", out)[0]
                crc_pos = 12
                if version >= 5:
                    f.seek(crc_pos)
                    psize = struct.unpack(">h", f.read(2))[0] & 0xFFFF
                    crc_pos += 2 + psize

            # rewrite it with crap
            with open(os.path.join(cl_dir, cl), "wb") as f:
                f.seek(crc_pos)
                f.write(struct.pack(">i", 123456))

            # verify said crap
            with open(os.path.join(cl_dir, cl), "rb") as f:
                f.seek(crc_pos)
                crc = struct.unpack(">i", f.read(4))[0]
                assert crc == 123456, f"expected 123456, actual crc={crc}"

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark)
        with pytest.raises(TimeoutError):
            node.wait_for_binary_interface(from_mark=mark, timeout=20)
        assert not node.is_running(), f"expected node is not running, actual is: {node.is_running()}"

    @pytest.mark.skip("scylladb/scylladb#8972")
    # Scylla doesn't support the commitlog compression, and Scylla doesn't have `commit_failure_policy` option.
    def test_compression_error(self):  # noqa: PLR0915
        """
        if the commit log header refers to an unknown compression class, and the commit_failure_policy is stop, C* shouldn't startup
        """
        expected_error = "Could not create Compression for type org.apache.cassandra.io.compress.LZ5Compressor"
        self.ignore_log_patterns.append(expected_error)
        node = self.node1
        assert isinstance(node, Node), f"expect node instance of Node, node is {type(node)}"
        node.set_configuration_options({"commit_failure_policy": "stop", "commitlog_compression": [{"class_name": "LZ4Compressor"}], "commitlog_sync_period_in_ms": 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(cursor, "ks1", 1)
        cursor.execute("CREATE TABLE ks1.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute(f"INSERT INTO ks1.tbl (k, v) VALUES ({i}, {i})")

        results = list(cursor.execute("SELECT * FROM ks1.tbl"))
        assert len(results) == 10, f"expect 10, len(results)={len(results)}"

        # with the commitlog_sync_period_in_ms set to 1000,
        # this sleep guarantees that the commitlog data is
        # actually flushed to disk before we kill -9 it
        time.sleep(1)

        node.stop(gently=False)

        # check that ks1.tbl hasn't been flushed
        path = node.get_path()
        ks_dir = os.path.join(path, "data", "ks1")
        db_dir = os.listdir(ks_dir)[0]
        sstables = len([f for f in os.listdir(os.path.join(ks_dir, db_dir)) if f.endswith(".db")])
        self.assertEqual(sstables, 0)

        def get_header_crc(header):
            """
            When calculating the header crc, C* splits up the 8b id, first adding the 4 least significant
            bytes to the crc, then the 5 most significant bytes, so this splits them and calculates the same way
            """
            new_header = header[:4]
            # C* evaluates most and least significant 4 bytes out of order
            new_header += header[8:12]
            new_header += header[4:8]
            # C* evaluates the short parameter length as an int
            new_header += b"\x00\x00" + header[12:14]  # the
            new_header += header[14:]
            return binascii.crc32(new_header)

        # modify the compression parameters to look for a compressor that isn't there
        # while this scenario is pretty unlikely, if a jar or lib got moved or something,
        # you'd have a similar situation, which would be fixable by the user
        cl_dir = os.path.join(path, "commitlogs")
        assert len(os.listdir(cl_dir)) > 0, f"expected >0, actual len(os.listdir(cl_dir))={len(os.listdir(cl_dir))}"
        for cl in os.listdir(cl_dir):
            if cl == ".lock":
                continue
            # read the header and find the crc location
            with open(os.path.join(cl_dir, cl), "rb") as f:
                f.seek(0)
                crc_pos = 12
                f.seek(crc_pos)
                out = f.read(2)
                if len(out) != 2:
                    logger.debug(f"{cl}, {out}")
                psize = struct.unpack(">h", out)[0] & 0xFFFF
                crc_pos += 2 + psize

                header_length = crc_pos
                f.seek(crc_pos)
                crc = struct.unpack(">i", f.read(4))[0]

                # check that we're going this right
                f.seek(0)
                header_bytes = f.read(header_length)
                assert get_header_crc(header_bytes) == crc, f"expect crc={crc}, actual={get_header_crc(header_bytes)}"

            # rewrite it with imaginary compressor
            assert "LZ4Compressor" in header_bytes, "expect 'LZ4Compressor', actual LZ4 not found in header_bytes"
            header_bytes = header_bytes.replace("LZ4Compressor", "LZ5Compressor")
            assert "LZ4Compressor" not in header_bytes, "not expect 'LZ4Compressor', actual LZ4 found in header_bytes"
            assert "LZ5Compressor" in header_bytes, "expect 'LZ5Compressor', actual LZ5 not found in header_bytes"
            with open(os.path.join(cl_dir, cl), "w") as f:
                f.seek(0)
                f.write(header_bytes)
                f.seek(crc_pos)
                f.write(struct.pack(">i", get_header_crc(header_bytes)))

            # verify we wrote everything correctly
            with open(os.path.join(cl_dir, cl)) as f:
                f.seek(0)
                header = f.read(header_length)
                assert header == header_bytes, f"expecting header bytes, got {header} \n header_bytes={header_bytes} "
                f.seek(crc_pos)
                crc = struct.unpack(">i", f.read(4))[0]
                assert crc == get_header_crc(header_bytes), f"expecting crc={crc}, got {get_header_crc(header_bytes)}"

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark)
        with pytest.raises(TimeoutError):
            node.wait_for_binary_interface(from_mark=mark, timeout=20)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_commitlog_replay_with_counters(self):
        """
        Test commit log replay with counters
        The goal of the test is to verify that commit log replay works correctly -
        we save the end result in the commit log, not delta.
        """
        node1 = self.node1
        node1.set_configuration_options(values={"commitlog_sync_period_in_ms": 200})
        self.cluster.start()

        logger.debug("Create table")
        session = self.patient_cql_connection(node1)
        create_ks(session, "Test", 1)
        session.execute(
            """
                    CREATE TABLE cf (
                        pk1 INT,
                        ck1 INT,
                        cnt COUNTER,
                        PRIMARY KEY(pk1, ck1)
                    );
                """
        )

        logger.debug("Increment counter")
        for i in range(1, 10):
            session.execute(f"UPDATE Test.cf SET cnt = cnt + {i} WHERE pk1 = 5 AND ck1 = 6;")

        res = session.execute("SELECT cnt FROM Test.cf WHERE pk1 = 5 AND ck1 = 6;")
        rows = rows_to_list(res)
        assert rows[0][0] == 45, f"expecting 45, got rows[0][0]={rows[0][0]}"

        logger.debug("Decrement counter")
        session.execute("UPDATE Test.cf SET cnt = cnt - 1 WHERE pk1 = 5 AND ck1 = 6;")
        logger.debug("Add one more counter")
        session.execute("UPDATE Test.cf SET cnt = cnt + 10 WHERE pk1 = 7 AND ck1 = 8;")

        res = session.execute("SELECT cnt FROM Test.cf;")
        rows = rows_to_list(res)
        assert rows[0][0] == 44, f"expecting 44, got rows[0][0]={rows[0][0]}"
        assert rows[1][0] == 10, f"expecting 10, got rows[1][0]={rows[1][0]}"

        # wait for commit log sync
        time.sleep(2)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Verify commitlog was written before abrupt stop")
        commitlog_dir = os.path.join(node1.get_path(), "commitlogs")
        commitlog_files = glob.glob(os.path.join(commitlog_dir, "*.log"))
        assert len(commitlog_files) > 0, f"expecting >0, got len(commitlog_files)={len(commitlog_files)}"

        logger.debug("Verify commit log was replayed on startup")
        node1.start()
        node1.watch_log_for("Log replay complete")
        # Here we verify there was more than 0 replayed mutations
        zero_replays = node1.grep_log(" 0 replayed mutations", filter_expr="DEBUG")
        assert len(zero_replays) == 0, f"expecting 0, got len(zero_replays)={len(zero_replays)}"

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT cnt FROM Test.cf;")
        rows = rows_to_list(res)
        assert rows[0][0] == 44, f"expecting 44, got rows[0][0]={rows[0][0]}"
        assert rows[1][0] == 10, f"expecting 10, got rows[1][0]={rows[1][0]}"

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
        node1.start(verbose=True, wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        assert_row_count_in_select_less(session=session, query="select * from Test.cf", max_rows_expected=100)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_batch_commitlog(self):
        """
        Test batch mode of commitlog flushing
        'batch' mode where each write is flushed as soon as possible (after previous flush completed)
        and writes are only ready after they are flushed.
        This mode is used for LWT
        """
        session, node1 = self.prepare_cluster_with_ks_cf()

        logger.debug("Insert 100 rows")
        for i in range(100):
            session.execute(f"UPDATE Test.cf SET v1={i} WHERE pk1 = {i} and ck1={i} IF v1 = NULL")

        assert_row_count(session=session, table_name="Test.cf", expected=100)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node")
        node1.start()

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        assert_row_count(session=session, table_name="Test.cf", expected=100)

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

    @pytest.mark.use_cassandra_stress
    def test_total_space_limit_of_commitlog_with_memory_based_limit(self):
        """
        Test with auto-sized commitlog files, and total space limit (based on available memory)
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=-1, commitlog_total_space_in_mb=-1)

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    @pytest.mark.use_cassandra_stress
    def test_total_space_limit_of_commitlog_with_large_limit(self):
        """
        Test with 512M commitlog files, total space limit is 3096M
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=512, commitlog_total_space_in_mb=3096)

    @pytest.mark.use_cassandra_stress
    def test_total_space_limit_of_commitlog_with_medium_limit(self):
        """
        Test with 100M commitlog files, total space limit is 1024M
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=100, commitlog_total_space_in_mb=1024)

    @pytest.mark.use_cassandra_stress
    def test_total_space_limit_of_commitlog_with_small_limit(self):
        """
        Test with 5M commitlog files, total space limit is 30M
        """
        self._test_total_space_limit_of_commitlog(commitlog_segment_size_in_mb=5, commitlog_total_space_in_mb=30)

    @pytest.mark.skipif(condition=running_in_podman(), reason="podman doesn't not support creating loop devices")
    def test_commitlog_enospc(self, cleanup_firstly_by_drain=True):  # noqa: PLR0915
        """
        Fill data until reach to ENSPC. Try to recover by extending space and restart scylla-server.
        In this test, it won't cleanup existing commitlog by a drain and restart before real test.
        The commitlog space will be limited by a small loop device.

        Related Scylla PR: https://github.com/scylladb/scylla/pull/6368
        """
        node1 = self.node1
        # Size of the loop device
        commitlog_dir_limit_in_mb = 20
        node1.set_configuration_options(values={"commitlog_segment_size_in_mb": 1})
        if cleanup_firstly_by_drain:
            node1.start(wait_for_binary_proto=True)
            logger.debug("Clean the existing commitlog by drain, otherwise ENOSPC occurs too early than expected")
            node1.nodetool("drain")
            node1.stop(gently=True)
            logger.debug(f"Commitlog size after stop: {self._get_commitlog_size()}")

        commitlog_dir = self._get_commitlog_path()
        tmp_img = os.path.join(self.node1.get_path(), "tmp_loopdev_for_commitlog.img")

        def exec_cmd(cmd, ignore_status=False, debug_output=False, shell=False, env=None):
            proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell, env=env)
            out, err = proc.communicate()
            exit_status = proc.wait()
            out = out.decode()
            if not ignore_status:
                assert exit_status == 0, f"Failed to execute command: {cmd}\nExit Status: {exit_status}\nError: {err}\nOutput: {out}"
            if debug_output:
                logger.debug(out)
            return exit_status, out

        logger.debug("Mount commitlog directory to a size limited device")
        exec_cmd(f"dd if=/dev/zero of={tmp_img} bs=1M count={commitlog_dir_limit_in_mb}")
        # new mkfs.xfs does not support <300MB file systems except for its own unit tests.
        # temp workaround, add same env vars as said tests to force create this small fs.
        # See: https://lkml.kernel.org/linux-xfs/Yv2A9Ggkv%2FNBrTd4@magnolia/
        exec_cmd(f"mkfs.xfs -f {tmp_img} -m crc=0,finobt=0", env={**os.environ, "TEST_DIR": "1", "TEST_DEV": "1", "QA_CHECK_FS": "1"})
        # checking existing loop devices and setup
        logger.debug(subprocess.getoutput("ls /dev/loop*"))
        exec_cmd("losetup -la", debug_output=True)

        # setup a new loop device
        loopdev = exec_cmd(f"sudo losetup --find --show {tmp_img}", debug_output=True)[1]
        logger.debug(subprocess.getoutput("ls /dev/loop*"))
        exec_cmd("losetup -la", debug_output=True)

        mount_cmd = f"sudo mount -o loop -t xfs {tmp_img} {commitlog_dir} -v"
        exec_cmd(mount_cmd, debug_output=True)
        user = os.environ.get("USER", exec_cmd("whoami")[1].strip())
        exec_cmd(f"sudo chown -R {user}:{user} {commitlog_dir}")

        unit_size = 10000
        total_size = 0

        try:
            logger.debug(f"Commitlog size before start: {self._get_commitlog_size()}")
            logger.debug("Start cluster ...")
            self.cluster.start(wait_for_binary_proto=True)

            logger.debug("Create test keyspace and table")
            session = self.patient_cql_connection(node1)
            create_ks(session, "ks", 1)
            create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
            while True:
                logger.debug(f"Current commitlog size: {self._get_commitlog_size(allow_errors=True)}")
                logger.debug(f"Insert {unit_size} rows ....")
                insert_c1c2(session, keys=range(total_size, total_size + unit_size))
                total_size += unit_size
        except Exception as ex:  # noqa: BLE001
            logger.debug(f"Commitlog size after exception raised: {self._get_commitlog_size(allow_errors=True)}")
            logger.debug(str(ex))

        # Recover from ENOSPC
        node1.stop(gently=False)
        tmpdir = tempfile.mkdtemp()
        copy_files_to(commitlog_dir, tmpdir, files_only=False)
        logger.debug("Umount commitlog dir and restart node")
        exec_cmd(f"sudo umount {commitlog_dir}")
        exec_cmd("losetup -la", debug_output=True)
        exec_cmd(f"sudo losetup -d {loopdev}")
        exec_cmd("losetup -la", debug_output=True)
        copy_files_to(tmpdir, commitlog_dir, files_only=False, dirs_exist_ok=True)
        exec_cmd(f"sudo chown -R {user}:{user} {commitlog_dir}")

        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)

        # Verified that ENOSPC occurred and not all the data is wrote into db
        assert_row_count_in_select_less(session=session, query="SELECT count(*) FROM ks.cf", max_rows_expected=total_size)
        timeout = 10
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            timeout *= 3
        node1.watch_log_for("No space left on device", timeout=timeout)

        logger.debug("Added more data after recovered from ENOSPC ...")
        insert_c1c2(session, n=int(total_size * 1.5))
        assert_row_count(session=session, table_name="ks.cf", expected=int(total_size * 1.5))

    @pytest.mark.skipif(condition=running_in_podman(), reason="podman doesn't not support creating loop devices")
    def test_commitlog_enospc_without_cleanup(self):
        """
        Fill data until reach to ENSPC. Try to recover by extending space and restart scylla-server.
        In this test, it won't cleanup existing commitlog by a drain and restart before real test.
        The commitlog space will be limited by a mounted small loop device.

        Related Scylla PR: https://github.com/scylladb/scylla/pull/6368
        """
        self.ignore_log_patterns += ["Could not retrieve CDC streams with timestamp"]
        self.test_commitlog_enospc(cleanup_firstly_by_drain=False)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_mixed_mode_commitlog_2_partitions_smp_1(self):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT
        """
        session, node1 = self.prepare_cluster_with_ks_cf(jvm_args=["--smp", "1"])

        expected_result = []
        logger.debug("Insert 200 rows")
        for i in range(100):
            # Row - candidate for 'batch' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
            # Row - candidate for 'periodic' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({2}, {i}, {i})")
            expected_result.append([2, i, i])
            expected_result.append([1, i, i])

        logger.debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(100, 200):
            if i == 150:
                session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
                expected_result.append([1, i, i])
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({2}, {i}, {i})")
            if i < 150:
                expected_result.append([2, i, i])

        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=1", num_rows_expected=101)
        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=2", num_rows_expected=200)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node")
        node1.start()

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=1", num_rows_expected=101)
        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=2", num_rows_expected=150)
        assert_all(session=session, query="select * from Test.cf", expected=expected_result, ignore_order=True)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_mixed_mode_commitlog_2_partitions_smp_2(self):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT

        By Glebs explanation:
            When LWT and non-LWT data is written into different partitions and smp > 1
            non-LWT rows may be flushed for many reasons, or may be not.
            Any number between 0 and total number of written non LWT rows are expected. So there is no expected
            result for non-LWT rows
        """
        session, node1 = self.prepare_cluster_with_ks_cf(jvm_args=["--smp", "2"])

        expected_result = []
        logger.debug("Insert 200 rows")
        for i in range(100):
            # Row - candidate for 'batch' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
            # Row - candidate for 'periodic' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({2}, {i}, {i})")
            expected_result.append([1, i, i])

        logger.debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(100, 200):
            if i == 150:
                session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
                expected_result.append([1, i, i])
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({2}, {i}, {i})")

        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=1", num_rows_expected=101)
        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=2", num_rows_expected=200)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node")
        node1.start()

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=1", num_rows_expected=101)
        assert_all(session=session, query="select * from Test.cf where pk1=1", expected=expected_result, ignore_order=True)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_mixed_mode_commitlog_same_partition_smp_1(self):
        self._mixed_mode_commitlog_same_partition(smp="1")

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_mixed_mode_commitlog_same_partition_smp_2(self):
        self._mixed_mode_commitlog_same_partition(smp="2")

    def _mixed_mode_commitlog_same_partition(self, smp):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT

          When LWT and non-LWT data is written into the same partition it isn't matter how many smp -
          all non-LWT rows that were arrived before last LWT row should be flushed
        """
        session, node1 = self.prepare_cluster_with_ks_cf(jvm_args=["--smp", smp])

        expected_result = []
        logger.debug("Insert 200 rows")
        for i in range(100):
            # Row - candidate for 'batch' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
            expected_result.append([1, i, i])
        for i in range(100, 200):
            # Row - candidate for 'periodic' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i}, {i})")
            expected_result.append([1, i, i])

        logger.debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(200, 300):
            if i == 250:
                session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
            else:
                session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i}, {i})")

            if i <= 250:
                expected_result.append([1, i, i])
        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=1", num_rows_expected=300)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node")
        node1.start()

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query="select * from Test.cf", num_rows_expected=251)
        assert_all(session=session, query="select * from Test.cf", expected=expected_result, ignore_order=True)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#18068"))
    def test_mixed_mode_with_delete_commitlog(self):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT
        """
        session, node1 = self.prepare_cluster_with_ks_cf()

        expected_result = []
        logger.debug("Insert 100 non-LWT rows")
        for i in range(100):
            # Row - candidate for 'periodic' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i}, {i})")
            expected_result.append([1, i, i])

        logger.debug("Insert 100 LWT rows")
        for i in range(100, 200):
            # Row - candidate for 'batch' mode
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i},{i}) IF NOT EXISTS")
            if i != 150:
                expected_result.append([1, i, i])

        logger.debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(200, 300):
            if i == 250:
                session.execute("DELETE FROM Test.cf WHERE pk1 = 1 and ck1 = 150 IF EXISTS")
            session.execute(f"INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({1}, {i}, {i})")
            if i < 250:
                expected_result.append([1, i, i])

        assert_row_count_in_select(session=session, query="select * from Test.cf where pk1=1", num_rows_expected=299)

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Start node")
        node1.start()

        logger.debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query="select * from Test.cf", num_rows_expected=249)
        assert_all(session=session, query="select * from Test.cf", expected=expected_result, ignore_order=True)

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

    @pytest.mark.xfail(reason="https://github.com/scylladb/scylladb/issues/14870")
    def test_pinned_cl_segment_doesnt_resurrect_data(self):  # noqa: PLR0915
        """
        The tested scenario is as follows:
        * Two tables, ks1.tbl1 and ks2.tbl2.
        * A commitlog segment contains writes for both tables.
        * ks1.tbl1 has very low write traffix, memtables are very rarely
          flushed, this results in ks1.tbl1 "pinning" any commitlog segment
          which has writes, that are still in the memtable.
        * ks2.tbl2 deletes the data, some of which is still in the segment
          pinned by ks1.tbl1.
        * ks2.tbl2 flushes the memtable, data gets to sstables, commitlog
          segments containing writes for both tables are still pinned.
        * ks2.tbl2 compacts and purges away both the data and the tombstone.
        * The node has an unclean restart, the pinned segments are replayed and
          data is resurrected, as the tombstone is already purged.

        This test uses gc_grace_seconds=0 to make the test fast.

        See https://github.com/scylladb/scylladb/issues/14870
        """
        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True, values={"commitlog_segment_size_in_mb": 1, "enable_cache": False})
        node1.start()

        session = self.patient_cql_connection(node1)

        logger.debug("Create keyspace")
        create_ks(session, "ks1", 1)
        create_ks(session, "ks2", 1)

        logger.debug("Create table")
        session.execute("CREATE TABLE ks1.tbl1 (pk int, ck int, PRIMARY KEY (pk, ck))")
        session.execute("CREATE TABLE ks2.tbl2 (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0")

        def get_segments_num():
            metrics_res = get_node_metrics(node_ip=self.cluster.get_node_ip(1), metrics=["scylla_commitlog_segments"])
            return int(metrics_res["scylla_commitlog_segments"])

        def get_cl_segments():
            cl_path = self._get_commitlog_path()
            return {os.path.basename(s) for s in glob.glob(os.path.join(cl_path, "CommitLog-*"))}

        segments_before_writes = get_segments_num()
        segments_after_writes = segments_before_writes

        logger.debug(f"Have {segments_after_writes} segments before writing data")

        insert_id_tbl1 = session.prepare("INSERT INTO ks1.tbl1 (pk, ck) VALUES (?, ?)")
        insert_id_tbl2 = session.prepare("INSERT INTO ks2.tbl2 (pk, ck, v) VALUES (?, ?, ?)")
        pk1 = 0
        pk2 = 1
        ck = 0
        value = "v" * 1024

        logger.debug(f"Filling segment with mixed data from ks1.tbl1 and ks2.tbl2")

        # Ensure at least one segment with writes from both tables
        while segments_after_writes < segments_before_writes + 1:
            session.execute(insert_id_tbl1, (pk1, ck))
            session.execute(insert_id_tbl2, (pk1, ck, value))
            ck = ck + 1
            segments_after_writes = get_segments_num()

        logger.debug(f"Filling segment(s) with ks2.tbl2 only")

        while segments_after_writes < segments_before_writes + 3:
            session.execute(insert_id_tbl2, (pk1, ck, value))
            ck = ck + 1
            segments_after_writes = get_segments_num()

        session.execute(f"DELETE FROM ks2.tbl2 WHERE pk = {pk1}")

        # We need to make sure the segment in which the above delete landed in
        # is full, otherwise the memtable flush will not be able to destroy it.
        logger.debug(f"Filling another segment with ks2.tbl2 (pk={pk2})")

        while segments_after_writes < segments_before_writes + 4:
            session.execute(insert_id_tbl2, (pk2, ck, value))
            ck = ck + 1
            segments_after_writes = get_segments_num()

        segments_before = get_cl_segments()
        logger.debug(f"Wrote {ck} rows, now have {segments_after_writes} segments ({segments_before}")

        logger.debug("Flush ks2.tbl2")
        node1.flush(ks="ks2", table="tbl2")
        node1.compact(keyspace="ks2", tables=["tbl2"])

        segments_after = get_cl_segments()
        logger.debug(f"After flush+compact, now have {get_segments_num()} segments ({segments_after})")

        assert len(list(session.execute(f"SELECT * FROM ks2.tbl2 WHERE pk = {pk1}"))) == 0
        # Need to ensure at least one segment was freed.
        # We assume the last segment, containing the tombstone, was among the freed ones.
        removed_segments = segments_before - segments_after
        assert len(removed_segments) > 0

        logger.debug(f"The following segments were removed: {removed_segments}")

        logger.debug("Kill + restart the node")
        node1.stop(gently=False)
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)

        # CL replay should not have resurrected the data
        assert len(list(session.execute(f"SELECT * FROM ks2.tbl2 WHERE pk = {pk1}"))) == 0

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

    @pytest.mark.required_features("tablets")
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

    @pytest.mark.required_features("tablets")
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
        for commitlog in glob.glob(os.path.join(node1.get_path(), "commitlogs") + "/*.log"):
            corrupt_file(commitlog)

        # restart the node
        node1.start(wait_for_binary_proto=True)
        session: Session = self.patient_cql_connection(node1)
        # check the data and the logs
        in_table = rows_to_list(session.execute(f"SELECT * FROM {self.ks}.{self.cf};"))
        assert not node1.grep_log(f"large_data - Writing large row {self.ks}/{self.cf}")
        assert node1.grep_log("commitlog_replayer - Corrupted file:")
        assert in_table == []

    @pytest.mark.required_features("tablets")
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
