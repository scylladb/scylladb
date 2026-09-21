import glob
import logging
import os
import re
import shutil
import subprocess
import time
from io import StringIO

import pytest

from dtest_class import Tester, create_cf, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.marks import issue_open, requireif, unmark, with_feature
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestSSTableGenerationAndLoading(Tester):
    @pytest.fixture(scope="class", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):  # pylist:disable=unused-argument
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"start_rpc": "true"})
        return dtest_setup_overrides

    @pytest.mark.single_node
    def test_promoted_index_generation_with_small_partition_followed_by_a_large_partition(self):
        """
        Tests for https://github.com/scylladb/scylla/issues/1567
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"enable_cache": False})
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            create_ks(session=session, name="ks", rf=1)
            session.execute("CREATE TABLE ks.test (pk int, ck text, s1 int static, v int, PRIMARY KEY (pk, ck));")
            session.execute("insert into ks.test (pk, s1) values (1, 7);")
            session.execute("insert into ks.test (pk, s1) values (0, 7);")

            for idx in range(2000):
                session.execute("insert into ks.test  (pk, ck, v) values (0, 'ck_%d', %d);" % (idx, idx))

        node1.stop()
        node1.start(wait_for_binary_proto=True)

        with self.patient_cql_connection(node1) as session:
            rows = list(session.execute("select * from ks.test where pk = 0 and ck = 'ck_0';"))
        assert len(rows) == 1
        assert [0, "ck_0", 7, 0], list(rows[0])

        # Fails due to https://github.com/scylladb/scylla/issues/1568
        # rows = list(session.execute('select * from ks.test where pk = 0 and ck = \'ck_45\''))
        # assert(len(rows) == 1)
        # assert [0, 'ck_45', 7, 45] == list(rows[0])

    @pytest.mark.single_node
    def test_incompressible_data_in_compressed_table(self):
        """
        tests for the bug that caused #3370:
        https://issues.apache.org/jira/browse/CASSANDRA-3370

        inserts random data into a compressed table. The compressed SSTable was
        compared to the uncompressed and was found to indeed be larger then
        uncompressed.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.5)

        with self.patient_cql_connection(node1) as session:
            create_ks(session=session, name="ks", rf=1)
            create_cf(session=session, name="cf", compression="Deflate")

            # make unique column names, and values that are incompressible
            for col in range(10):
                col_name = str(col)
                col_val = os.urandom(5000)
                col_val = col_val.hex()
                cql = f"UPDATE cf SET v='{col_val}' WHERE KEY='0' AND c='{col_name}';"
                # print cql
                session.execute(cql)

            node1.flush()
            time.sleep(2)
            rows = list(session.execute("SELECT * FROM cf WHERE KEY = '0' AND c < '8';"))
            assert rows

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    @pytest.mark.cluster_options(abort_on_malformed_sstable_error=False)
    def test_remove_index_file(self, fixture_dtest_setup):  # pylint:disable=too-many-statements, too-many-locals  # noqa: PLR0915
        """
        tests for situations similar to that found in #343:
        https://issues.apache.org/jira/browse/CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # Makinge sure the cluster is ready to accept the subsequent
        # stress connection. This was an issue on Windows.
        logger.info("Writing initial data")
        node1.stress(["write", "n=10000", "-rate", "threads=8"])

        # Query existing data and keep in original_rows
        with self.patient_cql_connection(node1) as session:
            stress_table = "keyspace1.standard1"
            logger.info("Retrieving initial data")
            original_rows = list(session.execute(f"SELECT * FROM {stress_table}"))

        logger.info("Stopping node and removing summary")
        node1.flush()
        node1.compact()
        node1.stop()
        time.sleep(1)
        path = ""
        basepath = os.path.join(node1.get_path(), "data", "keyspace1")
        for dir_name in os.listdir(basepath):
            if dir_name.startswith("standard1"):
                path = os.path.join(basepath, dir_name)

        # Verify that Summary can be regenerated
        # and that the data is still there
        os.system("rm %s/*Summary.db" % path)

        logger.info("Starting node")
        node1.start(wait_for_binary_proto=True)
        with self.patient_cql_connection(node1) as session:
            logger.info("Verifying data")
            new_rows = list(session.execute(f"SELECT * FROM {stress_table}"))
            assert original_rows == new_rows

        logger.info("Stopping node")
        node1.stop()
        time.sleep(1)
        os.system("rm -rf %s/snapshots" % path)
        os.system("mkdir %s/snapshots" % path)

        fixture_dtest_setup.ignore_log_patterns += [
            r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from.* " r"'.*': sstables::malformed_sstable_exception[ :]+\(?.*: file not found\)?",
            r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from.* " r"'.*': sstables::malformed_sstable_exception[ :]+\(?.*: No such file or directory\)?",
            r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from.*"
            r"'.*': std::filesystem::(__cxx11::)?filesystem_error[ :]+\(?(error system:2, )?filesystem error: (open|stat) "
            r"failed: No such file or directory \[.*\]\)?",
            r"database - Unrecognized error while processing .*: std::filesystem::(__cxx11::)?filesystem_error" r"[ :]+\(?(error system:2, )?filesystem error: (open|stat) failed: No such file or directory \[.*\]\)?",
            r"database - malformed sstable .*: .*: file not found",
            r"database - malformed sstable .*: .*: No such file or directory",
            r"sstable - Could not (create|open) SSTable component.*No such file or directory",
            r"init - Startup failed: std::runtime_error",
        ]

        timeout = 100

        # For each of these component files, verify that if it's removed
        # then the sstable is is detected is malformed but the data
        # file is not lost
        comps = ["Index.db", "Filter.db", "Statistics.db", "Digest.*", "Partitions.db", "Rows.db"]
        for comp in comps:
            if not glob.glob(f"*{comp}", root_dir=path):
                continue
            logger.info(f"Removing {comp}")
            os.system(f"mv {path}/*{comp} {path}/snapshots/")

            # Since 2026.2 ('Add digests for all sstable components in scylla metadata' from Taras Veretilnyk)
            # Missing Digest component is tolerated
            expected_to_fail = "Digest" not in comp
            logger.info(f"Starting node{', expected to fail' if expected_to_fail else ''}")
            mark = node1.mark_log()
            node1.start(no_wait=True)
            expected_pattern = "malformed_sstable_exception|Startup failed: std::runtime_error"
            if not expected_to_fail:
                expected_pattern += "|Starting listening for CQL clients"
            node1.watch_log_for(expected_pattern, timeout=timeout, from_mark=mark)
            logger.info("Stopping node")
            node1.stop(wait=False, gently=False)
            time.sleep(1)

            data_found = 0
            for fname in os.listdir(path):
                if fname.endswith("Data.db"):
                    data_found += 1
            assert data_found > 0, "After removing %s, the data file was deleted!" % comp

            os.system(f"mv {path}/snapshots/*{comp} {path}/")

        # Finally, verify that the data is still there after renaming
        # all components back.
        logger.info("Starting node")
        node1.start(wait_for_binary_proto=True)
        with self.patient_cql_connection(node1) as session:
            logger.info("Verifying data")
            new_rows = list(session.execute(f"SELECT * FROM {stress_table}"))
        assert original_rows == new_rows
