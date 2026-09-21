import logging
import os
import random
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import NodetoolError

from dtest_class import Tester, create_cf, create_ks
from tools import commitlog
from tools.cluster import new_node
from tools.data import insert_c1c2, query_c1c2_concurrent
from tools.files import copy_files_to, get_list_of_sstables, get_node_cf_dir, get_sstables_files, safe_mkdtemp
from tools.marks import with_feature

logger = logging.getLogger(__name__)


@pytest.mark.next_gating
@pytest.mark.dtest_full
class TestBackupRestore(Tester):
    SNAPSHOT_NAME = "test-snapshot"

    @pytest.mark.single_node
    def test_failure_durring_snapshot_no_corrupt_data(self):
        """
        Check that we can recover from a failure durring snapshot:

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Start create snapshot
        4. Kill node
        5. Start node
        6. Check that all data exists

        """
        node1, session = self.prepare()
        keys, c1_values, c2_values = self.create_data(session)

        logger.debug("Taking a snapshot...")
        self.start_nodetool_and_kill_node(node1, f"snapshot -t {self.SNAPSHOT_NAME}")

        logger.debug("Restarting node1...")
        node1.start(wait_for_binary_proto=True)

        logger.debug("Checking rows on node1...")
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c1_values, c2_values=c2_values)

    @pytest.mark.single_node
    def test_failure_durring_restore_no_corrupt_data(self):
        """
        Check that we can recover from a failure during restore

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table + populate new data + drain
        6. Start restore data
        7. Kill node
        8. Start node
        9. Check that all data exists
        """

        node1, session = self.prepare()
        keys, c1_values, c2_values = self.create_data(session)

        logger.info("Creating a snapshot...")
        node1.nodetool(f"snapshot -t {self.SNAPSHOT_NAME} -cf cf -- ks")

        logger.info("Dropping a keyspace...")
        session.execute(SimpleStatement("DROP KEYSPACE ks"))

        logger.info("Creating the same keyspace.table with different content...")
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE, c1_values=c2_values, c2_values=c1_values)

        # sanity check
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c2_values, c2_values=c1_values)

        logger.info("Draining the cluster...")
        node1.nodetool("drain")

        snapshot_dir = self.get_snapshot_dir(self.SNAPSHOT_NAME)
        assert snapshot_dir is not None, f"Can't find a snapshot directory for {self.SNAPSHOT_NAME}"
        logger.info(f"Snapshot dir is {snapshot_dir}")

        ks_dir = os.path.join(self.test_path, "test", "node1", "data", "ks")

        #
        # As a result of 'DROP KEYSPACE' and the following 'CF CREATE' there
        # will be two directories for the 'cf' CF: one with the old UUID and one
        # with the new one.
        #
        # Since we can't get a UUID of the cf we will just look for the
        # CF directory without a snapshot we've created.
        #
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, self.SNAPSHOT_NAME)
        logger.debug(f"Column family directory is {cf_dir}")

        logger.info("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        logger.info("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, "upload", f))

        logger.info("Running 'nodetool refresh'...")
        message = r"Loading new SSTables for (ks\.cf\.\.\.|keyspace=ks, table=cf,)"
        self.start_nodetool_and_kill_node(node1, "refresh -- ks cf", message)

        logger.info("Delete commitlogs...")
        commitlog_dir = os.path.join(self.test_path, "test", "node1", "commitlogs")
        commitlog.cleanup(commitlog_dir)

        logger.info("Restart the node...")
        node1.start(wait_for_binary_proto=True)

        logger.info("Running 'nodetool refresh -- ks cf' - after restart...")
        node1.nodetool("refresh -- ks cf")

        logger.info("Checking rows on node1...")
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c1_values, c2_values=c2_values)

    @pytest.mark.single_node
    def test_replay_restore_no_additional_data(self):
        """
        Check that we can restore snapshot files that use old schema

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table
        6. Run 'nodetool refresh'
        7. Check that all data exists
        8. Run 'nodetool refresh'
        9. Check that all data exists
        """
        node1, session = self.prepare()
        keys, c1_values, c2_values = self.create_data(session)

        logger.debug("Creating a snapshot...")
        node1.nodetool(f"snapshot -t {self.SNAPSHOT_NAME} -cf cf -- ks")

        snapshot_dir = self.get_snapshot_dir(self.SNAPSHOT_NAME)
        assert snapshot_dir is not None, f"Can't find a snapshot directory for {self.SNAPSHOT_NAME}"
        logger.debug(f"Snapshot dir is {snapshot_dir}")

        logger.debug("Dropping a keyspace...")
        session.execute(SimpleStatement("DROP KEYSPACE ks"))

        logger.debug("Creating the same keyspace.table...")
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        logger.debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        ks_dir = os.path.join(self.test_path, "test", "node1", "data", "ks")
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, self.SNAPSHOT_NAME)
        logger.debug(f"Column family directory is {cf_dir}")

        logger.debug("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        logger.debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, "upload", f))

        logger.debug("Running 'nodetool refresh -- ks cf' - first take...")
        node1.nodetool("refresh -- ks cf")

        logger.debug("Checking rows on node1...")
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c1_values, c2_values=c2_values)

        logger.debug("Running 'nodetool refresh -- ks cf' - second take...")
        node1.nodetool("refresh -- ks cf")

        logger.debug("Checking rows on node1...")
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c1_values, c2_values=c2_values)

    @pytest.mark.single_node
    def test_restore_snapshot_using_different_smp_setting(self):
        """
        Check that we can restore snapshot files that used a different smp setting

        1. Use a single node with smp=1 and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Stop node, start it with smp=2
        6. Create keyspace + table
        7. Restore data
        8. Check that all data exists
        """
        node1, session = self.prepare(jvm_args=["--smp", "1"])
        keys, c1_values, c2_values = self.create_data(session)

        logger.debug("Creating a snapshot...")
        node1.nodetool(f"snapshot -t {self.SNAPSHOT_NAME} -cf cf -- ks")

        snapshot_dir = self.get_snapshot_dir(self.SNAPSHOT_NAME)
        assert snapshot_dir is not None, f"Can't find a snapshot directory for {self.SNAPSHOT_NAME}"
        logger.debug(f"Snapshot dir is {snapshot_dir}")

        logger.debug("Dropping a keyspace...")
        session.execute(SimpleStatement("DROP KEYSPACE ks"))

        logger.debug("Stopping the node...")
        node1.stop(gently=True)

        logger.debug("Starting a node on two cores...")
        node1.start(wait_for_binary_proto=True, jvm_args=["--smp", "2"])

        logger.debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node1)

        logger.debug("Creating the same keyspace.table...")
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        logger.debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        ks_dir = os.path.join(self.test_path, "test", "node1", "data", "ks")
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, self.SNAPSHOT_NAME)
        logger.debug(f"Column family directory is {cf_dir}")

        logger.debug("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        logger.debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, "upload", f))

        logger.debug("Running 'nodetool refresh -- ks cf'")
        node1.nodetool("refresh -- ks cf")

        logger.debug("Checking rows on node1...")
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c1_values, c2_values=c2_values)

    @pytest.mark.skip_if(with_feature("tablets"))
    @pytest.mark.dtest_debug
    def test_restore_snapshot_using_old_token_ownership(self):
        """
        Check that we can restore snapshot files that use a non updated token ownership

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Add an additional node
        5. Drop keyspace
        6. Create keyspace + table
        7. Restore data
        8. Check that all data exists
        """
        node1, session = self.prepare()
        self.create_data(session)

        logger.debug("Creating a snapshot...")
        node1.nodetool(f"snapshot -t {self.SNAPSHOT_NAME} -cf cf -- ks")

        snapshot_dir = self.get_snapshot_dir(self.SNAPSHOT_NAME)
        assert snapshot_dir is not None, f"Can't find a snapshot directory for {self.SNAPSHOT_NAME}"
        logger.debug(f"Snapshot dir is {snapshot_dir}")

        logger.debug("Staring a new node (node2)...")
        node2 = new_node(self.cluster)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug("Dropping a keyspace...")
        session.execute(SimpleStatement("DROP KEYSPACE ks"))

        logger.debug("Creating the same keyspace.table...")
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        logger.debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        ks_dir = os.path.join(self.test_path, "test", "node1", "data", "ks")
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, self.SNAPSHOT_NAME)
        logger.debug(f"Column family directory is {cf_dir}")

        logger.debug("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        logger.debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, "upload", f))

        logger.debug("Running 'nodetool refresh -- ks cf'")
        node1.nodetool("refresh -- ks cf")

        logger.debug("Check that we may query ks.cf on node1...")
        session.execute(SimpleStatement("SELECT COUNT(*) FROM ks.cf"))

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_incremental_backup(self):
        """
        Check that incremetal backup works as expected

        1. Use a single node
        2. Enable incremental_backup
        3. Create a keyspace + table
        4. Insert data
        5. Check that while sstables are flushed - incremental backups are created
        6. Run compact - forcing all sstables to be merged
        7. Check that a backup contains only the original sstables and only them

        """
        node1, session = self.prepare(jvm_args=["--smp", "1"])
        node1.nodetool("enablebackup")
        self.create_data(session)

        logger.debug("Flushing...")
        node1.nodetool("flush -- ks cf")

        cf_dir = get_node_cf_dir(node1, "ks", "cf")
        logger.debug(f"'cf' directory is {cf_dir}")

        # Save the names of the current sstable files
        sstables_files1 = get_sstables_files(cf_dir)
        logger.debug(f"sstables before compaction: {sstables_files1}")

        # get the names of files in the 'backups' subdir
        backups1_files = get_sstables_files(f"{cf_dir}/backups")
        logger.debug(f"backups before compaction: {backups1_files}")

        assert sstables_files1 == backups1_files, "backup doesn't contain all sstable files"

        logger.debug("Run a compaction...")
        node1.compact()

        sstables_files2 = get_sstables_files(cf_dir)
        logger.debug(f"sstables after compaction: {sstables_files2}")

        backups2_files = get_sstables_files(f"{cf_dir}/backups")
        logger.debug(f"backups after compaction: {backups2_files}")

        # backup should not contain compacted sstables therefore its contents
        # should not change after a compaction
        assert backups1_files == backups2_files, "backup contents changed after a compaction"

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_restore_snapshot_from_cassandra(self):
        """
        Check that we can restore snapshot files that have been created by cassandra

        1. Use a single node and create a keyspace + table
        2. Restore data from a cassandra snapshot
        3. Check that all data exists

        """
        num_keys = 1000
        c1_values = list(map(lambda x: f"{x}", range(num_keys)))
        c2_values = list(map(lambda x: f"{x}", range(num_keys, 2 * num_keys)))
        keys = range(num_keys)

        node1, session = self.prepare(jvm_args=["--smp", "1"])
        node1.nodetool("enablebackup")
        self.create_ks_and_cf(session)

        logger.debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        cassandra_snapshot_dir = f"{os.path.dirname(os.path.realpath(__file__))}/cassandra-sstables/restore-snapshot-from-cassandra"
        logger.debug(f"cassandra snapshot dir is {cassandra_snapshot_dir}")

        cf_dir = get_node_cf_dir(node1, "ks", "cf")
        logger.debug(f"Column family directory is {cf_dir}")

        logger.debug("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        logger.debug("Copy sstables from the snapshot...")
        for f in os.listdir(cassandra_snapshot_dir):
            shutil.copy2(os.path.join(cassandra_snapshot_dir, f), os.path.join(cf_dir, "upload", f))

        logger.debug("Running 'nodetool refresh -- ks cf'")
        node1.nodetool("refresh -- ks cf")

        logger.debug("Checking rows on node1...")
        self.check_rows_on_node(node1, len(keys), found=keys, c1_values=c1_values, c2_values=c2_values)

    @pytest.mark.single_node
    def test_clearsnapshot_options(self):  # noqa: PLR0915
        """
        Check different 'nodetool clearsnapshot' options

        1. Use a single node and create a keyspace ks0 + table cf.
        2. Create a keyspace ks1 + table cf.
        3. Insert data into both tables above.
        4. Create a snapshot snapshot0.
        5. Add more data to both keyspaces and create a snapshot snapshot1.
        6. Add more data to both keyspaces and create a snapshot snapshot2.
        7. Call 'nodetool clearsnapshot -t snapshot0'.
        8. Check that
            1. snapshot0 has been deleted in both keyspaces.
            2. snapshot1 and snapshot2 are still present and haven't been touched.
        9. Call 'nodetool clearsnapshot -t snapshot1 -- ks1' and check that
            1. snapshot1 has been removed from ks1 and not from ks0.
            2. snapshot2 is still present and hasn't been touched.
        10. Call 'nodetool clearsnapshot' and check that all snapshots have been removed.
        """
        node1, session = self.prepare()

        for i in range(2):
            keyspace_name = f"ks{i}"
            logger.debug(f"Creating a keyspace '{keyspace_name}'...")
            create_ks(session, keyspace_name, 1)

            logger.debug("Creating a column family 'cf'...")
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        num_keys = 1000
        start_key = 0

        for i in range(3):
            snapshot_name = f"snapshot{i}"
            c1_values = list(map(lambda x: f"{x}", range(start_key, start_key + num_keys)))
            c2_values = list(map(lambda x: f"{x}", range(start_key + num_keys, start_key + 2 * num_keys)))
            keys = range(start_key, start_key + num_keys)

            logger.debug(f"Inserting concurrently {num_keys} keys into 'ks0.cf' and 'ks1.cf'...")
            insert_c1c2(session, ks="ks0", keys=keys, consistency=ConsistencyLevel.ONE, c1_values=c1_values, c2_values=c2_values)
            insert_c1c2(session, ks="ks1", keys=keys, consistency=ConsistencyLevel.ONE, c1_values=c1_values, c2_values=c2_values)

            logger.debug("Creating a snapshot for 'ks0' and 'ks1'...")
            node1.nodetool(f"snapshot -t {snapshot_name} ks0 ks1")

            start_key = start_key + num_keys

        ks_dir = [None, None]
        for i in range(2):
            ks_dir[i] = os.path.join(self.test_path, "test", "node1", "data", f"ks{i}")

        ks_snapshot_dir = [[None, None, None], [None, None, None]]

        for i in range(2):
            for j in [1, 2]:
                ks_snapshot_dir[i][j] = self.get_snapshot_dir(f"snapshot{j}", ks_dir=ks_dir[i])
                assert ks_snapshot_dir[i][j] is not None, f"Can't find a snapshot directory for 'ks{i}.snapshot{j}'"

        ks_snapshot_files = [[None, None, None], [None, None, None]]
        for i in range(2):
            for j in [1, 2]:
                ks_snapshot_files[i][j] = self.get_all_files_in_dir(ks_snapshot_dir[i][j])

        logger.debug("Call 'nodetool clearsnapshot -t snapshot0'...")
        node1.nodetool("clearsnapshot -t snapshot0")

        # First check that 'snapshot1' has been deleted...
        for i in range(2):
            logger.debug(f"Check that snapshot0 for ks{i} was deleted...")
            test_dir = self.get_snapshot_dir("snapshot0", ks_dir=ks_dir[i])
            assert test_dir is None, f"'ks{i}' snapshot 'snapshot0' has not been deleted!"

        # ...then check that other snapshots are untouched
        for i in range(2):
            for j in [1, 2]:
                logger.debug(f"Check that snapshot{j} for ks{i} was not deleted...")
                test_dir = self.get_snapshot_dir(f"snapshot{j}", ks_dir=ks_dir[i])
                assert test_dir is not None, f"'ks{i}' snapshot 'snapshot{j}' has not been deleted!"
                test_files = self.get_all_files_in_dir(ks_snapshot_dir[i][j])
                assert test_files == ks_snapshot_files[i][j], f"'ks{i}' snapshot 'snapshot{j}' direcotry contents has changed!"

        # Call 'nodetool clearsnapshot -t snapshot1 -- ks1'
        logger.debug("Call 'nodetool clearsnapshot -t snapshot1 -- ks1'")
        node1.nodetool("clearsnapshot -t snapshot1 -- ks1")

        # Check that snapshot1 for ks1 has been deleted...
        logger.debug("Check that snapshot1 for ks1 was deleted...")
        test_dir = self.get_snapshot_dir("snapshot1", ks_dir=ks_dir[1])
        assert test_dir is None, "'ks1' snapshot 'snapshot1' has not been deleted!"

        # ...but not for ks0!
        logger.debug("Check that snapshot1 for ks0 was not deleted...")
        test_dir = self.get_snapshot_dir("snapshot1", ks_dir=ks_dir[0])
        assert test_dir is not None, "'ks0' snapshot 'snapshot1' has been deleted!"
        test_files = self.get_all_files_in_dir(ks_snapshot_dir[0][1])
        assert test_files == ks_snapshot_files[0][1], "'ks0' snapshot 'snapshot1' direcotry contents has changed!"

        # ...then check that snapshot2 is intact
        for i in range(2):
            logger.debug(f"Check that snapshot2 for ks{i} was not deleted...")
            test_dir = self.get_snapshot_dir("snapshot2", ks_dir=ks_dir[i])
            assert test_dir is not None, f"'ks{i}' snapshot 'snapshot2' has not been deleted!"
            test_files = self.get_all_files_in_dir(ks_snapshot_dir[i][2])
            assert test_files == ks_snapshot_files[i][2], f"'ks{i}' snapshot 'snapshot2' direcotry contents has changed!"

        # Call 'nodetool clearsnapshot' and check that all snapshots has been cleared
        logger.debug("Call 'nodetool clearsnapshot'")
        node1.nodetool("clearsnapshot")
        for i in range(3):
            logger.debug(f"Check that snapshot{i} doesn't exist any more...")
            test_dir = self.get_snapshot_dir(f"snapshot{i}")
            assert test_dir is None, f"'snapshot{i}' has not been deleted!"

    @pytest.mark.skip("#7022")
    @pytest.mark.use_cassandra_stress
    @pytest.mark.single_node
    # nodetool refresh does not examine the main directory since
    # refresh was changed to use off-strategy compaction
    # in scylla@7351db7cab7bbf907172940d0bbf8b90afde90ba
    def test_nodetool_refresh_main_sstable_directory(self):
        """
        From 4.1 scylla won't support to refresh from main SSTable directory.
        This test verified that main directory refresh will fail, and only sub-directory refresh will succeed.
        """
        node1, self.cql_session = self.prepare()

        # Prepare test data by cassandra-stress workload
        self.cs_write_and_verify(node1, 1000, seq_start=1, verify_count=1000)

        logger.debug("Creating a snapshot for test table")
        node1.nodetool(f"snapshot -t {self.SNAPSHOT_NAME} -cf standard1 -- keyspace1")
        snapshot_dir = self.get_snapshot_dir(self.SNAPSHOT_NAME)
        cf_dir = get_node_cf_dir(node1, "keyspace1", "standard1")

        # Adding more data to test table
        self.cs_write_and_verify(node1, 1000, seq_start=1001, verify_count=2000)

        # logger.debug("Removing sstables in main SSTable directory")
        self.remove_sstable_and_verify(node1, cf_dir, restart_node=False, delete_commitlogs=False, verify_count=None)

        # Copying snapshot to main SSTable directory
        self.copy_snapshot_and_verify(node1, snapshot_dir, cf_dir, expect_refresh_fail=True, verify_count=2000)

        # Remove the test data and restart the cluster
        self.remove_sstable_and_verify(node1, cf_dir, restart_node=True, delete_commitlogs=True, verify_count=0)

        # Restore data by refreshing the snapshot in sub-directory
        self.copy_snapshot_and_verify(node1, snapshot_dir, os.path.join(cf_dir, "upload"), verify_count=1000)

        # Final read verify
        node1.stress(["read", "n=1000", "no-warmup", "-rate", "threads=2", "-pop", "seq=1...1000"])

    @pytest.mark.single_node
    def test_no_data_resurrection_or_loss_after_restore(self):  # noqa: PLR0915
        """
        Test restore procedure works correctly when we are doing backup of schema by `DESC SCHEMA WITH INTERNALS`.
        Main point is correctness of data when a column is dropped and re-added. Description with internals should
        contains `ALTER TABLE ks.cf DROP c2 USING TIMESTAMP <timestamp>` followed by `ALTER TABLE ks.cf ADD c2 text`.
        This allow to properly interpret data without data loss or data resurrection

        1. Single node, table (key, c1, c2) with disabled compaction
        2. Insert data part 1
        3. Flush the table - we want to save the data on disk before dropping the column
        4. Drop column c2 and re-add it
        5. Insert data part 2
        6. Make backup of schema (by doing `DESC SCHEMA WITH INTERNALS`) and sstables (copy them to tmp dir)
        7. Drop keyspace and remove its whole data directory
        8. Restore schema using saved description
        9. Validate new `DESC SCHEMA WITH INTERNALS` looks the same as the old one
        10. Restore sstables
        11. Validate the data is ok
            - c2 column in rows from part 1 should all have `Null`
            - c2 column in rows from part 2 should contain values corresponding to their keys
        """
        node1, session = self.prepare()
        num_keys = 10
        keys, _c1_values, _c2_values = self.create_data(session, num_keys=num_keys)
        node1.flush()

        logger.debug("Dropping and re-adding column c2")
        session.execute("ALTER TABLE ks.cf DROP c2")
        session.execute("ALTER TABLE ks.cf ADD c2 text")

        keys = range(num_keys)
        logger.debug("Inserting concurrently %d keys after re-adding column...", num_keys)
        readded_offset = 1000
        new_keys = range(num_keys, num_keys + num_keys)
        new_c1_values = [f"{x}" for x in range(readded_offset, readded_offset + num_keys)]
        new_c2_values = [f"{x}" for x in range(readded_offset + num_keys, readded_offset + num_keys * 2)]
        insert_c1c2(session, keys=new_keys, consistency=ConsistencyLevel.ONE, c1_values=new_c1_values, c2_values=new_c2_values)
        node1.flush()

        logger.debug("Saving table's sstables and schema")
        tmp_dir = safe_mkdtemp()
        node1.stop()
        sstables = get_list_of_sstables(node1, "ks", "cf", suffix="")
        logger.debug(f"{sstables=}")
        for f in sstables:
            shutil.copy2(f, tmp_dir)
        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        schema = session.execute("DESC KEYSPACE ks WITH INTERNALS").all()

        logger.debug("Removing keyspace and clearing the data")
        session.execute("DROP KEYSPACE ks")
        node1.stop()
        shutil.rmtree(os.path.join(node1.get_path(), "data", "ks"))
        node1.start(wait_for_binary_proto=True)

        logger.debug("Restoring schema from description")
        session = self.patient_cql_connection(node1)
        for element in schema:
            for cs in element.create_statement.split(";"):
                if cs.strip():
                    session.execute(cs)

        logger.debug("Validating restored schema looks the same as original one")
        restored_schema = session.execute("DESC KEYSPACE ks WITH INTERNALS").all()
        assert schema == restored_schema

        logger.debug("Restoring sstables")
        node1.stop()
        cf_dir = get_node_cf_dir(node1, "ks", "cf")
        copy_files_to(tmp_dir, cf_dir)
        node1.start(wait_for_binary_proto=True)

        logger.debug("Validating the table contains correct data")
        session = self.patient_cql_connection(node1)
        result = session.execute("SELECT key, c1, c2 FROM ks.cf").all()
        old_keys = set([f"k{i}" for i in keys])  # keys where c2 should contain null as the columns was dropped
        assert len(result) == num_keys * 2
        for row in result:
            if row.key in old_keys:
                assert row.c2 is None
            else:
                assert row.c2 is not None
                assert row.c2 == f"{readded_offset + int(row.key[1:])}"

    # ######################## Helper functions ####################################

    def prepare(self, jvm_args=None):
        """Prepare single node test cluster, with disabled hinted handoff and set batch commit log"""

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        logger.debug("Starting a cluster of one node...")
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]

        logger.debug("Creating a CQL connection...")
        return node1, self.patient_cql_connection(node1)

    def create_ks_and_cf(self, session):
        """Creates one keyspace (ks), one column family (cf)"""
        logger.info("Creating a keyspace 'ks'...")
        create_ks(session, "ks", 1)

        logger.info("Creating a column family 'cf'...")
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

    def create_data(self, session, num_keys=1000):
        """Creates one keyspace (ks), one column family (cf) with c1/c2 columns and inserts 1000 keys"""
        self.create_ks_and_cf(session)

        c1_values = [str(number) for number in range(num_keys)]
        c2_values = [str(number) for number in range(num_keys, 2 * num_keys)]
        keys = list(range(num_keys))

        logger.info(f"Inserting concurrently {len(keys)} keys...")
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE, c1_values=c1_values, c2_values=c2_values)
        return keys, c1_values, c2_values

    def cs_write_and_verify(self, node, n=1000, seq_start=1, verify_count=None):
        """Add test data by cassandra-stress workload"""

        logger.debug("Adding data by cassandra-stress workload")
        node.stress(["write", f"n={n}", "no-warmup", "-rate", "threads=2", "-pop", f"seq={seq_start}...{seq_start + n - 1}"])
        node.flush()
        if verify_count is not None:
            # Verify the data is added to cluster
            rows = list(self.cql_session.execute("SELECT * from keyspace1.standard1"))
            assert verify_count == len(rows)

    def copy_snapshot_and_verify(self, node, snapshot_dir, dest_dir, expect_refresh_fail=False, verify_count=None):
        """Copy snapshot files to an assigned directory, then try to refresh the test table"""

        logger.debug(f"Copying the snapshot to {dest_dir}, and restore the data by refreshing")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(dest_dir, f))

        expected_error = r"Loading SSTables from the main SSTable directory is unsafe and no longer supported"
        try:
            self.ignore_log_patterns.append(expected_error)
            node.nodetool("refresh -- keyspace1 standard1")
            if expect_refresh_fail:
                raise Exception("Refresh in main directory succeeded unexpectedly! It's no longer supported from 4.1")
        except NodetoolError as error:
            if expect_refresh_fail:
                logger.debug(f"Refresh failed as expected, error:\n{error}")
                assert re.search(expected_error, str(error)), f"Expected error is not found, expected error:\n{expected_error}"
            else:
                raise error

        if verify_count is not None:
            rows = list(self.cql_session.execute("SELECT * from keyspace1.standard1"))
            assert verify_count == len(rows)

    def remove_sstable_and_verify(self, node, cf_dir, restart_node=False, delete_commitlogs=False, verify_count=None):
        """The original sstable files should be removed before copying snapshot"""

        if restart_node:
            logger.debug("Kill the node ...")
            node.stop(gently=False)
        logger.debug("Removing sstables in main directory ...")
        self.delete_cf_sstables(cf_dir)

        if delete_commitlogs:
            logger.debug("Delete commitlogs ...")
            commitlog_dir = os.path.join(self.test_path, "test", "node1", "commitlogs")
            commitlog.cleanup(commitlog_dir)
        if restart_node:
            logger.debug("Restart the node ...")
            node.start(wait_for_binary_proto=True)
            logger.debug("Re-Creating a CQL connection after restart...")
            self.cql_session = self.patient_cql_connection(node)

        if verify_count is not None:
            rows = list(self.cql_session.execute("SELECT * from keyspace1.standard1"))
            assert verify_count == len(rows)

    def get_all_files_in_dir(self, dir_path):
        """
        Returs a set of all files in the given directory
        """
        dir_files = set()
        for f in os.listdir(dir_path):
            full_name = os.path.join(dir_path, f)
            if os.path.isfile(full_name):
                dir_files.add(f)

        return dir_files

    def delete_cf_sstables(self, cf_dir):
        for f in os.listdir(cf_dir):
            full_name = os.path.join(cf_dir, f)
            if os.path.isfile(full_name):
                os.remove(full_name)

    def get_snapshot_dir(self, snapshotname, ks_dir=None):
        search_base_dir = None
        if ks_dir is None:
            search_base_dir = self.test_path
        else:
            search_base_dir = ks_dir

        for root, dirs, files in os.walk(search_base_dir):
            for name in dirs:
                if name == snapshotname:
                    return os.path.join(root, name)

        return None

    # Return the first CF directory that doesn't have a snapshot with a given tag
    def get_non_snapshot_cf_dir(self, ks_dir, snapshotname):
        for root, dirs, files in os.walk(ks_dir):
            for d in dirs:
                if not os.path.isdir(os.path.join(root, d, "snapshots", snapshotname)):
                    return os.path.join(root, d)
            break

        return None

    def start_nodetool_and_kill_node(self, node, cmd, message=None):
        def run():
            try:
                logger.debug(f"Starting nodetool {cmd}...")
                node.nodetool(cmd)
                logger.debug(f"nodetool {cmd} done")
            except:
                logger.debug(f"nodetool {cmd} killed")

        executor = ThreadPoolExecutor(max_workers=1)
        nodetool_thread = executor.submit(run)
        if message:
            logger.debug(f"Watch log for '{message}")
            node.watch_log_for(message)
        random.seed()
        wait_time = random.random()

        logger.debug(f"Wait for {wait_time} seconds")
        time.sleep(wait_time)

        logger.debug("Killing a node...")
        node.stop(gently=False)

        nodetool_thread.result()

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, c1_values=None, c2_values=None):  # noqa: PLR0913
        s = self.patient_cql_connection(node_to_check, "ks")
        query = "SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        result = list(s.execute(statement))
        assert result[0].count == rows

        if found is not None:
            if c1_values is None:
                c1_values = ["value1"] * len(found)

            if c2_values is None:
                c2_values = ["value2"] * len(found)

            query_c1c2_concurrent(session=s, keys=found, c1_values=c1_values, c2_values=c2_values, consistency=ConsistencyLevel.ONE)

        if missings is not None:
            query_c1c2_concurrent(session=s, keys=missings, consistency=ConsistencyLevel.ONE, must_be_missing=True)
