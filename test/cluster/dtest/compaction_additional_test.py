import datetime
import glob
import itertools
import logging
import multiprocessing
import os
import random
import re
import shutil
import string
import time
import uuid
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime as dt
from operator import attrgetter
from pathlib import Path
from pprint import pformat
from textwrap import dedent
from typing import Any

import pytest
from cassandra import ConsistencyLevel, concurrent
from cassandra.cluster import Session
from ccmlib.node import Node, NodetoolError, TimeoutError
from ccmlib.scylla_cluster import ScyllaCluster, ScyllaNode
from deepdiff import DeepDiff

from dtest_class import Tester, create_cf, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_all, assert_none, assert_row_count
from tools.cluster import new_node, run_rest_api
from tools.cluster_topology import generate_cluster_topology
from tools.context import disable_autocompaction, disable_sstable_modifications
from tools.context import disable_load_balancing as disable_load_balancing_ctx
from tools.data import (
    create_c1c2_table,
    delete_c1c2,
    insert_c1c2,
    run_in_parallel,
    simulate_write_process_in_minutes,
)
from tools.files import (
    check_file_lists_are_equal,
    copy_files_to,
    get_list_of_sstables,
    get_node_cf_dir,
    get_sstables_files,
)
from tools.marks import issue_open, unmark, with_feature
from tools.misc import ImmutableMapping, dump_sstables
from tools.rest_clients import StorageServiceClient
from tools.scylla_defines import CompactionStrategy
from tools.stress import fill_data_by_cs

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.fixture(scope="function", autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    dtest_setup_overrides = DTestSetupOverrides()
    dtest_setup_overrides.cluster_options = ImmutableMapping(
        {
            "start_rpc": "true",
            "logger_log_level": {"compaction": "debug"},  # so we see compaction start/end log messages
        }
    )
    return dtest_setup_overrides


def generate_ids(val):
    if hasattr(val, "marks"):  # it's a pytest ParameterSet
        return f"{val.values[0]['class']}"
    return f"{val['class']}"


SpanningSStable = namedtuple("SpanningSStable", ["is_spanning_one_window", "min_timestamp_seconds", "max_timestamp_seconds"])


class CompactionAdditionalTester(Tester):
    def prepare(self, nodes: int, wait_for_binary_proto=True, jvm_args=None, configuration_options=None) -> tuple[list[ScyllaNode], Session]:
        if configuration_options is None:
            configuration_options = {}
        self.debug_mode = type(self.cluster) is ScyllaCluster and self.cluster.scylla_mode == "debug"
        configuration_options.update({"enable_sstable_key_validation": True})
        configuration_options.update({"tablets_initial_scale_factor": 1})  # Tests assume 1 compaction group per shard
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        node1 = self.cluster.nodelist()[0]
        return self.cluster.nodelist(), self.patient_cql_connection(node1)

    @staticmethod
    def get_stats(node, statistics_file: str, keyspace_name: str, table_name: str):
        """parse one statistics file"""
        metadata = node.dump_sstable_stats(keyspace=keyspace_name, column_family=table_name, datafiles=[statistics_file])
        assert len(metadata) == 1
        return next(iter(metadata.values()))["stats"]

    @staticmethod
    def micros_to_seconds(micros):
        return micros // (1000 * 1000)

    @staticmethod
    def seconds_to_micros(seconds):
        return seconds * 1000 * 1000


def get_strategies_upgrade_options() -> list[Any]:
    _strategies = [
        # Expect sstables are more than min_threshold in level 0
        {"class": "LeveledCompactionStrategy", "sstable_size_in_mb": 1, "min_threshold": 2},
        # Expect sstables are generated in multiple minutes for TimeWindowCompactionStrategy
        {"class": "TimeWindowCompactionStrategy", "compaction_window_size": 1, "compaction_window_unit": "MINUTES", "min_threshold": 2},
        # Expect there are more sstables than min_threshold in same bucket
        {"class": "SizeTieredCompactionStrategy", "bucket_high": 1.5, "bucket_low": 0.5, "min_sstable_size": 1, "min_threshold": 2},
        # DateTieredCompactionStrategy is deprecated
        # {'class': 'DateTieredCompactionStrategy'},
        # disabling for now, until we can figure when reshaping is expected in this case
        # scylladb/scylladb#9944 would help with that
        # {'class': 'IncrementalCompactionStrategy'}
    ]
    return list(itertools.product(_strategies, _strategies))


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCompactionAdditional(CompactionAdditionalTester):
    SSTABLE_PREFIX_REG_EXPR = "m[c-est]|n[a-b]|o[a]|d[a]"
    REG_EXPR_TEMPLATE = (
        r"\[shard (?P<run_shard>\d+)(?::\w+)?\] compaction - \[.* {ks}\.{table} (?P<run_task_id>.*)\] "
        r"((?P<compaction_type>Compacting|Cleaning) \[(?P<sstables>.*\/{ks}\/{table}-.*\/(?:%s)-.*))|"
        r"\[shard (?P<stop_shard>\d+)(?::\w+)?\] compaction - \[Compact {ks}\.{table} (?P<stop_task_id>.*)\] "
        r"Compacting of .* (?P<interrupt>interrupted due) to: .* user-triggered operation" % SSTABLE_PREFIX_REG_EXPR
    )

    @staticmethod
    def validate_tombstones_in_sstables(node: ScyllaNode):
        """
        Get sstable content in json format and validate that tombstones do not keep row data
        """
        jsoninfo = dump_sstables(node, "ks", "test")

        tombstones_found = False
        for partition in jsoninfo:
            # Expected deleted row details format in sstable:
            # {
            #   'key': {'token': '-4069959284402364209',
            #           'raw': '000400000001',
            #           'value': '1'},
            #   'tombstone': {'timestamp': 1690533264324595,
            #                 'deletion_time': '2023-07-28 08:34:24z'}
            # },
            if "tombstone" in partition:
                tombstones_found = True
                assert "clustering_elements" not in partition, f"Unexpectedly found row data in deleted partition: {partition}"

        assert tombstones_found, "Tombstones were not found"

    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_compaction_delete_with_smp_change(self):  # noqa: PLR0915
        """
        Test that data is not resurected when shared sstables
        are used
        1. smp=1 create sstable A with 100 keys
        2. shutdown
        3. boot with smp=2 (forcing step 1 sstables to be shared) delete all keys
        4. wait past gc_preiod
        5. insert a key forcing flush multiple times till a compaction is triggered
        6. stop and start the node
        7. check that no data was resurected and that some of the deletion markers still exist
        8. insert additional 100 keys forcing a flush multiple times till multiple compactions are trigerred
        9. check that no deletion marker is left and files have been removed
        """
        logger.debug("Starting node1 with 1 cpu")
        [node1], session = self.prepare(1, jvm_args=["--smp", "1"])
        create_ks(session, "ks", 1)

        gc_grace_seconds = 5
        keys = 100
        logger.debug(f"Inserting {keys} keys with gc_grace_seconds={gc_grace_seconds}")
        session.execute(f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'SizeTieredCompactionStrategy'}} and gc_grace_seconds = {gc_grace_seconds};")

        for x in range(keys):
            session.execute(f"insert into cf (key, val) values ({x},1)")

        node1.flush()
        node1.compact()
        logger.debug("Restarting node1 with 2 cpus")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=["--smp", "2"])

        session = self.patient_cql_connection(node1, "ks")

        def compactions_count():
            rows = session.execute("select count(*) from system.compaction_history where keyspace_name='ks' and columnfamily_name='cf' allow filtering")
            return rows.one()[0]

        compactions_1 = compactions_count()
        compactions_2 = compactions_1

        logger.debug(f"Deleting {keys} keys")
        for x in range(keys):
            session.execute(f"delete from cf where key = {x}")
        node1.flush()

        logger.debug(f"Waiting gc_grace_seconds={gc_grace_seconds} to pass")
        time.sleep(gc_grace_seconds + 1)

        # we passed gc_period and force an update so that compaction will
        # be triggered on a single shard (removing data and tombstone)
        logger.debug("Inserting data and waiting for new compaction")
        while compactions_1 == compactions_2:
            session.execute(f"insert into ks.cf (key, val) values ({keys + 1},1);")
            node1.flush()
            compactions_2 = compactions_count()
        node1.wait_for_compactions()

        compactions_2 = compactions_count()
        num_compactions = compactions_2 - compactions_1
        logger.debug(f"{num_compactions} compaction(s) completed")

        # reboot and verify that data  is not resurected
        logger.debug("Stopping node1")
        node1.stop(gently=False)

        # verify that only some deletion markers will be kept since we reshard the files
        # and gc_period passed so some tombstones have been removed by compaction
        jsoninfo = dump_sstables(node1, "ks", "cf")
        node1.info(jsoninfo)

        numfound = sum("tombstone" in partition for partition in jsoninfo)
        logger.debug("{} keys are now marked_deleted (0 {} expected < {})".format(numfound, "<" if num_compactions < 2 else "<=", keys))
        assert numfound <= keys, f"Number of found tombstones {numfound} greater than number of keys {keys}"
        if num_compactions < 2:
            assert numfound > 0, f"Number of found tombstones {numfound} != 0"

        logger.debug("Restarting node1")
        node1.start(wait_for_binary_proto=True, jvm_args=["--smp", "2"])
        session = self.patient_cql_connection(node1, "ks")
        logger.debug("Verify that no data was resurrected")
        for x in range(keys):
            assert_none(session, f"select * from cf where key = {x}")

        # trigger compaction on both shards
        logger.debug("Waiting for compaction")
        node1.wait_for_compactions()
        compactions_1 = compactions_count()
        compactions_2 = compactions_1

        logger.debug("Wait and force new compaction")
        while compactions_1 + 2 > compactions_2:
            time.sleep(gc_grace_seconds)
            node1.flush()
            node1.compact(keyspace="ks")
            compactions_2 = compactions_count()
        node1.wait_for_compactions()

        # validate that all deletion markers have been removed
        jsoninfo = dump_sstables(node1, "ks", "cf")
        node1.info(jsoninfo)

        numfound = sum("tombstone" in partition for partition in jsoninfo)
        logger.debug(f"{numfound} keys are now marked_deleted (Excpecting 0)")
        assert numfound == 0, "Not all tombstones were removed during compactions"

    @staticmethod
    def prepare_schema_and_data_for_memtable_tests(session: Session, rf: int, rows_amount: int, deleted_keys: int):
        create_ks(session=session, name="ks", rf=rf)
        logger.debug("Create test table")
        session.execute("CREATE TABLE test (i int PRIMARY KEY, t text)")

        insert_stmt = session.prepare("INSERT INTO test (i, t) values(?, 'skdjhdskjh')")
        logger.debug(f"Insert {rows_amount} rows")
        concurrent.execute_concurrent_with_args(session, insert_stmt, [[k] for k in range(rows_amount)])

        delete_stmt = session.prepare("DELETE FROM test where i = ?")
        logger.debug(f"Delete {deleted_keys} rows")
        concurrent.execute_concurrent_with_args(session, delete_stmt, [[k] for k in range(deleted_keys)])

    def test_compact_tombstones_when_memtable_flush_one_node(self):
        """
        Test for commit :
        https://github.com/scylladb/scylla/commit/bcadd8229b0345c50494e33ed4b3b7e3bd21cd85

        1. Create one node cluster
        2. Create test keyspace and table
        3. Insert 100 rows
        4. Delete 10 rows
        5. Flush
        6. Validate that no row data for deleted rows
        7. Validate that the table has 90 rows
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        rows_amount = 100
        deleted_keys = 10
        self.prepare_schema_and_data_for_memtable_tests(session=session, rf=1, rows_amount=rows_amount, deleted_keys=deleted_keys)
        logger.debug(f"Run 'nodetool flush'")
        node1.flush()

        self.validate_tombstones_in_sstables(node1)

        actual_rows_after_flush = next(iter(session.execute("select count(*) from test"))).count
        expected_row_after_flush = rows_amount - deleted_keys
        assert actual_rows_after_flush == expected_row_after_flush, f"Expected {expected_row_after_flush} rows after flush, but actually got {actual_rows_after_flush}"

    def test_compact_tombstones_when_memtable_flush_one_node_stopped(self):
        """
        Test for commit :
        https://github.com/scylladb/scylla/commit/bcadd8229b0345c50494e33ed4b3b7e3bd21cd85

        Run rows deletion when one node stopped

        1. Create 3 nodes cluster
        2. Stop node2
        3. Create test keyspace and table
        4. Insert 100 rows
        5. Delete 10 rows
        6. Flush
        7. Validate that no row data for deleted rows
        8. Start node2 and repair
        7. Validate that the table has 90 rows
        """
        self.cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)

        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        node2.stop(wait_other_notice=True)

        rows_amount = 100
        deleted_keys = 10
        self.prepare_schema_and_data_for_memtable_tests(session=session, rf=3, rows_amount=rows_amount, deleted_keys=deleted_keys)

        logger.debug(f"Run 'nodetool flush'")
        node1.flush()
        node3.flush()

        self.validate_tombstones_in_sstables(node1)
        self.validate_tombstones_in_sstables(node3)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.repair()

        actual_rows_after_flush = next(iter(session.execute("select count(*) from test"))).count
        expected_row_after_flush = rows_amount - deleted_keys
        assert actual_rows_after_flush == expected_row_after_flush, f"Expected {expected_row_after_flush} rows after flush, but actually got {actual_rows_after_flush}"

    @pytest.mark.single_node
    @pytest.mark.parametrize("timestamp_resolution", ["MILLISECONDS"])
    def test_compact_data_by_time_window(self, timestamp_resolution):
        """
        1. Create TABLE with compaction_window_size of 1 MINUTES
        2. Insert data for 4 minutes while flushing to disk.
        3. Insert more data for 2 mins while flushing to disk
        4. Verify that the previous files created and compacted still exist.
        (Otherwise it means they were compacted wrongly).
        """
        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(1)

        window_size_mins = 1

        session = self.patient_cql_connection(node1)
        key_space_name = "ks_" + timestamp_resolution.lower()
        logger.debug("Creating keyspace '%s'..." % key_space_name)
        create_ks(session, key_space_name, 1)

        logger.debug("Creating a column family 'cf' with TWCS")
        create_cf(
            session,
            "cf",
            columns={"c1": "text", "c2": "text"},
            compaction={"compaction_window_size": window_size_mins, "compaction_window_unit": "MINUTES", "timestamp_resolution": timestamp_resolution, "class": "TimeWindowCompactionStrategy"},
        )

        # Wait for new minute to start before inserting data - keep the test consistent
        self.wait_for_new_minute()

        # Write data for x4 time than the window_size (i.e. 4 mins) - to have 4 different windows.
        for minute in range(window_size_mins * 4):
            # Assuming writing the files take LESS than a MINUTE
            self.write_n_data_files(node=node1, session=session, key_space=key_space_name, num_of_files=7, num_of_keys=10)
            self.wait_for_new_minute()

        # Get list of sstables names
        cf_dir = get_node_cf_dir(node1, key_space_name, "cf")
        sstables_files1 = get_sstables_files(cf_dir, f_type="Data")
        logger.debug(f"Files BEFORE: {sstables_files1}")
        assert len(sstables_files1) > 0, "No SSTable files found in %s!" % cf_dir
        # Write additional data for 2 times the window-size (i.e. 2 mins)
        # (to verify that the original files remain the same and aren't compacted).
        for minute in range(window_size_mins * 2):
            # Assuming writing the files take LESS than a MINUTE
            self.write_n_data_files(node=node1, session=session, key_space=key_space_name, num_of_files=7, num_of_keys=10)
            self.wait_for_new_minute()

        # Get list of sstables names
        sstables_files2 = get_sstables_files(cf_dir, f_type="Data")
        logger.debug(f"Files AFTER adding data: {sstables_files2}")

        assert sstables_files1.issubset(sstables_files2), f"some of the original sstables are missing. Possibly due to wrong compactionExpecting {sstables_files1} but Found {sstables_files2}"

    @pytest.mark.single_node
    def test_major_compaction_with_several_timewindows(self):
        """
        Test major compaction will not bundle sstables from different time windows
        Test each time window (after major compaction) has only one table
        """
        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(
            1,
            configuration_options={
                "tablets_initial_scale_factor": 1,
            },
        )

        logger.debug("Creating keyspace 'ks'...")
        min_threshold = 7
        create_ks(session, "ks", 1)
        create_cf(
            session,
            "cf",
            columns={"c1": "text", "c2": "text"},
            compaction={"compaction_window_size": "1", "compaction_window_unit": "MINUTES", "class": "TimeWindowCompactionStrategy", "expired_sstable_check_frequency_seconds": "60", "min_threshold": min_threshold},
        )

        # Write data in different time windows
        num_of_keys = 1000 * random.randint(1, 10)
        first_key = 0
        start = time.time()
        duration = random.randint(70, 120)
        logger.debug(f"Will load data for {duration} seconds...")
        while time.time() - start < duration:
            logger.debug(f"Inserting keys {first_key}..{first_key + num_of_keys - 1}")
            insert_c1c2(session, keys=list(range(first_key, first_key + num_of_keys)), ks="ks")
            node1.flush()
            first_key += num_of_keys // 2

        def _get_time_window(timestamp):
            return int(timestamp / 60)

        number_of_time_windows = _get_time_window(time.time()) - _get_time_window(start) + 1

        node1.stop()

        def _get_sstables_per_timewindow_dict(node, cf_dir):
            # get sstables for each time window dictionary
            time_window_dict = {}
            statistics_files = get_sstables_files(cf_dir, f_type="Statistics")

            for sf in statistics_files:
                stats = self.get_stats(node, os.path.join(cf_dir, sf), keyspace_name="ks", table_name="cf")
                min_time_window = _get_time_window(self.micros_to_seconds(stats["min_timestamp"]))
                max_time_window = _get_time_window(self.micros_to_seconds(stats["max_timestamp"]))
                logger.debug("sf={} min_timestamp={} max_timestamp={} min_time_window={} max_time_window={}".format(sf, stats["min_timestamp"], stats["max_timestamp"], min_time_window, max_time_window))
                for time_window in range(min_time_window, max_time_window + 1):
                    if time_window not in time_window_dict:
                        time_window_dict[time_window] = [sf]
                    else:
                        time_window_dict[time_window].append(sf)
            return time_window_dict

        # save sstable data (before major compaction
        cf_dir = get_node_cf_dir(node1, "ks", "cf")
        time_window_dict_before_major_compaction = _get_sstables_per_timewindow_dict(node1, cf_dir)
        logger.debug(f"time_window_dict_before_major_compaction={time_window_dict_before_major_compaction}")

        # another time window may sneak in if we cross the 1-minute window in one of the sstables
        time_windows_before_major_compaction = len(time_window_dict_before_major_compaction.keys())
        assert time_windows_before_major_compaction >= number_of_time_windows - 1, f"Time window {time_windows_before_major_compaction} less than number of time windows {number_of_time_windows - 1}"
        assert time_windows_before_major_compaction <= number_of_time_windows + 1, f"Time window {time_windows_before_major_compaction} greater than number of time windows {number_of_time_windows - 1}"

        # Run major compaction
        node1.start()
        node1.compact()
        node1.wait_for_compactions()

        sstables_files_after_major_compaction = get_sstables_files(cf_dir, f_type="Data")
        time_window_dict_after_major_compaction = _get_sstables_per_timewindow_dict(node1, cf_dir)
        logger.debug(f"time_window_dict_after_major_compaction={time_window_dict_after_major_compaction}")

        # no new data and consequently, time windows, are expected
        # verify that major compaction didn't mess any time windows
        time_windows_after_major_compaction = len(time_window_dict_after_major_compaction.keys())
        assert time_windows_before_major_compaction == time_windows_after_major_compaction

        # For each time window after major compaction:
        # - It must contain at least one SSTable (statistics file).
        # - It cannot contain more SSTables than the number of shards.
        # This implies that data for a window on a shard is compacted into one SSTable,
        # and not all shards necessarily have data for every window.
        number_of_shards = getattr(node1, "_smp", 1)
        for window, sstables_in_window in time_window_dict_after_major_compaction.items():
            assert 0 < len(sstables_in_window) <= number_of_shards, f"For time window {window}, expected 1 to {number_of_shards} SSTables, but found {len(sstables_in_window)}"

    @pytest.mark.single_node
    def test_compaction_removes_ttld_data_by_time_windows(self):
        """
        Test that TWCS compaction removes TTLd data after gc_period by time windows
        2. Create a table with a DEFAULT TTL=70 and gc_period=10.
        3. Insert data into the table.
        4. Wait past ttl and gc_period
        5. write some data and force compaction
        6. check that ttl'd data was removed
        """

        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(1)

        TIME_TO_SLEEP_BETWEEN_FILES = 15
        NUMBER_OF_FILES = 11
        NUMBER_OF_KEYS = 10
        TTL = 70
        GC_GRACE = 10

        logger.debug("Creating keyspace 'ks'...")
        create_ks(session, "ks", 1)

        # DEFAULT TTL set to 70, gc_grace set to 10 and expiry check set to 60.
        # It means that every 60 seconds it should purge all sstabls that are older than 180+30
        logger.debug(f"Creating a column family 'cf' with TWCS and DEFAULT TTL of {TTL}")
        create_cf(
            session,
            "cf",
            gc_grace=GC_GRACE,
            columns={"c1": "text", "c2": "text"},
            default_ttl=TTL,
            compaction={"compaction_window_size": "1", "compaction_window_unit": "MINUTES", "class": "TimeWindowCompactionStrategy", "expired_sstable_check_frequency_seconds": "60"},
        )

        # Always start the test at th beginning of the minute for consistent results
        self.wait_for_new_minute()

        for t in range(NUMBER_OF_FILES):
            logger.debug(f"Inserting concurrently {NUMBER_OF_KEYS} keys...")
            insert_c1c2(session, n=NUMBER_OF_KEYS, consistency=ConsistencyLevel.ONE)
            node1.flush()
            time.sleep(TIME_TO_SLEEP_BETWEEN_FILES)

        node1.flush()
        cf_dir = get_node_cf_dir(node1, "ks", "cf")
        logger.debug(f"'cf' directory is {cf_dir}")

        # Save the names of the current sstable files
        sstables_files1 = get_sstables_files(cf_dir, f_type="Data")
        logger.debug(f"sstables BEFORE SLEEP: {sstables_files1}")

        logger.debug(f"Sleep for {TTL + GC_GRACE} seconds (TTL + GC) to let the files to completly TTL'ed")
        time.sleep(TTL + GC_GRACE)

        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, f_type="Data")
        logger.debug(f"sstables AFTER SLEEP: {sstables_files2}")

        # Even after the TTL+GC time has passed, the sstables remains till new data is inserted.
        # This assert just verifies that the files are still there.
        assert set(sstables_files1) == set(sstables_files2), f"Some or ALL of the files MISSING: {set(sstables_files1) - set(sstables_files2)}"

        logger.debug(f"Orig files {sstables_files1.intersection(sstables_files2)} havn't been purged yet(expected)")

        mark = node1.mark_log()
        # Insert one key to trigger a sstable expiration check (expired_sstable_check_frequency_seconds': '60').
        insert_c1c2(session, n=10, consistency=ConsistencyLevel.ONE)
        node1.flush()
        node1.wait_for_compactions()
        # CHECK log: should have something like:
        # "Compacted 2 sstables to []. 36623 bytes to 0 (~0% of original) in 2ms = 0.00MB/s.
        #  ~512 total partitions merged to 0."
        found = node1.watch_log_for(r"Compact ks.cf .* Compacted [0-9]+ sstables to \[\]", timeout=5, from_mark=mark)
        logger.debug(found)
        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, f_type="Data")
        logger.debug(f"sstables AFTER INSERT more data and EXPIRATION OF older sstables: {sstables_files2}")

        unpurged_files = set(sstables_files1).intersection(sstables_files2)
        assert not unpurged_files, f"PROBLEM Some of original files are still there and were NOT PURGED: {unpurged_files}"

        logger.debug(f"Purge SUCCEEDED, original files are not there {sstables_files2}")

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    @unmark.next_gating
    @pytest.mark.parametrize("strategy1,strategy2", get_strategies_upgrade_options(), ids=generate_ids)
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("scylladb/scylladb#16739"))
    def test_refresh_and_restart_after_compaction_strategy_change(self, strategy1, strategy2):  # noqa: PLR0915
        """
        This test tries to load backup sstable by refresh and restart after changing the compaction strategy.
        refreshing loads sstable from upload directory, and sstable in staging or main sstable directory will
        be loaded in cf populating during restart.

        Reshaping will only be triggered conditionally if current compaction strategy isn't satisfied.

        LeveledCompactionStrategy:
        - level 0 has more sstables than min_threshold (strict mode) or max_compaction_threshold (relax_mode) (covered in the test)
        - have 10% overlapping sstables on same level
        - sstable level out of MAX level (9)

        SizeTieredCompactionStrategy:
        - have more than min_threshold similar-sized SSTables

        TimeWindowCompactionStrategy:
        - have sstables that span more than 1 window
        - a given window has more than min_threshold SSTables.
        - Time-Window Compaction Strategy compacts SSTables within each time window
          using Size-tiered Compaction Strategy (STCS)

        DateTieredCompactionStrategy:
        - doesn't support reshaping
        """
        cluster = self.cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.start(wait_for_binary_proto=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("DROP KEYSPACE IF EXISTS keyspace1")

        logger.debug(f"Create test table with {strategy1}")
        node1.stress(["write", "n=0", "no-warmup", "-schema", "replication(factor=1)", "-rate", "threads=1"])

        session.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy1}")

        logger.debug("Insert test data by cassandra-stress and compact")
        # Use multiple workloads to generate multiple sstables, then it's easy to reach the threshold for reshaping

        fill_data_by_cs(node1, n_range=[500, 550, 600, 650])
        # Compact initially, make sure there are some compacted sstables before disable autocompaction
        node1.compact()

        # Here we disable autocompaction for leaving all sstables in level 0, then
        # it's easy to trigger reshape with small dataset during restart (strict mode).
        # Actually it's not always necessary.
        #
        # Refreshing from upload will use strict mode reshape, restart population
        # from staging or main sstable directory will use relax mode reshape.
        # Only in relaxed mode, all sstables will be mutated to level to 0, reshaping
        # will be trigger very easily.

        logger.debug("disable autocompaction to leave all sstables in level 0")
        node1.nodetool("disableautocompaction keyspace1 standard1")

        logger.debug("Insert test data by cassandra-stress without compacting, leave it for next strategy")

        if strategy2["class"] == "TimeWindowCompactionStrategy":
            # Prepare a sstable spans two 1 window, (window unit is 60 seconds)
            fill_data_by_cs(node1, n_range=[], duration_range=[70], other_opt=["-rate", "threads=1", "-col", "size=FIXED(1024)"])
        elif strategy2["class"] == "LeveledCompactionStrategy":
            # Need more than 10% overlapping sstables on same level
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650, 1000], start=5000)
        elif strategy2["class"] == "SizeTieredCompactionStrategy":
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650, 2000, 5000] * 2, start=10000)
        else:
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650], start=5000)

        cf_dir = get_node_cf_dir(node1, "keyspace1", "standard1", latest=True)
        logger.debug(cf_dir)

        # For troubleshot
        backup_dir = os.path.join(cf_dir, f"./backup.{time.time()}/")
        copy_files_to(cf_dir, backup_dir, files_only=True, create_to_dir=True)

        # Prepare for refresh
        copy_files_to(cf_dir, os.path.join(cf_dir, "./upload/"), files_only=True)

        logger.info(f"Change table compaction strategy to {strategy2}")
        session.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy2}")

        def assert_reshape(srcdir, log_mark, verify_reshape=True, followed_by=None):
            """
            Check Reshaping or Resharding really happened
            """
            try:
                exprs = [r"(Reshape|Reshard) keyspace1.standard1"]
                if followed_by:
                    exprs.append(followed_by)
                res = node1.watch_log_for(exprs, timeout=30, from_mark=log_mark)
                logger.debug(res)
            except TimeoutError:
                res = None
                msg = f"Reshape or Reshard didn't occur after loading sstables from {srcdir} directory"
                if strategy2["class"] not in ["DateTieredCompactionStrategy", "SizeTieredCompactionStrategy"] and verify_reshape:
                    if strategy2["class"] != strategy1["class"]:
                        assert res is not None, f"Reshape didn't occurred in loading sstables from {srcdir} directory"
                    else:
                        logger.debug(msg + ", as expected.")

        def verify_data(srcdir):
            """
            Verify the loaded data by cs read
            """
            logger.info(f"Verify data is loaded from {srcdir} directory")
            node1.stress(["read", "n=100", "no-warmup", "-rate", "threads=10", "-col", "size=FIXED(1024)"])

        logger.debug("Clean test data & sstables before subtest by TRUNCATE")
        session.execute("TRUNCATE keyspace1.standard1")

        logger.debug("Re-enable autocompaction, otherwise compaction & reshape wont' work in restart and refresh")
        node1.nodetool("enableautocompaction keyspace1 standard1")

        logger.info("Load data from upload directory by refresh")
        mark = node1.mark_log()
        logger.info("Refresh keyspace1.standard1 .....")
        node1.nodetool("refresh -- keyspace1 standard1")
        followed_by = r"Done loading new SSTables for keyspace=keyspace1.*table=standard1"
        assert_reshape(srcdir="upload/", log_mark=mark, followed_by=followed_by)
        verify_data(srcdir="upload/")

        logger.debug("Clean test data & sstables before subtest by TRUNCATE")
        session.execute("TRUNCATE keyspace1.standard1")
        logger.info("Restart to load sstables from staging directory")
        mark = node1.mark_log()
        logger.info("Restart the node .....")
        node1.stop(gently=True)

        # Prepare for cf population from staging during restart
        copy_files_to(backup_dir, os.path.join(cf_dir, "./staging/"), files_only=True)

        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        verify_reshape = not (strategy2["class"] == "LeveledCompactionStrategy" and strategy1["class"] != "LeveledCompactionStrategy")
        followed_by = r"Reshaped .* seconds"
        assert_reshape(srcdir="staging/", log_mark=mark, verify_reshape=verify_reshape, followed_by=followed_by)
        verify_data(srcdir="staging/")

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    @unmark.next_gating  # https://github.com/scylladb/scylla-enterprise/issues/3385
    @pytest.mark.parametrize("strategy1,strategy2", get_strategies_upgrade_options(), ids=generate_ids)
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_reshard_after_compaction_strategy_and_smp_change(self, strategy1, strategy2):  # noqa: PLR0915
        """
        This test tries to load backup sstable by refresh and restart after changing the compaction strategy and smp count.
        refreshing loads sstable from upload directory, and sstable in staging or main sstable directory will
        be loaded in cf populating during restart.
        """

        if multiprocessing.cpu_count() < 3:
            pytest.skip("This test requires a minimum of 3 cpus")

        cluster = self.cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        logging_args = ["--logger-log-level", "sstables_loader=debug", "--logger-log-level", "sstable_directory=trace", "--logger-log-level", "compaction=debug"]
        node1.start(wait_for_binary_proto=True, jvm_args=["--smp", "1", *logging_args])

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("DROP KEYSPACE IF EXISTS keyspace1")

        logger.debug(f"Create test table with {strategy1}")
        node1.stress(["write", "n=0", "no-warmup", "-schema", "replication(factor=1)", "-rate", "threads=1"])

        session.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy1}")

        logger.debug("Insert test data by cassandra-stress and compact")
        # Use multiple workloads to generate multiple sstables, then it's easy to reach the threshold for reshaping

        fill_data_by_cs(node1, n_range=[500, 550, 600, 650])
        # Compact initially, make sure there are some compacted sstables before disable autocompaction
        node1.compact()

        # Here we disable autocompaction for leaving all sstables in level 0, then
        # it's easy to trigger reshape with small dataset during restart (strict mode).
        # Actually it's not always necessary.
        #
        # Refreshing from upload will use strict mode reshape
        # Only in relaxed mode, all sstables will be mutated to level to 0, reshaping
        # will be trigger very easily.

        logger.debug("disable autocompaction to leave all sstables in level 0")
        node1.nodetool("disableautocompaction keyspace1 standard1")

        logger.debug("Insert test data by cassandra-stress without compacting, leave it for next strategy")

        if strategy2["class"] == "TimeWindowCompactionStrategy":
            # Prepare a sstable spans two 1 window, (window unit is 60 seconds)
            fill_data_by_cs(node1, n_range=[], duration_range=[70], other_opt=["-rate", "threads=1", "-col", "size=FIXED(1024)"])
        elif strategy2["class"] == "LeveledCompactionStrategy":
            # Need more than 10% overlapping sstables on same level
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650, 1000], start=5000)
        elif strategy2["class"] == "SizeTieredCompactionStrategy":
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650, 2000, 5000] * 2, start=10000)
        else:
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650], start=5000)

        cf_dir = get_node_cf_dir(node1, "keyspace1", "standard1", latest=True)
        logger.debug(cf_dir)

        # For troubleshot
        backup_dir = os.path.join(cf_dir, f"./backup.{time.time()}/")
        copy_files_to(cf_dir, backup_dir, files_only=True, create_to_dir=True)

        # Prepare for refresh
        copy_files_to(cf_dir, os.path.join(cf_dir, "./upload/"), files_only=True)

        logger.info(f"Change table compaction strategy to {strategy2}")
        session.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy2}")

        def assert_reshard(srcdir, log_mark, followed_by=None):
            """
            Check Resharding really happened
            """
            exprs = [r"Reshard keyspace1.standard1"]
            if followed_by:
                exprs.append(followed_by)
            res = node1.watch_log_for(exprs, timeout=30, from_mark=log_mark)
            logger.debug(res)

        def verify_data(srcdir):
            """
            Verify the loaded data by cs read
            """
            logger.info(f"Verify data is loaded from {srcdir} directory")
            node1.stress(["read", "n=100", "no-warmup", "-rate", "threads=10", "-col", "size=FIXED(1024)"])

        logger.debug("Clean test data & sstables before subtest by TRUNCATE")
        session.execute("TRUNCATE keyspace1.standard1")

        logger.debug("Re-enable autocompaction, otherwise compaction & reshape wont' work in restart and refresh")
        node1.nodetool("enableautocompaction keyspace1 standard1")

        node1.stop()
        node1.start(jvm_args=["--smp", "3", *logging_args])
        session = self.patient_cql_connection(node1)

        logger.info("Load data from upload directory by refresh")
        mark = node1.mark_log()
        logger.info("Refresh keyspace1.standard1 .....")
        node1.nodetool("refresh -- keyspace1 standard1")
        followed_by = r"Done loading new SSTables for keyspace=keyspace1.*table=standard1"
        assert_reshard(srcdir="upload/", log_mark=mark, followed_by=followed_by)
        verify_data(srcdir="upload/")

        logger.debug("Clean test data & sstables before subtest by TRUNCATE")
        session.execute("TRUNCATE keyspace1.standard1")
        logger.info("Restart to load sstables from staging directory")
        mark = node1.mark_log()
        logger.info("Restart the node .....")
        node1.stop(gently=True)

        # Prepare for cf population from staging during restart
        copy_files_to(backup_dir, os.path.join(cf_dir, "./staging/"), files_only=True)

        node1.start(wait_for_binary_proto=True, jvm_args=["--smp", "2"])
        session = self.patient_cql_connection(node1)
        followed_by = r"Resharded .* keyspace1.standard1"
        assert_reshard(srcdir="staging/", log_mark=mark, followed_by=followed_by)
        verify_data(srcdir="staging/")

    @pytest.mark.parametrize(
        "cf_sizes",
        [
            (100, 10_000, 100_000),  # cf_3 > cf_2 > cf_1
            (10_000, 100, 100_000),  # cf_2 > cf 1 > cf_3
            (10_000, 100_000, 100),  # cf_3 > cf_1 > cf_2
            (100, 100_000, 10_000),  # cf_2 > cf_3 > cf_1
            (100_000, 10_000, 100),  # cf_1 > cf_2 > cf_3
            (100_000, 100, 10_00),
        ],  # cf_1 > cf_3 > cf_2
        ids=["100-10_000-100_000", "10_000-100-100_000", "10_000-100_000-100", "100-100_000-10_000", "100_000-10_000-100", "100_000-100-10_00"],
    )
    @pytest.mark.single_node
    @pytest.mark.dtest_full
    def test_major_compaction_processes_tables_in_order_by_size(self, cf_sizes: tuple):
        """
        Major compaction should process tables in a sorted order,
        from smallest to largest. The test checks if the ordering is correct.

        Steps:
        1. Create 3 tables of different sizes.
        2. Trigger a major compaction.
        3. Query the system.compaction_history table to get the history of
        compactions.
        4. Sort the query results by time and by table size.
        5. Assert that the 2 sorts give identical results.
        """

        @dataclass
        class CfSizeTime:
            name: str
            size: int | None
            compaction_time: datetime.datetime | None

        def _prepare_tables_with_data(cf_sizes: tuple):
            cf_size_time = []

            for size in cf_sizes:
                cf_name = cf_names[cf_sizes.index(size)]
                create_c1c2_table(session=session, cf=cf_name)
                insert_c1c2(session=session, ks=ks_name, cf=cf_name, n=size, consistency=ConsistencyLevel.ONE)
                cf_size_time.append(CfSizeTime(name=cf_name, size=size, compaction_time=None))

            return cf_size_time

        def _perform_major_compaction():
            node1.compact()
            node1.wait_for_compactions()

        def _get_compaction_history(major_compaction_time) -> list:
            compaction_history_query = f"SELECT columnfamily_name, compacted_at, keyspace_name FROM system.compaction_history"
            compaction_history_result = session.execute(compaction_history_query).all()
            return [row for row in compaction_history_result if row.keyspace_name == ks_name and row.compacted_at.replace(tzinfo=datetime.UTC) >= major_compaction_time]

        def _sort_compaction_history(compaction_history: list) -> tuple:
            for item in cf_size_time:
                for row in compaction_history:
                    if item.name == row.columnfamily_name and (not item.compaction_time or row.compacted_at > item.compaction_time):
                        item.compaction_time = row.compacted_at
                        logger.debug(item)
            by_time = sorted(cf_size_time, key=attrgetter("compaction_time", "size"))
            by_size = sorted(cf_size_time, key=attrgetter("size"))
            return by_time, by_size

        nodelist, session = self.prepare(nodes=1, jvm_args=["--smp", "1"])
        node1 = nodelist[0]
        ks_name = "ks"
        cf_names = ["cf_1", "cf_2", "cf_3"]
        create_ks(session=session, name=ks_name, rf=1)
        cf_size_time = _prepare_tables_with_data(cf_sizes=cf_sizes)

        node1.flush()
        # get naive datetime at UTC timezone
        t = dt.fromtimestamp(time.time(), tz=datetime.UTC)
        _perform_major_compaction()
        sorted_by_time, sorted_by_size = _sort_compaction_history(_get_compaction_history(t))

        assert sorted_by_time == sorted_by_size, "The list of rows sorted by size is not identical to the list of rows sorted by compaction time"

    def wait_for_new_minute(self):
        while dt.now().second > 5:
            time.sleep(1)

    def get_sstables_compactions_flow(self, node, exprs, from_mark):
        compact_sstables = []
        for expr in exprs:
            compact_one_expr = []
            matches = node.grep_log(expr=expr, from_mark=from_mark)
            assert matches, f"Compactions were not started. Expression: {expr}"

            # Find sstable number from the line.
            # Example of line:
            #   compaction - [Compact ks.cf2 29176150-1957-11ec-8aaf-6d8518342838] Compacting
            #   [dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-5-big-Data.db:level=0:
            #   origin=memtable,
            #   .dtest/dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-1-big-Data.db:level=0:
            #   origin=memtable]
            # split_pattern = re.compile(r"(?:m[c-est]|n[a-b]|o[a]|d[a])-(\d+)-")
            split_pattern = re.compile(rf"(?:{self.SSTABLE_PREFIX_REG_EXPR})-(\d+)-")
            for one_match in matches:
                line_groups = one_match[1].groupdict()
                if not line_groups:
                    continue

                # line_groups[0:4] are Null for "Stopping" message
                event_type = line_groups["compaction_type"] or line_groups["interrupt"]
                # Find how many compaction tasks were stopped.
                # Example of line:
                #   compaction - [Compact ks.cf 6737cef0-ab8a-11ec-beb6-c840fb266497] Compacting of 1 sstables
                #   interrupted due to: sstables::compaction_stopped_exception (Compaction for ks/cf was stopped due
                #   to: user-triggered operation)
                if event_type == "interrupted due":
                    compact_one_expr.append({"type": "Stopping", "shard": line_groups["stop_shard"], "task_id": line_groups["stop_task_id"]})
                    continue

                if event_type not in ["Compacting", "Cleaning"]:
                    raise ValueError(f"Unexpected compaction type: {event_type}. Line: {one_match[0]}")

                sstable_numbers = re.findall(split_pattern, line_groups["sstables"])
                compact_one_expr.append({"type": event_type, "sstables": sstable_numbers, "shard": line_groups["run_shard"], "task_id": line_groups["run_task_id"]})
            compact_sstables.append(compact_one_expr)

        return compact_sstables

    @staticmethod
    def search_and_assert_for_double_compactions(compaction_flow, tables):
        """
        3 operations in the flow:
        1. Compacting (maybe major or ongoing)
        2. Cleaning
        3. Stop ongoing/major compaction task (before running cleaning)

        If stop task found, previous compacting events will be removed from list of running compactions according to
        amount of stopped task (found it in the log "Stopping 2 tasks for 1 ongoing compactions")

        This function checks that compaction on sstable is stopped before Cleaning task starts on the same sstable
        """
        logger.debug("Search for double compactions on the same sstable")
        for i, compacted_sstables_of_one_table in enumerate(compaction_flow):
            logger.debug(f"Compaction flow for '{tables[i]}' table: {compacted_sstables_of_one_table}")
            stopped_tasks = [task["task_id"] for task in compacted_sstables_of_one_table if task["type"] == "Stopping"]
            regular_compact = []
            cleanup_compact = []
            for compact_sstables in compacted_sstables_of_one_table:
                if compact_sstables["type"] == "Compacting":
                    if compact_sstables["task_id"] not in stopped_tasks:
                        regular_compact.append(compact_sstables["sstables"])
                elif compact_sstables["type"] == "Cleaning":
                    cleanup_compact.extend(compact_sstables["sstables"])
                elif compact_sstables["type"] == "Stopping":
                    continue
                else:
                    raise ValueError("Unexpected compaction type: %s", compact_sstables["type"])

            regular_compact = list(itertools.chain(*regular_compact))
            logger.debug(f"Sstable files of table '{tables[i]}' were compacted by regular compactions: {regular_compact}")
            logger.debug(f"Sstable files of table '{tables[i]}' were compacted by cleanup compactions: {cleanup_compact}")
            double_compacted_sstables = list(set(regular_compact).intersection(cleanup_compact))
            assert not double_compacted_sstables, f"Found sstables that were compacted by both regular compactions and cleanup (table '{tables[i]}'): {double_compacted_sstables}"

    @pytest.mark.single_node
    @unmark.next_gating
    def test_double_compaction_by_cleanup_and_major_compactions(self):
        """
        Cover the issue https://github.com/scylladb/scylla/issues/8155
        Test that cleanup is not started on the sstable that regular compaction runs on it

        1. Create a cluster with a single node with rf=1, create a table, insert data and flush
        2. Run in parallel cleanup and major compaction after delete/insert rows
        3. Check in the log that same sstable was not compacted twice by cleanup and major compaction
        """
        # Set compaction_static_shares to 10 to  make compaction slower, so increasing the chances of reproducing
        # the issue
        node_list, session_node1 = self.prepare(nodes=1, configuration_options={"compaction_static_shares": 10})
        node1 = node_list[0]
        rows = 20000 if self.debug_mode else 200000
        chunk = rows // 20

        create_ks(session_node1, "ks", 1)
        create_cf(session_node1, "cf", gc_grace=5, read_repair=0.0, columns={"c1": "text", "c2": "text"}, compaction={"class": "SizeTieredCompactionStrategy"})
        logger.debug(f"Insert {rows} rows to the table")
        insert_c1c2(session_node1, keys=range(rows), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        mark = node1.mark_log()

        proc_functions = [{"func": node1.nodetool, "args": ("compact ks",)}, {"func": node1.nodetool, "args": ("cleanup ks",)}]

        for _ in range(3):
            # Insert or delete data will change data files
            insert_c1c2(session_node1, keys=range(2 * chunk, 7 * chunk), consistency=ConsistencyLevel.ONE)
            node1.flush()
            run_in_parallel(proc_functions)

            delete_c1c2(session_node1, keys=list(range(2 * chunk, 7 * chunk)))
            node1.flush()
            run_in_parallel(proc_functions)

        # Find lines in the log about compacting and cleaning.
        # Examples:
        #   compaction - [Cleanup ks.cf2 661f8180-1958-11ec-a8dd-f95d7ef91c29] Cleaning
        #   [.dtest/dtest-2p6gfyf9/test/node1/data/ks/cf-0a06d880195811eca8ddf95d7ef91c29/md-5-big-Data.db:level=0:
        #   origin=memtable]
        #
        #   compaction - [Compact ks.cf 29176150-1957-11ec-8aaf-6d8518342838] Compacting
        #   [dtest-e7ugljs7/test/node1/data/ks/cf-cb269cf0195611ec8aaf6d8518342838/md-5-big-Data.db:level=0:
        #   origin=memtable,
        #   .dtest/dtest-e7ugljs7/test/node1/data/ks/cf-cb269cf0195611ec8aaf6d8518342838/md-1-big-Data.db:level=0:
        #   origin=memtable]
        #
        #   compaction - [Compact ks.cf 6737cef0-ab8a-11ec-beb6-c840fb266497] Compacting of 1 sstables interrupted due
        #   to: sstables::compaction_stopped_exception (Compaction for ks/cf was stopped due to: user-triggered
        #   operation)
        compaction_flow = self.get_sstables_compactions_flow(node=node1, exprs=[self.REG_EXPR_TEMPLATE.format(ks="ks", table="cf")], from_mark=mark)

        self.search_and_assert_for_double_compactions(compaction_flow, tables=["cf"])

        errors = node1.grep_log_for_errors()
        assert not errors, f"Failed with error: {errors}"

    # Cleanup compaction does not happen in tablets-enabled keyspaces by design
    @pytest.mark.required_features("!tablets")
    @pytest.mark.single_node
    def test_double_compaction_by_cleanup_and_ongoing_compaction(self):
        """
        Cover the issue https://github.com/scylladb/scylla/issues/8155
        Test that cleanup is not started on the sstable that regular compaction runs on it

        1. Create a cluster with a single node with rf=1
        2. Create 3 tables with NullCompactionStrategy, insert data and flush
        3. Run in parallel cleanup and alter tables, change tables compaction to SizeTieredCompactionStrategy. It cause
           to start ongoing compaction
        4. Check in the log that same sstable was not compacted twice by cleanup and ongoing compaction
        """
        # Set compaction_static_shares to 10 to  make compaction slower, so increasing the chances of reproducing
        # the issue
        node_list, session_node1 = self.prepare(nodes=1, configuration_options={"compaction_static_shares": 10})
        node1 = node_list[0]

        create_ks(session_node1, "ks", 1)

        tables = ["cf", "cf1", "cf2"]
        insert_proc_functions = []
        compact_proc_functions = []
        rows = 20000 if self.debug_mode else 200000
        for table in tables:
            create_cf(session_node1, table, gc_grace=5, read_repair=0.0, columns={"c1": "text", "c2": "text"}, compaction={"class": "NullCompactionStrategy"})
            insert_proc_functions.append({"func": insert_c1c2, "kwargs": {"session": session_node1, "keys": range(rows), "consistency": ConsistencyLevel.ONE, "cf": table}})
            compact_proc_functions.append({"func": session_node1.execute, "args": ("alter table %s with compaction = {'class':'SizeTieredCompactionStrategy'}" % table,)})

        logger.debug(f"Insert {rows} rows to the tables")
        run_in_parallel(insert_proc_functions)
        self.cluster.flush()

        compact_proc_functions.append({"func": node1.nodetool, "args": ("cleanup ks",)})
        mark = node1.mark_log()
        logger.debug("Run in parallel cleanup on the keyspace and alter tables")
        run_in_parallel(compact_proc_functions)

        # Find lines in the log about compacting and cleaning.
        # Examples:
        #   compaction - [Cleanup ks.cf2 661f8180-1958-11ec-a8dd-f95d7ef91c29] Cleaning
        #   [.dtest/dtest-2p6gfyf9/test/node1/data/ks/cf2-0a06d880195811eca8ddf95d7ef91c29/md-5-big-Data.db:level=0:
        #   origin=memtable]
        #
        #   compaction - [Compact ks.cf2 29176150-1957-11ec-8aaf-6d8518342838] Compacting
        #   [dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-5-big-Data.db:level=0:
        #   origin=memtable,
        #   .dtest/dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-1-big-Data.db:level=0:
        #   origin=memtable]
        #
        #   compaction - [Compact ks.cf 6737cef0-ab8a-11ec-beb6-c840fb266497] Compacting of 1 sstables interrupted due
        #   to: sstables::compaction_stopped_exception (Compaction for ks/cf was stopped due to: user-triggered
        #   operation)
        compacting_flow = self.get_sstables_compactions_flow(node=node1, exprs=[self.REG_EXPR_TEMPLATE.format(ks="ks", table=table) for table in tables], from_mark=mark)

        self.search_and_assert_for_double_compactions(compacting_flow, tables=tables)

        logger.debug("Validate expected rows")
        for table in tables:
            assert_row_count(session=session_node1, table_name=table, expected=rows)

        errors = node1.grep_log_for_errors()
        assert not errors, f"Failed with error: {errors}"

    def write_n_data_files(self, node, session, key_space, num_of_files, num_of_keys, consistency=ConsistencyLevel.ONE):  # noqa: PLR0913
        for t in range(num_of_files):
            logger.debug(f"Inserting concurrently {num_of_keys} keys...")
            insert_c1c2(session, n=num_of_keys, consistency=consistency, ks=key_space)
            node.flush()

    @pytest.mark.single_node
    def test_data_from_different_windows_compacted_together(self):
        """
        In specific cases, there's a need to delete from TWCS table. Normally, sstables from different time windows
        are not compacted together, but for deletion this required.
        https://github.com/scylladb/scylla-enterprise/pull/2055
        """
        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(1)
        logger.debug("Creating keyspace 'ks'...")
        create_ks(session, "ks", 1)
        session.execute(
            """CREATE TABLE ks.tb (
                        pk int,
                        ck int,
                        v int,
                        PRIMARY KEY (pk, ck)
                    ) WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES',
                        'compaction_window_size': '1'}
                    AND tombstone_gc = {'mode': 'disabled'};
                    """
        )
        # insert data with the same pk to multiple time windows - creating 4 sstables
        insert_statement = session.prepare("INSERT INTO ks.tb (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?")
        concurrent.execute_concurrent_with_args(session, insert_statement, [(1, 2, 1, 100000000001), (1, 3, 1, 200000000001), (1, 4, 1, 300000000001), (1, 5, 2, 500000000001)])
        node1.flush()
        node1.nodetool("compact ks tb")
        assert len(get_list_of_sstables(node1, "ks", "tb")) == 4, "there should be 1 sstable for each time window"

        # delete given pk
        session.execute("DELETE FROM ks.tb where pk=1")
        node1.flush()
        # major compaction should compact deleted pk regardless of time window
        node1.nodetool("compact ks tb")
        assert len(get_list_of_sstables(node1, "ks", "tb")) == 1, "after deletion, major compaction should have compacted sstables regardless of time window"


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCompactionAdditionalStrategy(CompactionAdditionalTester):
    strategy = None

    @pytest.fixture(
        params=[
            "LeveledCompactionStrategy",
            "SizeTieredCompactionStrategy",
            # DateTieredCompactionStrategy has been deprecated by https://github.com/scylladb/scylladb/pull/11458
            "TimeWindowCompactionStrategy",
            "IncrementalCompactionStrategy",
        ],
        autouse=True,
    )
    def fixture_set_cs(self, request):
        self.strategy = request.param

    @classmethod
    def _make_uuid_sstable_identifier(cls):
        # generate an id like: "3fw2_0tj4_46w3k2cpidnirvjy7k"
        alphabet = string.digits + string.ascii_lowercase
        alphabet_len = len(alphabet)
        decimicro_ratio = 10_000_000

        def encode(n):
            output = ""
            while n:
                n, index = divmod(n, alphabet_len)
                output += alphabet[index]
            return output[::-1]

        timeuuid = uuid.uuid1()
        seconds, decimicro = divmod(timeuuid.time, decimicro_ratio)
        delta = datetime.timedelta(seconds=seconds)
        encoded_days = encode(delta.days)
        encoded_seconds = encode(delta.seconds)
        encoded_decimicro = encode(decimicro)
        lsb = int.from_bytes(timeuuid.bytes[8:])
        encoded_lsb = encode(lsb)
        return f"{encoded_days:0>4}_{encoded_seconds:0>4}_{encoded_decimicro:0>5}{encoded_lsb:0>13}"

    @classmethod
    def _make_n_sstable_identifiers(cls, n, use_uuid):
        identifiers = []
        for _ in range(n):
            _id = None
            if use_uuid:
                _id = cls._make_uuid_sstable_identifier()
            else:
                while True:
                    _id = random.randint(10000, 100000)
                    if _id not in identifiers:
                        break
            identifiers.append(_id)
        return identifiers

    def test_compaction_is_started_on_boot(self):
        [node1], session = self.prepare(1)
        create_ks(session, "ks", 1)
        session.execute(f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{self.strategy}'}};")

        for x in range(100):
            session.execute(f"insert into cf (key, val) values ({x},1)")

        node1.flush()
        node1.compact()
        node1.stop()
        files = glob.glob(os.path.join(node1.get_path(), "commitlogs", "*"))
        for f in files:
            try:
                os.remove(f)
            except IsADirectoryError:
                shutil.rmtree(f)

        cf_dir = get_node_cf_dir(node1, "ks", "cf")
        sstablefiles = get_sstables_files(cf_dir)
        # prepare a mapping between each sstable generation
        # to new, unique generations it will be copied to
        gmap = dict()
        generations = set([self._get_sstable_generation(f) for f in sstablefiles])
        for gen in generations:
            try:
                _ = int(gen)
                use_uuid = False
            except ValueError:
                use_uuid = True
            mapped = self._make_n_sstable_identifiers(4, use_uuid)
            gmap[gen] = mapped
            logger.debug(f"Will copy SSTable with generation {gen} to generations {mapped}")

        for f in sstablefiles:
            gen = self._get_sstable_generation(f)
            for i in gmap[gen]:
                self._copy_sstable_file(os.path.join(cf_dir, f), str(i))

        before_start_sstables = get_sstables_files(cf_dir, "Data")

        from_mark = node1.mark_log()
        node1.start()
        node1.watch_log_for(r"compaction -.*(Compacted|Resharded|Reshaped) [0-9]+ sstables to \[.+/data/ks/cf-.+\]", from_mark=from_mark)

        after_start_sstables = get_sstables_files(cf_dir, "Data")

        assert before_start_sstables != after_start_sstables, f"No compaction detected after restarting {node1.name}. SSTables in ks/cf: {after_start_sstables}"

    @pytest.mark.dtest_debug
    def test_compaction_removes_ttld_data_after_gc_period(self):
        """
        Test that compaction removes TTLd data after gc_period
        1. start cluster
        2. create a table with a small gc_period
        3. write data into the table with a small ttl
        4. wait past ttl and gc_period
        5. write some data and force compaction
        6. check that ttl'd data was removed
        Please note that we do not test that ttl data exists - we have other tests for this
        """
        [node1], session = self.prepare(1)
        create_ks(session, "ks", 1)

        session.execute(f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{self.strategy}'}} and gc_grace_seconds = 1;")

        for x in range(100):
            session.execute(f"insert into cf (key, val) values ({x},1) USING TTL 29")

        time.sleep(31)

        # check that after gc_period compaction removes ttl'd data
        # force an update so that compact will have something to do
        session.execute("insert into ks.cf (key, val) values (99,1);")
        node1.flush()
        node1.compact()

        jsoninfo = dump_sstables(node1, "ks", "cf")
        node1.info(jsoninfo)

        numfound = len(jsoninfo)
        assert numfound == 1, f"Error: expected 1 partition but found {numfound}:\n{jsoninfo}"

    def _get_sstable_generation(self, file):
        sstable_split_parts = os.path.basename(file).split("-")
        if len(sstable_split_parts) == 5:
            # <= ka format
            return sstable_split_parts[-2]
        elif len(sstable_split_parts) == 4:
            # >= la format
            return sstable_split_parts[1]
        else:
            raise RuntimeError("Unexpected format of file name: '%s'" % file)

    def _copy_sstable_file(self, file, generation):
        # filter out scylla component for IncrementalCompactionStrategy
        # to force a new run-identifier
        if self.strategy == "IncrementalCompactionStrategy" and "Scylla.db" in file:
            return

        sstable_split_parts = os.path.basename(file).split("-")
        if len(sstable_split_parts) == 5:
            # <= ka format
            sstable_split_parts[-2] = generation
        elif len(sstable_split_parts) == 4:
            # >= la format
            sstable_split_parts[1] = generation
        else:
            raise RuntimeError("Unexpected format of file name: '%s'" % file)
        dest = os.path.join(os.path.dirname(file), "-".join(sstable_split_parts))
        if self.strategy != "IncrementalCompactionStrategy" or not "TOC.txt" in file:
            shutil.copy(file, dest)
        else:
            w = open(dest, "w+")
            r = open(file)
            line = r.readline()
            while line:
                if not "Scylla.db" in line:
                    w.writelines(line)
                line = r.readline()
            r.close()
            w.close()


@pytest.mark.dtest_full
class TestTimeWindowDataSegregation(CompactionAdditionalTester):
    keyspace_name = "ks"
    table_name = "test"
    window_size = 1
    window_unit = "MINUTES"
    ttl = 1800
    gc_period = 1800

    def _get_info_on_sstable_spanning_one_window(self, node, statistics_file, window_size_in_seconds) -> SpanningSStable:
        def is_sstable_one_time_window(min_timestamp_in_seconds, max_timestamp_in_seconds):
            def get_window_lower_bound(timestamp_in_seconds):
                return timestamp_in_seconds - (timestamp_in_seconds % window_size_in_seconds)

            return get_window_lower_bound(min_timestamp_in_seconds) == get_window_lower_bound(max_timestamp_in_seconds)

        stats = self.get_stats(node, statistics_file, keyspace_name=self.keyspace_name, table_name=self.table_name)
        min_timestamp_seconds = self.micros_to_seconds(stats["min_timestamp"])
        max_timestamp_seconds = self.micros_to_seconds(stats["max_timestamp"])
        is_spanning_one_window = is_sstable_one_time_window(min_timestamp_seconds, max_timestamp_seconds)
        res = SpanningSStable(is_spanning_one_window=is_spanning_one_window, min_timestamp_seconds=min_timestamp_seconds, max_timestamp_seconds=max_timestamp_seconds)
        return res

    def _get_time_window_in_seconds(self, node, statistics_file, stats=None):
        if not stats:
            stats = self.get_stats(node, statistics_file, keyspace_name=self.keyspace_name, table_name=self.table_name)
        min_timestamp = stats["min_timestamp"]
        max_timestamp = stats["max_timestamp"]
        return self.micros_to_seconds(max_timestamp - min_timestamp)

    def _check_sstable_timestamps(  # noqa: PLR0913
        self,
        node,
        window_size=None,
        window_unit=None,
        keyspace_name=keyspace_name,
        table_name=table_name,
        disable_load_balancing=True,
    ):
        window_size = window_size or self.window_size
        window_unit = window_unit or self.window_unit
        ctx_manager = disable_sstable_modifications if disable_load_balancing else disable_autocompaction
        with ctx_manager(node=node, keyspace_name=keyspace_name, table_name=table_name):
            node.nodetool(f"flush system_schema")

            statistics_files = self._get_list_of_sstables(node)
            assert len(statistics_files) > 0, "No statistics files"
            multiplier = 60 if window_unit == "MINUTES" else 3600
            for sf in statistics_files:
                stats = self.get_stats(node, sf, keyspace_name=keyspace_name, table_name=table_name)
                tw = self._get_time_window_in_seconds(node, sf, stats)
                # Allow an error margin of a half-window.
                margin = 1.5 * window_size * multiplier
                assert tw <= margin, (
                    f"time window of {tw} seconds is greater than {margin} \
                                       seconds margin: sstable={sf} \
                                       min_timestamp={stats['min_timestamp']} max_timestamp={stats['max_timestamp']}"
                )

    def _sstable_count_is_close_to_time_windows_multiplied_by_shards_count(self, node, time_windows, shards_count=1):
        sstables = self._get_list_of_sstables(node)
        expected_sstables_count = time_windows * shards_count
        sstables_count_margin = 2 * shards_count  # possible additional window before and after (half-windows)
        assert expected_sstables_count + sstables_count_margin >= len(sstables) >= time_windows * shards_count, f"Wrong sstables count on {node.name}. There should be at least one sstable per time window."

    def _sstable_count_is_equal_or_greater_than_time_windows(self, node, time_windows):
        sstables = self._get_list_of_sstables(node)
        assert len(sstables) >= time_windows, f"Missing sstables on {node.name}. There should be at least one sstable per time window."

    def _create_ks_cl_with_twcs(self, session, rf=1, keyspace_name="ks", table_name="test", ttl=None, gc_period=None, window_unit=None, window_size=None, tablet_count=None):  # noqa: PLR0913
        if ttl is None:
            ttl = self.ttl
        if gc_period is None:
            gc_period = self.gc_period
        if window_unit is None:
            window_unit = self.window_unit
        if window_size is None:
            window_size = self.window_size

        tablet_opts = ""
        if "tablets" in self.scylla_features and tablet_count:
            # Tests assume that there are at least two tablets because they verify rebalancing after bootstrap.
            # Also, we must suppress splits due to default tablet count per shard, as that can create
            # extra sstables which tests don't expect to find and cause assertion failures about sstable count.
            tablet_opts = f"and tablets = {{'initial': {tablet_count}}}"

        session.execute(f"CREATE KEYSPACE {self.keyspace_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} {tablet_opts}")
        session.execute(
            f"CREATE TABLE {keyspace_name}.{table_name} (pk int, ck int, v blob, PRIMARY KEY(pk, ck)) "
            f"WITH default_time_to_live = {ttl} AND "
            f"gc_grace_seconds = {gc_period} AND "
            f"compaction = {{"
            f"'class': 'TimeWindowCompactionStrategy',"
            f"'compaction_window_unit': '{window_unit}',"
            f"'compaction_window_size': {window_size} }} AND "
            "tombstone_gc = {'mode': 'timeout'}"
        )

    def _simulate_write_process_in_minutes(  # noqa: PLR0913
        self,
        session,
        duration_minutes=20,
        start_from_minute=0,
        flush_period_seconds=30,
        flushing_exclude_nodes=None,
        num_pks=100,
        size=1,
    ):
        """Simulate a write process across duration minutes.

        We use `USING TIMESTAMP` to distribute the writes evenly
        across the entire range, simulating a write every second (to
        several partitions).
        flush_period_seconds allow to control how many time windows could be
        in sstable

        """
        return simulate_write_process_in_minutes(
            cluster=self.cluster,
            session=session,
            keyspace=self.keyspace_name,
            table_name=self.table_name,
            duration_minutes=duration_minutes,
            start_from_minute=start_from_minute,
            flush_period_seconds=flush_period_seconds,
            flushing_exclude_nodes=flushing_exclude_nodes,
            num_pks=num_pks,
            size=size,
        )

    def _list_sstable_timestamps(self, node):
        statistics_files = self._get_list_of_sstables(node)
        list_sstables_timewindows = []
        for sf in statistics_files:
            time_window = self._get_time_window_in_seconds(node, sf)
            list_sstables_timewindows.append((sf, time_window))

        return list_sstables_timewindows

    def _get_compaction_history(self, session) -> list:
        compaction_history_query = f"SELECT * FROM system.compaction_history"
        compaction_history_result = session.execute(compaction_history_query).all()
        return [row for row in compaction_history_result if row.keyspace_name == self.keyspace_name]

    # off-strategy compaction is not required after tablet migration
    @pytest.mark.required_features("!tablets")
    def test_streaming_during_adding_node_with_boostrap(self):
        time_windows = 20
        [node1], session = self.prepare(1)

        self._create_ks_cl_with_twcs(session, rf=1)
        self._simulate_write_process_in_minutes(session, duration_minutes=time_windows)

        # Not really relevant to the test, just for sanity.
        self._check_sstable_timestamps(node1)

        # node added with bootstrap enabled
        node2 = new_node(self.cluster)
        node2.start(wait_for_binary_proto=True)

        shard_count = node2._smp
        msgs = [rf"\[shard {i}(?::\w+)?\].*Done with off-strategy compaction for {self.keyspace_name}.{self.table_name}" for i in range(shard_count)]
        matchings = node2.watch_log_for(msgs, timeout=60)
        offstrategy_count = len(matchings)
        assert offstrategy_count % shard_count == 0, f"'{matchings}' wer logged {offstrategy_count} times which is not a multiple of shard_count = {shard_count}"
        assert offstrategy_count >= shard_count, f"'{matchings}' were logged {offstrategy_count} times which is less than shard_count={shard_count}"
        assert offstrategy_count <= shard_count * 2, f"'{matchings}' were logged {offstrategy_count} times which is more than twice of shard_count={shard_count}"

        # After streaming the new node should also have at max one window per sstable.
        self._check_sstable_timestamps(node2)
        self._sstable_count_is_close_to_time_windows_multiplied_by_shards_count(node2, time_windows, shard_count)

        # verify write amplification by validation only single reshape compaction was executed for each shard
        reshapes = node2.grep_log(rf"compaction - \[Reshape {self.keyspace_name}.{self.table_name} .*\] Reshaped")
        assert len(list(reshapes)) == shard_count, "There should be only one table reshape per shard"

    def test_streaming_decommission(self):
        [node1, node2], session = self.prepare(2)
        self._create_ks_cl_with_twcs(session, rf=1)

        self._simulate_write_process_in_minutes(session, duration_minutes=20)
        # Not really relevant to the test, just for sanity.
        self._check_sstable_timestamps(node1)
        self._check_sstable_timestamps(node2)

        # run decommossion for node1
        node1.decommission()

        # trigger and wait for offstrategy compaction
        run_rest_api(node2, f"/storage_service/keyspace_offstrategy_compaction/{self.keyspace_name}", params={"cf": self.table_name})

        # After streaming and offstrategy compaction
        # the left node should also have at max one
        # window per sstable.
        self._check_sstable_timestamps(node2)

    def test_streaming_on_repair(self):
        self.cluster.populate(generate_cluster_topology(rack_num=2)).start(wait_for_binary_proto=True)

        [node1, node2] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=2)
        with disable_load_balancing_ctx(node1):
            self._simulate_write_process_in_minutes(session, duration_minutes=10)
            self._check_sstable_timestamps(node1, disable_load_balancing=False)
            self._check_sstable_timestamps(node2, disable_load_balancing=False)
            node2.stop()
            self._simulate_write_process_in_minutes(session, duration_minutes=20, start_from_minute=10, flushing_exclude_nodes=[node2])
            self._check_sstable_timestamps(node1, disable_load_balancing=False)
            node2.start(wait_for_binary_proto=True)
            node2.repair(keyspace=self.keyspace_name)
            node2.flush()

        # trigger and wait for offstrategy compaction
        run_rest_api(node2, f"/storage_service/keyspace_offstrategy_compaction/{self.keyspace_name}", params={"cf": self.table_name})

        self._check_sstable_timestamps(node1)
        self._check_sstable_timestamps(node2)

    # off-strategy compaction is not required after tablet migration
    @pytest.mark.required_features("!tablets")
    def test_data_is_segregated_during_off_strategy(self):
        """
        https://github.com/scylladb/scylla/issues/9199
        With data segregation on repair, thousands of sstables are potentially
        added to maintenance set which causes high latency due to stalls.

        That's because N*M sstables are created by a repair,
            where N = # of ranges
            and M = # of segregations

        For TWCS, M = # of windows.

        Assuming N = 768 and M = 20, ~15k sstables end up in sstable set.
        Fix: avoid performing data segregation in repair, as offstrategy will already perform the segregation anyway

        This test verifies this.
        """
        [_node1], session = self.prepare(1, configuration_options={"enable_repair_based_node_ops": True, "allowed_repair_based_node_ops": "bootstrap"})
        self._create_ks_cl_with_twcs(session, rf=1)
        duration_minutes = 5
        self._simulate_write_process_in_minutes(session, duration_minutes=duration_minutes, num_pks=30)

        node2 = new_node(self.cluster)
        node2.start(wait_for_binary_proto=True)

        # There is a rare chance that all rows redistributed to node2 will end up in a single shard.
        # So, deduce if both shards have data by looking at the table repair stats.
        expected = [rf"stats: repair_reason=bootstrap, keyspace={self.keyspace_name}, tables=\[\"{self.table_name}\"\].*rx_row_nr=([\d]+),"] * node2._smp
        matchings = node2.watch_log_for(expected, timeout=300)
        shards_with_data = 0
        for _, m in matchings:
            if int(m.group(1)) > 0:
                shards_with_data += 1
        # Assert to protect against future changes to the log format.
        # Both the shards can be empty iff the log format changes and the expected pattern is unable to capture rx_row_nr
        assert shards_with_data > 0, "Expected data in atleast one shard of node2, but rx_row_nr is 0 for both the shards"

        full_table_name = f"{self.keyspace_name}.{self.table_name}"
        expected = [f"Done with off-strategy compaction for {full_table_name}"] * shards_with_data
        node2.watch_log_for(expected, timeout=300)

        # get sstables count taken by off-strategy to compact.
        pattern = rf"Starting off-strategy compaction for {full_table_name}.*?([\d]+) candidates were found"
        expected = [pattern] * shards_with_data
        # set timeout=0 since the "Starting" message actually precedes the "Done"
        # message that we already watched for above
        matchings = node2.watch_log_for(expected, timeout=0)
        for l, m in matchings:
            sstables_for_compaction_count = int(m.groups()[0])

            # verify there's limited number of sstables for compaction
            assert sstables_for_compaction_count <= 30, f"There were too many sstables for off-strategy compaction. #9199\n{l}"

        # verify data is segregated
        self._sstable_count_is_close_to_time_windows_multiplied_by_shards_count(node2, duration_minutes, shards_count=shards_with_data)

    @pytest.mark.required_features("tablets")
    def test_tablets_data_is_segregated_after_bootstrap(self, dtest_config):
        """
        Check that data is segregated after tablet migration post bootstrap
        """
        [_node1], session = self.prepare(
            1,
            configuration_options={
                "enable_repair_based_node_ops": True,
                "tablets_initial_scale_factor": 1,
            },
        )
        self._create_ks_cl_with_twcs(session, rf=1, tablet_count=2)
        duration_minutes = 5
        self._simulate_write_process_in_minutes(session, duration_minutes=duration_minutes, num_pks=30)

        node2 = new_node(self.cluster)
        node2.start(wait_for_binary_proto=True)
        full_table_name = f"{self.keyspace_name}.{self.table_name}"
        expected = r"Streaming for tablet migration of .* finished|Tablet migration succeeded, took \d+ seconds, nr_ranges_remaining=0"

        node2.watch_log_for(expected, timeout=300)
        # verify data is segregated
        # we assume a single tablet here
        self._sstable_count_is_close_to_time_windows_multiplied_by_shards_count(node2, duration_minutes, shards_count=1)

    # removed from gating, cause of scylladb/scylladb#11848 changed behavior
    # @pytest.mark.next_gating
    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_streaming_on_rebuild_multidc(self):
        def _add_node(i, dc):
            return self.cluster.new_node(i, debug=True, data_center=dc)

        self.cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch", "enable_sstable_key_validation": True})
        node1 = _add_node(1, "dc1")  # type: ScyllaNode

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        session.execute(f"CREATE KEYSPACE {self.keyspace_name}  WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1':1}}")
        session.execute(
            f"CREATE TABLE {self.keyspace_name}.{self.table_name} (pk int, ck int, v blob, PRIMARY KEY(pk, ck))"
            " WITH compaction = {"
            "'class': 'TimeWindowCompactionStrategy',"
            "'compaction_window_unit': 'MINUTES',"
            f"'compaction_window_size': {self.window_size}}}"
        )
        session = self.patient_cql_connection(node1)
        self._simulate_write_process_in_minutes(session, duration_minutes=10)
        self._check_sstable_timestamps(node1)
        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = _add_node(2, "dc2")  # type=ScyllaNode
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        node2.watch_log_for("init - Scylla.*initialization completed")
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute(f"ALTER KEYSPACE {self.keyspace_name} WITH replication = {{'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1}};")

        self.rebuild_errors = 0
        self.unexpected_errors = 0
        mark = node2.mark_log()

        # rebuild dc2 from dc1
        def rebuild():
            try:
                node2.nodetool("rebuild dc1")
            except NodetoolError as e:
                if "rebuild is in progress" in str(e):
                    self.rebuild_errors += 1
                else:
                    logger.debug(f"Unexpected rebuild failure {e!s}")
                    self.unexpected_errors += 1

        executor = ThreadPoolExecutor(max_workers=1)
        cmd1 = executor.submit(rebuild)
        cmd1.result()

        assert self.unexpected_errors == 0, "unexpected rebuild errors encountered."

        node2.watch_log_for("Streaming for rebuild successful|rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
        run_rest_api(node2, f"/storage_service/keyspace_offstrategy_compaction/{self.keyspace_name}", params={"cf": self.table_name})
        full_table_name = f"{self.keyspace_name}.{self.table_name}"
        node2.watch_log_for(f"Starting off-strategy compaction for {full_table_name}", from_mark=mark)
        node2.watch_log_for(f"Done with off-strategy compaction for {full_table_name}", from_mark=mark)
        node2.wait_for_compactions()
        self._check_sstable_timestamps(node2)

    @pytest.mark.required_features("tablets")
    @pytest.mark.single_node
    def test_streaming_on_multidc_with_tablets(self):
        def _add_node(i, dc, rack):
            return self.cluster.new_node(i, debug=True, data_center=dc, rack=rack)

        self.cluster.set_configuration_options(
            values={
                "endpoint_snitch": "GossipingPropertyFileSnitch",
                "enable_sstable_key_validation": True,
                "tablets_initial_scale_factor": 1,
            }
        )
        node1 = _add_node(1, "dc1", "rack1")  # type: ScyllaNode

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        session.execute(f"CREATE KEYSPACE {self.keyspace_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['rack1']}}")
        session.execute(
            f"CREATE TABLE {self.keyspace_name}.{self.table_name} (pk int, ck int, v blob, PRIMARY KEY(pk, ck))"
            " WITH compaction = {"
            "'class': 'TimeWindowCompactionStrategy',"
            "'compaction_window_unit': 'MINUTES',"
            f"'compaction_window_size': {self.window_size}}}"
        )
        session = self.patient_cql_connection(node1)
        self._simulate_write_process_in_minutes(session, duration_minutes=10)
        self._check_sstable_timestamps(node1)

        # Add a new node in dc2
        node2 = _add_node(2, "dc2", "rack1")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for node initialization
        node2.watch_log_for("init - Scylla.*initialization completed")

        # alter keyspace to replicate to dc2 using rack lists
        session = self.patient_exclusive_cql_connection(node2)
        mark = node2.mark_log()
        session.execute(f"ALTER KEYSPACE {self.keyspace_name} WITH replication = {{'class':'NetworkTopologyStrategy', 'dc1':['rack1'], 'dc2':['rack1']}};")

        # With tablets, no rebuild is needed - tablet migration handles streaming automatically
        expected = r"Streaming for tablet migration of .* finished|Tablet migration succeeded"
        node2.watch_log_for(expected, from_mark=mark, timeout=300)

        node2.wait_for_compactions()
        self._check_sstable_timestamps(node2)

    def test_streaming_sstables_with_several_timewindows(self, dtest_config):
        self.cluster.populate(generate_cluster_topology(rack_num=2)).start(wait_for_binary_proto=True)

        [node1, _] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=2)

        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=120, num_pks=100)

        sstable_timewindows_list = self._list_sstable_timestamps(node1)
        max_timewindow = 1.5 * 2 * 60
        for sstable, timewindow in sstable_timewindows_list:
            assert timewindow <= max_timewindow, f"timewindow{timewindow} is greater than {max_timewindow}"
        new_nodes = [new_node(self.cluster, bootstrap=False, data_center="datacenter1", rack=f"rack{i + 1}") for i in range(2)]
        self.cluster.start()
        if "tablets" in self.scylla_features:
            expected = r"Streaming for tablet migration of .* finished|Tablet migration succeeded"
            for _new_node in new_nodes:
                _new_node.watch_log_for(expected, timeout=60)

        for _new_node in new_nodes:
            self._check_sstable_timestamps(_new_node)

    # off-strategy compaction is not required with tablets
    # (https://github.com/scylladb/scylladb/issues/17384)
    @pytest.mark.required_features("!tablets")
    def test_rebuild_node_streaming(self):
        [_node1, _, node3], session = self.prepare(3)
        self._create_ks_cl_with_twcs(session, rf=3)

        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=20)
        for node in self.cluster.nodelist():
            self._check_sstable_timestamps(node)
        # stop node and remove all data
        node3 = self.cluster.nodelist()[2]  # type: ScyllaNode
        node3.stop()
        data_dir = os.path.join(node3.get_path(), "data", self.keyspace_name)
        shutil.rmtree(data_dir, ignore_errors=True)
        # start node and rebuild
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        mark = node3.mark_log()
        node3.nodetool("rebuild")
        node3.watch_log_for("Streaming for rebuild successful|rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
        run_rest_api(node3, f"/storage_service/keyspace_offstrategy_compaction/{self.keyspace_name}", params={"cf": self.table_name})
        node3.watch_log_for(f"Done with off-strategy compaction for {self.keyspace_name}", from_mark=mark)
        self._check_sstable_timestamps(node3)

    def _get_list_of_sstables(self, node):
        return get_list_of_sstables(node, self.keyspace_name, self.table_name, suffix="-Statistics.db")

    def test_memtable_flush(self):
        """
        Verify scylla does memtable flush into separate sstables when TWCS is used.
        Also verify each sstable span one window.
        """
        self.cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        self._create_ks_cl_with_twcs(session, rf=3)

        # Simulating 10 minutes of writes without flush
        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=999999999999)
        list_of_sstables_pre_flush = []
        list_of_sstables_post_flush = []
        for node in self.cluster.nodelist():
            list_of_sstables_pre_flush = self._get_list_of_sstables(node)
            node.flush()
            with disable_sstable_modifications(node, keyspace_name=self.keyspace_name, table_name=self.table_name):
                list_of_sstables_post_flush = self._get_list_of_sstables(node)
                post = len(list_of_sstables_post_flush)
                pre = len(list_of_sstables_pre_flush)
                assert post - pre > 1, f"Expected more than one sstable after flush, lengths are {post} {pre}, {node}"
                for sf in list_of_sstables_post_flush:
                    span_info = self._get_info_on_sstable_spanning_one_window(node, sf, self.window_size * 60)
                    msg = (
                        f"Failure, sstable {sf} spans over more than one window, "
                        + f"min_timestamp_seconds={span_info.min_timestamp_seconds} "
                        + f"max_timestamp_seconds = {span_info.max_timestamp_seconds} "
                        + f"window_size_in_seconds = {self.window_size * 60}"
                    )
                    assert span_info.is_spanning_one_window, msg

    @pytest.mark.single_node
    def test_reshape_sstables_different_size_after_change_window_size(self):
        """Reshaping table of similar size

        If window size was changed and a lot of sstables should be
        reshaped, the bucket of sstables should contain files
        of similar size.

        1. Create 64 sstables with window size 1 minute and
        32 sstables with size ~1k, other 32 sstables with size ~2M
        2. Change window size to 64.
        3. Restart node
        4. Validate that reshape process sstables in buckets by size
        """
        self.window_size = 1
        self.window_unit = "MINUTES"
        self.new_window_size = 64
        self.new_window_unit = "MINUTES"
        self.small_file_size = 5_000
        self.sstable_size_distribution = [
            {"duration": 16, "sstable_size": 10},
            {"duration": 32, "sstable_size": 1_000_000},
            {"duration": 48, "sstable_size": 10},
            {"duration": 64, "sstable_size": 1_000_000},
        ]

        self.run_flow_generate_and_reshape_twcs_sstables()

    @pytest.mark.single_node
    def test_compact_several_timewindows_after_delete_all_rows_in_old_timewindows(self):
        """
        Feature presented by: scylladb/scylla: e44a28d

        validate that major compaction process sstables
        from different buckets for TWCS.

        Using window unit = minutes and window size = 1
        generate 5 sstable each containing data for 1 minute

        Delete all previous rows in next window unit and get one
        more sstable.

        after major compaction, sstables with deleted data should
        be removed.
        """
        self.window_unit = "MINUTES"
        self.window_size = 1
        number_of_sstables_with_delete = 1
        total_partitions = 1

        [node1], session = self.prepare(1, jvm_args=["--smp", "1"])
        self._create_ks_cl_with_twcs(session, 1)
        pks, _ = self._simulate_write_process_in_minutes(session=session, duration_minutes=5, flush_period_seconds=10, num_pks=total_partitions)
        self._check_sstable_timestamps(node1)

        logger.debug("Run major compaction and validate that there is only 1 table per unit")
        node1.nodetool(f"compact {self.keyspace_name}")
        self._check_sstable_timestamps(node1)

        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == 5, f"Number of sstables {num_sstables} more than expected 5"
        self.delete_rows_in_previous_time_windows(node1, start_window_for_delete=0, end_window_for_delete=5, del_ck_per_window=60, write_mutaion_from_minute=5, num_windows_with_del_mutation=number_of_sstables_with_delete, partitions=pks)
        self._check_sstable_timestamps(node1)

        logger.debug("Check that new sstables appeared with delete mutation")
        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == 5 + number_of_sstables_with_delete, f"Number of sstables {num_sstables} is not expected {5 + number_of_sstables_with_delete}"

        logger.debug("Run major compaction")
        node1.nodetool(f"compact {self.keyspace_name}")
        logger.debug("Check that only 1 sstable left with delete mutations")
        num_sstables = len(self._get_list_of_sstables(node1))
        self._check_sstable_timestamps(node1)
        assert num_sstables == number_of_sstables_with_delete, f"Number of sstables {num_sstables} more than {number_of_sstables_with_delete}"

        logger.debug("Check that all rows are removed")
        current_rows = list(session.execute(f"select * from {self.keyspace_name}.{self.table_name}"))
        assert [] == current_rows, f"Some rows were resurrected {current_rows}"

    @pytest.mark.single_node
    def test_compact_several_timewindows_after_delete_several_rows_per_timewindow(self):
        """
        Validate that sstables with previous timewindows saved if delete operations
        remove only several rows from each timewindow.

        1. generate time-series dataset with 5 minutes where each row is writen per second
        2. run major compaction and validate that there are 5 sstables: 1 sstable for each minute
        3. delete several rows for each time window by pk and ck
        4. validate that there 6 sstables: 1 sstable for each previous time window and 1 new sstable with
        delete operations
        5. run magor compaction and validate that 6 sstables left: 1 sstable for each timewindow
        """
        self.window_unit = "MINUTES"
        self.window_size = 1
        num_pks = 4

        [node1], session = self.prepare(
            1,
            jvm_args=["--smp", "1"],
            configuration_options={
                "tablets_initial_scale_factor": 1,
            },
        )
        self._create_ks_cl_with_twcs(session, 1)

        pks, total_row = self._simulate_write_process_in_minutes(session=session, duration_minutes=5, flush_period_seconds=10, num_pks=num_pks)
        self._check_sstable_timestamps(node1)
        logger.debug("Run major compaction and validate that there is only 1 table per unit")
        node1.nodetool(f"compact {self.keyspace_name}")
        self._check_sstable_timestamps(node1)
        num_sstables = len(self._get_list_of_sstables(node1))
        expected_num_sstables = 5
        assert num_sstables == expected_num_sstables, f"Number of sstables {num_sstables} more than expected {expected_num_sstables}"

        total_deleted_rows = self.delete_rows_in_previous_time_windows(node1, start_window_for_delete=0, end_window_for_delete=5, del_ck_per_window=20, write_mutaion_from_minute=5, num_windows_with_del_mutation=1, partitions=pks)
        self._check_sstable_timestamps(node1)

        logger.debug("Check that new sstables appeared with delete mutation")
        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == expected_num_sstables + 1, f"Number of sstables {num_sstables} less than expected 6"

        logger.debug("Run major compaction")
        node1.nodetool(f"compact {self.keyspace_name}")
        logger.debug("Check that only 6 sstable left")

        num_sstables = len(self._get_list_of_sstables(node1))
        self._check_sstable_timestamps(node1)
        assert num_sstables == expected_num_sstables + 1, f"Number of sstables {num_sstables} more than expected 6"

        logger.debug("Check that all rows are removed")
        current_rows = list(session.execute(f"select * from {self.keyspace_name}.{self.table_name}"))
        assert total_row - total_deleted_rows == len(current_rows), f"Some rows were resurrected {len(current_rows)}"

    @pytest.mark.single_node
    def test_compaction_remove_deleted_rows_in_previous_time_window(self):
        """
        Verify major compaction processes delete mutations for the same rows in different
        timewindow.

        1. generate time-series dataset with 1 minute timewindow where each row is writen per second
        2. run major compaction and validate that there is 1 sstable
        3. delete several rows by pk and ck belongs to previous timewindow in next timewindow
        4. validate that there 2 sstables for each previous time window and new with
        delete operations
        5. run magor compaction
        6. validate that stable for 1st timewindow doesn't contain deleted rows.
        rows
        """
        self.window_unit = "MINUTES"
        self.window_size = 1
        num_pks = [1]

        [node1], session = self.prepare(1, jvm_args=["--smp", "1"])
        gc_period = 15
        self._create_ks_cl_with_twcs(session, 1, gc_period=gc_period)
        pks, total_row = self._simulate_write_process_in_minutes(session=session, duration_minutes=1, flush_period_seconds=10, num_pks=num_pks)
        logger.debug("Run major compaction and validate that there is only 1 table per unit")
        node1.nodetool(f"compact {self.keyspace_name}")
        node1.wait_for_compactions()
        self._check_sstable_timestamps(node1)
        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == 1, f"Number of sstables {num_sstables} more than 1"

        deletion_time = time.time()
        # delete first 20 seconds ( first 20 rows )
        total_deleted_rows = self.delete_rows_in_previous_time_windows(node1, start_window_for_delete=0, end_window_for_delete=1, del_ck_per_window=20, write_mutaion_from_minute=1, num_windows_with_del_mutation=1, partitions=pks)
        self._check_sstable_timestamps(node1)
        cluster_keys = [i for i in range(20)]
        self.assert_deleted_rows_in_sstables_exists(node1, partition_key=1, cluster_keys=cluster_keys)

        logger.debug("Check that new sstables appeared with delete mutation")
        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == 2, f"Expected 2 sstables, but got {num_sstables}"

        # First compaction is expected to keep the tombstones
        # since we're still in the gc grace period.
        logger.debug("Run major compaction")
        node1.nodetool(f"compact {self.keyspace_name}")
        node1.wait_for_compactions()
        logger.debug("Check that only 2 sstable left")

        num_sstables = len(self._get_list_of_sstables(node1))
        self._check_sstable_timestamps(node1)
        assert num_sstables == 2, f"Expected 2 sstables, but got {num_sstables}"

        logger.debug("Check that all rows are removed")
        current_rows = list(session.execute(f"select * from {self.keyspace_name}.{self.table_name}"))
        assert total_row - total_deleted_rows == len(current_rows), f"Some rows were resurrected {len(current_rows)}"

        self.assert_deleted_rows_in_sstables_exists(node1, partition_key=1, cluster_keys=cluster_keys)

        time_to_sleep = max(0, gc_period - (time.time() - deletion_time) + 1)
        logger.debug(f"Sleep {time_to_sleep} seconds until tombstones expire")
        time.sleep(time_to_sleep)

        logger.debug("Run major compaction")
        node1.nodetool(f"compact {self.keyspace_name}")
        node1.wait_for_compactions()

        self.assert_deleted_rows_in_sstables_removed(node1, partition_key=1, cluster_keys=cluster_keys)

        num_sstables = len(self._get_list_of_sstables(node1))
        self._check_sstable_timestamps(node1)
        assert num_sstables == 1, f"Expected 1 sstable, but got {num_sstables}"

    @pytest.mark.single_node
    def test_compact_several_timewindows_after_delete_rows_in_first_timewindow(self):
        """
        Validate that sstables with previous timewindow compacted if delete operations
        remove only all rows from first timewindow.

        1. generate time-series dataset within 5 minutes where each row is writen per second
        2. run major compaction and validate that there are 5 sstables:1 sstable for each minute
        3. delete all rows for 1st time window by pk and ck
        4. validate that there 6 sstables: 1 sstable for each previous timewindows and new for delete operations
        5. run magor compaction and validate that 5 sstables left. sstable for 1st window
        has been compacted and removed
        """
        self.window_unit = "MINUTES"
        self.window_size = 1
        number_of_sstables_with_delete = 1
        number_of_sstables_with_insert = duration = 5
        number_of_partitions = 5

        node1: ScyllaNode
        [node1], session = self.prepare(
            1,
            jvm_args=["--smp", "1"],
            configuration_options={
                "tablets_initial_scale_factor": 1,
            },
        )
        self._create_ks_cl_with_twcs(session, 1)
        partitions, total_rows = self._simulate_write_process_in_minutes(session=session, duration_minutes=duration, flush_period_seconds=10, num_pks=number_of_partitions)
        self._check_sstable_timestamps(node1)
        logger.debug("Run major compaction and validate that there is only 1 table per unit")
        node1.nodetool(f"compact {self.keyspace_name}")
        self._check_sstable_timestamps(node1)
        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == number_of_sstables_with_insert, f"Number of sstables {num_sstables} more than expected {number_of_sstables_with_insert}"

        logger.debug("Delete all rows in 1st time window")
        self.delete_rows_in_previous_time_windows(
            node1, start_window_for_delete=0, end_window_for_delete=1, del_ck_per_window=60, num_windows_with_del_mutation=number_of_sstables_with_delete, write_mutaion_from_minute=duration, partitions=partitions
        )
        self._check_sstable_timestamps(node1)

        logger.debug("Check that new sstables appeared with delete mutation")
        num_sstables = len(self._get_list_of_sstables(node1))
        assert num_sstables == 5 + number_of_sstables_with_delete, f"Number of sstables {num_sstables} more than expected {number_of_sstables_with_insert + number_of_sstables_with_delete}"

        logger.debug("Run major compaction")
        node1.nodetool(f"compact {self.keyspace_name}")
        logger.debug("Check stable with 1st window is removed")
        num_sstables = len(self._get_list_of_sstables(node1))
        self._check_sstable_timestamps(node1)
        expected_number_of_sstables = number_of_sstables_with_insert + number_of_sstables_with_delete - 1
        assert num_sstables == expected_number_of_sstables, f"Number of sstables {num_sstables} more than expected {expected_number_of_sstables}"

        logger.debug("Check that all rows are removed")
        current_rows = list(session.execute(f"select * from {self.keyspace_name}.{self.table_name}"))
        assert total_rows - number_of_partitions * 60 == len(current_rows), f"Some rows were resurrected {len(current_rows)}"

    def delete_rows_in_previous_time_windows(  # noqa: PLR0913
        self,
        node: ScyllaNode,
        start_window_for_delete,
        end_window_for_delete,
        del_ck_per_window,
        write_mutaion_from_minute,
        num_windows_with_del_mutation,
        partitions,
    ):
        """
        Simulate delete mutation on previous time window

        Calculate ck key based on provided windows in start_window end_window.
        Assume that data was written with method self._simulate_write_process_in_minutes.
        and then remove appropriate rows with cluster keys in previous windows and write delete
        mutation with timestamp counted from write_from_minute
        """

        session = self.patient_cql_connection(node)
        logger.debug(f"Delete {del_ck_per_window} rows in time windows: {start_window_for_delete}-{end_window_for_delete}")
        del_statement = session.prepare(f"DELETE FROM {self.keyspace_name}.{self.table_name} USING TIMESTAMP ? where pk =? and ck=?")
        sec = write_mutaion_from_minute * 60
        delta = num_windows_with_del_mutation * 60 // int(end_window_for_delete - start_window_for_delete)
        for i in range(int(start_window_for_delete * 60), int(end_window_for_delete * 60), self.window_size * 60):
            for ck in range(i, i + del_ck_per_window):
                concurrent.execute_concurrent_with_args(session, del_statement, [(self.seconds_to_micros(sec), pk, ck) for pk in partitions])
            sec += delta

        node.flush()
        total_deleted_rows = del_ck_per_window * len(partitions) * (end_window_for_delete - start_window_for_delete)
        return total_deleted_rows

    def assert_deleted_rows_in_sstables_exists(self, node: ScyllaNode, partition_key: Any, cluster_keys: list):
        sstables = sorted(get_list_of_sstables(node, self.keyspace_name, self.table_name, suffix="-Data.db"))
        exist = False
        for sstable in sstables:
            if self.are_rows_in_sstable(node, sstable, partition_key, cluster_keys):
                return
        assert exist, f"Keys {cluster_keys} are deleted from sstables {sstables}"

    def assert_deleted_rows_in_sstables_removed(self, node: ScyllaNode, partition_key: Any, cluster_keys: list):
        sstables = sorted(get_list_of_sstables(node, self.keyspace_name, self.table_name, suffix="-Data.db"))
        exist = False
        for sstable in sstables:
            if self.are_rows_in_sstable(node, sstable, partition_key, cluster_keys):
                exist = True
                assert not exist, f"Keys {cluster_keys} are left in sstable {sstable}"

    def are_rows_in_sstable(self, node: ScyllaNode, sstable_data_file: str, partition_key: Any, cluster_keys: list):
        """check that rows was removed from sstable

        Dump sstable *-Data.db file to json object, and check that partition with partition_key
        doesn't have rows with cluster_keys

        """

        json_data = dump_sstables(node, self.keyspace_name, self.table_name, [sstable_data_file])
        node.info(json_data)

        partition_found = False
        cluster_keys_exist = False
        for partition in json_data:
            if int(partition["key"]["value"]) == partition_key:
                partition_found = True
                rows = partition["clustering_elements"]
                cluster_key_values = set([int(row["key"]["value"]) for row in rows if row["type"] == "clustering-row"])
                if set(cluster_keys).issubset(cluster_key_values):
                    cluster_keys_exist = True
        if not partition_found:
            logger.error(f"Partition {partition_key} was not found")
        logger.debug(f"Clustering keys {cluster_keys} were {'' if cluster_keys_exist else 'not '}found")
        return cluster_keys_exist

    @pytest.mark.single_node
    def test_reshape_sstables_after_change_window_size_when_small_files_more_than_large(self):
        """Reshaping table of similar size but different number

        If window size was changed and a lot of sstables should be
        reshaped, the bucket of sstables should contain files
        of similar size, Latest bucket will contain all left sstables
        with no matter of size.

        1. Create 96 sstables with window size 1 minute and
           80 sstables with size ~1k, other 16 sstables with size ~2M
        2. Change window size to 96.
        3. Restart node
        4. Validate that reshape process sstables in buckets by size


        """
        self.window_size = 1
        self.window_unit = "MINUTES"
        self.new_window_size = 100
        self.new_window_unit = "MINUTES"
        self.small_file_size = 5_000
        self.sstable_size_distribution = [
            {"duration": 32, "sstable_size": 10},
            {"duration": 40, "sstable_size": 10},
            {"duration": 72, "sstable_size": 10},
            {"duration": 80, "sstable_size": 1_000_000},
            {"duration": 96, "sstable_size": 10},
        ]

        self.run_flow_generate_and_reshape_twcs_sstables()

    @pytest.mark.single_node
    def test_reshape_sstables_after_change_window_size_and_unit(self):
        """Reshaping table of similar size but different number

        If window size was changed and a lot of sstables should be
        reshaped, the bucket of sstables should contain files
        of similar size, Latest bucket will contain all left sstables
        with no matter of size.

        1. Create 120 sstables with window size 1 minute and
           80 sstables with size ~1k, other 40 sstables with size ~2M
        2. Change window size to 2 HOURS.
        3. Restart node
        4. Validate that reshape process sstables in buckets by size
        """
        self.window_size = 1
        self.window_unit = "MINUTES"
        self.new_window_size = 2
        self.new_window_unit = "HOURS"
        self.small_file_size = 5_000
        self.sstable_size_distribution = [
            {"duration": 32, "sstable_size": 10},
            {"duration": 40, "sstable_size": 1_000_000},
            {"duration": 72, "sstable_size": 10},
            {"duration": 80, "sstable_size": 1_000_000},
            {"duration": 96, "sstable_size": 1},
        ]

        self.run_flow_generate_and_reshape_twcs_sstables()

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    def test_enable_disable_optimized_query_for_twcs(self):
        """
        Enable/disable optimized algorithimns for timewindow queries
        and validate that same data return by queries and data are not
        corrupted
        """
        self.window_size = 5
        self.window_unit = "MINUTES"
        tw_query_result = {"enabled": [], "disabled": []}

        # Target 20 time windows = 6000s. let's increase default TTL making sure
        # that rows won't be expired while performing the test to  compare query
        # results with optimization enabled and disabled.
        default_ttl = 5 * 60 * 20

        self.cluster.populate(generate_cluster_topology(rack_num=2)).start(wait_for_binary_proto=True)

        [node1, _node2] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=2, ttl=default_ttl)
        # The test uses 5-minute windows, and run for a duration of 60 min
        # To minimize write amplification, as a result of flushing too often,
        # we want to limit the amount of flushes performed throughout the
        # test. A flush frequency of 100s is enough to trigger STCS on each
        # window, while not generating a ton of compaction work. So the test
        # can complete in a timely manner
        pks, _ = self._simulate_write_process_in_minutes(session, duration_minutes=60, flush_period_seconds=100, num_pks=30)

        tw_query_result["enabled"] = self.get_tw_query_results(session, pks)

        logger.info("Disable optimized queries")
        self._enable_optimized_tw_queries_config(session, enable=False)

        logger.info("Run tw queries with disabled optimized algorithms")
        tw_query_result["disabled"] = self.get_tw_query_results(session, pks)

        self.assert_tw_query_results(tw_query_result["enabled"], tw_query_result["disabled"])

        logger.info("Enable optimized queries")
        self._enable_optimized_tw_queries_config(session, enable=True)

        logger.info("Run tw queries with disabled optimized algorithms")
        tw_query_result["enabled"] = self.get_tw_query_results(session, pks)

        self.assert_tw_query_results(tw_query_result["enabled"], tw_query_result["disabled"])

    def run_flow_generate_and_reshape_twcs_sstables(self):
        [node1], session = self.prepare(1)
        self._create_ks_cl_with_twcs(session, rf=1)
        logger.debug("Simulate write process according sstable_size_distribution")

        self.simulate_twcs_write_data_per_minute_by_size(session, self.sstable_size_distribution, num_pks=[1])

        logger.debug("Compact sstables and validate window size per table")
        node1.nodetool(f"compact {self.keyspace_name} {self.table_name}")
        self._check_sstable_timestamps(node1)

        logger.debug("Sort sstables by size")
        sorted_sstables = self._group_sstables_by_size(node1, self.small_file_size)

        node1.nodetool(f"disableautocompaction {self.keyspace_name} {self.table_name}")

        logger.debug(f"Change window size to {self.new_window_size} {self.new_window_unit} and alter table with new settings")
        session.execute(
            f"ALTER TABLE {self.keyspace_name}.{self.table_name} WITH compaction = {{'class': 'TimeWindowCompactionStrategy','compaction_window_unit': '{self.new_window_unit}','compaction_window_size': {self.new_window_size} }}"
        )
        logger.debug("Restart node and waiting reshaping")
        node1.stop(wait_other_notice=True)

        mark = node1.mark_log()
        node1.start(wait_other_notice=True)
        found = node1.grep_log(rf"compaction - \[Reshape {self.keyspace_name}\.{self.table_name} .*\] Reshaping \[(.*)\]", from_mark=mark)

        logger.debug("Verify that reshaping was run for buckets with sstable similar size")
        assert len(found) > 0, f"Reshaping buckets found: {len(found)}, Reshape was not run"

        logger.debug("Check 1st buckets. they should contain only small files")
        for bucket in found[:-1]:
            list_of_reshaping_sstables = [sstable.split(":")[0] for sstable in bucket[1].group(1).strip().split(",") if "origin=reshape" not in sstable]
            for sstable in list_of_reshaping_sstables:
                assert sstable in sorted_sstables["small"], f"Sstable {sstable} with size {sorted_sstables['large']['sstable']} in wrong bucket"
                sorted_sstables["small"].pop(sstable)

        logger.debug("Check that last bucket contain all rest sstables")
        bucket = found[-1:][0]
        list_of_reshaping_sstables = [sstable.split(":")[0] for sstable in bucket[1].group(1).strip().split(",") if "origin=reshape" not in sstable]
        for sstable in list_of_reshaping_sstables:
            assert sstable in sorted_sstables["large"] or sstable in sorted_sstables["small"], f"Sstable {sstable} doesn't belong to any bound"

        logger.debug("Compact sstables and validate window size per table")
        node1.nodetool(f"compact {self.keyspace_name} {self.table_name}")

        logger.debug("Validate that after node started, All sstables compacted to 1 with window {self.new_window_size} {self.new_window_unit}")
        self._check_sstable_timestamps(node1, window_size=self.new_window_size, window_unit=self.new_window_unit)
        sstables = get_list_of_sstables(node1, self.keyspace_name, self.table_name)
        assert len(sstables) == 1, f"invalid number of sstables {len(sstables)}, Expected 1"

    def get_tw_query_results(self, session: Session, primary_keys: list[Any]) -> list[dict[str, Any]]:
        results = []
        queries = [
            f"SELECT * FROM {self.keyspace_name}.{self.table_name} WHERE pk = {primary_keys[0]} and ck > {30 * 60}",
            f"SELECT * FROM {self.keyspace_name}.{self.table_name} WHERE pk = {primary_keys[-1]} and ck > {45 * 60}",
            f"SELECT * FROM {self.keyspace_name}.{self.table_name} WHERE pk = {primary_keys[30 // 2]} and ck > {55 * 60}",
        ]
        pk_set = ",".join([str(pk) for pk in primary_keys[5:10]])
        queries.append(f"SELECT * FROM {self.keyspace_name}.{self.table_name} WHERE pk in ({pk_set}) and ck > {2 * 60} and ck < {4 * 60}")
        pk_set = ",".join([str(pk) for pk in primary_keys[25:27]])
        queries.append(f"SELECT * FROM {self.keyspace_name}.{self.table_name} WHERE pk in ({pk_set}) and ck > {33 * 60} and ck < {34 * 60}")
        queries.append(f"SELECT * FROM {self.keyspace_name}.{self.table_name}")

        for query in queries:
            logger.info(f"Query: {query}")
            st = time.perf_counter_ns()
            res = list(row._asdict() for row in session.execute(query))
            ft = time.perf_counter_ns()
            results.append({"query": query, "result": res, "time": ft - st})
        return results

    def _enable_optimized_tw_queries_config(self, session: Session, enable=True):
        if not enable:
            config = "'enable_optimized_twcs_queries': false"
        else:
            config = "'enable_optimized_twcs_queries': true"

        session.execute(
            f"ALTER TABLE {self.keyspace_name}.{self.table_name} with compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': '{self.window_unit}','compaction_window_size': {self.window_size},{config} }}"
        )

    @staticmethod
    def assert_tw_query_results(optimize_enable, optimize_disabled):
        for results_enabled, results_disabled in zip(optimize_enable, optimize_disabled):
            logger.debug(f"Query's time execution for optimized query {results_enabled['time']} and not optimized {results_disabled['time']}")

            assert results_enabled["query"] == results_disabled["query"], f"Not same queries {results_enabled['query']} != {results_disabled['query']}"

            diff = DeepDiff(t1=results_enabled["result"], t2=results_disabled["result"])
            for t in ["iterable_item_added", "iterable_item_removed", "dictionary_item_added", "dictionary_item_removed"]:
                assert not diff.get(t), f"{t}: {len(diff.get(t))}, first 5 differences:\n{pformat(dict(list(diff.get(t).items())[:5]))}"
            assert not diff, f"Return results are not the same:\n{pformat(diff)}"

    def simulate_twcs_write_data_per_minute_by_size(self, session, sstable_size_distribution, num_pks):
        prev_min = 0
        for period in sstable_size_distribution:
            self._simulate_write_process_in_minutes(session, duration_minutes=period["duration"], start_from_minute=prev_min, flush_period_seconds=30, num_pks=num_pks, size=period["sstable_size"])
            prev_min = period["duration"]

    def _group_sstables_by_size(self, node: ScyllaNode, size_criteria: int) -> dict[str, dict[str, int]]:
        sstables = get_list_of_sstables(node, self.keyspace_name, self.table_name, suffix="-Data.db")
        sorted_sstables = {"small": {}, "large": {}}
        for sstable in sorted(sstables):
            size = os.path.getsize(sstable)
            if size <= size_criteria:
                sorted_sstables["small"].update({sstable: size})
            else:
                sorted_sstables["large"].update({sstable: size})

        return sorted_sstables


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestValidationCompaction(CompactionAdditionalTester):
    KS = "ks"
    CF = "cf"
    CF_2 = "cf2"
    RF = 1
    CORRUPT_DATA_FILE_NAME = "mc-1-big-Data.db"
    CORRUPT_DATA_FILE_DIR = Path("test-sstables/sstable_with_invalid_fragment/ks/cf-test")
    CORRUPT_DATA_FILE_PATH = CORRUPT_DATA_FILE_DIR / CORRUPT_DATA_FILE_NAME
    DATA_FILE_NAME = "md-1-big-Data.db"
    PK19_PATTERN = r"\x19\x00\x00\x00 \(\{key:\s*pk\{000419000000\},\s*token:\s*-5674409923619649499\}\)"
    PK06_PATTERN = r"\x06\x00\x00\x00 \(\{key:\s*pk\{000406000000\},\s*token:\s*-5566252076597558760\}\)"
    CK3_PATTERN = r"\{position:\s*clustered,\s*ckp\{000400000003\},\s*0\}"
    CK5_PATTERN = r"\{position:\s*clustered,\s*ckp\{000400000005\},\s*0\}"
    REGEX_PATTERNS = {
        "validation_start": r"compaction - Scrubbing in validate mode",
        "invalid_partition": rf"Invalid partition {PK19_PATTERN},?\s*partition is out-of-order compared to previous partition {PK06_PATTERN}" rf"|out-of-order partition key {PK19_PATTERN},?\s*previous partition key was {PK06_PATTERN}",
        "invalid_clustering_row": rf"Invalid clustering row fragment with key 3 \({CK3_PATTERN}\) in partition {PK06_PATTERN},?\s*"
        rf"fragment is out-of-order compared to previous clustered fragment with key 5 \({CK5_PATTERN}\)"
        rf"|out-of-order clustering row at position {CK3_PATTERN} in partition {PK06_PATTERN},?\s*"
        rf"previous clustering element was {CK5_PATTERN} at position clustering row",
        "validation_finish_invalid": r"Finished scrubbing in validate mode.*(sstable(\(s\)|s) are|sstable is) invalid",
        "validation_finish_valid": r"Finished scrubbing in validate mode.*(sstable(\(s\)|s) are|sstable is) valid",
    }

    def test_validation_compaction_detects_sstable_corruption(self):
        """
        The test checks whether running a validation compaction identifies
        corrupted fragments in a corrupted sstable, without modifying the
        sstable.

        Test steps:
        1. Create test keyspace and column families.
        2. Load a corrupted sstable for a column family ("cf").
        3. Trigger the validation compaction using the API.
        4. Assert that following the compaction:
        - the corrupted sstable files were moved to the quarantine dir
        - the number of sstable files before and after the compaction is the
        same
        5. Assert that the corrupted fragments were reported
        in the logs.
        """
        self.ignore_log_patterns += ["([Ii]nvalid|out-of-order) clustering row", "([Ii]nvalid|out-of-order) partition", "(Sscrub) compaction ks.cf.*"]

        node, session, storage_service_client = self._prepare()
        create_ks(session=session, name=self.KS, rf=self.RF)
        create_cf(session=session, name=self.CF, columns={"ck": "int", "s": "int", "v": "int"}, key_name="pk", key_type="text", primary_key="pk, ck", compaction_strategy="NullCompactionStrategy", debug_query=True)
        node.flush()
        cf_dir = Path(get_node_cf_dir(node=node, ks_name=self.KS, cf_name=self.CF))
        quarantined_sstables_dir = cf_dir / "quarantine"
        logger.debug("Copying the sstables with invalid fragment from source directory: %s to table directory: %s...", self.CORRUPT_DATA_FILE_DIR, cf_dir)
        node.stop()
        copy_files_to(self.CORRUPT_DATA_FILE_DIR, cf_dir)
        node.start()
        pre_scrub_file_list = [item for item in cf_dir.glob("*") if item.is_file()]
        storage_service_client.scrub_ks_cf(keyspace=self.KS, cf=self.CF, scrub_mode="VALIDATE")
        quarantined_file_list = [item for item in quarantined_sstables_dir.glob("*") if item.is_file()]

        assert check_file_lists_are_equal(file_list_a=pre_scrub_file_list, file_list_b=quarantined_file_list), "Pre scrub file list was expected to be the same as quarantined file list, but was not"
        self.validate_log_patterns(node=node, patterns=[self.REGEX_PATTERNS["validation_start"], self.REGEX_PATTERNS["invalid_partition"], self.REGEX_PATTERNS["invalid_clustering_row"], self.REGEX_PATTERNS["validation_finish_invalid"]])

    def test_validation_compaction_with_valid_sstable(self):
        """
        The test verifies whether running a validation compaction on
        a valid sstable does correctly informs of the sstable's validity
        and avoids modifying the sstable.

        Test steps:
        1. Create test keyspace and column families.
        2. Populate a column family with test data.
        3. Trigger the validation compaction using the API.
        4. Assert that following the compaction the sstable
        for the populated column family was not modified, i.e.
        the same sstable files are present in the table dir.
        5. Assert that no corrupted fragments were reported
        in the logs and the sstable was marked as valid.
        """
        node, session, storage_service_client = self._prepare()
        create_ks(session=session, name=self.KS, rf=self.RF)
        create_c1c2_table(session)
        insert_c1c2(session, n=10_000)
        node.flush()
        cf_dir = Path(get_node_cf_dir(node=node, ks_name=self.KS, cf_name=self.CF))
        pre_compaction_sstable_file_list = [item for item in cf_dir.glob("*") if item.is_file()]
        storage_service_client.scrub_ks_cf(keyspace=self.KS, cf=self.CF, scrub_mode="VALIDATE")
        post_compaction_sstable_file_list = [item for item in cf_dir.glob("*") if item.is_file()]

        assert check_file_lists_are_equal(file_list_a=pre_compaction_sstable_file_list, file_list_b=post_compaction_sstable_file_list), "Pre-scrub file list was expected to be the same as post-scrub file list, but was not"
        self.validate_log_patterns(node=node, patterns=[self.REGEX_PATTERNS["validation_start"], self.REGEX_PATTERNS["validation_finish_valid"]])

    def _prepare(self):
        [node], session = self.prepare(
            1,
            configuration_options={
                "tablets_initial_scale_factor": 1,
            },
        )
        storage_service_client = StorageServiceClient(node=node)
        return node, session, storage_service_client

    @staticmethod
    def validate_log_patterns(node: Node, patterns: list[str]):
        all_found = True
        for pattern in patterns:
            if not node.grep_log(pattern):
                node.error(f"Could not find pattern in log: {pattern}")
                all_found = False
        assert all_found


class TestLCSSSTablePromotion(CompactionAdditionalTester):
    KS = "ks"
    CF = "cf"
    LCS = {"class": CompactionStrategy.LEVELED.value, "sstable_size_in_mb": 1}
    STCS = {"class": CompactionStrategy.SIZE_TIERED.value}
    COMPACTION_CONVERGE_ITERATIONS = 10

    def _get_table_levels_after_convergence(self, node: Node) -> list[int]:
        levels = []
        for _ in range(self.COMPACTION_CONVERGE_ITERATIONS):
            new_levels = self._get_table_levels(node)
            if levels != new_levels:
                levels = new_levels
            else:
                break
            node.wait_for_compactions(self.KS, self.CF)
        return levels

    @pytest.mark.dtest_full
    def test_lcs_sstable_promotion(self):
        """
        This test validates that LCS adheres to the restrictions
        on promoting sstables to higher levels. The basic restriction
        is that when promoting sstables to higher levels, for sstable count of
        level L: L <= 10 x L-1

        Test steps:
        1. Create a single-node cluster.
        2. Create keyspace and column family with a small sstable
        size value for test efficiency.
        3. Populate the cluster with data.
        4. Flush the data to sstables.
        5. Wait for Scylla to finish compacting the sstables.
        6. Assert that the sstables levels conform to the LCS
        promotion restriction, i.e.:
        - sstables are not all in the top level
        - the distribution conforms to the L <= 10 x L-1 restriction

        Scylla commit: 9de7abdc80721c14663fc698c7132a0dce878c18
        """
        node, session, _ = self._prepare()
        create_ks(session=session, name=self.KS, rf=1)
        create_cf(session=session, name=self.CF, columns={"c1": "text", "c2": "text"}, compaction=self.LCS)
        # Reduced from 1M/100K to 50K/10K rows - with 1MB sstable size this is sufficient
        # to create multiple LCS levels while significantly reducing test time
        num_rows = 10_000 if node.scylla_mode() == "debug" else 50_000
        # Increased concurrency from default 20 to 50 for faster insertion
        insert_c1c2(session=session, n=num_rows, concurrency=50)
        node.flush()
        levels = self._get_table_levels_after_convergence(node)
        self._check_space_amplification(node, levels)

    def test_lcs_table_promotion_major_compaction(self):
        node, session, storage_service_client = self._prepare()
        create_ks(session=session, name="ks", rf=1)
        create_cf(session=session, name="cf", columns={"c1": "text", "c2": "text"}, compaction=self.LCS)
        node.nodetool(f"disableautocompaction {self.KS} {self.CF}")
        # Reduced from 1M/100K to 50K/10K rows for faster test execution
        num_rows = 10_000 if node.scylla_mode() == "debug" else 50_000
        # Increased concurrency from default 20 to 50 for faster insertion
        insert_c1c2(session=session, n=num_rows, concurrency=50)
        node.flush()

        storage_service_client.compact_ks_cf(keyspace=self.KS, cf=self.CF)
        levels = self._get_table_levels_after_convergence(node)
        self._check_space_amplification(node, levels)

    def test_lcs_table_promotion_after_stcs_migration(self):
        node, session, _ = self._prepare()
        create_ks(session=session, name=self.KS, rf=1)
        create_cf(session=session, name=self.CF, columns={"c1": "text", "c2": "text"}, compaction=self.STCS)
        # Reduced from [20K, 80K, 150K, 250K, 500K] to smaller values for faster execution
        # The test goal is to verify promotion behavior, not stress test with large data
        keys_to_insert = [5_000, 15_000, 30_000, 50_000, 80_000]
        if node.scylla_mode() == "debug":
            keys_to_insert = [i // 10 for i in keys_to_insert]

        for item in keys_to_insert:
            # Increased concurrency from default 20 to 50 for faster insertion
            insert_c1c2(session=session, n=item, concurrency=50)
            node.flush()

        session.execute(f"ALTER TABLE ks.cf WITH compaction={self.LCS}")
        node.nodetool(f"refresh {self.KS} {self.CF}")
        levels = self._get_table_levels_after_convergence(node)
        self._check_space_amplification(node, levels)

    def _get_table_levels(self, node: Node) -> list[int]:
        """
        Use the node's REST API and get the sstable levels info from it.
        """
        result = run_rest_api(run_on_node=node, cmd=f"/column_family/sstables/per_level/{self.KS}:{self.CF}", api_method="GET")
        # API call returns a list of sstables levels, e.g.: [0, 5, 15]
        return result.json()

    def _prepare(self):
        [node], session = self.prepare(1)
        storage_service_client = StorageServiceClient(node=node)
        return node, session, storage_service_client

    def _check_space_amplification(self, node: ScyllaNode, levels: list[int]):
        # The user is promissed ~1.1 space amplification. Increase space amplification factor from 1.1 to 1.2 for safety margin
        # https://github.com/scylladb/scylla-dtest/issues/4702#issuecomment-2346062445
        MAX_AMPLIFICATION = 1.2

        result = run_rest_api(run_on_node=node, cmd=f"/storage_service/sstable_info", api_method="GET", params={"keyspace": self.KS, "cf": self.CF})
        # API call returns a dict of the form:
        # [{'keyspace': 'ks', 'table': 'cf', 'sstables': [{'size': 1389093, 'data_size': ..., 'index_size': ..., 'filter_size': ..., 'timestamp': ..., 'generation': ..., 'level': 2, 'version': ...}, ...]}]
        sstable_info = result.json()

        nr_levels = len(levels)
        data_size = [0] * nr_levels
        for sstable in sstable_info[0]["sstables"]:
            data_size[sstable["level"]] += sstable["size"]

        amplification = [0] * (nr_levels - 1)
        # calculate the amplification using the formula
        # amplification = (size(level) + size(level+1)) / size(level+1)
        # data_size is usually like [0, 12641608, 49752323]
        for i in range(nr_levels - 1):
            if data_size[i + 1] != 0:
                amplification[i] = (data_size[i + 1] + data_size[i]) / data_size[i + 1]
            elif data_size[i] == 0 and data_size[i + 1] == 0:
                # if two consecutive levels have size 0, then we set amplification to 1
                amplification[i] = 1
            else:
                # in the unlikely scenario that we have sizes like [100, 0, 1500], give a better error than ZeroDivisionError
                pytest.fail(f"Invalid sstable info: {levels=}, {data_size=}")

        assert all([a < MAX_AMPLIFICATION for a in amplification]), dedent(
            f"""\
            Data amplification between levels should be lower than {MAX_AMPLIFICATION}:
                {", ".join([f"level {i + 1} -> level {i}: {amplification[i]:.3}" for i in range(nr_levels - 1)[::-1]])}
                {levels=}
                {data_size=}
            """
        )
