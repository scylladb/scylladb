# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import requests
import time
from collections import defaultdict
from dataclasses import dataclass, field

from ..cqlpy.util import new_test_table, new_test_keyspace, sleep_till_whole_second

@dataclass
class TombstonePurgeStats:
    attempts: int = 0
    failures_due_to_memtables: int = 0
    failures_due_to_other_sstables: int = 0


@dataclass
class SStablesStats:
    input: list[dict] = field(default_factory=list)
    output: list[dict] = field(default_factory=list)


def waitAndGetCompleteCompactionHistory(rest_api, table):
    # cql-pytest/run.py::run_scylla_cmd() passes "--smp 2" to scylla, so we
    # use this value to ensure compaction results from all shards arrived
    # to the table.
    SCYLLA_SMP_COUNT = 2
    ks, cf = table.split('.')
    while True:
        response = rest_api.send("GET", "compaction_manager/compaction_history")
        assert response.status_code == requests.codes.ok

        table_entry_count = sum(1 for data in response.json() if data["ks"] == ks and data["cf"] == cf)
        if table_entry_count == SCYLLA_SMP_COUNT:
            return response

        assert table_entry_count < SCYLLA_SMP_COUNT
        time.sleep(0.2)


def extractTombstonePurgeStatistics(response, ks) -> TombstonePurgeStats:
    '''
    Extract compaction history statistics for a given keyspace ks from the response and squash
    it as there will be as many items as the number of shards.
    '''
    stats = TombstonePurgeStats()
    for data in response.json():
        if data["ks"] == ks:
            stats.attempts += data["total_tombstone_purge_attempt"]
            stats.failures_due_to_memtables += data["total_tombstone_purge_failure_due_to_overlapping_with_memtable"]
            stats.failures_due_to_other_sstables += data["total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable"]

    return stats


def extractSStablesStatistics(response, ks) -> SStablesStats:
    '''
    Extract compaction history statistics for a given keyspace ks from the response and squash
    it as there will be as many items as the number of shards.
    '''
    stats = SStablesStats()
    for data in response.json():
        if data["ks"] == ks:
            stats.input += data["sstables_in"]
            stats.output += data["sstables_out"]

    return stats


def extractRowsMergedAsSortedList(response, ks):
    '''
    Extract rows_merged statistics for a given keyspace ks from the response and squash it as
    there will be as many items as the number of shards. Return a sorted list of the form
    [{"key": <number of sstables>, "value": <number ob rows>}...]
    '''
    total = defaultdict(int)
    for data in response.json():
        if data["ks"] == ks:
            for rows in data["rows_merged"]:
                total[rows["key"]] += rows["value"]

    return [{"key": key, "value": value} for key, value in sorted(total.items())]


def populateSomeData(cql, cf: str, pk_range: tuple[int], timestamp: int | None = None, step: int = 0):
    stmt = cql.prepare(f"INSERT INTO {cf} (pk, ck, v) VALUES (?, ?, ?) {'USING TIMESTAMP ?' if timestamp else ''}")
    for pk in range(*pk_range):
        for ck in range(1, 6):
            timestamp = timestamp + step if timestamp is not None else None
            cql.execute(stmt, [pk, ck*11+100, 0], timestamp)


def alterSomeData(cql, cf: str, timestamp: int | None = None):
    using_timestamp = f"USING TIMESTAMP {timestamp}" if timestamp else ''

    cql.execute(f"DELETE FROM {cf} {using_timestamp} WHERE pk=1 and ck=122")
    cql.execute(f"DELETE FROM {cf} {using_timestamp} WHERE pk=5 and ck=155")
    cql.execute(f"DELETE FROM {cf} {using_timestamp} WHERE pk=3 and ck>111 AND ck<144")
    cql.execute(f"UPDATE {cf} {using_timestamp} SET v=100 WHERE pk=2 AND ck=122")
    cql.execute(f"DELETE FROM {cf} {using_timestamp} WHERE pk=5")


def test_compactionhistory_rows_merged_null_compaction_strategy(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)", "WITH compaction = {'class': 'NullCompactionStrategy'};") as cf:
            populateSomeData(cql, cf, (1, 6))
            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            alterSomeData(cql, cf)
            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            response = rest_api.send("POST", f"storage_service/keyspace_compaction/{ks}")
            assert response.status_code == requests.codes.ok

            response = waitAndGetCompleteCompactionHistory(rest_api, cf)
            assert extractRowsMergedAsSortedList(response, ks) == [{"key": 1, "value": 27}, {"key": 2, "value": 10}]


def test_compactionhistory_rows_merged_time_window_compaction_strategy(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        compaction_opt = "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'compaction_window_size': 1}"
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                            f"WITH compaction = {compaction_opt};") as cf:
            timestamp = int(time.time())

            # Spread data across 2 windows by simulating a write process. `USING TIMESTAMP` is
            # provided to distribute the writes in the first one-minute window while updates and
            # deletes are propagated into the second 1-minute window.
            populateSomeData(cql, cf, (1, 6), timestamp - 60, 1)
            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            alterSomeData(cql, cf)
            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            response = rest_api.send("POST", f"storage_service/keyspace_compaction/{ks}")
            assert response.status_code == requests.codes.ok

            response = waitAndGetCompleteCompactionHistory(rest_api, cf)
            assert extractRowsMergedAsSortedList(response, ks) == [{"key": 1, "value": 27}, {"key": 2, "value": 10}]


def test_compactionhistory_tombstone_purge_statistics(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                            "WITH compaction = {'class': 'NullCompactionStrategy'} AND tombstone_gc = {'mode': 'immediate'};") as cf:
            timestamp = int(time.time())

            populateSomeData(cql, cf, (1, 11), timestamp - 10)
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            populateSomeData(cql, cf, (11, 21), timestamp - 5)
            alterSomeData(cql, cf, timestamp - 5)
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            # Sleep a second to let the commitlog minimum gc time, in seconds, be greater than the tombstone deletion time
            sleep_till_whole_second(1)
            response = rest_api.send("POST", f"storage_service/keyspace_compaction/{ks}")
            assert response.status_code == requests.codes.ok

            response = waitAndGetCompleteCompactionHistory(rest_api, cf)
            stats = extractTombstonePurgeStatistics(response, ks)
            assert stats == TombstonePurgeStats(5, 0, 0)

            stats = extractSStablesStatistics(response, ks)
            assert len(stats.input) == 4 and len(stats.output) == 2


def test_compactionhistory_tombstone_purge_statistics_overlapping_with_memtable(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                            "WITH compaction = {'class': 'NullCompactionStrategy'} AND tombstone_gc = {'mode': 'immediate'};") as cf:
            timestamp = int(time.time())

            populateSomeData(cql, cf, (11, 21), timestamp - 5)
            alterSomeData(cql, cf, timestamp - 5)
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            # Sleep a second to let the commitlog minimum gc time, in seconds, be greater than the tombstone deletion time
            sleep_till_whole_second(1)
            populateSomeData(cql, cf, (1, 11), timestamp - 10)
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            # Do not flush to keep it in memtable
            cql.execute(f"UPDATE {cf} USING TIMESTAMP {timestamp - 7} SET v=100 WHERE pk=1 AND ck=122")
            cql.execute(f"UPDATE {cf} USING TIMESTAMP {timestamp - 7} SET v=100 WHERE pk=7 AND ck=122")

            response = rest_api.send("POST", f"storage_service/keyspace_compaction/{ks}", {"flush_memtables": "false"})
            assert response.status_code == requests.codes.ok

            response = waitAndGetCompleteCompactionHistory(rest_api, cf)
            stats = extractTombstonePurgeStatistics(response, ks)
            assert stats == TombstonePurgeStats(5, 1, 0)


def test_compactionhistory_tombstone_purge_statistics_overlapping_with_other_sstables(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        compaction_opt = "{'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'min_sstable_size': 0}"
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                            f"WITH compaction = {compaction_opt} AND tombstone_gc = {{'mode': 'immediate'}};") as cf:
            timestamp = int(time.time())

            cql.execute(f"UPDATE {cf} USING TIMESTAMP {timestamp - 7} SET v=100 WHERE pk=1 AND ck=122")
            cql.execute(f"UPDATE {cf} USING TIMESTAMP {timestamp - 7} SET v=100 WHERE pk=7 AND ck=122")
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            # Now produce two additional sstable that will get into the same bucket
            # and hence be compacted together but not with the sstable from above.
            populateSomeData(cql, cf, (11, 21), timestamp - 5)
            alterSomeData(cql, cf, timestamp - 5)
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            # Sleep a second to let the commitlog minimum gc time, in seconds, be greater than the tombstone deletion time
            sleep_till_whole_second(1)
            populateSomeData(cql, cf, (1, 11), timestamp - 10)
            response = rest_api.send("POST", "storage_service/flush")
            assert response.status_code == requests.codes.ok

            response = waitAndGetCompleteCompactionHistory(rest_api, cf)

            stats = extractTombstonePurgeStatistics(response, ks)
            assert stats == TombstonePurgeStats(5, 0, 1)

            stats = extractSStablesStatistics(response, ks)
            assert len(stats.input) == 4 and len(stats.output) == 2
