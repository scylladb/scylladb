# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import requests
import sys
import time
from collections import defaultdict

# Use the util.py library from ../cqlpy:
sys.path.insert(1, sys.path[0] + '/test/cqlpy')
from ..cqlpy.util import new_test_table, new_test_keyspace


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


def test_compactionhistory_rows_merged_null_compaction_strategy(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)", "WITH compaction = {'class': 'NullCompactionStrategy'};") as cf:
            stmt = cql.prepare(f"INSERT INTO {cf} (pk, ck, v) VALUES (?, ?, ?)")
            cql.execute(stmt, [1, 111, 0])
            cql.execute(stmt, [1, 122, 0])
            cql.execute(stmt, [1, 133, 0])
            cql.execute(stmt, [2, 222, 0])
            cql.execute(stmt, [3, 333, 0])
            cql.execute(stmt, [3, 344, 0])
            cql.execute(stmt, [3, 355, 0])
            cql.execute(stmt, [3, 366, 0])
            cql.execute(stmt, [3, 377, 0])
            cql.execute(stmt, [4, 444, 0])
            cql.execute(stmt, [5, 555, 0])

            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            cql.execute(f"DELETE FROM {cf} WHERE pk=1 and ck=122")
            cql.execute(f"DELETE FROM {cf} WHERE pk=5 and ck=555")
            cql.execute(f"DELETE FROM {cf} WHERE pk=3 and ck>333 AND ck <366")
            cql.execute(f"UPDATE {cf} SET v=100 WHERE pk=2 AND ck=222")
            cql.execute(f"DELETE FROM {cf} WHERE pk=5")
            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            response = rest_api.send("POST", "storage_service/compact")
            assert response.status_code == requests.codes.ok

            response = rest_api.send("GET", "compaction_manager/compaction_history")
            assert response.status_code == requests.codes.ok
            assert extractRowsMergedAsSortedList(response, ks) == [{"key": 1, "value": 13}, {"key": 2, "value": 10}]


def test_compactionhistory_rows_merged_time_window_compaction_strategy(cql, rest_api):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "pk int, ck int, v int, PRIMARY KEY (pk, ck)", "WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'compaction_window_size': 1};") as cf:
            rest_api.send("POST", f"/column_family/autocompaction/{ks}:{cf.split('.')[-1]}")
            current_time = int(time.time())

            # Spread data across 2 windows by simulating a write process. `USING TIMESTAMP` is
            # provided to distribute the writes in the first one-minute window while updates and
            # deletes are propagated into the second 1-minute window.
            stmt = cql.prepare(f"INSERT INTO {cf} (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?")
            cql.execute(stmt, [1, 111, 0, current_time - 60 + 1])
            cql.execute(stmt, [1, 122, 0, current_time - 60 + 2])
            cql.execute(stmt, [1, 133, 0, current_time - 60 + 3])
            cql.execute(stmt, [2, 222, 0, current_time - 60 + 4])
            cql.execute(stmt, [3, 333, 0, current_time - 60 + 5])
            cql.execute(stmt, [3, 344, 0, current_time - 60 + 6])
            cql.execute(stmt, [3, 355, 0, current_time - 60 + 7])
            cql.execute(stmt, [3, 366, 0, current_time - 60 + 8])
            cql.execute(stmt, [3, 377, 0, current_time - 60 + 9])
            cql.execute(stmt, [4, 444, 0, current_time - 60 + 10])
            cql.execute(stmt, [5, 555, 0, current_time - 60 + 11])

            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            cql.execute(f"DELETE FROM {cf} WHERE pk=1 and ck=122")
            cql.execute(f"DELETE FROM {cf} WHERE pk=5 AND ck=555")
            cql.execute(f"DELETE FROM {cf} WHERE pk=3 and ck>333 AND ck <366")
            cql.execute(f"UPDATE {cf} SET v=100 WHERE pk=2 AND ck=222")
            cql.execute(f"DELETE FROM {cf} WHERE pk=5")
            response = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            assert response.status_code == requests.codes.ok

            response = rest_api.send("POST", "storage_service/compact")
            assert response.status_code == requests.codes.ok

            response = rest_api.send("GET", "compaction_manager/compaction_history")
            assert response.status_code == requests.codes.ok
            assert extractRowsMergedAsSortedList(response, ks) == [{"key": 1, "value": 13}, {"key": 2, "value": 10}]
