# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import os
import pytest
import sys
import requests
import threading
import time

from ..cqlpy.util import new_test_table, new_test_keyspace
from test.rest_api.rest_util import scylla_inject_error

def do_test_column_family_attribute_api_table(cql, this_dc, rest_api, api_name):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]

            resp = rest_api.send("GET", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("DELETE", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() == False

            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() == True

            # missing table
            resp = rest_api.send("POST", f"column_family/{api_name}/")
            assert resp.status_code == requests.codes.not_found

            # bad syntax 1
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}")
            assert resp.status_code == requests.codes.bad_request
            assert resp.json()['message'] == 'Column family name should be in keyspace:column_family format'

            # bad syntax 2
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}.{test_table}")
            assert resp.status_code == requests.codes.bad_request
            assert resp.json()['message'] == 'Column family name should be in keyspace:column_family format'

            # non-existing keyspace
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}XXX:{test_table}")
            assert resp.status_code == requests.codes.bad_request
            assert "Can't find a column family" in resp.json()['message']

            # non-existing table
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}:{test_table}XXX")
            assert resp.status_code == requests.codes.bad_request
            assert "Can't find a column family" in resp.json()['message']

def test_column_family_auto_compaction_table(cql, this_dc, rest_api):
    do_test_column_family_attribute_api_table(cql, this_dc, rest_api, "autocompaction")

def test_column_family_tombstone_gc_api(cql, this_dc, rest_api):
    do_test_column_family_attribute_api_table(cql, this_dc, rest_api, "tombstone_gc")

def test_column_family_tombstone_gc_correctness(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]

            # 1 Allow tombstones to be purged by compaction right away
            cql.execute(f"ALTER TABLE {test_keyspace}.{test_table} with gc_grace_seconds=0")

            # 2 Disable tombstone purge using API
            resp = rest_api.send("DELETE", f"column_family/tombstone_gc/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/tombstone_gc/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() == False

            # 3 Create partition tombstone
            cql.execute(f"DELETE from {test_keyspace}.{test_table} WHERE a = 1")

            # 4 Trigger major compaction on that sstable (flushes memtable by default)
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            # 5 Expect that tombstone was not purged
            resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
            resp.raise_for_status()
            assert "-Data.db" in resp.json()[0]

def test_column_family_major_compaction(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}", params={"flush_memtables": "true"})
            resp.raise_for_status()
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}", params={"flush_memtables": "false"})
            resp.raise_for_status()

def test_sstables_by_key_reader_closed(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]

            cql.execute(f"INSERT INTO {test_keyspace}.{test_table} (a) VALUES (1)")
            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{test_keyspace}")
            resp.raise_for_status()

            # Check if index reader is closed on happy path.
            resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
            resp.raise_for_status()

            # Check if index reader is closed if exception is thrown.
            with scylla_inject_error(rest_api, "advance_lower_and_check_if_present"):
                resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
                assert resp.status_code == 500

# Test how reading and changing compaction strategy works
def test_column_family_compaction_strategy(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)", extra = "with compaction = { 'class': 'NullCompactionStrategy' }") as t:
            table_name = t.replace('.', ':')

            def check_strategy(strategy):
                resp = rest_api.send("GET", f"column_family/compaction_strategy/{table_name}")
                resp.raise_for_status()
                assert resp.json() == strategy

            check_strategy("NullCompactionStrategy")

            resp = rest_api.send("POST", f"column_family/compaction_strategy/{table_name}", params={"class_name": "NoSuchCompactionStrategy"})
            assert resp.status_code == requests.codes.internal_server_error
            check_strategy("NullCompactionStrategy")

            for strategy in [ "IncrementalCompactionStrategy", "LeveledCompactionStrategy", "TimeWindowCompactionStrategy" ]:
                resp = rest_api.send("POST", f"column_family/compaction_strategy/{table_name}", params={"class_name": strategy})
                resp.raise_for_status()
                check_strategy(strategy)

            # SizeTieredCompactionStrategy is deprecated and is an alias of
            # IncrementalCompactionStrategy, so that's what the table ends up
            # running, and what this API reports back.
            resp = rest_api.send("POST", f"column_family/compaction_strategy/{table_name}", params={"class_name": "SizeTieredCompactionStrategy"})
            resp.raise_for_status()
            check_strategy("IncrementalCompactionStrategy")


# Test that the minimum partition size of a table, which nodetool cfstats
# shows, is the minimum of the shards that hold partitions of the table.
# The table has one partition, so only one shard holds a partition of the
# table, and a shard that holds no partition must not make the minimum 0.
def test_column_family_min_row_size(cql, this_dc, rest_api, storage_engine):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int", storage_engine.table_options) as t:
            test_table = t.split('.')[1]
            cql.execute(f"INSERT INTO {t} (pk, v) VALUES (1, 1)")
            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{test_keyspace}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/metrics/min_row_size/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() > 0

            resp = rest_api.send("GET", "column_family/metrics/min_row_size")
            resp.raise_for_status()
            assert resp.json() > 0


# Test that the column_family API reports the partition count and the
# partition sizes of a table. nodetool cfstats and nodetool tablehistograms
# read this API.
def test_column_family_partition_estimates(cql, this_dc, rest_api, storage_engine):
    key_count = 100
    value_size = 1000
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v text", storage_engine.table_options) as t:
            test_table = t.split('.')[1]

            # Returns the value of a metric of the table, which the server
            # computes over all shards.
            def get_metric(name):
                resp = rest_api.send("GET", f"column_family/metrics/{name}/{test_keyspace}:{test_table}")
                resp.raise_for_status()
                return resp.json()

            # Returns the partition count of the table and its mean partition
            # size.
            # The estimates of a normal table add up the partitions of each
            # sstable, so the function first compacts the memtable and the
            # sstables of the table, so that each partition is in one sstable
            # only. The index of a logstor table counts each key once, with or
            # without the compaction.
            def get_estimates():
                resp = rest_api.send("POST", f"storage_service/keyspace_compaction/{test_keyspace}", params={"cf": test_table})
                resp.raise_for_status()
                return (get_metric('estimated_row_count'), get_metric('mean_row_size'))

            # Returns the minimum and the maximum partition size of the table,
            # and the count of its partition size histogram. The caller must
            # call get_estimates() first, which compacts the table.
            def get_size_range():
                histogram = get_metric('estimated_row_size_histogram')
                return (get_metric('min_row_size'), get_metric('max_row_size'), sum(histogram['buckets']))

            assert get_estimates() == (0, 0)
            assert get_size_range() == (0, 0, 0)

            insert = cql.prepare(f"INSERT INTO {t} (pk, v) VALUES (?, ?)")
            value = 'x' * value_size
            for pk in range(key_count):
                cql.execute(insert, [pk, value])

            count, mean_size = get_estimates()
            assert count == key_count
            # The size of a partition adds the size of its key and of its
            # metadata to the size of its value.
            assert value_size < mean_size < 2 * value_size
            # The histogram counts the sizes in buckets, so the minimum and the
            # maximum are the bounds of buckets, and the mean is between them.
            min_size, max_size, histogram_count = get_size_range()
            assert 0 < min_size <= mean_size <= max_size
            assert histogram_count == key_count

            # An overwrite does not add a partition, and a delete leaves a
            # tombstone that both engines count as a partition. So neither
            # changes the count.
            # Compaction of a normal table does not purge a new tombstone, and
            # logstor never purges a tombstone, so the index keeps an entry
            # for the deleted key.
            for pk in range(key_count // 2):
                cql.execute(insert, [pk, value])
            for pk in range(key_count // 2, key_count):
                cql.execute(f"DELETE FROM {t} WHERE pk = {pk}")
            count, mean_size_after_deletes = get_estimates()
            assert count == key_count
            assert mean_size_after_deletes < mean_size
            # A tombstone is much smaller than a partition with a value, so it
            # is in a smaller bucket than the partitions that the deletes
            # replaced.
            min_size_after_deletes, max_size_after_deletes, histogram_count = get_size_range()
            assert min_size_after_deletes < min_size
            assert min_size_after_deletes <= mean_size_after_deletes <= max_size_after_deletes
            assert histogram_count == key_count

            cql.execute(f"TRUNCATE {t}")
            assert get_estimates() == (0, 0)
            assert get_size_range() == (0, 0, 0)


# Test that get_live|total_disk_space_used returns sane values
def test_sstables_total_disk_space_sanity(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]
            cql.execute(f"INSERT INTO {test_keyspace}.{test_table} (a) VALUES (1)")
            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{test_keyspace}")
            resp.raise_for_status()
            resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
            resp.raise_for_status()

            sstables = resp.json()
            assert len(sstables) > 0, "no sstables generated for a table"

            total_real_size = 0
            for sst in sstables:
                sstname = os.path.basename(sst).removesuffix('-big-Data.db')
                dirname = os.path.dirname(sst)
                print(f'Collecting {sstname}')
                for f in os.listdir(dirname):
                    if f.startswith(sstname):
                        size = os.stat(f'{dirname}/{f}').st_size
                        print(f'Add {f}, {size} bytes')
                        total_real_size += size

            print(f'Total real size : {total_real_size}')

            resp = rest_api.send("GET", f"column_family/metrics/total_disk_space_used/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            total_size = int(resp.json())
            print(f'Reported total size : {total_size}')
            assert total_size == total_real_size

            resp = rest_api.send("GET", f"column_family/metrics/live_disk_space_used/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            live_size = int(resp.json())
            print(f'Reported live size : {live_size}')
            assert live_size <= total_real_size
