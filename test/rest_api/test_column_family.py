# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import os
import pytest
import sys
import requests
import threading
import time

from cassandra.concurrent import execute_concurrent_with_args

from ..cqlpy.util import new_test_table, new_test_keyspace
from test.rest_api.rest_util import new_test_snapshot, scylla_inject_error

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

def get_metric(rest_api, metric, table):
    resp = rest_api.send("GET", f"column_family/metrics/{metric}/{table}")
    resp.raise_for_status()
    return resp.json()

def get_sstable_count_per_level(rest_api, table):
    resp = rest_api.send("GET", f"column_family/sstables/per_level/{table}")
    resp.raise_for_status()
    return resp.json()

def assert_ihistogram(h, min_count=1):
    for key in ("count", "sum", "min", "max", "mean", "variance"):
        assert key in h, f"{key} missing in histogram {h}"
    assert h["count"] >= min_count, h
    if h["count"] > 0:
        assert h["min"] <= h["mean"] <= h["max"], h

def assert_moving_average_histogram(h, min_count=1):
    assert "meter" in h and "hist" in h, h
    assert_ihistogram(h["hist"], min_count)
    assert h["meter"]["count"] >= min_count, h

def assert_estimated_histogram(h, min_count=1):
    assert len(h["buckets"]) == len(h["bucket_offsets"]) + 1, h
    assert sum(h["buckets"]) >= min_count, h
    assert h["buckets"][-1] == 0, f"histogram overflows: {h}"

# Exercise every per-table metric which "nodetool tablestats" (cfstats) and
# "nodetool tablehistograms" (cfhistograms) read, against a table which has
# data both in sstables and in the memtable, has row tombstones, and was read.
# Replaces the REST-level checks of dtest's TestNodetool::test_cfstats,
# test_cfhistograms, test_sstables_in_tablehistograms and the tablestats
# assertions of TestSnapshotOptions::test_snapshot_skip_flush.
# The keyspace uses vnodes, so the data is spread over all shards and the
# per-shard results of every metric are merged.
def test_column_family_metrics_endpoints(cql, rest_api, test_keyspace_vnodes):
    ks = test_keyspace_vnodes
    value = "x" * 200   # compressible, so that compression_ratio < 1
    with new_test_table(cql, ks, "pk int, ck int, v text, PRIMARY KEY (pk, ck)",
                        "WITH compression = {'sstable_compression': 'LZ4Compressor'}") as t:
        table = t.replace('.', ':')
        cf = t.split('.')[1]
        # Keep the sstables of the two flushes below apart.
        resp = rest_api.send("DELETE", f"column_family/autocompaction/{table}")
        resp.raise_for_status()

        def flush():
            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            resp.raise_for_status()

        insert = cql.prepare(f"INSERT INTO {t} (pk, ck, v) VALUES (?, ?, ?)")
        execute_concurrent_with_args(cql, insert, [(pk, ck, value) for pk in range(20) for ck in range(10)])
        for pk in range(10):
            list(cql.execute(f"SELECT * FROM {t} WHERE pk = {pk}"))

        assert get_metric(rest_api, "memtable_columns_count", table) > 0
        assert get_metric(rest_api, "memtable_live_data_size", table) > 0
        assert get_metric(rest_api, "memtable_off_heap_size", table) > 0
        flush()
        assert get_metric(rest_api, "memtable_switch_count", table) >= 1

        # Second sstable: overwrite some rows, add a new partition, and add
        # row tombstones and a partition tombstone.
        execute_concurrent_with_args(cql, insert, [(pk, ck, value) for pk in range(10, 30) for ck in range(5)])
        for ck in range(5):
            cql.execute(f"DELETE FROM {t} WHERE pk = 2 AND ck = {ck}")
        cql.execute(f"DELETE FROM {t} WHERE pk = 3")
        assert get_metric(rest_api, "memtable_columns_count", table) > 0
        memtable_data_size = get_metric(rest_api, "memtable_live_data_size", table)
        assert memtable_data_size > 0
        assert get_metric(rest_api, "memtable_off_heap_size", table) > 0

        # A snapshot with skip_flush must leave the memtable alone.
        switch_count = get_metric(rest_api, "memtable_switch_count", table)
        with new_test_snapshot(rest_api, ks, cf, skip_flush=True):
            assert get_metric(rest_api, "memtable_switch_count", table) == switch_count
            assert get_metric(rest_api, "memtable_live_data_size", table) >= memtable_data_size

        flush()
        assert get_metric(rest_api, "memtable_switch_count", table) >= 2
        assert get_metric(rest_api, "pending_flushes", table) == 0

        # Read from the sstables (not the cache), so that bloom filters,
        # the sstables-per-read histogram and the scanned histograms are fed.
        for pk in range(30):
            list(cql.execute(f"SELECT * FROM {t} WHERE pk = {pk} BYPASS CACHE"))

        sstable_count = get_metric(rest_api, "live_ss_table_count", table)
        # One sstable per shard per flush.
        assert sstable_count >= 2
        per_level = get_sstable_count_per_level(rest_api, table)
        assert sum(per_level) == sstable_count

        assert get_metric(rest_api, "estimated_row_count", table) >= 30
        min_size = get_metric(rest_api, "min_row_size", table)
        mean_size = get_metric(rest_api, "mean_row_size", table)
        max_size = get_metric(rest_api, "max_row_size", table)
        assert max_size > 0
        assert min_size <= mean_size <= max_size

        compression_ratio = get_metric(rest_api, "compression_ratio", table)
        assert 0 < compression_ratio <= 1

        assert get_metric(rest_api, "bloom_filter_disk_space_used", table) > 0
        assert get_metric(rest_api, "bloom_filter_off_heap_memory_used", table) > 0
        assert get_metric(rest_api, "index_summary_off_heap_memory_used", table) > 0
        assert get_metric(rest_api, "compression_metadata_off_heap_memory_used", table) >= 0
        assert get_metric(rest_api, "bloom_filter_false_positives", table) >= 0
        assert get_metric(rest_api, "recent_bloom_filter_false_positives", table) >= 0
        assert 0 <= float(get_metric(rest_api, "bloom_filter_false_ratio", table)) <= 1
        # The "recent" ratio is reset by every read of it.
        assert 0 <= float(get_metric(rest_api, "recent_bloom_filter_false_ratio", table)) <= 1
        assert float(get_metric(rest_api, "recent_bloom_filter_false_ratio", table)) == 0

        assert get_metric(rest_api, "pending_compactions", table) >= 0
        assert get_metric(rest_api, "live_disk_space_used", table) > 0
        assert get_metric(rest_api, "total_disk_space_used", table) > 0

        writes = 20 * 10 + 20 * 5 + 5 + 1
        reads = 10 + 30
        assert get_metric(rest_api, "write", table) >= writes
        assert get_metric(rest_api, "read", table) >= reads
        assert get_metric(rest_api, "write_latency", table) >= 0
        assert get_metric(rest_api, "read_latency", table) >= 0
        assert_moving_average_histogram(get_metric(rest_api, "write_latency/moving_average_histogram", table), writes)
        assert_moving_average_histogram(get_metric(rest_api, "read_latency/moving_average_histogram", table), reads)
        assert_ihistogram(get_metric(rest_api, "write_latency/histogram", table), writes)
        assert_ihistogram(get_metric(rest_api, "read_latency/histogram", table), reads)
        # Scylla doesn't collect these (nothing feeds column_family_stats::live_scanned
        # and tombstone_scanned), but nodetool tablestats reads them, so they must be
        # well-formed.
        assert_ihistogram(get_metric(rest_api, "live_scanned_histogram", table), 0)
        assert_ihistogram(get_metric(rest_api, "tombstone_scanned_histogram", table), 0)

        assert_estimated_histogram(get_metric(rest_api, "estimated_row_size_histogram", table), 30)
        assert_estimated_histogram(get_metric(rest_api, "estimated_column_count_histogram", table), 30)
        assert_estimated_histogram(get_metric(rest_api, "sstables_per_read_histogram", table))

        # The table is listed by GET /column_family/ (what nodetool tablestats
        # uses to find tables).
        resp = rest_api.send("GET", "column_family/")
        resp.raise_for_status()
        assert {"ks": ks, "cf": cf, "type": "ColumnFamilies"} in resp.json()

        # Space used by snapshots: once the snapshotted sstables were
        # compacted away, the snapshot holds the only copy of them.
        assert get_metric(rest_api, "snapshots_size", table) == 0
        with new_test_snapshot(rest_api, ks, cf):
            resp = rest_api.send("POST", f"column_family/major_compaction/{table}")
            resp.raise_for_status()
            assert get_metric(rest_api, "snapshots_size", table) > 0
            assert get_metric(rest_api, "live_ss_table_count", table) <= sstable_count
        assert get_metric(rest_api, "snapshots_size", table) == 0

        # Non-existing tables are reported as bad requests.
        for metric in ("live_ss_table_count", "read_latency/moving_average_histogram", "compression_ratio"):
            resp = rest_api.send("GET", f"column_family/metrics/{metric}/{ks}:{cf}XXX")
            assert resp.status_code == requests.codes.bad_request
            assert "Can't find a column family" in resp.json()['message']

# The size-based compaction backlog and pending compactions of LCS (and
# pending compactions of NullCompactionStrategy), which nodetool tablestats
# and compactionstats report.
def test_column_family_pending_compactions(cql, rest_api, test_keyspace_vnodes):
    ks = test_keyspace_vnodes
    value = "y" * 1024
    with new_test_table(cql, ks, "pk int, ck int, v text, PRIMARY KEY (pk, ck)",
                        "WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 1} "
                        "AND compression = {'sstable_compression': ''}") as t:
        table = t.replace('.', ':')
        insert = cql.prepare(f"INSERT INTO {t} (pk, ck, v) VALUES (?, ?, ?)")
        def write(first_pk, n_partitions):
            execute_concurrent_with_args(cql, insert,
                [(pk, ck, value) for pk in range(first_pk, first_pk + n_partitions) for ck in range(100)])
        # ~3MB, compacted into L1 and up.
        write(0, 30)
        resp = rest_api.send("POST", f"column_family/major_compaction/{table}")
        resp.raise_for_status()
        resp = rest_api.send("DELETE", f"column_family/autocompaction/{table}")
        resp.raise_for_status()
        # Four flushes of ~1MB into L0, which LCS wants promoted.
        for i in range(4):
            write(1000 + i * 10, 10)
            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
            resp.raise_for_status()
        per_level = get_sstable_count_per_level(rest_api, table)
        assert per_level[0] >= 4, per_level
        assert sum(per_level[1:]) >= 1, per_level
        assert sum(per_level) == get_metric(rest_api, "live_ss_table_count", table)
        assert get_metric(rest_api, "pending_compactions", table) >= 1
        # Uncompressed sstables have no compression ratio.
        assert get_metric(rest_api, "compression_ratio", table) == 0
        # Give the compaction controller (which polls the backlog every 250ms)
        # a chance to see the L0 backlog.
        time.sleep(1)

    with new_test_table(cql, ks, "pk int PRIMARY KEY", "WITH compaction = {'class': 'NullCompactionStrategy'}") as t:
        assert get_metric(rest_api, "pending_compactions", t.replace('.', ':')) == 0
