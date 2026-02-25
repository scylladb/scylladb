# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

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

            for strategy in [ "SizeTieredCompactionStrategy", "LeveledCompactionStrategy", "TimeWindowCompactionStrategy" ]:
                resp = rest_api.send("POST", f"column_family/compaction_strategy/{table_name}", params={"class_name": strategy})
                resp.raise_for_status()
                check_strategy(strategy)


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
