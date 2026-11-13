# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests that reading data from sstables doesn't read from the disk much
# more than the size of the data ("read amplification").
#############################################################################

import pytest
from cassandra.query import SimpleStatement

from . import nodetool
from .util import ScyllaMetrics, new_test_table

# Return the number of bytes which reads of user requests (in the default
# service level) read from the disk. Background work, like compactions of
# other tables, reads in other I/O classes, so it isn't counted.
def user_disk_read_bytes(cql):
    metrics = ScyllaMetrics.query(cql)
    iogroups = metrics.label_values('scylla_io_queue_total_read_bytes', 'iogroup')
    mountpoints = metrics.label_values('scylla_io_queue_total_read_bytes', 'mountpoint')
    return metrics.get('scylla_io_queue_total_read_bytes', {'class': 'sl:default', 'iogroup': iogroups, 'mountpoint': mountpoints})

# Write rows of 2 KB, totaling the given size, compact them into one
# sstable, read them all with BYPASS CACHE - each partition separately, or
# with a paged scan - and check that the disk reads are at most max_ratio
# times the size of the data.
# Translated from scylla-dtest's read_amplification_test.py::TestReadAmplification
# tests test_no_amplification_on_read_20kb, test_no_amplification_on_read_400kb,
# test_no_amplification_on_scanning_read_20kb and
# test_no_amplification_on_scanning_read_2mb. The dtests restarted the node
# to empty the cache instead of using BYPASS CACHE, and counted all the
# node's disk reads, including background ones.
@pytest.mark.parametrize('read_type,size,max_ratio', [
    ('partition', 20 * 1024, 20), ('partition', 400 * 1024, 10),
    ('scan', 20 * 1024, 10), ('scan', 2 * 1024 * 1024, 10)])
def test_no_read_amplification(cql, test_keyspace, scylla_only, read_type, size, max_ratio):
    with new_test_table(cql, test_keyspace, 'key text PRIMARY KEY, c1 text, c2 text') as table:
        count = size // 2048
        insert = cql.prepare(f'INSERT INTO {table} (key, c1, c2) VALUES (?, ?, ?)')
        for i in range(count):
            cql.execute(insert, [f'k{i}', 'a' * 1024, 'b' * 1024])
        nodetool.compact(cql, table)
        before = user_disk_read_bytes(cql)
        if read_type == 'partition':
            select = cql.prepare(f'SELECT c1, c2 FROM {table} WHERE key = ? BYPASS CACHE')
            for i in range(count):
                assert len(list(cql.execute(select, [f'k{i}']))) == 1
        else:
            assert len(list(cql.execute(SimpleStatement(f'SELECT * FROM {table} BYPASS CACHE', fetch_size=25)))) == count
        read_bytes = user_disk_read_bytes(cql) - before
        print(f'{read_type} read of {size} bytes read {read_bytes} bytes from disk')
        assert 0 < read_bytes <= max_ratio * size
