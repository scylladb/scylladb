#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import json
import math
import pytest
import random
import re
import statistics
import yaml
from collections import defaultdict
from textwrap import indent
from typing import NamedTuple
from test.nodetool.rest_api_mock import expected_request


class Table(NamedTuple):
    ks: str
    cf: str
    type: str


def response_from_list(keys, values):
    return [dict(zip(keys, value)) for value in values]


def histogram(count=0, sum_=0, min_=0, max_=0, variance=0, mean=0, sample=[]):
    assert count == len(sample)
    result = {
        'count': count,
        'sum': sum_,
        'min': min_,
        'max': max_,
        'variance': variance,
        'mean': mean
    }
    if count == 0:
        return result
    result['sample'] = sample
    return result


def make_random_histogram(count):
    lower = 100
    upper = 100
    #upper = 3000
    samples = [random.randint(lower, upper) for _ in range(count)]
    return histogram(count,
                     sum(samples),
                     min(samples),
                     max(samples),
                     statistics.variance(samples),
                     statistics.mean(samples),
                     samples)


def make_moving_avg_and_histogram(count):
    # 1, 5, 15 minutes rates
    rates = [random.random() for _ in (1, 5, 15)]
    # the mean rate from startup
    mean_rate = random.random()
    hist = make_random_histogram(count)
    return {
        'meter': {
            'rates': rates,
            'mean_rate': mean_rate,
            'count': count,
        },
        'hist': hist,
    }


class table_stats:
    def __init__(self, ks, cf):
        self.ks = ks
        self.cf = cf
        self.read = 351
        self.read_latency_hist = make_moving_avg_and_histogram(self.read)
        self.write = 278
        self.write_latency_hist = make_moving_avg_and_histogram(self.write)
        self.live_ss_table_count = 16
        self.sst_per_level = [16]
        self.live_disk_space_used = 1146924
        self.total_disk_space_used = 1146924
        self.snapshots_size = 0
        self.memtable_off_heap_size = 0
        self.bloom_filter_off_heap_memory_used = 304
        self.index_summary_off_heap_memory_used = 37248
        self.compression_metadata_off_heap_memory_used = 0
        self.compression_ratio = 0
        self.estimated_row_count = 100
        self.memtable_columns_count = 0
        self.memtable_live_data_size = 0
        self.memtable_switch_count = 16
        self.read_latency = 198
        self.write_latency = 152
        self.pending_flushes = 0
        # it's but a dummy value, scylla does not support it.
        self.percent_repaired = 0.0
        self.bloom_filter_false_positives = 0
        self.recent_bloom_filter_false_ratio = 0.0
        self.bloom_filter_disk_space_used = 304
        self.min_row_size = 216
        self.max_row_size = 258
        self.mean_row_size = 258
        self.live_scanned_histogram = histogram()
        self.tombstone_scanned_histogram = histogram()
        # it's but a dummy value, scylla does not support it.
        self.dropped_mutations = 0

    @property
    def table_name(self):
        return f'{self.ks}:{self.cf}'

    @property
    def local_read_count(self):
        return self.read_latency_hist['hist']['count']

    @property
    def local_read_latency(self):
        return self.read_latency_hist['hist']['mean']

    @property
    def local_write_count(self):
        return self.write_latency_hist['hist']['count']

    @property
    def local_write_latency(self):
        return self.write_latency_hist['hist']['mean']

    @property
    def total_off_heap_memory_used(self):
        return (self.memtable_off_heap_size +
                self.bloom_filter_off_heap_memory_used +
                self.index_summary_off_heap_memory_used +
                self.compression_metadata_off_heap_memory_used)

    def sstable_count_in_each_level(self):
        # scylla hardwire FANOUT_SIZE to 10
        sstable_fanout_size = 10
        for level, count in enumerate(self.sst_per_level):
            if level == 0:
                max_count = 4
            else:
                max_count = int(max.pow(sstable_fanout_size, level))
            if count > max_count:
                yield f'{count}/{max_count}'
            else:
                yield f'{count}'

    @property
    def sstables_in_each_level(self):
        return ', '.join(self.sstable_count_in_each_level())

    @property
    def avg_live_cells_per_slice(self):
        return self.live_scanned_histogram['mean']

    @property
    def max_live_cells_per_slice(self):
        return self.live_scanned_histogram['max']

    @property
    def avg_tombstones_per_slice(self):
        return self.tombstone_scanned_histogram['mean']

    @property
    def max_tombstones_per_slice(self):
        return self.tombstone_scanned_histogram['max']

    def req(self, name, response, **kwargs):
        return expected_request('GET', f'/column_family/metrics/{name}/{self.table_name}',
                                response=response, **kwargs)

    def hist(self, name, response):
        return expected_request('GET', f'/column_family/metrics/{name}/moving_average_histogram/{self.table_name}',
                                multiple=expected_request.ANY,
                                response=response)

    def ssttables_per_level(self):
        return expected_request('GET', f'/column_family/sstables/per_level/{self.table_name}',
                                response=self.sst_per_level)

    def expected_summary_requests(self, is_scylla):
        if is_scylla:
            return [
                self.hist('write_latency', self.write_latency_hist),
                self.req('write_latency', self.write_latency),
                self.hist('read_latency', self.read_latency_hist),
                self.req('read_latency', self.read_latency),
                self.req('pending_flushes', self.pending_flushes)]
        else:
            return [
                self.hist('write_latency', self.write_latency_hist),
                self.hist('read_latency', self.read_latency_hist),
                self.req('read_latency', self.read_latency),
                self.req('write_latency', self.write_latency),
                self.req('pending_flushes', self.pending_flushes)]

    def expected_details_requests(self, is_scylla):
        if is_scylla:
            return [
                # scylla only requests for this metric for plain text output
                self.req('live_ss_table_count', self.live_ss_table_count,
                         multiple=expected_request.ANY),
                self.ssttables_per_level(),
                self.req('live_disk_space_used', self.live_disk_space_used),
                self.req('total_disk_space_used', self.total_disk_space_used),
                self.req('snapshots_size', self.snapshots_size),
                self.req('memtable_off_heap_size', self.memtable_off_heap_size),
                self.req('bloom_filter_off_heap_memory_used', self.bloom_filter_off_heap_memory_used),
                self.req('index_summary_off_heap_memory_used', self.index_summary_off_heap_memory_used),
                self.req('compression_metadata_off_heap_memory_used', self.compression_metadata_off_heap_memory_used),
                self.req('compression_ratio', self.compression_ratio),
                self.req('estimated_row_count', self.estimated_row_count),
                self.req('memtable_columns_count', self.memtable_columns_count),
                self.req('memtable_live_data_size', self.memtable_live_data_size),
                self.req('memtable_switch_count', self.memtable_switch_count),
                self.hist('read_latency', self.read_latency_hist),
                self.hist('write_latency', self.write_latency_hist),
                self.req('pending_flushes', self.pending_flushes),
                self.req('bloom_filter_false_positives', self.bloom_filter_false_positives),
                self.req('recent_bloom_filter_false_ratio', self.recent_bloom_filter_false_ratio),
                self.req('bloom_filter_disk_space_used', self.bloom_filter_disk_space_used),
                self.req('min_row_size', self.min_row_size),
                self.req('max_row_size', self.max_row_size),
                self.req('mean_row_size', self.mean_row_size),
                self.req('live_scanned_histogram', self.live_scanned_histogram),
                self.req('tombstone_scanned_histogram', self.tombstone_scanned_histogram),
            ]
        else:
            return [
                self.req('live_ss_table_count', self.live_ss_table_count),
                self.ssttables_per_level(),
                self.req('memtable_off_heap_size', self.memtable_off_heap_size),
                self.req('bloom_filter_off_heap_memory_used', self.bloom_filter_off_heap_memory_used),
                self.req('index_summary_off_heap_memory_used', self.index_summary_off_heap_memory_used),
                self.req('compression_metadata_off_heap_memory_used', self.compression_metadata_off_heap_memory_used),
                self.req('live_disk_space_used', self.live_disk_space_used),
                self.req('total_disk_space_used', self.total_disk_space_used),
                self.req('snapshots_size', self.snapshots_size),
                self.req('compression_ratio', self.compression_ratio),
                self.req('estimated_row_count', self.estimated_row_count),
                self.req('memtable_columns_count', self.memtable_columns_count),
                self.req('memtable_live_data_size', self.memtable_live_data_size),
                self.req('memtable_switch_count', self.memtable_switch_count),
                self.hist('read_latency', self.read_latency_hist),
                self.hist('write_latency', self.write_latency_hist),
                self.req('pending_flushes', self.pending_flushes),
                self.req('bloom_filter_false_positives', self.bloom_filter_false_positives),
                self.req('recent_bloom_filter_false_ratio', self.recent_bloom_filter_false_ratio),
                self.req('bloom_filter_disk_space_used', self.bloom_filter_disk_space_used),
                self.req('min_row_size', self.min_row_size),
                self.req('max_row_size', self.max_row_size),
                self.req('mean_row_size', self.mean_row_size),
                self.req('live_scanned_histogram', self.live_scanned_histogram),
                self.req('tombstone_scanned_histogram', self.tombstone_scanned_histogram),
            ]

    def format(self):
        return f'''\
Table: {self.cf}
SSTable count: {self.live_ss_table_count}
SSTables in each level: [{self.sstables_in_each_level}]
Space used (live): {self.live_disk_space_used}
Space used (total): {self.total_disk_space_used}
Space used by snapshots (total): {self.snapshots_size}
Off heap memory used (total): {self.total_off_heap_memory_used}
SSTable Compression Ratio: {self.compression_ratio:.1f}
Number of partitions (estimate): {self.estimated_row_count}
Memtable cell count: {self.memtable_columns_count}
Memtable data size: {self.memtable_live_data_size}
Memtable off heap memory used: {self.memtable_off_heap_size}
Memtable switch count: {self.memtable_switch_count}
Local read count: {self.read}
Local read latency: {self.local_read_latency / 1000:.3f} ms
Local write count: {self.write}
Local write latency: {self.local_write_latency / 1000:.3f} ms
Pending flushes: {self.pending_flushes}
Percent repaired: {self.percent_repaired}
Bloom filter false positives: {self.bloom_filter_false_positives}
Bloom filter false ratio: {self.recent_bloom_filter_false_ratio:.5f}
Bloom filter space used: {self.bloom_filter_disk_space_used}
Bloom filter off heap memory used: {self.bloom_filter_off_heap_memory_used}
Index summary off heap memory used: {self.index_summary_off_heap_memory_used}
Compression metadata off heap memory used: {self.compression_metadata_off_heap_memory_used}
Compacted partition minimum bytes: {self.min_row_size}
Compacted partition maximum bytes: {self.max_row_size}
Compacted partition mean bytes: {self.mean_row_size}
Average live cells per slice (last five minutes): {self.avg_live_cells_per_slice:.1f}
Maximum live cells per slice (last five minutes): {self.max_live_cells_per_slice}
Average tombstones per slice (last five minutes): {self.avg_tombstones_per_slice:.1f}
Maximum tombstones per slice (last five minutes): {self.max_tombstones_per_slice}
Dropped Mutations: {self.dropped_mutations}

'''

    def to_map(self):
        return {
            'sstables_in_each_level': list(self.sstable_count_in_each_level()),
            'space_used_live': f'{self.live_disk_space_used}',
            'space_used_total': f'{self.total_disk_space_used}',
            'space_used_by_snapshots_total': f'{self.snapshots_size}',
            'off_heap_memory_used_total': f'{self.total_off_heap_memory_used}',
            'sstable_compression_ratio': self.compression_ratio,
            'number_of_partitions_estimate': self.estimated_row_count,
            'memtable_cell_count': self.memtable_columns_count,
            'memtable_data_size': f'{self.memtable_live_data_size}',
            'memtable_off_heap_memory_used': f'{self.memtable_off_heap_size}',
            'memtable_switch_count': self.memtable_switch_count,
            'local_read_count': self.read,
            'local_read_latency_ms': f'{self.local_read_latency / 1000:.3f}',
            'local_write_count': self.write,
            'local_write_latency_ms': f'{self.local_write_latency / 1000:.3f}',
            'pending_flushes': self.pending_flushes,
            'percent_repaired': self.percent_repaired,
            'bloom_filter_false_positives': self.bloom_filter_false_positives,
            'bloom_filter_false_ratio': f'{self.recent_bloom_filter_false_ratio:01.5f}',
            'bloom_filter_space_used': f'{self.bloom_filter_disk_space_used}',
            'bloom_filter_off_heap_memory_used': f'{self.bloom_filter_off_heap_memory_used}',
            'index_summary_off_heap_memory_used': f'{self.index_summary_off_heap_memory_used}',
            'compression_metadata_off_heap_memory_used': f'{self.compression_metadata_off_heap_memory_used}',
            'compacted_partition_minimum_bytes': self.min_row_size,
            'compacted_partition_maximum_bytes': self.max_row_size,
            'compacted_partition_mean_bytes': self.mean_row_size,
            'average_live_cells_per_slice_last_five_minutes': self.avg_live_cells_per_slice,
            'maximum_live_cells_per_slice_last_five_minutes': self.max_live_cells_per_slice,
            'average_tombstones_per_slice_last_five_minutes': self.avg_tombstones_per_slice,
            'maximum_tombstones_per_slice_last_five_minutes': self.max_live_cells_per_slice,
            'dropped_mutations': f'{self.dropped_mutations}',
        }


class scientific_notation:
    # Python and {fmt} prints a float like "1.234E-04",
    # while Java prints like     "1.23E-4"
    def __init__(self, value, is_scylla):
        self.value = value
        self.is_scylla = is_scylla

    def __format__(self, _):
        if self.is_scylla:
            if math.isnan(self.value):
                return 'nan'
            return f'{self.value:.15E}'
        else:
            if math.isnan(self.value):
                return 'NaN'
            matched = re.match(r'(\d.\d+)E(\+|\-)(\d+)', f'{self.value:.15E}')
            assert matched
            m, sign, e = matched.group(1), matched.group(2), int(matched.group(3))
            return f'{m}E{sign}{e}'


class keyspace_stats:
    def __init__(self, is_scylla):
        self.tables = []
        self.read_count = 0
        self.total_read_time = 0
        self.write_count = 0
        self.total_write_time = 0
        self.pending_flushes = 0
        self.is_scylla = is_scylla

    def add_table(self, table):
        self.tables.append(table)

        if table.read > 0:
            self.read_count += table.local_read_count
            self.total_read_time += table.read_latency

        if table.local_write_count > 0:
            self.write_count = table.local_write_count
            self.total_write_time += table.write_latency

        self.pending_flushes += table.pending_flushes

    @property
    def read_latency(self):
        if self.read_count == 0:
            v = math.nan
        else:
            v = self.total_read_time / self.read_count / 1000
        return scientific_notation(v, self.is_scylla)

    @property
    def write_latency(self):
        if self.write_count == 0:
            v = math.nan
        else:
            v = self.total_write_time / self.write_count / 1000
        return scientific_notation(v, self.is_scylla)

    def to_map(self, is_scylla):
        m = {
            'read_count': self.read_count,
            'read_latency_ms': self.read_latency.value,
            'write_count': self.write_count,
            'write_latency_ms': self.write_latency.value,
            'pending_flushes': self.pending_flushes,
        }
        if not is_scylla:
            # cassandra nodetool has a duplicated item
            m['read_latency'] = self.read_latency.value
        return m


@pytest.mark.parametrize('command,args,tables_to_print',
                         [
                             ('tablestats', [], ['keyspace1.standard1', 'system.local']),
                             ('tablestats', ['keyspace1'], ['keyspace1.standard1']),
                             ('tablestats', ['keyspace1.standard1'], ['keyspace1.standard1']),
                             ('cfstats', [], ['keyspace1.standard1', 'system.local']),
                         ])
def test_plain_text_output(request, nodetool, command, args, tables_to_print):
    is_scylla = request.config.getoption("nodetool") == 'scylla'

    tables = [Table('keyspace1', 'standard1', 'ColumnFamilies'),
              Table('system', 'local', 'ColumnFamilies')]
    expected_requests = [expected_request(
        'GET', '/column_family/',
        multiple=expected_request.MULTIPLE,
        response=response_from_list(['ks', 'cf', 'type'], tables))]

    total_nr_tables = 0
    keyspaces = defaultdict(lambda: keyspace_stats(is_scylla))
    for table in tables:
        total_nr_tables += 1
        stats = table_stats(table.ks, table.cf)
        included = f'{stats.ks}.{stats.cf}' in tables_to_print
        if included:
            if not is_scylla:
                # scylla tallies the table stats afterwards
                expected_requests += stats.expected_summary_requests(is_scylla)
            ks = keyspaces[table.ks]
            ks.add_table(stats)

    if not is_scylla:
        expected_requests.append(expected_request(
            'GET', '/storage_service/keyspaces',
            response=sorted(keyspaces.keys())))
    expected_output = f'''\
Total number of tables: {total_nr_tables}
----------------
'''
    for ks_name in sorted(keyspaces.keys()):
        keyspace = keyspaces[ks_name]
        if is_scylla:
            for table in keyspace.tables:
                # cassandra nodetool tallies the table stats in the loop above
                expected_requests += table.expected_summary_requests(is_scylla)
        expected_output += f'''\
Keyspace : {ks_name}
\tRead Count: {keyspace.read_count}
\tRead Latency: {keyspace.read_latency:.15E} ms
\tWrite Count: {keyspace.write_count}
\tWrite Latency: {keyspace.write_latency:.15E} ms
\tPending Flushes: {keyspace.pending_flushes}
'''
        for table in keyspace.tables:
            expected_requests.extend(table.expected_details_requests(is_scylla))
            expected_output += indent(table.format(), '\t\t')
        expected_output += '----------------\n'

    res = nodetool(command, *args, expected_requests=expected_requests)
    actual_output = res.stdout
    assert actual_output == expected_output


@pytest.mark.parametrize('output_format', ['json', 'yaml'])
def test_output_format(request, nodetool, output_format):
    is_scylla = request.config.getoption("nodetool") == 'scylla'

    tables = [Table('keyspace1', 'standard1', 'ColumnFamilies'),
              Table('system', 'local', 'ColumnFamilies')]
    expected_requests = [expected_request(
        'GET', '/column_family/',
        multiple=expected_request.MULTIPLE,
        response=response_from_list(['ks', 'cf', 'type'], tables))]

    keyspaces = defaultdict(lambda: keyspace_stats(is_scylla))
    for table in tables:
        stats = table_stats(table.ks, table.cf)
        if not is_scylla:
            # scylla tallies the table stats afterwards
            expected_requests += stats.expected_summary_requests(is_scylla)
        keyspaces[table.ks].add_table(stats)

    if not is_scylla:
        expected_requests.append(expected_request(
            'GET', '/storage_service/keyspaces',
            response=sorted(keyspaces.keys())))
    total_nr_tables = sum(len(ks.tables) for ks in keyspaces.values())
    expected_dict = {'total_number_of_tables': total_nr_tables}
    for ks_name in sorted(keyspaces.keys()):
        keyspace = keyspaces[ks_name]
        if is_scylla:
            for table in keyspace.tables:
                # cassandra nodetool tallies the table stats in the loop above
                expected_requests += table.expected_summary_requests(is_scylla)
        expected_dict[ks_name] = keyspace.to_map(is_scylla)

        expected_tables = {}
        for table in keyspace.tables:
            expected_requests.extend(table.expected_details_requests(is_scylla))
            expected_tables[table.cf] = table.to_map()
        expected_dict[ks_name]['tables'] = expected_tables

    parsers = {
        'yaml': lambda m: yaml.load(m, Loader=yaml.Loader),
        'json': json.loads
    }

    res = nodetool('tablestats', '--format', output_format,
                   expected_requests=expected_requests)
    actual_output = res.stdout
    actual_dict = parsers[output_format](actual_output)

    assert actual_dict == expected_dict
