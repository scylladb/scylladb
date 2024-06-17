#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import random
from typing import NamedTuple

import pytest

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import format_size


class moving_average(NamedTuple):
    rates: list[float]
    mean_rate: float
    count: int

    def to_json(self):
        return {
            'rates': self.rates,
            'mean_rate': self.mean_rate,
            'count': self.count
        }
    @staticmethod
    def make(count=0):
        # 1, 5, 15 minutes rates
        rates = [random.random() for _ in (1, 5, 15)]
        # the mean rate from startup
        mean_rate = random.random()
        return moving_average(rates, mean_rate, count)


class cache_metrics(NamedTuple):
    entries: int
    size: int
    capacity: int
    hits: moving_average
    requests: moving_average
    hit_rate: float
    save_period: int


def normalize_output(output):
    # Scylla does not run a JVM, so it the memory usage exposed by MemoryMXBean
    # is meaningless to it.
    normalized = ''
    for line in output.split('\n'):
        if line.startswith('Heap Memory'):
            continue
        if line.startswith('Uptime'):
            continue
        # cassandra nodetool use KiB and KB interchangeably, let's
        # normalize these two prefixes
        for iec, si in [('KiB', 'KB'), ('MiB', 'MB'), ('GiB', 'GB')]:
            line = line.replace(iec, si)
        normalized += f'{line}\n'
    return normalized


@pytest.mark.parametrize("display_all_tokens",
                         [
                             True,
                             False,
                         ]
                         )
def test_info(request, nodetool, display_all_tokens):
    host_id = 'hostid0'
    endpoint = '127.0.0.1'
    endpoint_to_host_id = {endpoint: host_id}
    generation_number = 42
    load = 42424242
    uptime = 12345
    datacenter = 'dc0'
    rack = 'rack0'
    nr_exceptions = 0
    join_ring = True

    caches = {
        'key':     cache_metrics(98, 123, 1220, moving_average.make(), moving_average.make(), 0.42, 20),
        'row':     cache_metrics(42, 223, 1200, moving_average.make(), moving_average.make(), 0.43, 20),
        'counter': cache_metrics(13, 523, 3200, moving_average.make(), moving_average.make(), 0.44, 20),
    }

    off_heap_mem_usages = {
        'memtable_off_heap_size': 1234,
        'bloom_filter_off_heap_memory_used': 1235,
        'index_summary_off_heap_memory_used': 1236,
        'compression_metadata_off_heap_memory_used': 1237,
    }
    tables = [('ks0', 'cf0', 'ColumnFamilies'),
              ('ks0', 'cf1', 'ColumnFamilies')]

    tokens = ['-9217327499541836964',
              '9066719992055809912',
              '50927788561116407']

    expected_requests = [
        expected_request('GET', '/storage_service/gossiping', response=True),
        expected_request('GET', '/storage_service/hostid/local', response=host_id),
        expected_request('GET', '/storage_service/rpc_server', response=False),
        expected_request('GET', '/storage_service/native_transport', response=True),
        expected_request('GET', '/storage_service/load', response=load),
        expected_request('GET', '/storage_service/generation_number', response=generation_number),
    ]

    is_scylla = request.config.getoption('nodetool') == 'scylla'

    if is_scylla:
        expected_requests.append(
            expected_request('GET', '/system/uptime_ms', response=uptime))
    else:
        # cassandra nodetool uses RuntimeMXBean
        pass
    expected_requests.append(
        expected_request('GET', '/column_family/',
                         multiple=expected_request.MULTIPLE,
                         response=[{'ks': ks, 'cf': cf, 'type': type_} for ks, cf, type_ in tables]))
    off_heap_mem_used = 0
    for ks, cf, _ in tables:
        for name, mem_used in off_heap_mem_usages.items():
            off_heap_mem_used += mem_used
            expected_requests.append(
                expected_request('GET', f'/column_family/metrics/{name}/{ks}:{cf}',
                                 response=mem_used))
    expected_requests += [
        expected_request('GET', '/snitch/datacenter', multiple=expected_request.ANY, response=datacenter),
        expected_request('GET', '/snitch/rack', multiple=expected_request.ANY, response=rack),
        expected_request('GET', '/storage_service/metrics/exceptions', response=nr_exceptions),
    ]

    for name, cache in caches.items():
        expected_requests += [
            expected_request('GET', f'/cache_service/metrics/{name}/entries',
                             response=cache.entries),
            expected_request('GET', f'/cache_service/metrics/{name}/size',
                             response=cache.size),
            expected_request('GET', f'/cache_service/metrics/{name}/capacity',
                             response=cache.capacity),
            expected_request('GET', f'/cache_service/metrics/{name}/hits_moving_avrage',
                             multiple=expected_request.ANY,
                             response=cache.hits.to_json()),
            expected_request('GET', f'/cache_service/metrics/{name}/requests_moving_avrage',
                             multiple=expected_request.ANY,
                             response=cache.requests.to_json()),
            expected_request('GET', f'/cache_service/metrics/{name}/hit_rate',
                             response=cache.hit_rate),
            expected_request('GET', f'/cache_service/{name}_cache_save_period',
                             response=cache.save_period)
        ]
    expected_requests.append(
        expected_request('GET', '/storage_service/join_ring',
                         response=join_ring))

    if join_ring:
        if is_scylla:
            expected_requests.append(
                expected_request('GET', '/storage_service/tokens',
                                 response=tokens))
        else:
            expected_requests += [
                expected_request('GET', '/storage_service/host_id',
                                 response=[{'key': endpoint, 'value': hostid}
                                           for endpoint, hostid in endpoint_to_host_id.items()]),
                expected_request('GET', '/storage_service/hostid/local', response=host_id),
                expected_request('GET', f'/storage_service/tokens/{endpoint}',
                                 response=tokens)
            ]

    mem_used = 0.0
    mem_max = 0.0
    off_heap_mem_used_in_mb = off_heap_mem_used / 1024 / 1024
    expected_output = f'''\
{'ID':<23}: {host_id}
{'Gossip active':<23}: true
{'Thrift active':<23}: false
{'Native Transport active':<23}: true
{'Load':<23}: {format_size(load)}
{'Generation No':<23}: {generation_number}
{'Uptime (seconds)':<23}: {uptime}
{'Heap Memory (MB)':<23}: {mem_used:.2f} / {mem_max:.2f}
{'Off Heap Memory (MB)':<23}: {off_heap_mem_used_in_mb:.2f}
{'Data Center':<23}: {datacenter}
{'Rack':<23}: {rack}
{'Exceptions':<23}: {nr_exceptions}
'''

    for n, c in caches.items():
        name = f'{n.capitalize()} Cache'
        expected_output += (f'{name:<23}: '
                            f'entries {c.entries}, size {format_size(c.size)}, '
                            f'capacity {format_size(c.capacity)}, '
                            f'{c.hits.count} hits, {c.requests.count} requests, '
                            f'{c.hit_rate:.3f} recent hit rate, '
                            f'{c.save_period} save period in seconds\n')

    percent_repaired = 0.0
    expected_output += f"{'Percent Repaired':<23}: {percent_repaired:.1f}%\n"
    if join_ring:
        name = 'Token'
        if display_all_tokens:
            for token in tokens:
                expected_output += f'{name:<23}: {token}\n'
        else:
            nr_tokens = len(tokens)
            expected_output += f'{name:<23}: (invoke with -T/--tokens to see all {nr_tokens} tokens)\n'
    else:
        expected_output += f'{name:<23}: (invoke with -T/--tokens to see all {nr_tokens} tokens)\n'

    args = []
    if display_all_tokens:
        args.append('--tokens')
    res = nodetool("info", *args, expected_requests=expected_requests)
    assert normalize_output(res.stdout) == normalize_output(expected_output)
