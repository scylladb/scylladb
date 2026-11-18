#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.nodetool.rest_api_mock import expected_request
from typing import NamedTuple

import pytest


class table_name_param(NamedTuple):
    args: list[str]
    keyspace_name: str
    table_name: str


row_size_histogram = {"bucket_offsets": [1, 2, 3, 4, 5, 6, 7], "buckets": [4, 4, 4, 4, 1, 1, 2, 0]}

column_count_histogram = {"bucket_offsets": [1, 2, 3, 4, 5, 6, 7], "buckets": [2, 4, 4, 2, 0, 0, 0, 0]}

sstables_per_read_histogram = {"bucket_offsets": [1, 2, 3, 4, 5, 6, 7], "buckets": [4, 4, 2, 2, 1, 1, 2, 0]}

def latency_histogram(first_bucket=None):
    """A response of a latency estimated_histogram endpoint.
    The histogram has 4 buckets for each power of 2, starting at 512us.
    When first_bucket is given, 100 values are placed, starting at that bucket:
    50 values in it, then 30, 15 and 5 in the buckets holding 2, 4 and 8 times its value."""
    offsets = [(512 << (i // 4)) + (i % 4) * ((512 << (i // 4)) // 4) for i in range(64)] + [33554432]
    buckets = [0] * len(offsets)
    if first_bucket is not None:
        for i, count in enumerate([50, 30, 15, 5]):
            buckets[first_bucket + 4 * i] = count
    return {"bucket_offsets": offsets, "buckets": buckets}


@pytest.mark.parametrize("param", (
    table_name_param(["tablehistograms", "ks", "tbl"], "ks", "tbl"),
    table_name_param(["tablehistograms", "ks.tbl"], "ks", "tbl"),
    table_name_param(["tablehistograms", "ks.1", "tbl"], "ks.1", "tbl"),
    table_name_param(["tablehistograms", "ks/tbl"], "ks", "tbl"),
    table_name_param(["tablehistograms", "ks.1/tbl"], "ks.1", "tbl"),
))
def test_tablehistograms(nodetool, param):
    table_param = f"{param.keyspace_name}:{param.table_name}"

    expected_requests = [
            expected_request("GET", "/column_family/",
                             response=[{"ks": param.keyspace_name, "cf": param.table_name, "type": "ColumnFamilies"}],
                             multiple=expected_request.ANY),
            expected_request("GET", f"/column_family/metrics/estimated_row_size_histogram/{table_param}",
                             response=row_size_histogram),
            expected_request("GET", f"/column_family/metrics/estimated_column_count_histogram/{table_param}",
                             response=column_count_histogram),
            expected_request("GET", f"/column_family/metrics/read_latency/estimated_histogram/{table_param}",
                             response=latency_histogram(0)),
            expected_request("GET", f"/column_family/metrics/write_latency/estimated_histogram/{table_param}",
                             response=latency_histogram(4)),
            expected_request("GET", f"/column_family/metrics/sstables_per_read_histogram/{table_param}",
                             response=sstables_per_read_histogram),
    ]

    res = nodetool(*param.args, expected_requests=expected_requests)

    assert res.stdout == f"""{param.keyspace_name}/{param.table_name} histograms
Percentile  SSTables     Write Latency      Read Latency    Partition Size        Cell Count
                              (micros)          (micros)           (bytes)                  
50%             2.00           1024.00            512.00                 3                 2
75%             4.00           2048.00           1024.00                 4                 3
95%             7.00           4096.00           2048.00                 7                 4
98%             7.00           8192.00           4096.00                 7                 4
99%             7.00           8192.00           4096.00                 7                 4
Min             0.00           1024.00            512.00                 0                 0
Max             7.00          10240.00           5120.00                 7                 4

"""

def test_tablehistograms_empty_histogram(nodetool):
    keyspace_name = "ks"
    table_name = "tbl"
    table_param = f"{keyspace_name}:{table_name}"

    empty_estimated_histogram = {"buckets": [0]}

    expected_requests = [
            expected_request("GET", "/column_family/",
                             response=[{"ks": keyspace_name, "cf": table_name, "type": "ColumnFamilies"}],
                             multiple=expected_request.ANY),
            expected_request("GET", f"/column_family/metrics/estimated_row_size_histogram/{table_param}",
                             response=empty_estimated_histogram),
            expected_request("GET", f"/column_family/metrics/estimated_column_count_histogram/{table_param}",
                             response=empty_estimated_histogram),
            expected_request("GET", f"/column_family/metrics/read_latency/estimated_histogram/{table_param}",
                             response=latency_histogram()),
            expected_request("GET", f"/column_family/metrics/write_latency/estimated_histogram/{table_param}",
                             response=latency_histogram()),
            expected_request("GET", f"/column_family/metrics/sstables_per_read_histogram/{table_param}",
                             response=empty_estimated_histogram),
    ]

    res = nodetool("tablehistograms", keyspace_name, table_name, expected_requests=expected_requests)

    assert res.stdout == f"""{keyspace_name}/{table_name} histograms
Percentile  SSTables     Write Latency      Read Latency    Partition Size        Cell Count
                              (micros)          (micros)           (bytes)                  
50%             0.00              0.00              0.00                 0                 0
75%             0.00              0.00              0.00                 0                 0
95%             0.00              0.00              0.00                 0                 0
98%             0.00              0.00              0.00                 0                 0
99%             0.00              0.00              0.00                 0                 0
Min             0.00              0.00              0.00                 0                 0
Max             0.00              0.00              0.00                 0                 0

"""
