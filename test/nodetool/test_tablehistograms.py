#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
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

read_latency_histogram = {"meter": {"rates": [1723.9181185133377, 368.14352124736297, 124.08049846282876],  "mean_rate": 117.64529914529915,  "count": 165174},  "hist": {"count": 165174,  "sum": 5339437,  "min": 5,  "max": 242,  "variance": 13736889.182246631,  "mean": 32.32613486384056,  "sample": [14, 26, 18, 12, 31, 32, 35, 20, 32, 26, 20, 12, 29, 10, 13, 38, 16, 13, 33, 26, 11, 33, 20, 32, 55, 17, 21, 22, 17, 40, 31, 22, 12, 38, 17, 11, 87, 45, 38, 21, 21, 14, 69, 61, 15, 33, 20, 38, 20, 21, 15, 38, 56, 37, 20, 18, 15, 39, 27, 12, 17, 28, 18, 36, 14, 44, 19, 15, 23, 32, 31, 15, 29, 21, 14, 32, 11, 32, 30, 34, 21, 28, 13, 14, 32, 11, 27, 21, 33, 13, 32, 27, 13, 13, 28, 33, 16, 39, 16, 14, 38, 24, 59, 15, 13, 39, 22, 31, 15, 28, 26, 33, 32, 11, 12, 19, 30, 12, 41, 41, 22, 29, 19, 56, 12, 29, 20, 12, 39, 12, 26, 10, 35, 16, 22, 36, 33, 65, 17, 32, 34, 23, 31, 32, 49, 15, 33, 21, 32, 32, 31, 55, 27, 38, 39, 39, 38, 16, 56, 39, 39, 39, 35, 16, 31, 55, 40, 15, 39, 40, 23, 39, 57, 23, 40, 14, 39, 38, 39, 57, 14, 43, 23, 42, 33, 32, 48, 20, 33, 31, 11, 32, 32, 50, 34, 32, 17, 10, 33, 32, 58, 20, 11, 40, 39, 39, 40, 56, 41, 40, 41, 40, 52, 17, 55, 51, 39, 33, 32, 15, 32, 60, 40, 18, 40, 41, 40, 40, 54, 39, 41, 39, 15, 50, 39, 55, 44, 37, 33, 13, 32, 32, 49, 33, 32, 32, 12, 32, 95, 48, 32, 32, 32, 11, 32, 31]}}

write_latency_histogram = {"meter": {"rates": [18.318261456969374, 4806.675944036122, 12175.520380360176],  "mean_rate": 2092.050209205021,  "count": 1000000},  "hist": {"count": 1000000,  "sum": 5757334,  "min": 1,  "max": 86,  "variance": 6698277.036767641,  "mean": 5.757334,  "sample": [4, 4, 4, 4, 3, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 5, 4, 4, 4, 10, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 10, 4, 4, 4, 4, 3, 11, 4, 3, 4, 4, 4, 10, 4, 4, 4, 4, 4, 11, 4, 4, 4, 3, 4, 10, 4, 4, 4, 4, 4, 10, 4, 4, 4, 4, 3, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 17, 11, 5, 4, 4, 4, 4, 11, 4, 3, 4, 4, 4, 11, 4, 4, 4, 4, 4, 10, 5, 4, 4, 4, 4, 10, 4, 3, 4, 4, 4, 10, 4, 4, 4, 3, 10, 4, 4, 3, 4, 10, 4, 4, 4, 4, 10, 4, 4, 4, 4, 11, 4, 4, 4, 4, 11, 4, 4, 4, 4, 11, 4, 4, 4, 4, 11, 4, 4, 4, 11, 4, 4, 4, 10, 4, 4, 4, 10, 4, 4, 4, 10, 5, 4, 4, 10, 4, 4, 4, 10, 4, 5, 4, 10, 4, 4, 4, 11, 4, 4, 4, 11, 4, 4, 4, 11, 4, 4, 4, 10, 5, 4, 11, 5, 4, 11, 4, 4, 11, 4, 4, 11, 4, 4, 11, 4, 10, 4, 10, 4, 10, 4, 10, 4, 11, 4, 10, 5, 10, 4, 11, 11, 11]}}


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
            expected_request("GET", f"/column_family/metrics/read_latency/moving_average_histogram/{table_param}",
                             response=read_latency_histogram),
            expected_request("GET", f"/column_family/metrics/write_latency/moving_average_histogram/{table_param}",
                             response=write_latency_histogram),
            expected_request("GET", f"/column_family/metrics/sstables_per_read_histogram/{table_param}",
                             response=sstables_per_read_histogram),
    ]

    res = nodetool(*param.args, expected_requests=expected_requests)

    assert res.stdout == f"""{param.keyspace_name}/{param.table_name} histograms
Percentile  SSTables     Write Latency      Read Latency    Partition Size        Cell Count
                              (micros)          (micros)           (bytes)                  
50%             2.00              4.00             32.00                 3                 2
75%             4.00              5.00             39.00                 4                 3
95%             7.00             11.00             56.00                 7                 4
98%             7.00             11.00             60.86                 7                 4
99%             7.00             11.00             76.74                 7                 4
Min             0.00              3.00             10.00                 0                 0
Max             7.00             17.00             95.00                 7                 4

"""

# The java implementation explodes with null pointer exception.
# It is not worth trying to fix, so just disable this test for the java nodetool.
def test_tablehistograms_empty_histogram(scylla_only, nodetool):
    keyspace_name = "ks"
    table_name = "tbl"
    table_param = f"{keyspace_name}:{table_name}"

    empty_estimated_histogram = {"buckets": [0]}
    empty_histogram = {"meter": {"rates": [0,0,0], "mean_rate": 0, "count": 0}, "hist": {"count": 0, "sum": 0, "min": 0, "max": 0, "variance": 0, "mean": 0}}

    expected_requests = [
            expected_request("GET", "/column_family/",
                             response=[{"ks": keyspace_name, "cf": table_name, "type": "ColumnFamilies"}],
                             multiple=expected_request.ANY),
            expected_request("GET", f"/column_family/metrics/estimated_row_size_histogram/{table_param}",
                             response=empty_estimated_histogram),
            expected_request("GET", f"/column_family/metrics/estimated_column_count_histogram/{table_param}",
                             response=empty_estimated_histogram),
            expected_request("GET", f"/column_family/metrics/read_latency/moving_average_histogram/{table_param}",
                             response=empty_histogram),
            expected_request("GET", f"/column_family/metrics/write_latency/moving_average_histogram/{table_param}",
                             response=empty_histogram),
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
