#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.nodetool.rest_api_mock import expected_request
from typing import NamedTuple


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


def test_proxyhistograms(nodetool):
    expected_requests = [
            expected_request("GET", "/storage_proxy/metrics/read/estimated_histogram",
                             response=latency_histogram(0)),
            expected_request("GET", "/storage_proxy/metrics/write/estimated_histogram",
                             response=latency_histogram(1)),
            expected_request("GET", "/storage_proxy/metrics/range/estimated_histogram",
                             response=latency_histogram(4)),
            expected_request("GET", "/storage_proxy/metrics/cas_read/estimated_histogram",
                             response=latency_histogram(8)),
            expected_request("GET", "/storage_proxy/metrics/cas_write/estimated_histogram",
                             response=latency_histogram(12)),
    ]

    res = nodetool("proxyhistograms", expected_requests=expected_requests)

    assert res.stdout == """proxy histograms
Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                     (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
50%                    512.00             640.00            1024.00            2048.00            4096.00               0.00
75%                   1024.00            1280.00            2048.00            4096.00            8192.00               0.00
95%                   2048.00            2560.00            4096.00            8192.00           16384.00               0.00
98%                   4096.00            5120.00            8192.00           16384.00           32768.00               0.00
99%                   4096.00            5120.00            8192.00           16384.00           32768.00               0.00
Min                    512.00             640.00            1024.00            2048.00            4096.00               0.00
Max                   5120.00            6144.00           10240.00           20480.00           40960.00               0.00

"""


def test_proxyhistograms_empty_histogram(nodetool):
    expected_requests = [
            expected_request("GET", "/storage_proxy/metrics/read/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/write/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/range/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/cas_read/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/cas_write/estimated_histogram",
                             response=latency_histogram()),
    ]

    res = nodetool("proxyhistograms", expected_requests=expected_requests)

    assert res.stdout == """proxy histograms
Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                     (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
50%                      0.00               0.00               0.00               0.00               0.00               0.00
75%                      0.00               0.00               0.00               0.00               0.00               0.00
95%                      0.00               0.00               0.00               0.00               0.00               0.00
98%                      0.00               0.00               0.00               0.00               0.00               0.00
99%                      0.00               0.00               0.00               0.00               0.00               0.00
Min                      0.00               0.00               0.00               0.00               0.00               0.00
Max                      0.00               0.00               0.00               0.00               0.00               0.00

"""


def test_proxyhistograms_latency_above_range(nodetool):
    """A latency above the histogram's range is counted in the last bucket,
    which has no upper limit. It is shown as the top of the range."""
    overflowed = latency_histogram()
    overflowed["buckets"][-1] = 1
    expected_requests = [
            expected_request("GET", "/storage_proxy/metrics/read/estimated_histogram",
                             response=overflowed),
            expected_request("GET", "/storage_proxy/metrics/write/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/range/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/cas_read/estimated_histogram",
                             response=latency_histogram()),
            expected_request("GET", "/storage_proxy/metrics/cas_write/estimated_histogram",
                             response=latency_histogram()),
    ]

    res = nodetool("proxyhistograms", expected_requests=expected_requests)

    for line in res.stdout.splitlines()[3:10]:
        assert line.split()[1] == "33554432.00"
