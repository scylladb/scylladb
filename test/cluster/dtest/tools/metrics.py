#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import re
import time

import requests


def prometheus_get(ip, port="9180"):
    prometheus_url = f"http://{ip}:{port}/metrics"
    resp = requests.get(prometheus_url)
    resp.raise_for_status()
    return resp.text


def get_node_metrics(node_ip: str, metrics: list[str], port="9180"):
    metrics_res = {k: 0 for k in metrics}
    filter_metrics = [metric for metric in prometheus_get(node_ip, port).splitlines() if not metric.startswith("#")]
    for metric in filter_metrics:
        for metric_name in metrics:
            # The name must not continue past metric_name (select_partition_range_scan must not
            # match select_partition_range_scan_no_bypass_cache), but metric_name may end inside
            # the labels, e.g. 'scylla_cql_deletes{.*conditional="yes"'.
            if re.search(metric_name + r"(?!\w)", metric):
                val = metric.split()[-1]
                try:
                    val = int(val)
                except ValueError:
                    val = float(val)
                metrics_res[metric_name] += val
    return metrics_res


# Restored verbatim from scylla-dtest's tools/metrics.py; it was trimmed when
# this module was first ported in-tree, but not-yet-adapted dtest/unported
# test modules still import it.
def wait_for_metric(metric: str, ip: str, port: str = "9180", max_retries: int = 10, initial_wait: float = 0.1) -> bool | None:
    retries = 0
    backoff_factor = 2

    while retries < max_retries:
        if metric in prometheus_get(ip, port):
            return True

        time.sleep(initial_wait * (backoff_factor**retries))
        retries += 1
