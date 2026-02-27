#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import re

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
            if re.search(metric_name, metric):
                val = metric.split()[-1]
                try:
                    val = int(val)
                except ValueError:
                    val = float(val)
                metrics_res[metric_name] += val
    return metrics_res
