import logging
import re
from datetime import datetime
from time import sleep, time

import pytest
import requests

from dtest_class import Tester, get_ip_from_node
from tools.metrics import prometheus_get

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestScyllaARestApi(Tester):
    def config_and_create_cluster(self, nodes):
        self.cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        return self.cluster.nodelist()

    @staticmethod
    def request_uptime(node):
        url = f"http://{node.address()}:{node.api_port}/system/uptime_ms"
        node_uptime = int(requests.get(url=url).text)
        return node_uptime

    @pytest.mark.single_node
    def test_basic_rest_uptime(self):
        node1 = self.config_and_create_cluster(1)[0]
        upper_start = time()
        previous_node1_uptime = self.request_uptime(node1)
        sleep_ms = 10000
        sleep(sleep_ms // 1000)
        current_node1_uptime = self.request_uptime(node1)
        upper_stop = time()
        lower_ms = sleep_ms - 10
        upper_ms = int((upper_stop - upper_start) * 1000) + 10
        logger.debug(f"lower bound={lower_ms} api delta={current_node1_uptime - previous_node1_uptime} upper bound={upper_ms}")
        assert lower_ms <= current_node1_uptime - previous_node1_uptime <= upper_ms, "The uptime received from scylla does not match the expected uptime"

    @pytest.mark.single_node
    def test_rest_uptime_after_restart(self):
        test_start_time = datetime.now()
        node1 = self.config_and_create_cluster(1)[0]
        sleep(10)
        node1.stop(wait_other_notice=False)
        node1.start(wait_other_notice=False, wait_for_binary_proto=True)

        renewed_node1_uptime = self.request_uptime(node1)
        now = datetime.now()
        assert (now - test_start_time).total_seconds() - 10 > renewed_node1_uptime / 1000, f"The uptime received from scylla does not match the expected uptime\nreceived uptime:{renewed_node1_uptime}"

    def test_compare_node_uptime(self):
        node1, node2 = self.config_and_create_cluster(2)
        sleep(10)
        node1.stop(wait_other_notice=False)
        node1.start(wait_other_notice=False, wait_for_binary_proto=True)

        node1_uptime = self.request_uptime(node1)
        node2_uptime = self.request_uptime(node2)
        assert node2_uptime - node1_uptime > 10000, f"The difference between the nodes' uptime did not match expectations\nnode1: {node1_uptime}\nnode2: {node2_uptime}"

    def get_metrics(self, node_ip, metrics, port="9180", metric_class=None):
        metrics_res = {}
        metric_pattern = re.compile(".*{")
        prometheus_results = [metric for metric in prometheus_get(node_ip, port).splitlines() if not metric.startswith("#")]
        for metric in prometheus_results:
            for metric_name in metrics:
                if metric_pattern.match(metric) and re.search(f"{metric_name}{{", metric):
                    if metric_class and not re.search(f'class="{metric_class}"', metric):
                        continue
                    name, val = metric.split()
                    metrics_res[name.split("{")[0]] = float(val)
        return metrics_res

    @staticmethod
    def drop_row_cache(node):
        url = f"http://{node.address()}:{node.api_port}/system/drop_sstable_caches"
        requests.post(url=url)

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    def test_drop_row_cache(self):
        node1 = self.config_and_create_cluster(1)[0]
        increase_metrics = [
            "scylla_cache_reads_with_misses",
            "scylla_cache_row_misses",
            "scylla_cache_row_evictions",
        ]
        decrease_metrics = [
            "scylla_lsa_used_space_bytes",
            "scylla_cache_partitions",
            "scylla_lsa_memory_allocated",
            "scylla_cache_rows",
        ]

        metrics = increase_metrics + decrease_metrics

        stress_cmd = ["n=50K", "-rate", "threads=4"]
        self.cluster.stress(["write", *stress_cmd])
        self.cluster.stress(["read", *stress_cmd])

        before = self.get_metrics(get_ip_from_node(node1), metrics)
        self.drop_row_cache(node1)
        after = self.get_metrics(get_ip_from_node(node1), metrics)

        for metric in increase_metrics:
            assert after[metric] >= before[metric], f"{metric} should have increased after dropping cache"
        for metric in decrease_metrics:
            assert after[metric] <= before[metric], f"{metric} should have decreased after dropping cache"
