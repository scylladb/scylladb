import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester
from tools.cluster_topology import generate_cluster_topology
from tools.metrics import get_node_metrics
from tools.stress import create_stress_compatible_table

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestHeatWeightedLB(Tester):
    METRICS = ["scylla_storage_proxy_coordinator_reads_local_node", "scylla_column_family_cache_hit_rate.*cf=.*standard1"]

    OP_CNT = 5000
    METRICS_COUNT = 60

    def _pretty_print(self, metrics):
        for key in metrics:
            for node_ind in (1, 2, 3):
                if not metrics[key][node_ind]:
                    logger.debug(f"WARNING: no metrics found for {key}")
                    continue
            logger.debug(key)
            logger.debug("{:10s}   {:10s}   {:10s}".format("node1", "node2", "node3"))
            for i in range(self.METRICS_COUNT):
                value = "delta" if "cache_hit_rate" not in key else "val"
                logger.debug(f"{metrics[key][1][i][value]!s:15s}  {metrics[key][2][i][value]!s:15s}  {metrics[key][3][i][value]!s:15s}")

    def get_metrics_from_nodes(self):
        logger.debug("Get metrics from all nodes")
        node_metrics = {k: {1: [], 2: [], 3: []} for k in self.METRICS}
        for _ in range(self.METRICS_COUNT):
            t = time.time()
            for node_ind in (1, 2, 3):
                metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node_ind), metrics=self.METRICS)
                for k, v in metrics.items():
                    delta = v - node_metrics[k][node_ind][-1]["val"] if node_metrics[k][node_ind] else 0
                    node_metrics[k][node_ind].append(dict(val=v, delta=delta))
            delta = time.time() - t
            if delta < 1:
                time.sleep(1 - delta)
        self._pretty_print(node_metrics)
        return node_metrics

    def wait_for_read_load(self, min_delta=50, timeout=120):
        """
        Block until every node serves real read load before sampling starts.

        The docker cassandra-stress read container can take tens of seconds to
        ramp up. Sampling before that leaves too few evenly loaded samples for
        verify_metrics, so gate collection on an observed per-second read delta
        on all three nodes instead of a fixed sleep.
        """
        metric = "scylla_storage_proxy_coordinator_reads_local_node"
        prev: dict[int, float | None] = {node_ind: None for node_ind in (1, 2, 3)}
        deadline = time.time() + timeout
        while time.time() < deadline:
            t = time.time()
            deltas = {}
            for node_ind in (1, 2, 3):
                val = get_node_metrics(node_ip=self.cluster.get_node_ip(node_ind), metrics=[metric])[metric]
                last = prev[node_ind]
                deltas[node_ind] = val - last if last is not None else 0
                prev[node_ind] = val
            if all(d >= min_delta for d in deltas.values()):
                logger.info(f"Read load established, per-node deltas: {deltas}")
                return
            delta = time.time() - t
            if delta < 1:
                time.sleep(1 - delta)
        logger.warning(f"Timed out after {timeout}s waiting for read load on all nodes, proceeding anyway")

    def verify_metrics(self, metrics, cached=True):  # noqa: PLR0912, PLR0915
        """
        On regular read all the parameters have an equal values for all the nodes,
        but after restart of one of the nodes(node2), the values for this node expected to be
        much less then on other nodes, and grow with cache filling.
        """
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            logger.debug("Skippipng verify metrics in debug mode")
            return

        logger.debug("Verify metrics")
        for key in ("scylla_storage_proxy_coordinator_reads_local_node",):
            # find eligible samples window
            min_count = 30
            first = None
            last = len(metrics[key][2])
            for i in range(last):
                has_all = True
                for node_ind in (1, 2, 3):
                    if metrics[key][node_ind][i]["delta"] == 0:
                        has_all = False
                        break
                if has_all:
                    if first is None:
                        first = i
                elif first is not None:
                    if i - first >= min_count:
                        # eligible window was found: finish the iteration
                        last = i
                        break
                    else:
                        # samples window doesn't contain enough elements: try again
                        first = None

            logger.debug(f"Verify {key}: first={first} last={last} cached={cached}")
            assert first is not None, f"Did not find eligible samples range for key={key}"
            assert last - first >= min_count, f"Did not find enough eligible samples range for key={key}. Expected at least {min_count} but found only {last - first} samples."
            first += 10
            last -= 10
            if cached:
                lower_bound = 0.25
                upper_bound = 4
                for i in range(first + 1, last - 1):
                    for node_ind in (1, 3):
                        # parameter's delta is within 0.25x - 4x for all the nodes
                        delta_ratio = metrics[key][node_ind][i]["delta"] / metrics[key][2][i]["delta"]
                        err_msg = "Cache difference between node{} and node2 is out of range: {}/{}={} expected to be {} <= ratio <= {}. index={} metric {}".format(
                            node_ind, metrics[key][node_ind][i]["delta"], metrics[key][2][i]["delta"], delta_ratio, lower_bound, upper_bound, i, key
                        )
                        assert lower_bound <= delta_ratio <= upper_bound, err_msg
            else:
                mean_window = 5
                last -= mean_window
                for i in range(first, last):
                    for node_ind in (1, 3):
                        # parameter's delta running average on the restarted node is within reasonable bounds
                        mean_avg = sum([metrics[key][node_ind][j]["delta"] for j in range(i, i + mean_window)]) / mean_window
                        node_mean_avg = sum([metrics[key][2][j]["delta"] for j in range(i, i + mean_window)]) / mean_window
                        ratio = mean_avg / node_mean_avg
                        lower_bound = 0.8
                        upper_bound = 11 + 2 * (last - i) / (last - first)
                        err_msg = f"Cache difference between node{node_ind} and node2 is out of range: {mean_avg}/{node_mean_avg}={ratio} expected to be {lower_bound} < ratio <= {upper_bound}. index={i} metric {key}"
                        assert lower_bound <= ratio <= upper_bound, err_msg
        key = "scylla_column_family_cache_hit_rate.*cf=.*standard1"
        last_drop = None
        # find eligible samples window
        first = None
        last = len(metrics[key][2])
        for i in range(last):
            has_all = True
            for node_ind in (1, 2, 3):
                if metrics[key][node_ind][i]["val"] == 0.0:
                    has_all = False
                    break
            if has_all:
                if first is None:
                    first = i
            elif first is not None:
                if i - first >= min_count:
                    # eligible window was found: finish the iteration
                    last = i
                    break
                else:
                    # samples window doesn't contain enough elements: try again
                    first = None
        logger.debug(f"Verify {key}: first={first} last={last} cached={cached}")
        assert first is not None, f"Did not find eligible samples range for key={key}"
        assert last - first >= min_count, f"Did not find enough eligible samples range for key={key}. Expected at least {min_count} but found only {last - first} samples."
        first += 5
        last -= 5
        for i in range(first, last):
            for node_ind in (1, 3):
                if cached:
                    # parameter's value is equal for all the nodes
                    assert metrics[key][node_ind][i]["val"] == metrics[key][2][i]["val"]
                else:
                    # parameter's value on the restarted node is less than others
                    assert metrics[key][node_ind][i]["val"] >= metrics[key][2][i]["val"]
            if not cached:
                # parameter's value on the restarted node may drop, but just a bit
                ratio = metrics[key][2][i]["val"] / metrics[key][2][i - 1]["val"]
                if ratio < 1.0:
                    # allow one slight drop and then plateau at most
                    assert ratio >= 0.98
                    assert last_drop is None
                    last_drop = i
                elif ratio > 1.0:
                    last_drop = None
        # parameter's value on the restarted node is on a growing trend
        if not cached:
            v = metrics[key][2][first]["val"]
            val_min = v
            val_min_pos = first
            val_max = v
            val_max_pos = first
            for i in range(first + 1, last):
                v = metrics[key][2][i]["val"]
                if v < val_min:
                    val_min = v
                    val_min_pos = i
                if v > val_max:
                    val_max = v
                    val_max_pos = i
            assert val_max_pos > val_min_pos
            assert (first + last) / 2 > val_min_pos
            assert val_max_pos >= (first + last) / 2

    def run_read_thread(self):
        executor = ThreadPoolExecutor(max_workers=1)

        def run_read():
            logger.debug("Run stress read")
            resp = self.node1.stress_object(["read", "cl=QUORUM", "duration=1m", "-schema", "replication(factor=3)", "-rate", "threads>=4", "threads<=64", "-pop", f"seq=1..{self.OP_CNT}"])
            if not resp or "total partitions:read" not in resp:
                raise Exception(f"Error running stress test: {resp}")

        return executor.submit(run_read)

    @pytest.mark.use_cassandra_stress
    def run_heat_weighted_load_balancing(self, cl):
        """
        Create 3-node cluster, run write, then read all the data(heat cache),
        restart one node, check that it starts to serve gradually due to a cold cache.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"enable_keyspace_column_family_metrics": True})
        cluster.populate(generate_cluster_topology(rack_num=3)).start(wait_for_binary_proto=True, wait_other_notice=True)
        self.node1, self.node2, self.node3 = cluster.nodelist()
        self.ignore_log_patterns += [r"sstable read queue overloaded"]

        logger.debug("Run stress write")
        create_stress_compatible_table(self, node=self.node1, rf=3)
        out, _err = self.node1.run_cqlsh("DESCRIBE SCHEMA; // post-line comment", return_output=True)
        logger.debug(out)
        resp = self.node1.stress_object(["write", f"cl={cl}", f"n={self.OP_CNT}", "-schema", "replication(factor=3)", "-rate", "threads=4", "-pop", f"seq=1..{self.OP_CNT}"])
        if not resp or "total partitions:write" not in resp:
            raise Exception(f"Error running stress test: {resp}")

        logger.debug("Flush system tables")
        self.node1.flush()
        self.node2.flush()
        self.node3.flush()

        thr = self.run_read_thread()
        self.wait_for_read_load()
        metrics = self.get_metrics_from_nodes()
        self.verify_metrics(metrics)

        if not thr.done():
            logger.debug("Cancel stress read")
            thr.cancel()

        logger.debug(f"Restart node {self.node2.name}")
        self.node2.stop(wait_other_notice=True)
        self.node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        thr = self.run_read_thread()
        self.wait_for_read_load()
        metrics = self.get_metrics_from_nodes()
        self.verify_metrics(metrics, cached=False)

        if not thr.done():
            logger.debug("Cancel stress read")
            thr.cancel()

    def test_heat_weighted_load_balancing_cl_one(self):
        self.run_heat_weighted_load_balancing("ONE")

    def test_heat_weighted_load_balancing_cl_two(self):
        self.run_heat_weighted_load_balancing("TWO")

    def test_heat_weighted_load_balancing_cl_any(self):
        self.run_heat_weighted_load_balancing("ANY")

    def test_heat_weighted_load_balancing_cl_quorum(self):
        self.run_heat_weighted_load_balancing("QUORUM")
