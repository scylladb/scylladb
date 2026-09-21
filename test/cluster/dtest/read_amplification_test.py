import logging
import math
import multiprocessing
import time
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from ccmlib.node import TimeoutError
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from tools.data import insert_c1c2
from tools.marks import unmark
from tools.metrics import get_node_metrics, wait_for_metric
from tools.paging import PageFetcher
from tools.retrying import retry_with_func_attempts

logger = logging.getLogger(__name__)
PARTITION_READ = "partition"
SCAN_READ = "scan"
KBYTE = 1024


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestReadAmplification(Tester):
    @staticmethod
    def get_metrics(metric_names, node_ips=None):
        node_ips = node_ips or []
        metrics = {n: 0 for n in metric_names}
        for node_ip in node_ips:
            for key in metrics:
                assert wait_for_metric(key, node_ip), f"Metric '{key}' is not exposed on {node_ip} node exporter target"
            node_metrics = get_node_metrics(node_ip=node_ip, metrics=list(metrics.keys()))
            metrics = {k: metrics[k] + node_metrics[k] for k in metrics}
        logger.debug(metrics)
        return metrics

    @staticmethod
    def get_metrics_safely(metric_names, node_ips=None):
        node_ips = node_ips or []
        metrics = {n: 0 for n in metric_names}
        for node_ip in node_ips:
            node_metrics = get_node_metrics(node_ip=node_ip, metrics=list(metrics.keys()))
            metrics = {k: metrics[k] + node_metrics.get(k, 0) for k in metrics}
        logger.debug(metrics)
        return metrics

    def test_no_read_amplification_on_repair(self):
        """
        Check total bytes read on repair corresponds to data size
        """
        self.no_read_amplification_on_repair(with_mv=False)

    def test_no_read_amplification_on_repair_with_mv(self):
        """
        Check total bytes read on repair corresponds to data size
        """
        self.no_read_amplification_on_repair(with_mv=True)

    def no_read_amplification_on_repair(self, with_mv, count=500):  # pylint:disable=too-many-locals,too-many-statements  # noqa: PLR0915
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False, "compaction_enforce_min_threshold": True})
        logger.info("Starting cluster..")
        cluster_topology = {"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}}
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        create_ks(session=session, name="ks", rf=3)
        create_cf(session=session, name="cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        if with_mv:
            statement = "CREATE MATERIALIZED VIEW ks.cf_mv AS SELECT * FROM ks.cf WHERE key is not null and c1 is not null PRIMARY KEY (c1, key)"
            logger.info(statement)
            session.execute(statement)
            session.execute("ALTER MATERIALIZED VIEW ks.cf_mv WITH read_repair_chance=0.0")

        keys = range(1, 100)
        c1_values = ["value1"] * len(keys)
        c2_values = ["value2"] * len(keys)
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ALL, c1_values=c1_values, c2_values=c2_values)

        node_to_repair = nodes[1]
        logger.info(f"Stop {node_to_repair.name}")
        node_to_repair.stop(wait_other_notice=True)

        cnt = count
        if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
            cnt = int(count / 5)
        size = 2 * KBYTE
        one_kb = "a" * 1024 * 1  # 1KB
        cs_value = [one_kb] * cnt
        logger.info("Insert data")
        insert_c1c2(session, keys=range(1, cnt + 1), consistency=ConsistencyLevel.QUORUM, c1_values=cs_value, c2_values=cs_value, concurrency=10)

        logger.info(f"Start {node_to_repair.name}")
        node_to_repair.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.info(f"Start {node_to_repair.name} repair")
        executor = ThreadPoolExecutor(max_workers=1)

        def repair():
            nodes[1].repair(local=True, keyspace="ks", tables=["cf"])

        def find_metrics_diffs(metrics_end, metrics_beg):
            diffs = {}
            for param in metrics_end:
                assert param in metrics_beg
                diffs[param] = metrics_end[param] - metrics_beg[param]
            return diffs

        node_ips = [cluster.get_node_ip(node_ind) for node_ind in range(1, len(nodes) + 1)]
        metric_names = ["rx_row_bytes", "tx_row_bytes"]
        start_metrics = self.get_metrics_safely(metric_names, node_ips)

        thr = executor.submit(repair)

        logger.info("Verify there is no read amplification on repair")
        amplification_rate = 3
        started = time.time()
        timeout = 600
        while not thr.done():
            bytes_total = find_metrics_diffs(self.get_metrics_safely(metric_names, node_ips), start_metrics)
            for param in bytes_total:
                assert 0 <= bytes_total[param] < size * cnt * amplification_rate
            if time.time() - started >= timeout:
                node_to_repair.wait_until_stopped(wait_seconds=0, dump_core=True)
                raise TimeoutError(f"{node_to_repair.name} repair timed out after {timeout} seconds")
            time.sleep(10)

        thr.result()

        max_val = find_metrics_diffs(self.get_metrics_safely(metric_names, node_ips), start_metrics)
        for key in max_val:
            assert 0 < max_val[key] < size * cnt * amplification_rate
            logger.info(f"{key}: {max_val[key]}(+{int(math.fabs(max_val[key] - (cnt * size)) * 100 / (cnt * size))}%)")

    def read_amplification(  # noqa: PLR0915
        self,
        read_type,
        read_size,  # pylint:disable=too-many-locals,too-many-statements
        wait_interval=1,
        max_ratio_expected=10,
    ):
        """
        Check total bytes read corresponds to data size
        """
        cluster = self.cluster
        logger.info("Starting cluster..")
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session=session, name="ks", rf=1)
        create_cf(session=session, name="cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        c1_value = "a" * KBYTE
        c2_value = "b" * KBYTE
        size = KBYTE * 2
        cnt = read_size // size
        logger.info(f"count: {cnt}")
        c1s = [c1_value] * cnt
        c2s = [c2_value] * cnt
        logger.info("Insert data")
        insert_c1c2(session, keys=range(cnt), consistency=ConsistencyLevel.ONE, c1_values=c1s, c2_values=c2s)

        node.flush()
        logger.info("Run compaction to prevent running this during read")
        node.compact()
        time.sleep(10)
        logger.info("Restart node - for cache cleanup")
        node.stop(wait_other_notice=True)
        node.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10)

        logger.info("Metrics before read")
        session = self.patient_cql_connection(node)
        metric_names = ["scylla_reactor_aio_bytes_read", "scylla_reactor_aio_reads"]
        node_ip = cluster.get_node_ip(1)
        io_bytes_before = self.get_metrics(metric_names, [node_ip])
        logger.info(io_bytes_before)

        def run_read():
            for idx in range(cnt):
                result = list(session.execute(f"SELECT c1,c2 FROM ks.cf where key = 'k{idx}'"))
                assert len(result) == 1, f'The expected result size is "1" (got "{len(result)}")'

        def run_scan_read():
            future = session.execute_async(SimpleStatement("select * from ks.cf", fetch_size=25))
            page_fetcher = PageFetcher(future).request_all(timeout=30)
            all_pages = page_fetcher.num_results_all()
            assert sum(all_pages) == cnt

        def get_total_read_bytes(bytes_read, bytes_before):
            return bytes_read["scylla_reactor_aio_bytes_read"] - bytes_before["scylla_reactor_aio_bytes_read"]

        logger.info("Start reading")
        thr_target = run_read if read_type == PARTITION_READ else run_scan_read
        executor = ThreadPoolExecutor(max_workers=1)
        thr = executor.submit(thr_target)

        logger.info("Metrics during read")
        total_read_bytes = 0
        while (not thr.done()) or total_read_bytes == 0:
            io_bytes_read = self.get_metrics(metric_names, [node_ip])
            total_read_bytes = get_total_read_bytes(io_bytes_read, io_bytes_before)
            logger.info(io_bytes_read)
            time.sleep(wait_interval)

        thr.result()

        logger.info("Metrics after read")
        io_bytes_after = self.get_metrics(metric_names, [node_ip])
        logger.info(io_bytes_after)

        logger.info("Verify there is no read amplification")
        total_read_bytes = get_total_read_bytes(io_bytes_after, io_bytes_before)
        total_written_bytes = cnt * size
        ampl = total_read_bytes / total_written_bytes
        ampl_percent = total_read_bytes * 100 / total_written_bytes
        size_formatted = read_size / KBYTE
        size_formatted = f"{size_formatted}kb" if size_formatted < KBYTE else f"{size_formatted / KBYTE}mb"
        logger.info(f"Read amplification for {size_formatted} data size: {ampl} times or {ampl_percent}%")
        assert ampl <= max_ratio_expected, f"Read amplification is too large: {ampl} times"

    @pytest.mark.single_node
    def test_no_amplification_on_read_20kb(self):
        self.read_amplification(PARTITION_READ, KBYTE * 20, 1, 20)

    @pytest.mark.single_node
    def test_no_amplification_on_read_400kb(self):
        self.read_amplification(PARTITION_READ, KBYTE * 400)

    @pytest.mark.single_node
    def test_no_amplification_on_read_20mb(self):
        self.read_amplification(PARTITION_READ, KBYTE * KBYTE * 20)

    @pytest.mark.single_node
    def test_no_amplification_on_scanning_read_20kb(self):
        self.read_amplification(SCAN_READ, KBYTE * 20)

    @pytest.mark.single_node
    def test_no_amplification_on_scanning_read_2mb(self):
        self.read_amplification(SCAN_READ, KBYTE * KBYTE * 2)

    @pytest.mark.single_node
    def test_no_amplification_on_scanning_read_20mb(self):
        self.read_amplification(SCAN_READ, KBYTE * KBYTE * 20)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestMultiShardReader(Tester):
    """
    This class holds the test that covers the issue that cause to read amplification

    Cover issue: https://github.com/scylladb/scylla/issues/8161
    Commit: https://github.com/scylladb/scylla/commit/bc1fcd3db20eb957524387214617b613f1cab3e7

    The multishard combining reader currently assumes that all shards have
    data for the read range. This however is not always true and in extreme
    cases (like reading a single token) it can lead to huge read
    amplification.
    After this commit, the multishard reader will only read from shards that
    have data relevant to the read range, both in the case of normal reads
    and also for read-ahead.
    """

    def prepare(
        self,
        nodes: int,
        rf: int,
        create_keyspace: bool = True,
        protocol_version: int = 4,
        jvm_args: list | None = None,
    ) -> tuple[Session, ScyllaNode]:
        if jvm_args is None:
            jvm_args = []

        cluster = self.cluster
        cluster.set_configuration_options(values={"max_cached_partition_size_in_bytes": 1})
        cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=jvm_args)

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            create_ks(session, "ks", rf)
            session.execute("USE ks")
        return session, node1

    @staticmethod
    def insert_rows_with_blob(session, n, table_name="cf"):
        keys = list(range(n))

        c_value = "a" * 1000000
        statement = session.prepare(f"INSERT INTO {table_name} (key, c1, c2, c3, c4) VALUES (?, textAsBlob('{c_value}'), textAsBlob('{c_value}'), textAsBlob('{c_value}'), textAsBlob('{c_value}'))")
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement, [[f"k{k}"] for k in keys])

    @staticmethod
    @retry_with_func_attempts
    def run_query_and_get_its_session_id(node: ScyllaNode, session: Session, query: str, num_attempts: int = 5) -> str:
        logger.debug("Run query: %s", query)
        session.execute(query=query, trace=True)
        logger.debug("Get session_id of the query: {query}")
        sessions = list(session.execute("select session_id, parameters from system_traces.sessions"))
        # Row(session_id=UUID('d1a0fa80-1c67-11ec-aea3-3a3e0d08d0b2'),
        # parameters=OrderedMapSerializedKey([('consistency_level', 'ONE'), ('page_size', '5000'),
        # ('query', 'select token(key) from ks.cf where token(key) = -4307320966523859'),
        # ('serial_consistency_level', 'SERIAL'), ('user_timestamp', '1632399276584071')]))
        session_id = [ses.session_id for ses in sessions if ses.parameters["query"] == f"{query}"]
        assert session_id, "Not found session for tested query"
        return session_id[0]

    @staticmethod
    def get_reader_shards(session: Session, session_id: str) -> defaultdict:
        logger.debug("Get events for session %s", session_id)
        events = list(session.execute(f"select source, activity, thread from system_traces.events where session_id = {session_id}"))

        # Row(source='127.0.68.1', activity='Creating shard reader on shard: 1', thread='shard 1')
        # Row(source='127.0.68.1', activity='node1/data/ks/cf-567d60d0242511ec80c5342185a9b495/md-3-big-Index.db:
        # scheduling bulk DMA read of size 33 at offset 0', thread='shard 1/sl:default')

        shards_by_source = defaultdict(set)
        for activity in ["Creating shard reader on shard", "scheduling bulk DMA read", "Reading partition range"]:
            for event in events:
                if activity in event.activity:
                    # `thread` has one of the following formats (depending on Scylla's version):
                    # - "shard {shard_nr}" (e.g. "shard 1")
                    # - "shard {shard_nr}/{scheduling_group_name}" (e.g. "shard 1/sl:default")
                    # The below expression should extract shard_nr from either of the above.
                    shard = event.thread.split("/")[0].replace("shard ", "")
                    shards_by_source[event.source].update(shard)

        assert shards_by_source, f"Failed to find reader shards from tracing events for session {session_id}. Events: {events}"
        logger.debug("Partition data has been read from shards: %s", shards_by_source)
        return shards_by_source

    def test_create_reader_on_one_shard_1node_cluster(self):
        """
        Test scenario:
         - start cluster with one node and SMP > 1
         - create table with 10 partitions
         - enable slow query tracing
         - read one token and find using system_traces.events table which shard it was read
         Expected to read from one shard
        """
        cluster_topology = {"dc1": {"rack1": 1}}
        self._create_reader_on_one_shard(nodes=cluster_topology, rf=1)

    def test_create_reader_on_one_shard_3nodes_cluster(self):
        """
        Test scenario:
         - start cluster with 3 nodes and SMP > 1
         - create keyspace with RF = 2
         - create table with 10 partitions
         - enable slow query tracing on all nodes
         - read one token and find using system_traces.events table which shard it was read
         Expected to read from one shard on every node
        """
        cluster_topology = {"dc1": {"rack1": 2, "rack2": 1}}
        self._create_reader_on_one_shard(nodes=cluster_topology, rf=2)

    def _create_reader_on_one_shard(self, nodes: dict | int, rf: int):
        smp = min(multiprocessing.cpu_count() // 2 + 1, 2)
        assert smp > 1, "The test can't be run with SMP 1. Run the test on the instance with more CPU"

        logger.debug("Start cluster with SMP %d", smp)
        session1, node1 = self.prepare(nodes=nodes, rf=rf, jvm_args=["--smp", str(smp), "--memory", f"{512 * int(smp)}M"])
        create_cf(session1, name="cf", columns={"c1": "blob", "c2": "blob", "c3": "blob", "c4": "blob"})

        logger.debug("Insert 10 row")
        self.insert_rows_with_blob(session1, n=10)
        self.cluster.flush()

        logger.debug("Get key token")
        query = "select token(key) from ks.cf where key = 'k9'"
        out = list(session1.execute(query))
        assert out, "Row with key 'k9' is not found"
        token = out[0][0]

        for node in self.cluster.nodelist():
            logger.debug("Restart %s to empty the cache", node.name)
            node.stop(wait_other_notice=True)
            node.start(wait_other_notice=True)

        logger.debug("Find a shard where the single token lives on")
        query = f"select count(*) from ks.cf where token(key) = {token}"

        session_id = self.run_query_and_get_its_session_id(node=node1, session=session1, query=query)
        one_token_reader_shards = self.get_reader_shards(session=session1, session_id=session_id)

        many_shards_source = ""
        for source, shards in one_token_reader_shards.items():
            if len(shards) > 1:
                many_shards_source += f"{source}: {shards}. "

        assert not many_shards_source, f"Reader on the next host(s) was created on more then one shards: {many_shards_source} Expected reader on the one shard"
