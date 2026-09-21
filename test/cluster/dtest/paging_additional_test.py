import logging
import uuid
from random import randint

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import create_ks, get_ip_from_node
from paging_test import BasePagingTester, PageAssertionMixin, PageFetcher
from tools.datahelp import create_rows
from tools.metrics import get_node_metrics

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestAggregatePaging(BasePagingTester, PageAssertionMixin):
    """
    Basic aggregation tests using paging
    """

    def _test_paged_count_with_limit(self, sizes):
        session = self.prepare()
        create_ks(session, "test_aggregate_paging", 3)
        session.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(text):
            return uuid.uuid4()

        data = """
               | id     |value   |
               +--------+--------+
          *5001| [uuid] |testing |
            """
        create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": random_txt, "value": str})

        # Note that both page size and limit is less than actual num of rows.
        # Thus we should always get a count == limit
        # They main thing we are testing is that page size > limit does not break the
        # count result. (#650)
        for page_size in sizes:
            future = session.execute_async(SimpleStatement(f"select count(*) from paging_test limit {1}", fetch_size=page_size, consistency_level=ConsistencyLevel.ALL))
            pf = PageFetcher(future).request_all()
            rows_count = pf.num_results_all()
            all_data = pf.all_data()
            assert rows_count == [1], f"Expected 1 row, but got {rows_count}"
            assert all_data == [{"count": 5001}], f'Expected "{"count": 5001}", but got {all_data}'

    @pytest.mark.scylla_mode("!debug")
    def test_paged_count_with_limit(self):
        self._test_paged_count_with_limit([10, 100, 1000, 3000, 5000])

    @pytest.mark.scylla_mode("debug")
    def test_paged_count_with_limit_debug(self):
        self._test_paged_count_with_limit([10, 100, 250])

    def _test_paged_count_with_clustering_key(self, order):
        session = self.prepare()
        create_ks(session, "test_aggregate_paging", 3)
        session.execute("CREATE TABLE paging_test (pk int, ck1 int, ck2 text, v int, PRIMARY KEY(pk, ck1, ck2))")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
               | pk | ck1 | ck2          | v |
               +----+-----+--------------+---+
          *1234| 0  | 1   | [random_txt] | 0 |
               """

        create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"pk": int, "ck1": int, "ck2": random_txt, "v": int})

        future = session.execute_async(SimpleStatement(f"select count(*) from paging_test where pk = 0 and ck1 = 1 order by ck1 {order}, ck2 {order}", fetch_size=100, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future).request_all()
        rows_count = pf.num_results_all()
        all_data = pf.all_data()
        assert rows_count == [1], f"Expected 1 row, but got {rows_count}"
        assert all_data == [{"count": 1234}], f'Expected "{"count": 1234}", but got {all_data}'

    @pytest.mark.dtest_debug
    def test_paged_count_with_clustering_key(self):
        self._test_paged_count_with_clustering_key("asc")

    def test_paged_count_with_clustering_key_reversed(self):
        self._test_paged_count_with_clustering_key("desc")


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestPagingSavedQueryStateBase(BasePagingTester):
    LOOKUPS = "querier_cache_lookups"
    MISSES = "querier_cache_misses"
    DROPS = "querier_cache_drops"
    TIME_BASED_EVICTIONS = "querier_cache_time_based_evictions"
    RESOURCE_BASED_EVICTIONS = "querier_cache_resource_based_evictions"
    POPULATION = "querier_cache_population"

    ALL_METRICS = [LOOKUPS, MISSES, DROPS, TIME_BASED_EVICTIONS, RESOURCE_BASED_EVICTIONS]

    def metrics_equal(self, node_metrics, expected_metrics):
        for metric in self.ALL_METRICS:
            expected_metric = expected_metrics.get(metric.replace("querier_cache_", ""), 0)

            if expected_metric == -1:
                continue

            if node_metrics[metric] != expected_metric:
                logger.debug(f"metrics_equal: node_metrics[{metric}] {node_metrics[metric]} != {expected_metric} expected_metric")
                return False

        return True

    def match_node_metrics(self, node_metrics, expected_metrics, matched):
        if expected_metrics is None:
            return

        matched_any = False

        for i, metrics_variant in enumerate(expected_metrics):
            if self.metrics_equal(node_metrics, metrics_variant):
                matched.add(i)
                matched_any = True

        # The node's metrics must match at least one expected metrics
        assert matched_any, f"Node metrics doesn't match any of the expected metrics: \nnode_metrics: {node_metrics}\nexpected_metrics: {expected_metrics}"

    def assert_nodes_metrics(self, expected_metrics, verifier=None):
        nodes = self.cluster.nodelist()

        matched = set()

        for node in nodes:
            node_metrics = get_node_metrics(get_ip_from_node(node), metrics=self.ALL_METRICS)
            logger.debug(f"{node.name} metrics: {node_metrics}")
            self.match_node_metrics(node_metrics, expected_metrics, matched)
            if verifier is not None:
                verifier(node_metrics)

        # All expected metrics have to match at least node's metrics
        assert len(matched) == len(expected_metrics), f"Expected {len(expected_metrics)}, but got {len(matched)}"


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestLargePaging(TestPagingSavedQueryStateBase, PageAssertionMixin):
    """
    Tests for queries attempting to fetch large pages
    Cluster topology is 1 dc 3 racks with one node in each rack
    Replication factor is 3
    """

    KS_NAME = "test_large_paging"
    CF_NAME = "paging_test"

    def prepare_schema(self):
        self.session = self.prepare()
        create_ks(self.session, self.KS_NAME, 3)

    def fill_data(self, data, data_size, keys, vals, format_funcs=None):
        if format_funcs is None:
            format_funcs = {}

        def get_key(text):
            return str(uuid.uuid4())

        def get_data(text):
            return " " * data_size

        format_funcs.update({key: get_key for key in keys})
        format_funcs.update({val: get_data for val in vals})
        create_rows(data, self.session, self.CF_NAME, cl=ConsistencyLevel.ALL, format_funcs=format_funcs)

    def validate_data(self, query, fetch_size, row_cnt, validate_metrics=False):
        future = self.session.execute_async(SimpleStatement(query, fetch_size=fetch_size, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future).request_all()
        all_pages = pf.num_results_all()

        assert sum(all_pages) == row_cnt, f"Expected {row_cnt}, got {sum(all_pages)}"
        for page in all_pages:
            assert page <= fetch_size, f"Fetch size {page} is more then {fetch_size} unexpectedly"

        def verify_misses(node_metrics):
            assert node_metrics["querier_cache_misses"] == node_metrics["querier_cache_resource_based_evictions"], node_metrics

        if validate_metrics:
            self.assert_nodes_metrics([{"lookups": pf.requested_pages - 1, "misses": -1, "resource_based_evictions": -1}] * len(self.cluster.nodelist()), verifier=verify_misses)

    @pytest.mark.dtest_debug
    def test_large_page_range_queries(self):
        self.prepare_schema()
        self.session.execute("CREATE TABLE %s (pk text, ck text, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | ck        | v          |
               +-----------+-----------+------------+
          *1000| [get_key] | [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=64 * 1024, keys=["pk", "ck"], vals=["v"])
        self.validate_data(query="select * from %s" % self.CF_NAME, fetch_size=1000, row_cnt=1000)

    @pytest.mark.dtest_debug
    def test_large_page_range_queries_static_columns(self):
        self.prepare_schema()
        self.session.execute("CREATE TABLE %s (pk text, ck text, s text static, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | s          |
               +-----------+------------+
          *1000| [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=64 * 1024, keys=["pk"], vals=["s"])
        self.validate_data(query="select * from %s" % self.CF_NAME, fetch_size=1000, row_cnt=1000)

    def test_large_page_single_partition(self):
        self.prepare_schema()
        self.session.execute("CREATE TABLE %s (pk int, ck text, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | ck        | v          |
               +-----------+-----------+------------+
          *1000| 0         | [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=32 * 1024, keys=["ck"], vals=["v"], format_funcs={"pk": int})
        self.validate_data(query="select * from %s where pk = 0" % self.CF_NAME, fetch_size=400, row_cnt=1000, validate_metrics=True)

    def test_small_page_single_partition(self):
        self.prepare_schema()
        self.session.execute("CREATE TABLE %s (pk int, ck text, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | ck        | v          |
               +-----------+-----------+------------+
          *1000| 0         | [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=32 * 1024, keys=["ck"], vals=["v"], format_funcs={"pk": int})
        self.validate_data(query="select * from %s where pk = 0" % self.CF_NAME, fetch_size=15, row_cnt=1000, validate_metrics=True)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestPagingSavedQueryStateSingularRanges(TestPagingSavedQueryStateBase):
    """
    Tests concerned with querier-reuse during paging.
    """

    def setup_simple_table(self, **kwargs):
        create_ks(self.session, "paging_additional_test_querier_reuse", 3)
        query = "CREATE TABLE test_singular (pk int, ck int, val text, PRIMARY KEY (pk, ck))"

        if len(kwargs) > 0:
            options = []
            for key, val in kwargs.items():
                if type(val) is float or type(val) is int:
                    options.append(f"{key}={val}")
                else:
                    options.append(f"{key}='{val}'")

            query += " WITH " + " AND ".join(options)

        self.session.execute(query)

        data = """
             | pk | ck | val    |
             +----+----+--------+
             | 1  | 1  | val1_1 |
             | 1  | 2  | val1_2 |
             | 1  | 3  | val1_3 |
             | 1  | 4  | val1_4 |
             | 2  | 1  | val2_1 |
             | 2  | 2  | val2_2 |
             | 2  | 3  | val2_3 |
             | 2  | 4  | val2_4 |
             | 2  | 5  | val2_5 |
             | 2  | 6  | val2_6 |
        """

        create_rows(
            data,
            self.session,
            "test_singular",
            cl=ConsistencyLevel.ALL,
            format_funcs={
                "pk": int,
                "ck": int,
                "val": str,
            },
        )

        return [
            {"pk": 1, "ck": 1, "val": "val1_1"},
            {"pk": 1, "ck": 2, "val": "val1_2"},
            {"pk": 1, "ck": 3, "val": "val1_3"},
            {"pk": 1, "ck": 4, "val": "val1_4"},
            {"pk": 2, "ck": 1, "val": "val2_1"},
            {"pk": 2, "ck": 2, "val": "val2_2"},
            {"pk": 2, "ck": 3, "val": "val2_3"},
            {"pk": 2, "ck": 4, "val": "val2_4"},
            {"pk": 2, "ck": 5, "val": "val2_5"},
            {"pk": 2, "ck": 6, "val": "val2_6"},
        ]

    def test_single_partition(self):
        """
        Test that the querier is saved and reused.
        """
        self.session = self.prepare()

        data = self.setup_simple_table()

        future = self.session.execute_async(SimpleStatement("select * from test_singular where pk = 1", fetch_size=3, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future)

        pf.request_all()
        all_data = pf.all_data()
        assert all_data == [p for p in data if p["pk"] == 1], f"Expected {[p for p in data if p['pk'] == 1]}, got {all_data}"

        requested_pages = pf.requested_pages
        assert requested_pages == 2, f"Expected 2 pages, got {requested_pages}"
        self.assert_nodes_metrics([{"lookups": requested_pages - 1}] * len(self.cluster.nodelist()))

    @pytest.mark.dtest_debug
    def test_two_partitions(self):
        """
        Test that when the coordinator throws away parts of the results
        the replica recognizes the position mismatch and drops the
        cached querier.
        """
        self.session = self.prepare()

        data = self.setup_simple_table()

        future = self.session.execute_async(SimpleStatement("select * from test_singular where pk in (1, 2)", fetch_size=5, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future)

        pf.request_all()

        requested_pages = pf.requested_pages
        all_data = pf.all_data()
        assert requested_pages == 3, f"Expected 3 pages, got {requested_pages}"
        assert all_data == data, f"Expected {data}, got {all_data}"
        self.assert_nodes_metrics([{"lookups": requested_pages - 1, "drops": 1}] * len(self.cluster.nodelist()))

    def test_replica_usage(self):
        """
        Test that the coordinator sends all page-requests consistently to the
        same replica.
        """
        self.session = self.prepare()

        data = self.setup_simple_table(speculative_retry="NONE", dclocal_read_repair_chance=0.0)

        future = self.session.execute_async(SimpleStatement("select * from test_singular where pk = 2", fetch_size=1, consistency_level=ConsistencyLevel.ONE))
        pf = PageFetcher(future)

        def get_coordinator_reads_metric(node_ip):
            return get_node_metrics(node_ip, metrics=["storage_proxy_coordinator_reads"])["storage_proxy_coordinator_reads"]

        node_ips = [get_ip_from_node(node) for node in self.cluster.nodelist()]
        coordinator_reads_baseline = {node_ip: get_coordinator_reads_metric(node_ip) for node_ip in node_ips}

        pf.request_all()

        for node_ip in node_ips:
            new_reads = get_coordinator_reads_metric(node_ip) - coordinator_reads_baseline[node_ip]
            # Verify that each node was used as a coordinator at least once
            # and therefore that the test is meaningful.
            # Currently the driver will round-robin through the nodes as all
            # them will have the read partitions. If this assumption will not
            # hold in the future this test will become obsolete.
            assert new_reads > 0, f"Expected reads more then 0, but got {new_reads}"

        requested_pages = pf.requested_pages
        all_data = pf.all_data()
        assert requested_pages == 7, f"Expected 7 pages, got {requested_pages}"
        assert all_data == [p for p in data if p["pk"] == 2], f"Expected \"[p for p in data if p['pk'] == 2]\", got {all_data}"
        self.assert_nodes_metrics(({"lookups": pf.requested_pages - 1}, {}))

    def test_per_query_read_repair_decision(self):
        """
        Test that that the read-repair decision made on the first page
        of the query is sticky to all pages of the query.
        """
        self.cluster.set_configuration_options(values={"tombstone_failure_threshold": 500})
        self.session = self.prepare()

        data = self.setup_simple_table(speculative_retry="NONE", dclocal_read_repair_chance=0.5)

        future = self.session.execute_async(SimpleStatement("select * from test_singular where pk = 2", fetch_size=1, consistency_level=ConsistencyLevel.ONE))
        pf = PageFetcher(future)
        pf.request_all()
        requested_pages = pf.requested_pages
        all_data = pf.all_data()
        assert requested_pages == 7, f"Expected 7 pages, got {requested_pages}"
        assert all_data == [p for p in data if p["pk"] == 2], f"Expected \"[p for p in data if p['pk'] == 2]\", got {all_data}"
        self.assert_nodes_metrics(({"lookups": pf.requested_pages - 1}, {}))


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestPagingQueryAlternativeConsistencyLevel(BasePagingTester):
    def test_consistency_level_quorum(self):
        test_ks_name = "keyspace_complex"
        test_table_name = "paged_query_test"
        statement = f"select * from {test_ks_name}.{test_table_name};"

        with self.prepare(consistency_level=ConsistencyLevel.QUORUM) as session:
            logger.info("Creating a new keyspace %s...", test_ks_name)
            create_ks(session, test_ks_name, 3)

            logger.info("Creating a new table %s...", test_table_name)
            session.execute(f"CREATE TABLE IF NOT EXISTS {test_ks_name}.{test_table_name} (k int PRIMARY KEY, v1 int, v2 int)")

            logger.info("Filling %s.%s with data...", test_ks_name, test_table_name)
            for i in range(1000):
                random_int = randint(1, 1000000)
                session.execute(f"INSERT INTO {test_ks_name}.{test_table_name} (k, v1, v2) VALUES ({i}, {random_int}, {random_int + i})")

            logger.info("Executing select query without paging feature...")
            query_result = [list(row.values()) for row in session.execute(statement)]
            assert query_result, f"Query '{statement}' returned no entries"

            logger.info("Executing select query with paging feature...")
            session.default_fetch_size = 100
            future = session.execute_async(query=statement)
            fetcher = PageFetcher(future).request_all()
            paging_query_result = [list(row.values()) for row in fetcher.all_data()]
            assert paging_query_result, f"Paged query '{statement}' returned no entries"

            logger.info("Comparing results...")
            assert sorted(query_result) == sorted(paging_query_result), "Results should be identical!"
