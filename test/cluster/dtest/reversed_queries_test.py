import logging
import os
import random
import signal
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from cassandra import ConsistencyLevel, ReadFailure
from cassandra.query import FETCH_SIZE_UNSET, SimpleStatement, dict_factory
from ccmlib.utils.version import ComparableScyllaVersion

from dtest_class import Tester, create_ks
from paging_test import BasePagingTester, PageAssertionMixin, PageFetcher
from tools.cluster import has_views_with_tablets_experimental_feature
from tools.cluster_topology import generate_cluster_topology
from tools.datahelp import create_rows
from tools.marks import unmark
from upgrade_test import UpgradeTester, upgrade_matrix_from_last_release_version

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class ConcurrentExecutor:
    request: pytest.FixtureRequest = None

    @pytest.fixture(scope="function", autouse=True)
    def attach_request(self, request):
        self.request = request

    def run_concurrently(self, worker_count, f, stop_event=None):
        if stop_event is None:
            stop_event = Event()
            self.request.addfinalizer(lambda: stop_event.set())
        worker_executor = ThreadPoolExecutor(max_workers=worker_count)

        def work(worker_id):
            try:
                f(stop_event, worker_id)
            except:
                stop_event.set()
                raise

        futs = [worker_executor.submit(work, i) for i in range(worker_count)]
        worker_executor.shutdown(wait=True)
        for future in futs:
            future.result()


@pytest.mark.dtest_full
class TestReversedQueriesPaging(BasePagingTester, PageAssertionMixin):
    def reversed_query_template(self, data, fetch_size, expected_page_count, expected_rows):
        session = self.prepare()
        create_ks(session, "test_reversed_queries", 3)
        session.execute("CREATE TABLE paging_test (bucket int, id int, value text, PRIMARY KEY (bucket, id))")

        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"bucket": int, "id": int, "value": str})

        future = session.execute_async(SimpleStatement("select id from paging_test where bucket = 1 order by id desc", fetch_size=fetch_size, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future)
        pf.request_all()

        assert not pf.has_more_pages
        data = pf.all_data()
        assert pf.pagecount() == expected_page_count
        assert len(expected_data) == len(data)
        assert data == expected_rows

    def test_with_less_results_than_page_size(self):
        data = """
            |bucket|id| value          |
            +------+--+----------------+
            |1     |1 |testing         |
            |1     |2 |and more testing|
            |1     |3 |and more testing|
            |1     |4 |and more testing|
            |1     |5 |and more testing|
            """
        expected = [{"id": i} for i in range(5, 0, -1)]
        self.reversed_query_template(data, fetch_size=100, expected_page_count=1, expected_rows=expected)

    def test_with_more_results_than_page_size(self):
        data = """
            |bucket|id| value          |
            +------+--+----------------+
            |1     |1 |testing         |
            |1     |2 |and more testing|
            |1     |3 |and more testing|
            |1     |4 |and more testing|
            |1     |5 |and more testing|
            |1     |6 |testing         |
            |1     |7 |and more testing|
            |1     |8 |and more testing|
            |1     |9 |and more testing|
            |1     |10|and more testing|
            |1     |11|testing         |
            |1     |12|and more testing|
            |1     |13|and more testing|
            """
        expected = [{"id": i} for i in range(13, 0, -1)]
        self.reversed_query_template(data, fetch_size=5, expected_page_count=3, expected_rows=expected)


class BaseReversedQuerySelector:
    def prepare_cluster(self, row_factory=dict_factory):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        self.node = node1
        session = self.patient_cql_connection(node1, row_factory=row_factory)
        return session

    def prepare_schema(self, session, rf=1):
        create_ks(session, "test_reversed_queries", rf)
        session.execute("CREATE TABLE paging_test (bucket int, id int, id2 int, value text, PRIMARY KEY (bucket, id, id2))")

    def execute(self, session, statement, fetch_size=FETCH_SIZE_UNSET):
        response = session.execute(SimpleStatement(statement, consistency_level=ConsistencyLevel.ALL, fetch_size=fetch_size))
        return list(response)

    def populate(self, session):
        data = """
            |bucket|id|id2| value          |
            +------+--+---+----------------+
            |1     |1 |  4|testing         |
            |1     |2 |  1|delete me!      |
            |1     |2 |  5|and more testing|
            |1     |2 |  9|and more testing|
            |1     |2 | 10|delete me!      |
            |1     |3 |  3|and more testing|
            |1     |4 |  2|and more testing|
            |1     |4 |  5|and more testing|
            |1     |4 |  6|delete me!      |
            |1     |5 |  4|delete me!      |
            |1     |6 |  1|delete me!      |
            |1     |6 |  3|and more testing|
            |1     |7 |  1|and more testing|
            |1     |8 |  1|delete me!      |
            |2     |1 |  4|testing         |
            |2     |2 |  1|testing         |
            |2     |2 |  5|and more testing|
            |2     |2 |  9|and more testing|
            |2     |2 | 10|testing         |
            |2     |3 |  3|and more testing|
            |2     |4 |  2|and more testing|
            |2     |4 |  5|and more testing|
            |2     |4 |  6|testing         |
            |2     |5 |  4|testing         |
            |2     |6 |  1|testing         |
            |2     |6 |  3|and more testing|
            |2     |7 |  1|and more testing|
            |2     |8 |  1|testing         |
            |3     |1 |  1|testing         |
            |4     |1 |  1|testing         |
            |5     |1 |  1|testing         |
            """
        _expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"bucket": int, "id": int, "id2": int, "value": str})

        # Introduce row tombstones by deleting marked rows
        self.execute(session, "DELETE FROM paging_test WHERE bucket = 1 AND id < 0")
        self.execute(session, "DELETE FROM paging_test WHERE bucket = 1 AND id = 2 AND id2 < 4")
        self.execute(session, "DELETE FROM paging_test WHERE bucket = 1 AND id = 2 AND id2 > 9")
        self.execute(session, "DELETE FROM paging_test WHERE bucket = 1 AND (id, id2) >= (4, 6) AND (id, id2) <= (6, 2)")
        self.execute(session, "DELETE FROM paging_test WHERE bucket = 1 AND id >= 8")

    def yield_single_partition_selects(self, bypass_cache=False):
        """
        Yield a test case by test case for a single partition. The function yields
        a tuple (description, query string, expected result)
        """

        def format_expected(data):
            return [{"id": a, "id2": b} for a, b in data]

        bypass_cache_str = " BYPASS CACHE" if bypass_cache else ""

        yield ("Test: No restrictions", "SELECT id, id2 FROM paging_test WHERE bucket = 1 ORDER BY id DESC" + bypass_cache_str, format_expected([(7, 1), (6, 3), (4, 5), (4, 2), (3, 3), (2, 9), (2, 5), (1, 4)]))

        # Single column restrictions

        yield ("Test: Single column equality restriction", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND id = 2 ORDER BY id DESC" + bypass_cache_str, format_expected([(2, 9), (2, 5)]))

        yield ("Test: Single column IN restriction", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND id IN (4, 2, 20) ORDER BY id DESC" + bypass_cache_str, format_expected([(4, 5), (4, 2), (2, 9), (2, 5)]))

        yield ("Test: Single column range restriction (less than)", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND id < 3 ORDER BY id DESC" + bypass_cache_str, format_expected([(2, 9), (2, 5), (1, 4)]))

        yield ("Test: Single column range restriction (greater than)", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND id > 4 ORDER BY id DESC" + bypass_cache_str, format_expected([(7, 1), (6, 3)]))

        yield ("Test: Single column range restriction (lt + gt)", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND id > 3 AND id < 7 ORDER BY id DESC" + bypass_cache_str, format_expected([(6, 3), (4, 5), (4, 2)]))

        # Multi column restrictions

        yield ("Test: Multi-column IN restriction", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND (id, id2) IN ((2, 9), (7, 1)) ORDER BY id DESC" + bypass_cache_str, format_expected([(7, 1), (2, 9)]))

        yield ("Test: Multi-column range restriction (less than)", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND (id, id2) < (2, 8) ORDER BY id DESC" + bypass_cache_str, format_expected([(2, 5), (1, 4)]))

        yield ("Test: Multi-column range restriction (greater than)", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND (id, id2) > (3, 3) ORDER BY id DESC" + bypass_cache_str, format_expected([(7, 1), (6, 3), (4, 5), (4, 2)]))

        yield (
            "Test: Multi-column range restriction (lt + gt)",
            "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND (id, id2) < (3, 5) AND (id, id2) > (1, 10) ORDER BY id DESC" + bypass_cache_str,
            format_expected([(3, 3), (2, 9), (2, 5)]),
        )

        # Multiple independent column restrictions

        yield ("Test: Independent IN restrictions for both clustering columns", "SELECT id, id2 FROM paging_test WHERE bucket = 1 AND id IN (3, 6) AND id2 IN (3, 4) ORDER BY id DESC" + bypass_cache_str, format_expected([(6, 3), (3, 3)]))

    def run_single_partition_selects(self, session, bypass_cache=False, callback: Callable[[], None] | None = None):
        for description, query, expected_results in self.yield_single_partition_selects(bypass_cache):
            logger.debug(description)
            data = self.execute(session, query)
            assert data == expected_results

            if callback is not None:
                callback()

    def yield_multi_partition_selects(self, bypass_cache=False):
        """
        Yield a test case by test case for a single partition. The function yields
        a tuple (description, query string, expected result)
        """

        def format_expected(data):
            return [{"bucket": a, "id": b, "id2": c} for a, b, c in data]

        bypass_cache_str = " BYPASS CACHE" if bypass_cache else ""

        yield (
            "Test: Multi-partition select with partial ordering",
            "SELECT bucket, id, id2 FROM paging_test WHERE bucket IN (1,2) AND id > 3 AND id < 7 ORDER BY id DESC" + bypass_cache_str,
            format_expected([(2, 6, 1), (2, 6, 3), (1, 6, 3), (2, 5, 4), (2, 4, 2), (2, 4, 5), (2, 4, 6), (1, 4, 2), (1, 4, 5)]),
        )

        yield (
            "Test: Multi-partition select with full ordering",
            "SELECT bucket, id, id2 FROM paging_test WHERE bucket IN (1,2) AND id > 3 AND id < 7 ORDER BY id DESC, id2 DESC" + bypass_cache_str,
            format_expected([(2, 6, 3), (1, 6, 3), (2, 6, 1), (2, 5, 4), (2, 4, 6), (2, 4, 5), (1, 4, 5), (2, 4, 2), (1, 4, 2)]),
        )

        yield (
            "Test: Multi-partition select with full ordering with per partition limit",
            "SELECT bucket, id, id2 FROM paging_test WHERE bucket IN (1,2) AND id > 3 AND id < 7 ORDER BY id DESC, id2 DESC PER PARTITION LIMIT 3" + bypass_cache_str,
            format_expected([(2, 6, 3), (1, 6, 3), (2, 6, 1), (2, 5, 4), (1, 4, 5), (1, 4, 2)]),
        )

    def run_multi_partition_selects(self, session, bypass_cache=False, callback: Callable[[], None] | None = None):
        # Multi-partition queries with ordering are only possible when paging is turned off
        fetch_size = 0

        for description, query, expected_results in self.yield_multi_partition_selects(bypass_cache):
            logger.debug(description)
            data = self.execute(session, query, fetch_size)
            assert data == expected_results

            if callback is not None:
                callback()

    def run_selects(self, session, bypass_cache=False, callback: Callable[[], None] | None = None):
        self.run_single_partition_selects(session, bypass_cache, callback)
        self.run_multi_partition_selects(session, bypass_cache, callback)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSinglePartitionReversedQueriesSelectors(Tester, BaseReversedQuerySelector):
    def test_reverse_selectors(self):
        logger.debug("Set up a cluster")
        session = self.prepare_cluster()
        logger.debug("Set up schema")
        BaseReversedQuerySelector.prepare_schema(self, session, rf=1)

        logger.debug("Populate the table")
        self.populate(session)

        logger.debug("Testing selects from memtable")
        self.run_single_partition_selects(session)

        logger.debug("Testing selects from cache")
        self.node.flush()
        self.run_single_partition_selects(session)

        logger.debug("Testing selects from sstable")
        self.run_single_partition_selects(session, bypass_cache=True)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestMultiPartitionReversedQueriesSelectors(Tester, BaseReversedQuerySelector):
    def test_reverse_selectors(self):
        logger.debug("Set up a cluster")
        session = self.prepare_cluster()
        logger.debug("Set up schema")
        BaseReversedQuerySelector.prepare_schema(self, session, rf=1)

        logger.debug("Populate the table")
        self.populate(session)

        logger.debug("Testing selects from memtable")
        self.run_multi_partition_selects(session)

        logger.debug("Testing selects from cache")
        self.node.flush()
        self.run_multi_partition_selects(session)

        logger.debug("Testing selects from sstable")
        self.run_multi_partition_selects(session, bypass_cache=True)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestReversedQueriesMemoryUsage(Tester, ConcurrentExecutor):
    def prepare(self, nr_nodes):
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=nr_nodes, nodes_per_rack=1)
        # Restrict shard memory to 0.5GB
        cluster.set_configuration_options(
            values={
                "smp": 1,
                "memory": "512M",
            }
        )
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        self.node = node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, row_factory=dict_factory)
        return session

    def test_memory(self):
        logger.debug("Set up a cluster")
        session = self.prepare(2)

        logger.debug("Set up schema")
        create_ks(session, "test_reversed_queries", 2)
        session.execute("CREATE TABLE memtest (bucket int, id int, value text, PRIMARY KEY (bucket, id))")

        worker_count = 10
        longstring = "x" * 10000
        row_count = 100 * 1000

        # The size of the partition should exceed the amount of memory available for the shard
        logger.debug(f"Writing a huge partition (1GB) using {worker_count} parallel workers")

        def run_writes(stop_event, worker_id):
            stmt = session.prepare(f"INSERT INTO memtest (bucket, id, value) VALUES(?, ?, ?)")
            # Using CL=ALL avoids generating hints and thus avoids generating
            # the OverloadedException ("Too many in-flight hints") in case
            # when the disk is slow.
            stmt.consistency_level = ConsistencyLevel.ALL
            for i in range(worker_id, row_count, worker_count):
                if stop_event.is_set():
                    return
                session.execute(stmt, (0, i, longstring))

        self.run_concurrently(worker_count, run_writes)

        # Select a portion of rows that not fit to 1MB (soft limit) to verify paging
        logger.debug("Selecting a small amount of rows from the end of the partition")
        future = session.execute_async(f"SELECT * FROM memtest WHERE bucket = 0 ORDER BY id DESC LIMIT 200")
        reverse_query_page_fetcher = PageFetcher(future)
        reverse_query_page_fetcher.request_all()
        rows = reverse_query_page_fetcher.all_data()

        assert reverse_query_page_fetcher.pagecount() > 1, "Response was not paged but should"
        assert len(rows) == 200

        # make sure reverse queries use the same size for paging as normal queries. scylladb/scylladb#9815
        future = session.execute_async(f"SELECT * FROM memtest WHERE bucket = 0 ORDER BY id LIMIT 200")
        normal_query_page_fetcher = PageFetcher(future)
        normal_query_page_fetcher.request_all()
        assert reverse_query_page_fetcher.pagecount() == normal_query_page_fetcher.pagecount()


@pytest.mark.dtest_full
class TestReversedQueriesReadRepair(Tester):
    def test_queries_with_read_repair(self):
        ROW_COUNT = 100

        logger.debug("Set up a cluster")
        # Explicitly disable hinted handoff because we want to test read repair
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        [node1, node2] = self.cluster.nodelist()

        logger.debug("Set up schema with read repair")
        session = self.patient_cql_connection(node1, row_factory=dict_factory)
        create_ks(session, "ks", 2)
        query = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair_chance = 100.0"
        session.execute(query)

        logger.debug("Shut down node2")
        node2.stop(wait_other_notice=True)

        logger.debug("Load some data to node1")
        stmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
        stmt.consistency_level = ConsistencyLevel.ONE
        for i in range(ROW_COUNT):
            session.execute(stmt, (0, i, 2 * i))

        logger.debug("Start node2")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug("Perform a reversed query with CL=ALL")
        query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck DESC", consistency_level=ConsistencyLevel.ALL)
        response = session.execute(query)

        expected_rows = [{"ck": i, "v": 2 * i} for i in list(range(ROW_COUNT))]
        expected_rows_reversed = expected_rows[::-1]

        logger.debug("Validate the response")
        assert list(response) == expected_rows_reversed

        logger.debug("Shut down node1")
        node1.stop(wait_other_notice=True)

        logger.debug("Check that all of the data was repaired on node2")
        session = self.patient_cql_connection(node2, row_factory=dict_factory)
        query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 BYPASS CACHE", consistency_level=ConsistencyLevel.ONE)
        response = session.execute(query, trace=True)
        # logger.debug(" ||| ".join(str(e) for e in response.get_query_trace().events))
        assert list(response) == expected_rows


@pytest.mark.dtest_full
class TestReversedQueriesMerging(Tester):
    def test_read_from_memtables_and_multiple_sstables(self):  # noqa: PLR0915
        # In order to test combining reader behavior, memtable/sstable rows
        # will interleave and some will be merged
        RANGE = 1000
        SSTABLE_1_CKS = list(range(0, RANGE, 2))
        SSTABLE_2_CKS = list(range(0, RANGE, 3))
        SSTABLE_3_CKS = list(range(0, RANGE, 5))
        SSTABLE_4_CKS = list(range(1, RANGE, 2))
        SSTABLE_5_CKS = list(range(1, RANGE, 3))
        SSTABLE_6_CKS = list(range(1, RANGE, 5))
        MEMTABLE_CKS = list(range(0, RANGE, 7))

        logger.debug("Set up a cluster")
        # Explicitly disable hinted handoff because we want data to be inconsistent
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        [node1, node2] = self.cluster.nodelist()

        logger.debug("Set up schema with a table with compactions disabled")
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        query = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACTION = {'class': 'NullCompactionStrategy'} AND read_repair_chance = 0.0"
        session.execute(query)

        expected_dict = {}

        logger.debug("Shut down node2")
        node2.stop(wait_other_notice=True)

        def load_cks(session, cks, v_offset):
            stmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
            stmt.consistency_level = ConsistencyLevel.ONE
            for pk in [0, 1]:
                for ck in cks:
                    v = 100 * ck + v_offset
                    session.execute(stmt, (pk, ck, v))
                    expected_dict[ck] = v

        logger.debug("Load data for the first sstable")
        load_cks(session, SSTABLE_1_CKS, 1)
        node1.flush()

        logger.debug("Load data for the second sstable")
        load_cks(session, SSTABLE_2_CKS, 2)
        node1.flush()

        logger.debug("Load data for the third sstable")
        load_cks(session, SSTABLE_3_CKS, 3)
        node1.flush()

        logger.debug("Start node2")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug("Shut down node1")
        node1.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node2, row_factory=dict_factory)

        logger.debug("Load data for the fourth sstable")
        load_cks(session, SSTABLE_4_CKS, 4)
        node2.flush()

        logger.debug("Load data for the fifth sstable")
        load_cks(session, SSTABLE_5_CKS, 5)
        node2.flush()

        logger.debug("Load data for the sixth sstable")
        load_cks(session, SSTABLE_6_CKS, 6)
        node2.flush()

        logger.debug("Start node1")
        node1.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug("Load data for the memtable")
        load_cks(session, MEMTABLE_CKS, 7)
        # No flush, keep in memory
        # Hopefully the amount of rows isn't big enough to trigger a flush

        expected_rows = [{"ck": ck, "v": expected_dict[ck]} for ck in list(range(RANGE))[::-1] if ck in expected_dict]

        def query(pk):
            return f"SELECT ck, v FROM ks.t WHERE pk = {pk} ORDER BY ck DESC"

        logger.debug("Perform a reversed query with cache and check results")
        response = session.execute(SimpleStatement(query(pk=0), consistency_level=ConsistencyLevel.ALL), trace=True)
        # logger.debug(" ||| ".join(str(e) for e in response.get_query_trace().events))
        assert list(response) == expected_rows

        logger.debug("Perform a reversed query without cache and check results")
        response = session.execute(SimpleStatement(query(pk=1) + " BYPASS CACHE", consistency_level=ConsistencyLevel.ALL), trace=True)
        # logger.debug(" ||| ".join(str(e) for e in response.get_query_trace().events))
        assert list(response) == expected_rows


@pytest.mark.dtest_full
class TestReversedQueriesOnTableWithReversedOrder(Tester):
    def test_reversed_query_on_table_with_reversed_order(self):
        ROW_COUNT = 100

        logger.debug("Set up a cluster")
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = self.cluster.nodelist()

        logger.debug("Set up schema with a table with reversed ordering of clustering keys")
        session = self.patient_cql_connection(node1, row_factory=dict_factory)
        create_ks(session, "ks", 1)
        query = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)"
        session.execute(query)

        stmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
        for i in range(ROW_COUNT):
            session.execute(stmt, (0, i, -i))

        response = session.execute("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck ASC")

        expected_rows = [{"ck": i, "v": -i} for i in range(ROW_COUNT)]
        assert list(response) == expected_rows


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestReversedQueriesWithOverlappingRangeTombstones(Tester, ConcurrentExecutor):
    @unmark.next_gating
    def test_reversed_query_with_overlapping_range_tombstones(self):
        TOMBSTONE_COUNT = 100 * 1000

        logger.debug("Set up a cluster")
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = self.cluster.nodelist()

        logger.debug("Set up schema")
        session = self.patient_cql_connection(node1, row_factory=dict_factory)
        create_ks(session, "ks", 1)
        query = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
        session.execute(query)

        logger.debug("Generate range tombstones")
        worker_count = 10

        def run_deletes(stop_event, worker_id):
            stmt = session.prepare("DELETE FROM ks.t WHERE pk = 0 AND ck >= ? AND ck < ?")
            for i in range(worker_id, TOMBSTONE_COUNT, worker_count):
                if stop_event.is_set():
                    return
                session.execute(stmt, (i, i + TOMBSTONE_COUNT))

        self.run_concurrently(10, run_deletes)

        logger.debug("Insert a row near the end of the range tombstones")
        ck = 2 * TOMBSTONE_COUNT - 10
        session.execute(f"INSERT INTO ks.t (pk, ck, v) VALUES (0, {ck}, 42)")

        logger.debug("Select the row and check result")
        response = session.execute("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck DESC LIMIT 1 BYPASS CACHE", trace=True)
        # logger.debug(" ||| ".join(str(e) for e in response.get_query_trace().events))
        try:
            assert list(response) == [{"ck": ck, "v": 42}]
        except AssertionError:
            logger.debug("Dump sstables")
            node1.flush("ks", "t")
            logger.debug(node1.dump_sstables("ks", "t"))

            logger.debug("Dump mutation fragments")
            logger.debug(list(session.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.t)")))
            raise


@pytest.mark.dtest_full
class TestReversedQueriesSelectorsDuringUpgrade(UpgradeTester, BaseReversedQuerySelector):
    __test__ = True
    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    def test_queries_during_upgrade(self, dtest_config):
        """
        Test that reverse queries work on a mixed cluster
        1. Prepare data for selects
        2. For each version in the upgrade chain:
            2.1. For each node in the cluster
                2.1.1. Upgrade the node
                2.1.2. Check that selects work
        """
        self.clone_upgrade_path(dtest_config)

        logger.debug("Creating a 3-node cluster")
        cluster_topology = generate_cluster_topology(rack_num=3)
        self.init_cluster(cluster_topology)
        # Execute queries on the middle node. This way it will cover all cases:
        #   - query executed on an old node in mixed-node cluster
        #   - query executed on a new node in mixed-node cluster
        #   - query executed on a new node in fully upgraded cluster
        session = self.patient_cql_connection(self.cluster.nodelist()[1], row_factory=dict_factory)
        raft_topology_enabled = self.is_consistent_topology_changes_enabled(session)

        logger.debug("Set up schema")
        BaseReversedQuerySelector.prepare_schema(self, session, rf=3)

        logger.debug("Populate the table")
        self.populate(session)

        logger.debug("Testing selects")
        self.run_selects(session)

        for version in self.current_upgrade_path:
            for node in self.cluster.nodelist():
                logger.debug(f"Upgrading node {node.name} from version {node.node_scylla_version} to {version}: scylla_features={self.scylla_features}")
                config_options = node.get_configuration_options()
                experimental_features_set = set(config_options.get("experimental_features", []))
                if has_views_with_tablets_experimental_feature(scylla_version=version):
                    experimental_features_set.add("views-with-tablets")
                if "tablets" in self.scylla_features:
                    experimental_features_set.add("tablets")
                node.set_configuration_options({"experimental_features": list(experimental_features_set)})
                node.upgrade(upgrade_to_version=version)

                logger.debug("Testing selects")
                self.run_selects(session)

        if "consistent-topology-changes" in self.scylla_features:
            if not raft_topology_enabled:
                supports_post_raft_procedures = ComparableScyllaVersion(self.cluster.nodelist()[0].node_scylla_version) >= ComparableScyllaVersion("2026.1-dev")
                if supports_post_raft_procedures:
                    self.wait_upgrade_schema_on_raft_finished()
                self.enable_raft_topology()
                if supports_post_raft_procedures:
                    self.wait_for_sl_v2()
            self.run_selects(session)


@pytest.mark.dtest_full
class TestReversedQueriesReadRepairDuringUpgrade(UpgradeTester, BaseReversedQuerySelector):
    __test__ = True
    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    def test_queries_during_upgrade(self, dtest_config):
        """
        Test that reconciliation with reverse queries work on a mixed cluster
        1. Prepare data for selects
        2. For each version in the upgrade chain:
            2.1. For each node in the cluster
                2.1.1. Delete all sstable files
                2.1.2. Upgrade the node
                2.1.3. Check that selects work
        """
        self.clone_upgrade_path(dtest_config)

        logger.debug("Creating a 3-node cluster")
        self.cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster_topology = generate_cluster_topology(rack_num=3)
        self.init_cluster(cluster_topology)

        # Execute queries on the middle node. This way it will cover all cases:
        #   - query executed on an old node in mixed-node cluster
        #   - query executed on a new node in mixed-node cluster
        #   - query executed on a new node in fully upgraded cluster
        session = self.patient_cql_connection(self.cluster.nodelist()[1], row_factory=dict_factory)

        logger.debug("Set up schema")
        BaseReversedQuerySelector.prepare_schema(self, session, rf=3)

        logger.debug("Populate the table")
        self.populate(session)

        for version in self.current_upgrade_path:
            for node in self.cluster.nodelist():

                def clear_node_data():
                    logger.debug(f"Deleting data for node {node.name}")
                    node.flush()
                    node.stop(wait_other_notice=True)
                    node.clear(only_data=True)
                    node.start(wait_for_binary_proto=True, wait_other_notice=True)

                logger.debug(f"Upgrading node {node.name} from version {node.node_scylla_version} to {version}: scylla_features={self.scylla_features}")
                config_options = node.get_configuration_options()
                experimental_features_set = set(config_options.get("experimental_features", []))
                if has_views_with_tablets_experimental_feature(scylla_version=version):
                    experimental_features_set.add("views-with-tablets")
                if "tablets" in self.scylla_features:
                    experimental_features_set.add("tablets")
                node.set_configuration_options({"experimental_features": list(experimental_features_set)})
                node.upgrade(upgrade_to_version=version)

                logger.debug("Testing selects")
                clear_node_data()
                self.run_selects(session, bypass_cache=True, callback=clear_node_data)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestReversedQueriesWithTimeWindowCompactionStrategy(Tester):
    def test_reversed_queries_with_time_window_compaction_strategy(self):
        ROW_COUNT = 300  # = 5 1-minute windows

        logger.debug("Set up a cluster")
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = self.cluster.nodelist()

        logger.debug("Set up schema")
        session = self.patient_cql_connection(node, row_factory=dict_factory)
        create_ks(session, "ks", 1)
        query = (
            "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC) AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'compaction_window_size': 1}"
        )
        session.execute(query)

        # Spread data across 5 windows by simulating a write process across 5 minutes duration time.
        # `USING TIMESTAMP` is provided to distribute the writes evenly simulating a write every
        # second.
        #
        node.nodetool("disableautocompaction")
        stmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?")
        current_time = int(time.time())
        for i in range(1, ROW_COUNT + 1):
            timestamp = current_time - ROW_COUNT + i
            session.execute(stmt, (0, i, -i, timestamp))

            if i % 60 == 0:
                node.flush()

        logger.debug("Check sstables created")
        assert len(node.get_sstables("ks", "t")) == 5

        response = session.execute("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck ASC")
        expected_rows = [{"ck": i, "v": -i} for i in range(1, ROW_COUNT + 1)]
        assert list(response) == expected_rows


@pytest.mark.dtest_full
class TestReversedQueriesReadRepairBigMutationsSmallChanges(Tester):
    def test_queries_with_read_repair(self):
        ROW_COUNT = 10_000

        logger.debug("Set up a cluster")
        # Explicitly disable hinted handoff because we want to test read repair
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        [node1, node2] = self.cluster.nodelist()

        logger.debug("Set up schema with read repair")
        session = self.patient_cql_connection(node1, row_factory=dict_factory)
        create_ks(session, "ks", 2)
        query = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = {'class': 'NullCompactionStrategy'} AND read_repair_chance = 100.0"
        session.execute(query)

        logger.debug("Load some data")
        delete_stmt = session.prepare("DELETE FROM ks.t WHERE pk = ? AND ck = ?")
        delete_stmt.consistency_level = ConsistencyLevel.ALL
        insert_stmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL

        expected_rows = []
        for i in range(1, ROW_COUNT + 1):
            session.execute(insert_stmt, (0, i, 2 * i))
            expected_rows.append({"ck": i, "v": 2 * i})

            if i % 1000 == 0:
                ck = random.randint(i - 999, i)
                session.execute(delete_stmt, (0, ck))
                node2.flush()
                expected_rows.remove({"ck": ck, "v": 2 * ck})

        logger.debug("Shut down node2")
        node2.stop(wait_other_notice=True)

        # Remove files associated with one sstable. There is node.clear() that deletes all files,
        # and node.get_sstables() and node.get_sstablepath() but both return path to *-big-Data.db
        # files only. To correctly restart the node, we shall delete all files associated with one
        # sstable.
        root = os.path.join(node2.get_path(), "data", "ks")
        data_files = sorted([os.path.join(root, name) for root, _, files in os.walk(root) for name in files])
        # For one sstable, the are files *-CompressionInfo.db, *-Data.db, *-Digest.crc32, *-Filter.db, *-Index.db,
        # *-Scylla.db, *-Statistics.db, *-Summary.db, *-TOC.txt. Delete all of them.
        for file in data_files[0:9]:
            os.remove(file)

        logger.debug("Start node2")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug("Perform a reversed query with CL=ALL")
        query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck DESC", consistency_level=ConsistencyLevel.ALL)
        response = session.execute(query, trace=True)
        expected_rows_reversed = expected_rows[::-1]

        logger.debug("Validate the response")
        assert list(response) == expected_rows_reversed

        logger.debug("Shut down node1")
        node1.stop(wait_other_notice=True)

        logger.debug("Check that all of the data was repaired on node2")
        session = self.patient_cql_connection(node2, row_factory=dict_factory)
        query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 BYPASS CACHE", consistency_level=ConsistencyLevel.ONE)
        response = session.execute(query, trace=True)
        assert list(response) == expected_rows
