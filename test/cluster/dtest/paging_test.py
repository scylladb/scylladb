import ctypes
import logging
import random
import re
import time
import uuid
from collections import Counter

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, ReadFailure, ReadTimeout
from cassandra.query import (
    FETCH_SIZE_UNSET,
    SimpleStatement,
    dict_factory,
    named_tuple_factory,
    tuple_factory,
)
from packaging.version import Version

from dtest_class import Tester, create_ks
from tools.assertions import assert_invalid
from tools.cluster_topology import generate_cluster_topology
from tools.data import rows_to_list
from tools.datahelp import create_rows, flatten_into_set, parse_data_into_dicts
from tools.paging import PageAssertionMixin, PageFetcher, run_scenarios

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class BasePagingTester(Tester):
    def prepare(self, row_factory=dict_factory, consistency_level=ConsistencyLevel.QUORUM):
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster = self.cluster
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, row_factory=row_factory, consistency_level=consistency_level)
        return session


@pytest.mark.dtest_full
class TestPagingSize(BasePagingTester, PageAssertionMixin):
    """
    Basic tests relating to page size (relative to results set)
    and validation of page size setting.
    """

    def test_with_no_results(self):
        """
        No errors when a page is requested and query has no results.
        """
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        # run a query that has no results and make sure it's exhausted
        future = session.execute_async(SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future)
        pf.request_all()

        assert [] == pf.all_data()
        assert not pf.has_more_pages

    def test_with_less_results_than_page_size(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": str})
        future = session.execute_async(SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future)
        pf.request_all()

        assert not pf.has_more_pages
        assert len(expected_data) == len(pf.all_data())

    def test_with_more_results_than_page_size(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |6 |testing         |
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": str})
        future = session.execute_async(SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [5, 4]

        # Make sure expected and actual have same data elements (ignoring order.)
        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_with_equal_results_to_page_size(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": str})
        future = session.execute_async(SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future).request_all()

        assert pf.num_results_all() == [5]
        assert pf.pagecount() == 1

        # make sure expected and actual have same data elements (ignoring order)
        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_undefined_page_size_default(self):
        """
        If the page size isn't sent then the default fetch size is used.
        """
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(_):
            return uuid.uuid4()

        data = """
               | id     |value   |
               +--------+--------+
          *5001| [uuid] |testing |
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": random_txt, "value": str})
        future = session.execute_async(SimpleStatement("select * from paging_test", consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future).request_all()

        assert pf.num_results_all() == [5000, 1]

        # make sure expected and actual have same data elements (ignoring order)
        self.assert_equal_ignore_order(pf.all_data(), expected_data)


@pytest.mark.dtest_full
class TestPagingWithModifiers(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when CQL modifiers (such as order, limit, allow filtering) are used.
    """

    def test_with_order_by(self):
        """ "
        Paging over a single partition with ordering should work.
        (Spanning multiple partitions won't though, by design. See CASSANDRA-6722).
        """
        session = self.prepare()
        create_ks(session, "test_paging", 3)
        session.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value ASC)
            """
        )

        data = """
            |id|value|
            +--+-----+
            |1 |a    |
            |1 |b    |
            |1 |c    |
            |1 |d    |
            |1 |e    |
            |1 |f    |
            |1 |g    |
            |1 |h    |
            |1 |i    |
            |1 |j    |
            """

        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": str})

        future = session.execute_async(SimpleStatement("select * from paging_test where id = 1 order by value asc", fetch_size=5, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [5, 5]

        # these should be equal (in the same order)
        assert pf.all_data() == expected_data

        # make sure we don't allow paging over multiple partitions with order because that's weird
        with pytest.raises(InvalidRequest, match="Cannot page queries with both ORDER BY and a IN restriction on the partition key"):
            stmt = SimpleStatement("select * from paging_test where id in (1,2) order by value asc", consistency_level=ConsistencyLevel.ALL)
            session.execute(stmt)

    def test_with_order_by_reversed(self):
        """ "
        Paging over a single partition with ordering and a reversed clustering order.
        """
        session = self.prepare()
        create_ks(session, "test_paging", 3)
        session.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                value2 text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value DESC)
            """
        )

        data = """
            |id|value|value2|
            +--+-----+------+
            |1 |a    |a     |
            |1 |b    |b     |
            |1 |c    |c     |
            |1 |d    |d     |
            |1 |e    |e     |
            |1 |f    |f     |
            |1 |g    |g     |
            |1 |h    |h     |
            |1 |i    |i     |
            |1 |j    |j     |
            """

        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": str, "value2": str})

        future = session.execute_async(SimpleStatement("select * from paging_test where id = 1 order by value asc", fetch_size=3, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all() == [3, 3, 3, 1]

        # these should be equal (in the same order)
        assert pf.all_data() == expected_data

        # drop the ORDER BY
        future = session.execute_async(SimpleStatement("select * from paging_test where id = 1", fetch_size=3, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all() == [3, 3, 3, 1]

        # these should be equal (in the same order)
        assert pf.all_data() == list(reversed(expected_data))

    def test_with_limit(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
               | id | value         |
               +----+---------------+
             *5| 1  | [random text] |
             *5| 2  | [random text] |
            *10| 3  | [random text] |
            *10| 4  | [random text] |
            *20| 5  | [random text] |
            *30| 6  | [random text] |
            """

        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": random_txt})

        def with_limit_scenario_handler(limit, fetch_size, whereclause, expect_pgcount, expect_pgsizes):
            future = session.execute_async(SimpleStatement(f"select * from paging_test {whereclause}" + ("" if limit is None else f" limit {limit}"), fetch_size=fetch_size, consistency_level=ConsistencyLevel.ALL))
            pf = PageFetcher(future).request_all()

            assert pf.num_results_all() == expect_pgsizes
            assert pf.pagecount() == expect_pgcount

            # Make sure all the data retrieved is a subset of input data.
            self.assert_is_subset_of(pf.all_data(), expected_data)

        run_scenarios(
            handler=with_limit_scenario_handler,
            scenarios=[
                # using equals clause w/single partition
                (10, 20, "WHERE id = 6", 1, [10]),  # limit < fetch < data
                (10, 30, "WHERE id = 5", 1, [10]),  # limit < data < fetch
                (20, 10, "WHERE id = 6", 2, [10, 10]),  # fetch < limit < data
                (30, 10, "WHERE id = 5", 2, [10, 10]),  # fetch < data < limit
                (20, 30, "WHERE id = 3", 1, [10]),  # data < limit < fetch
                (30, 20, "WHERE id = 3", 1, [10]),  # data < fetch < limit
                # using 'in' clause w/multi partitions
                (9, 20, "WHERE id in (1,2,3,4,5,6)", 1, [9]),  # limit < fetch < data
                (10, 30, "WHERE id in (3,4)", 1, [10]),  # limit < data < fetch
                (20, 10, "WHERE id in (4,5)", 2, [10, 10]),  # fetch < limit < data
                (30, 10, "WHERE id in (3,4)", 2, [10, 10]),  # fetch < data < limit
                (20, 30, "WHERE id in (1,2)", 1, [10]),  # data < limit < fetch
                (30, 20, "WHERE id in (1,2)", 1, [10]),  # data < fetch < limit
                # no limit but with a defined pagesize.  Scenarios added for CASSANDRA-8408.
                (None, 20, "WHERE id in (1,2,3,4,5,6)", 4, [20, 20, 20, 20]),  # fetch < data
                (None, 30, "WHERE id in (3,4)", 1, [20]),  # data < fetch
                (None, 10, "WHERE id in (4,5)", 3, [10, 10, 10]),  # fetch < data
                (None, 30, "WHERE id in (1,2)", 1, [10]),  # data < fetch
                # not setting fetch_size (unpaged) but using limit. Scenarios added for CASSANDRA-8408.
                (9, FETCH_SIZE_UNSET, "WHERE id in (1,2,3,4,5,6)", 1, [9]),  # limit < data
                (30, FETCH_SIZE_UNSET, "WHERE id in (1,2)", 1, [10]),  # data < limit
            ],
        )

    def test_with_allow_filtering(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        data = """
                |id|value           |
                +--+----------------+
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                |6 |testing         |
                |7 |and more testing|
                |8 |and more testing|
                |9 |and more testing|
                """
        create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": str})

        future = session.execute_async(SimpleStatement("select * from paging_test where value = 'and more testing' ALLOW FILTERING", fetch_size=4, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [4, 3]

        # make sure the allow filtering query matches the expected results (ignoring order)
        self.assert_equal_ignore_order(
            pf.all_data(),
            parse_data_into_dicts(
                """
                |id|value           |
                +--+----------------+
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                |7 |and more testing|
                |8 |and more testing|
                |9 |and more testing|
                """,
                format_funcs={"id": int, "value": str},
            ),
        )


@pytest.mark.dtest_full
class TestPagingData(BasePagingTester, PageAssertionMixin):
    def test_paging_a_single_wide_row(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
              | id | value                  |
              +----+------------------------+
        *10000| 1  | [replaced with random] |
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": random_txt})

        future = session.execute_async(SimpleStatement("select * from paging_test where id = 1", fetch_size=3000, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all() == [3000, 3000, 3000, 1000]

        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_paging_across_multi_wide_rows(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
              | id | value                  |
              +----+------------------------+
         *5000| 1  | [replaced with random] |
         *5000| 2  | [replaced with random] |
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "value": random_txt})

        future = session.execute_async(SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=3000, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all() == [3000, 3000, 3000, 1000]

        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_paging_using_secondary_indexes(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test (id int, mybool boolean, sometext text, PRIMARY KEY (id, sometext))")
        session.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(_):
            return str(uuid.uuid4())

        def bool_from_str_int(text):
            return bool(int(text))

        data = """
             | id | mybool| sometext |
             +----+-------+----------+
         *100| 1  | 1     | [random] |
         *300| 2  | 0     | [random] |
         *500| 3  | 1     | [random] |
         *400| 4  | 0     | [random] |
            """
        all_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "mybool": bool_from_str_int, "sometext": random_txt})

        future = session.execute_async(SimpleStatement("select * from paging_test where mybool = true", fetch_size=400, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        # the query only searched for True rows, so let's pare down the expectations for comparison
        expected_data = filter(lambda x: x.get("mybool") is True, all_data)

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [400, 200]
        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_paging_with_in_orderby_and_two_partition_keys(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test (col_1 int, col_2 int, col_3 int, PRIMARY KEY ((col_1, col_2), col_3))")

        assert_invalid(session, "select * from paging_test where col_1=1 and col_2 IN (1, 2) order by col_3 desc;", expected=InvalidRequest)
        assert_invalid(session, "select * from paging_test where col_2 IN (1, 2) and col_1=1 order by col_3 desc;", expected=InvalidRequest)

    def test_group_by_paging(self):  # noqa: PLR0915
        """
        @jira_ticket CASSANDRA-10707
        """

        session = self.prepare()
        create_ks(session, "test_paging_with_group_by", 3)
        session.execute("CREATE TABLE test (a int, b int, c int, d int, e int, primary key (a, b, c, d))")

        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 3, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (5, 8, 2, 12)")

        # Makes sure that we have some tombstones
        session.execute("DELETE FROM test WHERE a = 1 AND b = 3 AND c = 2")
        session.execute("DELETE FROM test WHERE a = 5")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a")[:]
            assert res == [{"a": 1, "system.count(b)": 4, "b": 2, "e": 6, "system.max(e)": 24}, {"a": 2, "system.count(b)": 2, "b": 2, "e": 6, "system.max(e)": 12}, {"a": 4, "system.count(b)": 1, "b": 8, "e": 24, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 2, "b": 4, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
                {"a": 4, "b": 8, "e": 24, "system.count(b)": 1, "system.max(e)": 24},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 7, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE b = 2 GROUP BY a, b ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12}, {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE b = 2 ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 3, "system.max(e)": 12}]

            # Range queries without aggregates
            res = session.execute("SELECT a, b, c, d FROM test GROUP BY a, b, c")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 2, "c": 2, "d": 6}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}, {"a": 2, "b": 4, "c": 3, "d": 6}, {"a": 4, "b": 8, "c": 2, "d": 12}]

            res = session.execute("SELECT a, b, c, d FROM test GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}, {"a": 2, "b": 4, "c": 3, "d": 6}, {"a": 4, "b": 8, "c": 2, "d": 12}]

            # Range query with LIMIT
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test LIMIT 2")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 7, "system.max(e)": 24}]

            # Range queries without aggregates and with LIMIT
            res = session.execute("SELECT a, b, c, d FROM test GROUP BY a, b, c LIMIT 3")[:]
            assert len(res) == 3
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 2, "c": 2, "d": 6}, {"a": 1, "b": 4, "c": 2, "d": 6}]

            res = session.execute("SELECT a, b, c, d FROM test GROUP BY a, b LIMIT 3")[:]
            assert len(res) == 3
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}]

            # Range query with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 2, "b": 4, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
                {"a": 4, "b": 8, "e": 24, "system.count(b)": 1, "system.max(e)": 24},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 1")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 4, "b": 8, "e": 24, "system.count(b)": 1, "system.max(e)": 24},
            ]

            # Range queries with PER PARTITION LIMIT and LIMIT
            # FIXME: EXPECTED RESULT MUST BE UPDATED --> https://github.com/scylladb/scylla/issues/5363
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 3")[:]
            assert len(res) == 3
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 2, "b": 4, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
                {"a": 4, "b": 8, "e": 24, "system.count(b)": 1, "system.max(e)": 24},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 10")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 2, "b": 4, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
                {"a": 4, "b": 8, "e": 24, "system.count(b)": 1, "system.max(e)": 24},
            ]

            # Range queries without aggregates and with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, c, d FROM test GROUP BY a, b, c PER PARTITION LIMIT 2")[:]
            assert res == [
                {"a": 1, "b": 2, "c": 1, "d": 3},
                {"a": 1, "b": 2, "c": 2, "d": 6},
                {"a": 2, "b": 2, "c": 3, "d": 3},
                {"a": 2, "b": 4, "c": 3, "d": 6},
                {"a": 4, "b": 8, "c": 2, "d": 12},
            ]

            res = session.execute("SELECT a, b, c, d FROM test GROUP BY a, b PER PARTITION LIMIT 1")[:]
            assert res == [
                {"a": 1, "b": 2, "c": 1, "d": 3},
                {"a": 2, "b": 2, "c": 3, "d": 3},
                {"a": 4, "b": 8, "c": 2, "d": 12},
            ]

            # Range query with DISTINCT
            res = session.execute("SELECT DISTINCT a, count(a)FROM test GROUP BY a")[:]
            assert res == [{"a": 1, "system.count(a)": 1}, {"a": 2, "system.count(a)": 1}, {"a": 4, "system.count(a)": 1}]

            res = session.execute("SELECT DISTINCT a, count(a)FROM test")[:]
            assert res == [{"a": 1, "system.count(a)": 3}]

            # Range query with DISTINCT and LIMIT
            res = session.execute("SELECT DISTINCT a, count(a)FROM test GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [{"a": 1, "system.count(a)": 1}, {"a": 2, "system.count(a)": 1}]

            res = session.execute("SELECT DISTINCT a, count(a)FROM test LIMIT 2")[:]
            assert res == [{"a": 1, "system.count(a)": 3}]

            # Single partition queries
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}, {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 4, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 AND b = 2 GROUP BY a, b, c")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 AND b = 2")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 2, "system.max(e)": 12}]

            # Single partition queries without aggregates
            res = session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b, c")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 2, "c": 2, "d": 6}, {"a": 1, "b": 4, "c": 2, "d": 6}]

            # Single partition query with DISTINCT
            res = session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a = 1 GROUP BY a")[:]
            assert res == [{"a": 1, "system.count(a)": 1}]

            # Single partition queries with LIMIT
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 10")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}, {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 2")[:]
            # FIXME: EXPECTED RESULT MUST BE UPDATED --> https://github.com/scylladb/scylla/issues/
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 LIMIT 2")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 4, "system.max(e)": 24}]

            res = session.execute("SELECT count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 1")[:]
            assert len(res) == 1
            assert res == [{"system.count(b)": 1, "system.max(e)": 6}]

            # Single partition queries with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 3")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}, {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 3")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}, {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24}]

            # Single partition queries without aggregates and with LIMIT
            res = session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b LIMIT 2")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b LIMIT 1")[:]
            assert len(res) == 1
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 2")[:]
            assert len(res) == 2
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 2, "c": 2, "d": 6}]

            # Single partition queries with ORDER BY
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC")[:]
            assert res == [{"a": 1, "b": 4, "e": 24, "system.count(b)": 2, "system.max(e)": 24}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}, {"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 ORDER BY b DESC, c DESC")[:]
            assert res == [{"a": 1, "b": 4, "e": 24, "system.count(b)": 4, "system.max(e)": 24}]

            # Single partition queries with ORDER BY and LIMIT
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2")[:]
            assert len(res) == 2
            assert res == [{"a": 1, "b": 4, "e": 24, "system.count(b)": 2, "system.max(e)": 24}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 ORDER BY b DESC, c DESC LIMIT 2")[:]
            assert res == [{"a": 1, "b": 4, "e": 24, "system.count(b)": 4, "system.max(e)": 24}]

            # Multi-partitions queries
            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4) GROUP BY a, b, c")[:]
            assert res == [
                {"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
                {"a": 1, "b": 4, "e": 12, "system.count(b)": 2, "system.max(e)": 24},
                {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6},
                {"a": 2, "b": 4, "e": 12, "system.count(b)": 1, "system.max(e)": 12},
                {"a": 4, "b": 8, "e": 24, "system.count(b)": 1, "system.max(e)": 24},
            ]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4)")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 7, "system.max(e)": 24}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}, {"a": 1, "b": 2, "e": 12, "system.count(b)": 1, "system.max(e)": 12}, {"a": 2, "b": 2, "e": 6, "system.count(b)": 1, "system.max(e)": 6}]

            res = session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4) AND b = 2")[:]
            assert res == [{"a": 1, "b": 2, "e": 6, "system.count(b)": 3, "system.max(e)": 12}]

            # Multi-partitions queries without aggregates
            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}, {"a": 2, "b": 4, "c": 3, "d": 6}, {"a": 4, "b": 8, "c": 2, "d": 12}]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b, c")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 2, "c": 2, "d": 6}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}, {"a": 2, "b": 4, "c": 3, "d": 6}, {"a": 4, "b": 8, "c": 2, "d": 12}]

            # Multi-partitions queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4) GROUP BY a")[:]
            assert res == [{"a": 1, "system.count(a)": 1}, {"a": 2, "system.count(a)": 1}, {"a": 4, "system.count(a)": 1}]

            res = session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4)")[:]
            assert res == [{"a": 1, "system.count(a)": 3}]

            # Multi-partitions query with DISTINCT and LIMIT
            res = session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [{"a": 1, "system.count(a)": 1}, {"a": 2, "system.count(a)": 1}]

            res = session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4) LIMIT 2")[:]
            assert res == [{"a": 1, "system.count(a)": 3}]

            # Multi-partitions queries without aggregates and with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1")[:]
            assert res == [
                {"a": 1, "b": 2, "c": 1, "d": 3},
                {"a": 2, "b": 2, "c": 3, "d": 3},
                {"a": 4, "b": 8, "c": 2, "d": 12},
            ]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 2")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}, {"a": 2, "b": 4, "c": 3, "d": 6}, {"a": 4, "b": 8, "c": 2, "d": 12}]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 3")[:]
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}, {"a": 2, "b": 2, "c": 3, "d": 3}, {"a": 2, "b": 4, "c": 3, "d": 6}, {"a": 4, "b": 8, "c": 2, "d": 12}]

            # Multi-partitions queries without aggregates, with PER PARTITION LIMIT and with LIMIT
            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2")[:]
            assert len(res) == 2
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 2, "b": 2, "c": 3, "d": 3}]

            res = session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 3 LIMIT 2")[:]
            assert len(res) == 2
            assert res == [{"a": 1, "b": 2, "c": 1, "d": 3}, {"a": 1, "b": 4, "c": 2, "d": 6}]

    def test_group_by_with_range_name_query_paging(self):
        """
        @jira_ticket CASSANDRA-10707
        """

        session = self.prepare()
        create_ks(session, "group_by_with_range_name_query_paging_test", 3)
        session.execute("CREATE TABLE test (a int, b int, c int, d int, primary key (a, b, c))")

        for i in range(1, 5):
            for j in range(1, 5):
                for k in range(1, 5):
                    session.execute(f"INSERT INTO test (a, b, c, d) VALUES ({i}, {j}, {k}, {i + j})")

        # Makes sure that we have some tombstones
        session.execute("DELETE FROM test WHERE a = 3")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2}, {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3}, {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5}]

            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2}, {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3}, {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5}]

            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING")[:]
            assert res == [
                {"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2},
                {"a": 1, "b": 2, "d": 3, "system.count(b)": 2, "system.max(d)": 3},
                {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3},
                {"a": 2, "b": 2, "d": 4, "system.count(b)": 2, "system.max(d)": 4},
                {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5},
                {"a": 4, "b": 2, "d": 6, "system.count(b)": 2, "system.max(d)": 6},
            ]

            # Range queries with LIMIT
            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2}, {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3}, {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5}]

            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2}, {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3}, {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5}]

            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING")[:]
            assert len(res) == 3
            assert res == [
                {"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2},
                {"a": 1, "b": 2, "d": 3, "system.count(b)": 2, "system.max(d)": 3},
                {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3},
            ]

            # Range queries with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2}, {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3}, {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5}]

            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING")[:]
            assert res == [
                {"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2},
                {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3},
                {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5},
            ]

            # Range queries with PER PARTITION LIMIT and LIMIT
            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2}, {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3}, {"a": 4, "b": 1, "d": 5, "system.count(b)": 2, "system.max(d)": 5}]

            res = session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 1, "d": 2, "system.count(b)": 2, "system.max(d)": 2},
                {"a": 2, "b": 1, "d": 3, "system.count(b)": 2, "system.max(d)": 3},
            ]

    def test_group_by_with_static_columns_paging(self):  # noqa: PLR0915
        """
        @jira_ticket CASSANDRA-10707
        """
        session = self.prepare()
        create_ks(session, "test_paging_with_group_by_and_static_columns", 3)
        session.execute("CREATE TABLE test (a int, b int, c int, s int static, d int, primary key (a, b, c))")

        # ------------------------------------
        # Test with non static columns empty
        # ------------------------------------

        session.execute("UPDATE test SET s = 1 WHERE a = 1")
        session.execute("UPDATE test SET s = 2 WHERE a = 2")
        session.execute("UPDATE test SET s = 3 WHERE a = 4")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a")[:]
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 999 GROUP BY a, b ALLOW FILTERING")[:]
            assert res == []

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b")[:]
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 3}]

            # Range query without aggregates
            res = session.execute("SELECT a, b, s FROM test GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": None, "s": 1}, {"a": 2, "b": None, "s": 2}, {"a": 4, "b": None, "s": 3}]

            # Range queries with LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b LIMIT 2")[:]
            assert len(res) == 2

            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test LIMIT 2")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 3}]

            # Range query with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 2")[:]
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            # Range queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test GROUP BY a")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 1}, {"a": 2, "s": 2, "system.count(s)": 1}, {"a": 4, "s": 3, "system.count(s)": 1}]

            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test ")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 3}]

            # Range queries with DISTINCT and LIMIT
            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "s": 1, "system.count(s)": 1},
                {"a": 2, "s": 2, "system.count(s)": 1},
            ]

            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test LIMIT 2")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 3}]

            # Single partition queries
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1}]

            # Single partition query without aggregates
            res = session.execute("SELECT a, b, s FROM test WHERE a = 1 GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": None, "s": 1}]

            # Single partition queries with LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a, b LIMIT 2")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 LIMIT 2")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1}]

            # Single partition queries with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1}]

            # Single partition queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a = 1 GROUP BY a")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 1}]

            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a = 1")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 1}]

            # Multi-partitions queries
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a")[:]
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b")[:]
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4)")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 3}]

            # Multi-partitions query without aggregates
            res = session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": None, "s": 1}, {"a": 2, "b": None, "s": 2}, {"a": 4, "b": None, "s": 3}]

            # Multi-partitions query with LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2")[:]
            assert res == [{"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 3}]

            # Multi-partitions query with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1")[:]
            assert res == [
                {"a": 1, "b": None, "s": 1, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 2, "b": None, "s": 2, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            # Multi-partitions queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 1}, {"a": 2, "s": 2, "system.count(s)": 1}, {"a": 4, "s": 3, "system.count(s)": 1}]

            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4)")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 3}]

            # Multi-partitions queries with DISTINCT and LIMIT
            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "s": 1, "system.count(s)": 1},
                {"a": 2, "s": 2, "system.count(s)": 1},
            ]

            res = session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2")[:]
            assert res == [{"a": 1, "s": 1, "system.count(s)": 3}]

        # ------------------------------------
        # Test with non static columns not empty
        # ------------------------------------
        session.execute("UPDATE test SET s = 3 WHERE a = 3")
        session.execute("DELETE s FROM test WHERE a = 4")

        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 1, 3)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 2, 6)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 3, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (2, 2, 3, 3)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (2, 4, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (4, 8, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (5, 8, 2, 12)")

        # Makes sure that we have some tombstones
        session.execute("DELETE FROM test WHERE a = 1 AND b = 3 AND c = 2")
        session.execute("DELETE FROM test WHERE a = 5")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 1, "b": 4, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 7, "system.count(s)": 7}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE b = 2 GROUP BY a, b ALLOW FILTERING")
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2}, {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE b = 2 ALLOW FILTERING")[:]
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 3, "system.count(s)": 3}]

            # Range queries without aggregates
            res = session.execute("SELECT a, b, s FROM test GROUP BY a")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1}, {"a": 2, "b": 2, "s": 2}, {"a": 4, "b": 8, "s": None}, {"a": 3, "b": None, "s": 3}]

            res = session.execute("SELECT a, b, s FROM test GROUP BY a, b")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1}, {"a": 1, "b": 4, "s": 1}, {"a": 2, "b": 2, "s": 2}, {"a": 2, "b": 4, "s": 2}, {"a": 4, "b": 8, "s": None}, {"a": 3, "b": None, "s": 3}]

            # Range query with LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test LIMIT 2")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 7, "system.count(s)": 7}]

            # Range queries without aggregates and with LIMIT
            res = session.execute("SELECT a, b, s FROM test GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "s": 1},
                {"a": 2, "b": 2, "s": 2},
            ]

            res = session.execute("SELECT a, b, s FROM test GROUP BY a, b LIMIT 10")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1}, {"a": 1, "b": 4, "s": 1}, {"a": 2, "b": 2, "s": 2}, {"a": 2, "b": 4, "s": 2}, {"a": 4, "b": 8, "s": None}, {"a": 3, "b": None, "s": 3}]

            # Range queries with PER PARTITION LIMITS
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 2")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 1, "b": 4, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            # Range queries with PER PARTITION LIMITS and LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 5")[:]
            assert len(res) == 4
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 4")[:]
            assert len(res) == 4
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
            ]

            # Range queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test GROUP BY a")[:]
            assert res == [
                {"a": 1, "s": 1, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 2, "s": 2, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 4, "s": None, "system.count(a)": 1, "system.count(s)": 0},
                {"a": 3, "s": 3, "system.count(a)": 1, "system.count(s)": 1},
            ]

            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test")[:]
            assert res == [{"a": 1, "s": 1, "system.count(a)": 4, "system.count(s)": 3}]

            # Range queries with DISTINCT and LIMIT
            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "s": 1, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 2, "s": 2, "system.count(a)": 1, "system.count(s)": 1},
            ]

            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test LIMIT 2")[:]
            assert res == [{"a": 1, "s": 1, "system.count(a)": 4, "system.count(s)": 3}]

            # Single partition queries
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a")[:]
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 3 GROUP BY a, b")[:]
            assert res == [{"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 3")[:]
            assert res == [{"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 AND b = 2 GROUP BY a, b")[:]
            assert res == [{"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 AND b = 2")[:]
            assert res == [{"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1}]

            # Single partition queries without aggregates
            res = session.execute("SELECT a, b, s FROM test WHERE a = 1 GROUP BY a")[:]
            assert res == [{"a": 1, "b": 2, "s": 1}]

            res = session.execute("SELECT a, b, s FROM test WHERE a = 4 GROUP BY a, b")[:]
            assert res == [{"a": 4, "b": 8, "s": None}]

            # Single partition queries with LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b LIMIT 1")[:]
            assert len(res) == 1
            assert res == [
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 LIMIT 1")[:]
            assert res == [{"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2}]

            # Single partition queries without aggregates and with LIMIT
            res = session.execute("SELECT a, b, s FROM test WHERE a = 2 GROUP BY a, b LIMIT 1")[:]
            # FIXME: EXPECTED RESULT MUST BE UPDATED --> https://github.com/scylladb/scylladb/issues/14514
            # assert res == [{u'a': 2, u'b': 2, u's': 2}]
            assert len(res) == 1
            assert res == [
                {"a": 2, "b": 2, "s": 2},
            ]

            res = session.execute("SELECT a, b, s FROM test WHERE a = 2 GROUP BY a, b LIMIT 2")[:]
            assert res == [{"a": 2, "b": 2, "s": 2}, {"a": 2, "b": 4, "s": 2}]

            # Single partition queries with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b PER PARTITION LIMIT 1")[:]
            assert res == [
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
            ]

            # Single partition queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a = 2 GROUP BY a")[:]
            assert res == [{"a": 2, "s": 2, "system.count(a)": 1, "system.count(s)": 1}]

            # Single partition queries with ORDER BY
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC")[:]
            assert res == [{"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1}, {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 ORDER BY b DESC, c DESC")[:]
            assert res == [{"a": 2, "b": 4, "s": 2, "system.count(b)": 2, "system.count(s)": 2}]

            # Single partition queries with ORDER BY and LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 1")[:]
            assert len(res) == 1
            assert res == [
                {"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 ORDER BY b DESC, c DESC LIMIT 2")[:]
            assert res == [{"a": 2, "b": 4, "s": 2, "system.count(b)": 2, "system.count(s)": 2}]

            # Single partition queries with ORDER BY and PER PARTITION LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC PER PARTITION LIMIT 1")[:]
            assert res == [
                {"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
            ]

            # Multi-partitions queries
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 1, "b": 4, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4)")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 7, "system.count(s)": 7}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) AND b = 2 GROUP BY a, b")[:]
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2}, {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) AND b = 2")[:]
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 3, "system.count(s)": 3}]

            # Multi-partitions queries without aggregates
            res = session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1}, {"a": 2, "b": 2, "s": 2}, {"a": 3, "b": None, "s": 3}, {"a": 4, "b": 8, "s": None}]

            res = session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1}, {"a": 1, "b": 4, "s": 1}, {"a": 2, "b": 2, "s": 2}, {"a": 2, "b": 4, "s": 2}, {"a": 3, "b": None, "s": 3}, {"a": 4, "b": 8, "s": None}]

            # Multi-partitions queries with LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 7, "system.count(s)": 7}]

            # Multi-partitions queries without aggregates and with LIMIT
            res = session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "b": 2, "s": 1},
                {"a": 2, "b": 2, "s": 2},
            ]

            res = session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 10")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1}, {"a": 1, "b": 4, "s": 1}, {"a": 2, "b": 2, "s": 2}, {"a": 2, "b": 4, "s": 2}, {"a": 3, "b": None, "s": 3}, {"a": 4, "b": 8, "s": None}]

            # Multi-partitions queries with PER PARTITION LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 1")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 1, "b": 4, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 2, "b": 4, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
            ]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1")[:]
            # Reproduces scylladb/scylladb#21267
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
                {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1},
                {"a": 4, "b": 8, "s": None, "system.count(b)": 1, "system.count(s)": 0},
            ]

            # Multi-partitions queries with DISTINCT
            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a")[:]
            assert res == [
                {"a": 1, "s": 1, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 2, "s": 2, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 3, "s": 3, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 4, "s": None, "system.count(a)": 1, "system.count(s)": 0},
            ]

            # Multi-partitions queries with PER PARTITION LIMIT and LIMIT
            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 1 LIMIT 3")[:]
            assert len(res) == 3
            # Reproduces scylladb/scylladb#21267
            assert res == [{"a": 1, "b": 2, "s": 1, "system.count(b)": 4, "system.count(s)": 4}, {"a": 2, "b": 2, "s": 2, "system.count(b)": 2, "system.count(s)": 2}, {"a": 3, "b": None, "s": 3, "system.count(b)": 0, "system.count(s)": 1}]

            res = session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 3")[:]
            assert len(res) == 3
            assert res == [
                {"a": 1, "b": 2, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 1, "b": 4, "s": 1, "system.count(b)": 2, "system.count(s)": 2},
                {"a": 2, "b": 2, "s": 2, "system.count(b)": 1, "system.count(s)": 1},
            ]

            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4)")[:]
            assert res == [{"a": 1, "s": 1, "system.count(a)": 4, "system.count(s)": 3}]

            # Multi-partitions query with DISTINCT and LIMIT
            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2")[:]
            assert len(res) == 2
            assert res == [
                {"a": 1, "s": 1, "system.count(a)": 1, "system.count(s)": 1},
                {"a": 2, "s": 2, "system.count(a)": 1, "system.count(s)": 1},
            ]

            res = session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2")[:]
            assert res == [{"a": 1, "s": 1, "system.count(a)": 4, "system.count(s)": 3}]

    def test_static_columns_paging(self):  # noqa: PLR0912, PLR0915
        """
        Exercises paging with static columns to detect bugs
        @jira_ticket CASSANDRA-8502.
        """

        session = self.prepare(row_factory=named_tuple_factory)
        create_ks(session, "test_paging_static_cols", 3)
        session.execute("CREATE TABLE test (a int, b int, c int, s1 int static, s2 int static, PRIMARY KEY (a, b))")

        for i in range(4):
            for j in range(4):
                session.execute(f"INSERT INTO test (a, b, c, s1, s2) VALUES ({i}, {j}, {j}, 17, 42)")

        selectors = ("*", "a, b, c, s1, s2", "a, b, c, s1", "a, b, c, s2", "a, b, c")

        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test"))
                test_desc = f"{page_size=}, {selector=}"
                assert 16 == len(results), test_desc
                assert [0] * 4 + [1] * 4 + [2] * 4 + [3] * 4 == sorted([r.a for r in results])
                assert [0, 1, 2, 3] * 4 == [r.b for r in results]
                assert [0, 1, 2, 3] * 4 == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results], dest_desc

        # IN over the partitions
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a IN (0, 1, 2, 3)"))
                test_desc = f"{page_size=}, {selector=}"
                assert 16 == len(results), test_desc
                assert [0] * 4 + [1] * 4 + [2] * 4 + [3] * 4 == sorted([r.a for r in results]), test_desc
                assert [0, 1, 2, 3] * 4 == [r.b for r in results], test_desc
                assert [0, 1, 2, 3] * 4 == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results], test_desc

        # single partition
        for i in range(16):
            session.execute(f"INSERT INTO test (a, b, c, s1, s2) VALUES (99, {i}, {i}, 17, 42)")

        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99"))
                test_desc = f"{page_size=}, {selector=}"
                assert 16 == len(results), test_desc
                assert [99] * 16 == [r.a for r in results], test_desc
                assert list(range(16)) == [r.b for r in results], test_desc
                assert list(range(16)) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results], test_desc

        # reversed
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 ORDER BY b DESC"))
                test_desc = f"{page_size=}, {selector=}"
                assert 16 == len(results), test_desc
                assert [99] * 16 == [r.a for r in results], test_desc
                assert list(reversed(range(16))) == [r.b for r in results], test_desc
                assert list(reversed(range(16))) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results], test_desc

        # IN on clustering column
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15)"))
                test_desc = f"{page_size=}, {selector=}"
                assert 5 == len(results), test_desc
                assert [99] * 5 == [r.a for r in results], test_desc
                assert [3, 4, 8, 14, 15] == [r.b for r in results], test_desc
                assert [3, 4, 8, 14, 15] == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 5 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 5 == [r.s2 for r in results], test_desc

        # reversed IN on clustering column
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15) ORDER BY b DESC"))
                test_desc = f"{page_size=}, {selector=}"
                assert 5 == len(results), test_desc
                assert [99] * 5 == [r.a for r in results], test_desc
                assert list(reversed([3, 4, 8, 14, 15])) == [r.b for r in results], test_desc
                assert list(reversed([3, 4, 8, 14, 15])) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 5 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 5 == [r.s2 for r in results], test_desc

        # slice on clustering column with set start
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b > 3"))
                test_desc = f"{page_size=}, {selector=}"
                assert 12 == len(results), test_desc
                assert [99] * 12 == [r.a for r in results], test_desc
                assert list(range(4, 16)) == [r.b for r in results], test_desc
                assert list(range(4, 16)) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 12 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 12 == [r.s2 for r in results], test_desc

        # reversed slice on clustering column with set finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b > 3 ORDER BY b DESC"))
                test_desc = f"{page_size=}, {selector=}"
                assert 12 == len(results), test_desc
                assert [99] * 12 == [r.a for r in results], test_desc
                assert list(reversed(range(4, 16))) == [r.b for r in results], test_desc
                assert list(reversed(range(4, 16))) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 12 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 12 == [r.s2 for r in results], test_desc

        # slice on clustering column with set finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b < 14"))
                test_desc = f"{page_size=}, {selector=}"
                assert 14 == len(results), test_desc
                assert [99] * 14 == [r.a for r in results], test_desc
                assert list(range(14)) == [r.b for r in results], test_desc
                assert list(range(14)) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 14 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 14 == [r.s2 for r in results], test_desc

        # reversed slice on clustering column with set start
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b < 14 ORDER BY b DESC"))
                test_desc = f"{page_size=}, {selector=}"
                assert 14 == len(results), test_desc
                assert [99] * 14 == [r.a for r in results], test_desc
                assert list(reversed(range(14))) == [r.b for r in results], test_desc
                assert list(reversed(range(14))) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 14 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 14 == [r.s2 for r in results]

        # slice on clustering column with start and finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b > 3 AND b < 14"))
                test_desc = f"{page_size=}, {selector=}"
                assert 10 == len(results), test_desc
                assert [99] * 10 == [r.a for r in results], test_desc
                assert list(range(4, 14)) == [r.b for r in results], test_desc
                assert list(range(4, 14)) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 10 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 10 == [r.s2 for r in results], test_desc

        # reversed slice on clustering column with start and finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                test_desc = f"{page_size=}, {selector=}"
                results = list(session.execute(f"SELECT {selector} FROM test WHERE a = 99 AND b > 3 AND b < 14 ORDER BY b DESC"))
                assert 10 == len(results), test_desc
                assert [99] * 10 == [r.a for r in results], test_desc
                assert list(reversed(range(4, 14))) == [r.b for r in results], test_desc
                assert list(reversed(range(4, 14))) == [r.c for r in results], test_desc
                if "s1" in selector:
                    assert [17] * 10 == [r.s1 for r in results], test_desc
                if "s2" in selector:
                    assert [42] * 10 == [r.s2 for r in results], test_desc

    def test_paging_using_secondary_indexes_with_static_cols(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test (id int, s1 int static, s2 int static, mybool boolean, sometext text, PRIMARY KEY (id, sometext))")
        session.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(_):
            return str(uuid.uuid4())

        def bool_from_str_int(text):
            return bool(int(text))

        data = """
             | id | s1 | s2 | mybool| sometext |
             +----+----+----+-------+----------+
         *100| 1  | 1  | 4  | 1     | [random] |
         *300| 2  | 2  | 3  | 0     | [random] |
         *500| 3  | 3  | 2  | 1     | [random] |
         *400| 4  | 4  | 1  | 0     | [random] |
            """
        all_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "mybool": bool_from_str_int, "sometext": random_txt, "s1": int, "s2": int})

        future = session.execute_async(SimpleStatement("select * from paging_test where mybool = true", fetch_size=400, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future).request_all()

        # the query only searched for True rows, so let's pare down the expectations for comparison
        expected_data = filter(lambda x: x.get("mybool") is True, all_data)

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [400, 200]
        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_static_columns_with_empty_non_static_columns_paging(self):
        """
        @jira_ticket CASSANDRA-10381.
        """

        session = self.prepare(row_factory=named_tuple_factory)
        create_ks(session, "test_paging_static_cols", 3)
        session.execute("CREATE TABLE test (a int, b int, c int, s int static, PRIMARY KEY (a, b))")

        for i in range(10):
            session.execute(f"UPDATE test SET s = {i} WHERE a = {i}")

        session.default_fetch_size = 2
        results = list(session.execute("SELECT * FROM test"))
        assert [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] == sorted([r.s for r in results])

        results = list(session.execute("SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4)"))
        assert [0, 1, 2, 3, 4] == sorted([r.s for r in results])

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_paging_on_compact_table_with_tombstone_on_first_column(self):
        """
        test paging, on  COMPACT tables without clustering columns, when the first column has a tombstone
        @jira_ticket CASSANDRA-11467
        """

        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, "test_paging_on_compact_table_with_tombstone", 3)
        session.execute("CREATE TABLE test (a int primary key, b int, c int) WITH COMPACT STORAGE")

        for i in range(5):
            session.execute(f"INSERT INTO test (a, b, c) VALUES ({i}, {1}, {1})")
            session.execute(f"DELETE b FROM test WHERE a = {i}")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT * FROM test"))
            assert res == [[1, None, 1], [0, None, 1], [2, None, 1], [4, None, 1], [3, None, 1]]

    def test_paging_with_empty_row_and_empty_static_columns(self):
        """
        test paging when the rows and the static columns are empty
        @jira_ticket CASSANDRA-13017
        """

        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, "test_paging_with_empty_rows_and_static_columns", 3)
        session.execute("CREATE TABLE test (pk int, c int, v int, s int static, primary key(pk, c))")

        for i in range(5):
            for j in range(5):
                session.execute(f"INSERT INTO test (pk, c) VALUES ({i}, {j})")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT DISTINCT pk FROM test"))
            assert res == [[1], [0], [2], [4], [3]]

            res = rows_to_list(session.execute("SELECT DISTINCT pk FROM test LIMIT 4"))
            assert res == [[1], [0], [2], [4]]

            res = rows_to_list(session.execute("SELECT DISTINCT pk, s FROM test"))
            assert res == [[1, None], [0, None], [2, None], [4, None], [3, None]]

            res = rows_to_list(session.execute("SELECT DISTINCT pk, s FROM test LIMIT 4"))
            assert res == [[1, None], [0, None], [2, None], [4, None]]

    def test_per_partition_limit_paging(self):
        """
        Test paging with per partition limit queries.
        It tests per partition limits with:
        * filter
        * limit
        * order by

        each one with and without paging.

        @jira_ticket CASSANDRA-11535
        implements scylla #2202
        """

        def is_per_partition_result_correct(result, per_partition_limit):
            result_count = Counter(row[0] for row in result)

            return any(val > per_partition_limit for val in result_count.values())

        def query_and_compare_results(query, expected_result, per_partition_limit, page_size=None, ignore_order=True):
            query_addition = f"PER PARTITION LIMIT {per_partition_limit}"
            if ignore_order:
                assert_test = self.assert_equal_ignore_order
            else:

                def assert_test(a, b):
                    assert a == b

            if page_size:
                future = session.execute_async(SimpleStatement(query.format(query_addition), fetch_size=page_size, consistency_level=ConsistencyLevel.ALL))
                pf = PageFetcher(future)
                pf.request_all()
                res = rows_to_list(pf.all_data())
                if callable(expected_result):
                    assert expected_result(res)
                else:
                    assert_test(res, expected_result)
            else:
                res = rows_to_list(session.execute(query.format(query_addition)))
                if callable(expected_result):
                    assert expected_result(res)
                else:
                    assert_test(res, expected_result)
            is_per_partition_result_correct(res, per_partition_limit)

        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, "test_paging_with_per_partition_limit", 3)
        session.execute("CREATE TABLE test (a int, b int, c int, PRIMARY KEY (a, b))")

        for i in range(5):
            for j in range(5):
                session.execute(f"INSERT INTO test (a, b, c) VALUES ({i}, {j}, {j})")

        # CREATING QUERIES X EXPECTED DATA
        query_and_results = [
            {"query": "SELECT * FROM test {}", "expected_result": [[0, 0, 0], [0, 1, 1], [1, 0, 0], [1, 1, 1], [2, 0, 0], [2, 1, 1], [3, 0, 0], [3, 1, 1], [4, 0, 0], [4, 1, 1]], "per_partition_limit": 2, "ignore_order": True},
            {"query": "SELECT * FROM test WHERE a IN (1,2,3) {}", "expected_result": [[1, 0, 0], [1, 1, 1], [1, 2, 2], [2, 0, 0], [2, 1, 1], [2, 2, 2], [3, 0, 0], [3, 1, 1], [3, 2, 2]], "per_partition_limit": 3, "ignore_order": True},
            {"query": "SELECT * FROM test WHERE a = 1 {}", "expected_result": [[1, 0, 0], [1, 1, 1], [1, 2, 2], [1, 3, 3]], "per_partition_limit": 4, "ignore_order": True},
            {"query": "SELECT * FROM test WHERE a = 1 ORDER BY b DESC {}", "expected_result": [[1, 4, 4], [1, 3, 3], [1, 2, 2], [1, 1, 1]], "per_partition_limit": 4, "ignore_order": False},
            {"query": "SELECT * FROM test WHERE a = 1 {} LIMIT 3", "expected_result": [[1, 0, 0], [1, 1, 1], [1, 2, 2]], "per_partition_limit": 4, "ignore_order": True},
            {"query": "SELECT * FROM test WHERE a = 1 AND b > 1 {}", "expected_result": [[1, 2, 2], [1, 3, 3]], "per_partition_limit": 2, "ignore_order": True},
            {"query": "SELECT * FROM test WHERE a = 1 AND b > 1 ORDER BY b DESC {}", "expected_result": [[1, 4, 4], [1, 3, 3]], "per_partition_limit": 2, "ignore_order": False},
            {"query": "SELECT * FROM test {} LIMIT 6", "expected_result": lambda result: len(result) == 6, "per_partition_limit": 2, "ignore_order": True},
            {"query": "SELECT * FROM test {} LIMIT 5", "expected_result": lambda result: len(result) == 5, "per_partition_limit": 2, "ignore_order": True},
        ]

        # EXECUTING CMDS
        for query_and_result in query_and_results:
            for page_size in (None, 2, 3, 4, 5, 15, 16, 17, 100):
                query_and_result["page_size"] = page_size
                query_and_compare_results(**query_and_result)


@pytest.mark.dtest_full
class TestPagingDatasetChanges(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """

    def test_data_change_impacting_earlier_page(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
              | id | mytext   |
              +----+----------+
          *500| 1  | [random] |
          *500| 2  | [random] |
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "mytext": random_txt})

        # get 501 rows so we have definitely got the 1st row of the second partition
        future = session.execute_async(SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=501, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # we got one page and should be done with the first partition (for id=1)
        # let's add another row for that first partition (id=1) and make sure it won't sneak into results
        session.execute(SimpleStatement("insert into paging_test (id, mytext) values (1, 'foo')", consistency_level=ConsistencyLevel.ALL))

        pf.request_all()
        assert pf.pagecount() == 2
        assert pf.num_results_all() == [501, 499]

        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_data_change_impacting_later_page(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
              | id | mytext   |
              +----+----------+
          *500| 1  | [random] |
          *499| 2  | [random] |
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "mytext": random_txt})

        future = session.execute_async(SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=500, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # we've already paged the first partition, but adding a row for the second (id=2)
        # should still result in the row being seen on the subsequent pages
        session.execute(SimpleStatement("insert into paging_test (id, mytext) values (2, 'foo')", consistency_level=ConsistencyLevel.ALL))

        pf.request_all()
        assert pf.pagecount() == 2
        assert pf.num_results_all() == [500, 500]

        # add the new row to the expected data and then do a compare
        expected_data.append({"id": 2, "mytext": "foo"})
        self.assert_equal_ignore_order(pf.all_data(), expected_data)

    def test_row_ttl_expiry_during_paging(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(_):
            return str(uuid.uuid4())

        # create rows with TTL (some of which we'll try to get after expiry)
        create_rows(
            """
                | id | mytext   |
                +----+----------+
            *300| 1  | [random] |
            *400| 2  | [random] |
            """,
            session,
            "paging_test",
            cl=ConsistencyLevel.ALL,
            format_funcs={"id": int, "mytext": random_txt},
            postfix="USING TTL 10",
        )

        # create rows without TTL
        create_rows(
            """
                | id | mytext   |
                +----+----------+
            *500| 3  | [random] |
            """,
            session,
            "paging_test",
            cl=ConsistencyLevel.ALL,
            format_funcs={"id": int, "mytext": random_txt},
        )

        future = session.execute_async(SimpleStatement("select * from paging_test where id in (1,2,3)", fetch_size=300, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved
        # this page will be partition id=1, it has TTL rows but they are not expired yet

        # sleep so that the remaining TTL rows from partition id=2 expire
        time.sleep(15)

        pf.request_all()
        assert pf.pagecount() == 3
        assert pf.num_results_all() == [300, 300, 200]

    def test_cell_ttl_expiry_during_paging(self):
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute(
            """
            CREATE TABLE paging_test (
                id int,
                mytext text,
                somevalue text,
                anothervalue text,
                PRIMARY KEY (id, mytext) )
            """
        )

        def random_txt(_):
            return str(uuid.uuid4())

        data = create_rows(
            """
                | id | mytext   | somevalue | anothervalue |
                +----+----------+-----------+--------------+
            *500| 1  | [random] | foo       |  bar         |
            *500| 2  | [random] | foo       |  bar         |
            *500| 3  | [random] | foo       |  bar         |
            """,
            session,
            "paging_test",
            cl=ConsistencyLevel.ALL,
            format_funcs={"id": int, "mytext": random_txt},
        )

        future = session.execute_async(SimpleStatement("select * from paging_test where id in (1,2,3)", fetch_size=500, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future)

        # no need to request page here, because the first page is automatically retrieved
        page1 = pf.page_data(1)
        self.assert_equal_ignore_order(page1, data[:500])

        # set some TTLs for data on page 3
        for row in data[1000:1500]:
            _id, mytext = row["id"], row["mytext"]
            stmt = SimpleStatement(
                f"""
                update paging_test using TTL 10
                set somevalue='one', anothervalue='two' where id = {_id} and mytext = '{mytext}'
                """,
                consistency_level=ConsistencyLevel.ALL,
            )
            session.execute(stmt)

        # check page two
        pf.request_one()
        page2 = pf.page_data(2)
        self.assert_equal_ignore_order(page2, data[500:1000])

        page3expected = []
        for row in data[1000:1500]:
            _id, mytext = row["id"], row["mytext"]
            page3expected.append({"id": _id, "mytext": mytext, "somevalue": None, "anothervalue": None})

        time.sleep(15)

        pf.request_one()
        page3 = pf.page_data(3)
        self.assert_equal_ignore_order(page3, page3expected)

    def test_node_unavailable_during_paging(self):
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id uuid, mytext text, PRIMARY KEY (id, mytext) )")

        def make_uuid(_):
            return uuid.uuid4()

        create_rows(
            """
                  | id      | mytext |
                  +---------+--------+
            *10000| [uuid]  | foo    |
            """,
            session,
            "paging_test",
            cl=ConsistencyLevel.ALL,
            format_funcs={"id": make_uuid},
        )

        future = session.execute_async(SimpleStatement("select * from paging_test where mytext = 'foo' allow filtering", fetch_size=2000, consistency_level=ConsistencyLevel.ALL))

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # stop a node and make sure we get an error trying to page the rest
        node1.stop()
        with pytest.raises(RuntimeError, match="Requested pages were not delivered before timeout"):
            pf.request_all()

        # TODO: can we resume the node and expect to get more results from the result set or is it done?


@pytest.mark.dtest_full
class TestPagingQueryIsolation(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with isolation of paged queries (queries can't affect each other).
    """

    def test_query_isolation(self):
        """
        Interleave some paged queries and make sure nothing bad happens.
        """
        session = self.prepare()
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
               | id | mytext   |
               +----+----------+
          *5000| 1  | [random] |
          *5000| 2  | [random] |
          *5000| 3  | [random] |
          *5000| 4  | [random] |
          *5000| 5  | [random] |
          *5000| 6  | [random] |
          *5000| 7  | [random] |
          *5000| 8  | [random] |
          *5000| 9  | [random] |
          *5000| 10 | [random] |
            """
        expected_data = create_rows(data, session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "mytext": random_txt})

        stmts = [
            SimpleStatement("select * from paging_test where id in (1)", fetch_size=500, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (2)", fetch_size=600, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (3)", fetch_size=700, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (4)", fetch_size=800, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (5)", fetch_size=900, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (1)", fetch_size=1000, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (2)", fetch_size=1100, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (3)", fetch_size=1200, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (4)", fetch_size=1300, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (5)", fetch_size=1400, consistency_level=ConsistencyLevel.ALL),
            SimpleStatement("select * from paging_test where id in (1,2,3,4,5,6,7,8,9,10)", fetch_size=1500, consistency_level=ConsistencyLevel.ALL),
        ]

        page_fetchers = []

        for stmt in stmts:
            future = session.execute_async(stmt)
            page_fetchers.append(PageFetcher(future))
            # first page is auto-retrieved, so no need to request it

        for pf in page_fetchers:
            pf.request_one()

        for pf in page_fetchers:
            pf.request_one()

        for pf in page_fetchers:
            pf.request_all()

        assert page_fetchers[0].pagecount() == 10
        assert page_fetchers[1].pagecount() == 9
        assert page_fetchers[2].pagecount() == 8
        assert page_fetchers[3].pagecount() == 7
        assert page_fetchers[4].pagecount() == 6
        assert page_fetchers[5].pagecount() == 5
        assert page_fetchers[6].pagecount() == 5
        assert page_fetchers[7].pagecount() == 5
        assert page_fetchers[8].pagecount() == 4
        assert page_fetchers[9].pagecount() == 4
        assert page_fetchers[10].pagecount() == 34

        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[0].all_data()), flatten_into_set(expected_data[:5000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[1].all_data()), flatten_into_set(expected_data[5000:10000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[2].all_data()), flatten_into_set(expected_data[10000:15000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[3].all_data()), flatten_into_set(expected_data[15000:20000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[4].all_data()), flatten_into_set(expected_data[20000:25000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[5].all_data()), flatten_into_set(expected_data[:5000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[6].all_data()), flatten_into_set(expected_data[5000:10000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[7].all_data()), flatten_into_set(expected_data[10000:15000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[8].all_data()), flatten_into_set(expected_data[15000:20000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[9].all_data()), flatten_into_set(expected_data[20000:25000]))
        self.assert_equal_ignore_order(flatten_into_set(page_fetchers[10].all_data()), flatten_into_set(expected_data[:50000]))


@pytest.mark.dtest_full
class TestPagingWithDeletions(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when deletions occur.
    """

    def setup_data(self):
        create_ks(self.session, "test_paging_size", 3)
        self.session.execute("CREATE TABLE paging_test ( id int, mytext text, col1 int, col2 int, col3 int, PRIMARY KEY (id, mytext) )")

        def random_txt(_):
            return str(uuid.uuid4())

        data = """
             | id | mytext   | col1 | col2 | col3 |
             +----+----------+------+------+------+
          *40| 1  | [random] | 1    | 1    | 1    |
          *40| 2  | [random] | 2    | 2    | 2    |
          *40| 3  | [random] | 4    | 3    | 3    |
          *40| 4  | [random] | 4    | 4    | 4    |
          *40| 5  | [random] | 5    | 5    | 5    |
        """

        create_rows(data, self.session, "paging_test", cl=ConsistencyLevel.ALL, format_funcs={"id": int, "mytext": random_txt, "col1": int, "col2": int, "col3": int})

        pf = self.get_page_fetcher()
        pf.request_all()
        return pf.all_data()

    def get_page_fetcher(self):
        future = self.session.execute_async(SimpleStatement("select * from paging_test where id in (1,2,3,4,5)", fetch_size=25, consistency_level=ConsistencyLevel.ALL))

        return PageFetcher(future)

    def check_all_paging_results(self, expected_data, pagecount, num_page_results):
        """Check all paging results: pagecount, num_results per page, data."""

        page_size = 25
        expected_pages_data = [expected_data[x : x + page_size] for x in range(0, len(expected_data), page_size)]

        pf = self.get_page_fetcher()
        pf.request_all()
        assert pf.pagecount() == pagecount
        assert pf.num_results_all() == num_page_results

        for i in range(pf.pagecount()):
            page_data = pf.page_data(i + 1)
            assert page_data == expected_pages_data[i]

    def test_single_partition_deletions(self):
        """Test single partition deletions"""
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the a single partition at the beginning
        self.session.execute(SimpleStatement("delete from paging_test where id = 1", consistency_level=ConsistencyLevel.ALL))
        expected_data = [row for row in expected_data if row["id"] != 1]
        self.check_all_paging_results(expected_data, 7, [25, 25, 25, 25, 25, 25, 10])

        # Delete the a single partition in the middle
        self.session.execute(SimpleStatement("delete from paging_test where id = 3", consistency_level=ConsistencyLevel.ALL))
        expected_data = [row for row in expected_data if row["id"] != 3]
        self.check_all_paging_results(expected_data, 5, [25, 25, 25, 25, 20])

        # Delete the a single partition at the end
        self.session.execute(SimpleStatement("delete from paging_test where id = 5", consistency_level=ConsistencyLevel.ALL))
        expected_data = [row for row in expected_data if row["id"] != 5]
        self.check_all_paging_results(expected_data, 4, [25, 25, 25, 5])

        # Keep only the partition '2'
        self.session.execute(SimpleStatement("delete from paging_test where id = 4", consistency_level=ConsistencyLevel.ALL))
        expected_data = [row for row in expected_data if row["id"] != 4]
        self.check_all_paging_results(expected_data, 2, [25, 15])

    def test_multiple_partition_deletions(self):
        """Test multiple partition deletions"""
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Keep only the partition '1'
        self.session.execute(SimpleStatement("delete from paging_test where id in (2,3,4,5)", consistency_level=ConsistencyLevel.ALL))
        expected_data = [row for row in expected_data if row["id"] == 1]
        self.check_all_paging_results(expected_data, 2, [25, 15])

    def test_single_row_deletions(self):
        """Test single row deletions"""
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the first row
        row = expected_data.pop(0)
        self.session.execute(SimpleStatement(("delete from paging_test where id = {} and mytext = '{}'".format(row["id"], row["mytext"])), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 24])

        # Delete a row in the middle
        row = expected_data.pop(100)
        self.session.execute(SimpleStatement(("delete from paging_test where id = {} and mytext = '{}'".format(row["id"], row["mytext"])), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 23])

        # Delete the last row
        row = expected_data.pop()
        self.session.execute(SimpleStatement(("delete from paging_test where id = {} and mytext = '{}'".format(row["id"], row["mytext"])), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 22])

        # Delete all the last page row by row
        rows = expected_data[-22:]
        for row in rows:
            self.session.execute(SimpleStatement(("delete from paging_test where id = {} and mytext = '{}'".format(row["id"], row["mytext"])), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 7, [25, 25, 25, 25, 25, 25, 25])

    def test_multiple_row_deletions(self):
        """Test multiple row deletions.
        This test should be finished when CASSANDRA-6237 is done.
        """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete a bunch of rows
        rows = expected_data[100:105]
        expected_data = expected_data[0:100] + expected_data[105:]
        in_condition = ",".join("'{}'".format(r["mytext"]) for r in rows)

        self.session.execute(SimpleStatement((f"delete from paging_test where id = {3} and mytext in ({in_condition})"), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 20])

    def test_paged_read_with_tombstones(self):
        session = self.prepare()
        seed = random.randint(0, 1000)
        logger.debug(f"Testing with a seed of: {seed}")
        random.seed(seed)
        deletion_range = random.randint(10, 200)
        rows_number = deletion_range * random.randint(3, 20)
        page_size = random.randint(5, 100)
        logger.info(f"Testing a total {rows_number=}, with a {deletion_range=} and a {page_size=}")
        create_ks(session, "paging_test", 3)
        session.execute(
            """
            CREATE TABLE paging_test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            );
        """
        )
        node1 = self.cluster.nodelist()[0]
        num_of_deletions = 0
        for index in range(0, rows_number, deletion_range):
            for insert in range(index, index + deletion_range):
                session.execute("INSERT INTO paging_test (k, c, v) VALUES (0, %i, %i)" % (insert, insert))
            node1.flush()
            deletion_end = index + deletion_range // 2
            deletes = deletion_end - index
            num_of_deletions += deletes
            session.execute("DELETE FROM paging_test WHERE k = 0 AND c >= %d AND c < %d" % (index, deletion_end))
            node1.flush()

        def _get_all_data():
            future = session.execute_async(SimpleStatement("select * from paging_test BYPASS CACHE", fetch_size=page_size, consistency_level=ConsistencyLevel.QUORUM))
            pager = PageFetcher(future)
            pager.request_all()
            return pager.all_data()

        all_data = _get_all_data()
        left_rows = rows_number - num_of_deletions
        assert len(all_data) == left_rows
        deletion_factor = random.randint(2, 7)
        for index in range(0, len(all_data), deletion_factor):
            clustering_key = all_data[index]["c"]
            session.execute("DELETE FROM paging_test WHERE k = 0 AND c = %d" % clustering_key)
            left_rows -= 1
        all_data = _get_all_data()
        assert len(all_data) == left_rows

    def test_single_cell_deletions(self):
        """Test single cell deletions"""
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the first cell of some rows of the last partition
        pkeys = [r["mytext"] for r in expected_data if r["id"] == 5][:20]
        for r in expected_data:
            if r["id"] == 5 and r["mytext"] in pkeys:
                r["col1"] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement((f"delete col1 from paging_test where id = 5 and mytext = '{pkey}'"), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 25])

        # Delete the mid cell of some rows of the first partition
        pkeys = [r["mytext"] for r in expected_data if r["id"] == 1][20:]
        for r in expected_data:
            if r["id"] == 1 and r["mytext"] in pkeys:
                r["col2"] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement((f"delete col2 from paging_test where id = 1 and mytext = '{pkey}'"), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 25])

        # Delete the last cell of all rows of the mid partition
        pkeys = [r["mytext"] for r in expected_data if r["id"] == 3]
        for r in expected_data:
            if r["id"] == 3 and r["mytext"] in pkeys:
                r["col3"] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement((f"delete col3 from paging_test where id = 3 and mytext = '{pkey}'"), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 25])

    def test_multiple_cell_deletions(self):
        """Test multiple cell deletions"""
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the multiple cells of some rows of the second partition
        pkeys = [r["mytext"] for r in expected_data if r["id"] == 2][20:]
        for r in expected_data:
            if r["id"] == 2 and r["mytext"] in pkeys:
                r["col1"] = None
                r["col2"] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement((f"delete col1, col2 from paging_test where id = 2 and mytext = '{pkey}'"), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 25])

        # Delete the multiple cells of all rows of the fourth partition
        pkeys = [r["mytext"] for r in expected_data if r["id"] == 4]
        for r in expected_data:
            if r["id"] == 4 and r["mytext"] in pkeys:
                r["col2"] = None
                r["col3"] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement((f"delete col2, col3 from paging_test where id = 4 and mytext = '{pkey}'"), consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(expected_data, 8, [25, 25, 25, 25, 25, 25, 25, 25])

    def test_ttl_deletions(self):
        """Test ttl deletions. Paging over a query that has only tombstones"""
        self.session = self.prepare()
        data = self.setup_data()

        # Set TTL to all row
        ttl_seconds = 15
        for row in data:
            s = ("insert into paging_test (id, mytext, col1, col2, col3) values ({}, '{}', {}, {}, {}) using ttl {};").format(row["id"], row["mytext"], row["col1"], row["col2"], row["col3"], ttl_seconds)
            self.session.execute(SimpleStatement(s, consistency_level=ConsistencyLevel.ALL))
        self.check_all_paging_results(data, 8, [25, 25, 25, 25, 25, 25, 25, 25])
        time.sleep(ttl_seconds + 1)
        self.check_all_paging_results([], 0, [])

    @pytest.mark.skip(reason="test doesn't behave as expected - tombstone_failure_threshold supported ?")
    def test_failure_threshold_deletions(self, fixture_dtest_setup):
        """Test that paging throws a failure in case of tombstone threshold"""
        self.cluster.set_configuration_options(values={"tombstone_failure_threshold": 500})
        self.session = self.prepare()
        node1, node2, node3 = self.cluster.nodelist()

        self.setup_data()

        # Add more data
        values = map(lambda i: uuid.uuid4(), range(3000))
        for value in values:
            self.session.execute(SimpleStatement(f"insert into paging_test (id, mytext, col1) values (1, '{value}', null) ", consistency_level=ConsistencyLevel.ALL))

        assert_invalid(self.session, SimpleStatement("select * from paging_test", fetch_size=1000, consistency_level=ConsistencyLevel.ALL), expected=ReadTimeout if Version(self.cluster.version()) < Version("2.2") else ReadFailure)

        if Version(self.cluster.version()) < Version("3.0"):
            failure_msg = "Scanned over.* tombstones in test_paging_size.paging_test.* query aborted"
        else:
            failure_msg = "Scanned over.* tombstones during query.* query aborted"
        failure = node1.grep_log(failure_msg) or node2.grep_log(failure_msg) or node3.grep_log(failure_msg)
        fixture_dtest_setup.ignore_log_patterns += [failure_msg]

        assert failure, "Cannot find tombstone failure threshold error in log"

    def test_deletion_with_distinct_paging(self):
        """
        Test that deletion does not affect paging for distinct queries.

        @jira_ticket CASSANDRA-10010
        """
        self.session = self.prepare()
        create_ks(self.session, "test_paging_size", 3)
        self.session.execute("CREATE TABLE paging_test ( k int, s int static, c int, v int, PRIMARY KEY (k, c) )")

        for whereClause in ("", "WHERE k IN (0, 1, 2, 3)"):
            for i in range(4):
                for j in range(2):
                    self.session.execute("INSERT INTO paging_test (k, s, c, v) VALUES (%s, %s, %s, %s)", (i, i, j, j))

            self.session.default_fetch_size = 2
            result = self.session.execute(f"SELECT DISTINCT k, s FROM paging_test {whereClause}")
            result = list(result)
            assert 4 == len(result)

            future = self.session.execute_async(f"SELECT DISTINCT k, s FROM paging_test {whereClause}")

            # this will fetch the first page
            fetcher = PageFetcher(future)

            # delete the first row in the last partition that was returned in the first page
            self.session.execute("DELETE FROM paging_test WHERE k = %s AND c = %s", (result[1]["k"], 0))

            # finish paging
            fetcher.request_all()
            assert [2, 2] == fetcher.num_results_all()


@pytest.mark.dtest_full
class TestPagingWithIndexingAndAggregation(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when deletions occur.
    """

    data = """
             | id | mybool | sometext | someint | somebigint |
             +----+--------+----------+---------+------------+
         *100| 1  | 1      | [random] | [random]| [random]
         *300| 2  | 0      | [random] | [random]| [random]
         *500| 3  | 1      | [random] | [random]| [random]
         *400| 4  | 0      | [random] | [random]| [random]
            """

    @staticmethod
    def create_table(session):
        create_ks(session, "test_paging_size", 3)
        session.execute("CREATE TABLE paging_test (id int, mybool boolean, sometext text, someint int, somebigint bigint, PRIMARY KEY (id, sometext))")

    @staticmethod
    def create_and_insert_data(data, session, table_name="paging_test", cl=ConsistencyLevel.ALL):
        def random_txt(_):
            return str(uuid.uuid4())

        def bool_from_str_int(text):
            return bool(int(text))

        def random_int(_):
            return ctypes.c_int(random.getrandbits(32)).value

        def random_bigint(_):
            return ctypes.c_long(random.getrandbits(64)).value

        all_data = create_rows(data, session, table_name, cl=cl, format_funcs={"id": int, "mybool": bool_from_str_int, "sometext": random_txt, "someint": random_int, "somebigint": random_bigint})
        return all_data

    def execute_query_and_compare_results(self, session, query, expected_data, assert_msg=""):
        logger.info(f"Validating '{query}'. Expected result: '{expected_data}'")
        future = session.execute_async(SimpleStatement(query, fetch_size=40, consistency_level=ConsistencyLevel.ALL))
        pf = PageFetcher(future).request_all()
        assert pf.pagecount() == 1, f"Expected 1 page but received {pf.pagecount()}"
        assert pf.num_results_all() == [1], f"Expected 1 single result, but received {pf.num_results_all()}"
        # we expect a single page, with a single row
        expected_col_name, expected_col_value = next(iter(expected_data[0].items()))
        col_name, col_value = next(iter(pf.all_data()[0].items()))
        if type(expected_col_name) == str:
            assert expected_col_name == col_name, assert_msg
        else:  # expect re.Pattern
            assert expected_col_name.fullmatch(col_name) is not None, assert_msg
        assert expected_col_value == col_value, assert_msg

    def _verify_col_func_results(  # noqa: PLR0913
        self,
        session,
        filtered_list,
        core_query,
        result_desc,
        col,
        query_func,
        exp_func,
        where_clause,
        allow_filtering,
    ):
        query = "select {} from paging_test where {}{}".format(core_query, where_clause, " ALLOW FILTERING" if allow_filtering else "")
        expected_data = [{result_desc: exp_func([item[col] for item in filtered_list])}]
        self.execute_query_and_compare_results(session=session, query=query, expected_data=expected_data, assert_msg=f"{core_query} returned wrong value")

    def _verify_col_results(self, session, filtered_list, col, where_clause, allow_filtering):
        for query_func, exp_func in [("count", len), ("min", min), ("max", max)]:
            query_fmt = f"{query_func}({col})"
            result_desc = f"system.{query_func}({col})"
            self._verify_col_func_results(session, filtered_list, query_fmt, result_desc, col, query_func, exp_func, where_clause, allow_filtering)

        if col.endswith("int"):
            query_func = "sum"
            if col.endswith("bigint"):
                int_type = "varint"
            else:
                int_type = "bigint"
            query_fmt = f"{query_func}(cast({col} as {int_type}))"
            result_desc = re.compile(f"system.{query_func}\\(cast\\({col} as {int_type}\\)\\)|system.{query_func}\\(system.castas{int_type}\\({col}\\)\\)")
            self._verify_col_func_results(session, filtered_list, query_fmt, result_desc, col, query_func, sum, where_clause, allow_filtering)

    def _create_and_verify_results(self, session, cols, filter_func, where_clause, allow_filtering):
        all_data = self.create_and_insert_data(self.data, session)
        filtered_list = [entry for entry in all_data if filter_func(entry) is True]
        if not isinstance(cols, list):
            cols = [cols]
        for col in cols:
            self._verify_col_results(session, filtered_list, col, where_clause, allow_filtering)

    def create_and_verify_mybool_results(self, session, cols, mybool_val=True, allow_filtering=False):
        def filter_func(entry):
            return entry["mybool"] == mybool_val

        where_clause = "mybool = {}".format("true" if mybool_val else "false")
        self._create_and_verify_results(session, cols, filter_func, where_clause, allow_filtering=allow_filtering)

    def create_and_verify_id_results(self, session, cols, id_val=2, allow_filtering=False):
        def filter_func(entry):
            return entry["id"] == id_val

        where_clause = f"id = {id_val}"
        self._create_and_verify_results(session, cols, filter_func, where_clause, allow_filtering=allow_filtering)

    def test_filter_indexed_column(self):
        session = self.prepare()
        self.create_table(session)

        session.execute("CREATE INDEX ON paging_test(mybool)")
        self.create_and_verify_mybool_results(session, ["someint", "somebigint"])

    def test_filter_non_indexed_column(self):
        session = self.prepare()
        self.create_table(session)

        self.create_and_verify_mybool_results(session, ["someint", "somebigint"], allow_filtering=True)

    def test_group_pk_column_index_filter(self):
        session = self.prepare()
        self.create_table(session)

        session.execute("CREATE INDEX ON paging_test(mybool)")
        self.create_and_verify_mybool_results(session, "id")

    def test_group_pk_column_non_index_filter(self):
        session = self.prepare()
        self.create_table(session)

        self.create_and_verify_mybool_results(session, "id", allow_filtering=True)

    def test_group_ck_column_index_filter(self):
        session = self.prepare()
        self.create_table(session)

        session.execute("CREATE INDEX ON paging_test(mybool)")
        self.create_and_verify_mybool_results(session, "sometext")

    def test_group_ck_column_non_index_filter(self):
        session = self.prepare()
        self.create_table(session)

        self.create_and_verify_mybool_results(session, "sometext", allow_filtering=True)

    def test_filter_pk_column(self):
        session = self.prepare()
        self.create_table(session)

        session.execute("CREATE INDEX ON paging_test(mybool)")
        self.create_and_verify_id_results(session, ["someint", "somebigint"], id_val=2)


@pytest.mark.dtest_full
class TestUnpagedQueryLimit(Tester):
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns += [r"Memory usage of unpaged query exceeds hard limit of [0-9]+" r" \(configured via max_memory_for_unlimited_query_hard_limit\)"]

    def test_unpaged_large_partition(self):
        self.cluster.set_configuration_options(values={"max_memory_for_unlimited_query_soft_limit": 1024, "max_memory_for_unlimited_query_hard_limit": 1024 * 1024})
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        self.cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE TestUnpagedQueryLimit WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute("CREATE TABLE TestUnpagedQueryLimit.test_unpaged_large_partition (pk int, ck int, v text, PRIMARY KEY (pk, ck) )")

        prepared_insert = session.prepare("INSERT INTO TestUnpagedQueryLimit.test_unpaged_large_partition (pk, ck, v) VALUES (?, ?, ?)")

        v = "a" * 1024

        for i in range(4 * 1024):
            session.execute(prepared_insert.bind((0, i, v)))

        for node in self.cluster.nodelist():
            session = self.patient_cql_connection(node)
            session.default_fetch_size = -1

            # Partition scan
            try:
                session.execute("SELECT * FROM TestUnpagedQueryLimit.test_unpaged_large_partition WHERE pk = 0")
                pytest.fail("Expected query to fail")
            except Exception as e:  # noqa: BLE001
                logger.info(f"Exception caught as expected: {e}")

            # Full scan
            try:
                session.execute("SELECT * FROM TestUnpagedQueryLimit.test_unpaged_large_partition")
                pytest.fail("Expected query to fail")
            except Exception as e:  # noqa: BLE001
                logger.info(f"Exception caught as expected: {e}")

    def mark_all_nodes_logs(self):
        nodes_with_marks = dict()
        for node in self.cluster.nodelist():
            nodes_with_marks[node] = node.mark_log()
        return nodes_with_marks

    @staticmethod
    def check_log_with_multiple_marks(filter_expr, nodes_with_marks):
        for node in nodes_with_marks.keys():
            if node.grep_log(filter_expr, from_mark=nodes_with_marks[node]):
                return True
        return False

    def test_unpaged_large_partition_soft_limit(self):
        limit = 1024
        self.cluster.set_configuration_options(values={"max_memory_for_unlimited_query_soft_limit": limit})
        warning_message = f"mutation_partition - Memory usage of unpaged query exceeds soft limit of {limit} \\(configured via max_memory_for_unlimited_query_soft_limit\\)"
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        self.cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE TestUnpagedQueryLimit WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute("CREATE TABLE TestUnpagedQueryLimit.test_unpaged_large_partition (pk int, ck int, v text, PRIMARY KEY (pk, ck) )")

        prepared_insert = session.prepare("INSERT INTO TestUnpagedQueryLimit.test_unpaged_large_partition (pk, ck, v) VALUES (?, ?, ?)")

        v = "a" * 1024

        for i in range(4 * 1024):
            session.execute(prepared_insert.bind((0, i, v)))

        node = random.choice(self.cluster.nodelist())
        session = self.patient_cql_connection(node)
        session.default_fetch_size = -1

        # Partition scan
        partition_scan_mark = self.mark_all_nodes_logs()
        session.execute("SELECT * FROM TestUnpagedQueryLimit.test_unpaged_large_partition WHERE pk = 0")

        if not self.check_log_with_multiple_marks(warning_message, partition_scan_mark):
            pytest.fail(f"Message {warning_message} not found for partition scan, hence failing")

        # Full scan
        full_scan_mark = self.mark_all_nodes_logs()
        session.execute("SELECT * FROM TestUnpagedQueryLimit.test_unpaged_large_partition")
        if not self.check_log_with_multiple_marks(warning_message, full_scan_mark):
            pytest.fail(f"Message {warning_message} not found for full scan, hence failing")
