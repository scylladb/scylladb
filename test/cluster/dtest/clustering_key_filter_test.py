import logging
import time

import pytest
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks

logger = logging.getLogger(__name__)


# All tests here should run with row cache disabled to make sure that
# the filtering capability is indeed working properly.
# start_cluster_and_get_node1() starts Scylla with --enable-cache set to 0.


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
@pytest.mark.parametrize("strategy", ["TimeWindowCompactionStrategy", "NullCompactionStrategy"])
class TestClusteringKeyFilter(Tester):
    # Check that a row tombstone is not discarded when its sstable doesn't contain clustering range
    # specified in the query.

    @pytest.fixture(scope="function")
    def strategy_string(self, strategy: str):
        return "'class':'" + strategy + "', 'enabled' : 'false'"

    @pytest.mark.dtest_debug
    def test_check_consistence_after_row_tombstone(self, strategy_string: str):
        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, r1 int, PRIMARY KEY (p1, c1)) WITH compaction= {" + strategy_string + "};"
        self.create_ks_and_cf(node1, query)

        query = "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'a', 1);"
        self.insert(node1, query)
        query = "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key2', 'b', 1);"
        self.insert(node1, query)
        self.check_number_of_rows(node1, 2)

        # create sstable with pkey 'key1' and ckey 'c' for it to be checked by clustering filter.
        # purpose here is to check that this sstable will not be filtered out because it will not
        # contain a clustering row that overlaps with clustering range filter.
        query = "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'c', 1);"
        self.insert(node1, query, False)
        self.remove_row(node1, "key2")
        self.check_number_of_rows(node1, 2)

        # check that row tombstone from sstable above was consired and only key1 was returned.
        query = "SELECT * FROM ks.cf WHERE p1 IN ('key1', 'key2') AND c1 >= 'a' AND c1 <= 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", ["a"])

    @pytest.mark.dtest_debug
    def test_check_non_composite(self, strategy_string: str):
        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, r1 int, PRIMARY KEY (p1, c1)) WITH compaction= {" + strategy_string + "};"
        # print query
        self.create_ks_and_cf(node1, query)

        query = "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'a', 1);"
        # print query
        self.insert(node1, query)
        query = "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'b', 1);"
        # print query
        self.insert(node1, query)

        self.check_number_of_rows(node1, 2)

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 <= 'a' AND c1 >= 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", [])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 >= 'a' AND c1 <= 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", ["a", "b"])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 > 'a' AND c1 <= 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", ["b"])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 >= 'a' AND c1 < 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", ["a"])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 > 'a' AND c1 < 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", [])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 > 'a';"
        result = self.select(node1, query)
        self.check_result(result, "key1", ["b"])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 < 'b';"
        result = self.select(node1, query)
        self.check_result(result, "key1", ["a"])

    @pytest.mark.dtest_debug
    def test_check_composite(self, strategy_string: str):  # noqa: PLR0915
        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, c2 text, r1 int, PRIMARY KEY (p1, c1, c2)) WITH compaction= {" + strategy_string + "};"
        self.create_ks_and_cf(node1, query)

        query = "INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', 'a', '1', 1);"
        self.insert(node1, query)
        query = "INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', 'a', '2', 1);"
        self.insert(node1, query)
        query = "INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', 'b', '1', 1);"
        self.insert(node1, query)
        query = "INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', 'b', '2', 1);"
        self.insert(node1, query)

        self.check_number_of_rows(node1, 4)

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 <= 'a' AND c1 >= 'b';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 >= 'a' AND c1 <= 'b';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "1"], ["a", "2"], ["b", "1"], ["b", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 > 'a' AND c1 <= 'b';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["b", "1"], ["b", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 >= 'a' AND c1 < 'b';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "1"], ["a", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 > 'a' AND c1 < 'b';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 > 'a';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["b", "1"], ["b", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 < 'b';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "1"], ["a", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 > '1' AND c2 < '2';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 >= '1' AND c2 < '2';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "1"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 > '1' AND c2 <= '2';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 >= '1' AND c2 <= '2';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "1"], ["a", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 > '1';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "2"]])

        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 >= '1';"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "1"], ["a", "2"]])

    def test_check_composite_2(self, strategy_string: str):
        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, c2 text, r1 int, PRIMARY KEY (p1, c1, c2)) WITH compaction= {" + strategy_string + "};"
        self.create_ks_and_cf(node1, query)

        # This will create a sstable with min max ranges [a, a] and [c, c].
        query = "INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', 'a', 'c', 1);"
        self.insert(node1, query)

        # Check that this query will properly generate a clustering range filter
        # [a, a] for first component, and (b, d) for the second.
        query = "SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 > 'b' AND c2 < 'd'"
        result = self.select(node1, query)
        self.check_result_composite(result, "key1", [["a", "c"]])

        self.check_number_of_rows(node1, 1)

    # HELPER FUNCTIONS
    def check_result(self, result, pkey, ckeys):
        assert len(result) == len(ckeys), "check number of clustering rows"
        for i in range(len(result)):
            assert result[i].p1 == pkey, "check partition key"
            assert result[i].c1 == ckeys[i], "check clustering key"

    def check_result_composite(self, result, pkey, composite_ckeys):
        assert len(result) == len(composite_ckeys), "check number of clustering rows"
        for i in range(len(result)):
            composite_ckey = composite_ckeys[i]
            assert len(composite_ckey) == 2, "check size of composite ckey"
            assert result[i].p1 == pkey, "check partition key"
            assert result[i].c1 == composite_ckey[0], "check clustering key 1"
            assert result[i].c2 == composite_ckey[1], "check clustering key 2"

    def start_cluster_and_get_node1(self):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        logger.debug("Starting a cluster of one node...")
        cluster.populate(1)
        cluster.start(jvm_args=["--enable-cache", "0"])

        return cluster.nodelist()[0]

    def create_ks_and_cf(self, node, query):
        logger.debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node)

        logger.debug("Creating a keyspace 'ks'...")
        create_ks(session, "ks", 1)

        logger.debug("Creating a column family 'cf'...")
        session.execute(query)
        time.sleep(0.2)

        logger.debug("Flushing a keyspace...")
        node.nodetool("flush -- ks")

    def check_number_of_rows(self, node, expected_number_of_rows):
        logger.debug("Checking number of rows on node1...")
        query = "SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node, "ks")
        result = list(s.execute(statement))
        assert result[0].count == expected_number_of_rows, str(len(result))

    def select(self, node1, query):
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, "ks")
        return list(s.execute(statement))

    def remove_row(self, node1, row):
        query = f"DELETE FROM cf WHERE p1='{row}';"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, "ks")
        s.execute(statement)
        node1.nodetool("flush -- ks")
        time.sleep(0.2)

    def insert(self, node1, query, flush=True):
        s = self.patient_cql_connection(node1, "ks")
        statement = SimpleStatement(query)
        s.execute(statement)
        # spread new data into different sstables to test correctness of filter.
        if flush:
            node1.nodetool("flush -- ks")
            time.sleep(0.2)
