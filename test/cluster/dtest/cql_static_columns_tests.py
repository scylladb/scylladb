import pytest
from cassandra.cluster import ResultSet, Session

from tools.testers import ClusterSetupArgs, ConnectionArgs, CQLTester


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestStaticColumnQueries(CQLTester):
    CONNECTION_ARGS = ConnectionArgs()
    CLUSTER_SETUP_ARGS = ClusterSetupArgs()
    CF_NAME = "ks.static_queries"
    INDEXED_COL = "r1"
    SCHEMA = f"(k int, p int, s int static, {INDEXED_COL} int, PRIMARY KEY (k, p))"
    COLUMNS = f"(k, p, s, {INDEXED_COL})"
    DATA_SIZE = 10_000
    col1 = [1] * (DATA_SIZE // 2) + [0] * (DATA_SIZE // 2)
    col2 = [i for i in range(1, DATA_SIZE)]
    col3 = [11] * (DATA_SIZE // 2) + [22] * (DATA_SIZE // 2)
    col4 = col2
    DATA = list(zip(col1, col2, col3, col4))

    def _prepare_data(self, session: Session):
        self._create_schema(session=session, cf_name=self.CF_NAME, node_count=self.CLUSTER_SETUP_ARGS.nodes, schema=self.SCHEMA)
        self._create_index(session=session, cf_name=self.CF_NAME, indexed_col=self.INDEXED_COL)
        self._insert_data(session=session, data=self.DATA, cf_name=self.CF_NAME, columns=self.COLUMNS)

    @pytest.mark.parametrize(
        argnames=("select_columns", "indexed_col_value", "expected", "expected_count"),
        argvalues=[
            ("*", "= 3900", [{"k": 1, "p": 3900, "s": 11, "r1": 3900}], 1),
            ("k, p", "< 3", [{"k": 1, "p": 1}, {"k": 1, "p": 2}], 2),
            ("p", "= 2", [{"p": 2}], 1),
            ("s", "> 8000", [{"s": 22}], 1999),
            ("s", f" >= 5000 AND {INDEXED_COL} <= 5001", [{"s": 11}, {"s": 22}], 2),
        ],
        ids=("select_all", "select_primary_keys", "select_single_col", "select_static_col_only", "select_static_col_only_across_partitions"),
    )
    def test_query_static_column_when_using_index(self, select_columns: str, indexed_col_value: str, expected: list[dict], expected_count: int):
        """
        Query the data that has an indexed column and a static column:
                columns:            expected results:
                ----------------- partition 1
                 k |  p |  s | r1
                 1 |  1 | 11 | 1
                 1 |  2 | 11 | 2
                 ...
                 ...
                 1 |5000| 11 |5000
                 ----------------- partition 2
                 0 |5001| 22 |5001
                 0 |5002| 22 |5002
                 ...
                 0 |10000|22 |10000

        Assert that the returned ResulSets are equal to the expected results.
        """
        session = self.prepare(connection_args=self.CONNECTION_ARGS, cluster_setup_args=self.CLUSTER_SETUP_ARGS)
        self._prepare_data(session)
        query_result = self._query_using_index(session=session, select_columns=select_columns, indexed_col_value=indexed_col_value)
        self._validate_rows(query_result=query_result, expected_result=expected, expected_count=expected_count)

    def _query_using_index(self, session: Session, select_columns: str, indexed_col_value: str) -> ResultSet:
        query_str = f"SELECT {select_columns} FROM {self.CF_NAME} WHERE {self.INDEXED_COL} {indexed_col_value} ALLOW FILTERING"
        return session.execute(query_str)
