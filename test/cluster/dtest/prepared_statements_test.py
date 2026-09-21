import pytest
from cassandra import InvalidRequest

from dtest_class import Tester

KEYSPACE = "foo"
pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestPreparedStatements(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    def test_dropped_index(self):
        """
        Prepared statements using dropped indexes should be handled correctly
        """

        self.cluster.populate(1).start()
        node = self.cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }
            """
            % KEYSPACE
        )

        session.set_keyspace(KEYSPACE)
        session.execute("CREATE TABLE IF NOT EXISTS mytable (a int PRIMARY KEY, b int)")
        session.execute("CREATE INDEX IF NOT EXISTS bindex ON mytable(b)")

        insert_statement = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        for i in range(10):
            session.execute(insert_statement, (i, 0))

        query_statement = session.prepare("SELECT * FROM mytable WHERE b=?")
        print("Number of matching rows:", len(list(session.execute(query_statement, (0,)))))

        session.execute("DROP INDEX bindex")

        try:
            print("Executing prepared statement with dropped index...")
            session.execute(query_statement, (0,))
        except InvalidRequest as ir:
            print(ir)
        except Exception:
            raise

    def test_prepared_select_star_on_schema_change(self):
        """
        Testing cassandra issue mentioned in:
        https://docs.datastax.com/en/developer/java-driver/3.1/manual/statements/prepared/#avoid-preparing-select-queries
        https://issues.apache.org/jira/browse/CASSANDRA-10786
        https://datastax-oss.atlassian.net/browse/JAVA-1196

        this should be address in 4.0 version
        """

        self.cluster.populate(1).start()
        node1 = self.cluster.nodelist()[0]

        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node1)

        # create key space and table
        session1.execute(
            """
             CREATE KEYSPACE IF NOT EXISTS %s
             WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }
             """
            % KEYSPACE
        )

        session1.set_keyspace(KEYSPACE)
        session2.set_keyspace(KEYSPACE)
        session1.execute("CREATE TABLE IF NOT EXISTS mytable (a int PRIMARY KEY, b int)")

        # insert data
        client1_insert_statement = session1.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        num_rows = 10

        for i in range(num_rows):
            session1.execute(client1_insert_statement, (i, 0))

        # check rows, and save prepared queries
        query_statements = []
        for session in [session1, session2]:
            query_statement = session.prepare("SELECT * FROM mytable")
            rows = list(session.execute(query_statement))
            assert num_rows == len(rows)
            for row in rows:
                assert hasattr(row, "b"), "row missing b column"

            query_statements += [(session, query_statement)]

        # alter the table
        session1.execute("ALTER TABLE mytable ADD c int")
        session1.execute("ALTER TABLE mytable DROP b")

        # run the prepared queries again to check they got the update of the table
        for session, query_statement in query_statements:
            for row in list(session.execute(query_statement)):
                assert hasattr(row, "c"), "row missing c column"
                assert not hasattr(row, "b"), "row shouldn't have b column"
