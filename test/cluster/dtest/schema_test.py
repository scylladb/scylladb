import time

import pytest
from cassandra.concurrent import execute_concurrent

from dtest_class import Tester, create_cf, create_ks
from tools.assertions import assert_all, assert_invalid, assert_one
from tools.cluster_topology import generate_cluster_topology
from tools.schema import describe_rf, get_replication_options
from tools.scylla_defines import CompactionStrategy


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestSchema(Tester):
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_drop_column_compact(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        assert_invalid(session, "ALTER TABLE cf DROP c1", "Cannot drop columns from a")

    @pytest.mark.dtest_debug
    def test_drop_column_compaction(self):
        session = self.prepare()
        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        node = self.cluster.nodelist()[0]
        node.flush()
        node.compact()

        # test that c1 values have been compacted away.
        session = self.patient_cql_connection(node)
        assert_all(session, "SELECT c1 FROM ks.cf", [[None], [None], [None], [4]], ignore_order=True)

    @pytest.mark.dtest_debug
    def test_drop_column_queries(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")
        # FIXME: ScyllaDB: Indexes not supported yet
        # session.execute("CREATE INDEX ON cf(c2)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        assert_all(session, "SELECT c1 FROM cf", [[None], [None], [None], [4]], ignore_order=True)
        assert_all(session, "SELECT * FROM cf", [[0, None, 2], [1, None, 3], [2, None, 4], [3, 4, 5]], ignore_order=True)
        assert_one(session, "SELECT c1 FROM cf WHERE key = 0", [None])

        assert_one(session, "SELECT c1 FROM cf WHERE key = 3", [4])

        # FIXME: ScyllaDB: Indexes not supported yet
        # rows = session.execute("SELECT * FROM cf WHERE c2 = 2")
        # self.assertEqual([[0, None, 2]], rows_to_list(rows))

        # FIXME: ScyllaDB: Indexes not supported yet
        # rows = session.execute("SELECT * FROM cf WHERE c2 = 5")
        # self.assertEqual([[3, 4, 5]], rows_to_list(rows))

    # cluster_topology is a dict with dc, rack and nodes per rack
    # rf may be given as an int, or a dictionary stating number of replicas per dc
    def prepare(self, cluster_topology: dict[str, dict[str, int]] | None = None, rf: int | dict[str, int] | None = None):
        cluster = self.cluster
        cluster_topology = cluster_topology or generate_cluster_topology()
        rf = rf or 1
        cluster.populate(cluster_topology).start(wait_other_notice=True, wait_for_binary_proto=True)
        nodes = cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])
        # It is forbidden to re-add a column with client-side timestamps
        session.use_client_timestamp = False
        create_ks(session, "ks", rf)
        return session

    # Reproducer for https://github.com/scylladb/scylla/issues/2623
    def test_restart_with_large_tables(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={"max_cached_partition_size_in_kb": 1})
        session = self.prepare()

        n_tables = 100
        col_name = "a" * 1024
        session.execute("USE ks")
        cmds = [(f"CREATE TABLE cf_{i} (key int PRIMARY KEY, {col_name} int)", ()) for i in range(n_tables)]

        nodes = cluster.nodelist()
        execute_concurrent(session, cmds, raise_on_first_error=True, concurrency=10)

        nodes[0].stop()
        nodes[0].start()

        session = self.patient_cql_connection(nodes[0])
        session.execute("select * from ks.cf_0")
        session.execute(f"select * from ks.cf_{n_tables - 1}")

    def test_alter_keyspace_then_table_with_udt(self):
        """
        This subtest is used to reproduce issue https://github.com/scylladb/scylla-enterprise/issues/2989
        The sequence of schema changes is this:

        Create keyspace ks
        Add UDT "my_type"
        Create table "cf"
        Alter keyspace replication-factor
        Alter table "cf" (change compaction strategy option) <- this used to fail on multiple nodes,
         which are missing the type.
        Failure example:
        E   cassandra.InvalidRequest: Error from server: code=2200 [Invalid query] message="Unknown type ks.my_type"
        """
        cluster_topology = generate_cluster_topology(rack_num=2)
        with self.prepare(cluster_topology, rf={"datacenter1": 1}) as session:
            keyspace_name = "ks"
            udt_name = "my_type"
            session.execute(f"CREATE TYPE IF NOT EXISTS {keyspace_name}.{udt_name} (md5 text, basename text)")
            stcs_compaction = {"class": CompactionStrategy.SIZE_TIERED.value}
            table_name = f"{keyspace_name}.cf"
            create_cf(session=session, name=table_name, compaction=stcs_compaction, columns={"c1": "text", "c2": f"{keyspace_name}.{udt_name} "})
            rf = get_replication_options(session, keyspace_name)["datacenter1"]
            rf = ["rack1", "rack2"] if type(rf) is list else 2
            session.execute(f"ALTER KEYSPACE {keyspace_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': {describe_rf(rf)}}};")
            lcs_compaction = {"class": CompactionStrategy.LEVELED.value, "sstable_size_in_mb": 1}

            session.execute(f"ALTER TABLE {table_name} WITH compaction={lcs_compaction}")
