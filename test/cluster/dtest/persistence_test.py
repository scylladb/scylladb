import logging
import time

import pytest

from dtest_class import Tester
from tools.data import print_table

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestPersistence(Tester):
    """
    Insert data into clusters, then restart them and verify if data persisted.
    """

    def restart_cluster(self):
        self.cluster.stop()
        time.sleep(0.5)
        self.cluster.start(wait_for_binary_proto=True)
        time.sleep(0.5)

    def prepare(self, nodes=1):
        cluster = self.cluster

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        return session

    def stress_with_col_size(self, size):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        node.stress(["write", "n=5", "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=1)", "-col", "n=fixed(1)", "size=fixed(%s)" % size, "-rate", "threads=1"])
        self.restart_cluster()
        node.stress(["read", "n=5", "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=1)", "-col", "n=fixed(1)", "size=fixed(%s)" % size, "-rate", "threads=1"])

    @pytest.mark.dtest_debug
    def test_persist_simple(self):
        """
        1) Create a 1 node cluster.
        2) Create a keyspace and a table with simple schema.
        3) Add data to the table.
        4) Query table and save the result.
        5) Restart the node.
        6) Query table, compare with previous result.
        """
        session = self.prepare()
        session.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        session.execute("CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, r1 int, PRIMARY KEY (p1, c1)) WITH compaction={'class': 'SizeTieredCompactionStrategy'}")
        session.execute("INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'a', 1)")
        session.execute("INSERT INTO ks.cf (p1, c1, r1) VALUES ('key2', 'b', 1)")
        res_before_restart = list(session.execute("SELECT * FROM ks.cf"))
        self.restart_cluster()
        session2 = self.prepare()
        res_after_restart = list(session2.execute("SELECT * FROM ks.cf"))

        msg_error = f"The data before restart ('{res_before_restart}') should be equal to data after restart ('{res_after_restart}')"
        assert res_before_restart == res_after_restart, msg_error

    @pytest.mark.use_cassandra_stress
    def test_persist_small_columns(self):
        """
        1) Create a 1 node cluster.
        2) Run cassandra-stress with column size=1 (read workload) to
           populate the DB.
        3) Restart the node.
        4) Run a read workload, to see if data was persisted.
        """
        self.stress_with_col_size(1)

    def test_data_persistence_of_map_with_empty_keys(self):
        """
        https://github.com/scylladb/scylla-enterprise/issues/909
        Data loss issue with "map" type and empty key in the map.

        1) CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'} AND
            durable_writes = true;
        2) CREATE TABLE ks.user_stats3 ( user_id text PRIMARY KEY, clients_usage map<text, text>, last_seen timestamp );
        3) Insert into ks.user_stats3(user_id, clients_usage) values ('Piotr', {'':'2019-05-05T04:14:16.954407'});
        4) Select * from ks.user_stats3;
        5) docker exec -it scyllaU nodetool flush
        6) docker exec -it scyllaU nodetool compact
        7) Reboot the cluster
        8) Select * from ks.user_stats3;
        """
        keyspace_name = "keyspace1"
        table_name = f"{keyspace_name}.user_stats3"
        keyspace_cmd = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}  AND durable_writes = true;" % keyspace_name
        new_table_cmd = f"CREATE TABLE {table_name} ( user_id text PRIMARY KEY, clients_usage map<text, text>, last_seen timestamp );"
        add_new_row_cmd = "Insert into {} (user_id, clients_usage) values ('{}', {});"
        show_table_cmd = f"Select * from {table_name};"

        logger.debug("Opening CQL session")
        session = self.prepare()
        logger.debug(f"Creating new table '{table_name}'")
        session.execute(keyspace_cmd)
        session.execute(new_table_cmd)
        session.execute(add_new_row_cmd.format(table_name, "Piotr", {"": "2019-05-05T04:14:16.954407"}))
        logger.info(f"Showing table '{table_name}' data")
        table_before_reboot = session.execute(show_table_cmd)
        row_before_reboot = table_before_reboot.current_rows[0]
        print_table(table=table_before_reboot)
        node = self.cluster.nodelist()[0]
        logger.debug("Executing flush")
        node.flush()
        logger.debug("Executing compact")
        node.compact()
        logger.debug("Rebooting the cluster")
        self.restart_cluster()
        logger.debug("Opening CQL session after rebooting")
        session = self.prepare()
        table_after_reboot = session.execute(show_table_cmd)
        row_after_reboot = table_after_reboot.current_rows[0]
        print_table(table=table_after_reboot)
        msg_error = f"The data before reboot ('{row_before_reboot}') should be equal to data after reboot ('{row_after_reboot}')"
        assert row_before_reboot == row_after_reboot, msg_error
