import os
import shutil

import pytest

from dtest_class import Tester
from tools.files import get_node_cf_dir


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.single_node
@pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
class TestCompactStorage(Tester):
    row_size = 1000

    def load_and_read_from_sstables(self, data_dir, lines):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.tb (key1 int, key2 int, val blob, PRIMARY KEY (key1,key2)) WITH COMPACT STORAGE;")

        node1.stop()

        dst1 = get_node_cf_dir(node1, "ks", "tb")
        src1 = os.path.join("test_data", data_dir)

        shutil.copytree(src1, dst1, dirs_exist_ok=True)

        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)

        result = list(session.execute("SELECT key2 FROM ks.tb"))

        assert len(result) == lines

    def test_read_old_format_wide_row_data(self):
        self.load_and_read_from_sstables("scylla_compact_storage_wide_partition_old_format", self.row_size - 200)

    @pytest.mark.dtest_debug
    def test_read_cassandra_wide_row_data(self):
        self.load_and_read_from_sstables("cassandra_compact_storage_wide_partition", self.row_size - 100)

    @pytest.mark.dtest_debug
    def test_wide_row(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.tb (key1 int, key2 int, val blob, PRIMARY KEY (key1,key2)) WITH COMPACT STORAGE;")

        blob = "a" * 10000

        insert = session.prepare("INSERT INTO ks.tb (key1,key2,val) values (1,?,textAsBlob(?));")
        for key2 in range(self.row_size):
            session.execute(insert, (key2, blob))

        node1.flush()
        for key2 in range(100):
            session.execute("delete from ks.tb where key1=1 and key2 = %d" % (key2 * 10))

        result = list(session.execute("SELECT * FROM ks.tb"))
        assert len(result) == self.row_size - 100

        node1.flush()
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        result = list(session.execute("SELECT * FROM ks.tb"))
        assert len(result) == self.row_size - 100
