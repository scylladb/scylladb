import os
import pprint
import shutil

import pytest
from cassandra.cluster import Session, SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.scylla_cluster import ScyllaNode

from cdc_test import CDCInitializeHelper
from dtest_class import Tester, create_ks
from tools.snapshots import make_snapshot, restore_snapshot_with_refresh

PP = pprint.PrettyPrinter(indent=2)


@pytest.mark.dtest_full
@pytest.mark.scylla_cdc
@pytest.mark.single_node
class TestCDCSnapshotOperation(Tester, CDCInitializeHelper):
    """To restore cdc log table from snapshot should be used only operation with refresh"""

    keyspace = "ks"
    table = "cf"
    table_cdc_log = f"{table}_scylla_cdc_log"

    def prepare_cluster_and_schema(
        self,
        num_nodes=1,
        rf=1,
        value_type="text",
        preimage_enable=False,
        postimage_enable=False,
    ):
        self.cluster.populate(num_nodes)
        self.cluster.set_configuration_options(values={"experimental_features": ["cdc"]})
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_schema_with_cdc(session, rf=rf, value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        return (node, session)

    def create_schema_with_cdc(self, session, rf=1, value_type="text", preimage_enable=False, postimage_enable=False):
        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pkey int, \
                     ckey int, \
                     value {value_type}, \
                     PRIMARY KEY (pkey, ckey)\
                    )"
        statement += " WITH cdc={'enabled': true"
        if preimage_enable:
            statement += ", 'preimage': true"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += "}"
        session.execute(f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf} }}")
        create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def drop_keyspaces_and_clear_files(self, session, ks, node):
        session.execute(f"DROP KEYSPACE {ks}")
        shutil.rmtree(os.path.join(node.get_path(), "data", ks))

    @property
    def insert_stm(self):
        return SimpleStatement(
            f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) \
                               VALUES (%(pkey)s, %(ckey)s, %(value)s )"
        )

    @property
    def update_stm(self):
        return SimpleStatement(
            f"UPDATE {self.keyspace}.{self.table} SET value = %(value)s \
                               WHERE pkey = %(pkey)s and ckey = %(ckey)s"
        )

    @property
    def delete_stm(self):
        return SimpleStatement(
            f"DELETE FROM {self.keyspace}.{self.table} \
                               WHERE pkey = %(pkey)s and ckey = %(ckey)s"
        )

    def test_create_snapshot_with_native_type_without_base_rows_delete(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="text")

    def test_create_snapshot_with_native_type_with_base_rows_delete(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="text", with_delete_rows=True)

    def test_create_snapshot_with_native_type_without_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="varchar", preimage_enable=True)

    def test_create_snapshot_with_native_type_with_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="varchar", preimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_native_type_without_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="varchar", postimage_enable=True)

    def test_create_snapshot_with_native_type_with_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="varchar", postimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_native_type_without_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="ascii", preimage_enable=True, postimage_enable=True)

    @pytest.mark.next_gating
    def test_create_snapshot_with_native_type_with_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="ascii", preimage_enable=True, postimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_list_without_base_rows_delete_type(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<text>")

    def test_create_snapshot_with_collection_list_with_base_rows_delete_type(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<text>", with_delete_rows=True)

    def test_create_snapshot_with_collection_list_without_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<varchar>", preimage_enable=True)

    def test_create_snapshot_with_collection_list_with_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<varchar>", preimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_list_without_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<varchar>", postimage_enable=True)

    def test_create_snapshot_with_collection_list_with_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<varchar>", postimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_list_without_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<ascii>", preimage_enable=True, postimage_enable=True)

    def test_create_snapshot_with_collection_list_with_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="list<ascii>", with_delete_rows=True, preimage_enable=True, postimage_enable=True)

    def test_create_snapshot_with_collection_set_without_base_rows_delete_type(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<text>")

    def test_create_snapshot_with_collection_set_with_base_rows_delete_type(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<text>", with_delete_rows=True)

    def test_create_snapshot_with_collection_set_without_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<varchar>", preimage_enable=True)

    def test_create_snapshot_with_collection_set_with_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<varchar>", preimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_set_without_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<varchar>", postimage_enable=True)

    def test_create_snapshot_with_collection_set_with_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<varchar>", postimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_set_without_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<ascii>", preimage_enable=True, postimage_enable=True)

    def test_create_snapshot_with_collection_set_with_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="set<ascii>", with_delete_rows=True, preimage_enable=True, postimage_enable=True)

    def test_create_snapshot_with_collection_map_without_base_rows_delete_type(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<int,text>")

    def test_create_snapshot_with_collection_map_with_base_rows_delete_type(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<int,text>", with_delete_rows=True)

    def test_create_snapshot_with_collection_map_without_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<bigint,varchar>", preimage_enable=True)

    def test_create_snapshot_with_collection_map_with_base_rows_delete_preimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<bigint,varchar>", preimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_map_without_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<smallint,varchar>", postimage_enable=True)

    def test_create_snapshot_with_collection_map_with_base_rows_delete_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<smallint,varchar>", postimage_enable=True, with_delete_rows=True)

    def test_create_snapshot_with_collection_map_without_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<tinyint,ascii>", preimage_enable=True, postimage_enable=True)

    def test_create_snapshot_with_collection_map_with_base_rows_delete_preimage_postimage(self):
        self.workflow_with_restore_snapshot_with_refresh(value_type="map<tinyint,ascii>", with_delete_rows=True, preimage_enable=True, postimage_enable=True)

    def workflow_with_restore_snapshot_with_refresh(self, value_type="text", with_delete_rows=False, preimage_enable=False, postimage_enable=False):
        self.prepare_cluster_and_schema(value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        node: ScyllaNode = self.cluster.nodelist()[0]
        session: Session = self.patient_cql_connection(node)

        self.populate_base_table(session, value_type, with_delete_rows)
        node.flush()
        base_rows = self.get_base_rows(session)
        log_rows = self.get_log_rows(session)

        snapshot_dir = make_snapshot(node, ks=self.keyspace, name="basic")

        self.drop_keyspaces_and_clear_files(session, self.keyspace, node)

        self.create_schema_with_cdc(session, value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        restore_snapshot_with_refresh(snapshot_dir, node, self.keyspace, self.table, name="basic")
        restored_base_rows = self.get_base_rows(session)
        assert base_rows == restored_base_rows, "Base table rows are differs"
        no_restored_log_rows = self.get_log_rows(session)
        assert no_restored_log_rows == []

        restore_snapshot_with_refresh(snapshot_dir, node, self.keyspace, self.table_cdc_log, name="basic")

        restored_log_rows = self.get_log_rows(session)
        assert len(log_rows) == len(restored_log_rows)
        assert log_rows == restored_log_rows

    def populate_base_table(self, session, value_type="text", delete_rows=False):
        if value_type in ["text", "varchar", "ascii"]:
            self.populate_base_table_with_native_type(session, delete_rows)
        elif "list" in value_type:
            self.populate_base_table_with_collection_list_type(session, delete_rows)
        elif "set" in value_type:
            self.populate_base_table_with_collection_set_type(session, delete_rows)
        elif "map" in value_type:
            self.populate_base_table_with_collection_map_type(session, delete_rows)
        else:
            pytest.fail("The assigned value_type isn't valid")

    def populate_base_table_with_native_type(self, session, delete_rows=False):
        insert_values = [{"pkey": i % 10, "ckey": i, "value": f"{i}"} for i in range(100)]
        update_values = [{"pkey": i % 10, "ckey": i, "value": f"new_{i}"} for i in range(100)]
        delete_values = [{"pkey": i % 10, "ckey": i} for i in range(100)]

        self._execute_queries(session, insert_values, update_values, delete_values, delete_rows)

    def populate_base_table_with_collection_list_type(self, session, delete_rows=False):
        insert_values = [{"pkey": i % 10, "ckey": i, "value": [f"{i}"]} for i in range(100)]
        update_values = [{"pkey": i % 10, "ckey": i, "value": [f"new_{i}"]} for i in range(100)]
        delete_values = [{"pkey": i % 10, "ckey": i} for i in range(100)]

        self._execute_queries(session, insert_values, update_values, delete_values, delete_rows)

    def populate_base_table_with_collection_map_type(self, session, delete_rows=False):
        insert_values = [{"pkey": i % 10, "ckey": i, "value": {i: f"{i}"}} for i in range(100)]
        update_values = [{"pkey": i % 10, "ckey": i, "value": {i: f"new_{i}"}} for i in range(100)]
        delete_values = [{"pkey": i % 10, "ckey": i} for i in range(100)]

        self._execute_queries(session, insert_values, update_values, delete_values, delete_rows)

    def populate_base_table_with_collection_set_type(self, session, delete_rows=False):
        insert_values = [{"pkey": i % 10, "ckey": i, "value": set(f"{i}")} for i in range(100)]
        update_values = [{"pkey": i % 10, "ckey": i, "value": set(f"new_{i}")} for i in range(100)]
        delete_values = [{"pkey": i % 10, "ckey": i} for i in range(100)]

        self._execute_queries(session, insert_values, update_values, delete_values, delete_rows)

    def _execute_queries(self, session, insert_dataset, update_dataset, delete_dataset, delete_rows):
        execute_concurrent_with_args(session, self.insert_stm, insert_dataset)
        execute_concurrent_with_args(session, self.update_stm, update_dataset)
        if delete_rows:
            execute_concurrent_with_args(session, self.delete_stm, delete_dataset)
        execute_concurrent_with_args(session, self.update_stm, update_dataset)

    def get_base_rows(self, session):
        return list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table}"))

    def get_log_rows(self, session):
        return list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table_cdc_log}"))
