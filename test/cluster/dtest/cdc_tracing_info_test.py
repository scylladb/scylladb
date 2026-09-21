import logging
from uuid import uuid4

import pytest

from dtest_class import Tester, create_ks
from tools.cdc_utils import CDCInitializeHelper, CdcLogOperations, CDCTraceInfoMatcher

logger = logging.getLogger(__name__)


@pytest.mark.single_node
@pytest.mark.scylla_cdc
@pytest.mark.dtest_full
class TestCDCTraceInfo(Tester, CDCInitializeHelper):
    keyspace = "ks"
    table = "cf"
    table_cdc_log = f"{table}_scylla_cdc_log"

    def prepare_cluster_and_schema(  # noqa: PLR0913
        self,
        num_nodes=1,
        rf=1,
        value_type="text",
        preimage_enable=False,
        postimage_enable=False,
        primary_key_type=None,
    ):
        self.populate_sequentially(num_nodes, wait_other_notice=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_schema_with_cdc(session, rf=rf, value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        return (node, session)

    def create_schema_with_cdc(
        self,
        session,
        rf=1,
        value_type="text",
        preimage_enable=False,
        postimage_enable=False,
    ):
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
        session.execute(f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf}}}")
        create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def test_tracing_insert_native_type(self):
        self.check_tracing_info_for_operation()

    def test_tracing_insert_native_type_preimage(self):
        self.check_tracing_info_for_operation(preimage_enable=True)

    def test_tracing_insert_native_type_postimage(self):
        self.check_tracing_info_for_operation(postimage_enable=True)

    def test_tracing_insert_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(preimage_enable=True, postimage_enable=True)

    def test_tracing_update_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE)

    def test_tracing_update_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True)

    def test_tracing_delete_partition_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, expect_splitting=True)

    def test_tracing_delete_partition_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, preimage_enable=True, postimage_enable=True, expect_splitting=True)

    def test_tracing_delete_row_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE)

    def test_tracing_insert_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type="list<text>", expect_splitting=True)

    def test_tracing_insert_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type="list<text>", preimage_enable=True, postimage_enable=True, expect_splitting=True)

    def test_tracing_update_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type="list<text>")

    def test_tracing_update_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type="list<text>", preimage_enable=True, postimage_enable=True)

    def test_tracing_delete_partition_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type="list<text>", expect_splitting=True)

    def test_tracing_delete_partition_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type="list<text>", preimage_enable=True, postimage_enable=True, expect_splitting=True)

    def test_tracing_delete_row_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type="list<text>")

    def test_tracing_delete_row_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type="list<text>", preimage_enable=True, postimage_enable=True)

    def test_tracing_delete_row_range_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type="list<text>")

    def test_tracing_delete_row_range_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type="list<text>", preimage_enable=True, postimage_enable=True)

    def test_tracing_info_for_batch_insert_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type="text", use_batch=True)

    def test_tracing_info_for_batch_insert_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type="text", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_batch_insert_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type="list<text>", use_batch=True, expect_splitting=True)

    def test_tracing_info_for_batch_insert_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type="list<text>", preimage_enable=True, postimage_enable=True, use_batch=True, expect_splitting=True)

    def test_tracing_info_for_native_type_batch_update_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type="text", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_batch_update_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type="list<text>", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_partition_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type="text", preimage_enable=True, postimage_enable=True, use_batch=True, expect_splitting=True)

    def test_tracing_info_for_batch_delete_partition_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type="list<text>", preimage_enable=True, postimage_enable=True, use_batch=True, expect_splitting=True)

    def test_tracing_info_for_batch_delete_row_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type="text", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_batch_delete_row_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type="list<text>", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_exclusive_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type="text", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_exclusive_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type="list<text>", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_inclusive_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_INCLUSIVE, value_type="text", preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_inclusive_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_INCLUSIVE, value_type="list<text>", preimage_enable=True, postimage_enable=True, use_batch=True)

    def check_tracing_info_for_operation(  # noqa: PLR0913
        self,
        operation=CdcLogOperations.INSERT,
        value_type="text",
        preimage_enable=False,
        postimage_enable=False,
        use_batch=False,
        expect_splitting=False,
    ):
        self.prepare_cluster_and_schema(value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        cql_command = self.prepare_cql_with_tracing(session, operation, value_type, use_batch)
        # get tokens of partitions before delete operation
        token_ids = self.get_tokens(session)

        result = node.run_cqlsh(cmds=cql_command, return_output=True)

        # if operation insert/update, table was empty before executing cql_command
        if not token_ids:
            token_ids = self.get_tokens(session)

        trace_matcher = CDCTraceInfoMatcher(tokens=token_ids, splitting=expect_splitting, preimage=preimage_enable, postimage=postimage_enable)
        trace_matcher.verify_cdc_trace_info(output=result[0])

    def prepare_cql_with_tracing(self, session, operation, value_type="text", use_batch=False):
        num_of_partitions = 2 if use_batch else 1
        if operation == CdcLogOperations.INSERT:
            cql_command = self.get_insert_query(value_type, use_batch)
        elif operation == CdcLogOperations.UPDATE:
            cql_command = self.get_update_query(value_type, use_batch)
        elif operation == CdcLogOperations.PARTITION_DELETE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_partition_query(use_batch)
        elif operation == CdcLogOperations.ROW_DELETE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_row_query(use_batch)
        elif operation == CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_row_range_bound_exclusive_query(use_batch)
        elif operation == CdcLogOperations.RANGE_DELETE_END_INCLUSIVE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_row_range_bound_inclusive_query(use_batch)

        return cql_command

    def get_insert_query(self, value_type, use_batch=False):
        value = self.generate_value(value_type)
        if use_batch:
            ops = ""
            for i in range(5):
                ops += f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) VALUES ({i}, 1, {value});"

            cql_command = f"""TRACING ON; \
                              BEGIN BATCH
                                {ops}
                              APPLY BATCH;"""
        else:
            cql_command = f"""TRACING ON; \
                                INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) \
                                  VALUES (1, 1, {value});
                               """
        return cql_command

    def get_update_query(self, value_type, use_batch=False):
        value = self.generate_value(value_type)
        if use_batch:
            ops = ""
            for i in range(5):
                ops += f"UPDATE {self.keyspace}.{self.table} SET value = {value} WHERE pkey = {i} and ckey = 1;"
            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                    {ops}
                              APPLY BATCH;"""
        else:
            cql_command = f"""TRACING ON;
                              UPDATE {self.keyspace}.{self.table} SET value = {value} \
                                    WHERE pkey = 1 and ckey = 1;
                """
        return cql_command

    def get_delete_partition_query(self, use_batch=False):
        if use_batch:
            ops = ""
            for i in range(2):
                ops += f"DELETE FROM {self.keyspace}.{self.table} WHERE pkey = {i};"

            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                {ops}
                              APPLY BATCH;"""
        else:
            cql_command = f"""TRACING ON;
                              DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0;"""
        return cql_command

    def get_delete_row_query(self, use_batch=False):
        if use_batch:
            ops = ""
            for i in range(10):
                ops += f"DELETE FROM {self.keyspace}.{self.table} WHERE pkey = {i % 2} and ckey = {i % 5};"

            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                {ops}
                              APPLY BATCH;
                          """
        else:
            cql_command = f"""TRACING ON;
                          DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey = 1;"""
        return cql_command

    def get_delete_row_range_bound_exclusive_query(self, use_batch):
        if use_batch:
            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 1 and ckey > 1 and ckey < 4;
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey > 1 and ckey < 4;
                              APPLY BATCH;
                            """
        else:
            cql_command = f"""TRACING ON;
                          DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey >1 and ckey< 4;"""

        return cql_command

    def get_delete_row_range_bound_inclusive_query(self, use_batch):
        if use_batch:
            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 1 and ckey >= 1 and ckey <= 4;
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey >= 1 and ckey <= 4;
                              APPLY BATCH;
                            """
        else:
            cql_command = f"""TRACING ON;
                          DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey >=1 and ckey <= 4;"""

        return cql_command

    def generate_partitions_with_5_rows(self, session, value_type="text", p_num=2):
        value = self.generate_value(value_type)
        for i in range(p_num * 5):
            session.execute(f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) VALUES ({i % p_num}, {i % 5}, {value});")

    def get_tokens(self, session):
        base_rows = list(session.execute(f"SELECT token(pkey) as tkn from {self.keyspace}.{self.table}"))
        token_ids = {row.tkn for row in base_rows}
        return token_ids

    def generate_value(self, value_type="text"):
        if value_type in ["text", "varchar", "ascii"]:
            return f"'{uuid4()}'"
        elif "list" in value_type:
            return [str(uuid4()), str(uuid4())]
        else:
            return None
