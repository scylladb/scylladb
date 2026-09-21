import logging
import pprint
import re
import time

import pytest
from cassandra.cluster import Session, SimpleStatement
from ccmlib.scylla_node import ScyllaNode

from cdc_test import CDCInitializeHelper, CdcLogOperations
from dtest_class import Tester, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.cluster import new_node
from tools.misc import ImmutableMapping

PP = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)


@pytest.mark.scylla_cdc
@pytest.mark.dtest_full
class TestCDCTTLFunctionality(Tester, CDCInitializeHelper):
    keyspace = "ks"
    table = "cf"
    log_table = f"{table}_scylla_cdc_log"

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        if not dtest_config.is_scylla:
            pytest.skip("CDC tests are intended for Scylla only")
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"experimental_features": ["cdc"]})
        return dtest_setup_overrides

    def prepare_cluster_and_schema(  # noqa: PLR0913
        self,
        num_nodes=1,
        rf=1,
        preimage_enable=False,
        postimage_enable=False,
        cdc_ttl=30,
        value_type="text",
    ):
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        for i in range(1, num_nodes):
            node = new_node(self.cluster, bootstrap=True)
            node.start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_schema_with_cdc(session, rf=rf, preimage_enable=preimage_enable, postimage_enable=postimage_enable, cdc_ttl=cdc_ttl, value_type=value_type)

        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        return (node, session)

    def create_schema_with_cdc(self, session, rf=1, preimage_enable=False, postimage_enable=False, cdc_ttl=30, value_type="text"):  # noqa: PLR0913
        """Create column family with cdc enabled

        Create table with cdc enabled
        method overrides CDCInitializeHelper.create_schema_with_cdc

        Arguments:
            session {Session} -- opened session to scylla cluster

        Keyword Arguments:
            rf {number} -- replication factor for keyspace (default: {1})
            preimage_enable {bool} -- enable preimage mode (default: {False})
            postimage_enable {bool} -- enable postimage mode (default: {False})
            primary_key_type {[type]} -- type for primary key (default: {None})
        """
        cdc_properties = self.build_cdc_properties(preimage_enable, postimage_enable, cdc_ttl)

        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pkey bigint, \
                     ckey int, \
                     cval1 {value_type}, \
                     cval2 {value_type}, \
                     PRIMARY KEY (pkey, ckey)\
                    ) WITH {cdc_properties}"

        session.execute(f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf}}}")
        create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    @staticmethod
    def build_cdc_properties(preimage_enable, postimage_enable, cdc_ttl):
        statement = "cdc={'enabled': true"
        if preimage_enable:
            statement += ", 'preimage': true"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += f", 'ttl': {cdc_ttl} }}"

        return statement

    def check_cdc_ttl_column_value_in_cdc_log_table(self, preimage_enable=False, postimage_enable=False, base_ttl=None, value_type="text"):
        """Main test flow to validate that each row in cdc log table
        has valid value in cdc$ttl column

        1. Create a cluster with defined cdc_ttl parameter
        2. run data manipulation for base table with ttl
        3. get rows from cdc log table
        4. verify that each row in cdc log table has correct value

        Keyword Arguments:
            preimage_enable {bool} -- enable preimage for cdc log table (default: {False})
            postimage_enable {bool} -- enable postimage for cdc log table (default: {False})
            base_ttl {[type]} -- ttl using in query to base table (default: {None})
            value_type {str} -- data type for columns (not primary key) in base table (default: {'text'})
        """

        node: ScyllaNode = None
        session: Session = None
        _node, session = self.prepare_cluster_and_schema(value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        # cdc log table will contain only first records (no preimage if enabled)
        self.populate_base_table(session, CdcLogOperations.INSERT, value_type, updating_column="cval1", ttl=base_ttl)
        self.populate_base_table(session, CdcLogOperations.INSERT, value_type, updating_column="cval2", ttl=base_ttl)

        cdc_log_rows = self.get_log_rows(session)

        self.verify_log_row_ttl(cdc_log_rows, base_ttl)

        # cdc log table will contain all rows (preimage if enabled)

        self.populate_base_table(session, CdcLogOperations.UPDATE, value_type, updating_column="cval1", ttl=base_ttl)
        self.populate_base_table(session, CdcLogOperations.UPDATE, value_type, updating_column="cval2", ttl=base_ttl)

        cdc_log_rows = self.get_log_rows(session)

        self.verify_log_row_ttl(cdc_log_rows, base_ttl)

    def test_cdc_ttl_column_value_for_native_type(self):
        self.check_cdc_ttl_column_value_in_cdc_log_table(base_ttl=30)

    def test_cdc_ttl_column_value_for_native_type_preimage_postimage(self):
        self.check_cdc_ttl_column_value_in_cdc_log_table(preimage_enable=True, postimage_enable=True, base_ttl=30)

    def test_cd_cttl_column_value_for_collection_type(self):
        self.check_cdc_ttl_column_value_in_cdc_log_table(base_ttl=30, value_type="text")

    def test_cdc_ttl_column_value_for_collection_preimage_postimage(self):
        self.check_cdc_ttl_column_value_in_cdc_log_table(preimage_enable=True, postimage_enable=True, base_ttl=30, value_type="list<text>")

    def check_rows_cleared_after_ttl_expired_for_operation(  # noqa: PLR0913
        self,
        operation=CdcLogOperations.INSERT,
        value_type="text",
        cdc_ttl=30,
        base_ttl=None,
        preimage_enable=False,
        postimage_enable=False,
    ):
        """Validate rows are cleared according ttl expiring

        Main flow for tests:
        1. create cluster with cdc feature
        2. create keyspaces and table with enabled cdc with appropriate parameters and configured ttl
        3. populate base table with/without ttl for operation
        4. validate:
            - Validate that if cdc_ttl expired, all rows will removed from cdc log table
            - Validate that if base_ttl is expired, rows removed from base table,
              rows in cdc log table are not cleared and no new entry
            - Validate that rows removed in base table and cdc log table according certain ttl

        Keyword Arguments:
            operation {[type]} -- operation to use for data manipulation over base table (default: {CdcLogOperations.INSERT})
            value_type {str} -- data type for columns (not primary key) in base table (default: {'text'})
            cdc_ttl {number} -- set cdc_ttl for cdc log table (default: {30})
            base_ttl {[type]} -- ttl using in query to base table (default: {None})
            preimage_enable {bool} -- enable preimage for cdc log table (default: {False})
            postimage_enable {bool} -- enable postimage for cdc log table (default: {False})
        """
        node: ScyllaNode = None
        session: Session = None
        _node, session = self.prepare_cluster_and_schema(cdc_ttl=cdc_ttl, value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        self.populate_base_table(session, operation, value_type, updating_column="cval1", ttl=base_ttl)
        base_table_rows = self.get_base_rows(session)
        log_table_rows = self.get_log_rows(session)

        # validate that base table and log table are not empty
        assert len(base_table_rows) > 0
        assert len(log_table_rows) > 0

        if base_ttl and cdc_ttl > base_ttl:
            # if row in base table have ttl less than cdc log table
            # validate that base table was cleared and cdc log table not
            # and not new records added to base log_table
            logger.debug(f"Wait for {base_ttl}")
            time.sleep(base_ttl)

            expired_base_table_rows = self.get_base_rows(session)
            assert expired_base_table_rows == []

            not_expired_log_table_rows = self.get_log_rows(session)
            assert not_expired_log_table_rows == log_table_rows

            # wait rest of time to check that cdc log table is cleared
            rest_timeout = cdc_ttl - base_ttl
            time.sleep(rest_timeout)
        elif base_ttl and cdc_ttl < base_ttl:
            # if row in base table have ttl greater than cdc log table
            # check that base table is not empty and cdc log table cleared
            logger.debug(f"Wait for {cdc_ttl}")
            time.sleep(cdc_ttl)
            expired_log_table_rows = self.get_log_rows(session)
            assert expired_log_table_rows == []

            not_expired_base_table_rows = self.get_base_rows(session)
            assert not_expired_base_table_rows == base_table_rows

            # wait rest of time
            rest_timeout = base_ttl - cdc_ttl
            time.sleep(rest_timeout)
        else:
            # if mutation has not ttl or it is equal to configured cdc ttl
            logger.debug(f"Wait for {cdc_ttl}")
            time.sleep(cdc_ttl)

        expired_base_table_rows = self.get_base_rows(session)
        expired_log_table_rows = self.get_log_rows(session)

        if base_ttl:
            assert expired_base_table_rows == []
        else:
            assert base_table_rows == expired_base_table_rows

        assert expired_log_table_rows == []

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, cdc_ttl=30, value_type="text")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_with_preimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, preimage_enable=True, cdc_ttl=30, value_type="varchar")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_native_type_with_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, postimage_enable=True, cdc_ttl=30, value_type="ascii")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_with_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=30, value_type="ascii")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, cdc_ttl=30, value_type="text")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_preimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, cdc_ttl=30, value_type="varchar")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, postimage_enable=True, cdc_ttl=30, value_type="ascii")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=30, value_type="ascii")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, cdc_ttl=30, value_type="list<varchar>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_preimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, preimage_enable=True, cdc_ttl=30, value_type="list<text>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, postimage_enable=True, cdc_ttl=30, value_type="list<ascii>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=30, value_type="list<text>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, cdc_ttl=30, value_type="list<varchar>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_preimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, cdc_ttl=30, value_type="list<text>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, postimage_enable=True, cdc_ttl=30, value_type="list<ascii>")

    def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=30, value_type="list<text>")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_native_type(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, cdc_ttl=30, base_ttl=30, value_type="text")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_native_type_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=30, value_type="text")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_update_native_type(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, cdc_ttl=30, base_ttl=30, value_type="text")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_update_native_type_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=30, value_type="text")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_collection(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, cdc_ttl=30, base_ttl=30, value_type="list<text>")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_collection_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=30, value_type="list<text>")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_update_collection(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, cdc_ttl=30, base_ttl=30, value_type="list<text>")

    def test_base_and_log_tables_cleared_after_ttl_expired_for_update_collection_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=30, value_type="list<text>")

    def test_clear_base_table_by_expired_ttl_and_not_affect_log_table_for_insert_native_types(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(cdc_ttl=30, base_ttl=15, value_type="text")

    def test_clear_base_table_by_expired_ttl_and_not_affect_log_table_for_insert_native_types_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=15, value_type="text")

    def test_clear_base_table_by_expired_ttl_and__not_affect_log_table_for_update_collection(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, cdc_ttl=30, base_ttl=15, value_type="text")

    def test_clear_base_table_by_expired_ttl_and__not_affect_log_table_for_update_collection_preimage_postimage(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=15, value_type="text")

    def test_clear_log_table_by_ttl_and_not_affect_base_table_for_insert_native_type(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=45, value_type="text")

    def test_clear_log_table_by_ttl_and_not_affect_base_table_for_update_collection(self):
        self.check_rows_cleared_after_ttl_expired_for_operation(operation=CdcLogOperations.UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=30, base_ttl=45, value_type="list<text>")

    def check_clear_log_rows_according_altered_cdc_ttl(
        self,
        operation=CdcLogOperations.INSERT,
        preimage_enable=False,
        postimage_enable=False,
        cdc_ttl=30,
        value_type="text",
    ):
        """Validate that rows for cdc log table removed according cdc ttl parameter

        Main flow to validate that rows in cdc log table are cleared according cdc ttl parameter.
        If this cdc_ttl was changed, the newly added rows will removed according new cdc ttl

        Issue with altering the ttl, cdc is disabled: https://github.com/scylladb/scylla/issues/6475
        Workaround is used.

        Keyword Arguments:
            operation {[type]} -- operation to use for data manipulation over base table (default: {CdcLogOperations.INSERT})
            value_type {str} -- data type for columns (not primary key) in base table (default: {'text'})
            cdc_ttl {number} -- set cdc_ttl for cdc log table (default: {30})
            preimage_enable {bool} -- enable preimage for cdc log table (default: {False})
            postimage_enable {bool} -- enable postimage for cdc log table (default: {False})
        """
        node: ScyllaNode = None
        session: Session = None
        new_cdc_ttl = cdc_ttl + 30
        node, session = self.prepare_cluster_and_schema(value_type=value_type, cdc_ttl=cdc_ttl, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        # add data to base table
        self.populate_base_table(session, operation, value_type, updating_column="cval1")
        log_rows_with_first_ttl = self.get_log_rows(session)
        assert len(log_rows_with_first_ttl) > 0

        # change cdc ttl with new value + 30
        self.alter_base_table_with_cdc_ttl(session, new_cdc_ttl, preimage_enable, postimage_enable)
        self.verify_cdc_ttl_configured(node, expected_cdc_ttl=new_cdc_ttl)

        # update base table with new data
        self.populate_base_table(session, operation, value_type, updating_column="cval2")

        log_rows_with_old_new_ttl = self.get_log_rows(session)
        assert len(log_rows_with_old_new_ttl) > len(log_rows_with_first_ttl)

        logger.debug(f"Wait for {cdc_ttl}")
        time.sleep(cdc_ttl)

        # get left rows from cdc log table
        not_expired_log_rows = self.get_log_rows(session)

        # validate that rows with old ttl are not in table
        for row in log_rows_with_first_ttl:
            assert row not in not_expired_log_rows

        # validate that rows with new ttl are same
        # as berfore rows with first ttl were cleaned
        for row in not_expired_log_rows:
            assert row in log_rows_with_old_new_ttl

        # wait rest of time
        time.sleep(new_cdc_ttl - cdc_ttl)

        expired_log_rows = self.get_log_rows(session)
        assert expired_log_rows == []

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_native_type(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.INSERT, cdc_ttl=30, value_type="varchar")

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_native_type_preimage_postimage(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.INSERT, cdc_ttl=30, value_type="varchar", preimage_enable=True, postimage_enable=True)

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_native_type(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.UPDATE, cdc_ttl=30, value_type="varchar")

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_native_type_preimage_postimage(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.UPDATE, cdc_ttl=30, value_type="varchar", preimage_enable=True, postimage_enable=True)

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_collection(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.INSERT, cdc_ttl=30, value_type="list<varchar>")

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_collection_preimage_postimage(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.INSERT, cdc_ttl=30, value_type="list<varchar>", preimage_enable=True, postimage_enable=True)

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_collection(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.UPDATE, cdc_ttl=30, value_type="list<varchar>")

    def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_collection_preimage_postimage(self):
        self.check_clear_log_rows_according_altered_cdc_ttl(operation=CdcLogOperations.UPDATE, cdc_ttl=30, value_type="list<varchar>", preimage_enable=True, postimage_enable=True)

    def get_insert_stm(self, col="cval1", ttl=None):
        stm = f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, {col}) \
                VALUES (%(pkey)s, %(ckey)s, %({col})s)"
        if ttl:
            stm += f" USING TTL {ttl}"
        return SimpleStatement(stm)

    def get_update_stm(self, column="cval1", ttl=None):
        using_ttl = f"USING TTL {ttl}" if ttl else ""

        stm = f"UPDATE {self.keyspace}.{self.table} {using_ttl} \
               SET {column} = %({column})s \
               WHERE pkey = %(pkey)s and ckey = %(ckey)s"

        return SimpleStatement(stm)

    def populate_base_table(
        self,
        session,
        operation=CdcLogOperations.INSERT,
        value_type="text",
        updating_column="cval1",
        ttl=None,
    ):
        if operation == CdcLogOperations.INSERT:
            self.insert_rows_to_base_table(session, value_type, inserting_column=updating_column, ttl=ttl)
        elif operation == CdcLogOperations.UPDATE:
            self.update_rows_in_base_table(session, value_type, updating_column, ttl)

    def verify_log_row_ttl(self, log_rows, expected_ttl):
        for row in log_rows:
            if row.cdc_operation not in [0, 9] and (not row.cdc_deleted_cval1 and not row.cdc_deleted_cval2):
                assert row.cdc_ttl == expected_ttl, row

    def insert_rows_to_base_table(self, session, value_type, inserting_column="cval1", ttl=None):
        if value_type in ["text", "varchar", "ascii"]:
            self.insert_rows_to_base_table_with_text_value(session, inserting_column, ttl)
        elif "list" in value_type:
            self.insert_rows_to_base_table_with_list_value(session, inserting_column, ttl)

    def insert_rows_to_base_table_with_text_value(self, session, inserting_column="cval1", ttl=None):
        for i in range(10):
            session.execute(self.get_insert_stm(inserting_column, ttl), {"pkey": i % 2, "ckey": i, inserting_column: f"text{i}"})

    def insert_rows_to_base_table_with_list_value(self, session, inserting_column="cval1", ttl=None):
        for i in range(10):
            session.execute(self.get_insert_stm(inserting_column, ttl), {"pkey": i % 2, "ckey": i, inserting_column: [f"text{i}", f"text-{i}"]})

    def update_rows_in_base_table(self, session, value_type, updating_column="cval1", ttl=None):
        if value_type in ["text", "varchar", "ascii"]:
            self.update_rows_in_base_table_with_text_value(session, updating_column, ttl)
        elif "list" in value_type:
            self.update_rows_in_base_table_with_list_value(session, updating_column, ttl)

    def update_rows_in_base_table_with_text_value(self, session, updating_column="cval1", ttl=None):
        for i in range(10):
            session.execute(self.get_update_stm(column=updating_column, ttl=ttl), {"pkey": i % 2, "ckey": i, updating_column: f"new_text{i}"})

    def update_rows_in_base_table_with_list_value(self, session, updating_column="cval1", ttl=None):
        for i in range(10):
            session.execute(self.get_update_stm(column=updating_column, ttl=ttl), {"pkey": i % 2, "ckey": i, updating_column: [f"new_text{i}", f"new_text{i + 1}"]})

    def get_base_rows(self, session):
        return list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table}"))

    def get_log_rows(self, session):
        return list(session.execute(f"SELECT * FROM {self.keyspace}.{self.log_table}"))

    def verify_cdc_ttl_configured(self, node, expected_cdc_ttl):
        result = node.run_cqlsh(f"desc keyspace {self.keyspace}", return_output=True)
        logger.debug(result)

        matched = re.search(r"cdc\s?=\s?{.*'ttl':\s+'(?P<ttl>[\d]+?)'", result[0], flags=re.MULTILINE)
        found_cdc_ttl = int(matched.group("ttl")) if matched else None
        assert found_cdc_ttl == expected_cdc_ttl

    def alter_base_table_with_cdc_ttl(self, session, cdc_ttl, preimage_enable=False, postimage_enable=False):
        cdc_properties = self.build_cdc_properties(preimage_enable, postimage_enable, cdc_ttl)
        session.execute(f"ALTER TABLE {self.keyspace}.{self.table} WITH {cdc_properties};")
