import time
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid1

import pytest
from cassandra.cluster import Session, SimpleStatement
from cassandra.util import OrderedMapSerializedKey, Time, uuid_from_time

from cdc_test import CDCInitializeHelper, CdcLogOperations, wait_for
from dtest_class import Tester, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.cdc_utils import mkident
from tools.misc import ImmutableMapping

native_types_values = [
    {"cl_type": "bigint", "ins_dataset": 1, "upd_dataset": 2},
    {"cl_type": "int", "ins_dataset": 3, "upd_dataset": 4},
    {"cl_type": "smallint", "ins_dataset": 5, "upd_dataset": 6},
    {"cl_type": "tinyint", "ins_dataset": 8, "upd_dataset": 7},
    {"cl_type": "varint", "ins_dataset": 1, "upd_dataset": 4},
    {"cl_type": "boolean", "ins_dataset": True, "upd_dataset": False},
    {"cl_type": "blob", "ins_dataset": b"1234567890qwertyuiop", "upd_dataset": b"a"},
    {"cl_type": "date", "ins_dataset": date(2020, 2, 2), "upd_dataset": date(2020, 12, 12)},
    {"cl_type": "decimal", "ins_dataset": Decimal("10.1"), "upd_dataset": Decimal("12.2")},
    {"cl_type": "double", "ins_dataset": 10.1000001, "upd_dataset": 22.22222},
    {"cl_type": "float", "ins_dataset": 33.33000183105469, "upd_dataset": 44.44000244140625},
    {"cl_type": "inet", "ins_dataset": "1.1.1.1", "upd_dataset": "2.2.2.2"},
    {"cl_type": "time", "ins_dataset": Time("02:02:02.222"), "upd_dataset": Time("12:12:12.121")},
    {"cl_type": "timestamp", "ins_dataset": datetime(2020, 2, 2, 2, 2, 2), "upd_dataset": datetime(2020, 3, 3, 3, 3, 3)},
    {"cl_type": "timeuuid", "ins_dataset": UUID("b478b7c2-5d3c-11ea-84b5-5aa95d83d60f"), "upd_dataset": UUID("c2ecebac-5d3c-11ea-9fd2-3cd5439c36c3")},
    {"cl_type": "uuid", "ins_dataset": uuid1(), "upd_dataset": uuid1()},
    {"cl_type": "varint", "ins_dataset": 1, "upd_dataset": 4},
    {"cl_type": "text", "ins_dataset": "aaaaaaa", "upd_dataset": "bbbbbbb"},
    {"cl_type": "varchar", "ins_dataset": "cccccccc", "upd_dataset": "ddddddddd"},
    {"cl_type": "ascii", "ins_dataset": "0123456789abcdef", "upd_dataset": "abcdef0123456789"},
]

collections_types = [
    {
        "cl_type": "map<text, text>",
        "ins_dataset": {"key1": "value1", "key2": "value2"},
        "upd_dataset": {"key3": "value3", "key4": "value4"},
        "add_el_dataset": {"key5": "value5"},
        "del_el_dataset": {"key1"},
        "result_add_element_dataset": {"key1": "value1", "key2": "value2", "key5": "value5"},
        "result_delete_element_dataset": {"key2": "value2"},
    },
    {
        "cl_type": "map<bigint, text>",
        "ins_dataset": {1: "value1", 10000: "value2"},
        "upd_dataset": {2000: "value3", 3: "value4"},
        "add_el_dataset": {5000: "value5"},
        "del_el_dataset": {1},
        "result_add_element_dataset": {1: "value1", 10000: "value2", 5000: "value5"},
        "result_delete_element_dataset": {10000: "value2"},
    },
    {
        "cl_type": "set<text>",
        "ins_dataset": {"value1", "value2"},
        "upd_dataset": {"value3", "value4"},
        "add_el_dataset": {"value5"},
        "del_el_dataset": {"value1"},
        "result_add_element_dataset": {"value1", "value2", "value5"},
        "result_delete_element_dataset": {"value2"},
    },
    {"cl_type": "list<int>", "ins_dataset": [1, 2], "upd_dataset": [3, 4], "add_el_dataset": [5], "del_el_dataset": [1], "result_add_element_dataset": [1, 2, 5], "result_delete_element_dataset": [2]},
]

frozen_collections = [
    {"cl_type": "frozen<map<text, text>>", "ins_dataset": {"key1": "value1", "key2": "value2"}, "upd_dataset": {"key3": "value3", "key4": "value4"}},
    {"cl_type": "frozen<set<text>>", "ins_dataset": {"value1", "value2"}, "upd_dataset": {"value3", "value4"}},
    {"cl_type": "frozen<list<text>>", "ins_dataset": ["value1", "value2"], "upd_dataset": ["value3", "value4"]},
    {"cl_type": "frozen<list<int>>", "ins_dataset": [1, 12], "upd_dataset": [3, 13]},
]


class CustomUDT:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.__dict__[key] = value

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)


udt_types = [
    {
        "cl_type": {"udt_name": "non_frozen_udt_with_native_types", "frozen": False, "fields": {"f_text": "text", "f_bigint": "bigint"}},
        "ins_dataset": CustomUDT(f_text="text", f_bigint=1),
        "upd_dataset": CustomUDT(f_text="text2", f_bigint=3),
        "update_udt_element": {"f_text": "newtext"},
        "delete_udt_element": {"f_text": None},
        "update_element_delta_result": CustomUDT(f_text="newtext", f_bigint=None),
        "delete_element_delta_result": CustomUDT(f_text=None, f_bigint=None),
        "postimage_upd_element_dataset": CustomUDT(f_text="newtext", f_bigint=1),
        "postimage_del_element_dataset": CustomUDT(f_text=None, f_bigint=1),
    },
    {
        "cl_type": {"udt_name": "non_frozen_udt_with_native_types_3_fields", "frozen": False, "fields": {"f_varchar": "text", "f_int": "int", "f_timestamp": "timestamp"}},
        "ins_dataset": CustomUDT(f_varchar="text", f_int=1, f_timestamp=datetime(2020, 2, 2, 2, 2, 2)),
        "upd_dataset": CustomUDT(f_varchar="text2", f_int=3, f_timestamp=datetime(2021, 3, 3, 3, 3, 3)),
        "update_udt_element": {"f_varchar": "newtext", "f_timestamp": datetime(2024, 4, 4, 4, 4, 4)},
        "delete_udt_element": {"f_varchar": None, "f_timestamp": None},
        "update_element_delta_result": CustomUDT(f_varchar="newtext", f_timestamp=datetime(2024, 4, 4, 4, 4, 4), f_int=None),
        "delete_element_delta_result": CustomUDT(f_varchar=None, f_timestamp=None, f_int=None),
        "postimage_upd_element_dataset": CustomUDT(f_varchar="newtext", f_timestamp=datetime(2024, 4, 4, 4, 4, 4), f_int=1),
        "postimage_del_element_dataset": CustomUDT(f_varchar=None, f_timestamp=None, f_int=1),
    },
    {
        "cl_type": {"udt_name": "frozen_udt_with_collection", "frozen": True, "fields": {"f_text": "text", "f_map": "map<int,text>"}},
        "ins_dataset": CustomUDT(f_text="text", f_map={1: "text"}),
        "upd_dataset": CustomUDT(f_text="text2", f_map={3: "new_text"}),
    },
    {
        "cl_type": {"udt_name": "frozen_udt_with_several_collection", "frozen": True, "fields": {"f_text": "text", "f_map": "map<int,text>", "f_list": "list<int>"}},
        "ins_dataset": CustomUDT(f_text="text", f_map={1: "text"}, f_list=[1, 2, 3]),
        "upd_dataset": CustomUDT(f_text="text2", f_map={3: "new_text"}, f_list=[4, 5, 6]),
    },
    {
        "cl_type": {"udt_name": "frozen_udt_with_frozen_collection", "frozen": True, "fields": {"f_text": "text", "f_map": "frozen<map<int,text>>"}},
        "ins_dataset": CustomUDT(f_text="text", f_map={1: "text"}),
        "upd_dataset": CustomUDT(f_text="text2", f_map={3: "new_text"}),
    },
    {
        "cl_type": {"udt_name": "udt_with_frozen_collection", "frozen": False, "fields": {"f_text": "text", "f_map": "frozen<set<ascii>>"}},
        "ins_dataset": CustomUDT(f_text="text", f_map={"text"}),
        "upd_dataset": CustomUDT(f_text="text2", f_map={"new_text"}),
        "update_udt_element": {"f_text": "newtext"},
        "delete_udt_element": {"f_text": None},
        "update_element_delta_result": CustomUDT(f_text="newtext", f_map=None),
        "delete_element_delta_result": CustomUDT(f_text=None, f_map=None),
        "postimage_upd_element_dataset": CustomUDT(f_text="newtext", f_map={"text"}),
        "postimage_del_element_dataset": CustomUDT(f_text=None, f_map={"text"}),
    },
]


class CdcTools(Tester, CDCInitializeHelper):
    keyspace = "ks"
    table = "cf"
    table_cdc_log = f"{table}_scylla_cdc_log"
    _last_timestamp = 0

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"experimental_features": ["cdc"], "start_rpc": "true"})
        return dtest_setup_overrides

    @staticmethod
    def _convert_from_micro_to_milli_seconds(timestamp):
        return timestamp // 1000

    def get_next_timestamp(self):
        if self._last_timestamp == 0:
            self._last_timestamp = int(time.time() * 1000000)

        self._last_timestamp += 1000

        return self._last_timestamp

    def prepare_cluster_and_schema(
        self,
        num_nodes=1,
        rf=1,
        preimage_enable=False,
        postimage_enable=False,
        primary_key_type=None,
    ):
        self.cluster.set_configuration_options(values={"experimental_features": ["cdc"]})
        self.populate_sequentially(num_nodes)
        node = self.cluster.nodelist()[0]
        session = self.fixture_dtest_setup.patient_cql_connection(node)
        self.create_schema_with_cdc(session, rf=rf, preimage_enable=preimage_enable, postimage_enable=postimage_enable, primary_key_type=primary_key_type)

        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        return (node, session)

    def create_schema_with_cdc(self, session, rf=1, preimage_enable=False, postimage_enable=False, primary_key_type=None):
        key_type = primary_key_type if primary_key_type else self.columns_data["cl_type"]
        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pkey {key_type}, \
                     ckey {key_type}, \
                     value {self.columns_data['cl_type']}, \
                     always_null {self.columns_data['cl_type']}, \
                     PRIMARY KEY (pkey, ckey)\
                    )"
        statement += " WITH cdc={'enabled': true"
        if preimage_enable:
            statement += f", 'preimage': '{str(preimage_enable).lower()}'"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += "}"
        session.execute(f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf}}}")
        create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def insert_one_with_timestamp(self, session, data, timestamp):
        stm = SimpleStatement(f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) VALUES (%(pkey)s, %(ckey)s, %(value)s) USING TIMESTAMP {timestamp}")
        session.execute(stm, data)

    def update_one_with_timestamp(self, session, data, timestamp):
        stm = SimpleStatement(f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET value = %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s")
        session.execute(stm, data)

    def update_collection_with_element_with_timestamp(self, session, data, timestamp, add=True):
        if not add:
            stm = SimpleStatement(f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET value = value - %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s")
        else:
            stm = SimpleStatement(f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET value = value + %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s")

        session.execute(stm, data)

    def update_udt_with_element_with_timestamp(self, session, data: dict, timestamp, add=True):
        updating_data = data.pop("value")
        stm = ", ".join([f"value.{key}=%({key})s" for key in updating_data])
        data.update({key: value for key, value in updating_data.items()})
        stm = SimpleStatement(
            f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET {stm} \
                                    WHERE pkey=%(pkey)s and ckey=%(ckey)s"
        )

        session.execute(stm, data)

    def delete_one_with_timestamp(self, session, data, timestamp):
        stm = SimpleStatement(f"DELETE value FROM {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} WHERE pkey=%(pkey)s and ckey=%(ckey)s")
        session.execute(stm, data)

    def get_cdc_log_records_by_timestamp(self, session, timestamp):
        timestamp = self._convert_from_micro_to_milli_seconds(timestamp)

        def get_res_cdc_log():
            return list(session.execute(f'SELECT * FROM {self.keyspace}.{self.table_cdc_log} WHERE "cdc$time" >= minTimeuuid({timestamp}) AND "cdc$time" <= maxTimeuuid({timestamp}) ALLOW FILTERING'))

        res_cdc_log = wait_for(get_res_cdc_log, timeout=10)
        return res_cdc_log

    def get_all_cdc_log_records(self, session):
        res_cdc_log = list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table_cdc_log}"))
        return res_cdc_log

    def get_all_base_records(self, session):
        res_cdc_log = list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table}"))
        return res_cdc_log

    def check_cdc_base_field_values(self, row, expected_field_values):
        for key in expected_field_values:
            cdc_field_value = getattr(row, key)
            assert expected_field_values[key] == cdc_field_value

    def check_cdc_base_collection_values(self, row, expected_field_values):
        cf_value = expected_field_values.pop("value")
        for key in expected_field_values:
            cdc_field_value = getattr(row, key)
            assert expected_field_values[key] == cdc_field_value

        if not cf_value:
            assert not row.value
        elif isinstance(row.value, OrderedMapSerializedKey):
            if isinstance(cf_value, list):
                for _, value in row.value.items():
                    assert value in cf_value
            if isinstance(cf_value, dict):
                for key, value in row.value.items():
                    assert key in cf_value.keys()
                    assert value in cf_value.values()
        else:
            assert row.value == cf_value

    def check_cdc_rec_timestamp(self, rows, timestamp):
        pass

    def check_cdc_log_num_row(self, cdc_log_results, expected_num_rows):
        assert expected_num_rows == len(cdc_log_results)

    @staticmethod
    def check_cdc_deleted_always_null_column_in_preimage_row(row, preimage_enabled=None):
        if row.cdc_operation != CdcLogOperations.PREIMAGE:
            return
        if preimage_enabled == "full":
            assert getattr(row, f"cdc_deleted_always_null") is True
        if preimage_enabled is True:
            assert getattr(row, f"cdc_deleted_always_null") is None

    def check_cdc_log_row(self, row, operation, batch_seq, expected_data, deleted_col=None, preimage_enabled=None):  # noqa: PLR0913
        assert operation == row.cdc_operation
        assert batch_seq == row.cdc_batch_seq_no

        self.check_cdc_deleted_always_null_column_in_preimage_row(row, preimage_enabled)
        if deleted_col:
            self.check_cdc_deleted_columns(row, deleted_col)
        self.check_cdc_base_field_values(row, expected_data)

    def check_cdc_log_row_collection(  # noqa: PLR0913
        self,
        row,
        operation,
        batch_seq,
        expected_data,
        deleted_col=None,
        deleted_keys=None,
        preimage_enabled=None,
    ):
        assert operation == row.cdc_operation
        assert batch_seq == row.cdc_batch_seq_no

        self.check_cdc_deleted_always_null_column_in_preimage_row(row, preimage_enabled)
        if deleted_col:
            self.check_cdc_deleted_columns(row, deleted_col)
        if deleted_keys:
            self.check_cdc_deleted_elements(row, deleted_keys)
        self.check_cdc_base_collection_values(row, expected_data)

    def check_cdc_log_row_udt(self, row, operation, batch_seq, expected_data, preimage_enabled=None):
        assert operation == row.cdc_operation
        assert batch_seq == row.cdc_batch_seq_no

        self.check_cdc_deleted_always_null_column_in_preimage_row(row, preimage_enabled)
        self.verify_udt_fields(row.value, expected_data)

    def verify_udt_fields(self, actual_udt, expected_udt):
        if expected_udt is None:
            assert actual_udt is None, f"Column with UDT type is not empty: {actual_udt}"
        else:
            assert expected_udt.__dict__ == actual_udt.__dict__, f"Actual UDT {actual_udt} is not equal to expected UDT {expected_udt}"

    def check_cdc_deleted_columns(self, row, deleted_columns):
        for col in deleted_columns:
            assert getattr(row, f"cdc_deleted_{col}")

    def check_cdc_deleted_elements(self, row, deleted_elements):
        if isinstance(deleted_elements, list):
            assert row.cdc_deleted_elements_value
        else:
            for key in deleted_elements:
                assert key in row.cdc_deleted_elements_value


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.scylla_cdc
class TestCDCNativeType(CdcTools):
    columns_data = None

    @pytest.fixture(params=native_types_values + frozen_collections, ids=[mkident(column["cl_type"]) for column in native_types_values + frozen_collections], autouse=True)
    def fixture_columns_data(self, request):
        self.columns_data = request.param

    @property
    def inserted_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"], "ckey": self.columns_data["ins_dataset"], "value": self.columns_data["ins_dataset"]}

    @property
    def updated_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"], "ckey": self.columns_data["ins_dataset"], "value": self.columns_data["upd_dataset"]}

    @property
    def updated_with_null_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"], "ckey": self.columns_data["ins_dataset"], "value": None}

    @property
    def deleted_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"], "ckey": self.columns_data["ins_dataset"]}

    @property
    def null_value_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"], "ckey": self.columns_data["ins_dataset"], "value": None, "always_null": None}

    def test_native_type_insert(self):
        self.insert_operation_tmpl()

    def test_native_type_insert_with_preimage(self):
        self.insert_operation_tmpl(preimage_enable=True)

    def test_native_type_insert_with_preimage_full(self):
        self.insert_operation_tmpl(preimage_enable="full")

    def test_native_type_insert_with_postimage(self):
        self.insert_operation_tmpl(postimage_enable=True)

    def test_native_type_insert_with_preimage_postimage(self):
        self.insert_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_natitve_type_update(self):
        self.update_operation_tmpl()

    def test_natitve_type_update_with_preimage(self):
        self.update_operation_tmpl(preimage_enable=True)

    def test_natitve_type_update_with_preimage_full(self):
        self.update_operation_tmpl(preimage_enable="full")

    def test_natitve_type_update_with_postimage(self):
        self.update_operation_tmpl(postimage_enable=True)

    def test_natitve_type_update_with_preimage_postimage(self):
        self.update_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_update_with_null(self):
        self.update_with_null()

    def test_update_with_null_with_preimage(self):
        self.update_with_null(preimage_enable=True)

    def test_update_with_null_with_preimage_full(self):
        self.update_with_null(preimage_enable="full")

    def test_update_with_null_with_postimage(self):
        self.update_with_null(postimage_enable=True)

    def test_update_with_null_with_preimage_postimage(self):
        self.update_with_null(preimage_enable=True, postimage_enable=True)

    def test_native_type_delete(self):
        self.delete_operation_tmpl()

    def test_native_type_delete_with_preimage(self):
        self.delete_operation_tmpl(preimage_enable=True)

    def test_native_type_delete_with_preimage_full(self):
        self.delete_operation_tmpl(preimage_enable="full")

    def test_native_type_delete_with_postimage(self):
        self.delete_operation_tmpl(postimage_enable=True)

    def test_native_type_delete_with_preimage_postimage(self):
        self.delete_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_all_operation(self):
        self.all_operation_tmpl()

    def test_all_operation_with_preimage(self):
        self.all_operation_tmpl(preimage_enable=True)

    def test_all_operation_with_preimage_full(self):
        self.all_operation_tmpl(preimage_enable="full")

    def test_all_operation_with_postimage(self):
        self.all_operation_tmpl(postimage_enable=True)

    def test_all_operation_with_preimage_postimage(self):
        self.all_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def insert_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_operation(
            cdc_log_data,
            operation=CdcLogOperations.INSERT,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.inserted_dataset,
            postimage_expected_dataset=self.inserted_dataset,
            first_record=True,
        )

        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_operation(
            cdc_log_data,
            operation=CdcLogOperations.INSERT,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.inserted_dataset,
            postimage_expected_dataset=self.inserted_dataset,
        )

    def update_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.inserted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_operation(
            cdc_log_data,
            operation=CdcLogOperations.UPDATE,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.inserted_dataset,
            postimage_expected_dataset=self.inserted_dataset,
            first_record=True,
        )
        # will contain 2 records
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.updated_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        # first record is the same
        self.verify_cdc_log_rows_after_operation(
            cdc_log_data,
            operation=CdcLogOperations.UPDATE,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.updated_dataset,
            postimage_expected_dataset=self.updated_dataset,
        )

    def update_with_null(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.inserted_dataset, timestamp)

        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.updated_with_null_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_operation(
            cdc_log_data,
            operation=CdcLogOperations.UPDATE,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.updated_with_null_dataset,
            postimage_expected_dataset=self.updated_with_null_dataset,
        )

    def delete_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)

        timestamp = self.get_next_timestamp()
        self.delete_one_with_timestamp(session, self.deleted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_operation(
            cdc_log_data,
            CdcLogOperations.UPDATE,
            preimage_enable,
            postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.deleted_dataset,
            postimage_expected_dataset=self.updated_with_null_dataset,
            deleted_col=["value"],
        )

    def all_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.updated_dataset, timestamp)
        timestamp = self.get_next_timestamp()
        self.delete_one_with_timestamp(session, self.deleted_dataset, timestamp)

        cdc_log_data = self.get_all_cdc_log_records(session)
        self.verify_cdc_log_rows_after_several_operations(cdc_log_data, preimage_enable, postimage_enable)

    def verify_cdc_log_rows_after_operation(  # noqa: PLR0913
        self,
        cdc_log_data,
        operation,
        preimage_enable,
        postimage_enable,
        preimage_expected_dataset,
        delta_expected_dataset,
        postimage_expected_dataset,
        deleted_col=None,
        first_record=False,
    ):
        delta_index = 0
        postimage_index = 1
        preimage_expected_dataset = preimage_expected_dataset if not first_record else self.null_value_dataset

        if preimage_enable and not first_record:
            delta_index += 1
            postimage_index += 1
            self.check_cdc_log_row(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0, expected_data=preimage_expected_dataset, preimage_enabled=preimage_enable)
        self.check_cdc_log_row(cdc_log_data[delta_index], operation=operation, batch_seq=delta_index, expected_data=delta_expected_dataset)

        if postimage_enable:
            self.check_cdc_log_row(cdc_log_data[postimage_index], operation=CdcLogOperations.POSTIMAGE, batch_seq=postimage_index, expected_data=postimage_expected_dataset)

    def verify_cdc_log_rows_after_several_operations(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 8)
            cdc_log_insert_rows = cdc_log_data[:2]
            cdc_log_update_rows = cdc_log_data[2:5]
            cdc_log_deleted_rows = cdc_log_data[5:]
        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 5)
            cdc_log_insert_rows = cdc_log_data[:1]
            cdc_log_update_rows = cdc_log_data[1:3]
            cdc_log_deleted_rows = cdc_log_data[3:]
        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 6)
            cdc_log_insert_rows = cdc_log_data[:2]
            cdc_log_update_rows = cdc_log_data[2:4]
            cdc_log_deleted_rows = cdc_log_data[4:]
        else:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            cdc_log_insert_rows = [cdc_log_data[0]]
            cdc_log_update_rows = [cdc_log_data[1]]
            cdc_log_deleted_rows = [cdc_log_data[2]]
        self.verify_cdc_log_rows_after_operation(
            cdc_log_insert_rows,
            operation=CdcLogOperations.INSERT,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.inserted_dataset,
            postimage_expected_dataset=self.inserted_dataset,
            first_record=True,
        )
        self.verify_cdc_log_rows_after_operation(
            cdc_log_update_rows,
            operation=CdcLogOperations.UPDATE,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.inserted_dataset,
            delta_expected_dataset=self.updated_dataset,
            postimage_expected_dataset=self.updated_dataset,
        )
        self.verify_cdc_log_rows_after_operation(
            cdc_log_deleted_rows,
            operation=CdcLogOperations.UPDATE,
            preimage_enable=preimage_enable,
            postimage_enable=postimage_enable,
            preimage_expected_dataset=self.updated_dataset,
            delta_expected_dataset=self.deleted_dataset,
            postimage_expected_dataset=self.updated_with_null_dataset,
            deleted_col=["value"],
        )


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCDCCollectionsType(CdcTools):
    columns_data = None
    timeuuid = uuid_from_time(time.time())

    @pytest.fixture(params=collections_types, ids=[mkident(column["cl_type"]) for column in collections_types], autouse=True)
    def fixture_columns_data(self, request):
        self.columns_data = request.param

    @property
    def inserted_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data["ins_dataset"]}

    @property
    def null_value_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": None, "always_null": None}

    @property
    def updated_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data["upd_dataset"]}

    @property
    def added_element_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data["add_el_dataset"]}

    @property
    def deleted_element_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data["del_el_dataset"]}

    @property
    def result_dataset_after_delete_element(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data["result_delete_element_dataset"]}

    @property
    def result_dataset_after_add_element(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data["result_add_element_dataset"]}

    @property
    def deleted_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid}

    def test_collection_insert(self):
        self.insert_operation_tmpl()

    def test_collection_insert_with_preimage(self):
        self.insert_operation_tmpl(preimage_enable=True)

    def test_collection_insert_with_preimage_full(self):
        self.insert_operation_tmpl(preimage_enable="full")

    def test_collection_insert_with_postimage(self):
        self.insert_operation_tmpl(postimage_enable=True)

    def test_collection_insert_with_preimage_postimage(self):
        self.insert_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_update_collection_with_add_element(self):
        self.collection_update_tmpl(add_element=True)

    def test_update_collection_with_add_element_with_preimage(self):
        self.collection_update_tmpl(add_element=True, preimage_enable=True)

    def test_update_collection_with_add_element_with_preimage_full(self):
        self.collection_update_tmpl(add_element=True, preimage_enable="full")

    def test_update_collection_with_add_element_with_postimage(self):
        self.collection_update_tmpl(add_element=True, postimage_enable=True)

    def test_update_collection_with_add_element_with_preimage_postimage(self):
        self.collection_update_tmpl(add_element=True, preimage_enable=True, postimage_enable=True)

    def test_update_collection_with_delete_element(self):
        self.collection_update_tmpl(remove_element=True)

    def test_update_collection_with_delete_element_with_preimage(self):
        self.collection_update_tmpl(remove_element=True, preimage_enable=True)

    def test_update_collection_with_delete_element_with_preimage_full(self):
        self.collection_update_tmpl(remove_element=True, preimage_enable="full")

    def test_update_collection_with_delete_element_with_postimage(self):
        self.collection_update_tmpl(remove_element=True, postimage_enable=True)

    def test_update_collection_with_delete_element_with_preimage_postimage(self):
        self.collection_update_tmpl(remove_element=True, preimage_enable=True, postimage_enable=True)

    def test_update_collection(self):
        self.collection_update_tmpl()

    def test_update_collection_with_preimage(self):
        self.collection_update_tmpl(preimage_enable=True)

    def test_update_collection_with_preimage_full(self):
        self.collection_update_tmpl(preimage_enable="full")

    def test_update_collection_with_postimage(self):
        self.collection_update_tmpl(postimage_enable=True)

    def test_update_collection_with_preimage_postimage(self):
        self.collection_update_tmpl(preimage_enable=True, postimage_enable=True)

    def test_collection_delete(self):
        self.collection_delete_tmpl()

    def test_collection_delete_with_preimage(self):
        self.collection_delete_tmpl(preimage_enable=True)

    def test_collection_delete_with_preimage_full(self):
        self.collection_delete_tmpl(preimage_enable="full")

    def test_collection_delete_with_postimage(self):
        self.collection_delete_tmpl(postimage_enable=True)

    def test_collection_delete_with_preimage_postimage(self):
        self.collection_delete_tmpl(preimage_enable=True, postimage_enable=True)

    def insert_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable, primary_key_type="timeuuid")
        # insert first record to partitions
        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)

        self.verify_cdc_log_rows_after_insert_to_base_table(cdc_log_data, preimage_enable, postimage_enable, first_record=True)
        # insert record to not empty parition
        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_insert_to_base_table(cdc_log_data, preimage_enable, postimage_enable)

    def collection_update_tmpl(self, add_element=False, remove_element=False, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable, primary_key_type="timeuuid")
        # insert first record
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.inserted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_update_to_base_table(cdc_log_data, preimage_enable, postimage_enable, first_record=True)
        timestamp = self.get_next_timestamp()
        if add_element:
            self.update_collection_with_element_with_timestamp(session, self.added_element_dataset, timestamp, add=True)
        elif remove_element:
            self.update_collection_with_element_with_timestamp(session, self.deleted_element_dataset, timestamp, add=False)
        else:
            self.update_one_with_timestamp(session, self.updated_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_update_to_base_table(cdc_log_data, preimage_enable, postimage_enable, add_element, remove_element)

    def collection_delete_tmpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable, primary_key_type="timeuuid")
        # insert first record
        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.inserted_dataset, timestamp)

        timestamp = self.get_next_timestamp()
        self.delete_one_with_timestamp(session, self.deleted_dataset, timestamp)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_delete_value(cdc_log_data, preimage_enable, postimage_enable)

    def verify_cdc_log_rows_after_insert_to_base_table(self, cdc_log_data, preimage_enable, postimage_enable, first_record=False):
        if preimage_enable:
            preimage_dataset = self.inserted_dataset if not first_record else None
        else:
            preimage_dataset = None

        self._check_collection_in_cdc_log_rows(
            cdc_log_data, CdcLogOperations.INSERT, preimage_enable, postimage_enable, preimage_expected_data=preimage_dataset, delta_expected_data=self.inserted_dataset, postimage_expected_data=self.inserted_dataset, deleted_col=["value"]
        )

    def verify_cdc_log_rows_after_update_to_base_table(  # noqa: PLR0913
        self,
        cdc_log_data,
        preimage_enable,
        postimage_enable,
        add_element=None,
        remove_element=None,
        first_record=False,
    ):
        if preimage_enable:
            preimage_dataset = self.inserted_dataset if not first_record else None
        else:
            preimage_dataset = None

        if add_element or remove_element:
            deleted_element = self.deleted_element_dataset["value"] if remove_element else None
            updating_dataset = self.added_element_dataset if add_element else self.null_value_dataset
            postimage_dataset = self.result_dataset_after_add_element if add_element else self.result_dataset_after_delete_element
            deleted_col = None

        else:
            deleted_element = None
            updating_dataset = self.updated_dataset if not first_record else self.inserted_dataset
            postimage_dataset = self.updated_dataset if not first_record else self.inserted_dataset
            deleted_col = ["value"]

        self._check_collection_in_cdc_log_rows(
            cdc_log_data,
            CdcLogOperations.UPDATE,
            preimage_enable,
            postimage_enable,
            preimage_expected_data=preimage_dataset,
            delta_expected_data=updating_dataset,
            postimage_expected_data=postimage_dataset,
            deleted_col=deleted_col,
            deleted_element=deleted_element,
        )

    def verify_cdc_log_rows_after_delete_value(self, cdc_log_data, preimage_enable, postimage_enable):
        self._check_collection_in_cdc_log_rows(
            cdc_log_data,
            CdcLogOperations.UPDATE,
            preimage_enable,
            postimage_enable,
            preimage_expected_data=self.inserted_dataset,
            delta_expected_data=self.null_value_dataset,
            postimage_expected_data=self.null_value_dataset,
            deleted_col=["value"],
        )

    def _check_collection_in_cdc_log_rows(  # noqa: PLR0913
        self,
        cdc_log_rows,
        base_operation,
        preimage_enable,
        postimage_enable,
        preimage_expected_data,
        delta_expected_data,
        postimage_expected_data,
        deleted_col=None,
        deleted_element=None,
    ):
        delta_index = 0
        postimage_index = 1

        if preimage_enable and preimage_expected_data:
            delta_index += 1
            postimage_index += 1
            self.check_cdc_log_row_collection(cdc_log_rows[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0, expected_data=preimage_expected_data, deleted_col=None, preimage_enabled=preimage_enable)

        self.check_cdc_log_row_collection(cdc_log_rows[delta_index], operation=base_operation, batch_seq=delta_index, expected_data=delta_expected_data, deleted_col=deleted_col)

        if postimage_enable:
            self.check_cdc_log_row_collection(cdc_log_rows[postimage_index], operation=CdcLogOperations.POSTIMAGE, batch_seq=postimage_index, expected_data=postimage_expected_data)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.scylla_cdc
class TestCdcUDT(CdcTools):
    columns_data = None

    @pytest.fixture(params=udt_types, ids=[mkident(column["cl_type"]["udt_name"]) for column in udt_types], autouse=True)
    def fixture_columns_data(self, request):
        self.columns_data = request.param

    @property
    def insert_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": self.columns_data["ins_dataset"]}

    @property
    def update_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": self.columns_data["upd_dataset"]}

    @property
    def updating_field_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": self.columns_data["update_udt_element"]}

    @property
    def deleting_field_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": self.columns_data["delete_udt_element"]}

    def create_schema_with_cdc(self, session: Session, rf=1, preimage_enable=False, postimage_enable=False, primary_key_type=None):
        primary_key_type = primary_key_type if primary_key_type else "bigint"
        self.parse_udt_type_name()
        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pkey {primary_key_type}, \
                     ckey {primary_key_type}, \
                     value {self.udt_type}, \
                     always_null {self.udt_type}, \
                     PRIMARY KEY (pkey, ckey)\
                    )"
        statement += " WITH cdc={'enabled': true"
        if preimage_enable:
            statement += f", 'preimage': '{str(preimage_enable).lower()}'"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += "}"
        session.execute("ALTER keyspace system_distributed with replication={'class': 'SimpleStrategy', 'replication_factor': '1'}")
        create_ks(session, self.keyspace, rf=rf)
        self._create_udt(session)
        session.cluster.register_user_type(self.keyspace, self.udt_name, CustomUDT)
        session.execute(statement)

    def test_insert_udt_delta(self):
        self.insert_udt_tpl()

    def test_insert_udt_preimage(self):
        self.insert_udt_tpl(preimage_enable=True)

    def test_insert_udt_preimage_full(self):
        self.insert_udt_tpl(preimage_enable="full")

    def test_insert_udt_postimage(self):
        self.insert_udt_tpl(postimage_enable=True)

    def test_insert_udt_preimage_postimage(self):
        self.insert_udt_tpl(preimage_enable=True, postimage_enable=True)

    def test_update_udt_delta(self):
        self.update_udt_tpl()

    def test_update_udt_preimage(self):
        self.update_udt_tpl(preimage_enable=True)

    def test_update_udt_preimage_full(self):
        self.update_udt_tpl(preimage_enable="full")

    def test_update_udt_postimage(self):
        self.update_udt_tpl(postimage_enable=True)

    @pytest.mark.next_gating
    def test_update_udt_preimage_postimage(self):
        self.update_udt_tpl(preimage_enable=True, postimage_enable=True)

    def test_update_field_non_frozen_udt(self):
        self.udt_update_field_on_non_frozen(remove_field_value=False)

    def test_update_field_non_frozen_udt_preimage(self):
        self.udt_update_field_on_non_frozen(preimage_enable=True)

    def test_update_field_non_frozen_udt_preimage_full(self):
        self.udt_update_field_on_non_frozen(preimage_enable="full")

    def test_update_field_non_frozen_udt_postimage(self):
        self.udt_update_field_on_non_frozen(postimage_enable=True)

    def test_update_field_non_frozen_udt_preimage_postimage(self):
        self.udt_update_field_on_non_frozen(preimage_enable=True, postimage_enable=True)

    def test_delete_field_value_in_non_frozen_udt(self):
        self.udt_update_field_on_non_frozen(remove_field_value=True)

    def test_delete_field_value_in_non_frozen_udt_preimage(self):
        self.udt_update_field_on_non_frozen(preimage_enable=True, remove_field_value=True)

    def test_delete_field_value_in_non_frozen_udt_preimage_full(self):
        self.udt_update_field_on_non_frozen(preimage_enable="full", remove_field_value=True)

    def test_delete_field_value_in_non_frozen_udt_postimage(self):
        self.udt_update_field_on_non_frozen(postimage_enable=True, remove_field_value=True)

    def test_delete_value_in_non_frozen_udt_field_preimage_postimage(self):
        self.udt_update_field_on_non_frozen(preimage_enable=True, postimage_enable=True, remove_field_value=True)

    def insert_udt_tpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.insert_dataset, timestamp)

        res_log_rows = self.get_cdc_log_records_by_timestamp(session, timestamp)
        # first record in partition will have no preimage
        self.verify_cdc_log_rows_with_udt(CdcLogOperations.INSERT, res_log_rows, {"delta": self.columns_data["ins_dataset"], "postimage": self.columns_data["ins_dataset"]}, False, postimage_enable)

        timestamp = self.get_next_timestamp()
        self.insert_one_with_timestamp(session, self.update_dataset, timestamp)
        res_log_rows = self.get_cdc_log_records_by_timestamp(session, timestamp)

        self.verify_cdc_log_rows_with_udt(
            CdcLogOperations.INSERT, res_log_rows, {"preimage": self.columns_data["ins_dataset"], "delta": self.columns_data["upd_dataset"], "postimage": self.columns_data["upd_dataset"]}, preimage_enable, postimage_enable
        )

    def update_udt_tpl(self, preimage_enable=False, postimage_enable=False):
        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.insert_dataset, timestamp)

        res_log_rows = self.get_cdc_log_records_by_timestamp(session, timestamp)
        # first record in partition will have no preimage
        self.verify_cdc_log_rows_with_udt(CdcLogOperations.UPDATE, res_log_rows, {"delta": self.columns_data["ins_dataset"], "postimage": self.columns_data["ins_dataset"]}, False, postimage_enable)

        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.update_dataset, timestamp)
        res_log_rows = self.get_cdc_log_records_by_timestamp(session, timestamp)

        self.verify_cdc_log_rows_with_udt(
            CdcLogOperations.UPDATE, res_log_rows, {"preimage": self.columns_data["ins_dataset"], "delta": self.columns_data["upd_dataset"], "postimage": self.columns_data["upd_dataset"]}, preimage_enable, postimage_enable
        )

    def udt_update_field_on_non_frozen(self, preimage_enable=False, postimage_enable=False, remove_field_value=False):
        self._skip_test_if_frozen_is_used()

        using_data_set = self.deleting_field_dataset if remove_field_value else self.updating_field_dataset

        _node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(session, self.insert_dataset, timestamp)

        timestamp = self.get_next_timestamp()
        self.update_udt_with_element_with_timestamp(session, using_data_set, timestamp, remove_field_value)

        expected_udt_result = self.get_expected_udt_element_mutation(remove_field_value)

        res_log_rows = self.get_cdc_log_records_by_timestamp(session, timestamp)

        self.verify_cdc_log_rows_with_udt(CdcLogOperations.UPDATE, res_log_rows, expected_udt_result, preimage_enable, postimage_enable)

    def _skip_test_if_frozen_is_used(self):
        if self.columns_data["cl_type"]["frozen"]:
            pytest.skip("Update UDT field for frozen UDT is not supported")

    def parse_udt_type_name(self):
        self.udt_name = self.columns_data["cl_type"]["udt_name"]
        self.udt_type = f"frozen<{self.udt_name}>" if self.columns_data["cl_type"]["frozen"] else self.udt_name
        self.udt_fields = [f"{field_name} {field_type}" for field_name, field_type in self.columns_data["cl_type"]["fields"].items()]

    def _create_udt(self, session):
        stm = f"""CREATE TYPE {self.keyspace}.{self.udt_name} ("""
        stm += ", ".join(self.udt_fields)
        stm += ");"
        session.execute(stm)

    def get_expected_udt_element_mutation(self, remove_field_value=False):
        expected_udt_result = {}
        expected_udt_result["preimage"] = self.columns_data["ins_dataset"]
        if remove_field_value:
            expected_udt_result["delta"] = self.columns_data["delete_element_delta_result"]
            expected_udt_result["postimage"] = self.columns_data["postimage_del_element_dataset"]
        else:
            expected_udt_result["delta"] = self.columns_data["update_element_delta_result"]
            expected_udt_result["postimage"] = self.columns_data["postimage_upd_element_dataset"]
        return expected_udt_result

    def verify_cdc_log_rows_with_udt(
        self,
        expected_operation,
        log_rows,
        expected_udt_result,
        preimage_enable=False,
        postimage_enable=False,
    ):
        delta_index = 0
        postimage_index = 1

        if preimage_enable:
            delta_index = 1
            postimage_index = 2
            self.check_cdc_log_row_udt(log_rows[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0, expected_data=expected_udt_result["preimage"], preimage_enabled=preimage_enable)

        self.check_cdc_log_row_udt(log_rows[delta_index], operation=expected_operation, batch_seq=delta_index, expected_data=expected_udt_result["delta"])

        if postimage_enable:
            self.check_cdc_log_row_udt(log_rows[postimage_index], operation=CdcLogOperations.POSTIMAGE, batch_seq=postimage_index, expected_data=expected_udt_result["postimage"])
