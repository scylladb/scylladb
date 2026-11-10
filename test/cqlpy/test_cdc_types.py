# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests of the CDC log of single writes - INSERT, UPDATE, DELETE of a column,
# collection element and UDT field updates - of values of every type, in
# every combination of the preimage and postimage options, ported from
# scylla-dtest cdc_types_test.py, one test per dtest, each parametrized over
# the dtest's column types. As in the dtests, the values are formatted into
# the statements' text by the driver (unprepared statements), every write
# has an explicit timestamp, the log rows of a write are read back with a
# range on "cdc$time" covering its timestamp, and each log row's operation,
# batch sequence number and values are checked.

from test import DEBUG_MODES
from test.pylib.skip_types import skip_env
import time
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid1

import pytest
from cassandra.util import OrderedMapSerializedKey, Time, uuid_from_time

from .util import new_test_table, new_type

# CDC log operation codes, see cdc::operation.
PREIMAGE, UPDATE, INSERT, POSTIMAGE = 0, 1, 2, 9

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


# The Python class the UDT values are mapped to, as in the dtests: the
# driver builds it from the fields of a UDT value read, and formats it as a
# UDT literal when it is a statement parameter.
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


def mkident(s):
    """The dtests' test id of a type name."""
    s = s.replace(" ", "")
    for c in "<>,":
        s = s.replace(c, "_")
    return s.rstrip("_")


def params_of(types):
    return [pytest.param(t, id=mkident(t["cl_type"] if isinstance(t["cl_type"], str) else t["cl_type"]["udt_name"])) for t in types]


class CdcTools:
    """The dtests' CdcTools: the writes with explicit timestamps to a table
    (pkey, ckey, value, always_null) of the tested type, and the checks of
    their log rows."""

    def __init__(self, cql, table, columns_data):
        self.cql = cql
        self.table = table
        self.columns_data = columns_data
        self._last_timestamp = 0

    def get_next_timestamp(self):
        if self._last_timestamp == 0:
            self._last_timestamp = int(time.time() * 1000000)
        self._last_timestamp += 1000
        return self._last_timestamp

    def insert_one_with_timestamp(self, data, timestamp):
        self.cql.execute(f"INSERT INTO {self.table} (pkey, ckey, value) VALUES (%(pkey)s, %(ckey)s, %(value)s) USING TIMESTAMP {timestamp}", data)

    def update_one_with_timestamp(self, data, timestamp):
        self.cql.execute(f"UPDATE {self.table} USING TIMESTAMP {timestamp} SET value = %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s", data)

    def update_collection_with_element_with_timestamp(self, data, timestamp, add=True):
        op = "+" if add else "-"
        self.cql.execute(f"UPDATE {self.table} USING TIMESTAMP {timestamp} SET value = value {op} %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s", data)

    def update_udt_with_element_with_timestamp(self, data, timestamp):
        updating_data = data.pop("value")
        stm = ", ".join([f"value.{key}=%({key})s" for key in updating_data])
        data.update(updating_data)
        self.cql.execute(f"UPDATE {self.table} USING TIMESTAMP {timestamp} SET {stm} WHERE pkey=%(pkey)s and ckey=%(ckey)s", data)

    def delete_one_with_timestamp(self, data, timestamp):
        self.cql.execute(f"DELETE value FROM {self.table} USING TIMESTAMP {timestamp} WHERE pkey=%(pkey)s and ckey=%(ckey)s", data)

    def get_cdc_log_records_by_timestamp(self, timestamp):
        """The log rows of the write with this timestamp, waited for (up to
        10 seconds) like the dtests did."""
        ms = timestamp // 1000
        deadline = time.time() + 10
        while True:
            rows = list(self.cql.execute(f'SELECT * FROM {self.table}_scylla_cdc_log WHERE "cdc$time" >= minTimeuuid({ms}) AND "cdc$time" <= maxTimeuuid({ms}) ALLOW FILTERING'))
            if rows or time.time() > deadline:
                return rows
            time.sleep(0.1)

    def get_all_cdc_log_records(self):
        return list(self.cql.execute(f"SELECT * FROM {self.table}_scylla_cdc_log"))

    @staticmethod
    def check_cdc_base_field_values(row, expected_field_values):
        for key in expected_field_values:
            assert expected_field_values[key] == getattr(row, key)

    @staticmethod
    def check_cdc_base_collection_values(row, expected_field_values):
        cf_value = expected_field_values.pop("value")
        for key in expected_field_values:
            assert expected_field_values[key] == getattr(row, key)
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

    @staticmethod
    def check_cdc_deleted_always_null_column_in_preimage_row(row, preimage_enabled=None):
        if row.cdc_operation != PREIMAGE:
            return
        if preimage_enabled == "full":
            assert row.cdc_deleted_always_null is True
        if preimage_enabled is True:
            assert row.cdc_deleted_always_null is None

    @staticmethod
    def check_cdc_deleted_columns(row, deleted_columns):
        for col in deleted_columns:
            assert getattr(row, f"cdc_deleted_{col}")

    @staticmethod
    def check_cdc_deleted_elements(row, deleted_elements):
        if isinstance(deleted_elements, list):
            assert row.cdc_deleted_elements_value
        else:
            for key in deleted_elements:
                assert key in row.cdc_deleted_elements_value

    def check_cdc_log_row(self, row, operation, batch_seq, expected_data, deleted_col=None, preimage_enabled=None):
        assert operation == row.cdc_operation
        assert batch_seq == row.cdc_batch_seq_no
        self.check_cdc_deleted_always_null_column_in_preimage_row(row, preimage_enabled)
        if deleted_col:
            self.check_cdc_deleted_columns(row, deleted_col)
        self.check_cdc_base_field_values(row, expected_data)

    def check_cdc_log_row_collection(self, row, operation, batch_seq, expected_data, deleted_col=None, deleted_keys=None, preimage_enabled=None):
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
        if expected_data is None:
            assert row.value is None, f"Column with UDT type is not empty: {row.value}"
        else:
            assert expected_data.__dict__ == row.value.__dict__, f"Actual UDT {row.value} is not equal to expected UDT {expected_data}"


def cdc_options(preimage_enable, postimage_enable):
    """The cdc option of the dtests' tables."""
    statement = " WITH cdc={'enabled': true"
    if preimage_enable:
        statement += f", 'preimage': '{str(preimage_enable).lower()}'"
    if postimage_enable:
        statement += ", 'postimage': true"
    return statement + "}"


def create_table(cql, keyspace, key_type, value_type, preimage_enable, postimage_enable):
    schema = f"pkey {key_type}, ckey {key_type}, value {value_type}, always_null {value_type}, PRIMARY KEY (pkey, ckey)"
    return new_test_table(cql, keyspace, schema, cdc_options(preimage_enable, postimage_enable))


# TestCDCNativeType: native types and frozen collections, the same type
# being used for the keys.

class NativeType(CdcTools):
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

    def verify_cdc_log_rows_after_operation(self, cdc_log_data, operation, preimage_enable, postimage_enable, preimage_expected_dataset,
                                            delta_expected_dataset, postimage_expected_dataset, deleted_col=None, first_record=False):
        delta_index = 0
        postimage_index = 1
        preimage_expected_dataset = preimage_expected_dataset if not first_record else self.null_value_dataset
        if preimage_enable and not first_record:
            delta_index += 1
            postimage_index += 1
            self.check_cdc_log_row(cdc_log_data[0], operation=PREIMAGE, batch_seq=0, expected_data=preimage_expected_dataset, preimage_enabled=preimage_enable)
        # As in the dtests, deleted_col is not checked.
        self.check_cdc_log_row(cdc_log_data[delta_index], operation=operation, batch_seq=delta_index, expected_data=delta_expected_dataset)
        if postimage_enable:
            self.check_cdc_log_row(cdc_log_data[postimage_index], operation=POSTIMAGE, batch_seq=postimage_index, expected_data=postimage_expected_dataset)

    def verify_cdc_log_rows_after_several_operations(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            assert len(cdc_log_data) == 8
            insert_rows, update_rows, deleted_rows = cdc_log_data[:2], cdc_log_data[2:5], cdc_log_data[5:]
        elif preimage_enable:
            assert len(cdc_log_data) == 5
            insert_rows, update_rows, deleted_rows = cdc_log_data[:1], cdc_log_data[1:3], cdc_log_data[3:]
        elif postimage_enable:
            assert len(cdc_log_data) == 6
            insert_rows, update_rows, deleted_rows = cdc_log_data[:2], cdc_log_data[2:4], cdc_log_data[4:]
        else:
            assert len(cdc_log_data) == 3
            insert_rows, update_rows, deleted_rows = [cdc_log_data[0]], [cdc_log_data[1]], [cdc_log_data[2]]
        self.verify_cdc_log_rows_after_operation(insert_rows, INSERT, preimage_enable, postimage_enable, self.inserted_dataset,
                                                 self.inserted_dataset, self.inserted_dataset, first_record=True)
        self.verify_cdc_log_rows_after_operation(update_rows, UPDATE, preimage_enable, postimage_enable, self.inserted_dataset,
                                                 self.updated_dataset, self.updated_dataset)
        self.verify_cdc_log_rows_after_operation(deleted_rows, UPDATE, preimage_enable, postimage_enable, self.updated_dataset,
                                                 self.deleted_dataset, self.updated_with_null_dataset, deleted_col=["value"])

    def insert_operation_tmpl(self, preimage_enable, postimage_enable):
        for _ in range(2):
            first = self._last_timestamp == 0
            timestamp = self.get_next_timestamp()
            self.insert_one_with_timestamp(self.inserted_dataset, timestamp)
            self.verify_cdc_log_rows_after_operation(self.get_cdc_log_records_by_timestamp(timestamp), INSERT, preimage_enable, postimage_enable,
                                                     self.inserted_dataset, self.inserted_dataset, self.inserted_dataset, first_record=first)

    def update_operation_tmpl(self, preimage_enable, postimage_enable):
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(self.inserted_dataset, timestamp)
        self.verify_cdc_log_rows_after_operation(self.get_cdc_log_records_by_timestamp(timestamp), UPDATE, preimage_enable, postimage_enable,
                                                 self.inserted_dataset, self.inserted_dataset, self.inserted_dataset, first_record=True)
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(self.updated_dataset, timestamp)
        self.verify_cdc_log_rows_after_operation(self.get_cdc_log_records_by_timestamp(timestamp), UPDATE, preimage_enable, postimage_enable,
                                                 self.inserted_dataset, self.updated_dataset, self.updated_dataset)

    def update_with_null(self, preimage_enable, postimage_enable):
        self.update_one_with_timestamp(self.inserted_dataset, self.get_next_timestamp())
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(self.updated_with_null_dataset, timestamp)
        self.verify_cdc_log_rows_after_operation(self.get_cdc_log_records_by_timestamp(timestamp), UPDATE, preimage_enable, postimage_enable,
                                                 self.inserted_dataset, self.updated_with_null_dataset, self.updated_with_null_dataset)

    def delete_operation_tmpl(self, preimage_enable, postimage_enable):
        self.insert_one_with_timestamp(self.inserted_dataset, self.get_next_timestamp())
        timestamp = self.get_next_timestamp()
        self.delete_one_with_timestamp(self.deleted_dataset, timestamp)
        self.verify_cdc_log_rows_after_operation(self.get_cdc_log_records_by_timestamp(timestamp), UPDATE, preimage_enable, postimage_enable,
                                                 self.inserted_dataset, self.deleted_dataset, self.updated_with_null_dataset, deleted_col=["value"])

    def all_operation_tmpl(self, preimage_enable, postimage_enable):
        self.insert_one_with_timestamp(self.inserted_dataset, self.get_next_timestamp())
        self.update_one_with_timestamp(self.updated_dataset, self.get_next_timestamp())
        self.delete_one_with_timestamp(self.deleted_dataset, self.get_next_timestamp())
        self.verify_cdc_log_rows_after_several_operations(self.get_all_cdc_log_records(), preimage_enable, postimage_enable)


def native_type(cql, keyspace, columns_data, scenario, preimage_enable=False, postimage_enable=False):
    typ = columns_data["cl_type"]
    with create_table(cql, keyspace, typ, typ, preimage_enable, postimage_enable) as table:
        getattr(NativeType(cql, table, columns_data), scenario)(preimage_enable, postimage_enable)


native_params = params_of(native_types_values + frozen_collections)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_insert.
# INSERT into an empty row, then again: the log has the delta (and the
# images, as enabled; no preimage for the first write).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_insert(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_insert_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_insert_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_insert_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_insert_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_insert_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_insert_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_insert_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_insert_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_natitve_type_update.
# UPDATE of an empty row, then UPDATE to another value.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_natitve_type_update(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_operation_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_natitve_type_update_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_natitve_type_update_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_operation_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_natitve_type_update_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_natitve_type_update_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_operation_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_natitve_type_update_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_natitve_type_update_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_operation_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_natitve_type_update_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_natitve_type_update_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_operation_tmpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_update_with_null.
# UPDATE of a set column to null.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_update_with_null(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_with_null")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_update_with_null_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_update_with_null_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_with_null", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_update_with_null_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_update_with_null_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_with_null", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_update_with_null_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_update_with_null_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_with_null", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_update_with_null_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_update_with_null_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "update_with_null", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_delete.
# DELETE of an inserted column: logged as an UPDATE with the column's
# "deleted" flag.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_delete(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "delete_operation_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_delete_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_delete_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "delete_operation_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_delete_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_delete_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "delete_operation_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_delete_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_delete_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "delete_operation_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_native_type_delete_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_native_type_delete_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "delete_operation_tmpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_all_operation.
# INSERT, UPDATE and DELETE of the row, then the whole log is read and
# checked.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_all_operation(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "all_operation_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_all_operation_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_all_operation_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "all_operation_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_all_operation_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_all_operation_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "all_operation_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_all_operation_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_all_operation_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "all_operation_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCNativeType::test_all_operation_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", native_params)
def test_all_operation_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    native_type(cql, test_keyspace_vnodes, columns_data, "all_operation_tmpl", preimage_enable=True, postimage_enable=True)



# TestCDCCollectionsType: non-frozen collections, with timeuuid keys.

class CollectionsType(CdcTools):
    timeuuid = uuid_from_time(time.time())

    def _dataset(self, key):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": self.columns_data[key]}

    @property
    def inserted_dataset(self):
        return self._dataset("ins_dataset")

    @property
    def null_value_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid, "value": None, "always_null": None}

    @property
    def updated_dataset(self):
        return self._dataset("upd_dataset")

    @property
    def added_element_dataset(self):
        return self._dataset("add_el_dataset")

    @property
    def deleted_element_dataset(self):
        return self._dataset("del_el_dataset")

    @property
    def result_dataset_after_delete_element(self):
        return self._dataset("result_delete_element_dataset")

    @property
    def result_dataset_after_add_element(self):
        return self._dataset("result_add_element_dataset")

    @property
    def deleted_dataset(self):
        return {"pkey": self.timeuuid, "ckey": self.timeuuid}

    def insert_operation_tmpl(self, preimage_enable, postimage_enable):
        for first in [True, False]:
            timestamp = self.get_next_timestamp()
            self.insert_one_with_timestamp(self.inserted_dataset, timestamp)
            self.verify_cdc_log_rows_after_insert_to_base_table(self.get_cdc_log_records_by_timestamp(timestamp), preimage_enable, postimage_enable, first_record=first)

    def collection_update_tmpl(self, preimage_enable, postimage_enable, add_element=False, remove_element=False):
        timestamp = self.get_next_timestamp()
        self.update_one_with_timestamp(self.inserted_dataset, timestamp)
        self.verify_cdc_log_rows_after_update_to_base_table(self.get_cdc_log_records_by_timestamp(timestamp), preimage_enable, postimage_enable, first_record=True)
        timestamp = self.get_next_timestamp()
        if add_element:
            self.update_collection_with_element_with_timestamp(self.added_element_dataset, timestamp, add=True)
        elif remove_element:
            self.update_collection_with_element_with_timestamp(self.deleted_element_dataset, timestamp, add=False)
        else:
            self.update_one_with_timestamp(self.updated_dataset, timestamp)
        self.verify_cdc_log_rows_after_update_to_base_table(self.get_cdc_log_records_by_timestamp(timestamp), preimage_enable, postimage_enable, add_element, remove_element)

    def update_collection_with_add_element(self, preimage_enable, postimage_enable):
        self.collection_update_tmpl(preimage_enable, postimage_enable, add_element=True)

    def update_collection_with_delete_element(self, preimage_enable, postimage_enable):
        self.collection_update_tmpl(preimage_enable, postimage_enable, remove_element=True)

    def collection_delete_tmpl(self, preimage_enable, postimage_enable):
        self.insert_one_with_timestamp(self.inserted_dataset, self.get_next_timestamp())
        timestamp = self.get_next_timestamp()
        self.delete_one_with_timestamp(self.deleted_dataset, timestamp)
        self._check_collection_in_cdc_log_rows(self.get_cdc_log_records_by_timestamp(timestamp), UPDATE, preimage_enable, postimage_enable,
                                               self.inserted_dataset, self.null_value_dataset, self.null_value_dataset, deleted_col=["value"])

    def verify_cdc_log_rows_after_insert_to_base_table(self, cdc_log_data, preimage_enable, postimage_enable, first_record=False):
        preimage_dataset = self.inserted_dataset if preimage_enable and not first_record else None
        self._check_collection_in_cdc_log_rows(cdc_log_data, INSERT, preimage_enable, postimage_enable, preimage_dataset,
                                               self.inserted_dataset, self.inserted_dataset, deleted_col=["value"])

    def verify_cdc_log_rows_after_update_to_base_table(self, cdc_log_data, preimage_enable, postimage_enable, add_element=None, remove_element=None, first_record=False):
        preimage_dataset = self.inserted_dataset if preimage_enable and not first_record else None
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
        self._check_collection_in_cdc_log_rows(cdc_log_data, UPDATE, preimage_enable, postimage_enable, preimage_dataset,
                                               updating_dataset, postimage_dataset, deleted_col=deleted_col, deleted_element=deleted_element)

    def _check_collection_in_cdc_log_rows(self, cdc_log_rows, base_operation, preimage_enable, postimage_enable, preimage_expected_data,
                                          delta_expected_data, postimage_expected_data, deleted_col=None, deleted_element=None):
        delta_index = 0
        postimage_index = 1
        if preimage_enable and preimage_expected_data:
            delta_index += 1
            postimage_index += 1
            self.check_cdc_log_row_collection(cdc_log_rows[0], operation=PREIMAGE, batch_seq=0, expected_data=preimage_expected_data, preimage_enabled=preimage_enable)
        # As in the dtests, deleted_element is not checked: the delta row's
        # check only gets deleted_col.
        self.check_cdc_log_row_collection(cdc_log_rows[delta_index], operation=base_operation, batch_seq=delta_index, expected_data=delta_expected_data, deleted_col=deleted_col)
        if postimage_enable:
            self.check_cdc_log_row_collection(cdc_log_rows[postimage_index], operation=POSTIMAGE, batch_seq=postimage_index, expected_data=postimage_expected_data)


def collections_type(cql, keyspace, columns_data, scenario, preimage_enable=False, postimage_enable=False):
    with create_table(cql, keyspace, "timeuuid", columns_data["cl_type"], preimage_enable, postimage_enable) as table:
        getattr(CollectionsType(cql, table, columns_data), scenario)(preimage_enable, postimage_enable)


collection_params = params_of(collections_types)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_insert.
# INSERT of a whole collection into an empty row, then again: the delta
# also has the column's "deleted" flag (the old value is overwritten).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_insert(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_insert_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_insert_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_insert_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_insert_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_insert_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_insert_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_insert_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_insert_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "insert_operation_tmpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_add_element.
# UPDATE of the whole collection into an empty row, then UPDATE adding
# elements: the delta has just the added elements.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_add_element(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_add_element")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_add_element_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_add_element_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_add_element", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_add_element_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_add_element_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_add_element", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_add_element_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_add_element_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_add_element", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_add_element_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_add_element_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_add_element", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_delete_element.
# UPDATE of the whole collection into an empty row, then UPDATE removing
# elements.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_delete_element(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_delete_element")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_delete_element_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_delete_element_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_delete_element", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_delete_element_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_delete_element_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_delete_element", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_delete_element_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_delete_element_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_delete_element", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_delete_element_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_delete_element_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "update_collection_with_delete_element", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection.
# UPDATE of the whole collection into an empty row, then to another value.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_update_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_update_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_update_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_update_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_update_collection_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_update_collection_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_update_tmpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_delete.
# DELETE of an inserted collection column.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_delete(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_delete_tmpl")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_delete_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_delete_with_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_delete_tmpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_delete_with_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_delete_with_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_delete_tmpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_delete_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_delete_with_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_delete_tmpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCDCCollectionsType::test_collection_delete_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", collection_params)
def test_collection_delete_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    collections_type(cql, test_keyspace_vnodes, columns_data, "collection_delete_tmpl", preimage_enable=True, postimage_enable=True)



# TestCdcUDT: frozen and non-frozen UDTs, with bigint keys.

class UDTType(CdcTools):
    @property
    def insert_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": self.columns_data["ins_dataset"]}

    @property
    def update_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": self.columns_data["upd_dataset"]}

    @property
    def updating_field_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": dict(self.columns_data["update_udt_element"])}

    @property
    def deleting_field_dataset(self):
        return {"pkey": 1, "ckey": 1, "value": dict(self.columns_data["delete_udt_element"])}

    def verify_cdc_log_rows_with_udt(self, expected_operation, log_rows, expected_udt_result, preimage_enable=False, postimage_enable=False):
        delta_index = 0
        postimage_index = 1
        if preimage_enable:
            delta_index = 1
            postimage_index = 2
            self.check_cdc_log_row_udt(log_rows[0], operation=PREIMAGE, batch_seq=0, expected_data=expected_udt_result["preimage"], preimage_enabled=preimage_enable)
        self.check_cdc_log_row_udt(log_rows[delta_index], operation=expected_operation, batch_seq=delta_index, expected_data=expected_udt_result["delta"])
        if postimage_enable:
            self.check_cdc_log_row_udt(log_rows[postimage_index], operation=POSTIMAGE, batch_seq=postimage_index, expected_data=expected_udt_result["postimage"])

    def write_twice(self, write, operation, preimage_enable, postimage_enable):
        ins, upd = self.columns_data["ins_dataset"], self.columns_data["upd_dataset"]
        timestamp = self.get_next_timestamp()
        write(self.insert_dataset, timestamp)
        # The first record in the partition has no preimage.
        self.verify_cdc_log_rows_with_udt(operation, self.get_cdc_log_records_by_timestamp(timestamp), {"delta": ins, "postimage": ins}, False, postimage_enable)
        timestamp = self.get_next_timestamp()
        write(self.update_dataset, timestamp)
        self.verify_cdc_log_rows_with_udt(operation, self.get_cdc_log_records_by_timestamp(timestamp),
                                          {"preimage": ins, "delta": upd, "postimage": upd}, preimage_enable, postimage_enable)

    def insert_udt_tpl(self, preimage_enable, postimage_enable):
        self.write_twice(self.insert_one_with_timestamp, INSERT, preimage_enable, postimage_enable)

    def update_udt_tpl(self, preimage_enable, postimage_enable):
        self.write_twice(self.update_one_with_timestamp, UPDATE, preimage_enable, postimage_enable)

    def udt_update_field_on_non_frozen(self, preimage_enable, postimage_enable, remove_field_value):
        using_data_set = self.deleting_field_dataset if remove_field_value else self.updating_field_dataset
        self.update_one_with_timestamp(self.insert_dataset, self.get_next_timestamp())
        timestamp = self.get_next_timestamp()
        self.update_udt_with_element_with_timestamp(using_data_set, timestamp)
        expected = {"preimage": self.columns_data["ins_dataset"]}
        if remove_field_value:
            expected["delta"] = self.columns_data["delete_element_delta_result"]
            expected["postimage"] = self.columns_data["postimage_del_element_dataset"]
        else:
            expected["delta"] = self.columns_data["update_element_delta_result"]
            expected["postimage"] = self.columns_data["postimage_upd_element_dataset"]
        self.verify_cdc_log_rows_with_udt(UPDATE, self.get_cdc_log_records_by_timestamp(timestamp), expected, preimage_enable, postimage_enable)

    def update_field(self, preimage_enable, postimage_enable):
        self.udt_update_field_on_non_frozen(preimage_enable, postimage_enable, remove_field_value=False)

    def delete_field(self, preimage_enable, postimage_enable):
        self.udt_update_field_on_non_frozen(preimage_enable, postimage_enable, remove_field_value=True)


def udt_type(cql, keyspace, columns_data, scenario, preimage_enable=False, postimage_enable=False):
    cl_type = columns_data["cl_type"]
    fields = ", ".join(f"{name} {typ}" for name, typ in cl_type["fields"].items())
    with new_type(cql, keyspace, f"({fields})") as udt:
        cql.cluster.register_user_type(keyspace, udt.split(".")[1], CustomUDT)
        typ = f"frozen<{udt}>" if cl_type["frozen"] else udt
        with create_table(cql, keyspace, "bigint", typ, preimage_enable, postimage_enable) as table:
            getattr(UDTType(cql, table, columns_data), scenario)(preimage_enable, postimage_enable)


udt_params = params_of(udt_types)


def skip_frozen(columns_data):
    """Like the dtests, field updates are skipped for frozen UDTs."""
    if columns_data["cl_type"]["frozen"]:
        skip_env("Update UDT field for frozen UDT is not supported")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_insert_udt_delta.
# INSERT of a UDT value into an empty row, then of another value.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_insert_udt_delta(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "insert_udt_tpl")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_insert_udt_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_insert_udt_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "insert_udt_tpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_insert_udt_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_insert_udt_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "insert_udt_tpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_insert_udt_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_insert_udt_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "insert_udt_tpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_insert_udt_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_insert_udt_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "insert_udt_tpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_udt_delta.
# UPDATE of a UDT value into an empty row, then to another value.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_udt_delta(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_udt_tpl")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_udt_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_udt_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_udt_tpl", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_udt_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_udt_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_udt_tpl", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_udt_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_udt_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_udt_tpl", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_udt_preimage_postimage.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_udt_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_udt_tpl", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_field_non_frozen_udt.
# UPDATE of some fields of a non-frozen UDT value (skipped for frozen
# UDTs): the delta has just the written fields.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_field_non_frozen_udt(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_field")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_field_non_frozen_udt_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_field_non_frozen_udt_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_field", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_field_non_frozen_udt_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_field_non_frozen_udt_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_field", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_field_non_frozen_udt_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_field_non_frozen_udt_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_field", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_update_field_non_frozen_udt_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_update_field_non_frozen_udt_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "update_field", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_delete_field_value_in_non_frozen_udt.
# UPDATE of some fields of a non-frozen UDT value to null.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_delete_field_value_in_non_frozen_udt(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "delete_field")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_delete_field_value_in_non_frozen_udt_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_delete_field_value_in_non_frozen_udt_preimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "delete_field", preimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_delete_field_value_in_non_frozen_udt_preimage_full.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_delete_field_value_in_non_frozen_udt_preimage_full(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "delete_field", preimage_enable="full")


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_delete_field_value_in_non_frozen_udt_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_delete_field_value_in_non_frozen_udt_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "delete_field", postimage_enable=True)


# Ported from scylla-dtest cdc_types_test.py::TestCdcUDT::test_delete_value_in_non_frozen_udt_field_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("columns_data", udt_params)
def test_delete_value_in_non_frozen_udt_field_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, columns_data):
    skip_frozen(columns_data)
    udt_type(cql, test_keyspace_vnodes, columns_data, "delete_field", preimage_enable=True, postimage_enable=True)

