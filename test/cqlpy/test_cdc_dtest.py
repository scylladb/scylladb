# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the contents of the CDC log - delta rows, preimages and
# postimages - for writes to columns of many types: native types, frozen
# collections (as key and regular columns), non-frozen collections, and
# frozen and non-frozen user-defined types. CDC is a Scylla-only feature,
# so all tests here are scylla_only.
#
# These tests were translated from scylla-dtest's cdc_types_test.py. That
# test created a separate table - and a separate cluster - for each column
# type, each kind of write and each CDC image mode. To be fast, the tests
# here share one table for each image mode ("delta", "preimage",
# "preimage_full", "postimage" and "preimage_postimage"):
# * The table has a column for each type tested: The partition key has a
#   pk_<type> column for each native type and frozen collection, and the
#   clustering key has a ck_<type> column for each. There are regular
#   columns value_<type> and always_null_<type> for each of these types,
#   for the non-frozen collections and for the user-defined types.
# * Each test writes all the columns of the types it tests in each write,
#   and checks the CDC log entry of each of these columns.
# * Each test uses its own partition, by using its own value for the
#   additional partition-key column "scenario", so it starts, like the
#   dtest did, from an empty partition, and only reads its own CDC log rows.
#############################################################################

import re
import time
from contextlib import ExitStack
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid1

import pytest
from cassandra.encoder import Encoder
from cassandra.query import SimpleStatement
from cassandra.util import OrderedMapSerializedKey, Time

from .util import new_test_table, new_type


# Values of the cdc$operation column of the CDC log
PREIMAGE = 0
UPDATE = 1
INSERT = 2
POSTIMAGE = 9

# The native types tested, and the values written to them: ins is written
# first, and upd is written in the updates. These types are also used for
# key columns. The dtest also tested varchar - which is just another name
# for text - and listed varint twice.
NATIVE_TYPES = [
    {'cl_type': 'bigint', 'ins': 1, 'upd': 2},
    {'cl_type': 'int', 'ins': 3, 'upd': 4},
    {'cl_type': 'smallint', 'ins': 5, 'upd': 6},
    {'cl_type': 'tinyint', 'ins': 8, 'upd': 7},
    {'cl_type': 'varint', 'ins': 1, 'upd': 4},
    {'cl_type': 'boolean', 'ins': True, 'upd': False},
    {'cl_type': 'blob', 'ins': b'1234567890qwertyuiop', 'upd': b'a'},
    {'cl_type': 'date', 'ins': date(2020, 2, 2), 'upd': date(2020, 12, 12)},
    {'cl_type': 'decimal', 'ins': Decimal('10.1'), 'upd': Decimal('12.2')},
    {'cl_type': 'double', 'ins': 10.1000001, 'upd': 22.22222},
    {'cl_type': 'float', 'ins': 33.33000183105469, 'upd': 44.44000244140625},
    {'cl_type': 'inet', 'ins': '1.1.1.1', 'upd': '2.2.2.2'},
    {'cl_type': 'time', 'ins': Time('02:02:02.222'), 'upd': Time('12:12:12.121')},
    {'cl_type': 'timestamp', 'ins': datetime(2020, 2, 2, 2, 2, 2), 'upd': datetime(2020, 3, 3, 3, 3, 3)},
    {'cl_type': 'timeuuid', 'ins': UUID('b478b7c2-5d3c-11ea-84b5-5aa95d83d60f'), 'upd': UUID('c2ecebac-5d3c-11ea-9fd2-3cd5439c36c3')},
    {'cl_type': 'uuid', 'ins': uuid1(), 'upd': uuid1()},
    {'cl_type': 'text', 'ins': 'aaaaaaa', 'upd': 'bbbbbbb'},
    {'cl_type': 'ascii', 'ins': '0123456789abcdef', 'upd': 'abcdef0123456789'},
    # Frozen collections, which can also be used as keys
    {'cl_type': 'frozen<map<text, text>>', 'ins': {'key1': 'value1', 'key2': 'value2'}, 'upd': {'key3': 'value3', 'key4': 'value4'}},
    {'cl_type': 'frozen<set<text>>', 'ins': {'value1', 'value2'}, 'upd': {'value3', 'value4'}},
    {'cl_type': 'frozen<list<text>>', 'ins': ['value1', 'value2'], 'upd': ['value3', 'value4']},
    {'cl_type': 'frozen<list<int>>', 'ins': [1, 12], 'upd': [3, 13]},
]

# The non-frozen collection types tested: ins is written first and upd is
# written in an update; add_el is added to ins and del_el is removed from
# it, with the results add_result and del_result.
COLLECTION_TYPES = [
    {
        'cl_type': 'map<text, text>',
        'ins': {'key1': 'value1', 'key2': 'value2'},
        'upd': {'key3': 'value3', 'key4': 'value4'},
        'add_el': {'key5': 'value5'},
        'del_el': {'key1'},
        'add_result': {'key1': 'value1', 'key2': 'value2', 'key5': 'value5'},
        'del_result': {'key2': 'value2'},
    },
    {
        'cl_type': 'map<bigint, text>',
        'ins': {1: 'value1', 10000: 'value2'},
        'upd': {2000: 'value3', 3: 'value4'},
        'add_el': {5000: 'value5'},
        'del_el': {1},
        'add_result': {1: 'value1', 10000: 'value2', 5000: 'value5'},
        'del_result': {10000: 'value2'},
    },
    {
        'cl_type': 'set<text>',
        'ins': {'value1', 'value2'},
        'upd': {'value3', 'value4'},
        'add_el': {'value5'},
        'del_el': {'value1'},
        'add_result': {'value1', 'value2', 'value5'},
        'del_result': {'value2'},
    },
    {'cl_type': 'list<int>', 'ins': [1, 2], 'upd': [3, 4], 'add_el': [5], 'del_el': [1], 'add_result': [1, 2, 5], 'del_result': [2]},
]

# A value of a user-defined type, written or expected by the tests: an
# object whose attributes are the type's fields. The driver writes an object
# of a class it doesn't know as str() of the object, so str() returns the
# value as a CQL literal.
_encoder = Encoder()
class CustomUDT:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.__dict__[key] = value

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return '{' + ', '.join(f'{field}: {_encoder.cql_encode_all_types(value)}' for field, value in self.__dict__.items()) + '}'

# The user-defined types tested: ins is written first and upd is written
# in an update. For the non-frozen types, update_fields are the fields set
# in an update of individual fields, and delete_fields the fields deleted
# (set to null) in one, and the other entries are the expected delta and
# postimage after each of these updates.
UDT_TYPES = [
    {
        'name': 'non_frozen_udt_with_native_types', 'frozen': False, 'fields': {'f_text': 'text', 'f_bigint': 'bigint'},
        'ins': CustomUDT(f_text='text', f_bigint=1),
        'upd': CustomUDT(f_text='text2', f_bigint=3),
        'update_fields': {'f_text': 'newtext'},
        'delete_fields': {'f_text': None},
        'update_fields_delta': CustomUDT(f_text='newtext', f_bigint=None),
        'delete_fields_delta': CustomUDT(f_text=None, f_bigint=None),
        'update_fields_postimage': CustomUDT(f_text='newtext', f_bigint=1),
        'delete_fields_postimage': CustomUDT(f_text=None, f_bigint=1),
    },
    {
        'name': 'non_frozen_udt_with_native_types_3_fields', 'frozen': False, 'fields': {'f_varchar': 'text', 'f_int': 'int', 'f_timestamp': 'timestamp'},
        'ins': CustomUDT(f_varchar='text', f_int=1, f_timestamp=datetime(2020, 2, 2, 2, 2, 2)),
        'upd': CustomUDT(f_varchar='text2', f_int=3, f_timestamp=datetime(2021, 3, 3, 3, 3, 3)),
        'update_fields': {'f_varchar': 'newtext', 'f_timestamp': datetime(2024, 4, 4, 4, 4, 4)},
        'delete_fields': {'f_varchar': None, 'f_timestamp': None},
        'update_fields_delta': CustomUDT(f_varchar='newtext', f_timestamp=datetime(2024, 4, 4, 4, 4, 4), f_int=None),
        'delete_fields_delta': CustomUDT(f_varchar=None, f_timestamp=None, f_int=None),
        'update_fields_postimage': CustomUDT(f_varchar='newtext', f_timestamp=datetime(2024, 4, 4, 4, 4, 4), f_int=1),
        'delete_fields_postimage': CustomUDT(f_varchar=None, f_timestamp=None, f_int=1),
    },
    {
        'name': 'frozen_udt_with_collection', 'frozen': True, 'fields': {'f_text': 'text', 'f_map': 'map<int,text>'},
        'ins': CustomUDT(f_text='text', f_map={1: 'text'}),
        'upd': CustomUDT(f_text='text2', f_map={3: 'new_text'}),
    },
    {
        'name': 'frozen_udt_with_several_collection', 'frozen': True, 'fields': {'f_text': 'text', 'f_map': 'map<int,text>', 'f_list': 'list<int>'},
        'ins': CustomUDT(f_text='text', f_map={1: 'text'}, f_list=[1, 2, 3]),
        'upd': CustomUDT(f_text='text2', f_map={3: 'new_text'}, f_list=[4, 5, 6]),
    },
    {
        'name': 'frozen_udt_with_frozen_collection', 'frozen': True, 'fields': {'f_text': 'text', 'f_map': 'frozen<map<int,text>>'},
        'ins': CustomUDT(f_text='text', f_map={1: 'text'}),
        'upd': CustomUDT(f_text='text2', f_map={3: 'new_text'}),
    },
    {
        'name': 'udt_with_frozen_collection', 'frozen': False, 'fields': {'f_text': 'text', 'f_map': 'frozen<set<ascii>>'},
        'ins': CustomUDT(f_text='text', f_map={'text'}),
        'upd': CustomUDT(f_text='text2', f_map={'new_text'}),
        'update_fields': {'f_text': 'newtext'},
        'delete_fields': {'f_text': None},
        'update_fields_delta': CustomUDT(f_text='newtext', f_map=None),
        'delete_fields_delta': CustomUDT(f_text=None, f_map=None),
        'update_fields_postimage': CustomUDT(f_text='newtext', f_map={'text'}),
        'delete_fields_postimage': CustomUDT(f_text=None, f_map={'text'}),
    },
]
NON_FROZEN_UDT_TYPES = [u for u in UDT_TYPES if not u['frozen']]

# A column-name suffix for a type, like "frozen_map_text_text"
def ident(cl_type):
    return re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', cl_type)).strip('_')

for t in NATIVE_TYPES + COLLECTION_TYPES:
    t['id'] = ident(t['cl_type'])
for t in UDT_TYPES:
    t['id'] = t['name']

# The image modes: the preimage and postimage options of each table
IMAGE_MODES = {
    'delta': (False, False),
    'preimage': (True, False),
    'preimage_full': ('full', False),
    'postimage': (False, True),
    'preimage_postimage': (True, True),
}

# A table for each image mode, with the columns described at the top of
# this file. Returns a dict from image-mode name to table name.
@pytest.fixture(scope='module')
def cdc_tables(cql, test_keyspace, scylla_only):
    with ExitStack() as stack:
        udt_column_types = {}
        for udt in UDT_TYPES:
            fields = ', '.join(f'{name} {t}' for name, t in udt['fields'].items())
            type_name = stack.enter_context(new_type(cql, test_keyspace, f'({fields})'))
            udt_column_types[udt['id']] = f'frozen<{type_name}>' if udt['frozen'] else type_name
        columns = ['scenario int']
        columns += [f'pk_{t["id"]} {t["cl_type"]}' for t in NATIVE_TYPES]
        columns += [f'ck_{t["id"]} {t["cl_type"]}' for t in NATIVE_TYPES]
        for t in NATIVE_TYPES + COLLECTION_TYPES:
            columns += [f'value_{t["id"]} {t["cl_type"]}', f'always_null_{t["id"]} {t["cl_type"]}']
        for udt_id, udt_type in udt_column_types.items():
            columns += [f'value_{udt_id} {udt_type}', f'always_null_{udt_id} {udt_type}']
        pk = ', '.join(['scenario'] + [f'pk_{t["id"]}' for t in NATIVE_TYPES])
        ck = ', '.join(f'ck_{t["id"]}' for t in NATIVE_TYPES)
        schema = ', '.join(columns) + f', PRIMARY KEY (({pk}), {ck})'
        tables = {}
        for mode, (preimage, postimage) in IMAGE_MODES.items():
            options = "{'enabled': true"
            if preimage:
                options += f", 'preimage': '{str(preimage).lower()}'"
            if postimage:
                options += ", 'postimage': true"
            options += '}'
            tables[mode] = stack.enter_context(new_test_table(cql, test_keyspace, schema, f'WITH cdc = {options}'))
        yield tables

# Each test uses its own scenario number - and therefore its own
# partition - and each write uses a different timestamp, at least a
# millisecond apart, so the CDC log rows of each write can be read by their
# time. CDC doesn't allow timestamps too far in the past, so the timestamps
# start from the current time. Each timestamp is in the middle of a
# millisecond, because CDC shows the deletion of a collection at timestamp T
# as a change at T+1 (see cdc/split.cc), which must be in the same
# millisecond as T.
_last_scenario = 0
def new_scenario():
    global _last_scenario
    _last_scenario += 1
    return _last_scenario

_last_timestamp = 0
def next_timestamp():
    global _last_timestamp
    _last_timestamp = max(_last_timestamp + 1000, int(time.time() * 1000) * 1000 + 500)
    return _last_timestamp

# The key columns of a scenario's row and their values
def keys(scenario):
    return {'scenario': scenario, **{f'pk_{t["id"]}': t['ins'] for t in NATIVE_TYPES}, **{f'ck_{t["id"]}': t['ins'] for t in NATIVE_TYPES}}

def where_keys(scenario):
    return ' AND '.join(f'{column} = %({column})s' for column in keys(scenario))

def insert(cql, table, data, timestamp):
    columns = ', '.join(data)
    values = ', '.join(f'%({column})s' for column in data)
    cql.execute(SimpleStatement(f'INSERT INTO {table} ({columns}) VALUES ({values}) USING TIMESTAMP {timestamp}'), data)

def update(cql, table, scenario, data, timestamp, op=''):
    # op is '' to set the columns, or '+' or '-' to add elements to or
    # remove elements from collection columns.
    assignments = ', '.join(f'{column} = {column} {op} %({column})s' if op else f'{column} = %({column})s'
                            for column in data if column not in keys(scenario))
    cql.execute(SimpleStatement(f'UPDATE {table} USING TIMESTAMP {timestamp} SET {assignments} WHERE {where_keys(scenario)}'),
                {**data, **keys(scenario)})

def update_udt_fields(cql, table, scenario, fields_by_column, timestamp):
    params = {}
    assignments = []
    for column, fields in fields_by_column.items():
        for field, value in fields.items():
            params[f'{column}__{field}'] = value
            assignments.append(f'{column}.{field} = %({column}__{field})s')
    cql.execute(SimpleStatement(f'UPDATE {table} USING TIMESTAMP {timestamp} SET {", ".join(assignments)} WHERE {where_keys(scenario)}'),
                {**params, **keys(scenario)})

def delete(cql, table, scenario, columns, timestamp):
    cql.execute(SimpleStatement(f'DELETE {", ".join(columns)} FROM {table} USING TIMESTAMP {timestamp} WHERE {where_keys(scenario)}'),
                keys(scenario))

def log_rows_by_timestamp(cql, table, timestamp):
    ms = timestamp // 1000
    return list(cql.execute(f'SELECT * FROM {table}_scylla_cdc_log WHERE "cdc$time" >= minTimeuuid({ms}) AND "cdc$time" <= maxTimeuuid({ms}) ALLOW FILTERING'))

def log_rows_of_scenario(cql, table, scenario):
    return list(cql.execute(f'SELECT * FROM {table}_scylla_cdc_log WHERE scenario = {scenario} ALLOW FILTERING'))

# In a preimage row, the cdc$deleted_ column of a column never written is
# true with a full preimage, and null with a non-full preimage, which only
# includes the columns that the write changed.
def check_always_null_in_preimage(row, ids, preimage_enabled):
    if row.cdc_operation != PREIMAGE:
        return
    for i in ids:
        deleted = getattr(row, f'cdc_deleted_always_null_{i}')
        if preimage_enabled == 'full':
            assert deleted is True, i
        if preimage_enabled is True:
            assert deleted is None, i

def check_deleted_columns(row, columns):
    for column in columns or []:
        assert getattr(row, f'cdc_deleted_{column}'), column

#############################################################################
# Native types and frozen collections, as keys and regular columns.
# Translated from scylla-dtest's cdc_types_test.py::TestCDCNativeType.
#############################################################################

NATIVE_IDS = [t['id'] for t in NATIVE_TYPES]

def native_dataset(scenario, value=None, with_value=True):
    data = keys(scenario)
    if with_value:
        data.update({f'value_{t["id"]}': (t[value] if value else None) for t in NATIVE_TYPES})
    return data

def inserted(s):
    return native_dataset(s, 'ins')

def updated(s):
    return native_dataset(s, 'upd')

def updated_with_null(s):
    return native_dataset(s)

def deleted(s):
    return native_dataset(s, with_value=False)

NATIVE_VALUE_COLUMNS = [f'value_{i}' for i in NATIVE_IDS]

def check_native_log_row(row, operation, batch_seq, expected, deleted_columns=None, preimage_enabled=None):
    assert row.cdc_operation == operation
    assert row.cdc_batch_seq_no == batch_seq
    check_always_null_in_preimage(row, NATIVE_IDS, preimage_enabled)
    check_deleted_columns(row, deleted_columns)
    for column, value in expected.items():
        assert getattr(row, column) == value, column

# Check the CDC log rows of one write: a preimage row (if enabled and the
# row existed before), a delta row and a postimage row (if enabled).
def check_native_log_rows(rows, operation, preimage_enabled, postimage_enabled, preimage_expected, delta_expected,
                          postimage_expected, deleted_columns=None, first_record=False):
    delta_index = 0
    postimage_index = 1
    if preimage_enabled and not first_record:
        delta_index += 1
        postimage_index += 1
        check_native_log_row(rows[0], PREIMAGE, 0, preimage_expected, preimage_enabled=preimage_enabled)
    check_native_log_row(rows[delta_index], operation, delta_index, delta_expected, deleted_columns)
    if postimage_enabled:
        check_native_log_row(rows[postimage_index], POSTIMAGE, postimage_index, postimage_expected)
    assert len(rows) == (postimage_index if postimage_enabled else delta_index) + 1

# Test the CDC log rows of two INSERTs of the same row: the first creates
# the row, and the second overwrites it with the same values.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_native_types_insert(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    for first_record in [True, False]:
        timestamp = next_timestamp()
        insert(cql, table, inserted(s), timestamp)
        check_native_log_rows(log_rows_by_timestamp(cql, table, timestamp), INSERT, pre, post,
                              inserted(s), inserted(s), inserted(s), first_record=first_record)

# Test the CDC log rows of two UPDATEs of the same row: the first creates
# the row, and the second changes its values.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_native_types_update(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    timestamp = next_timestamp()
    update(cql, table, s, inserted(s), timestamp)
    check_native_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                          inserted(s), inserted(s), inserted(s), first_record=True)
    timestamp = next_timestamp()
    update(cql, table, s, updated(s), timestamp)
    check_native_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                          inserted(s), updated(s), updated(s))

# Test the CDC log rows of an UPDATE setting the values to null.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_native_types_update_with_null(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    update(cql, table, s, inserted(s), next_timestamp())
    timestamp = next_timestamp()
    update(cql, table, s, updated_with_null(s), timestamp)
    check_native_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                          inserted(s), updated_with_null(s), updated_with_null(s))

# Test the CDC log rows of a DELETE of the values.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_native_types_delete(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    insert(cql, table, inserted(s), next_timestamp())
    timestamp = next_timestamp()
    delete(cql, table, s, NATIVE_VALUE_COLUMNS, timestamp)
    check_native_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                          inserted(s), deleted(s), updated_with_null(s), deleted_columns=NATIVE_VALUE_COLUMNS)

# Test all the CDC log rows of an INSERT, an UPDATE and a DELETE of the
# same row.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_native_types_all_operations(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    insert(cql, table, inserted(s), next_timestamp())
    update(cql, table, s, updated(s), next_timestamp())
    delete(cql, table, s, NATIVE_VALUE_COLUMNS, next_timestamp())
    rows = log_rows_of_scenario(cql, table, s)
    if pre and post:
        assert len(rows) == 8
        insert_rows, update_rows, delete_rows = rows[:2], rows[2:5], rows[5:]
    elif pre:
        assert len(rows) == 5
        insert_rows, update_rows, delete_rows = rows[:1], rows[1:3], rows[3:]
    elif post:
        assert len(rows) == 6
        insert_rows, update_rows, delete_rows = rows[:2], rows[2:4], rows[4:]
    else:
        assert len(rows) == 3
        insert_rows, update_rows, delete_rows = [rows[0]], [rows[1]], [rows[2]]
    check_native_log_rows(insert_rows, INSERT, pre, post, inserted(s), inserted(s), inserted(s), first_record=True)
    check_native_log_rows(update_rows, UPDATE, pre, post, inserted(s), updated(s), updated(s))
    check_native_log_rows(delete_rows, UPDATE, pre, post, updated(s), deleted(s), updated_with_null(s),
                          deleted_columns=NATIVE_VALUE_COLUMNS)

#############################################################################
# Non-frozen collections.
# Translated from scylla-dtest's cdc_types_test.py::TestCDCCollectionsType.
#############################################################################

COLLECTION_IDS = [t['id'] for t in COLLECTION_TYPES]
COLLECTION_VALUE_COLUMNS = [f'value_{i}' for i in COLLECTION_IDS]

def collection_dataset(scenario, value=None, with_value=True, with_always_null=False):
    data = keys(scenario)
    if with_value:
        data.update({f'value_{t["id"]}': (t[value] if value else None) for t in COLLECTION_TYPES})
    if with_always_null:
        data.update({f'always_null_{t["id"]}': None for t in COLLECTION_TYPES})
    return data

# Check the expected values of a log row. For a collection column, the
# delta row has the written elements, and for a list it is a map from
# the elements' keys (timeuuids, which sort in the list's order) to their
# values, so for a list compare the values in key order.
def check_collection_values(row, expected):
    for column, value in expected.items():
        actual = getattr(row, column)
        if column not in COLLECTION_VALUE_COLUMNS:
            assert actual == value, column
        elif not value:
            assert not actual, column
        elif isinstance(actual, OrderedMapSerializedKey) and isinstance(value, list):
            assert list(actual.values()) == value, column
        elif isinstance(actual, OrderedMapSerializedKey):
            assert dict(actual.items()) == value, column
        else:
            assert actual == value, column

def check_deleted_elements(row, deleted_elements):
    for column, elements in deleted_elements.items():
        deleted = getattr(row, f'cdc_deleted_elements_{column}')
        if isinstance(elements, list):
            # A list's elements are deleted by their keys (timeuuids), so
            # only their number is checked
            assert len(deleted) == len(elements), column
        else:
            assert set(deleted) == set(elements), column

def check_collection_log_row(row, operation, batch_seq, expected, deleted_columns=None, deleted_elements=None, preimage_enabled=None):
    assert row.cdc_operation == operation
    assert row.cdc_batch_seq_no == batch_seq
    check_always_null_in_preimage(row, COLLECTION_IDS, preimage_enabled)
    check_deleted_columns(row, deleted_columns)
    if deleted_elements:
        check_deleted_elements(row, deleted_elements)
    check_collection_values(row, expected)

def check_collection_log_rows(rows, operation, preimage_enabled, postimage_enabled, preimage_expected, delta_expected,
                              postimage_expected, deleted_columns=None, deleted_elements=None):
    delta_index = 0
    postimage_index = 1
    if preimage_enabled and preimage_expected:
        delta_index += 1
        postimage_index += 1
        check_collection_log_row(rows[0], PREIMAGE, 0, preimage_expected, preimage_enabled=preimage_enabled)
    check_collection_log_row(rows[delta_index], operation, delta_index, delta_expected, deleted_columns, deleted_elements)
    if postimage_enabled:
        check_collection_log_row(rows[postimage_index], POSTIMAGE, postimage_index, postimage_expected)
    assert len(rows) == (postimage_index if postimage_enabled else delta_index) + 1

# Test the CDC log rows of two INSERTs of the same row: the first creates
# the row, and the second overwrites it with the same values.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_collections_insert(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    inserted = collection_dataset(s, 'ins')
    for first_record in [True, False]:
        timestamp = next_timestamp()
        insert(cql, table, inserted, timestamp)
        preimage = inserted if pre and not first_record else None
        check_collection_log_rows(log_rows_by_timestamp(cql, table, timestamp), INSERT, pre, post,
                                  preimage, inserted, inserted, deleted_columns=COLLECTION_VALUE_COLUMNS)

# Test the CDC log rows of an UPDATE which creates the row, and then of an
# UPDATE which either sets the collections to new values, adds elements to
# them, or removes elements from them.
def check_collection_update(cql, cdc_tables, mode, op):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    inserted = collection_dataset(s, 'ins')
    timestamp = next_timestamp()
    update(cql, table, s, inserted, timestamp)
    check_collection_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                              None, inserted, inserted, deleted_columns=COLLECTION_VALUE_COLUMNS)
    timestamp = next_timestamp()
    preimage = inserted if pre else None
    if op == 'add':
        update(cql, table, s, collection_dataset(s, 'add_el'), timestamp, '+')
        check_collection_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                                  preimage, collection_dataset(s, 'add_el'), collection_dataset(s, 'add_result'))
    elif op == 'remove':
        update(cql, table, s, collection_dataset(s, 'del_el'), timestamp, '-')
        deleted_elements = {f'value_{t["id"]}': t['del_el'] for t in COLLECTION_TYPES}
        check_collection_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                                  preimage, collection_dataset(s, with_always_null=True), collection_dataset(s, 'del_result'),
                                  deleted_elements=deleted_elements)
    else:
        update(cql, table, s, collection_dataset(s, 'upd'), timestamp)
        check_collection_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                                  preimage, collection_dataset(s, 'upd'), collection_dataset(s, 'upd'),
                                  deleted_columns=COLLECTION_VALUE_COLUMNS)

@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_collections_update(cql, cdc_tables, mode):
    check_collection_update(cql, cdc_tables, mode, 'set')

@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_collections_add_elements(cql, cdc_tables, mode):
    check_collection_update(cql, cdc_tables, mode, 'add')

@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_collections_remove_elements(cql, cdc_tables, mode):
    check_collection_update(cql, cdc_tables, mode, 'remove')

# Test the CDC log rows of a DELETE of the collections.
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_collections_delete(cql, cdc_tables, mode):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    inserted = collection_dataset(s, 'ins')
    insert(cql, table, inserted, next_timestamp())
    timestamp = next_timestamp()
    delete(cql, table, s, COLLECTION_VALUE_COLUMNS, timestamp)
    null_value = collection_dataset(s, with_always_null=True)
    check_collection_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, pre, post,
                              inserted, null_value, null_value, deleted_columns=COLLECTION_VALUE_COLUMNS)

#############################################################################
# User-defined types.
# Translated from scylla-dtest's cdc_types_test.py::TestCdcUDT.
#############################################################################

def udt_dataset(scenario, udts, value):
    return {**keys(scenario), **{f'value_{u["id"]}': u[value] for u in udts}}

def check_udt_log_row(row, operation, batch_seq, udts, expected, preimage_enabled=None):
    assert row.cdc_operation == operation
    assert row.cdc_batch_seq_no == batch_seq
    check_always_null_in_preimage(row, [u['id'] for u in udts], preimage_enabled)
    for u in udts:
        assert getattr(row, f'value_{u["id"]}')._asdict() == u[expected].__dict__, u['id']

# Check the log rows of one write, where preimage, delta and postimage are
# the names of the entries in each UDT_TYPES item with the expected values.
def check_udt_log_rows(rows, operation, udts, preimage_enabled, postimage_enabled, preimage, delta, postimage):
    delta_index = 0
    postimage_index = 1
    if preimage_enabled:
        delta_index = 1
        postimage_index = 2
        check_udt_log_row(rows[0], PREIMAGE, 0, udts, preimage, preimage_enabled)
    check_udt_log_row(rows[delta_index], operation, delta_index, udts, delta)
    if postimage_enabled:
        check_udt_log_row(rows[postimage_index], POSTIMAGE, postimage_index, udts, postimage)
    assert len(rows) == (postimage_index if postimage_enabled else delta_index) + 1

# Test the CDC log rows of two INSERTs (or two UPDATEs) of the same row:
# the first creates the row, and the second changes its values.
@pytest.mark.parametrize('operation', [INSERT, UPDATE], ids=['insert', 'update'])
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_udt_insert_update(cql, cdc_tables, mode, operation):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    for first_record, value in [(True, 'ins'), (False, 'upd')]:
        timestamp = next_timestamp()
        if operation == INSERT:
            insert(cql, table, udt_dataset(s, UDT_TYPES, value), timestamp)
        else:
            update(cql, table, s, udt_dataset(s, UDT_TYPES, value), timestamp)
        rows = log_rows_by_timestamp(cql, table, timestamp)
        # The first write to the row has no preimage
        check_udt_log_rows(rows, operation, UDT_TYPES, False if first_record else pre, post, 'ins', value, value)

# Test the CDC log rows of an UPDATE of individual fields of non-frozen
# user-defined types: setting some of their fields to new values, or
# deleting them (setting them to null).
@pytest.mark.parametrize('fields', ['update_fields', 'delete_fields'])
@pytest.mark.parametrize('mode', IMAGE_MODES)
def test_cdc_udt_update_fields(cql, cdc_tables, mode, fields):
    table, (pre, post) = cdc_tables[mode], IMAGE_MODES[mode]
    s = new_scenario()
    update(cql, table, s, udt_dataset(s, NON_FROZEN_UDT_TYPES, 'ins'), next_timestamp())
    timestamp = next_timestamp()
    update_udt_fields(cql, table, s, {f'value_{u["id"]}': u[fields] for u in NON_FROZEN_UDT_TYPES}, timestamp)
    check_udt_log_rows(log_rows_by_timestamp(cql, table, timestamp), UPDATE, NON_FROZEN_UDT_TYPES, pre, post,
                       'ins', f'{fields}_delta', f'{fields}_postimage')
