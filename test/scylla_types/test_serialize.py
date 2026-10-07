#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


@pytest.mark.parametrize("type_name,value,serialized", [
    ("Int32Type", "1", "00000001"),
    ("Int32Type", "-1", "ffffffff"),
    ("Int32Type", "-1286905132", "b34b62d4"),
    ("LongType", "1", "0000000000000001"),
    ("BooleanType", "true", "01"),
    ("UTF8Type", "abc", "616263"),
    ("AsciiType", "abc", "616263"),
    ("SimpleDateType", "2021-03-27", "80004919"),
    ("UUIDType", "c61a3321-0459-41c3-8e56-75255feb0196", "c61a3321045941c38e5675255feb0196"),
    ("TimeUUIDType", "d0081989-6f6b-11ea-0000-0000001c571b", "d00819896f6b11ea00000000001c571b"),
    ("ReversedType(Int32Type)", "1", "00000001"),
])
def test_serialize(scylla_types, type_name, value, serialized):
    res = scylla_types("serialize", "-t", type_name, "--", value)
    assert res.stdout == f"{serialized}\n"


def test_serialize_invalid_value(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "-t", "Int32Type", "--", "abc", error="Invalid number format 'abc'")


def test_serialize_too_many_values(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "-t", "Int32Type", "--", "1", "2",
                            error="expected 1 value for non-compound type, got 2")


def test_serialize_prefix_compound(scylla_types):
    res = scylla_types("serialize", "--prefix-compound", "-t", "TimeUUIDType", "-t", "Int32Type", "--",
                       "d0081989-6f6b-11ea-0000-0000001c571b", "16")
    assert res.stdout == "0010d00819896f6b11ea00000000001c571b000400000010\n"


def test_serialize_prefix_compound_partial(scylla_types):
    res = scylla_types("serialize", "--prefix-compound", "-t", "TimeUUIDType", "-t", "Int32Type", "--",
                       "d0081989-6f6b-11ea-0000-0000001c571b")
    assert res.stdout == "0010d00819896f6b11ea00000000001c571b\n"


def test_serialize_prefix_compound_too_many_values(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "--prefix-compound", "-t", "Int32Type", "--", "1", "2",
                            error="expected at most 1 (number of subtypes) values for prefix compound type, got 2")


def test_serialize_full_compound(scylla_types):
    res = scylla_types("serialize", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "--", "1", "abc")
    assert res.stdout == "0004000000010003616263\n"


def test_serialize_full_compound_single_component(scylla_types):
    res = scylla_types("serialize", "--full-compound", "-t", "Int32Type", "--", "1")
    assert res.stdout == "000400000001\n"


def test_serialize_full_compound_too_few_values(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "--", "1",
                            error="expected 2 (number of subtypes) values for non-prefix compound type, got 1")


def test_serialize_tuple(scylla_types):
    res = scylla_types("serialize", "-t", "TupleType(Int32Type,UTF8Type)", "--", "1:a")
    assert res.stdout == "00000004000000010000000161\n"


@pytest.mark.parametrize("type_name", [
    "ListType(Int32Type)",
    "SetType(Int32Type)",
    "MapType(Int32Type,UTF8Type)",
    "FrozenType(ListType(Int32Type))",
    "ReversedType(ListType(Int32Type))",
    "VectorType(FloatType,3)",
    "TupleType(Int32Type,ListType(Int32Type))",
])
def test_serialize_unsupported_type(scylla_types_fails_with, type_name):
    """Values of collection and vector types cannot be serialized, the tool should reject them cleanly."""
    scylla_types_fails_with("serialize", "-t", type_name, "--", "1", error="is not supported")


def test_serialize_unsupported_type_in_compound(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "--prefix-compound", "-t", "Int32Type", "-t", "FrozenType(ListType(Int32Type))", "--", "1", "1",
                            error="error: serializing values of type frozen<list<int>> is not supported")


def test_serialize_value_with_spaces(scylla_types):
    res = scylla_types("serialize", "-t", "UTF8Type", "--", "a b c")
    assert res.stdout == "6120622063\n"


def test_serialize_full_compound_value_with_spaces(scylla_types):
    res = scylla_types("serialize", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "--", "1", "a b")
    assert res.stdout == "0004000000010003612062\n"


@pytest.mark.parametrize("cql_type,cassandra_type,value,serialized", [
    ("ascii", "AsciiType", "abc", "616263"),
    ("bigint", "LongType", "1", "0000000000000001"),
    ("blob", "BytesType", "0102", "0102"),
    ("boolean", "BooleanType", "true", "01"),
    ("counter", "CounterColumnType", "1", "0000000000000001"),
    ("date", "SimpleDateType", "2021-03-27", "80004919"),
    ("decimal", "DecimalType", "1.5", "000000010f"),
    ("double", "DoubleType", "1.5", "3ff8000000000000"),
    ("duration", "DurationType", "1h", "0000fc068c61714000"),
    ("float", "FloatType", "1.5", "3fc00000"),
    ("inet", "InetAddressType", "127.0.0.1", "7f000001"),
    ("int", "Int32Type", "1", "00000001"),
    ("smallint", "ShortType", "1", "0001"),
    ("text", "UTF8Type", "abc", "616263"),
    ("varchar", "UTF8Type", "abc", "616263"),
    ("time", "TimeType", "08:12:54", "00001ae5bbc3bc00"),
    ("timestamp", "TimestampType", "2021-03-27 10:00:00+0000", "0000017873204d00"),
    ("timeuuid", "TimeUUIDType", "d0081989-6f6b-11ea-0000-0000001c571b", "d00819896f6b11ea00000000001c571b"),
    ("tinyint", "ByteType", "1", "01"),
    ("uuid", "UUIDType", "c61a3321-0459-41c3-8e56-75255feb0196", "c61a3321045941c38e5675255feb0196"),
    ("varint", "IntegerType", "1", "01"),
])
def test_serialize_cql_type_name(scylla_types, cql_type, cassandra_type, value, serialized):
    """Types can be specified with their CQL name, which is equivalent to the cassandra type class name."""
    for type_name in (cql_type, cql_type.upper(), cassandra_type):
        res = scylla_types("serialize", "-t", type_name, "--", value)
        assert res.stdout == f"{serialized}\n"


def test_serialize_prefix_compound_cql_type_names(scylla_types):
    res = scylla_types("serialize", "--prefix-compound", "-t", "timeuuid", "-t", "int", "--",
                       "d0081989-6f6b-11ea-0000-0000001c571b", "16")
    assert res.stdout == "0010d00819896f6b11ea00000000001c571b000400000010\n"


@pytest.mark.parametrize("type_name", ["list<int>", "frozen<map<int, text>>", "vector<float, 3>", "tuple<int, frozen<set<int>>>"])
def test_serialize_unsupported_cql_type(scylla_types_fails_with, type_name):
    scylla_types_fails_with("serialize", "-t", type_name, "--", "1", error="is not supported")


def test_serialize_legacy_composite(scylla_types):
    res = scylla_types("serialize", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type", "--", "1", "abc")
    assert res.stdout == "00040000000100000361626300\n"


def test_serialize_legacy_composite_single_component(scylla_types):
    """Single-component keys are serialized as-is in the legacy composite format."""
    res = scylla_types("serialize", "--legacy-composite", "-t", "Int32Type", "--", "1")
    assert res.stdout == "00000001\n"


def test_serialize_legacy_composite_too_few_values(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type", "--", "1",
                            error="expected 2 (number of subtypes) values for non-prefix compound type, got 1")
