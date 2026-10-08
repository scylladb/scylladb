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
