#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


@pytest.mark.parametrize("type_name,serialized,value", [
    ("Int32Type", "00000001", "1"),
    ("Int32Type", "b34b62d4", "-1286905132"),
    ("LongType", "0000000000000001", "1"),
    ("BooleanType", "01", "true"),
    ("UTF8Type", "616263", "abc"),
    ("SimpleDateType", "80004919", "2021-03-27"),
    ("TimeUUIDType", "d00819896f6b11ea00000000001c571b", "d0081989-6f6b-11ea-0000-0000001c571b"),
    ("ReversedType(Int32Type)", "00000001", "1"),
    ("MapType(Int32Type,UTF8Type)", "0000000100000004000000010000000161", "{1 : a}"),
])
def test_deserialize(scylla_types, type_name, serialized, value):
    res = scylla_types("deserialize", "-t", type_name, serialized)
    assert res.stdout == f"{value}\n"


def test_deserialize_multiple_values(scylla_types):
    res = scylla_types("deserialize", "-t", "Int32Type", "00000001", "00000002", "ffffffff")
    assert res.stdout.splitlines() == ["1", "2", "-1"]


def test_deserialize_invalid_hex(scylla_types_fails_with):
    scylla_types_fails_with("deserialize", "-t", "Int32Type", "0g", error="Non-hex characters in 0g")


def test_deserialize_prefix_compound(scylla_types):
    res = scylla_types("deserialize", "--prefix-compound", "-t", "TimeUUIDType", "-t", "Int32Type",
                       "0010d00819896f6b11ea00000000001c571b000400000010")
    assert res.stdout == "(d0081989-6f6b-11ea-0000-0000001c571b, 16)\n"


def test_deserialize_prefix_compound_partial(scylla_types):
    res = scylla_types("deserialize", "--prefix-compound", "-t", "TimeUUIDType", "-t", "Int32Type",
                       "0010d00819896f6b11ea00000000001c571b")
    assert res.stdout == "(d0081989-6f6b-11ea-0000-0000001c571b)\n"


def test_deserialize_full_compound(scylla_types):
    res = scylla_types("deserialize", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "0004000000010003616263")
    assert res.stdout == "(1, abc)\n"
