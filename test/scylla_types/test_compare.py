#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


@pytest.mark.parametrize("lhs,rhs,result", [
    ("00000001", "00000002", "1 < 2"),
    ("00000002", "00000002", "2 == 2"),
    ("00000002", "00000001", "2 > 1"),
    ("ffffffff", "00000001", "-1 < 1"),
])
def test_compare(scylla_types, lhs, rhs, result):
    res = scylla_types("compare", "-t", "Int32Type", lhs, rhs)
    assert res.stdout == f"{result}\n"


def test_compare_reversed_type(scylla_types):
    res = scylla_types("compare", "-t", "ReversedType(TimeUUIDType)",
                       "b34b62d46a8d11ea0000005000237906", "d00819896f6b11ea00000000001c571b")
    assert res.stdout == "b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b\n"


@pytest.mark.parametrize("values", [["00000001"], ["00000001", "00000002", "00000003"]])
def test_compare_wrong_number_of_values(scylla_types_fails_with, values):
    scylla_types_fails_with("compare", "-t", "Int32Type", *values, error=f"expected 2 values, got {len(values)}")


def test_compare_prefix_compound(scylla_types):
    res = scylla_types("compare", "--prefix-compound", "-t", "Int32Type", "-t", "UTF8Type",
                       "000400000001000162", "000400000001000161")
    assert res.stdout == "(1, b) > (1, a)\n"


def test_compare_prefix_compound_partial(scylla_types):
    """A prefix sorts before the keys it is a prefix of."""
    res = scylla_types("compare", "--prefix-compound", "-t", "Int32Type", "-t", "UTF8Type",
                       "000400000001", "000400000001000161")
    assert res.stdout == "(1) < (1, a)\n"


def test_compare_full_compound(scylla_types):
    """Full compounds are compared by their components, not by token."""
    res = scylla_types("compare", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type",
                       "0004000000010003616263", "0004000000020003616263")
    assert res.stdout == "(1, abc) < (2, abc)\n"


def test_compare_cql_type_name(scylla_types):
    res = scylla_types("compare", "-t", "ReversedType(timeuuid)",
                       "b34b62d46a8d11ea0000005000237906", "d00819896f6b11ea00000000001c571b")
    assert res.stdout == "b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b\n"


def test_compare_legacy_composite(scylla_types):
    res = scylla_types("compare", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type",
                       "00040000000100000361626300", "00040000000200000361626300")
    assert res.stdout == "(1, abc) < (2, abc)\n"


def test_compare_schema_file_column(scylla_types, schema_file):
    """The type of the column is used, including its clustering order."""
    res = scylla_types("compare", "--schema-file", schema_file, "--column", "ck1",
                       "b34b62d46a8d11ea0000005000237906", "d00819896f6b11ea00000000001c571b")
    assert res.stdout == "b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b\n"
    res = scylla_types("compare", "--schema-file", schema_file, "--column", "ck2", "00000001", "00000002")
    assert res.stdout == "1 < 2\n"


def test_compare_text(scylla_types):
    res = scylla_types("compare", "-t", "Int32Type", "-f", "text", "--", "2", "-1")
    assert res.stdout == "2 > -1\n"


def test_compare_text_prefix_compound(scylla_types):
    """The first half of the values make up the first compared value, the second half the second one."""
    res = scylla_types("compare", "--prefix-compound", "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--", "1", "b", "1", "a")
    assert res.stdout == "(1, b) > (1, a)\n"


@pytest.mark.parametrize("compound_option", ["--full-compound", "--legacy-composite"])
def test_compare_text_full_compound(scylla_types, compound_option):
    res = scylla_types("compare", compound_option, "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--", "1", "abc", "2", "abc")
    assert res.stdout == "(1, abc) < (2, abc)\n"


def test_compare_text_odd_number_of_values(scylla_types_fails_with):
    scylla_types_fails_with("compare", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--", "1", "abc", "2",
                            error="error: expected the number of unserialized values (3) to be divisible by 2")


def test_compare_text_wrong_number_of_components(scylla_types_fails_with):
    scylla_types_fails_with("compare", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--", "1", "2",
                            error="expected 2 (number of subtypes) values for non-prefix compound type, got 1")


def test_compare_text_unsupported_type(scylla_types_fails_with):
    scylla_types_fails_with("compare", "-t", "list<int>", "-f", "text", "--", "1", "2",
                            error="error: serializing values of type list<int> is not supported")


def test_compare_json(scylla_types):
    res = scylla_types("compare", "-t", "frozen<list<int>>", "-f", "json", "--", "[1, 2]", "[1, 3]")
    assert res.stdout == "1, 2 < 1, 3\n"
