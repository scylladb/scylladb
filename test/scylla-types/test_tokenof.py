#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


def test_tokenof(scylla_types):
    res = scylla_types("tokenof", "--full-compound", "-t", "UTF8Type", "-t", "SimpleDateType", "-t", "UUIDType",
                       "000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196")
    assert res.stdout == "(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): -5043005771368701888\n"


def test_tokenof_single_component(scylla_types):
    res = scylla_types("tokenof", "--full-compound", "-t", "Int32Type", "000400000001")
    assert res.stdout == "(1): -4069959284402364209\n"


def test_tokenof_multiple_values(scylla_types):
    res = scylla_types("tokenof", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type",
                       "0004000000010003616263", "0004000000020003616263")
    assert res.stdout.splitlines() == [
        "(1, abc): 8771735466527499816",
        "(2, abc): -3504390351319460166",
    ]


@pytest.mark.parametrize("compound_args", [[], ["--prefix-compound"]])
def test_tokenof_requires_full_compound(scylla_types_fails_with, compound_args):
    scylla_types_fails_with("tokenof", *compound_args, "-t", "Int32Type", "00000001",
                            error="tokenof action requires --full-compound (--partition-key) or --legacy-composite (--legacy-partition-key) input")


def test_tokenof_legacy_composite(scylla_types):
    res = scylla_types("tokenof", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type", "00040000000100000361626300")
    assert res.stdout == "(1, abc): 8771735466527499816\n"


def test_tokenof_legacy_composite_single_component(scylla_types):
    res = scylla_types("tokenof", "--legacy-composite", "-t", "Int32Type", "00000001")
    assert res.stdout == "(1): -4069959284402364209\n"


@pytest.mark.parametrize("compound_option,key", [
    ("--partition-key", "0004000000010003616263"),
    ("--legacy-partition-key", "00040000000100000361626300"),
])
def test_tokenof_schema_file(scylla_types, schema_file, compound_option, key):
    res = scylla_types("tokenof", "--schema-file", schema_file, compound_option, key)
    assert res.stdout == "(1, abc): 8771735466527499816\n"


@pytest.mark.parametrize("compound_option", ["--full-compound", "--legacy-composite"])
def test_tokenof_text(scylla_types, compound_option):
    """With -f text, all values make up a single partition key."""
    res = scylla_types("tokenof", compound_option, "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--", "1", "abc")
    assert res.stdout == "(1, abc): 8771735466527499816\n"


def test_tokenof_text_schema_file(scylla_types, schema_file):
    res = scylla_types("tokenof", "--schema-file", schema_file, "--partition-key", "-f", "text", "--", "1", "abc")
    assert res.stdout == "(1, abc): 8771735466527499816\n"


def test_tokenof_text_wrong_number_of_values(scylla_types_fails_with):
    scylla_types_fails_with("tokenof", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--", "1",
                            error="expected 2 (number of subtypes) values for non-prefix compound type, got 1")
