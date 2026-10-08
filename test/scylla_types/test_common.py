#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for behaviour common to all scylla-types actions."""

import pytest


ACTIONS = ["serialize", "deserialize", "compare", "ring-order-compare", "validate", "tokenof", "shardof"]


@pytest.mark.parametrize("action", ACTIONS)
def test_missing_type(scylla_types_fails_with, action):
    scylla_types_fails_with(action, "00000001", error="error: missing required option '--type'")


@pytest.mark.parametrize("action", ACTIONS)
def test_missing_values(scylla_types_fails_with, action):
    scylla_types_fails_with(action, "-t", "Int32Type", error="error: no values specified")


@pytest.mark.parametrize("action", ACTIONS)
def test_multiple_types_for_non_compound(scylla_types_fails_with, action):
    scylla_types_fails_with(action, "-t", "Int32Type", "-t", "Int32Type", "00000001",
                            error="error: expected a single '--type' argument, got  2")


def test_unknown_action(scylla_types_fails_with):
    scylla_types_fails_with("foo", "-t", "Int32Type", "00000001", error="error: unrecognized operation argument")


def test_fully_qualified_type_name(scylla_types):
    """The org.apache.cassandra.db.marshal. prefix of the type class names is optional."""
    res = scylla_types("serialize", "-t", "org.apache.cassandra.db.marshal.Int32Type", "--", "1")
    assert res.stdout == "00000001\n"


@pytest.mark.parametrize("type_name,error", [
    ("list<int", "failed to parse type 'list<int' at position 8: expected '>'"),
    ("foo<int>", "failed to parse type 'foo<int>' at position 4: unknown parametric type foo"),
    ("map<int, text> x", "failed to parse type 'map<int, text> x' at position 15: unexpected trailing characters"),
    ("<int>", "failed to parse type '<int>' at position 0: expected type name"),
])
def test_invalid_cql_type_name(scylla_types_fails_with, type_name, error):
    scylla_types_fails_with("deserialize", "-t", type_name, "00000001", error=error)


def test_unknown_type_name(scylla_types_fails_with):
    scylla_types_fails_with("deserialize", "-t", "foo", "00000001", error="unknown type: org.apache.cassandra.db.marshal.foo")


COMPOUND_OPTION_ALIASES = [
    ("prefix-compound", "clustering-key"),
    ("full-compound", "partition-key"),
    ("legacy-composite", "legacy-partition-key"),
]


def compound_option_args(action, option_name, option):
    """The arguments for invoking action with the given compound option, on an int (compound) value."""
    value = "00000001" if option_name == "legacy-composite" else "000400000001"
    if action == "serialize":
        return [option, "-t", "Int32Type", "--", "1"]
    if action in ("compare", "ring-order-compare"):
        return [option, "-t", "Int32Type", value, value]
    if action == "shardof":
        return [option, "-t", "Int32Type", "--shards=8", value]
    return [option, "-t", "Int32Type", value]


@pytest.mark.parametrize("action,option_name,alias", [
    (action, option_name, alias)
    for action in ACTIONS
    for option_name, alias in COMPOUND_OPTION_ALIASES
    # These actions only support partition keys.
    if not (action in ("ring-order-compare", "tokenof", "shardof") and option_name == "prefix-compound")
])
def test_compound_option_alias(scylla_types, action, option_name, alias):
    """The compound options have human-friendly aliases, which are equivalent to the original option."""
    expected = scylla_types(action, *compound_option_args(action, option_name, f"--{option_name}")).stdout
    assert expected
    assert scylla_types(action, *compound_option_args(action, option_name, f"--{alias}")).stdout == expected


def test_schema_file_and_type(scylla_types_fails_with, schema_file):
    scylla_types_fails_with("serialize", "--schema-file", schema_file, "--column", "pk1", "-t", "Int32Type", "--", "1",
                            error="error: --type and --schema-file are mutually exclusive")


def test_column_without_schema_file(scylla_types_fails_with):
    scylla_types_fails_with("serialize", "--column", "pk1", "-t", "Int32Type", "--", "1",
                            error="error: --column requires --schema-file")


def test_schema_file_unknown_column(scylla_types_fails_with, schema_file):
    scylla_types_fails_with("serialize", "--schema-file", schema_file, "--column", "foo", "--", "1",
                            error="error: column foo not found in table ks.tbl")


def test_schema_file_without_type_selector(scylla_types_fails_with, schema_file):
    scylla_types_fails_with("serialize", "--schema-file", schema_file, "--", "1",
                            error="error: --schema-file requires one of: --column, --prefix-compound (--clustering-key),"
                                  " --full-compound (--partition-key) or --legacy-composite (--legacy-partition-key)")


@pytest.mark.parametrize("compound_option", ["--prefix-compound", "--clustering-key", "--full-compound", "--partition-key",
                                             "--legacy-composite", "--legacy-partition-key"])
def test_schema_file_column_and_compound(scylla_types_fails_with, schema_file, compound_option):
    scylla_types_fails_with("serialize", "--schema-file", schema_file, "--column", "pk1", compound_option, "--", "1",
                            error="error: --column cannot be used together with --prefix-compound (--clustering-key),"
                                  " --full-compound (--partition-key) or --legacy-composite (--legacy-partition-key)")


def test_schema_file_not_found(scylla_types_fails_with, tmp_path):
    path = str(tmp_path / "nonexistent.cql")
    res = scylla_types_fails_with("serialize", "--schema-file", path, "--column", "pk1", "--", "1",
                                  error="No such file or directory")
    assert path in res.stderr


@pytest.mark.parametrize("input_format_args", [["-f", "hex"], ["-fhex"], ["--input-format", "hex"], ["--input-format=hex"]])
def test_input_format_option_spelling(scylla_types, input_format_args):
    res = scylla_types("compare", "-t", "Int32Type", *input_format_args, "00000001", "00000002")
    assert res.stdout == "1 < 2\n"


def test_input_format_text(scylla_types):
    res = scylla_types("compare", "-t", "Int32Type", "-f", "text", "--", "1", "2")
    assert res.stdout == "1 < 2\n"


def test_invalid_input_format(scylla_types_fails_with):
    scylla_types_fails_with("compare", "-t", "Int32Type", "-f", "foo", "00000001", "00000002",
                            error="error: invalid input format 'foo', expected one of: hex, text")


def test_input_format_short_option_with_equal_sign(scylla_types_fails_with):
    """Boost program options doesn't support -f=<format>, the error message should point this out."""
    scylla_types_fails_with("compare", "-t", "Int32Type", "-f=text", "--", "1", "2",
                            error="note that -f=<format> is not supported, use -f <format> or --input-format=<format>")


@pytest.mark.parametrize("action,input_format,supported_formats", [
    ("serialize", "hex", "text"),
    ("deserialize", "text", "hex"),
    ("validate", "text", "hex"),
])
def test_unsupported_input_format(scylla_types_fails_with, action, input_format, supported_formats):
    scylla_types_fails_with(action, "-t", "Int32Type", "-f", input_format, "--", "1",
                            error=f"error: the {action} action doesn't support the {input_format} input format, supported input formats: {supported_formats}")


@pytest.mark.parametrize("action,input_format,value,expected", [
    ("serialize", "text", "1", "00000001"),
    ("deserialize", "hex", "00000001", "1"),
    ("validate", "hex", "00000001", "00000001: VALID - 1"),
])
def test_default_input_format_explicitly(scylla_types, action, input_format, value, expected):
    """The default input format of the action can be selected explicitly too."""
    res = scylla_types(action, "-t", "Int32Type", "-f", input_format, "--", value)
    assert res.stdout == f"{expected}\n"
