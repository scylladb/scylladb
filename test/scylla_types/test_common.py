#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for behaviour common to all scylla-types actions."""

import pytest


ACTIONS = ["serialize", "deserialize", "compare", "validate", "tokenof", "shardof"]


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
    if action == "compare":
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
