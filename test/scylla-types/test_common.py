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
