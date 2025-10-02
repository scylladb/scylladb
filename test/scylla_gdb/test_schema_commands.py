# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a schema to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest
import re


@pytest.fixture(scope="module")
def schema(gdb_execute):
    """
    Returns pointer to schema of the first table it finds
    Even without any user tables, we will always have system tables.
    """
    result = gdb_execute(full_command="python schema_config()")
    match = re.search(r"schema=\s*(0x[0-9a-fA-F]+)", result)
    assert match, f"Failed to find schema pointer in response: {result}"
    schema_pointer = match.group(1) if match else None

    return schema_pointer


@pytest.mark.parametrize(
    "command",
    [
        "find -r",
        "ptr",
        "schema (const schema *)",  # `schema` requires type-casted pointer
    ],
)
def test_schema(gdb_execute, command, schema):
    gdb_execute(f"{command} {schema}")


def test_generate_object_graph(gdb_execute, schema, request):
    tmpdir = request.config.getoption("--tmpdir")
    gdb_execute(f"generate-object-graph -o {tmpdir}/og.dot -d 2 -t 10 {schema}")
