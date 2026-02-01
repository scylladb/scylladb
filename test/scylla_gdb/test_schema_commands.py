# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a schema to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest
import re

from test.scylla_gdb.conftest import execute_gdb_command

pytestmark = [
    pytest.mark.skip_mode(
        mode=["dev", "debug"],
        reason="Scylla was built without debug symbols; use release mode",
    ),
    pytest.mark.skip_mode(
        mode=["dev", "debug", "release"],
        platform_key="aarch64",
        reason="GDB is broken on aarch64: https://sourceware.org/bugzilla/show_bug.cgi?id=27886",
    ),
]


@pytest.fixture(scope="module")
def schema(gdb_cmd):
    """
    Returns pointer to schema of the first table it finds
    Even without any user tables, we will always have system tables.
    """
    result = execute_gdb_command(gdb_cmd, full_command="python get_schema()").stdout
    match = re.search(r"schema=\s*(0x[0-9a-fA-F]+)", result)
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
def test_schema(gdb_cmd, command, schema):
    assert schema, "Failed to find schema of any table"

    result = execute_gdb_command(gdb_cmd, f"{command} {schema}")
    assert result.returncode == 0, (
        f"GDB command {command} failed. stdout: {result.stdout} stderr: {result.stderr}"
    )


def test_generate_object_graph(gdb_cmd, schema, request):
    assert schema, "Failed to find schema of any table"

    tmpdir = request.config.getoption("--tmpdir")
    result = execute_gdb_command(
        gdb_cmd, f"generate-object-graph -o {tmpdir}/og.dot -d 2 -t 10 {schema}"
    )
    assert result.returncode == 0, (
        f"GDB command `generate-object-graph` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )
