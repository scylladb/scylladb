# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a schema to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest

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

@pytest.mark.parametrize(
    "command",
    [
        "find -r",
        "ptr",
        "schema (const schema *)",  # `schema` requires type-casted pointer
    ],
)
def test_schema(gdb_cmd, command):
    result = execute_gdb_command(gdb_cmd, f"{command} $get_schema()")
    assert result.returncode == 0, (
        f"GDB command {command} failed. stdout: {result.stdout} stderr: {result.stderr}"
    )


def test_generate_object_graph(gdb_cmd, request):
    tmpdir = request.config.getoption("--tmpdir")
    result = execute_gdb_command(
        gdb_cmd, f"generate-object-graph -o {tmpdir}/og.dot -d 2 -t 10 $get_schema()"
    )
    assert result.returncode == 0, (
        f"GDB command `generate-object-graph` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )
