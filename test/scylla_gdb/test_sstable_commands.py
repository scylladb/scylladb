# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a sstable to work on.
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
        "sstable-summary",
        "sstable-index-cache",
    ],
)
def test_sstable(gdb_cmd, command):
    result = execute_gdb_command(gdb_cmd, f"{command} $get_sstable()")
    assert result.returncode == 0, (
        f"GDB command {command} failed. stdout: {result.stdout} stderr: {result.stderr}"
    )
