# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a some task to work on.
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


def test_coroutine_frame(gdb_cmd):
    """
    Offsets the pointer by two words to shift from the outer coroutine frame
    to the inner `seastar::task`, as required by `$coro_frame`, which expects
    a `seastar::task*`.
    """
    result = execute_gdb_command(
        gdb_cmd, full_command="p *$coro_frame($get_coroutine() + 16)"
    )
    if "COROUTINE_NOT_FOUND" in result.stdout:
        # See https://github.com/scylladb/scylladb/issues/22501
        pytest.skip("Failed to find coroutine task. Skipping test.")
    assert result.returncode == 0, (
        f"GDB command `coro_frame` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )


def test_fiber(gdb_cmd):
    result = execute_gdb_command(gdb_cmd, "fiber $get_task()")
    assert result.returncode == 0, (
        f"GDB command `fiber` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )
