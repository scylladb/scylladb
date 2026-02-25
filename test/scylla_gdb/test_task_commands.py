# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a some task to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""

import re

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


@pytest.fixture(scope="module")
def task(gdb_cmd):
    """
    Finds a Scylla fiber task using a `find_vptrs()` loop.

    Since Scylla is fresh‑booted, `get_local_tasks()` returns nothing.
    Nevertheless, a `find_vptrs()` scan can still discover the first task
    skeleton created by `http_server::do_accept_one` (often the earliest
    “Scylla fiber” to appear).
    """
    result = execute_gdb_command(gdb_cmd, full_command="python get_task()").stdout
    match = re.search(r"task=(\d+)", result)
    task = match.group(1) if match else None
    return task


@pytest.fixture(scope="module")
def coroutine_task(gdb_cmd, scylla_server):
    """
    Finds a coroutine task, similar to the `task` fixture.

    This fixture executes the `coroutine_config` script in GDB to locate a
    specific coroutine task.
    """
    result = execute_gdb_command(gdb_cmd, full_command="python get_coroutine()").stdout
    match = re.search(r"coroutine_config=\s*(.*)", result)
    if not match:
        # See https://github.com/scylladb/scylladb/issues/22501
        pytest.skip("Failed to find coroutine task. Skipping test.")

    return match.group(1).strip()


def test_coroutine_frame(gdb_cmd, coroutine_task):
    """
    Offsets the pointer by two words to shift from the outer coroutine frame
    to the inner `seastar::task`, as required by `$coro_frame`, which expects
    a `seastar::task*`.
    """
    assert coroutine_task, "No coroutine task was found"

    result = execute_gdb_command(
        gdb_cmd, full_command=f"p *$coro_frame({coroutine_task} + 16)"
    )
    assert result.returncode == 0, (
        f"GDB command `coro_frame` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )


def test_fiber(gdb_cmd, task):
    assert task, f"No task was found using `find_vptrs()`"

    result = execute_gdb_command(gdb_cmd, f"fiber {task}")
    assert result.returncode == 0, (
        f"GDB command `fiber` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )
