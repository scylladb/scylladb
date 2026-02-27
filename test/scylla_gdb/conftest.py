# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""Conftest for Scylla GDB tests"""

import os
import subprocess

import pytest

from test.pylib.runner import testpy_test_fixture_scope
from test.pylib.suite.python import PythonTest


@pytest.fixture(scope=testpy_test_fixture_scope)
async def scylla_server(testpy_test: PythonTest | None):
    """Return a running Scylla server instance from the active test cluster."""
    async with testpy_test.run_ctx(options=testpy_test.suite.options) as cluster:
        yield next(iter(cluster.running.values()))


@pytest.fixture(scope="module")
def gdb_cmd(scylla_server, request):
    """
    Returns a command-line (argv list) that attaches to the `scylla_server` PID, loads `scylla-gdb.py`
    and `gdb_utils.py`. This is meant to be executed by `execute_gdb_command()` in `--batch` mode.
    """
    scylla_gdb_py = os.path.join(request.fspath.dirname, "..", "..", "scylla-gdb.py")
    script_py = os.path.join(request.fspath.dirname, "gdb_utils.py")
    cmd = [
        "gdb",
        "-q",
        "--batch",
        "--nx",
        "-se",
        str(scylla_server.exe),
        "-p",
        str(scylla_server.cmd.pid),
        "-ex",
        "set python print-stack full",
        "-x",
        scylla_gdb_py,
        "-x",
        script_py,
    ]
    return cmd


def execute_gdb_command(gdb_cmd, scylla_command: str = None, full_command: str = None):
    """Execute a single GDB command attached to the running Scylla process.

    Builds on `gdb_cmd` and runs GDB via `subprocess.run()` in `--batch` mode.
    `scylla_command` is executed as `scylla <cmd>` through GDB's Python interface.

    Args:
        gdb_cmd: Base GDB argv list returned by the `gdb_cmd` fixture.
        scylla_command: Scylla GDB command name/args (from scylla-gdb.py). Mutually exclusive with `full_command`.
        full_command: Raw GDB command string to execute. Mutually exclusive with `scylla_command`.

    Returns:
        Command stdout as a decoded string.
    """
    if full_command:
        command = [*gdb_cmd, "-ex", full_command]
    else:
        command = [
            *gdb_cmd,
            "-ex",
            f"python gdb.execute('scylla {scylla_command}')",
        ]

    result = subprocess.run(
        command, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    return result
