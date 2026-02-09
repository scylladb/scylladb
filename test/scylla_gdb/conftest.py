# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""Conftest for Scylla GDB tests"""

import logging
import os

import pexpect
import pytest
import re

from test.pylib.suite.python import PythonTest
from test.pylib.util import LogPrefixAdapter


@pytest.fixture(scope="module")
async def scylla_server(testpy_test: PythonTest | None):
    """Return a running Scylla server instance from the active test cluster."""
    logger_prefix = testpy_test.mode + "/" + testpy_test.uname
    logger = LogPrefixAdapter(
        logging.getLogger(logger_prefix), {"prefix": logger_prefix}
    )
    scylla_cluster = await testpy_test.suite.clusters.get(logger)
    scylla_server = next(iter(scylla_cluster.running.values()))

    yield scylla_server

    await testpy_test.suite.clusters.put(scylla_cluster, is_dirty=True)


@pytest.fixture(scope="module")
def gdb_process(scylla_server, request):
    """Spawn an interactive GDB attached to the Scylla process.

    Loads `scylla-gdb.py` and test helpers (`gdb_utils.py`) so tests can run GDB/Python helpers
    against the live Scylla process.
    """
    scylla_gdb_py = os.path.join(request.fspath.dirname, "..", "..", "scylla-gdb.py")
    script_py = os.path.join(request.fspath.dirname, "gdb_utils.py")
    cmd = (
        f"gdb -q "
        "--nx "
        "-iex 'set confirm off' "
        "-iex 'set pagination off' "
        f"-se {scylla_server.exe} "
        f"-p {scylla_server.cmd.pid} "
        f"-ex set python print-stack full "
        f"-x {scylla_gdb_py} "
        f"-x {script_py}"
    )
    gdb_process = pexpect.spawn(cmd, maxread=10, searchwindowsize=10)
    gdb_process.expect_exact("(gdb)")

    yield gdb_process

    gdb_process.terminate()


def execute_gdb_command(
    gdb_process, scylla_command: str = None, full_command: str = None
):
    """
    Execute a command in an interactive GDB session and return its output.

    The command can be provided either as a Scylla GDB command (which will be
    wrapped and executed via GDB's Python interface) or as a full raw GDB
    command string.

    The function waits for the GDB prompt to reappear, enforces a timeout,
    and fails the test if the command does not complete or if GDB reports an
    error.

    Args:
        gdb_process (pexpect.pty_spawn.spawn): An active GDB process spawned via pexpect
        scylla_command (str, optional): A GDB Scylla command (from scylla-gdb.py) to execute.
        full_command (str, optional): A raw GDB command string to execute.
    """
    command = f"python gdb.execute('scylla {scylla_command}')"
    if full_command:
        command = full_command

    gdb_process.sendline(command)
    try:
        gdb_process.expect_exact("(gdb)", timeout=180)
    except pexpect.exceptions.TIMEOUT:
        gdb_process.sendcontrol("c")
        gdb_process.expect_exact("(gdb)", timeout=1)
        pytest.fail("GDB command did not complete within the timeout period")
    result = gdb_process.before.decode("utf-8")

    # The task_histogram command may include "error::Error" in its output, so
    # allow it.
    assert not re.search(r'(?<!error::)Error', result), f'Unexpected error reported in {result}'
    return result
