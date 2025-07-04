# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""Conftest for Scylla GDB tests"""

import subprocess
import pytest

from test.cqlpy import run


@pytest.fixture(scope="package")
def scylla_pid():
    """
    Runs Scylla and waits until it can respond to CQL queries.
    Skips the test if the Scylla executable lacks debugging symbols.
    """
    scylla_dump = subprocess.run(
        ["objdump", "-h", run.find_scylla()], capture_output=True, text=True
    )
    if ".debug_info" not in scylla_dump.stdout:
        pytest.skip(
            "Scylla executable was compiled without debugging symbols (-g). "
            "Set the SCYLLA environment variable to a debug build to run GDB tests."
        )

    pid = run.run_with_temporary_dir(run.run_scylla_cmd)
    ip = run.pid_to_ip(pid)
    run.wait_for_services(pid, [lambda: run.check_cql(ip)])
    return pid


@pytest.fixture(scope="package")
def gdb_config(scylla_pid):
    """
    Prepares the GDB configuration for attaching to the Scylla process.
    Imports scylla-gdb.py to the GDB
    """
    args = [
        "gdb",
        "-batch",
        "-n",
        "-se",
        run.scylla,
        "-p",
        str(scylla_pid),
        "-ex",
        "set auto-load safe-path /",
        "-ex",
        "set python print-stack full",
        "-x",
        "../../scylla-gdb.py",
    ]

    return args


@pytest.fixture(scope="package")
def gdb_execute(gdb_config):
    """
    Executes GDB commands in the context of the configured Scylla process.
    """

    def _execute(command: str = None, args: str = None, assert_response: bool = True):
        """
        Args:
            command (str, optional): A GDB command to execute.
            args (list, optional): Additional GDB arguments.
            assert_response (bool, optional): Assert GDB response returncode and stderr/
        """
        if args is None:
            args = []
        if command is None:
            command = []
        else:
            command = ["-ex", f'python print(gdb.execute("scylla {command}"))']

        args = gdb_config + args + command
        result = subprocess.run(args, capture_output=True, text=True)

        if assert_response:
            assert (
                result.stderr == ""
            ), f"GDB command produced unexpected error output: {result.stderr}"
            assert (
                result.returncode == 0
            ), f"GDB command failed with return code {result.returncode}"

        return result

    return _execute
