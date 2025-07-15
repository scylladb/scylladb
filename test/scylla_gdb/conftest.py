# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""Conftest for Scylla GDB tests"""
import os
import subprocess

import pytest

from test.cqlpy import run


@pytest.fixture(scope="package")
def scylla_pid(request, build_mode):
    """
    Runs Scylla and waits until it can respond to CQL queries.

    - Skips tests if running on anything else than x86_64. Gdb tests are known to be broken on aarch64 (
      https://sourceware.org/bugzilla/show_bug.cgi?id=27886) and untested on anything else.
    - Skips tests if the Scylla executable lacks debugging symbols.
    """
    if os.uname().machine != "x86_64":
        pytest.skip("Tests are skipped for non-x86_64 architectures")

    if build_mode == "dev":
        pytest.skip("Scylla executable was compiled without debugging symbols (-g). Use `release` or `debug` modes")

    run.find_scylla(build_mode)
    pid = run.run_with_temporary_dir(run.run_scylla_cmd)
    ip = run.pid_to_ip(pid)
    run.wait_for_services(pid, [lambda: run.check_cql(ip)])

    return pid


@pytest.fixture(scope="package")
def gdb_config(scylla_pid, request):
    """
    Prepares the GDB configuration for attaching to the Scylla process.
    Imports scylla-gdb.py to the GDB
    """
    scylla_gdb_py = os.path.join(request.fspath.dirname, '..', '..', 'scylla-gdb.py')
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
        scylla_gdb_py,
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
