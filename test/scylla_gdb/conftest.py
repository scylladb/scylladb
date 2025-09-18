# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""Conftest for Scylla GDB tests"""
import os

import pexpect
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

    if build_mode != "release":
        pytest.skip("Scylla executable was compiled without debugging symbols (-g). Use `release` mode")

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
    cmd = f"gdb -q -se {run.scylla} -p {str(scylla_pid)} -ex set python print-stack full -x {scylla_gdb_py}"
    # cmd = (
    #     f"gdb -se {run.scylla} "
    #     '-ex "set python print-stack full" '
    #     '-ex "set pagination off" '
    #     "-ex \"python import gdb as gdb_library; import importlib; save_sys_path = sys.path; sys.path.insert(1, '/home/jsmolar/ScyllaDB/scylladb/'); importlib.import_module('scylla-gdb'); sys.path = save_sys_path\" "
    #     f"-ex \"python gdb_library.execute('attach {scylla_pid}')\""
    # )
    child = pexpect.spawn(cmd, maxread=10, searchwindowsize=10)
    child.sendline("y")     # confirms debug symbols used by scylla
    child.expect_exact("(gdb)")
    return child


@pytest.fixture(scope="package")
def gdb_execute(gdb_config):
    """
    Executes GDB commands in the context of the configured Scylla process.
    """

    def _execute(command: str = None, full_command: str = None, skip_lines = 0):
        """
        Args:
            command (str, optional): A GDB command to execute.
            args (list, optional): Additional GDB arguments.
            assert_response (bool, optional): Assert GDB response returncode and stderr/
        """
        command = f"python gdb.execute('scylla {command}')"
        if full_command:
            command = full_command

        gdb_config.sendline(command)
        try:
            gdb_config.expect_exact("(gdb)", timeout=120)
        except pexpect.exceptions.TIMEOUT:
            gdb_config.sendcontrol("c")
            gdb_config.expect_exact("(gdb)", timeout=0.1)
            pytest.fail("GDB command did not complete within the timeout period")
        result = gdb_config.before.decode('utf-8')

        assert "Error" not in result
        return result

    return _execute
