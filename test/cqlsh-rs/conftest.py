# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Fixtures for cqlsh-rs integration test suite."""

import fcntl
import os
import shutil
import subprocess
import tempfile

import pytest

from test.pylib.suite.python import PythonTest, add_host_option, add_cql_connection_options
from test.pylib.skip_types import skip_env
from test.conftest import dynamic_scope


def pytest_addoption(parser):
    add_host_option(parser)
    add_cql_connection_options(parser)


@pytest.fixture(scope=dynamic_scope())
async def host(request, testpy_test: PythonTest | None):
    if testpy_test is None:
        yield request.config.getoption("--host")
    else:
        async with testpy_test.run_ctx(options=testpy_test.suite.options):
            yield testpy_test.server_address


@pytest.fixture(scope=dynamic_scope())
def port(request, testpy_test: PythonTest | None):
    return int(request.config.getoption("--port"))


@pytest.fixture(scope="session")
def cargo_available():
    """Skip the entire session if cargo is not available in PATH."""
    if shutil.which("cargo") is None:
        skip_env("cargo not found in PATH — skipping cqlsh-rs integration tests")


@pytest.fixture(scope="session")
def cqlsh_rs_repo_dir():
    return os.path.join(os.path.dirname(__file__), "..", "..", "tools", "cqlsh-rs")


@pytest.fixture(scope="session")
def cargo_precompile(cargo_available, cqlsh_rs_repo_dir):
    """Pre-compile the integration test binary once, using a filelock to prevent
    multiple xdist workers from compiling simultaneously."""
    lock_path = os.path.join(tempfile.gettempdir(), "cqlsh-rs-cargo-build.lock")
    with open(lock_path, "w") as lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            print("\n=== cqlsh-rs: compiling integration test binary (may take several minutes) ===",
                  flush=True)
            proc = subprocess.Popen(
                ["cargo", "test", "--test", "integration", "--features", "test-plain", "--no-run"],
                cwd=cqlsh_rs_repo_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            output_lines = []
            for line in proc.stdout:
                line = line.rstrip()
                output_lines.append(line)
                if line.lstrip().startswith("Compiling") or line.lstrip().startswith("Finished"):
                    print(f"  cargo: {line.strip()}", flush=True)
            proc.wait(timeout=600)
            if proc.returncode != 0:
                skip_env(f"cargo pre-compile failed:\n" + "\n".join(output_lines))
            print("=== cqlsh-rs: compilation complete ===", flush=True)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
