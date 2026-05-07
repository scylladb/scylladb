# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

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
    # SSL suite uses the dedicated SSL port (9142) configured via
    # native_transport_port_ssl in test_config.yaml
    return 9142


@pytest.fixture(scope="session")
def cargo_available():
    if shutil.which("cargo") is None:
        skip_env("cargo not found in PATH")


@pytest.fixture(scope="session")
def cqlsh_rs_repo_dir():
    return os.path.join(os.path.dirname(__file__), "..", "..", "..", "tools", "cqlsh-rs")


@pytest.fixture(scope="session")
def ssl_ca_path():
    """Path to the CA cert used by the test Scylla instance."""
    return os.path.join(os.path.dirname(__file__), "..", "..", "pylib", "resources", "scylla.crt")


@pytest.fixture(scope="session")
def cargo_precompile(cargo_available, cqlsh_rs_repo_dir):
    lock_path = os.path.join(tempfile.gettempdir(), "cqlsh-rs-cargo-build-ssl.lock")
    with open(lock_path, "w") as lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            print("\n=== cqlsh-rs: compiling SSL integration test binary ===", flush=True)
            proc = subprocess.Popen(
                ["cargo", "test", "--test", "integration", "--features", "test-ssl", "--no-run"],
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
            print("=== cqlsh-rs: SSL compilation complete ===", flush=True)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
