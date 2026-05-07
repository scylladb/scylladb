# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import fcntl
import os
import shutil
import subprocess
import tempfile

import pytest

from test.pylib.suite.python import PythonTest, add_host_option, add_cql_connection_options
from test.pylib.skip_types import skip_env
from test.conftest import dynamic_scope


async def wait_for_cql(host: str, port: int, timeout: float = 60):
    """Wait until Scylla responds to CQL queries with auth credentials."""
    from cassandra.cluster import Cluster, NoHostAvailable
    from cassandra.auth import PlainTextAuthProvider

    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            cluster = Cluster(contact_points=[host], port=port,
                              connect_timeout=5,
                              auth_provider=PlainTextAuthProvider(
                                  username="cassandra", password="cassandra"))
            session = cluster.connect()
            session.execute("SELECT key FROM system.local")
            session.shutdown()
            cluster.shutdown()
            return
        except (NoHostAvailable, OSError, Exception):
            await asyncio.sleep(1)
    raise RuntimeError(f"CQL (auth) not ready at {host}:{port} after {timeout}s")


def pytest_addoption(parser):
    add_host_option(parser)
    add_cql_connection_options(parser)


@pytest.fixture(scope=dynamic_scope())
async def host(request, testpy_test: PythonTest | None):
    if testpy_test is None:
        yield request.config.getoption("--host")
    else:
        async with testpy_test.run_ctx(options=testpy_test.suite.options):
            port = int(request.config.getoption("--port"))
            await wait_for_cql(testpy_test.server_address, port)
            yield testpy_test.server_address


@pytest.fixture(scope=dynamic_scope())
def port(request, testpy_test: PythonTest | None):
    return int(request.config.getoption("--port"))


@pytest.fixture(scope="session")
def cargo_available():
    if shutil.which("cargo") is None:
        skip_env("cargo not found in PATH")


@pytest.fixture(scope="session")
def cqlsh_rs_repo_dir():
    return os.path.join(os.path.dirname(__file__), "..", "..", "..", "tools", "cqlsh-rs")


@pytest.fixture(scope="session")
def cargo_precompile(cargo_available, cqlsh_rs_repo_dir):
    lock_path = os.path.join(tempfile.gettempdir(), "cqlsh-rs-cargo-build-auth.lock")
    with open(lock_path, "w") as lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            print("\n=== cqlsh-rs: compiling auth integration test binary ===", flush=True)
            proc = subprocess.Popen(
                ["cargo", "test", "--test", "integration", "--features", "test-auth", "--no-run"],
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
            print("=== cqlsh-rs: auth compilation complete ===", flush=True)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
