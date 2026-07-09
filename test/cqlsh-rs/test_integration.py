# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Parametrized pytest wrapper for cqlsh-rs Rust integration tests.

See tools/cqlsh-rs/tests/test_categories.toml for the mapping of test
categories to required Scylla configurations and env vars.
"""

import os
import shutil
import subprocess

import pytest


CQLSH_RS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "tools", "cqlsh-rs")


def discover_cargo_tests(category: str = "test-plain") -> list[str]:
    """List cargo integration tests. Only works if binary is already compiled
    (short timeout). Returns empty list otherwise — conftest.cargo_precompile
    ensures compilation happens before test execution."""
    if shutil.which("cargo") is None:
        return []

    try:
        result = subprocess.run(
            ["cargo", "test", "--test", "integration", "--features", category,
             "--", "--ignored", "--list"],
            cwd=CQLSH_RS_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, OSError):
        return []

    if result.returncode != 0:
        return []

    tests = []
    for line in result.stdout.splitlines():
        if line.endswith(": test"):
            tests.append(line[: -len(": test")])
    return tests


DISCOVERED_TESTS = discover_cargo_tests("test-plain")


@pytest.mark.parametrize("test_name", DISCOVERED_TESTS or ["PENDING_COMPILATION"])
def test_cargo(host, port, test_name, cargo_precompile, cqlsh_rs_repo_dir):
    if test_name == "PENDING_COMPILATION":
        # Binary wasn't compiled at collection time; re-discover after precompile
        tests = discover_cargo_tests("test-plain")
        if not tests:
            pytest.skip("No cargo tests discovered after compilation")
        # Run all discovered tests in a single invocation as fallback
        env = {**os.environ, "CQLSH_TEST_HOST": host, "CQLSH_TEST_PORT": str(port)}
        result = subprocess.run(
            ["cargo", "test", "--test", "integration", "--features", "test-plain",
             "--", "--ignored", "--test-threads=1"],
            cwd=cqlsh_rs_repo_dir,
            env=env,
            timeout=600,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"cargo integration tests failed:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
        return

    env = {**os.environ, "CQLSH_TEST_HOST": host, "CQLSH_TEST_PORT": str(port)}
    result = subprocess.run(
        ["cargo", "test", "--test", "integration", "--features", "test-plain",
         test_name, "--", "--exact", "--ignored"],
        cwd=cqlsh_rs_repo_dir,
        env=env,
        timeout=120,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"cargo test {test_name!r} failed:\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
