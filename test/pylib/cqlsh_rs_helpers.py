# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Shared helpers for cqlsh-rs cargo integration test wrappers."""

import fcntl
import os
import shutil
import subprocess
import tempfile
import time


CQLSH_RS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "tools", "cqlsh-rs")

CACHE_TTL_SECONDS = 60


def _write_cache(path: str, content: str):
    with open(path, "w") as f:
        f.write(content)


def discover_cargo_tests(category: str, invalidate_cache: bool = False) -> list[str]:
    """List cargo integration tests for the given category.

    Uses a file-based cache with locking to ensure all xdist workers
    see the same result (avoiding collection mismatch errors).
    Returns empty list if binary is not compiled yet.
    """
    if shutil.which("cargo") is None:
        return []

    cache_file = os.path.join(tempfile.gettempdir(), f"cqlsh-rs-tests-{category}.txt")
    lock_file = os.path.join(tempfile.gettempdir(), f"cqlsh-rs-discovery-{category}.lock")

    with open(lock_file, "w") as lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            if invalidate_cache and os.path.exists(cache_file):
                os.unlink(cache_file)
            if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < CACHE_TTL_SECONDS:
                with open(cache_file) as f:
                    content = f.read().strip()
                if content:
                    return content.splitlines()
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
                _write_cache(cache_file, "")
                return []

            if result.returncode != 0:
                _write_cache(cache_file, "")
                return []

            tests = []
            for line in result.stdout.splitlines():
                if line.endswith(": test"):
                    tests.append(line[: -len(": test")])

            _write_cache(cache_file, "\n".join(tests))
            return tests
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)


def run_cargo_test(test_name: str, category: str, env: dict, cqlsh_rs_repo_dir: str,
                   timeout: int = 120):
    result = subprocess.run(
        ["cargo", "test", "--test", "integration", "--features", category,
         test_name, "--", "--exact", "--ignored"],
        cwd=cqlsh_rs_repo_dir,
        env=env,
        timeout=timeout,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"cargo test {test_name!r} failed:\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def run_cargo_test_all(category: str, env: dict, cqlsh_rs_repo_dir: str,
                       timeout: int = 600):
    result = subprocess.run(
        ["cargo", "test", "--test", "integration", "--features", category,
         "--", "--ignored", "--test-threads=1"],
        cwd=cqlsh_rs_repo_dir,
        env=env,
        timeout=timeout,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"cargo integration tests ({category}) failed:\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
