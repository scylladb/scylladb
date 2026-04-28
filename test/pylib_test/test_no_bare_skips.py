#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Codebase guard tests: verify no bare pytest.skip usage remains.

These are project-specific tests that scan the ScyllaDB test tree
for bare @pytest.mark.skip decorators and bare pytest.skip() calls.
Any new skip must use the typed markers or the typed skip() helper.
"""

import ast
import os
import pathlib
import subprocess
import sys

_TEST_ROOT = pathlib.Path(__file__).resolve().parent.parent

def _iter_test_py_files():
    """Yield all .py files under test/ excluding pylib_test/, pylib/ and __pycache__."""
    for p in sorted(_TEST_ROOT.rglob("*.py")):
        rel = p.relative_to(_TEST_ROOT)
        parts = rel.parts
        if "__pycache__" in parts:
            continue
        if parts[0] in ("pylib_test", "pylib"):
            continue
        yield p


def test_no_bare_pytest_skip_calls_in_codebase():
    """Verify no test files use bare pytest.skip() (must use typed skip() helper)."""
    violations = []
    for path in _iter_test_py_files():
        source = path.read_text()
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if (isinstance(func, ast.Attribute) and func.attr == "skip"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "pytest"):
                violations.append(f"  {path}:{node.lineno}")
    assert not violations, (
        "Found bare pytest.skip() — use the typed skip() helper instead "
        "(from test.pylib.skip_reason_plugin import skip):\n"
        + "\n".join(violations)
    )


def test_no_bare_skip_markers_in_collection():
    """Collect all real Python tests and verify no bare @pytest.mark.skip exists.

    The skip_reason_plugin raises pytest.UsageError during collection
    if any bare skip decorator is found, so --collect-only is enough.
    No Scylla binary is needed — only Python collection.
    """

    # When running under xdist (-n), workers inherit PYTEST_XDIST_WORKER.
    # If the subprocess inherits it, the runner plugin thinks it is a
    # worker and skips creating the log directory — causing a
    # FileNotFoundError.  Strip xdist env vars so the subprocess runs
    # as a standalone main process.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("PYTEST_XDIST")}
    result = subprocess.run(
        [sys.executable, "-m", "pytest",
         "--collect-only",
         "--ignore=boost", "--ignore=raft",
         "--ignore=ldap", "--ignore=vector_search",
         "--ignore=unit",
         "-p", "no:sugar"],
        capture_output=True, text=True,
        cwd=str(_TEST_ROOT),
        env=env,
    )
    # If a bare skip exists, plugin raises UsageError → non-zero exit.
    assert result.returncode == 0, (
            "Collection failed — a bare @pytest.mark.skip was found.\n"
            + result.stdout + result.stderr
    )