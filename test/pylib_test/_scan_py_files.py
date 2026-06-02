#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Shared helpers for AST-based guard tests in ``pylib_test``.

Centralizes the boilerplate (test-file enumeration + safe AST parsing)
shared by ``test_no_bare_skips``, ``test_skip_bug_links`` and
``test_pr_message_bug_references``.
"""

import ast
import pathlib
from collections.abc import Iterator

from test import TEST_DIR

# Directories under test/ that AST guard tests should not scan:
# - 'pylib_test' contains the guards themselves and their fixtures
#   (which intentionally embed example skip patterns).
# - 'pylib' is shared framework code, not test code.
FRAMEWORK_DIRS = frozenset({"pylib", "pylib_test"})


def iter_test_py_files(*, exclude: frozenset[str] = FRAMEWORK_DIRS) -> Iterator[pathlib.Path]:
    """Yield Python files under ``test/``, skipping ``__pycache__`` and *exclude* dirs."""
    for path in sorted(TEST_DIR.rglob("*.py")):
        rel = path.relative_to(TEST_DIR)
        parts = rel.parts
        if "__pycache__" in parts:
            continue
        if exclude and parts[0] in exclude:
            continue
        yield path


def parse_python_file(path: pathlib.Path) -> ast.AST | None:
    """Return the parsed AST of *path* or ``None`` on read/syntax errors."""
    try:
        source = path.read_text()
    except (OSError, UnicodeDecodeError):
        return None
    try:
        return ast.parse(source, filename=str(path))
    except SyntaxError:
        return None
