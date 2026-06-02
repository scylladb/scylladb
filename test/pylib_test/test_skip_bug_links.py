#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Codebase guard test: validate links used by @pytest.mark.skip_bug.

The link validator is intentionally imported from
``test.pylib.skip_types`` so this CI guard and marker validation logic
cannot drift apart.
"""


import ast

from test.pylib.skip_types import _is_valid_skip_bug_link
from test.pylib_test._scan_py_files import iter_test_py_files, parse_python_file


def _is_skip_bug_marker(node: ast.Call) -> bool:
    func = node.func
    if not isinstance(func, ast.Attribute) or func.attr != "skip_bug":
        return False
    mark_attr = func.value
    if not isinstance(mark_attr, ast.Attribute) or mark_attr.attr != "mark":
        return False
    base = mark_attr.value
    return isinstance(base, ast.Name) and base.id == "pytest"


def test_skip_bug_markers_use_valid_links():
    violations: list[str] = []

    for path in iter_test_py_files():
        tree = parse_python_file(path)
        if tree is None:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not _is_skip_bug_marker(node):
                continue

            link_kw = next((kw for kw in node.keywords if kw.arg == "link"), None)
            if link_kw is None:
                violations.append(f"  {path}:{node.lineno} missing link=...")
                continue

            if not isinstance(link_kw.value, ast.Constant) or not isinstance(link_kw.value.value, str):
                violations.append(f"  {path}:{node.lineno} link must be a string literal")
                continue

            link = link_kw.value.value
            if not _is_valid_skip_bug_link(link):
                violations.append(
                    f"  {path}:{node.lineno} invalid link {link!r} (expected GitHub issue URL or ScyllaDB Jira URL)"
                )

    assert not violations, "Invalid @pytest.mark.skip_bug link(s) found:\n" + "\n".join(violations)
