#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Guard fixed issues from staying in skip_bug references.

CI provides PR_MESSAGE, which may contain lines like::

    Fixes: SCYLLADB-123                                    # JIRA key
    Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-123
    Fixes: 9999                                            # GitHub issue in scylladb/scylladb
    Fixes: #9999                                           # same, with leading '#'
    Fixes: scylladb/scylla-enterprise#9999                 # cross-repo GitHub issue
    Fixes: https://github.com/scylladb/scylla-enterprise/issues/9999

For each fixed issue listed in PR_MESSAGE, this test verifies no Python
test still references it via ``skip_bug()`` / ``@pytest.mark.skip_bug(...)``.

CI runs this with::

    ./tools/toolchain/dbuild -e PR_MESSAGE='<commit message>' -- \\
        pytest test/pylib_test --junit-xml=testlog/framework_tests/framework_test.xml
"""

import ast
import os
import pathlib
import re
from collections.abc import Iterator

import pytest

from test.pylib_test._scan_py_files import iter_test_py_files, parse_python_file

_FIXES_PREFIX = "fixes:"
# Bare 'Fixes: 9999' / 'Fixes: #9999' refer to the main scylladb/scylladb repo.
_DEFAULT_GH_REPO = "scylladb/scylladb"

# Recognized GitHub issue URL forms in PR_MESSAGE 'Fixes:' tokens,
# including an optional trailing slash.
_GH_URL_RE = re.compile(
    r"^https?://github\.com/(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+)/issues/(?P<num>\d+)/?$",
)
# owner/repo#123 cross-repo shorthand.
_GH_SHORTHAND_RE = re.compile(
    r"^(?P<owner>[^/\s#]+)/(?P<repo>[^/\s#]+)#(?P<num>\d+)$",
    re.IGNORECASE,
)
# Full JIRA browse URL.
_JIRA_URL_RE = re.compile(
    r"^https?://scylladb\.atlassian\.net/browse/(?P<key>[A-Z][A-Z0-9]+-\d+)$",
    re.IGNORECASE,
)
# Bare JIRA key like SCYLLADB-123.
_JIRA_KEY_RE = re.compile(r"^[A-Z][A-Z0-9]+-\d+$")


def _parse_fixes_token(token: str) -> tuple | None:
    """Classify a ``Fixes:`` token.

    Returns one of:
      * ``('jira', KEY)``                — JIRA key (matched as substring).
      * ``('gh', 'owner/repo', NUM)``    — GitHub issue. Bare numbers default
                                            to the main scylladb/scylladb repo.
      * ``None``                         — unrecognized token.
    """
    token = token.strip()
    if not token:
        return None

    if (m := _JIRA_URL_RE.match(token)):
        return ("jira", m.group("key").upper())

    if (m := _GH_URL_RE.match(token)):
        return ("gh", f"{m.group('owner')}/{m.group('repo')}".lower(), m.group("num"))

    if (m := _GH_SHORTHAND_RE.match(token)):
        return ("gh", f"{m.group('owner')}/{m.group('repo')}".lower(), m.group("num"))

    bare = token.lstrip("#").upper()
    if _JIRA_KEY_RE.match(bare):
        return ("jira", bare)
    if bare.isdigit():
        return ("gh", _DEFAULT_GH_REPO, bare)

    return None


def _extract_fixed_issues(pr_message: str) -> list[tuple]:
    """Extract issue references from ``Fixes:`` lines in PR_MESSAGE."""
    issues: list[tuple] = []
    for line in pr_message.splitlines():
        stripped = line.strip()
        if not stripped.lower().startswith(_FIXES_PREFIX):
            continue
        value = stripped[len(_FIXES_PREFIX):].strip()
        if not value:
            continue
        parsed = _parse_fixes_token(value.split()[0])
        if parsed is not None and parsed not in issues:
            issues.append(parsed)
    return issues


def _is_skip_bug_call(node: ast.Call) -> bool:
    """Match ``skip_bug(...)`` and ``<anything>.skip_bug(...)`` calls."""
    func = node.func
    if isinstance(func, ast.Name) and func.id == "skip_bug":
        return True
    if isinstance(func, ast.Attribute) and func.attr == "skip_bug":
        return True
    return False


def _iter_string_literals(node: ast.AST) -> Iterator[str]:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child.value


def _text_references_issue(text: str, issue: tuple) -> bool:
    """Return True if ``text`` references the given issue.

    * JIRA: case-insensitive substring of the key.
    * GitHub: substring of '/owner/repo/issues/NUM' (case-insensitive).
    """
    kind = issue[0]
    if kind == "jira":
        return issue[1] in text.upper()
    repo, num = issue[1], issue[2]
    return f"/{repo}/issues/{num}" in text.lower()


def _format_issue(issue: tuple) -> str:
    if issue[0] == "jira":
        return issue[1]
    repo, num = issue[1], issue[2]
    return f"{repo}#{num}"


def _collect_violations(path: pathlib.Path, fixed_issues: list[tuple]) -> list[str]:
    """Collect violations only from skip_bug(...) call sites."""
    tree = parse_python_file(path)
    if tree is None:
        return []

    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _is_skip_bug_call(node):
            continue
        for text in _iter_string_literals(node):
            for issue in fixed_issues:
                if _text_references_issue(text, issue):
                    violations.append(
                        f"  {path}:{node.lineno} references fixed issue "
                        f"{_format_issue(issue)}: {text.strip()}"
                    )
    return violations


def test_fixed_issues_not_referenced_by_skip_bug():
    """Verify fixed issues from PR_MESSAGE are not referenced in skip_bug usage."""
    pr_message = os.environ.get("PR_MESSAGE")
    if not pr_message:
        pytest.skip(
            "PR_MESSAGE environment variable is not set. "
            "This test is intended to run in CI with PR_MESSAGE provided."
        )

    fixed_issues = _extract_fixed_issues(pr_message)
    if not fixed_issues:
        pytest.skip(
            "PR_MESSAGE has no recognized 'Fixes:' entry "
            "(SCYLLADB-NNN, NNN, #NNN, owner/repo#NNN, or full GitHub/JIRA URL)"
        )

    violations: list[str] = []
    for path in iter_test_py_files():
        violations.extend(_collect_violations(path, fixed_issues))

    assert not violations, (
        "Found references to issue(s) marked as fixed in PR_MESSAGE. "
        "Remove/adjust those references before merging:\n"
        + "\n".join(violations)
    )
