#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""ScyllaDB skip-type definitions and convenience wrappers.

This module is the single source of truth for:
* :class:`SkipType` — the enum of all typed skip categories.
* Per-category runtime skip helpers (``skip_bug``, ``skip_env``, …)
  that are thin wrappers around :func:`skip_reason_plugin.skip`.

Import ``SkipType`` or the helpers from here instead of defining
them in ``conftest.py``.

Usage::

    from test.pylib.skip_types import SkipType, skip_env

    # As a runtime skip inside a test or fixture:
    skip_env("HTTPS-specific tests need the '--https' option")
"""

from enum import StrEnum

import re

import pytest

from test.pylib.skip_reason_plugin import skip


_GITHUB_ISSUE_LINK_RE = re.compile(r"^https://github\.com/[^/\s]+/[^/\s]+/issues/\d+/?$")
_JIRA_LINK_RE = re.compile(r"^https://scylladb\.atlassian\.net/browse/[A-Z][A-Z0-9]+-\d+$")


def _is_valid_skip_bug_link(link: str) -> bool:
    """Return True if *link* is a supported skip_bug issue URL."""
    return bool(_GITHUB_ISSUE_LINK_RE.fullmatch(link) or _JIRA_LINK_RE.fullmatch(link))


# ScyllaDB-specific skip categories.
# Each member name (lowercased) is the pytest marker, e.g.
# ``SKIP_BUG`` → ``@pytest.mark.skip_bug``, tag ``"bug"``.
class SkipType(StrEnum):
    SKIP_BUG = "bug"
    SKIP_NOT_IMPLEMENTED = "not_implemented"
    SKIP_SLOW = "slow"
    SKIP_ENV = "env"

    @staticmethod
    def _validate_skip_bug_args(link: str, reason: str, context: str) -> str:
        """Validate and format skip_bug arguments."""
        link = link.strip()
        reason = reason.strip()
        if not link:
            raise pytest.UsageError(f"{context}: 'link' is required.")
        if not _is_valid_skip_bug_link(link):
            raise pytest.UsageError(
                f"{context}: invalid 'link' value {link!r}. "
                "Expected a GitHub issue URL or a ScyllaDB Jira URL.",
            )
        if not reason:
            raise pytest.UsageError(f"{context}: 'reason' is required.")
        return f"{reason} ({link})"

    @staticmethod
    def get_reason_default(mark, context):
        """Default: just extract 'reason' or first arg."""
        return mark.kwargs.get("reason") or (mark.args[0] if mark.args else "")

    @staticmethod
    def get_reason_skip_bug(mark, context):
        """Strict skip_bug: require link and reason, validate link format."""
        if mark.args:
            raise pytest.UsageError(f"{context}: no longer accepts positional arguments; use link=... and reason=...")
        if set(mark.kwargs) != {"link", "reason"}:
            raise pytest.UsageError(f"{context}: requires both 'link' and 'reason' keyword arguments (got {sorted(mark.kwargs)}).")
        return SkipType._validate_skip_bug_args(mark.kwargs["link"], mark.kwargs["reason"], context)

    @property
    def marker_name(self) -> str:
        return self.name.lower()


# ── Convenience wrappers ────────────────────────────────────────────────
# Each wrapper calls :func:`skip` with the appropriate ``SkipType``,
# so callers never need to import ``SkipType`` separately.


def skip_bug(*, link: str, reason: str) -> None:
    """Runtime skip for a known bug with strict ``link`` and ``reason`` fields."""
    context = "skip_bug()"
    msg = SkipType._validate_skip_bug_args(link, reason, context)
    skip(msg, skip_type=SkipType.SKIP_BUG)


def skip_not_implemented(reason: str) -> None:
    """Runtime skip for a feature that is not yet implemented."""
    skip(reason, skip_type=SkipType.SKIP_NOT_IMPLEMENTED)


def skip_slow(reason: str) -> None:
    """Runtime skip for a test that is too slow for regular CI."""
    skip(reason, skip_type=SkipType.SKIP_SLOW)


def skip_env(reason: str) -> None:
    """Runtime skip for a missing environment requirement."""
    skip(reason, skip_type=SkipType.SKIP_ENV)
