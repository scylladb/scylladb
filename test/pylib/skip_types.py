#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

from __future__ import annotations

from enum import StrEnum

from test.pylib.skip_reason_plugin import skip


class SkipType(StrEnum):
    """ScyllaDB-specific skip categories.

    Each member name (lowercased) is the pytest marker, e.g.
    ``SKIP_BUG`` → ``@pytest.mark.skip_bug``, tag ``"bug"``.
    """

    SKIP_BUG = "bug"
    SKIP_NOT_IMPLEMENTED = "not_implemented"
    SKIP_SLOW = "slow"
    SKIP_ENV = "env"

    @property
    def marker_name(self) -> str:
        return self.name.lower()


# ── Convenience wrappers ────────────────────────────────────────────────
# Each wrapper calls :func:`skip` with the appropriate ``SkipType``,
# so callers never need to import ``SkipType`` separately.


def skip_bug(reason: str) -> None:
    """Runtime skip for a known bug.  *reason* should be an issue reference."""
    skip(reason, skip_type=SkipType.SKIP_BUG)


def skip_not_implemented(reason: str) -> None:
    """Runtime skip for a feature that is not yet implemented."""
    skip(reason, skip_type=SkipType.SKIP_NOT_IMPLEMENTED)


def skip_slow(reason: str) -> None:
    """Runtime skip for a test that is too slow for regular CI."""
    skip(reason, skip_type=SkipType.SKIP_SLOW)


def skip_env(reason: str) -> None:
    """Runtime skip for a missing environment requirement."""
    skip(reason, skip_type=SkipType.SKIP_ENV)
