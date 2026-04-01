#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Pytest plugin for typed skip markers.

Framework-agnostic: the concrete skip types are provided by the
project's ``conftest.py`` via :class:`SkipReasonPlugin`.

Usage as decorator (after conftest.py registers the plugin)::

    @pytest.mark.skip_bug(reason="scylladb/scylladb#12345")
    @pytest.mark.skip_not_implemented(reason="no per tablet support yet")

Usage at runtime (inside test body or fixture) — prefer the
convenience wrappers from :mod:`test.pylib.skip_types`::

    from test.pylib.skip_types import skip_env
    skip_env("need --runveryslow option")
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum

import pytest

# StashKeys to carry skip metadata from collection to reporting.
SKIP_TYPE_KEY = pytest.StashKey[str]()
SKIP_REASON_KEY = pytest.StashKey[str]()


def skip(reason: str, skip_type: StrEnum):
    """Skip the current test at runtime with a typed reason.

    Use instead of bare ``pytest.skip()``.
    The *skip_type* should be a :class:`~enum.StrEnum` member defined
    by the project (e.g. ``SkipType.SKIP_ENV``).
    """
    pytest.skip(f"[{skip_type}] {reason}")


def skip_marker(item: pytest.Item, reason: str, skip_type: str) -> None:
    """Add a typed skip marker to an item during collection.

    Use from ``pytest_collection_modifyitems`` hooks (e.g. runner.py's
    ``skip_mode`` processing) to skip a test with full type metadata.
    The caller does not need to know how the metadata is stored.
    """
    item.add_marker(pytest.mark.skip(reason=f"[{skip_type}] {reason}"))
    item.stash[SKIP_TYPE_KEY] = skip_type
    item.stash[SKIP_REASON_KEY] = reason


class SkipReasonPlugin:
    """Pytest plugin that converts typed skip markers into real skips
    and enriches reports with skip metadata.

    Args:
        skip_types: A :class:`~enum.StrEnum` whose members define the
            typed skip markers.  Each member's ``marker_name`` property
            becomes the pytest marker name, and its value becomes the
            tag written to reports.
        report_callback: Optional callback invoked with
            ``(skip_type, reason)`` for every skipped test that has
            type metadata.
    """

    def __init__(
        self,
        skip_types: type[StrEnum],
        *,
        report_callback: Callable[[str, str], None] | None = None,
    ) -> None:
        self._skip_types = skip_types
        self._report_callback = report_callback

    @staticmethod
    def _get_reason(mark: pytest.Mark) -> str:
        """Extract reason from a marker (keyword or positional)."""
        return mark.kwargs.get("reason") or (mark.args[0] if mark.args else "")

    @staticmethod
    def _parse_skip_type(longrepr) -> tuple[str, str] | None:
        """Try to extract ``(skip_type, reason)`` from a ``[type] reason`` message.

        Returns ``None`` when *longrepr* does not match the expected format.
        """
        text = longrepr[-1] if isinstance(longrepr, tuple) else str(longrepr)
        text = text.removeprefix("Skipped: ")
        if text.startswith("[") and "]" in text:
            tag, _, reason = text[1:].partition("]")
            return tag, reason.lstrip()
        return None

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        """Apply typed markers and warn on bare skips."""
        for item in items:
            # Convert typed skip markers to real pytest.mark.skip.
            for st in self._skip_types:
                for mark in item.iter_markers(st.marker_name):
                    reason = self._get_reason(mark)
                    if not reason:
                        raise pytest.UsageError(
                            f"Marker @pytest.mark.{st.marker_name} on {item.nodeid} "
                            f"requires a 'reason' argument."
                        )
                    item.add_marker(pytest.mark.skip(reason=f"[{st}] {reason}"))
                    item.stash[SKIP_TYPE_KEY] = str(st)
                    item.stash[SKIP_REASON_KEY] = reason

            # Reject bare pytest.mark.skip not added by typed markers.
            # skip_mode sets SKIP_TYPE_KEY before this hook runs (trylast).
            if SKIP_TYPE_KEY not in item.stash:
                bare = [self._get_reason(m) for m in item.iter_markers("skip")]
                if bare:
                    alternatives = ", ".join(
                        f"@pytest.mark.{st.marker_name}" for st in self._skip_types)
                    raise pytest.UsageError(
                        f"Untyped skip on {item.nodeid}: {'; '.join(bare)}. "
                        f"Use {alternatives} instead.",
                    )

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item: pytest.Item):
        """Enrich JUnit XML reports (and optionally custom reporters) with skip metadata."""
        outcome = yield
        report = outcome.get_result()

        if not report.skipped:
            return

        skip_type = item.stash.get(SKIP_TYPE_KEY, None)
        reason = item.stash.get(SKIP_REASON_KEY, "")

        # Runtime skips (via skip()) don't set stash keys — parse from message.
        if skip_type is None and report.longrepr:
            parsed = self._parse_skip_type(report.longrepr)
            if parsed is None:
                return
            skip_type, reason = parsed

        if skip_type is None:
            return

        item.user_properties.append(("skip_type", skip_type))
        if reason:
            item.user_properties.append(("skip_reason", reason))

        if self._report_callback is not None:
            self._report_callback(skip_type, reason)
