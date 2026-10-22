#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for deciding which suites have their coverage profiles routed."""

from types import SimpleNamespace

import pytest

from test.pylib.runner import coverage_enabled


def config(coverage: bool, coverage_modes: list[str] | None) -> SimpleNamespace:
    return SimpleNamespace(option=SimpleNamespace(coverage=coverage, coverage_modes=coverage_modes))


def suite(**cfg) -> SimpleNamespace:
    return SimpleNamespace(cfg=cfg)


@pytest.mark.parametrize("collect, modes, mode, expected", [
    (False, None, "dev", False),
    (False, None, "coverage", True),
    # Plain pytest --coverage has no --coverage-mode: every mode is collected.
    (True, None, "dev", True),
    (True, ["dev"], "dev", True),
    (True, ["dev"], "release", False),
])
def test_coverage_enabled(collect: bool, modes: list[str] | None, mode: str, expected: bool) -> None:
    assert coverage_enabled(config(collect, modes), mode, suite()) == expected


def test_coverage_disabled_for_suite() -> None:
    assert not coverage_enabled(config(True, None), "coverage", suite(coverage=False))
