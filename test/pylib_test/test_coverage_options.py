#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A bare pytest run must resolve --coverage and --coverage-mode like test.py.

test.py always forwards both options, already resolved. A bare pytest run did
not resolve them: `--coverage-mode dev` alone collected no profiles, and
`--coverage` alone made the cluster gate test membership in None.

These tests need no build: they call the resolver on a stand-in for
`config.option`.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from test.pylib.runner import normalize_coverage_options


def _option(coverage: bool, coverage_modes: list[str] | None) -> SimpleNamespace:
    return SimpleNamespace(coverage=coverage, coverage_modes=coverage_modes)


def test_coverage_mode_implies_coverage() -> None:
    option = _option(coverage=False, coverage_modes=["dev"])
    normalize_coverage_options(option, ["dev", "release"])
    assert option.coverage
    assert option.coverage_modes == ["dev"]


def test_coverage_alone_defaults_to_the_modes_being_run() -> None:
    option = _option(coverage=True, coverage_modes=None)
    normalize_coverage_options(option, ["dev", "release"])
    assert option.coverage_modes == ["dev", "release"]
    # The cluster gate's membership test, which raised TypeError on None.
    assert "dev" in option.coverage_modes


def test_neither_option_leaves_coverage_off() -> None:
    option = _option(coverage=False, coverage_modes=None)
    normalize_coverage_options(option, ["dev"])
    assert not option.coverage
    assert option.coverage_modes is None


def test_test_py_forwarded_options_are_unchanged() -> None:
    # test.py passes both, already resolved; resolving again must be a no-op.
    option = _option(coverage=True, coverage_modes=["dev"])
    normalize_coverage_options(option, ["dev", "release"])
    assert option.coverage
    assert option.coverage_modes == ["dev"]


def test_coverage_mode_not_being_run_is_rejected() -> None:
    # `--mode dev --coverage-mode release` runs nothing in release, so it would
    # end without the profiles it asked for. test.py rejects it too.
    option = _option(coverage=False, coverage_modes=["dev", "release"])
    with pytest.raises(pytest.UsageError, match="--coverage-mode release is not being run"):
        normalize_coverage_options(option, ["dev"])
