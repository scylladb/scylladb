#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A ``file::test`` argument in a file the suite config disables for every
requested mode is dropped before pytest sees it: pytest would exit 4
("found no collectors") for it, while a plain file argument just collects
nothing."""

import pathlib

import pytest

from test.pylib.runner import TestSuiteConfig, drop_disabled_tests


@pytest.fixture
def suite(tmp_path: pathlib.Path) -> pathlib.Path:
    (tmp_path / "test_config.yaml").write_text("run_in_release:\n  - test_a\n")
    (tmp_path / "sub").mkdir()
    for name in ("test_a.py", "test_b.py", "sub/test_c.py"):
        (tmp_path / name).touch()
    return tmp_path


def test_from_path_finds_the_nearest_config(suite: pathlib.Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    assert TestSuiteConfig.from_path(suite / "sub" / "test_c.py").path == suite
    assert TestSuiteConfig.from_path(tmp_path_factory.mktemp("no_config") / "test_d.py") is None


def test_drop_disabled_tests(suite: pathlib.Path, capsys: pytest.CaptureFixture[str]) -> None:
    a, b = str(suite / "test_a.py"), str(suite / "test_b.py")
    # Only the explicitly named test in a disabled file goes; the file argument stays.
    assert drop_disabled_tests([f"{a}::test_x", f"{b}::test_y", a], ["dev"]) == [f"{b}::test_y", a]
    assert f"skipping {a}::test_x: disabled in dev mode" in capsys.readouterr().err
    # Enabled in one of the requested modes: kept.
    assert drop_disabled_tests([f"{a}::test_x"], ["dev", "release"]) == [f"{a}::test_x"]
    # No suite config above the file: nothing to decide, kept.
    assert drop_disabled_tests(["/no/such/suite/test_e.py::test_z"], ["dev"]) == ["/no/such/suite/test_e.py::test_z"]
