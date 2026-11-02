#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A ``file::test`` argument in a file the suite config disables for every
requested mode is dropped before pytest sees it: pytest would exit 4
("found no collectors") for it, while a plain file argument just collects
nothing."""

import argparse
import importlib.util
import pathlib
import sys

import pytest

from test import TOP_SRC_DIR
from test.pylib import runner
from test.pylib.runner import TestSuiteConfig, drop_disabled_tests

CONFIG = """\
disable:
  - test_off
skip_in_dev:
  - sub/test_c
skip_in_debug_modes:
  - test_dbg
run_in_release:
  - test_a
"""


@pytest.fixture
def suite(tmp_path: pathlib.Path) -> pathlib.Path:
    (tmp_path / "test_config.yaml").write_text(CONFIG)
    (tmp_path / "sub").mkdir()
    for name in ("test_a.py", "test_b.py", "test_off.py", "test_dbg.py", "sub/test_c.py"):
        (tmp_path / name).touch()
    return tmp_path


def test_from_path_finds_the_nearest_config(suite: pathlib.Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    assert TestSuiteConfig.from_path(suite / "sub" / "test_c.py").path == suite
    assert TestSuiteConfig.from_path(tmp_path_factory.mktemp("no_config") / "test_d.py") is None


def test_from_path_stops_at_test_dir(suite: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A config above test/ is not looked at.
    monkeypatch.setattr(runner, "TEST_DIR", suite / "sub")
    assert TestSuiteConfig.from_path(suite / "sub" / "test_c.py") is None


@pytest.mark.parametrize(("file", "modes", "dropped"), [
    ("test_a.py", ["dev"], True),  # run_in_<other mode>
    ("test_a.py", ["custom_exe"], True),
    ("test_a.py", ["release"], False),
    ("test_a.py", ["dev", "release"], False),  # enabled in one of the modes
    ("test_off.py", ["release"], True),  # disable
    ("sub/test_c.py", ["dev"], True),  # skip_in_<mode>, entry relative to the suite
    ("sub/test_c.py", ["release"], False),
    ("test_dbg.py", ["debug", "sanitize"], True),  # skip_in_debug_modes
    ("test_dbg.py", ["debug", "dev"], False),
    ("test_b.py", ["dev"], False),  # not in the config
])
def test_drop_disabled_tests(suite: pathlib.Path, file: str, modes: list[str], dropped: bool,
                             capsys: pytest.CaptureFixture[str]) -> None:
    arg = f"{suite / file}::test_x"
    assert drop_disabled_tests([arg], modes) == ([] if dropped else [arg])
    assert (f"skipping {arg}: disabled in {', '.join(modes)} mode" in capsys.readouterr().err) == dropped


def test_drop_disabled_tests_keeps_other_arguments(suite: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    a, b = str(suite / "test_a.py"), str(suite / "test_b.py")
    # Every node ID form in a disabled file goes; the rest keeps its order.
    args = [f"{a}::test_x", f"{b}::test_y", f"{a}::TestCls::test_x", a, f"{a}::test_x[1-a]", f"{b}::test_z"]
    assert drop_disabled_tests(args, ["dev"]) == [f"{b}::test_y", a, f"{b}::test_z"]
    # A path relative to the current directory.
    monkeypatch.chdir(suite)
    assert drop_disabled_tests(["test_a.py::test_x", "sub/test_c.py::test_x"], ["dev"]) == []
    # A missing file is left for pytest to report, even if its stem is disabled.
    assert drop_disabled_tests([f"{a[:-3]}::test_x"], ["dev"]) == [f"{a[:-3]}::test_x"]
    # No suite config above the file: nothing to decide, kept.
    assert drop_disabled_tests(["/no/such/suite/test_e.py::test_z"], ["dev"]) == ["/no/such/suite/test_e.py::test_z"]


def test_drop_disabled_tests_reads_each_config_once(suite: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    looked_up = []
    from_path = TestSuiteConfig.from_path
    monkeypatch.setattr(TestSuiteConfig, "from_path", lambda path: looked_up.append(path.parent) or from_path(path))
    a, b, c = (suite / name for name in ("test_a.py", "test_b.py", "sub/test_c.py"))
    drop_disabled_tests([f"{a}::test_x", f"{b}::test_y", f"{a}::test_z", f"{c}::test_x", f"{c}::test_y"], ["dev"])
    assert looked_up == [suite, suite / "sub"]


def test_run_pytest_returns_5_when_every_named_test_is_dropped(suite: pathlib.Path,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    # test.py is a script next to the test/ package, so it is loaded by path;
    # its dataclasses look the module up in sys.modules.
    spec = importlib.util.spec_from_file_location("test_py", TOP_SRC_DIR / "test.py")
    test_py = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "test_py", test_py)
    spec.loader.exec_module(test_py)
    options = argparse.Namespace(tmpdir=str(suite), name=[f"{suite / 'test_a.py'}::test_x"],
                                 keep_duplicates=True, modes=["dev"])
    # Returns before building the pytest command line, which would otherwise run the whole test/ directory.
    assert test_py.run_pytest(options) == pytest.ExitCode.NO_TESTS_COLLECTED
