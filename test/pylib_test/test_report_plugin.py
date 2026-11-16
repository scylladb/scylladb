#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the Allure run parameters set by ReportPlugin.

Uses pytester to run a sub-pytest process with Allure results enabled and
reads the result files it writes.
"""

import json
import os
import pathlib
import platform
import textwrap

import pytest

pytest_plugins = ["pytester"]

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# Collects *.cc files as one C++-like test case: like CppTestCase, a plain pytest.Item, so pytest runs no fixtures for it.
_CONFTEST = textwrap.dedent("""\
    import pytest

    from test.pylib.report_plugin import ReportPlugin
    from test.pylib.runner import BUILD_MODE, RUN_ID


    class CppLikeItem(pytest.Item):
        def runtest(self):
            pass


    class CppLikeFile(pytest.File):
        def collect(self):
            yield CppLikeItem.from_parent(self, name="cpp_case")


    def pytest_collect_file(file_path, parent):
        if file_path.suffix == ".cc":
            return CppLikeFile.from_parent(parent, path=file_path)


    def pytest_configure(config):
        config.pluginmanager.register(ReportPlugin())


    def pytest_collection_modifyitems(items):
        for item in items:
            item.stash[BUILD_MODE] = "dev"
            item.stash[RUN_ID] = 3
""")


def test_cpp_and_python_tests_get_run_parameters(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    """A C++ test case and a Python test get the same Allure run parameters and mode tag."""
    # The inner session runs in a temporary directory, so it can only import
    # test.pylib if the repository root is on the path.
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(_REPO_ROOT), os.environ.get("PYTHONPATH")])))
    pytester.makeini("[pytest]\naddopts = -p no:sugar -p no:xdist -p no:cacheprovider\n")
    pytester.makeconftest(_CONFTEST)
    pytester.makepyfile(test_python="def test_python():\n    pass\n")
    (pytester.path / "case_test.cc").touch()
    alluredir = pytester.path / "allure"

    result = pytester.runpytest_subprocess(f"--alluredir={alluredir}")

    assert result.ret == pytest.ExitCode.OK
    result.assert_outcomes(passed=2)
    results = [json.loads(p.read_text()) for p in alluredir.glob("*-result.json")]
    assert sorted(r["name"] for r in results) == ["cpp_case", "test_python"]
    params = {"arch": repr(platform.machine()), "build_mode": repr("dev"), "run_id": "3"}
    assert {r["name"]: {p["name"]: p["value"] for p in r.get("parameters", [])} for r in results} == {
        "cpp_case": params,
        "test_python": params,
    }
    assert {r["name"]: [label["value"] for label in r["labels"] if label["name"] == "tag"] for r in results} == {
        "cpp_case": ["dev"],
        "test_python": ["dev"],
    }
