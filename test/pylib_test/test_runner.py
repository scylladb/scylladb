#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
import sys
import xml.etree.ElementTree as ET

import pytest

from test import TOP_SRC_DIR
import test.pylib.runner as runner_plugin
from test.pylib.runner import TestSuiteConfig as RunnerTestSuiteConfig


def _load_test_runner_module():
    spec = spec_from_file_location("scylla_test_runner", TOP_SRC_DIR / "test.py")
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def test_runner_module():
    return _load_test_runner_module()


@pytest.mark.parametrize(
    ("nr_cpus", "memory_cap", "cpus_per_test_job", "expected"),
    [
        (1, 1, 1.0, 4),
        (12, 3, 1.0, 12),
        (12, 20, 2.0, 24),
    ],
)
def test_threads_calculator_scales_auto_jobs_by_four(
    test_runner_module,
    nr_cpus: int,
    memory_cap: int,
    cpus_per_test_job: float,
    expected: int,
) -> None:
    calculator = object.__new__(test_runner_module.ThreadsCalculator)
    calculator.default_num_jobs_mem = memory_cap
    calculator.cpus_per_test_job = cpus_per_test_job

    assert calculator.get_number_of_threads(nr_cpus) == expected


class _FakeCollector:
    def __init__(self, path: Path, extra_opts: str) -> None:
        self.path = path
        self.parent = None
        self.stash = {}
        self.config = SimpleNamespace(getoption=lambda name: extra_opts if name == "--extra-scylla-cmdline-options" else None)


def test_testsuite_config_extra_cmdline_options_are_idempotent(tmp_path: Path) -> None:
    """Regression test for repeated suite config collection with CLI extra Scylla options."""

    suite_dir = tmp_path / "test" / "python"
    suite_dir.mkdir(parents=True)
    (suite_dir / "test_config.yaml").write_text(
        "type: Python\nextra_scylla_cmdline_options:\n  - --logger-log-level\n  - raft=debug\n",
        encoding="utf-8",
    )

    parent = _FakeCollector(suite_dir, "--logger-log-level raft=trace")
    collector = _FakeCollector(suite_dir / "test_a.py", "--logger-log-level raft=trace")
    collector.parent = parent

    suite_config = RunnerTestSuiteConfig.from_pytest_node(collector)
    assert suite_config is not None
    options = list(suite_config.cfg["extra_scylla_cmdline_options"])

    assert options == ["--logger-log-level", "raft=debug", "--logger-log-level", "raft=trace"]
    assert options.count("raft=trace") == 1


class _FakeItem:
    def __init__(self, nodeid: str, config, marks: dict[str, object] | None = None) -> None:
        self.nodeid = nodeid
        self.path = Path(nodeid.split("::", 1)[0])
        self.config = config
        self.stash: dict[object, object] = {}
        self._marks = marks or {}

    def get_closest_marker(self, name: str):
        return self._marks.get(name)

    def iter_markers(self, name: str | None = None):
        if name is None:
            values = self._marks.values()
        else:
            mark = self._marks.get(name)
            if mark is None:
                values = ()
            elif isinstance(mark, (list, tuple)):
                values = mark
            else:
                values = (mark,)
        return iter(values)


class _FakeNodeReporter:
    def __init__(self) -> None:
        self.to_xml_calls = 0

    def to_xml(self):
        self.to_xml_calls += 1
        return ET.Element("testcase")


class _FakeXmlReporter:
    def __init__(self) -> None:
        self.node_reporter_obj = _FakeNodeReporter()

    def node_reporter(self, report):
        return self.node_reporter_obj


def test_skipped_tests_are_not_converted_into_budget_failures() -> None:
    """Regression test for skipped items being short-circuited before synthetic budget failures."""

    options = {
        "--collect-only": False,
        "--scylla-resource-scheduler": "on",
        "--scylla-resource-cpus": 1,
        "--scylla-resource-memory": "1G",
        "--tmpdir": "/tmp",
    }
    config = SimpleNamespace(getoption=lambda name: options.get(name))
    item = _FakeItem(
        "test/cluster/dtest/auth_test.py::test_one",
        config,
        {
            "skip": pytest.mark.skip(reason="skip").mark,
            "scylla_resources": pytest.mark.scylla_resources(cpu=100, mem="100G").mark,
        },
    )
    item.stash[runner_plugin.TEST_SUITE] = SimpleNamespace(name="cluster", cfg={"type": "Topology"}, path=Path("test/cluster"))
    item.stash[runner_plugin.BUILD_MODE] = "dev"

    assert runner_plugin._scylla_resource_budget_failure_for_item(item) is None


def test_skipif_tests_are_not_converted_into_budget_failures() -> None:
    """Regression test for skipif items being short-circuited before synthetic budget failures."""

    options = {
        "--collect-only": False,
        "--scylla-resource-scheduler": "on",
        "--scylla-resource-cpus": 1,
        "--scylla-resource-memory": "1G",
        "--tmpdir": "/tmp",
    }
    config = SimpleNamespace(getoption=lambda name: options.get(name))
    item = _FakeItem(
        "test/cluster/dtest/auth_test.py::test_one",
        config,
        {
            "skipif": pytest.mark.skipif(True, reason="skipif").mark,
            "scylla_resources": pytest.mark.scylla_resources(cpu=100, mem="100G").mark,
        },
    )
    item.stash[runner_plugin.TEST_SUITE] = SimpleNamespace(name="cluster", cfg={"type": "Topology"}, path=Path("test/cluster"))
    item.stash[runner_plugin.BUILD_MODE] = "dev"

    assert runner_plugin._scylla_resource_budget_failure_for_item(item) is None


def test_junit_function_path_keeps_test_prefix(pytestconfig: pytest.Config) -> None:
    """Regression test for the JUnit function_path attribute preserving the real test path."""

    fake_xml = _FakeXmlReporter()
    previous_xml = pytestconfig.stash.get(runner_plugin.xml_key, None)
    pytestconfig.stash[runner_plugin.xml_key] = fake_xml

    try:
        report = SimpleNamespace(
            nodeid="test/cluster/test_a.py::test_one.dev.1",
            location=("test/cluster/test_a.py", 0, "test_one.dev.1"),
        )

        runner_plugin.pytest_runtest_logreport(report)

        assert fake_xml.node_reporter_obj.to_xml().attrib["function_path"] == "test/cluster/test_a.py::test_one"
    finally:
        if previous_xml is None:
            del pytestconfig.stash[runner_plugin.xml_key]
        else:
            pytestconfig.stash[runner_plugin.xml_key] = previous_xml
