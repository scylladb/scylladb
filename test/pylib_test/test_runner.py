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

import pytest

from test import TOP_SRC_DIR
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
