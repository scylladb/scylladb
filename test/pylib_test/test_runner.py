#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
import sys

import pytest

from test import TOP_SRC_DIR


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
