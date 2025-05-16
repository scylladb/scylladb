#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from pathlib import Path
from types import SimpleNamespace


def make_test_object(test_name: str, suite: str, run_id: int, mode: str = 'no_mode', log_dir: Path = None) -> SimpleNamespace:
    """
    Returns object that used in resource gathering.
    It needed to not change the logic of writing metrics to DB that used in test types from test.py.
    """
    test = SimpleNamespace()
    test.time_end = 0
    test.time_start = 0
    test.id = run_id
    test.mode = mode
    test.success = False
    test.shortname = test_name

    test.suite = SimpleNamespace()
    test.suite.log_dir = log_dir / mode
    test.suite.name = suite

    return test
