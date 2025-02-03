#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from pathlib import PosixPath

from pytest import Collector

from test.pylib.cpp.common_cpp_conftest import collect_items
from test.pylib.cpp.unit.unit_facade import UnitTestFacade


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    """
    Method triggered automatically by pytest to collect files from a directory. Boost and unit have the same logic for
    collection, the only difference in execution, and it's covered by facade
    """
    if file_path.suffix == '.cc':
        return collect_items(file_path, parent, facade=UnitTestFacade(parent.config))
