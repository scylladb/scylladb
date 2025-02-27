#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from pathlib import PosixPath

from pytest import Collector

from test.pylib.cpp.boost.boost_facade import BoostTestFacade, COMBINED_TESTS
from test.pylib.cpp.common_cpp_conftest import collect_items, get_combined_tests


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    """
    Method triggered automatically by pytest to collect files from a directory. Boost and unit have the same logic for
    collection, the only difference in execution, and it's covered by facade
    """
    # One of the files in the directory has additional extensions .inc. It's not a test and will not have a binary for
    # execution, so it should be excluded from collecting
    if file_path.suffix == '.cc' and '.inc' not in file_path.suffixes and file_path.stem != COMBINED_TESTS.stem:
        return collect_items(file_path, parent, facade=BoostTestFacade(parent.config, get_combined_tests(parent.session)))
