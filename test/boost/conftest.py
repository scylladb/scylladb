#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import os
import sys
from pathlib import Path, PosixPath


import pytest
from pytest import Collector

from test.pylib.cpp.boost.boost_facade import BoostTestFacade, COMBINED_TESTS
from test.pylib.cpp.boost.prepare_instance import get_env_manager
from test.pylib.cpp.common_cpp_conftest import collect_items, get_combined_tests
from test.pylib.util import get_modes_to_run


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    """
    Method triggered automatically by pytest to collect files from a directory. Boost and unit have the same logic for
    collection, the only difference in execution, and it's covered by facade
    """
    # One of the files in the directory has additional extensions .inc. It's not a test and will not have a binary for
    # execution, so it should be excluded from collecting
    if file_path.suffix == '.cc' and '.inc' not in file_path.suffixes and file_path.stem != COMBINED_TESTS.stem:
        return collect_items(file_path, parent, facade=BoostTestFacade(parent.config, get_combined_tests(parent.session)))


@pytest.hookimpl(wrapper=True)
def pytest_runtestloop(session):
    """
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_runtestloop
    This hook is needed to start the Minio and S3 mock servers before tests. After starting the servers, the default
    pytest's runtestloop takes control. Finally part is responsible for stopping servers regardless of failure in the tests.
    """
    if session.config.getoption('collectonly'):
        yield
        return
    temp_dir = Path(session.config.rootpath, '..', session.config.getoption('tmpdir'))
    modes = get_modes_to_run(session)
    is_worker = False
    if 'xdist' in sys.modules:
        is_worker = sys.modules['xdist'].is_xdist_worker(session)

    with get_env_manager(temp_dir, is_worker, modes):
        yield
