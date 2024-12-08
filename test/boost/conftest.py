#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import sys
from pathlib import Path, PosixPath

import pytest
from pytest import Collector

from test.pylib.cpp.boost.boost_facade import BoostTestFacade
from test.pylib.cpp.boost.prepare_instance import get_prepare_instance
from test.pylib.cpp.common_cpp_conftest import collect_items, get_modes_to_run


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    """
    Method triggered automatically by pytest to collect files from a directory. Boost and unit have the same logic for
    collection, the only difference in execution, and it's covered by facade
    """
    # One of the files in the directory has additional extensions .inc. It's not a test and will not have a binary for
    # execution, so it should be excluded from collecting
    if file_path.suffix == '.cc' and '.inc' not in file_path.suffixes:
        return collect_items(file_path, parent, facade=BoostTestFacade(parent.config), run_id=parent.config.getoption('run_id'))


@pytest.hookimpl(wrapper=True)
def pytest_runtestloop(session):
    """
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_runtestloop
    This hook is needed to start the Minio and S3 mock servers before tests. After starting the servers, the default
    pytest's runtestloop takes control. Finally part is responsible for stopping servers regardless of failure in the tests.
    """
    modes = get_modes_to_run(session)
    is_worker = False
    if 'xdist' in sys.modules:
        sys.modules['xdist'] .is_xdist_worker(session)
    temp_dir = Path(session.config.getoption('tmpdir'))

    prepare_instance = get_prepare_instance(temp_dir, is_worker, modes)
    prepare_instance.prepare()
    try:
        yield
    finally:
        prepare_instance.cleanup()
