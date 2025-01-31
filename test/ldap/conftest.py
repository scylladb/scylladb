#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import sys
from pathlib import PosixPath
from random import randint

import pytest
from pytest import Collector

from test.pylib.cpp.boost.boost_facade import BoostTestFacade
from test.pylib.cpp.ldap.prepare_instance import get_env_manager
from test.pylib.cpp.common_cpp_conftest import collect_items, get_modes_to_run, get_root_path, get_combined_tests


def pytest_addoption(parser):
    parser.addoption('--byte-limit', action="store", default=None, type=int,
                        help="Specific byte limit for failure injection (random by default)")


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    """
    Method triggered automatically by pytest to collect files from a directory.
    These tests can use BoostFacade since they're Boost tests located in different directory.
    """
    if file_path.suffix == '.cc':
        return collect_items(file_path, parent, facade=BoostTestFacade(parent.config))


@pytest.hookimpl(wrapper=True)
def pytest_runtestloop(session):
    """
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_runtestloop
    This hook is needed to start the Minio and S3 mock servers before tests. After starting the servers, the default
    pytest's runtestloop takes control. Finally part is responsible for stopping servers regardless of failure in the tests.
    """
    # make a collection only without starting unnecessary services
    if session.config.getoption('collectonly'):
        yield
        return
    root_dir = get_root_path(session)
    temp_dir = root_dir / session.config.getoption('tmpdir')
    try:
        byte_limit = session.config.getoption('byte-limit')
    except ValueError:
        byte_limit = randint(0, 2000)
    modes = get_modes_to_run(session)
    worker_id = 'master'
    if 'xdist' in sys.modules:
        worker_id = sys.modules['xdist'].get_xdist_worker_id(session)
    with get_env_manager(root_dir, temp_dir, worker_id, modes, byte_limit):
        yield
