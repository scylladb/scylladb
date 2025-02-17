#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import shutil
import sys
from argparse import BooleanOptionalAction
from pathlib import Path

import pytest

from test.pylib.cpp.boost.prepare_instance import get_env_manager
from test.pylib.report_plugin import ReportPlugin
from test.pylib.resource_gather import setup_cgroup
from test.pylib.util import get_modes_to_run

ALL_MODES = {'debug': 'Debug',
             'release': 'RelWithDebInfo',
             'dev': 'Dev',
             'sanitize': 'Sanitize',
             'coverage': 'Coverage'}

def pytest_addoption(parser):
    parser.addoption('--mode', choices=ALL_MODES.keys(), action="append", dest="modes",
                     help="Run only tests for given build mode(s)")
    parser.addoption('--tmpdir', action='store', default='testlog', help='''Path to temporary test data and log files. The data is
            further segregated per build mode. Default: ./testlog.''', )
    parser.addoption('--run_id', action='store', default=None, help='Run id for the test run')
    parser.addoption("--gather-metrics", action=BooleanOptionalAction, default=False)
    parser.addoption("--prepare-env", action=BooleanOptionalAction, default=True)


@pytest.fixture(scope="session")
def build_mode(request):
    """
    This fixture returns current build mode.
    This is for running tests through the test.py script, where only one mode is passed to the test
    """
    # to avoid issues when there's no provided mode parameter, do it in two steps: get the parameter and if it's not
    # None, get the first value from the list
    mode = request.config.getoption("modes")
    if mode:
        return mode[0]
    return mode

def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())

def pytest_collection_modifyitems(config, items):
    """
    This is a standard pytest method.
    This is needed to modify the test names with dev mode and run id to differ them one from another
    """
    run_id = config.getoption('run_id', None)

    for item in items:
        # check if this is custom cpp tests that have additional attributes for name modification
        if hasattr(item, 'mode'):
            # modify name with mode that is always present in cpp tests
            item.nodeid = f'{item.nodeid}.{item.mode}'
            item.name = f'{item.name}.{item.mode}'
            if item.run_id:
                item.nodeid = f'{item.nodeid}.{item.run_id}'
                item.name = f'{item.name}.{item.run_id}'
        else:
            # here go python tests that are executed through test.py
            # since test.py is responsible for creating several tests with the required mode,
            # a list with modes contains only one value,
            # that's why in name modification the first element is used
            modes = config.getoption('modes')
            if modes:
                item._nodeid = f'{item._nodeid}.{modes[0]}'
                item.name = f'{item.name}.{modes[0]}'
            if run_id:
                item._nodeid = f'{item._nodeid}.{run_id}'
                item.name = f'{item.name}.{run_id}'

@pytest.hookimpl(wrapper=True)
def pytest_runtestloop(session):
    # temporary solution to make object_store tests pass, since they are requiring S3 and check that the main logic works correctly
    # should be deleted when https://github.com/scylladb/scylladb/pull/23087 wil be merged, until that this PR is blocked
    if session.config.getoption('collectonly'):
        yield
        return
    temp_dir = Path(session.config.rootpath).parent / session.config.getoption('tmpdir')
    modes = get_modes_to_run(session)

    temp_dir.mkdir(exist_ok=True)
    pytest_dirs = [temp_dir / mode / 'pytest' for mode in modes]
    for directory in [*pytest_dirs, temp_dir / 'ldap_instances']:
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True, exist_ok=True)

    if not session.config.getoption('prepare_env'):
        yield
        return

    is_worker = False
    if 'xdist' in sys.modules:
        is_worker = sys.modules['xdist'].is_xdist_worker(session)

    with get_env_manager(temp_dir, is_worker, session.config.getoption('gather_metrics')):
        yield