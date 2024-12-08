#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from copy import copy

import pytest

from test.pylib.report_plugin import ReportPlugin

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
    parser.addoption('--repeat', action='store', type=int, help='Number of times to repeat each test')
    parser.addoption('--run_id', action='store', default=None, help='Run id for the test run')


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
    This is a standard pytest method. This is needed to apply the repeats for each test without making collect again
    """
    repeat = config.getoption('repeat')
    new_items = []
    if not repeat:
        return items
    for item in items:
        for i in range(1, repeat + 1):
            new_item = copy(item)
            new_item.nodeid = f"{item.nodeid}.{i}"
            new_item.name = new_item.nodeid
            new_items.append(new_item)
    items[:] = new_items
