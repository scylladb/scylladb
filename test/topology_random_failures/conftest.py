#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import pytest


pytest_plugins = [
    "test.topology.conftest",
]


def pytest_addoption(parser: pytest.Parser) -> None:
    # Limit for number of tests in topology_experimental_raft/test_random_failures.  Can be set in pytest.ini
    parser.addoption('--raft-failure-injections-tests-count', action='store', default=None, type=int,
                     help='how many tests to generate for RAFT randomized failure injections')

    # Following options can be used to run same test sequence as some another run.
    parser.addoption('--raft-failure-injections-seed', action='store', default=None, type=int,
                     help='seed for RAFT randomized failure injections')
    parser.addoption('--raft-errors-last-index', action='store', default=None, type=int,
                     help='last index for RAFT failure injection types')
    parser.addoption('--raft-events-last-index', action='store', default=None, type=int,
                     help='last index for event types during RAFT failure injections')
