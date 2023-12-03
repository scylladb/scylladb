# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file contains "test fixtures", a pytest concept described in
# https://docs.pytest.org/en/latest/fixture.html.
# A "fixture" is some sort of setup which an individual test requires to run.
# The fixture has setup code and teardown code, and if multiple tests
# require the same fixture, it can be set up only once - while still allowing
# the user to run individual tests and automatically set up the fixtures they need.

import pytest

def pytest_addoption(parser):
    parser.addoption("--redis-host", action="store", default="localhost",
        help="ip address")
    parser.addoption("--redis-port", action="store", type=int, default=6379,
        help="port number")


@pytest.fixture
def redis_host(request):
    return request.config.getoption('--redis-host')

@pytest.fixture
def redis_port(request):
    return request.config.getoption('--redis-port')
