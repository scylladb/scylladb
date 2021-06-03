# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# This file contains "test fixtures", a pytest concept described in
# https://docs.pytest.org/en/latest/fixture.html.
# A "fixture" is some sort of setup which an invididual test requires to run.
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
