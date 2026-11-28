# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""cqlsh-rs 'auth' integration tests: PasswordAuthenticator.

cqlsh-rs has no tests in this category yet, so the whole suite is deselected
during collection and no Scylla is started for it.  See the issue referenced
from test/cqlsh-rs/conftest.py.
"""

import os

import pytest

from test.pylib.cqlsh_rs_helpers import discover_tests, run_test

CATEGORY = "auth"


@pytest.mark.parametrize("test_name", discover_tests(CATEGORY))
async def test_cargo(host, port, test_name, require_cargo):
    env = {**os.environ,
           "CQLSH_TEST_HOST": host,
           "CQLSH_TEST_PORT": str(port),
           "CQLSH_TEST_USERNAME": "cassandra",
           "CQLSH_TEST_PASSWORD": "cassandra",
           "CQLSH_DEFAULT_CONNECT_TIMEOUT_SECONDS": "30"}
    run_test(test_name, env)
