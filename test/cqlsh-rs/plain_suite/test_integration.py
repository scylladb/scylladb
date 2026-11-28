# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""cqlsh-rs 'plain' integration tests: default CQL, no encryption, no auth.

See tools/cqlsh-rs/tests/test_categories.toml for what the category covers.
"""

import os

import pytest

from test.pylib.cqlsh_rs_helpers import discover_tests, run_test

CATEGORY = "plain"


@pytest.mark.parametrize("test_name", discover_tests(CATEGORY))
async def test_cargo(host, port, test_name, require_cargo):
    env = {**os.environ,
           "CQLSH_TEST_HOST": host,
           "CQLSH_TEST_PORT": str(port),
           "CQLSH_DEFAULT_CONNECT_TIMEOUT_SECONDS": "30"}
    run_test(test_name, env)
