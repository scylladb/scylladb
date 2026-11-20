# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""cqlsh-rs 'ssl' integration tests: client_encryption_options enabled.

See tools/cqlsh-rs/tests/test_categories.toml for what the category covers.
"""

import os

import pytest

from test.pylib.cqlsh_rs_helpers import discover_tests, run_test

CATEGORY = "ssl"


@pytest.mark.parametrize("test_name", discover_tests(CATEGORY))
async def test_cargo(host, port, test_name, ssl_ca_path, require_cargo):
    env = {**os.environ,
           "CQLSH_TEST_HOST": host,
           "CQLSH_TEST_PORT": str(port),
           "CQLSH_TEST_SSL_CA_PATH": ssl_ca_path,
           "CQLSH_DEFAULT_CONNECT_TIMEOUT_SECONDS": "30"}
    run_test(test_name, env)
