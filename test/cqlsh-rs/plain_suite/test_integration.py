# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Parametrized pytest wrapper for cqlsh-rs 'test-plain' integration tests.

See tools/cqlsh-rs/tests/test_categories.toml for the mapping of test
categories to required Scylla configurations and env vars.
"""

import os

import pytest

from test.pylib.cqlsh_rs_helpers import discover_cargo_tests, run_cargo_test, run_cargo_test_all

CATEGORY = "test-plain"
DISCOVERED_TESTS = discover_cargo_tests(CATEGORY)


def make_env(host: str, port: int) -> dict:
    return {**os.environ, "CQLSH_TEST_HOST": host, "CQLSH_TEST_PORT": str(port),
            "CQLSH_DEFAULT_CONNECT_TIMEOUT_SECONDS": "30"}


@pytest.mark.parametrize("test_name", DISCOVERED_TESTS or ["PENDING_COMPILATION"])
async def test_cargo(host, port, test_name, cargo_precompile, cqlsh_rs_repo_dir):
    if test_name == "PENDING_COMPILATION":
        tests = discover_cargo_tests(CATEGORY, invalidate_cache=True)
        if not tests:
            pytest.skip("No cargo tests discovered after compilation")
        run_cargo_test_all(CATEGORY, make_env(host, port), cqlsh_rs_repo_dir)
        return

    run_cargo_test(test_name, CATEGORY, make_env(host, port), cqlsh_rs_repo_dir)
